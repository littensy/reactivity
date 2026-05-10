-- Flags for tracking the state of reactive nodes and their types. These are
-- typed as string literals for better type inference, but are actually numbers
-- for better performance and memory.

local CLEAN: "CLEAN" = 0 :: any -- Default state, no changes detected
local DIRTY: "DIRTY" = 1 :: any -- Value is outdated and needs recomputation
local PENDING: "PENDING" = 2 :: any -- Might be dirty, pending verification
local DISPOSED: "DISPOSED" = 3 :: any -- Should be cleaned up and ignored

local SIGNAL: "SIGNAL" = 0 :: any
local COMPUTED: "COMPUTED" = 1 :: any
local EFFECT: "EFFECT" = 2 :: any
local EFFECT_SCOPE: "EFFECT_SCOPE" = 3 :: any

export type SignalNode<T = any> = Dependency & {
	-- The type of the node (0 = signal).
	read type: typeof(SIGNAL),
	-- If the signal's status is dirty, it has a pending value change that has
	-- not been applied yet. This allows the signal to defer updates until the
	-- next access, skipping updates for changes that were reverted.
	status: typeof(CLEAN) | typeof(DIRTY),
	-- The value the signal last returned, which may be outdated.
	currentValue: T,
	-- The latest value passed to the signal, which may not have been applied
	-- yet if the signal is dirty.
	pendingValue: T,
}

export type ComputedNode<T = any> = Subscriber & Dependency & {
	-- The type of the node (1 = computed signal).
	read type: typeof(COMPUTED),
	-- The status of the computed signal, which tracks whether it needs to be
	-- recomputed based on changes to its dependencies. Dirty means it needs to
	-- be recomputed, and pending means to check dependencies for changes.
	status: typeof(CLEAN) | typeof(DIRTY) | typeof(PENDING),
	-- The function used to compute the value of the computed signal, taking
	-- the previous value as an argument.
	read getter: (T?) -> T,
	-- The current value of the computed signal.
	value: T,
}

export type EffectNode = Subscriber & Dependency & {
	-- The type of the node (2 = effect).
	read type: typeof(EFFECT),
	-- The status of the effect. An additional disposed status determines
	-- whether future updates should be ignored.
	status: typeof(CLEAN) | typeof(DIRTY) | typeof(PENDING) | typeof(DISPOSED),
	-- The function to run when the effect is triggered, which may return a
	-- cleanup function or subscribe to dependencies.
	read callback: () -> ...(() -> ())?,
	-- Cleanup functions returned by the effect's callback, which are run when
	-- the effect is re-run or disposed.
	read cleanups: { () -> () },
	-- Whether this effect is currently queued to run, used to avoid queuing
	-- the same effect until it has a chance to run and reset this flag.
	queued: boolean,
}

export type EffectScopeNode = Subscriber & Dependency & {
	-- The type of the node (3 = effect scope).
	read type: typeof(EFFECT_SCOPE),
	-- The status of the effect scope should never be dirty or pending, but it
	-- can be disposed to clean up all of its child effects at once.
	status: typeof(CLEAN) | typeof(DIRTY) | typeof(PENDING) | typeof(DISPOSED),
	-- Cleanup functions returned by the effect scope's callback, which are run
	-- when the effect scope is disposed.
	read cleanups: { () -> () },
}

type Subscriber = {
	-- The dependencies that this subscriber is subscribed to, which determine
	-- when it should be notified of updates.
	read deps: { DependencyNode },
	-- The range of stale dependencies that can potentially be reused during
	-- the next run. After all dependencies have been processed, any stale
	-- dependency that was not reused will be removed.
	staleDepsStart: number,
	staleDepsEnd: number,
}

type Dependency = {
	-- The subscribers to notify when this dependency updates. In effects and
	-- effect scopes, this tracks the parent subscriber instead.
	read subs: { SubscriberNode },
}

export type SubscriberNode = ComputedNode | EffectNode | EffectScopeNode
export type DependencyNode = SignalNode | ComputedNode | EffectNode | EffectScopeNode
export type ReactiveNode = SignalNode | ComputedNode | EffectNode | EffectScopeNode

export type Update<T> = ((current: T) -> T) | T
export type Signal<T> = (...Update<T>) -> T

local queue: { EffectNode } = {}
local queueLength = 0
local notifyIndex = 0
local batchDepth = 0
local activeSub: SubscriberNode?

local unwatched: (sub: SubscriberNode) -> ()

local function getActiveSub(): SubscriberNode?
	return activeSub
end

local function setActiveSub(sub: SubscriberNode?): SubscriberNode?
	local prevSub = activeSub
	activeSub = sub
	return prevSub
end

local function queueEffect(effect: EffectNode)
	if effect.queued then
		return
	end

	local startIndex = queueLength + 1
	local nextEffect = effect

	-- Queue this effect and its parent effects to ensure dirty/pending outer
	-- effects can run before inner effects
	repeat
		nextEffect.queued = true
		queueLength += 1
		queue[queueLength] = nextEffect

		-- Traverse up the parent chain to queue any parent effects as well
		nextEffect = nextEffect.subs[1]
	until not nextEffect or nextEffect.type ~= EFFECT or nextEffect.queued

	local endIndex = queueLength

	-- Reverse the order of the newly queued effects to ensure outer effects
	-- are checked before inner effects
	while startIndex < endIndex do
		queue[startIndex], queue[endIndex] = queue[endIndex], queue[startIndex]
		startIndex += 1
		endIndex -= 1
	end
end

-- Marks immediate subscribers as dirty without checking their dependencies.
-- Used when a node is updated to cause all of its subscribers to be updated.
local function shallowPropagateDirty(dep: DependencyNode)
	for _, sub in dep.subs do
		if sub.status == PENDING then
			sub.status = DIRTY
			if sub.type == EFFECT then
				queueEffect(sub)
			end
		end
	end
end

-- Marks nested subscribers as pending and queue effect runs. Used when a node
-- receives an update but has not been accessed yet, so it may not be known if
-- the update will cause a change.
local function propagatePending(dep: DependencyNode)
	for _, sub in dep.subs do
		if sub.status == CLEAN then
			sub.status = PENDING
			if sub.type == COMPUTED then
				propagatePending(sub)
			elseif sub.type == EFFECT then
				queueEffect(sub)
			end
		end
	end
end

-- Links a dependency to a subscriber, so that the subscriber will be notified
-- when the dependency updates. If the subscriber has stale dependencies from a
-- previous run, it will attempt to reuse the existing dependency.
local function link(dep: DependencyNode, sub: SubscriberNode)
	local staleDepsStart = sub.staleDepsStart

	-- If the next stale dependency is the same as the new dependency, we can
	-- skip linking and mark it as used.
	--
	-- Note that staleDepsStart and staleDepsEnd are not reset until the next
	-- evaluation. Always running this check has a chance to reuse newly
	-- created dependencies within the same evaluation with minimal overhead.
	if sub.deps[staleDepsStart] == dep then
		sub.staleDepsStart = staleDepsStart + 1
	else
		table.insert(sub.deps, dep)
		table.insert(dep.subs, sub)
	end
end

-- Unlinks a dependency from a subscriber. If the dependency has no more
-- subscribers after unlinking, it will be unwatched to release its resources.
local function unlink(dep: DependencyNode, sub: SubscriberNode, subDepIndex: number?)
	table.remove(sub.deps, subDepIndex or table.find(sub.deps, dep) or 0)
	table.remove(dep.subs, table.find(dep.subs, sub) or 0)

	if not dep.subs[1] then
		unwatched(dep) -- Release nodes when they have no subscribers
	end
end

-- Removes any stale dependencies that were not reused during the current run.
-- A dependency is stale if the link function did not mark it as used by
-- excluding it from the stale dependency range.
local function purgeStaleDeps(sub: SubscriberNode)
	local staleDepsStart = sub.staleDepsStart
	local staleDepsEnd = sub.staleDepsEnd

	if staleDepsStart > staleDepsEnd then
		return
	end

	local subDeps = sub.deps
	local range = staleDepsEnd - staleDepsStart + 1

	-- Unlink from depSubs first to access stale dependencies before they are
	-- removed from subDeps
	for depIndex = staleDepsStart, staleDepsEnd do
		local dep = subDeps[depIndex]
		if dep then
			local depSubs = dep.subs
			table.remove(depSubs, table.find(depSubs, sub) or 0)
			if not depSubs[1] then
				unwatched(dep) -- Release nodes when they have no subscribers
			end
		end
	end

	-- Optimize table removal by shifting the remaining dependencies in a
	-- batch, instead of removing each dependency one by one, which would cause
	-- shifting on every removal.
	table.move(subDeps, staleDepsEnd + 1, #subDeps + range, staleDepsStart)
end

local function clearDeps(sub: SubscriberNode)
	local nextDep = sub.deps[1]
	while nextDep do
		unlink(nextDep, sub, 1)
		nextDep = sub.deps[1]
	end
	sub.staleDepsStart = 1
	sub.staleDepsEnd = 0
end

local function unlinkInnerEffects(sub: SubscriberNode)
	local deps = sub.deps
	local index = 1
	local nextDep = deps[index]
	while nextDep do
		if nextDep.type == EFFECT or nextDep.type == EFFECT_SCOPE then
			unlink(nextDep, sub, index)
		else
			index += 1
		end
		nextDep = deps[index]
	end
end

local function runCleanups(cleanups: { () -> () })
	for index, cleanup in cleanups do
		cleanups[index] = nil

		local prevSub = setActiveSub(nil)
		local success, err = pcall(cleanup)
		activeSub = prevSub

		if not success then
			error(err, 0)
		end
	end
end

local function stopEffect(effect: EffectNode | EffectScopeNode)
	effect.status = DISPOSED

	if effect.type == EFFECT then
		effect.queued = false
	end

	-- Unlink from dependencies and child effects
	clearDeps(effect)

	-- Unlink from the parent node
	if effect.subs[1] then
		unlink(effect, effect.subs[1])
	end

	runCleanups(effect.cleanups)
end

function unwatched(node: DependencyNode)
	if node.type == EFFECT or node.type == EFFECT_SCOPE then
		-- If an effect loses its only subscriber, its parent effect re-ran or
		-- got disposed, so this child effect should be disposed as well
		stopEffect(node)
	elseif node.type == COMPUTED then
		-- Clearing a computed node's dependencies when it loses all of its
		-- subscribers allows it to be garbage collected
		node.status = DIRTY
		clearDeps(node)
	end
end

local function updateComputed(computed: ComputedNode): boolean
	computed.status = CLEAN
	computed.staleDepsStart = 1
	computed.staleDepsEnd = #computed.deps

	local prevSub = setActiveSub(computed)
	local oldValue = computed.value
	local success, newValue = pcall(computed.getter, oldValue)
	activeSub = prevSub

	purgeStaleDeps(computed)

	if success then
		computed.value = newValue
		return oldValue ~= newValue
	else
		error(newValue, 0)
	end
end

local function updateSignal(signal: SignalNode): boolean
	local pendingValue = signal.pendingValue
	signal.status = CLEAN
	if signal.currentValue ~= pendingValue then
		signal.currentValue = pendingValue
		return true
	else
		return false
	end
end

local function updateNode(node: SignalNode | ComputedNode): boolean
	if node.type == COMPUTED then
		return updateComputed(node)
	else
		return updateSignal(node)
	end
end

local function checkDirty(sub: SubscriberNode): boolean
	for _, dep in sub.deps do
		if sub.status == DIRTY then
			-- The original node may have been indirectly marked dirty while
			-- checking a dependency, so this is checked every iteration
			return true
		elseif dep.type == COMPUTED or dep.type == SIGNAL then
			if dep.status == DIRTY then
				-- Mark immediate subscribers as dirty if an update occurs
				if updateNode(dep) then
					shallowPropagateDirty(dep)
					return true
				end
			elseif dep.status == PENDING then
				-- Check if the node has any dirty or pending dependencies that
				-- would cause it to be dirty as well
				if checkDirty(dep) then
					-- Mark immediate subscribers as dirty if an update occurs
					if updateNode(dep) then
						shallowPropagateDirty(dep)
						return true
					end
				elseif dep.status == PENDING then
					-- The update resulted in no change and can be skipped
					dep.status = CLEAN
				end
			end
		end
	end
	return false
end

local function run(effect: EffectNode)
	if effect.status == DIRTY or (effect.status == PENDING and checkDirty(effect)) then
		runCleanups(effect.cleanups)

		-- Clean up inner effects before re-evaluating this effect, since
		-- relying on purgeStaleDeps to clean up effects after re-running
		-- might run cleanups in an unexpected order
		unlinkInnerEffects(effect)

		-- Cancel the run if the effect was stopped during cleanup
		if effect.status == DISPOSED then
			return
		end

		effect.status = CLEAN
		effect.queued = false
		effect.staleDepsStart = 1
		effect.staleDepsEnd = #effect.deps

		local prevSub = setActiveSub(effect)
		local success, cleanup = pcall(effect.callback)
		activeSub = prevSub

		-- Remove any dependencies that were not accessed during this run
		purgeStaleDeps(effect)

		if success and type(cleanup) == "function" then
			table.insert(effect.cleanups, cleanup)
		elseif not success then
			error(cleanup, 0)
		end

		-- If the effect was disposed during its execution, run any cleanups
		-- that might have been missed
		if effect.status == DISPOSED then
			stopEffect(effect)
		end
	elseif effect.status == PENDING then
		effect.status = CLEAN
		effect.queued = false
	end
end

local function flush()
	-- Bail out early if a flush is already in progress to allow effects to
	-- complete before the next effect runs
	if batchDepth ~= 0 or notifyIndex ~= 0 then
		return
	end

	local errors: { string } = {}
	local failed = false

	-- Run all queued effects, catching errors to ensure all effects have a
	-- chance to run and clean up before the error is thrown
	while notifyIndex < queueLength do
		local success, err = pcall(function()
			repeat
				notifyIndex += 1
				local effect = queue[notifyIndex]
				queue[notifyIndex] = nil
				run(effect)
			until notifyIndex >= queueLength
		end)

		if not success then
			failed = true
			table.insert(errors, tostring(err))
		end
	end

	notifyIndex = 0
	queueLength = 0

	if failed then
		error(`An error occurred during effect execution:\n\n{table.concat(errors, "\n\n")}`, 0)
	end
end

local function startBatch()
	batchDepth += 1
end

local function endBatch()
	batchDepth -= 1
	flush()
end

local function signal<T>(initialValue: T): Signal<T>
	local node: SignalNode = {
		type = SIGNAL,
		status = CLEAN,
		subs = {},
		currentValue = initialValue,
		pendingValue = initialValue,
	}

	return function(...: Update<T>): T
		if select("#", ...) == 0 then
			-- Check if the signal has a pending value that needs to be applied
			if node.status == DIRTY then
				if updateSignal(node) then
					shallowPropagateDirty(node)
				end
			end

			-- Add the signal as a dependency of the active subscriber, so it
			-- will be notified of future updates
			if activeSub then
				link(node, activeSub)
			end
		else
			local update: Update<T> = ...
			local value: T

			-- Unwrap the value if an updater function was passed, otherwise
			-- use the value directly
			if type(update) == "function" then
				local prevSub = setActiveSub(nil)
				local success, result = pcall(update, node.pendingValue)
				activeSub = prevSub
				if success then
					value = result
				else
					error(result, 2)
				end
			else
				value = update
			end

			-- If the new value is different from the current pending value,
			-- mark the signal as dirty and propagate the change to subscribers
			if node.pendingValue ~= value then
				node.pendingValue = value
				node.status = DIRTY
				propagatePending(node)
				flush()
			end
		end

		return node.pendingValue
	end
end

local function computed<T>(getter: (previousValue: T?) -> T): () -> T
	local node: ComputedNode = {
		type = COMPUTED,
		status = CLEAN,
		deps = {},
		subs = {},
		staleDepsStart = 1,
		staleDepsEnd = 0,
		getter = getter,
	}
	local initialized = false

	return function(): T
		if node.status == DIRTY then
			if updateComputed(node) then
				shallowPropagateDirty(node)
			end
		elseif node.status == PENDING then
			if checkDirty(node) then
				if updateComputed(node) then
					shallowPropagateDirty(node)
				end
			elseif node.status == PENDING then
				node.status = CLEAN
			end
		elseif not initialized then
			-- First access should initialize the computed value
			local prevSub = setActiveSub(node)
			local success, result = pcall(node.getter)
			activeSub = prevSub

			if success then
				initialized = true
				node.value = result
			else
				error(result, 2)
			end
		end

		if activeSub then
			link(node, activeSub)
		end

		return node.value
	end
end

local function effect(callback: () -> ...(() -> ())?): () -> ()
	local node: EffectNode = {
		type = EFFECT,
		status = CLEAN,
		deps = {},
		subs = {},
		staleDepsStart = 1,
		staleDepsEnd = 0,
		callback = callback,
		queued = false,
		cleanups = {},
	}

	-- Clean up the node when the parent subscriber updates or gets disposed by
	-- linking to the parent subscriber, which allows the process of clearing
	-- dependencies to also clean up inner effects.
	if activeSub then
		link(node, activeSub)
	end

	-- Start a batch so that the effect can register a cleanup function before
	-- it can receive notifications, ensuring the cleanup always runs
	startBatch()

	local prevSub = setActiveSub(node)
	local success, result = pcall(node.callback)
	activeSub = prevSub

	if success and type(result) == "function" then
		table.insert(node.cleanups, result)
	elseif not success then
		endBatch()
		error(result, 2)
	end

	endBatch()

	return function()
		stopEffect(node)
	end
end

local function effectScope(callback: () -> ...(() -> ())?, detached: boolean?): () -> ()
	local node: EffectScopeNode = {
		type = EFFECT_SCOPE,
		status = CLEAN,
		deps = {},
		subs = {},
		staleDepsStart = 1,
		staleDepsEnd = 0,
		cleanups = {},
	}

	-- Clean up the node when the parent subscriber updates or gets disposed by
	-- linking to the parent subscriber, which allows the process of clearing
	-- dependencies to also clean up inner effects.
	if activeSub and not detached then
		link(node, activeSub)
	end

	local prevSub = setActiveSub(node)
	local success, result = pcall(callback)
	activeSub = prevSub

	if success and type(result) == "function" then
		table.insert(node.cleanups, result)
	elseif not success then
		error(result, 2)
	end

	return function()
		stopEffect(node)
	end
end

local function trigger(callback: () -> ...any)
	local sub: EffectNode = {
		type = EFFECT,
		status = DISPOSED,
		deps = {},
		subs = {},
		staleDepsStart = 1,
		staleDepsEnd = 0,
		callback = function() end,
		queued = false,
		cleanups = {},
	}

	local prevSub = setActiveSub(sub)
	local success, err = pcall(callback)
	activeSub = prevSub

	local dep = sub.deps[1]
	while dep do
		unlink(dep, sub, 1)
		propagatePending(dep)
		shallowPropagateDirty(dep)
		dep = sub.deps[1]
	end

	flush()

	if not success then
		error(err, 2)
	end
end

local function untracked<Args..., Results...>(callback: (Args...) -> Results..., ...: Args...): Results...
	if activeSub then
		local prevSub = setActiveSub(nil)
		local results: { any } = { pcall(callback, ...) }
		activeSub = prevSub

		if not results[1] then
			error(results[2], 2)
		end

		return unpack(results, 2)
	else
		return callback(...)
	end
end

local function onCleanup(callback: () -> ())
	if activeSub and (activeSub.type == EFFECT or activeSub.type == EFFECT_SCOPE) then
		table.insert(activeSub.cleanups, callback)
	end
end

return table.freeze({
	getActiveSub = getActiveSub,
	setActiveSub = setActiveSub,
	startBatch = startBatch,
	endBatch = endBatch,
	signal = signal,
	computed = computed,
	effect = effect,
	effectScope = effectScope,
	trigger = trigger,
	untracked = untracked,
	onCleanup = onCleanup,
})
