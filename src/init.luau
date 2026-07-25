local CLEAN: "CLEAN" = 0 :: any -- Default state, no changes detected
local DIRTY: "DIRTY" = 1 :: any -- Value is outdated and needs recomputation
local PENDING: "PENDING" = 2 :: any -- Might be dirty, pending verification
local DISPOSED: "DISPOSED" = 3 :: any -- Should be cleaned up and ignored

local SIGNAL: "SIGNAL" = 0 :: any
local COMPUTED: "COMPUTED" = 1 :: any
local EFFECT: "EFFECT" = 2 :: any
local EFFECT_SCOPE: "EFFECT_SCOPE" = 3 :: any

export type SignalNode<T = any> = BaseDependency & {
	read type: typeof(SIGNAL),
	status: typeof(CLEAN) | typeof(DIRTY),
	currentValue: T,
	pendingValue: T,
}

export type ComputedNode<T = any> = BaseSubscriber & BaseDependency & {
	read type: typeof(COMPUTED),
	status: typeof(CLEAN) | typeof(DIRTY) | typeof(PENDING),
	read getter: (T?) -> T,
	value: T?,
}

export type EffectNode = BaseSubscriber & BaseDependency & {
	read type: typeof(EFFECT),
	status: typeof(CLEAN) | typeof(DIRTY) | typeof(PENDING) | typeof(DISPOSED),
	read callback: () -> ...(() -> ())?,
	read cleanups: { () -> () },
	queued: boolean,
	hasChildEffect: boolean?,
}

export type EffectScopeNode = BaseSubscriber & BaseDependency & {
	read type: typeof(EFFECT_SCOPE),
	status: typeof(CLEAN) | typeof(DIRTY) | typeof(PENDING) | typeof(DISPOSED),
	read cleanups: { () -> () },
}

type BaseSubscriber = {
	read deps: { Dependency },
	staleDepCursor: number,
}

type BaseDependency = {
	read subs: { Subscriber },
}

export type Subscriber = ComputedNode | EffectNode | EffectScopeNode
export type Dependency = SignalNode | ComputedNode | EffectNode | EffectScopeNode
export type ReactiveNode = SignalNode | ComputedNode | EffectNode | EffectScopeNode

export type Update<T> = ((current: T) -> T) | T
export type Signal<T> = (...Update<T>) -> T

local flags = {
	-- Enforces synchronous, non-yielding behavior in signals and effects.
	-- Also enables state validation in Charm Sync to catch sync errors early.
	-- Enabled by default in Roblox Studio.
	strict = false,
	-- Enforces immutability of tables by deep-freezing table values. Enabled
	-- by default in Roblox Studio.
	frozen = false,
}

local queue: { EffectNode } = {}
local queueLength = 0
local notifyIndex = 0
local batchDepth = 0
local activeSub: Subscriber?

-- Hoisted so unwatched() and unlink() can depend on each other
local unwatched: (sub: Subscriber) -> ()

-- Wraps a user-created function to prevent yielding when in strict mode.
local function noYield<Args..., Results...>(callback: (Args...) -> Results...): (Args...) -> Results...
	-- If the debug sources match, the function was created in this module, and
	-- can be expected to not yield. Also simplifies internal stack traces.
	if not flags.strict or debug.info(1, "s") == debug.info(callback, "s") then
		return callback
	end

	local handler = function(thread: thread, ok: boolean, ...: Results...): Results...
		if coroutine.status(thread) == "suspended" then
			error(debug.traceback(thread, "Attempted to yield in an effect or scope"), 0)
		elseif ok then
			return ...
		else
			local err = (...)
			if type(err) == "string" then
				error(debug.traceback(thread, err), 0)
			else
				error(tostring(err), 0)
			end
		end
	end

	return function(...: Args...): Results...
		local thread = coroutine.create(callback)
		return handler(thread, coroutine.resume(thread, ...))
	end
end

-- Recursively freezes a table and all nested tables to enforce immutability.
-- Tables with metatables are skipped, as they are assumed to be mutable.
local function deepFreeze(value: any)
	if type(value) == "table" and not table.isfrozen(value) and getmetatable(value) == nil then
		table.freeze(value)
		for _, entry in value do
			deepFreeze(entry)
		end
	end
end

local function getActiveSub(): Subscriber?
	return activeSub
end

local function setActiveSub(sub: Subscriber?): Subscriber?
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

	repeat
		nextEffect.queued = true
		queueLength += 1
		queue[queueLength] = nextEffect

		-- Traverse up the parent chain to queue any parent effects as well
		nextEffect = nextEffect.subs[1]
	until not nextEffect or nextEffect.type ~= EFFECT or nextEffect.queued

	local endIndex = queueLength

	-- Reverse the order of the newly queued effects to ensure parent effects
	-- are checked before child effects
	while startIndex < endIndex do
		queue[startIndex], queue[endIndex] = queue[endIndex], queue[startIndex]
		startIndex += 1
		endIndex -= 1
	end
end

-- Marks immediate subscribers as dirty and queues effects. Used when a node
-- has a valid update to propagate, so pending subscribers should be marked as
-- dirty, or having an updated dependency.
local function shallowPropagateDirty(dep: Dependency)
	for _, sub in dep.subs do
		if sub.status == PENDING then
			sub.status = DIRTY
			if sub.type == EFFECT then
				queueEffect(sub)
			end
		end
	end
end

-- Marks all subscribers as pending and queues effects. Used when a root node
-- initiates an update that notifies multiple levels of subscribers that they
-- might have become dirty.
--
-- While effects immediately check for updates on the next flush, computed
-- nodes update lazily, so they are instead recursively marked as pending.
local function propagatePending(dep: Dependency)
	for _, sub in dep.subs do
		if sub.status == CLEAN then
			sub.status = PENDING
			if sub.type == COMPUTED or sub.type == EFFECT_SCOPE then
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
local function link(dep: Dependency, sub: Subscriber)
	local staleDepCursor = sub.staleDepCursor
	local subDeps = sub.deps

	-- If the next stale dependency is the same as the new dependency, we can
	-- skip linking and mark it as used.
	if subDeps[staleDepCursor] == dep then
		sub.staleDepCursor = staleDepCursor + 1
	else
		table.insert(subDeps, dep)
		table.insert(dep.subs, sub)
	end
end

-- Unlinks a dependency from a subscriber. If the dependency has no more
-- subscribers after unlinking, it will be unwatched to release its resources.
local function unlink(dep: Dependency, sub: Subscriber, subDepIndex: number?)
	local subDeps = sub.deps
	local depSubs = dep.subs

	table.remove(subDeps, subDepIndex or table.find(subDeps, dep) or 0)
	table.remove(depSubs, table.find(depSubs, sub) or 0)

	if not depSubs[1] then
		unwatched(dep)
	end
end

-- Removes any stale dependencies that were not reused during the current run.
-- A dependency is stale if the link function did not mark it as used by
-- excluding it from the stale dependency range.
local function purgeStaleDeps(sub: Subscriber, lastStaleDep: number)
	local staleDepCursor = sub.staleDepCursor

	if staleDepCursor > lastStaleDep then
		return
	end

	local deps = sub.deps
	local staleDepCount = lastStaleDep - staleDepCursor + 1

	-- Purge stale dependencies in reverse order to optimize table removals
	for depIndex = lastStaleDep, staleDepCursor, -1 do
		local dep = deps[depIndex]
		if dep then
			local depSubs = dep.subs
			table.remove(depSubs, table.find(depSubs, sub) or 0)
			if not depSubs[1] then
				unwatched(dep)
			end
		end
	end

	-- Optimize table removal by shifting the remaining dependencies in a
	-- batch, instead of removing each dependency one by one
	table.move(deps, lastStaleDep + 1, #deps + staleDepCount, staleDepCursor)
end

local function purgeDeps(sub: Subscriber)
	local deps = sub.deps
	local dep = deps[1]
	while dep do
		unlink(dep, sub, 1)
		dep = deps[1]
	end
	sub.staleDepCursor = 1
end

local function unlinkChildEffects(sub: Subscriber)
	local deps = sub.deps
	local dep = deps[1]
	local index = 1
	repeat
		if dep.type == EFFECT or dep.type == EFFECT_SCOPE then
			unlink(dep, sub, index)
		else
			index += 1
		end
		dep = deps[index]
	until not dep
end

local function runCleanups(cleanups: { () -> () })
	local failed = false
	local errors: { string } = {}

	for index, cleanup in cleanups do
		cleanups[index] = nil

		local prevSub = setActiveSub(nil)
		local success, err = pcall(cleanup)
		activeSub = prevSub

		if not success then
			failed = true
			table.insert(errors, tostring(err))
		end
	end

	if failed then
		error(`Errors occurred during effect cleanup:\n\n{table.concat(errors, "\n\n")}`, 0)
	end
end

local function stopEffect(effect: EffectNode | EffectScopeNode)
	effect.status = DISPOSED

	if effect.type == EFFECT then
		effect.queued = false
	end

	-- Unlink from dependencies and child effects
	purgeDeps(effect)

	-- Unlink from the parent node
	if effect.subs[1] then
		unlink(effect, effect.subs[1])
	end

	runCleanups(effect.cleanups)
end

function unwatched(node: Dependency)
	if node.type == EFFECT or node.type == EFFECT_SCOPE then
		-- If an effect loses its only subscriber, its parent effect re-ran or
		-- got disposed, so this child effect should be disposed as well
		stopEffect(node)
	elseif node.type == COMPUTED then
		-- Clearing a computed node's dependencies when it loses all of its
		-- subscribers allows it to be garbage collected
		node.status = DIRTY
		purgeDeps(node)
	end
end

local function updateComputed(computed: ComputedNode): boolean
	local lastStaleDep = #computed.deps
	local oldValue = computed.value

	computed.status = CLEAN
	computed.staleDepCursor = 1

	local prevSub = setActiveSub(computed)
	local success, newValue = pcall(computed.getter, oldValue)
	activeSub = prevSub

	purgeStaleDeps(computed, lastStaleDep)

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

local function updateNode(node: SignalNode | ComputedNode | EffectScopeNode): boolean
	if node.type == COMPUTED then
		return updateComputed(node)
	elseif node.type == SIGNAL then
		return updateSignal(node)
	else
		node.status = CLEAN
		return true
	end
end

-- Returns whether a pending node is actually dirty by checking its
-- dependencies for updates. Used to avoid unnecessary updates when a node has
-- been notified of a change that might not require an update.
local function checkDirty(sub: Subscriber): boolean
	for _, dep in sub.deps do
		if sub.status == DIRTY then
			-- The original node may have been indirectly marked dirty while
			-- checking a dependency, so this is checked every iteration
			return true
		elseif dep.type == COMPUTED or dep.type == SIGNAL or dep.type == EFFECT_SCOPE then
			if dep.status == DIRTY then
				if updateNode(dep) then
					shallowPropagateDirty(dep)
					return true
				end
			elseif dep.status == PENDING then
				if checkDirty(dep) then
					if updateNode(dep) then
						shallowPropagateDirty(dep)
						return true
					end
				else
					dep.status = CLEAN
				end
			end
		end
	end
	return false
end

local function run(effect: EffectNode)
	if effect.status == DIRTY or (effect.status == PENDING and checkDirty(effect)) then
		if effect.hasChildEffect then
			unlinkChildEffects(effect)
		end

		runCleanups(effect.cleanups)

		-- Cancel the run if the effect was stopped during cleanup
		if effect.status == DISPOSED then
			return
		end

		local lastStaleDep = #effect.deps

		effect.status = CLEAN
		effect.queued = false
		effect.staleDepCursor = 1

		local prevSub = setActiveSub(effect)
		local success, cleanup = pcall(effect.callback)
		activeSub = prevSub

		purgeStaleDeps(effect, lastStaleDep)

		if success and type(cleanup) == "function" then
			table.insert(effect.cleanups, noYield(cleanup))
		elseif not success then
			error(cleanup, 0)
		end

		-- If the effect was disposed during its execution, run any cleanups
		-- that might have been missed
		if effect.status == DISPOSED then
			stopEffect(effect)
		end
	elseif effect.status ~= DISPOSED then
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
	local node: SignalNode<T> = {
		type = SIGNAL,
		status = CLEAN,
		subs = {},
		currentValue = initialValue,
		pendingValue = initialValue,
	}

	if flags.frozen then
		deepFreeze(initialValue)
	end

	return function(...: Update<T>): T
		if select("#", ...) == 0 then
			if node.status == DIRTY and updateSignal(node) then
				shallowPropagateDirty(node)
			end
			if activeSub then
				link(node, activeSub)
			end
		else
			local value = ...

			if type(value) == "function" then
				local prevSub = setActiveSub(nil)
				local success, result = pcall(noYield(value), node.pendingValue)
				activeSub = prevSub

				if success then
					value = result
				else
					error(result, 2)
				end
			end

			if node.pendingValue ~= value then
				if flags.frozen then
					deepFreeze(value)
				end

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
	local node: ComputedNode<T> = {
		type = COMPUTED,
		status = CLEAN,
		deps = {},
		subs = {},
		staleDepCursor = 1,
		getter = noYield(getter),
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
			else
				node.status = CLEAN
			end
		elseif not initialized then
			local prevSub = setActiveSub(node)
			local success, result = pcall(node.getter, nil)
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
		staleDepCursor = 1,
		callback = noYield(callback),
		queued = false,
		cleanups = {},
	}

	-- Clean up the effect when the parent subscriber updates or gets disposed
	-- by linking it as a dependency.
	if activeSub then
		if activeSub.type == EFFECT then
			activeSub.hasChildEffect = true
		end
		link(node, activeSub)
	end

	-- Start a batch so that the effect can register a cleanup function before
	-- it can receive notifications, ensuring the cleanup always runs
	startBatch()

	local prevSub = setActiveSub(node)
	local success, result = pcall(node.callback)
	activeSub = prevSub

	if success and type(result) == "function" then
		table.insert(node.cleanups, noYield(result))
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
		staleDepCursor = 1,
		cleanups = {},
	}

	-- Clean up the effect scope when the parent subscriber updates or gets
	-- disposed by linking it as a dependency.
	if activeSub and not detached then
		if activeSub.type == EFFECT then
			activeSub.hasChildEffect = true
		end
		link(node, activeSub)
	end

	local prevSub = setActiveSub(node)
	local success, result = pcall(noYield(callback))
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
	local sub: EffectScopeNode = {
		type = EFFECT_SCOPE,
		status = DISPOSED,
		deps = {},
		subs = {},
		staleDepCursor = 1,
		cleanups = {},
	}

	startBatch()

	local prevSub = setActiveSub(sub)
	local success, err = pcall(noYield(callback))
	activeSub = prevSub

	local dep = sub.deps[1]
	while dep do
		unlink(dep, sub, 1)
		propagatePending(dep)
		shallowPropagateDirty(dep)
		dep = sub.deps[1]
	end

	endBatch()

	if not success then
		error(err, 2)
	end
end

local function untracked<Args..., Results...>(callback: (Args...) -> Results..., ...: Args...): Results...
	if activeSub then
		local prevSub = setActiveSub(nil)
		local results: { any } = { pcall(noYield(callback), ...) }
		activeSub = prevSub

		if not results[1] then
			error(results[2], 2)
		end

		return unpack(results, 2)
	else
		return callback(...)
	end
end

local function batch<Args..., Results...>(fn: (Args...) -> Results..., ...: Args...): Results...
	startBatch()
	local results: { any } = { pcall(noYield(fn), ...) }
	endBatch()

	if not results[1] then
		error(results[2], 2)
	end

	return unpack(results, 2)
end

local function onCleanup(callback: () -> ())
	if activeSub and (activeSub.type == EFFECT or activeSub.type == EFFECT_SCOPE) then
		table.insert(activeSub.cleanups, noYield(callback))
	end
end

return table.freeze({
	flags = flags,
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
	batch = batch,
	onCleanup = onCleanup,
})
