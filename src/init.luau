type ReactiveStatus = "clean" | "pending" | "dirty"

type Subscriber = {
	read deps: { DependencyNode },
	staleDepsStart: number?,
	staleDepsEnd: number?,
}

type Dependency = {
	read subs: { SubscriberNode },
}

type SignalNode<T = any> = Dependency & {
	read type: "signal",
	status: "clean" | "dirty",
	currentValue: T,
	pendingValue: T,
}

type EffectNode = Subscriber & Dependency & {
	read type: "effect",
	status: ReactiveStatus | "dead",
	read callback: () -> ...(() -> ())?,
	read cleanups: { () -> () },
	queued: boolean,
}

type ComputedNode<T = any> = Subscriber & Dependency & {
	read type: "computed",
	status: ReactiveStatus,
	read getter: (T?) -> T,
	value: T,
}

type EffectScopeNode = Subscriber & Dependency & {
	read type: "effectScope",
	status: ReactiveStatus,
	read cleanups: { () -> () },
}

type SubscriberNode = ComputedNode | EffectNode | EffectScopeNode
type DependencyNode = SignalNode | ComputedNode | EffectNode | EffectScopeNode
type ReactiveNode = SignalNode | ComputedNode | EffectNode | EffectScopeNode

type Update<T> = ((current: T) -> T) | T
type Setter<T> = (update: Update<T>) -> T

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

	-- Collect this effect and its parent effects to ensure dirty/pending
	-- parent effects are checked
	repeat
		effect.queued = true
		queueLength += 1
		queue[queueLength] = effect

		-- Traverse to the next parent if it exists and is not queued
		local sub = effect.subs[1]
		if sub and sub.type == "effect" and not sub.queued then
			effect = sub
		else
			break
		end
	until false

	local endIndex = queueLength

	-- Reverse the order of the newly queued effects to ensure outer effects
	-- are checked before inner effects
	while startIndex < endIndex do
		queue[startIndex], queue[endIndex] = queue[endIndex], queue[startIndex]
		startIndex += 1
		endIndex -= 1
	end
end

-- Mark immediate subscribers as dirty without checking their dependencies,
-- used when a node is updated to cause all of its subscribers to be updated.
local function shallowPropagateDirty(dep: DependencyNode)
	for _, sub in dep.subs do
		if sub.status == "pending" then
			sub.status = "dirty"
			if sub.type == "effect" then
				queueEffect(sub)
			end
		end
	end
end

-- Mark nested subscribers as pending and queue effect runs, used when a node
-- receives an update but has not been accessed yet, so it may not be known if
-- the update will cause a change.
local function propagatePending(dep: DependencyNode)
	for _, sub in dep.subs do
		if sub.status == "clean" then
			sub.status = "pending"
			if sub.type == "computed" then
				propagatePending(sub)
			elseif sub.type == "effect" then
				queueEffect(sub)
			end
		end
	end
end

-- Link a dependency to a subscriber, so that the subscriber will be notified
-- when the dependency updates. If the subscriber has stale dependencies from a
-- previous run, it will attempt to reuse the existing dependency.
local function link(dep: DependencyNode, sub: SubscriberNode)
	local depSubs = dep.subs
	local subDeps = sub.deps
	local staleDepsStart = sub.staleDepsStart

	-- If the next stale dependency is the same as the new dependency, we can
	-- skip linking and mark it as used.
	if staleDepsStart and subDeps[staleDepsStart] == dep then
		sub.staleDepsStart = staleDepsStart + 1
		return
	end

	table.insert(subDeps, dep)
	table.insert(depSubs, sub)
end

-- Unlink a dependency from a subscriber. If the dependency has no more
-- subscribers after unlinking, it will be unwatched to release its resources.
local function unlink(dep: DependencyNode, sub: SubscriberNode, depIndex: number?)
	local depSubs = dep.subs
	local subDeps = sub.deps

	depIndex = depIndex or table.find(subDeps, dep)
	if depIndex then
		table.remove(subDeps, depIndex)
	end

	local subIndex = table.find(depSubs, sub)
	if subIndex then
		table.remove(depSubs, subIndex)

		-- Release nodes when they have no subscribers
		if not depSubs[1] then
			unwatched(dep)
		end
	end
end

-- Remove any stale dependencies that were not reused during the current run.
-- A dependency is stale if the link function did not mark it as used and
-- increment staleDepsStart to skip it.
local function purgeStaleDeps(sub: SubscriberNode)
	local staleDepsStart = sub.staleDepsStart
	if staleDepsStart then
		local subDeps = sub.deps
		for index = sub.staleDepsEnd :: number, staleDepsStart, -1 do
			unlink(subDeps[index], sub, index)
		end
		sub.staleDepsStart = nil
		sub.staleDepsEnd = nil
	end
end

-- Clear all dependencies from a subscriber. Dependencies are removed in
-- reverse order to avoid shifting indices when unlinking.
local function clearDeps(sub: SubscriberNode)
	local subDeps = sub.deps
	if sub.staleDepsStart then
		sub.staleDepsStart = nil
		sub.staleDepsEnd = nil
	end
	for index = #subDeps, 1, -1 do
		unlink(subDeps[index], sub)
	end
end

local function runCleanups(cleanups: { () -> () })
	local errors: { string } = {}
	local hasErrors = false

	for index, cleanup in cleanups do
		cleanups[index] = nil

		local prevSub = setActiveSub(nil)
		local success, err = pcall(cleanup)
		activeSub = prevSub

		if not success then
			hasErrors = true
			table.insert(errors, tostring(err))
		end
	end

	if hasErrors then
		error(`Errors occurred during effect cleanup:\n\n{table.concat(errors, "\n\n")}`, 0)
	end
end

local function stopEffect(effect: EffectNode)
	effect.status = "dead"
	effect.queued = false

	-- Unlink from dependencies and child effects
	clearDeps(effect)

	-- Unlink from the parent effect, if it exists
	local sub = effect.subs[1]
	if sub then
		unlink(effect, sub)
	end

	runCleanups(effect.cleanups)
end

local function stopEffectScope(effectScope: EffectScopeNode)
	-- Unlink from dependencies and child effects
	clearDeps(effectScope)

	-- Unlink from the parent effect, if it exists
	local sub = effectScope.subs[1]
	if sub then
		unlink(effectScope, sub)
	end

	runCleanups(effectScope.cleanups)
end

function unwatched(node: ReactiveNode)
	if node.type == "effect" then
		-- If an effect loses its only subscriber, its parent effect re-ran or
		-- got disposed, so this child effect should be disposed as well
		stopEffect(node)
	elseif node.type == "effectScope" then
		stopEffectScope(node)
	elseif node.type == "computed" then
		-- Clearing a computed node's dependencies when it loses all of its
		-- subscribers allows it to be garbage collected without weak tables
		node.status = "dirty"
		clearDeps(node)
	end
end

local function updateComputed(computed: ComputedNode): boolean
	local oldValue = computed.value

	computed.status = "clean"
	computed.staleDepsStart = 1
	computed.staleDepsEnd = #computed.deps

	local prevSub = setActiveSub(computed)
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
	signal.status = "clean"
	if signal.currentValue ~= pendingValue then
		signal.currentValue = pendingValue
		return true
	else
		return false
	end
end

local function updateNode(node: SignalNode | ComputedNode): boolean
	if node.type == "computed" then
		return updateComputed(node)
	else
		return updateSignal(node)
	end
end

local function checkDirty(sub: SubscriberNode): boolean
	for _, dep in sub.deps do
		if sub.status == "dirty" then
			-- The original node may have been indirectly marked dirty while
			-- checking a dependency, so this is checked every iteration
			return true
		elseif dep.type == "computed" or dep.type == "signal" then
			if dep.status == "dirty" then
				-- Mark immediate subscribers as dirty if an update occurs
				if updateNode(dep) then
					shallowPropagateDirty(dep)
					return true
				end
			elseif dep.status == "pending" then
				-- Check if the node has any dirty or pending dependencies that
				-- would require it to be dirty as well
				if checkDirty(dep) then
					-- Mark immediate subscribers as dirty if an update occurs
					if updateNode(dep) then
						shallowPropagateDirty(dep)
						return true
					end
				else
					-- The update resulted in no change and can be skipped
					dep.status = "clean"
				end
			end
		end
	end
	return false
end

local function run(effect: EffectNode)
	if effect.status == "dirty" or (effect.status == "pending" and checkDirty(effect)) then
		runCleanups(effect.cleanups)

		-- Cancel the run if the effect was stopped during cleanup
		if effect.status == "dead" then
			return
		end

		effect.status = "clean"
		effect.queued = false
		effect.staleDepsStart = 1
		effect.staleDepsEnd = #effect.deps

		local prevSub = setActiveSub(effect)
		local success, cleanup = pcall(effect.callback)
		activeSub = prevSub

		-- Remove any dependencies that were not accessed during this run.
		-- Child effects will lose their only subscriber and get unwatched.
		purgeStaleDeps(effect)

		if success and type(cleanup) == "function" then
			table.insert(effect.cleanups, cleanup)
		elseif not success then
			error(cleanup, 0)
		end

		-- If the effect was disposed during its execution, run any cleanups
		-- that might have been missed
		if effect.status == "dead" then
			runCleanups(effect.cleanups)
		end
	else
		effect.status = "clean"
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
	local didError = false

	while notifyIndex < queueLength do
		local success, err = pcall(function()
			while notifyIndex < queueLength do
				notifyIndex += 1
				local effect = queue[notifyIndex]
				queue[notifyIndex] = nil
				run(effect)
			end
		end)

		if not success then
			didError = true
			table.insert(errors, tostring(err))
		end
	end

	notifyIndex = 0
	queueLength = 0

	if didError then
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

local function signal<T>(initialValue: T): (...Update<T>) -> T
	local node: SignalNode = {
		type = "signal",
		status = "clean",
		subs = {},
		currentValue = initialValue,
		pendingValue = initialValue,
	}

	return function(...: Update<T>): T
		if select("#", ...) > 0 then
			local update = ...
			local value: T

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

			if node.pendingValue ~= value then
				node.pendingValue = value
				node.status = "dirty"
				propagatePending(node)
				flush()
				return value
			end
		else
			-- Check if the signal has a pending update that needs to be applied
			if node.status == "dirty" then
				if updateSignal(node) then
					shallowPropagateDirty(node)
				end
			end

			-- Register this signal as a dependency of the first subscriber in the
			-- chain that is a computed signal or an effect, skipping effect scopes
			local sub = activeSub
			while sub do
				if sub.type == "effect" or sub.type == "computed" then
					link(node, sub)
					break
				end
				sub = sub.subs[1]
			end
		end

		return node.currentValue
	end
end

local function computed<T>(getter: (previousValue: T?) -> T): () -> T
	local node: ComputedNode = {
		type = "computed",
		status = "clean",
		deps = {},
		subs = {},
		getter = getter,
	}
	local initialized = false

	return function(): T
		if node.status == "dirty" then
			if updateComputed(node) then
				shallowPropagateDirty(node)
			end
		elseif node.status == "pending" then
			if checkDirty(node) then
				if updateComputed(node) then
					shallowPropagateDirty(node)
				end
			else
				node.status = "clean"
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
		type = "effect",
		status = "clean",
		deps = {},
		subs = {},
		queued = false,
		callback = callback,
		cleanups = {},
	}

	-- Inner effects are tracked as a dependency of the parent effect, so that
	-- they can be automatically cleaned up when the parent effect re-runs, and
	-- to ensure the correct execution order when nested effects are notified
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
		type = "effectScope",
		status = "clean",
		deps = {},
		subs = {},
		cleanups = {},
	}

	-- Inner effect scopes are tracked as a dependency of the parent effect
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
		stopEffectScope(node)
	end
end

local function trigger(callback: () -> ...any)
	local sub: EffectNode = {
		type = "effect",
		status = "dead",
		deps = {},
		subs = {},
		queued = false,
		callback = function() end,
		cleanups = {},
	}

	local prevSub = setActiveSub(sub)
	local success, err = pcall(callback)
	activeSub = prevSub

	local dep = sub.deps[1]
	while dep do
		unlink(dep, sub)
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
	if activeSub and (activeSub.type == "effect" or activeSub.type == "effectScope") then
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
