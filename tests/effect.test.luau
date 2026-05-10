local reactivity = require("../src")
local test = require("./test")

local computed = reactivity.computed
local effect = reactivity.effect
local effectScope = reactivity.effectScope
local endBatch = reactivity.endBatch
local onCleanup = reactivity.onCleanup
local setActiveSub = reactivity.setActiveSub
local signal = reactivity.signal
local startBatch = reactivity.startBatch

test("should clear subscriptions when untracked by all subscribers", function()
	local bRunTimes = 0

	local a = signal(1)
	local b = computed(function()
		bRunTimes += 1
		return a() * 2
	end)
	local stopEffect = effect(function()
		b()
	end)

	assert(bRunTimes == 1, "should run initially")
	a(2)
	assert(bRunTimes == 2, "should run after change")
	stopEffect()
	a(3)
	assert(bRunTimes == 2, "should not run after stop")
end)

test("should not run untracked inner effect", function()
	local a = signal(3)
	local b = computed(function()
		return a() > 0
	end)

	effect(function()
		if b() then
			effect(function()
				if a() == 0 then
					error("bad")
				end
			end)
		end
	end)

	a(2)
	a(1)
	a(0)
end)

test("should run outer effect first", function()
	local a = signal(1)
	local b = signal(1)

	effect(function()
		if a() > 0 then
			effect(function()
				b()
				if a() == 0 then
					error("bad")
				end
			end)
		end
	end)

	startBatch()
	b(0)
	a(0)
	endBatch()
end)

test("should not trigger inner effect when resolve maybe dirty", function()
	local a = signal(0)
	local b = computed(function()
		return a() % 2
	end)

	local innerTriggerTimes = 0

	effect(function()
		effect(function()
			b()
			innerTriggerTimes += 1
			if innerTriggerTimes >= 2 then
				error("bad")
			end
		end)
	end)

	a(2)
end)

test("should notify inner effects in the same order as non-inner effects", function()
	local a = signal(0)
	local b = signal(0)
	local c = computed(function()
		return a() - b()
	end)
	local order1 = {}
	local order2 = {}
	local order3 = {}

	effect(function()
		table.insert(order1, "effect1")
		a()
	end)
	effect(function()
		table.insert(order1, "effect2")
		a()
		b()
	end)

	effect(function()
		c()
		effect(function()
			table.insert(order2, "effect1")
			a()
		end)
		effect(function()
			table.insert(order2, "effect2")
			a()
			b()
		end)
	end)

	effectScope(function()
		effect(function()
			table.insert(order3, "effect1")
			a()
		end)
		effect(function()
			table.insert(order3, "effect2")
			a()
			b()
		end)
	end)

	table.clear(order1)
	table.clear(order2)
	table.clear(order3)

	startBatch()
	b(1)
	a(1)
	endBatch()

	assert(table.concat(order1, ", ") == "effect2, effect1", "wrong order 1")
	assert(table.concat(order2, ", ") == "effect2, effect1", "wrong order 2")
	assert(table.concat(order3, ", ") == "effect2, effect1", "wrong order 3")
end)

test("should custom effect support batch", function()
	local function batchEffect(fn: () -> ())
		return effect(function()
			startBatch()
			local success, result: unknown = pcall(fn)
			endBatch()
			if not success then
				error(result)
			end
		end)
	end

	local logs: { string } = {}
	local a = signal(0)
	local b = signal(0)

	local aa = computed(function()
		table.insert(logs, "aa-0")
		if a() == 0 then
			b(1)
		end
		table.insert(logs, "aa-1")
		return nil
	end)

	local bb = computed(function()
		table.insert(logs, "bb")
		return b()
	end)

	batchEffect(function()
		bb()
	end)
	batchEffect(function()
		aa()
	end)

	assert(table.concat(logs, ", ") == "bb, aa-0, aa-1, bb", `wrong order: {table.concat(logs, ", ")}`)
end)

test("should duplicate subscribers do not affect the notify order", function()
	local src1 = signal(0)
	local src2 = signal(0)
	local order: { string } = {}

	effect(function()
		table.insert(order, "a")
		local currentSub = setActiveSub(nil)
		local isOne = src2() == 1
		setActiveSub(currentSub)
		if isOne then
			src1()
		end
		src2()
		src1()
	end)
	effect(function()
		table.insert(order, "b")
		src1()
	end)
	src2(1) -- src1.subs: a -> b -> a

	table.clear(order)
	src1(src1() + 1)

	assert(table.concat(order) == "ab", `wrong order: {table.concat(order)}`)
end)

test("should handle side effect with inner effects", function()
	local a = signal(0)
	local b = signal(0)
	local order: { string } = {}

	effect(function()
		effect(function()
			a()
			table.insert(order, "a")
		end)
		effect(function()
			b()
			table.insert(order, "b")
		end)
		assert(table.concat(order) == "ab", `wrong order: {table.concat(order)}`)

		table.clear(order)
		b(1)
		a(1)
	end)

	-- DEVIATION: This assertion was moved outside of the effect so that it
	-- runs after the batch updates have been processed
	assert(table.concat(order) == "ba", `wrong order: {table.concat(order)}`)
end)

test("should handle flags are indirectly updated during checkDirty", function()
	local a = signal(false)
	local b = computed(function()
		return a()
	end)
	local c = computed(function()
		b()
		return 0
	end)
	local d = computed(function()
		c()
		return b()
	end)

	local triggers = 0

	effect(function()
		d()
		triggers += 1
	end)
	assert(triggers == 1, "did not run initially")
	a(true)
	assert(triggers == 2, "did not update indirectly")
end)

test("should execute all effects even if some fail", function()
	local a = signal(0)
	local b = signal(0)

	local effect3Executed = false

	effect(function()
		a()
	end)
	effect(function()
		if a() == 2 then
			error("Error in effect 2")
		end
	end)
	effect(function()
		a()
		effect3Executed = true
	end)
	effect(function()
		b()
	end)

	a(1)

	effect3Executed = false
	local success, err = pcall(a, 2)
	assert(not success and string.find(tostring(err), "Error in effect 2"), `bad error: {err}`)
	assert(effect3Executed, "effect 3 should have executed")
end)

test("should allow finite recursion", function()
	local src = signal(0)
	local order = {}

	effect(function()
		table.insert(order, src())
		src(math.min(src() + 1, 5))
	end)
	assert(table.concat(order) == "012345", `bad order: {table.concat(order)}`)
end)

test("should run cleanups in sequence", function()
	local order = {}

	local a = signal(false)
	local stopEffect = effect(function()
		a()
		onCleanup(function()
			table.insert(order, "a")
		end)
		onCleanup(function()
			table.insert(order, "b")
		end)
	end)

	assert(table.concat(order) == "", "should not cleanup immediately")

	a(true)
	assert(table.concat(order) == "ab", `bad update: {table.concat(order)}`)

	table.clear(order)
	stopEffect()
	assert(table.concat(order) == "ab", `bad dispose: {table.concat(order)}`)

	table.clear(order)
	a(false)
	assert(table.concat(order) == "", "should not cleanup after update")
end)

test("should run inner effect cleanup", function()
	local cleanups = 0

	local src = signal(false)
	local stopEffect = effect(function()
		src()
		effect(function()
			onCleanup(function()
				cleanups += 1
			end)
		end)
		effect(function()
			onCleanup(function()
				cleanups += 1
			end)
		end)
	end)

	assert(cleanups == 0, "should not cleanup immediately")
	src(true)
	assert(cleanups == 2, "should cleanup after outer update")
	stopEffect()
	assert(cleanups == 4, "should cleanup after outer dispose")
	src(false)
	assert(cleanups == 4, "should not cleanup after update")
end)

test("should run cleanups outside any evaluation context", function()
	local calls = 0
	local a = signal(0)
	local b = signal(0)
	local c = computed(function()
		if a() == 0 then
			effect(function()
				onCleanup(function()
					b()
				end)
			end)
		end
		return a()
	end)

	effect(function()
		calls += 1
		c()
	end)
	assert(calls == 1, "should run once")

	calls = 0
	a(1)
	assert(calls == 1, "should run once after a update")

	calls = 0
	b(1)
	assert(calls == 0, "should not run after b update")
end)

test("should not retrigger the effect if the cleanup modifies one of the dependencies", function()
	local a = signal(0)
	local calls = {}

	effect(function()
		table.insert(calls, a())
		return function()
			a(2)
		end
	end)
	assert(#calls == 1, "should have run once initially")
	table.clear(calls)

	a(1)
	assert(#calls == 1, "should run once after an update")
	assert(calls[1] == 2, `bad call: {calls[1]}`)
end)

test("should run the cleanup if the effect disposes itself", function()
	local a = signal(0)
	local calls = 0
	local dispose: () -> ()

	dispose = effect(function()
		if a() > 0 then
			dispose()
			return function()
				calls += 1
			end
		end
	end)
	assert(calls == 0, "should not have called cleanup initially")
	a(1)
	assert(calls == 1, "should have called cleanup once after first update")
	a(2)
	assert(calls == 1, "should not call cleanup again after second update")
end)

test("should not run the effect if the cleanup function disposes it", function()
	local a = signal(0)
	local calls = 0
	local dispose: () -> ()

	dispose = effect(function()
		a()
		calls += 1
		return function()
			dispose()
		end
	end)
	assert(calls == 1, "should have run effect once initially")
	a(1)
	assert(calls == 1, "should not run effect again after update")
end)

test("should reset the cleanup if the effect throws", function()
	local a = signal(0)
	local calls = 0

	effect(function()
		if a() == 0 then
			return function()
				calls += 1
			end
		else
			error("hello")
		end
	end)
	assert(calls == 0, "should not have called cleanup initially")

	local success, err = pcall(a, 1)
	assert(not success and string.find(tostring(err), "hello"), `bad error: {err}`)
	assert(calls == 1, "should have called cleanup once after throwing")

	a(0)
	assert(calls == 1, "should not call cleanup again after resetting value")
end)

test("recursive effects should run and cleanup in order", function()
	local a = signal(0)
	local order: { string } = {}

	effect(function()
		local value = a()
		table.insert(order, `+{value}`)
		if value < 3 then
			a(value + 1)
		end
		return function()
			table.insert(order, `-{value}`)
		end
	end)
	assert(table.concat(order) == "+0-0+1-1+2-2+3", `bad order: {table.concat(order)}`)

	table.clear(order)
	a(100)
	assert(table.concat(order) == "-3+100", `bad order: {table.concat(order)}`)
end)

test("inner effects should dispose in order", function()
	local src = signal(false)
	local order: { number } = {}

	local dispose = effect(function()
		src()
		for i = 1, 3 do
			effect(function()
				return function()
					table.insert(order, i)
				end
			end)
		end
	end)

	src(true)
	assert(table.concat(order) == "123", `bad rerun order: {table.concat(order)}`)
	dispose()
	assert(table.concat(order) == "123123", `bad dispose order: {table.concat(order)}`)
	src(false)
	assert(table.concat(order) == "123123", "should not cleanup after dispose")
end)

test("inner effects should dispose before outer effect re-runs", function()
	local src = signal(false)
	local dispose: () -> ()
	local order: { number } = {}

	dispose = effect(function()
		src()
		table.insert(order, 0)
		for i = 1, 4 do
			effect(function()
				return function()
					table.insert(order, i)
					dispose()
				end
			end)
		end
	end)

	src(true)
	assert(table.concat(order) == "01234", `bad rerun order: {table.concat(order)}`)
	src(false)
	assert(table.concat(order) == "01234", "should not cleanup after dispose")
end)

-- https://github.com/stackblitz/alien-signals/pull/110
test.describe("issue #109", function()
	test("should handle inner effect clearing outer computed's deps on rerun", function()
		local src = signal(false)
		local stopEffect: () -> ()
		local c = computed(function()
			effect(function()
				return function()
					stopEffect()
				end
			end)
			effect(function() end)
			return src()
		end)
		stopEffect = effect(function()
			c()
		end)
		src(true)
	end)

	test("#109", function()
		local s = signal(false)
		local dispose: () -> ()
		local a = computed(function()
			if s() then
				dispose()
			end
			return 0
		end)
		local b = computed(function()
			return a()
		end)
		dispose = effect(function()
			b()
		end)
		s(true)
	end)

	-- (1) Side-effect after the dispose-trigger inside the effect body
	--     should still run, just like in any other JS function — disposing
	--     swaps the .fn property but the closure already on the stack must
	--     finish executing.
	test("self-dispose inside inner effect: code after dispose() still runs", function()
		local s = signal(0)
		local dispose: () -> ()
		local stages: { string } = {}

		dispose = effect(function()
			table.insert(stages, "start")
			s()
			if s() == 1 then
				dispose()
				table.insert(stages, "after-dispose")
			end
			table.insert(stages, "end")
		end)

		local function assertStages(expected: string)
			local text = table.concat(stages, ", ")
			assert(text == expected, `expected "{expected}", got "{text}"`)
		end

		assertStages("start, end")
		s(1)
		assertStages("start, end, start, after-dispose, end")
	end)

	-- (2) When dispose is triggered from *another* node's update (the
	--     #109 path), the swapped fn replaces the user fn before run() calls
	--     it — so the side-effect inside the user fn is never invoked.
	--     This documents that this run is *skipped*, not just suppressed.
	test("disposed-by-other-node effect: scheduled run is skipped entirely", function()
		local s = signal(0)
		local dispose: () -> ()
		local bodyRuns = 0

		local a = computed(function()
			if s() == 1 then
				dispose()
			end
			return s()
		end)

		dispose = effect(function()
			a()
			bodyRuns += 1
		end)
		effect(function()
			a()
		end)

		assert(bodyRuns == 1, `should have run once initially, got {bodyRuns}`)
		s(1)
		-- The flush queued e1 because of the propagation; if e1 had been
		-- disposed *between flushes*, this run wouldn't have been scheduled.
		-- The fix-by-fn-swap turns the scheduled run into a no-op.
		assert(bodyRuns == 1, `should not have run after dispose, got {bodyRuns}`)
	end)

	-- (3) After being disposed mid-flush, the effect's graph state should
	--     be clean: no deps, not Watching, no subs — otherwise future
	--     propagations would still walk through it.
	test("disposed effect: graph state is fully cleaned up", function()
		local s = signal(0)
		local dispose: () -> ()
		local e1Node: reactivity.EffectNode?

		local a = computed(function()
			if s() == 1 then
				dispose()
			end
			return s()
		end)

		dispose = effect(function()
			e1Node = e1Node or reactivity.getActiveSub() :: reactivity.EffectNode?
			a()
		end)
		effect(function()
			a()
		end)

		s(1)

		assert(e1Node ~= nil, "e1Node should have been set")
		assert(next(e1Node.deps) == nil, "e1Node should have no deps")
		assert(e1Node.status == 3 :: any, `e1Node should be disposed, got {e1Node.status}`)
	end)

	test("disposed effect should not be re-notified on later updates", function()
		local s = signal(0)
		local dispose1: () -> ()
		local e1runs = 0

		local a = computed(function()
			if s() == 1 then
				dispose1()
			end
			return s()
		end)

		dispose1 = effect(function()
			a()
			e1runs += 1
		end)
		effect(function()
			a()
		end)

		assert(e1runs == 1, `should have run once initially, got {e1runs}`)
		s(1)
		assert(e1runs == 1, `should not have run after dispose, got {e1runs}`)

		-- If e1 was actually disposed, further signal changes shouldn't
		-- involve it at all. With the fn-swap fix, e1 stays subscribed to `a`
		-- and gets notified each time, even though fn is now a no-op.
		s(2)
		s(3)
		s(4)
		assert(e1runs == 1, `should not have run after further updates, got {e1runs}`)
	end)

	-- Same shape as test #109, but the dispose target is an effectScope
	-- instead of an effect. effectScopeOper has no defer branch in the current
	-- fix, so unwatched() cascades immediately and the original crash should
	-- still reproduce.
	test("scope-variant: dispose effectScope during computed update", function()
		local s = signal(false)
		local disposeScope: () -> ()

		local a = computed(function()
			if s() then
				disposeScope()
			end
			return 0
		end)
		local b = computed(function()
			return a()
		end)

		disposeScope = effectScope(function()
			effect(function()
				b()
			end)
		end)

		s(true)
	end)

	test("shallowPropagate walks stale link after sibling disposal", function()
		local s = signal(0)
		local dispose1: () -> ()
		local e2Value = -1

		local a = computed(function()
			if s() == 1 then
				dispose1()
			end
			return s()
		end)

		dispose1 = effect(function()
			a()
		end)
		effect(function()
			e2Value = a()
		end)

		assert(e2Value == 0, `should have initial value 0, got {e2Value}`)
		s(1)
		assert(e2Value == 1, `should have updated to 1, got {e2Value}`)
	end)

	test("shallowPropagate walks stale link with 3 subscribers", function()
		local s = signal(0)
		local dispose1: () -> ()
		local e2Value = -1
		local e3Value = -1

		local a = computed(function()
			if s() == 1 then
				dispose1()
			end
			return s()
		end)

		dispose1 = effect(function()
			a()
		end)
		effect(function()
			e2Value = a()
		end)
		effect(function()
			e3Value = a()
		end)

		assert(e2Value == 0, `e2Value should have initial value 0, got {e2Value}`)
		assert(e3Value == 0, `e3Value should have initial value 0, got {e3Value}`)
		s(1)
		assert(e2Value == 1, `e2Value should have updated to 1, got {e2Value}`)
		assert(e3Value == 1, `e3Value should have updated to 1, got {e3Value}`)
	end)

	test("stale-link: disposed effect still runs", function()
		local s = signal(0)
		local dispose1: () -> ()
		local e1runs = 0

		local a = computed(function()
			if s() == 1 then
				dispose1()
			end
			return s()
		end)

		dispose1 = effect(function()
			a()
			e1runs += 1
		end)
		effect(function()
			a() -- second subscriber keeps 'a' alive
		end)

		assert(e1runs == 1, `should have run once initially, got {e1runs}`)
		s(1)
		assert(e1runs == 1, `should not have run after dispose, got {e1runs}`)
	end)

	test("variant - dep.subs undefined at line 190", function()
		local s = signal(0)
		local dispose: () -> ()
		local a = computed(function()
			s()
			return 0
		end)
		local a2 = computed(function()
			if s() == 1 then
				dispose()
			end
			return s()
		end)
		local b = computed(function()
			a()
			a2()
			return 0
		end)
		dispose = effect(function()
			b()
		end)
		s(1)
	end)
end)

return {}
