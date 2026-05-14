local reactivity = require("../src")
local test = require("./test")

local signal = reactivity.signal
local computed = reactivity.computed
local effect = reactivity.effect
local effectScope = reactivity.effectScope
local trigger = reactivity.trigger
local onCleanup = reactivity.onCleanup

test("should not trigger after stop", function()
	local count = signal(1)

	local triggers = 0

	local stopScope = effectScope(function()
		effect(function()
			triggers += 1
			count()
		end)
		assert(triggers == 1, "did not run initially")

		count(2)
		assert(triggers == 2, "did not run after update")
	end)

	count(3)
	assert(triggers == 3, "did not run after update outside scope")
	stopScope()
	count(4)
	assert(triggers == 3, "ran after stop")
end)

test("should run indirect scope cleanup", function()
	local cleanups = 0
	local cleanup: () -> ()

	local stopScope = effectScope(function()
		cleanup = effectScope(function()
			onCleanup(function()
				cleanups += 1
			end)
		end)
	end)

	assert(cleanups == 0, "should not cleanup immediately")
	stopScope()
	assert(cleanups == 1, "should cleanup from parent dispose")
	cleanup()
	assert(cleanups == 1, "should skip redundant dispose")
end)

test("should run indirect effect cleanup", function()
	local cleanups = 0
	local a = signal(false)

	local stopScope = effectScope(function()
		effect(function()
			a()
			onCleanup(function()
				cleanups += 1
			end)
		end)
	end)

	assert(cleanups == 0, "should not cleanup immediately")
	a(true)
	assert(cleanups == 1, "should cleanup once")
	stopScope()
	assert(cleanups == 2, "should cleanup after outer dispose")
	a(false)
	assert(cleanups == 2, "should not cleanup after update")
end)

test("should dispose inner effects if created in an effect", function()
	local src = signal(1)
	local triggers = 0
	local dispose: () -> ()

	effect(function()
		dispose = effectScope(function()
			effect(function()
				src()
				triggers += 1
			end)
		end)
		assert(triggers == 1, "did not run initially")
	end)

	src(2)
	assert(triggers == 2, "did not run after update")
	dispose()
	src(3)
	assert(triggers == 2, "ran after dispose")
end)

test("signal and computed should link to the same node in effectScope", function()
	local a = signal(0)
	local b = computed(function()
		return 0
	end)
	local triggers = 0

	effect(function()
		triggers += 1
		effectScope(function()
			effectScope(function()
				a()
				b()
			end)
		end)
	end)

	assert(triggers == 1, "did not run initially")
	a(a() + 1)
	assert(triggers == 2, "did not run after signal update")
	trigger(b)
	assert(triggers == 3, "did not run after computed update")
end)

test("effect should respond to both signal and computed changes through scope", function()
	local s = signal(0)
	local c = computed(function()
		return s() * 2
	end)
	local triggers = 0

	effect(function()
		triggers += 1
		effectScope(function()
			s()
			c()
		end)
	end)

	assert(triggers == 1, "did not run initially")

	s(1)
	assert(triggers == 2, "did not run after signal update")

	trigger(c)
	assert(triggers == 3, "did not run after computed update")
end)

test("scope should respond to consecutive signal updates", function()
	local s = signal(0)
	local triggers = 0

	effect(function()
		triggers += 1
		effectScope(function()
			s()
		end)
	end)

	assert(triggers == 1, "did not run initially")
	s(1)
	assert(triggers == 2, "did not run after first update")
	s(2)
	assert(triggers == 3, "did not run after second update")
end)

test("computed in standalone scope should cache and clean up", function()
	local s = signal(0)
	local computeCount = 0

	local dispose = effectScope(function()
		local c = computed(function()
			computeCount += 1
			return s()
		end)
		assert(c() == 0, "computed value should be 0 (1)")
		assert(c() == 0, "computed value should be 0 (2)")
	end)

	-- Computed should cache (only 1 evaluation, not 2)
	assert(computeCount == 1, "computed value should be cached")

	dispose()
end)

return {}
