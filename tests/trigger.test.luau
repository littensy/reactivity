local reactivity = require("../src")
local test = require("./test")

local computed = reactivity.computed
local effect = reactivity.effect
local signal = reactivity.signal
local trigger = reactivity.trigger

test("should not throw when triggering with no dependencies", function()
	trigger(function() end)
end)

test("should trigger updates for dependent computed signals", function()
	local arr = signal({} :: { number })
	local length = computed(function()
		return #arr()
	end)

	assert(length() == 0, "initial length should be 0")
	table.insert(arr(), 1)
	trigger(arr)
	assert(length() == 1, "length should be 1")
end)

test("should trigger updates for the second source signal", function()
	local src1 = signal({} :: { number })
	local src2 = signal({} :: { number })
	local length = computed(function()
		return #src2()
	end)

	assert(length() == 0, "initial length should be 0")
	table.insert(src2(), 1)
	trigger(function()
		src1()
		src2()
	end)
	assert(length() == 1, "length should be 1")
end)

test("should trigger effect once", function()
	local src1 = signal({} :: { number })
	local src2 = signal({} :: { number })
	local triggers = 0

	effect(function()
		triggers += 1
		src1()
		src2()
	end)
	assert(triggers == 1, "effect should run once initially")

	trigger(function()
		src1()
		src2()
	end)
	assert(triggers == 2, `effect should run once after trigger, but ran {triggers - 1} times`)
end)

test("should not notify the trigger function sub", function()
	local src1 = signal({} :: { number })
	local src2 = computed(function()
		return src1()
	end)

	effect(function()
		src1()
		src2()
	end)
	trigger(function()
		src1()
		src2()
	end)
end)

test("should allow writing a signal after reading it", function()
	local src1 = signal(1)

	trigger(function()
		src1()
		src1(src1() + 1)
	end)
	assert(src1() == 2, "src1 should be 2")
end)

test("should rerun effect once when writing a signal after reading it", function()
	local src1 = signal(1)
	local triggers = 0

	effect(function()
		triggers += 1
		src1()
	end)

	assert(triggers == 1, "effect should run once initially")
	trigger(function()
		src1()
		src1(src1() + 1)
	end)
	assert(triggers == 2, `effect should run once after trigger, but ran {triggers - 1} times`)
	assert(src1() == 2, "src1 should be 2")
end)

return {}
