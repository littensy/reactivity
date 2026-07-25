--!optimize 1

local reactivity = require("../src")
local test = require("./test")

test("should disallow yielding", function()
	local success = pcall(function()
		reactivity.effectScope(function()
			coroutine.yield()
		end)
	end)
	assert(not success, "should error on yield")
end)

test.describe("traceback", function()
	local DEFAULT_ERROR = "Test error"

	local function fail(message: string?)
		error(message or DEFAULT_ERROR, 0)
	end

	local function indirectError(message: string?)
		fail(message)
	end

	local function assertStackTrace(err: unknown, expectation: string?)
		expectation = expectation or DEFAULT_ERROR
		assert(type(err) == "string", "Expected error to be a string")
		assert(string.find(err, expectation), `Expected error message to contain "{expectation}", got: {err}`)
		assert(string.find(err, "fail"), `Expected stack trace to contain fail, got: {err}`)
		assert(string.find(err, "indirectError"), `Expected stack trace to contain indirectError, got: {err}`)
	end

	test("signal update", function()
		local _, err = pcall(function()
			local src = reactivity.signal(0)
			src(function()
				indirectError()
				return 1
			end)
		end)
		assertStackTrace(err)
	end)

	test("computed", function()
		local _, err = pcall(function()
			local src = reactivity.computed(function()
				indirectError()
			end)
			src()
		end)
		assertStackTrace(err)
	end)

	test("effect init", function()
		local _, err = pcall(function()
			reactivity.effect(function()
				indirectError()
			end)
		end)
		assertStackTrace(err)
	end)

	test("effect run", function()
		local _, err = pcall(function()
			local src = reactivity.signal(0)
			reactivity.effect(function()
				if src() == 1 then
					indirectError()
				end
			end)
			src(1)
		end)
		assertStackTrace(err)
	end)

	test("effect cleanup", function()
		local _, err = pcall(function()
			local dispose = reactivity.effect(function()
				return function()
					indirectError()
				end
			end)
			dispose()
		end)
		assertStackTrace(err)
	end)

	test("effect cleanup (onCleanup)", function()
		local _, err = pcall(function()
			local dispose = reactivity.effect(function()
				reactivity.onCleanup(function()
					indirectError("onCleanup1")
				end)
				reactivity.onCleanup(function()
					indirectError("onCleanup2")
				end)
			end)
			dispose()
		end)
		assertStackTrace(err, "onCleanup1")
		assertStackTrace(err, "onCleanup2")
	end)

	test("trigger", function()
		local _, err = pcall(function()
			reactivity.trigger(function()
				indirectError()
			end)
		end)
		assertStackTrace(err)
	end)

	test("untracked", function()
		local _, err = pcall(function()
			reactivity.effectScope(function()
				reactivity.untracked(function()
					indirectError()
				end)
			end)
		end)
		assertStackTrace(err)
	end)

	test("batch", function()
		local _, err = pcall(function()
			reactivity.batch(function()
				indirectError()
			end)
		end)
		assertStackTrace(err)
	end)
end)

return {}
