local reactivity = require("../src")
local test = require("../tests/test")

reactivity.flags.strict = true

test.describe("computed", function()
	require("../tests/computed.test")
end)

test.describe("effect", function()
	require("../tests/effect.test")
end)

test.describe("effectScope", function()
	require("../tests/effectScope.test")
end)

test.describe("errors", function()
	require("../tests/errors.test")
end)

test.describe("gc", function()
	require("../tests/gc.test")
end)

test.describe("topology", function()
	require("../tests/topology.test")
end)

test.describe("trigger", function()
	require("../tests/trigger.test")
end)

test.describe("untracked", function()
	require("../tests/untracked.test")
end)

test.finish()
