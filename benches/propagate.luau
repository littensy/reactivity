local bench = require("./bench")
local reactivity = require("../src")

local computed = reactivity.computed
local effect = reactivity.effect
local signal = reactivity.signal

for _, w in { 10, 100 } do
	for _, h in { 10, 100 } do
		bench(`propagate {w}*{h}`, function()
			local src = signal(1)
			for _ = 1, w do
				local last = src
				for _ = 1, h do
					local prev = last
					last = computed(function()
						return prev() + 1
					end)
				end
				effect(function()
					last()
				end)
			end
			src(src() + 1)
		end)
	end
end
