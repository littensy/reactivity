local function bench(name: string, test: () -> ())
	local start = os.clock()
	local iter = 0

	while os.clock() - start < 2 do
		iter += 1
		test()
	end

	print(`{name}: \t{string.format("%.2f", (os.clock() - start) / iter * 1000 * 1000)} us/iter`)
end

return bench
