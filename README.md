# reactivity

I wanted to learn more about [`alien-signals`](https://github.com/stackblitz/alien-signals), so here's a fun little test implementation of it with some changes and deoptimizations:

- Store subscribers and dependencies in-place in arrays instead of a linked list
- Add cleanup functions to effects and effect scopes
- Nested effects are cleaned up before the next evaluation instead of while purging stale dependencies
- Signals accept an update function to avoid tracking dependencies during updates that access the old value
- Error handling in `flush()` runs all queued effects instead of stopping at the first error
- Remove recursion checks for effect callbacks and computed signal getters

The [`alien-signals` Deep Dive](https://gist.github.com/johnsoncodehk/59e79a0cfa5bb3421b5d166a08e42f30) by Johnson Chu has been a big help!

## Todo

- Ensure old inner effects get disposed before new inner effects

## Testing

Install Rokit: https://github.com/rojo-rbx/rokit

```sh
rokit install
lute run test
lute run benches/propagate # 68us (reactivity) vs 80us (Charm) for 10*10
```

## Recursive computed signals

In `alien-signals`, there are recursion checks in place to handle computed getter functions that recursively update the signals that they depend on. However, these checks have overhead in this implementation, and I think computed side effects should generally be avoided.

`reactivity` will omit these recursion checks to reduce code complexity, but a working implementation is recorded here:

### `isValidLink`

```luau
-- Returns true if the link exists and is not stale
local function isValidLink(dep: DependencyNode, sub: SubscriberNode): boolean
	local index = table.find(sub.deps, dep)
	if sub.staleDepsStart then
		return index ~= nil and (index < sub.staleDepsStart or index > sub.staleDepsEnd :: number)
	else
		return index ~= nil
	end
end
```

### `propagatePending`

```luau
local function propagatePending(dep: DependencyNode)
	for _, sub in dep.subs do
		if sub.type == COMPUTED then
			if sub.status == CLEAN and not (sub.recursedCheck or sub.recursed) then
				sub.status = PENDING
			elseif not (sub.recursedCheck or sub.recursed) then
				continue
			elseif not sub.recursedCheck then
				sub.status = PENDING
				sub.recursed = nil
			elseif sub.status == CLEAN and isValidLink(dep, sub) then
				sub.status = PENDING
				sub.recursed = true
			else
				continue
			end
			propagatePending(sub)
		elseif sub.type == EFFECT and sub.status == CLEAN then
			sub.status = PENDING
			queueEffect(sub)
		end
	end
end
```
