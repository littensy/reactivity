# reactivity

I wanted to learn more about [`alien-signals`](https://github.com/stackblitz/alien-signals), so here's a fun little test implementation of it with some changes and deoptimizations:

- Store subscribers and dependencies in-place in arrays instead of a linked list
- Nested effects are cleaned up in order of first-in, first-out, instead of LIFO reverse order
- Signals accept an update function, preventing accidental dependency tracking when performing updates that depend on the old value
- Error handling in `flush()` runs all queued effects instead of stopping at the first error
- Remove recursion checks for effects and computed signals
- Nested effects in computed signals is treated as undefined behavior

The [`alien-signals` Deep Dive](https://gist.github.com/johnsoncodehk/59e79a0cfa5bb3421b5d166a08e42f30) by Johnson Chu has been a big help!

## Testing

Install Rokit: https://github.com/rojo-rbx/rokit

```sh
rokit install
lute run test
lute run benches/propagate # 65us (reactivity) vs 80us (Charm) for 10*10
lute run benches/multi
```
