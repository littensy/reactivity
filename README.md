# reactivity

I wanted to learn more about [`alien-signals`](https://github.com/stackblitz/alien-signals), so here's a fun little test implementation of it with some changes and deoptimizations:

- Store subscribers and dependencies in-place in arrays instead of a linked list
- Add cleanup functions to effects and effect scopes
- Signals accept an update function to avoid tracking dependencies during updates that access the old value
- Error handling in `flush()` runs all queued effects instead of stopping at the first error
- Remove recursion checks for effects and computed signals

This matches Charm's performance, which I think is surprising considering the changes made to improve readability.

Read more about `alien-signals`: [`alien-signals` Deep Dive](https://gist.github.com/johnsoncodehk/59e79a0cfa5bb3421b5d166a08e42f30)

## Todo

- Removing recursion checks from computed signals breaks some tests, and I want to learn why this happens or whether it's important. The tests are:
  - `computed > should chained computeds keep reactivity when computed effect happens`
  - `computed > should not trigger effect scheduler by recursive computed effect`
  - `computed > should trigger effect even computed already dirty`

## Testing

Install Rokit: https://github.com/rojo-rbx/rokit

```sh
rokit install
lute run test
lute run benches/propagate # 79.5us (reactivity) vs 80us (Charm) for 10*10
```
