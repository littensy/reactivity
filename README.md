# reactivity

I wanted to learn more about alien-signals, so here's a fun little test implementation of [alien-signals](https://github.com/stackblitz/alien-signals) with some changes and deoptimizations:

- Store subscribers and dependencies in arrays instead of linked lists
- Add cleanup functions to effects and effect scopes
- Signals accept an update function to avoid tracking dependencies while setting
- Error handling in `flush()` runs all queued effects instead of stopping at the first error
- Remove recursion checks for effects and computed signals

## Todo

- Try passing the `effect > should duplicate subscribers do not affect the notify order` test by using versioned link objects and deduplicating links in the `link()` function
  - `purgeStaleDeps()` would check link versions instead of removing links made before a marker
- Removing recursion checks from computed signals breaks some tests, and I want to learn why this happens or whether it's important. The tests are:
  - `computed > should chained computeds keep reactivity when computed effect happens`
  - `computed > should not trigger effect scheduler by recursive computed effect`
  - `computed > should trigger effect even computed already dirty`
- Remove `length` field from arrays if the performance difference is negligible

## Testing

Install Rokit: https://github.com/rojo-rbx/rokit

```sh
rokit install
lute run test
lute run benches/propagate # 113us (reactivity) vs 80us (Charm) for 10*10
```
