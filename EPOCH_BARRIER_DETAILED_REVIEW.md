# Epoch Barrier Implementation Review

## Architecture Summary

The barrier protocol has three layers:

1. **`epoch_barrier_manager`** (leader singleton): drives rounds — get candidate epoch from health reports, fan-out invalidate, poll-drain all nodes, publish safe epoch.
2. **`epoch_barrier_coordinator`** (per-node sharded): handles the local side — invalidate epoch cache, drain inflight writes, collect seal points, check LRO catch-up.
3. **`ctp_stm_state::estimate_barrier_eligible_epoch()`**: the per-partition source of candidate epochs fed via health reports.

---

## Critical Findings

### 1. `estimate_barrier_eligible_epoch()` returns `_max_applied_epoch` unconditionally — not `_barrier_epoch_estimate`

`ctp_stm_state.cc:100-102`:
```cpp
std::optional<cluster_epoch>
ctp_stm_state::estimate_barrier_eligible_epoch() const noexcept {
    return _max_applied_epoch;
}
```

You added and maintain `_barrier_epoch_estimate` in `advance_epoch()` and `advance_last_reconciled_offset()`, yet it's never actually read. The method returns `_max_applied_epoch` directly, which is *always* the highest epoch the partition has ever seen.

This is **less conservative than intended**. Consider this scenario: epoch advances from 5 to 6 at some offset O. Before LRO catches up to O, `_max_applied_epoch` is 6, but `_barrier_epoch_estimate` would still be 5 (from its previous value). With the barrier protocol in play this may be safe — epoch 6 data hasn't been written at epoch <= 5, so GC'ing epoch 5 objects doesn't touch epoch 6 data. But then `_barrier_epoch_estimate` is dead code, and the doc comments on the field and `advance_last_reconciled_offset()` are misleading about what actually gates this value.

**Question**: Is the intent to use `_barrier_epoch_estimate` (more conservative — tracks LRO advancement) or `_max_applied_epoch` (aggressive — always the latest)? If it's always `_max_applied_epoch`, remove `_barrier_epoch_estimate` and its maintenance. If `_barrier_epoch_estimate` was meant to gate things on LRO catch-up, use it.

### 2. The `invalidate()` sequencing with `force_epoch_update` is fire-and-forget

`epoch_barrier_coordinator.cc:91-107`:
```cpp
co_await container().invoke_on_all(
  [](epoch_barrier_coordinator& c) { c._round.reset(); });

co_await _epoch_service.local().force_epoch_update(candidate());
co_await _epoch_service.local().invalidate_epoch_cache(
  cluster_epoch::max());
```

`force_epoch_update(int64_t)` (`cluster_epoch_service.cc:367`) submits to a work queue internally but **doesn't await completion of the actual epoch update** — it enqueues it and returns. So:
- The `co_await` here returns before the controller stm has actually been bumped.
- The immediately-following `invalidate_epoch_cache(cluster_epoch::max())` invalidates with max, so future reads will re-fetch.
- But there's a window where a writer could still get the old cached epoch between `force_epoch_update` returning and the cache actually being invalidated.

This is the TODO you flagged: *"error handling in here is really bad"*. The safety depends on the invalidation happening before any writes under the new epoch are accepted. Because `invalidate_epoch_cache(max())` immediately invalidates the cache (it's a synchronous shard-local operation under `invoke_on_all`), this is likely safe — subsequent `get_cached_epoch` calls will block until the update completes. But the `force_epoch_update` call's return value is unused, and if it fails, the barrier silently proceeds.

### 3. Drain-then-seal is not atomic — writes can arrive between drain and seal collection

In `poll_drain()` (`epoch_barrier_coordinator.cc:197-216`):
```cpp
co_await _data_plane.drain_inflight_writes();
co_await container().invoke_on_all(
  [candidate](epoch_barrier_coordinator& c) {
      c.collect_local_seal_points(candidate);
  });
```

After `drain_inflight_writes()` returns and before `collect_local_seal_points()` runs on each shard, **new writes can start and advance committed offsets**. The seal points will capture whatever committed offset is current at collection time, which may include data from writes that started *after* the drain.

This is **safe for correctness** — those new writes will use the new (higher) epoch from the invalidated cache, and the seal captures a committed offset that's >= what was present pre-drain. The seal+LRO check will ensure those writes are reconciled before completing the round. But it means the barrier isn't a clean "quiesce at exactly this point"; it's "quiesce, then snapshot a moving target." This could delay convergence if writes are rapid.

### 4. Inflight write token lifetime: safe in practice, fragile in principle

The `inflight_write_token` uses an intrusive list — the batcher owns a reference to the token, but the caller owns the `unique_ptr`:

```cpp
auto token = std::make_unique<inflight_write_token>();
_inflight_writes.push_back(*token);  // reference stored
return token;                          // ownership transferred to caller
```

The call sites are correct — both write paths in `frontend.cc` use `ss::defer` to set the done promise before the coroutine frame is destroyed:

```cpp
auto token = api->track_inflight_write();
auto on_write_exit = ss::defer([&token] { token->done.set_value(); });
```

This works because `ss::defer` fires at scope exit (including exception/early return), and the token lives on the coroutine frame. But:
- If someone adds a write path that moves the token elsewhere or forgets the deferred, `drain_writes()` will hang forever (no timeout).
- The intrusive list `drain.swap()` detaches the reference, so even if a token is destroyed without setting the promise, the dangling reference would have been in the old (drained) list, not the active one — but the drain future would still never complete.

**Recommendation**: Consider an RAII wrapper that auto-sets the promise on destruction, or at minimum add a timeout to `drain_writes()`.

### 5. `check_local_seal_points()` returns `reconciled` when `_round` is `nullopt`

```cpp
seal_check_result
epoch_barrier_coordinator::check_local_seal_points() {
    if (!_round) {
        return seal_check_result::reconciled;
    }
```

This is fine for the normal path (shard 0 drives, all shards have `_round` set during the same `invoke_on_all`). But if a race between `invoke_on_all` (resetting `_round`) and `map_reduce0` (checking seal points) causes one shard to see `_round == nullopt`, it would report `reconciled` even though the round was actually reset due to staleness. The `max` reducer would then under-count the problem.

In the single-threaded-per-shard Seastar model, this can't happen — `invoke_on_all` and `map_reduce0` are sequential on each shard. So this is safe. Just flagging it as something to be aware of if the coordination model changes.

### 6. Manager leader failover gap

The `epoch_barrier_manager` runs on the L1 metastore partition 0 leader. If leadership moves:
- The old leader's `barrier_loop` is stopped (via `reset_loop(needs_loop::no)`).
- The new leader starts fresh — no state transfer of `_safe_epoch` or in-progress round.
- `_safe_epoch` on the new leader starts as `nullopt`, so GC goes cold until the first barrier round completes on the new leader.

This is safe (GC just pauses) but could cause a temporary stall in GC progress. Worth documenting.

### 7. Fan-out is sequential, not parallel

All three fan-out operations (`fan_out_invalidate`, `poll_all_nodes`, `fan_out_publish`) iterate nodes sequentially:
```cpp
for (auto node_id : nodes) {
    // ... RPC to each node one at a time
}
```

For correctness, invalidate doesn't need to be sequential — you just need all-or-nothing. But for **safety**, the sequential approach means that if node N fails, you don't send to nodes N+1..M, which is the right early-exit behavior for invalidate (where you need all nodes invalidated). For `poll_drain`, sequential is fine since any single `false` means "not done yet."

For performance, you might want to parallelize (especially invalidate, which is latency-sensitive), but this isn't a safety issue.

### 8. Minor: typo in log message

`epoch_barrier_manager.cc:227`:
```cpp
vlog(cd_log.warn, "Successfully invalidaded epoch on {}", node_id);
```
"invalidaded" -> "invalidated", and this should probably be `debug` not `warn`.

---

## Summary

The protocol design is sound. The key safety property — "GC only deletes objects at epochs where no new data can arrive and all existing data has been reconciled to L1" — is upheld by the combination of:
1. Epoch cache invalidation preventing new writes at old epochs
2. Inflight drain ensuring in-progress writes complete
3. Seal points + LRO checks ensuring reconciler has caught up

**Action items by priority**:
1. **Decide on `_barrier_epoch_estimate` vs `_max_applied_epoch`** — dead code or intended usage? (Finding 1)
2. **Error handling in `invalidate()`** — at minimum check the return of epoch service calls (Finding 2)
3. **Timeout/RAII for inflight tokens** — defensive against future misuse (Finding 4)
4. **Fix the typo and log level** (Finding 8)
