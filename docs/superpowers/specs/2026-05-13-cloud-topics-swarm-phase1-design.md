# Cloud Topics Swarm Test — Phase 1 Design

Date: 2026-05-13
Author: Evgeny Lazin

## Goal

Lay the foundation for a `cloud_topics_swarm_test.py` integration suite. Phase 1
delivers the Z3-based mechanism/effect model, the per-mechanism enable
primitives and per-effect validate primitives, and one end-to-end smoke test
that proves the abstraction works. Phase 2 (separate spec) builds the
swarm-of-many-runs harness on top.

The new test diverges from `tiered_storage_model_test.py` in approach. Rather
than enumerating combinations, the test picks a *target effect* per run, asks
the model what must be enabled to make that effect possible, and validates by
waiting on the effect's terminal Prometheus metric and on
`KgoVerifierSeqConsumer` content validation.

## Non-goals (Phase 1)

- Many-effect-per-run random swarm — Phase 2.
- Failure injection (node kill, S3 errors) — Phase 2.
- Multiple producer instances and transactional/idempotent flows actually
  exercised in a real test run — Phase 1 wires the mechanism vars, Phase 2
  uses them.
- Test matrix across all effects — Phase 1 ships one smoke test only.

## Approach

Two layers of Z3 booleans:

- **Mechanism** = a knob the test owns. Default value is "off / huge interval
  / huge limit" so nothing fires during baseline. Enabling sets the knob to a
  value that *makes the effect possible* within the test's deadline.
- **Effect** = an observable outcome with a single terminal Prometheus
  metric. Each effect adds an `Implies(effect, And(mechanisms...))` clause to
  the solver.

For a target effect `E`, the solver returns the minimum mechanism set whose
mechanisms must be enabled. Validation only ever waits on `E`'s terminal
metric — never on intermediate metrics along the dependency chain. After
produce/consume, a global content check runs `KgoVerifierSeqConsumer` against
the producer's tally.

### Why this shape

- Mirrors the user's mental model: "I want X to happen, what do I turn on?"
- Validates the *outcome*, not the path. Less brittle than chained waits.
- Keeps Phase 2 simple: random selection of one or more target effects, union
  of dependency sets, layer optional "spice" mechanisms on top.

## File layout

```
tests/rptest/tests/cloud_topics_swarm_model.py       # Z3 model
tests/rptest/tests/cloud_topics_swarm_primitives.py  # enable + validate primitives
tests/rptest/tests/cloud_topics_swarm_test.py        # ducktape test class + smoke test
```

`cloud_topics_swarm_model.py` exposes:
- `Mechanism` and `Effect` dataclasses (each owns its Z3 var + metadata).
- `SwarmModel` with `add_mechanism`, `add_effect`, `solve_for(effect_name)`.
- A module-level `default_model()` builder that registers the catalogs below.

`cloud_topics_swarm_primitives.py` exposes:
- An `EnablePrimitive` per mechanism: applies cluster-config / topic-config
  changes; declares whether a node restart is required.
- An `EffectValidator` per effect: terminal metric, threshold, deadline,
  snapshot() + assert_observed() methods.

`cloud_topics_swarm_test.py` exposes:
- `CloudTopicsSwarmTestBase(RedpandaTest)` with helpers to apply primitives,
  create the cloud-mode topic, drive the producer, and assert validators.
- `CloudTopicsSwarmSmokeTest::test_long_term_gc_via_model` — the Phase 1
  smoke test.

## Mechanism catalog

All mechanisms default to the baseline (off / huge / unreachable) and the
enabled value is what the test will set when the solver picks it.

| Mechanism | Baseline | Enabled | Knob(s) |
|---|---|---|---|
| `reconciliation` | `cloud_topics_disable_reconciliation_loop=true` | `false` + min/max interval 2000ms | `cloud_topics_disable_reconciliation_loop`, `cloud_topics_reconciliation_min_interval`, `cloud_topics_reconciliation_max_interval` |
| `retention_low` | `retention.ms` and `retention.bytes` set very large | `retention.ms` ≈ 30s | topic config |
| `long_term_gc_fast` | `cloud_topics_long_term_garbage_collection_interval` very large (≥ 24h) | 5s | same |
| `short_term_gc_fast` | `cloud_topics_disable_level_zero_gc_for_tests=true` | `false`, interval 2s, grace 10s | `cloud_topics_short_term_gc_interval`, `cloud_topics_short_term_gc_minimum_object_age` |
| `compaction` | topic `cleanup.policy=delete`; `cloud_topics_compaction_interval_ms` very large | topic `cleanup.policy=compact`; interval 5s | topic config + cluster config |
| `epoch_increment_fast` | `cloud_topics_epoch_service_epoch_increment_interval` very large (1h) | 5s | `cloud_topics_epoch_service_epoch_increment_interval`, `cloud_topics_epoch_service_local_epoch_cache_duration` |
| `transactional_producer` | KgoVerifier default (off) | transactional mode | KgoVerifierProducer arg |
| `idempotent_producer` | off | idempotent | KgoVerifierProducer arg |
| `multiple_producers` | 1 producer | N>1 producers | harness |
| `l1_reader_cache_evict_fast` | eviction interval very large *(TBC: exact config key resolved during implementation)* | low | TBC |
| `produce_inflight_limit_low` | `cloud_topics_produce_write_inflight_limit` very large | low (forces backpressure) | same |

Implicit Z3 constraints:
- `transactional_producer ⇒ idempotent_producer`
- Baseline always: `storage.mode == cloud` for the target topic; one producer;
  no transactions; no idempotency.

## Effect catalog

| Effect | Terminal metric | Requires (mechanisms) |
|---|---|---|
| `l1_upload_observed` | `vectorized_cloud_topics_reconciler_objects_uploaded` | `reconciliation` |
| `short_term_gc_observed` | `vectorized_cloud_topics_l0_gc_objects_deleted_total` | `reconciliation`, `short_term_gc_fast`, `epoch_increment_fast` |
| `long_term_gc_observed` | `vectorized_cloud_topics_gc_objects_deleted_total` | `reconciliation`, `retention_low`, `long_term_gc_fast` |
| `compaction_observed` | `vectorized_cloud_topics_log_compactions_total` | `reconciliation`, `compaction` |
| `retention_eviction_observed` | L1 partition size shrinks via admin API (`get_l1_partition_size`); fallback metric TBC | `reconciliation`, `retention_low`, `long_term_gc_fast` |
| `epoch_increment_observed` | `vectorized_cloud_topics_l0_gc_min_partition_gc_epoch` | `reconciliation`, `epoch_increment_fast` |
| `l1_reader_eviction_observed` | reader-cache eviction counter *(metric name TBC)* | `reconciliation`, `l1_reader_cache_evict_fast` + consumer activity |
| `inflight_backpressure_observed` | write-path backpressure counter *(metric name TBC)* | `produce_inflight_limit_low` |

`TBC` items will be resolved during implementation by reading the relevant
probe source. They are localised to two effects and do not block the smoke
test, which targets `long_term_gc_observed`.

## Validate-primitive shape

```python
class EffectValidator:
    name: str            # matches Effect.name
    metric: str          # Prometheus metric or None for admin-API mode
    threshold: int = 1
    deadline_sec: int = 120

    def snapshot(test: RedpandaTest) -> None:
        self.baseline = _sum_metric(test, self.metric)

    def assert_observed(test: RedpandaTest) -> None:
        wait_until(
            lambda: _sum_metric(test, self.metric) - self.baseline >= self.threshold,
            timeout_sec=self.deadline_sec, backoff_sec=2, retry_on_exc=True,
            err_msg=f"effect {self.name}: metric {self.metric} did not advance",
        )
```

Two validator subclasses exist:
- `MetricEffectValidator` (terminal counter delta — the common case).
- `AdminApiEffectValidator` (e.g. `retention_eviction_observed` polls
  `get_l1_partition_size` and asserts it shrank).

After produce, a global content check always runs:

```python
KgoVerifierSeqConsumer.wait()
assert cstatus.validator.invalid_reads == 0
assert cstatus.validator.out_of_scope_invalid_reads == 0
assert cstatus.validator.valid_reads >= acked
```

## Enable-primitive shape

```python
class EnablePrimitive:
    name: str                              # matches Mechanism.name
    cluster_config_overrides: dict[str, Any]  # applied at startup
    topic_config_overrides: dict[str, str]    # applied at topic create
    producer_overrides: dict[str, Any]        # KgoVerifierProducer kwargs
    needs_restart: bool = False
    def apply_runtime(test) -> None: ...      # optional, for non-restart knobs
```

Two registries: a baseline `BaselineConfig` always applied, plus the selected
mechanisms layered on top in solver-returned order. The harness merges
overrides and applies cluster config before `super().setUp()` runs.

## Smoke test (Phase 1 end-to-end)

`CloudTopicsSwarmSmokeTest::test_long_term_gc_via_model`:

1. `model = default_model()`; `mechanisms = model.solve_for("long_term_gc_observed")`.
2. Build cluster config by merging baseline + each mechanism's
   `cluster_config_overrides`.
3. Construct `RedpandaTest` with that config; call `setUp()`.
4. Create topic with `storage.mode=cloud` + merged topic overrides.
5. Resolve `EffectValidator` for `long_term_gc_observed`; call `snapshot()`.
6. Start `KgoVerifierProducer` (msg_count sized so produce duration > 3×
   long-term GC interval, e.g. 5000 × 1KiB messages over ~30s).
7. Producer completes (`wait()`).
8. Call validator `assert_observed()` — waits for the L1-GC-deleted metric
   to move.
9. Start `KgoVerifierSeqConsumer`, wait, assert no data loss.

## Testing the model itself

A separate `tests/rptest/tests/cloud_topics_swarm_model_test.py` is *not*
included in Phase 1 — the model is exercised by the smoke test. If the solver
returns the wrong mechanism set, the smoke test fails loudly with a clear
error: either RP startup fails (bad config) or the terminal metric never
moves (missing dependency).

A future pytest-level unit test for the model could assert, e.g., that
`solve_for("long_term_gc_observed")` returns exactly
`{reconciliation, retention_low, long_term_gc_fast}`. Defer to Phase 2.

## Open items resolved during implementation

- Exact config key for `l1_reader_cache_evict_fast`.
- Exact terminal metric for `l1_reader_eviction_observed`.
- Exact terminal metric for `inflight_backpressure_observed`.
- Whether `retention_eviction_observed` has a dedicated counter or must rely
  on admin-API partition-size polling (current assumption: admin-API).

None of these block the smoke test, which targets `long_term_gc_observed`.

## Risks

- **Metric name churn.** Cloud-topics metrics are still evolving. The
  validate-primitive layer is the single point of update; any rename is a
  one-line fix.
- **Restart cost.** Some mechanism toggles need a restart (config flags read
  at boot). The harness picks restart-needed first and applies all overrides
  in one shot before `super().setUp()` — never restarts mid-test in Phase 1.
- **Solver returning a too-small set.** If a dependency edge is missing, the
  smoke test fails by timeout. Acceptable for Phase 1 since the failure mode
  is loud and the fix is to add an `Implies()` edge.
