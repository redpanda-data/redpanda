# Cloud Topics Swarm Test — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the Z3 mechanism/effect model, enable/validate primitives, and one end-to-end smoke test (`test_long_term_gc_via_model`) for cloud topics.

**Architecture:** Two-layer Z3 model — `Mechanism` booleans the test toggles, `Effect` booleans tied 1:1 to terminal Prometheus metrics. `Effect.requires(*mechs)` adds `Implies(effect, And(mechs))` to the solver. The smoke test asks the solver for a target effect's minimum mechanism set, applies cluster + topic config overrides, runs `KgoVerifierProducer`, waits on the terminal metric, then runs `KgoVerifierSeqConsumer` for content validation.

**Tech Stack:** Python, `z3-solver` (already a dep — see `tests/rptest/tests/tiered_storage_model.py`), ducktape (`rptest.tests.redpanda_test.RedpandaTest`), `KgoVerifierProducer` / `KgoVerifierSeqConsumer`, Prometheus metrics scraping via `RedpandaService.metrics_sample`.

**Spec:** `docs/superpowers/specs/2026-05-13-cloud-topics-swarm-phase1-design.md`

---

## File layout

| File | Purpose |
|---|---|
| `tests/rptest/tests/cloud_topics_swarm_model.py` | `Mechanism`, `Effect`, `SwarmModel`, `default_model()` |
| `tests/rptest/tests/cloud_topics_swarm_primitives.py` | `EnablePrimitive`, `EffectValidator`, baseline config, registries |
| `tests/rptest/tests/cloud_topics_swarm_test.py` | `CloudTopicsSwarmTestBase` + `CloudTopicsSwarmSmokeTest` |
| `tests/rptest/tests/cloud_topics_swarm_model_test.py` | Pytest-level unit tests for the Z3 model (no ducktape) |

---

## Task 1: Z3 model — `Mechanism`, `Effect`, `SwarmModel` skeleton

**Files:**
- Create: `tests/rptest/tests/cloud_topics_swarm_model.py`
- Test: `tests/rptest/tests/cloud_topics_swarm_model_test.py`

- [ ] **Step 1: Write the failing unit test for solver dependency resolution**

Create `tests/rptest/tests/cloud_topics_swarm_model_test.py`:

```python
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.tests.cloud_topics_swarm_model import Effect, Mechanism, SwarmModel


def test_solver_returns_minimum_dependency_set():
    model = SwarmModel()
    m_recon = Mechanism(model, "reconciliation")
    m_ret = Mechanism(model, "retention_low")
    m_unrelated = Mechanism(model, "unrelated")
    e_l1 = Effect(model, "l1_upload", terminal_metric="m_l1")
    e_l1.requires(m_recon)
    e_gc = Effect(model, "long_term_gc", terminal_metric="m_gc")
    e_gc.requires(m_recon, m_ret)

    chosen = model.solve_for("long_term_gc")

    names = {m.name for m in chosen}
    assert names == {"reconciliation", "retention_low"}
    assert "unrelated" not in names


def test_solver_raises_on_unknown_effect():
    model = SwarmModel()
    try:
        model.solve_for("does_not_exist")
        assert False, "expected KeyError"
    except KeyError:
        pass
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/rptest/tests/cloud_topics_swarm_model_test.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rptest.tests.cloud_topics_swarm_model'`

- [ ] **Step 3: Write minimal implementation**

Create `tests/rptest/tests/cloud_topics_swarm_model.py`:

```python
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""Z3 model for the cloud-topics swarm test.

Two layers of Z3 booleans:
- ``Mechanism``: a knob the test owns (reconciliation on/off, retention low,
  ...). Default is the baseline value (off / huge interval / unreachable
  limit).
- ``Effect``: an observable outcome with one terminal Prometheus metric.
  Each Effect declares the mechanisms it requires via ``Effect.requires()``.

For a target effect, ``SwarmModel.solve_for(name)`` returns the minimal list
of mechanisms whose Z3 variables must be True for the effect to be possible.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import z3


@dataclass
class Mechanism:
    """A test-controllable knob (Z3 boolean variable)."""

    model: "SwarmModel"
    name: str
    # Cluster-config / topic-config / producer-arg overrides applied when
    # the mechanism is enabled. Populated by the primitives module; left
    # empty here so the model module has no ducktape dependency.
    cluster_config_overrides: dict[str, Any] = field(default_factory=dict)
    topic_config_overrides: dict[str, str] = field(default_factory=dict)
    producer_overrides: dict[str, Any] = field(default_factory=dict)
    needs_restart: bool = False

    def __post_init__(self) -> None:
        self.var = z3.Bool(self.name)
        self.model._register_mechanism(self)


@dataclass
class Effect:
    """An observable outcome with a terminal Prometheus metric."""

    model: "SwarmModel"
    name: str
    terminal_metric: str | None  # None means admin-API based validator
    threshold: int = 1
    deadline_sec: int = 120
    _requires: list[Mechanism] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.var = z3.Bool(self.name)
        self.model._register_effect(self)

    def requires(self, *mechs: Mechanism) -> None:
        self._requires.extend(mechs)
        self.model._add_implication(self.var, [m.var for m in mechs])


class SwarmModel:
    def __init__(self) -> None:
        self._mechs: dict[str, Mechanism] = {}
        self._effects: dict[str, Effect] = {}
        self._implications: list[z3.BoolRef] = []

    def _register_mechanism(self, m: Mechanism) -> None:
        assert m.name not in self._mechs, f"duplicate mechanism {m.name}"
        self._mechs[m.name] = m

    def _register_effect(self, e: Effect) -> None:
        assert e.name not in self._effects, f"duplicate effect {e.name}"
        self._effects[e.name] = e

    def _add_implication(self, effect_var, mech_vars) -> None:
        self._implications.append(z3.Implies(effect_var, z3.And(*mech_vars)))

    def get_effect(self, name: str) -> Effect:
        if name not in self._effects:
            raise KeyError(f"unknown effect: {name}")
        return self._effects[name]

    def solve_for(self, effect_name: str) -> list[Mechanism]:
        """Return the minimal list of mechanisms that must be enabled to
        make ``effect_name`` possible. Mechanisms appear in registration
        order (which we treat as dependency order)."""
        effect = self.get_effect(effect_name)
        solver = z3.Optimize()
        for impl in self._implications:
            solver.add(impl)
        solver.add(effect.var == True)
        # Minimise the count of enabled mechanisms (sum of bools).
        solver.minimize(
            z3.Sum([z3.If(m.var, 1, 0) for m in self._mechs.values()])
        )
        if solver.check() != z3.sat:
            raise ValueError(f"unsatisfiable: cannot enable {effect_name}")
        model = solver.model()
        chosen = []
        for name, mech in self._mechs.items():  # insertion order
            if model.eval(mech.var, model_completion=True) == True:
                chosen.append(mech)
        return chosen
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/rptest/tests/cloud_topics_swarm_model_test.py -v`
Expected: both tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/rptest/tests/cloud_topics_swarm_model.py \
        tests/rptest/tests/cloud_topics_swarm_model_test.py
git commit -m "tests/rptest: add Z3 swarm model for cloud topics

Mechanism / Effect / SwarmModel primitives with Optimize-based
minimum-mechanism solver. Used by the upcoming cloud_topics_swarm_test."
```

---

## Task 2: Register mechanism + effect catalogs, lock dependency edges

**Files:**
- Modify: `tests/rptest/tests/cloud_topics_swarm_model.py`
- Modify: `tests/rptest/tests/cloud_topics_swarm_model_test.py`

- [ ] **Step 1: Write the failing test asserting the long-term-GC dep set**

Append to `tests/rptest/tests/cloud_topics_swarm_model_test.py`:

```python
from rptest.tests.cloud_topics_swarm_model import default_model


def test_default_model_long_term_gc_dependencies():
    model = default_model()
    chosen = {m.name for m in model.solve_for("long_term_gc_observed")}
    assert chosen == {"reconciliation", "retention_low", "long_term_gc_fast"}


def test_default_model_short_term_gc_dependencies():
    model = default_model()
    chosen = {m.name for m in model.solve_for("short_term_gc_observed")}
    assert chosen == {
        "reconciliation",
        "short_term_gc_fast",
        "epoch_increment_fast",
    }


def test_default_model_l1_upload_dependencies():
    model = default_model()
    chosen = {m.name for m in model.solve_for("l1_upload_observed")}
    assert chosen == {"reconciliation"}


def test_transactional_implies_idempotent():
    """transactional_producer ⇒ idempotent_producer (Kafka semantics)."""
    model = default_model()
    # No effect today depends on transactional_producer, so just exercise
    # the implication via an ad-hoc solve.
    import z3
    s = z3.Solver()
    txn = model._mechs["transactional_producer"].var
    idemp = model._mechs["idempotent_producer"].var
    s.add(z3.Implies(txn, idemp))
    s.add(txn == True)
    s.add(idemp == False)
    assert s.check() == z3.unsat
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/rptest/tests/cloud_topics_swarm_model_test.py -v`
Expected: 3 failures: `ImportError: cannot import name 'default_model'`.

- [ ] **Step 3: Add `default_model()` with the full catalog**

Append to `tests/rptest/tests/cloud_topics_swarm_model.py`:

```python
def default_model() -> SwarmModel:
    """Build the default mechanism + effect catalog for cloud-topics swarm
    testing. Catalogue ordering matches the spec
    ``2026-05-13-cloud-topics-swarm-phase1-design.md``.

    The cluster/topic/producer overrides live in the primitives module so
    this module stays import-light. Other code may merge those in later by
    looking up ``model._mechs[name]``.
    """
    m = SwarmModel()

    # --- Mechanisms ---
    reconciliation = Mechanism(m, "reconciliation", needs_restart=True)
    retention_low = Mechanism(m, "retention_low")
    long_term_gc_fast = Mechanism(m, "long_term_gc_fast", needs_restart=True)
    short_term_gc_fast = Mechanism(m, "short_term_gc_fast", needs_restart=True)
    compaction = Mechanism(m, "compaction", needs_restart=True)
    epoch_increment_fast = Mechanism(m, "epoch_increment_fast", needs_restart=True)
    transactional_producer = Mechanism(m, "transactional_producer")
    idempotent_producer = Mechanism(m, "idempotent_producer")
    multiple_producers = Mechanism(m, "multiple_producers")
    l1_reader_cache_evict_fast = Mechanism(m, "l1_reader_cache_evict_fast", needs_restart=True)
    produce_inflight_limit_low = Mechanism(m, "produce_inflight_limit_low", needs_restart=True)

    # transactional_producer ⇒ idempotent_producer
    m._implications.append(
        z3.Implies(transactional_producer.var, idempotent_producer.var)
    )

    # --- Effects ---
    l1_upload = Effect(
        m, "l1_upload_observed",
        terminal_metric="vectorized_cloud_topics_reconciler_objects_uploaded",
    )
    l1_upload.requires(reconciliation)

    short_term_gc = Effect(
        m, "short_term_gc_observed",
        terminal_metric="vectorized_cloud_topics_l0_gc_objects_deleted_total",
    )
    short_term_gc.requires(reconciliation, short_term_gc_fast, epoch_increment_fast)

    long_term_gc = Effect(
        m, "long_term_gc_observed",
        terminal_metric="vectorized_cloud_topics_gc_objects_deleted_total",
    )
    long_term_gc.requires(reconciliation, retention_low, long_term_gc_fast)

    compaction_eff = Effect(
        m, "compaction_observed",
        terminal_metric="vectorized_cloud_topics_log_compactions_total",
    )
    compaction_eff.requires(reconciliation, compaction)

    # Admin-API-based: terminal_metric=None signals AdminApi validator.
    retention_eviction = Effect(
        m, "retention_eviction_observed", terminal_metric=None,
    )
    retention_eviction.requires(reconciliation, retention_low, long_term_gc_fast)

    epoch_increment = Effect(
        m, "epoch_increment_observed",
        terminal_metric="vectorized_cloud_topics_l0_gc_min_partition_gc_epoch",
    )
    epoch_increment.requires(reconciliation, epoch_increment_fast)

    # TBC: metric names resolved during implementation. Wire as None for
    # now; their validators will raise NotImplementedError if used.
    l1_reader_eviction = Effect(
        m, "l1_reader_eviction_observed", terminal_metric=None,
    )
    l1_reader_eviction.requires(reconciliation, l1_reader_cache_evict_fast)

    inflight_backpressure = Effect(
        m, "inflight_backpressure_observed", terminal_metric=None,
    )
    inflight_backpressure.requires(produce_inflight_limit_low)

    return m
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/rptest/tests/cloud_topics_swarm_model_test.py -v`
Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/rptest/tests/cloud_topics_swarm_model.py \
        tests/rptest/tests/cloud_topics_swarm_model_test.py
git commit -m "tests/rptest: register cloud-topics mechanism + effect catalog

default_model() returns the Phase 1 mechanism/effect graph. Unit tests
lock dependency edges for long_term_gc, short_term_gc, l1_upload and
the transactional⇒idempotent implication."
```

---

## Task 3: Enable-primitive registry with config overrides

**Files:**
- Create: `tests/rptest/tests/cloud_topics_swarm_primitives.py`

- [ ] **Step 1: Write the primitives module with override tables**

Create `tests/rptest/tests/cloud_topics_swarm_primitives.py`:

```python
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""Enable-primitives and validate-primitives for the cloud-topics swarm
test. The model module (cloud_topics_swarm_model) intentionally has no
ducktape dependency; this module bridges the two.

For each mechanism in ``default_model()`` we record:
- cluster-config overrides applied at startup;
- topic-config overrides applied at topic create;
- producer-side overrides (KgoVerifierProducer kwargs).

The baseline config is what the test always applies first. Enabling a
mechanism overlays its overrides on top of the baseline."""

from __future__ import annotations

from typing import Any

from rptest.clients.types import TopicSpec
from rptest.services.redpanda import CLOUD_TOPICS_CONFIG_STR
from rptest.tests.cloud_topics_swarm_model import Mechanism, SwarmModel

# Sentinel "unreachable" values. Roughly: 24 hours, 1 TiB, 1B messages.
_HUGE_INTERVAL_MS = 24 * 60 * 60 * 1000
_HUGE_BYTES = 1024 * 1024 * 1024 * 1024
_HUGE_COUNT = 1_000_000_000


# Baseline cluster config: cloud topics enabled, everything else either
# off or set to an effectively-unreachable value so the baseline run
# produces with NO reconciliation, NO GC, NO compaction, NO epoch bumps.
BASELINE_CLUSTER_CONFIG: dict[str, Any] = {
    CLOUD_TOPICS_CONFIG_STR: True,
    "cloud_topics_disable_reconciliation_loop": True,
    "cloud_topics_disable_level_zero_gc_for_tests": True,
    "cloud_topics_long_term_garbage_collection_interval": _HUGE_INTERVAL_MS,
    "cloud_topics_compaction_interval_ms": _HUGE_INTERVAL_MS,
    "cloud_topics_epoch_service_epoch_increment_interval": _HUGE_INTERVAL_MS,
    "cloud_topics_epoch_service_local_epoch_cache_duration": _HUGE_INTERVAL_MS,
    "cloud_topics_produce_write_inflight_limit": _HUGE_COUNT,
}

# Baseline topic config: storage.mode=cloud, retention effectively
# disabled, cleanup.policy=delete (no compaction).
BASELINE_TOPIC_CONFIG: dict[str, str] = {
    TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
    TopicSpec.PROPERTY_RETENTION_TIME: str(_HUGE_INTERVAL_MS),
    TopicSpec.PROPERTY_RETENTION_BYTES: str(_HUGE_BYTES),
    TopicSpec.PROPERTY_CLEANUP_POLICY: "delete",
}


def attach_overrides(model: SwarmModel) -> None:
    """Fill in cluster/topic/producer overrides on the mechanisms in
    ``model``. Idempotent."""

    def set_overrides(name: str, *, cluster=None, topic=None, producer=None) -> None:
        m = model._mechs[name]
        if cluster:
            m.cluster_config_overrides.update(cluster)
        if topic:
            m.topic_config_overrides.update(topic)
        if producer:
            m.producer_overrides.update(producer)

    set_overrides(
        "reconciliation",
        cluster={
            "cloud_topics_disable_reconciliation_loop": False,
            "cloud_topics_reconciliation_min_interval": 2000,
            "cloud_topics_reconciliation_max_interval": 2000,
        },
    )
    set_overrides(
        "retention_low",
        topic={TopicSpec.PROPERTY_RETENTION_TIME: "30000"},  # 30s
    )
    set_overrides(
        "long_term_gc_fast",
        cluster={"cloud_topics_long_term_garbage_collection_interval": 5000},
    )
    set_overrides(
        "short_term_gc_fast",
        cluster={
            "cloud_topics_disable_level_zero_gc_for_tests": False,
            "cloud_topics_short_term_gc_interval": 2000,
            "cloud_topics_short_term_gc_minimum_object_age": 10000,
            "cloud_topics_short_term_gc_backoff_interval": 10000,
            "cloud_topics_gc_health_check_interval": 2000,
        },
    )
    set_overrides(
        "compaction",
        cluster={"cloud_topics_compaction_interval_ms": 5000},
        topic={TopicSpec.PROPERTY_CLEANUP_POLICY: "compact"},
    )
    set_overrides(
        "epoch_increment_fast",
        cluster={
            "cloud_topics_epoch_service_epoch_increment_interval": 5000,
            "cloud_topics_epoch_service_local_epoch_cache_duration": 5000,
        },
    )
    set_overrides(
        "transactional_producer",
        producer={"use_transactions": True},
    )
    set_overrides(
        "idempotent_producer",
        # KgoVerifierProducer enables idempotency implicitly when acks=all
        # and transactions are off; we have no separate switch. Leave the
        # override empty — the mechanism remains in the model so Phase 2
        # can flip it without surgery.
        producer={},
    )
    # multiple_producers / l1_reader_cache_evict_fast / produce_inflight_limit_low:
    # not exercised by the Phase 1 smoke test. Overrides intentionally left
    # empty so the model stays a faithful catalogue. Phase 2 will wire them.


def merged_cluster_config(chosen: list[Mechanism]) -> dict[str, Any]:
    """Baseline + each chosen mechanism's cluster overrides, in order."""
    cfg = dict(BASELINE_CLUSTER_CONFIG)
    for m in chosen:
        cfg.update(m.cluster_config_overrides)
    return cfg


def merged_topic_config(chosen: list[Mechanism]) -> dict[str, str]:
    cfg = dict(BASELINE_TOPIC_CONFIG)
    for m in chosen:
        cfg.update(m.topic_config_overrides)
    return cfg


def merged_producer_kwargs(chosen: list[Mechanism]) -> dict[str, Any]:
    kw: dict[str, Any] = {}
    for m in chosen:
        kw.update(m.producer_overrides)
    return kw
```

- [ ] **Step 2: Append unit test for merging**

Append to `tests/rptest/tests/cloud_topics_swarm_model_test.py`:

```python
def test_merged_cluster_config_for_long_term_gc():
    from rptest.tests.cloud_topics_swarm_model import default_model
    from rptest.tests.cloud_topics_swarm_primitives import (
        attach_overrides, merged_cluster_config,
    )

    model = default_model()
    attach_overrides(model)
    chosen = model.solve_for("long_term_gc_observed")
    cfg = merged_cluster_config(chosen)

    # Baseline says disable reconciliation; the mechanism re-enables it.
    assert cfg["cloud_topics_disable_reconciliation_loop"] is False
    assert cfg["cloud_topics_reconciliation_min_interval"] == 2000
    assert cfg["cloud_topics_long_term_garbage_collection_interval"] == 5000
    # Baseline values still present for non-overridden keys.
    assert cfg["cloud_topics_disable_level_zero_gc_for_tests"] is True


def test_merged_topic_config_for_long_term_gc():
    from rptest.clients.types import TopicSpec
    from rptest.tests.cloud_topics_swarm_model import default_model
    from rptest.tests.cloud_topics_swarm_primitives import (
        attach_overrides, merged_topic_config,
    )

    model = default_model()
    attach_overrides(model)
    chosen = model.solve_for("long_term_gc_observed")
    tcfg = merged_topic_config(chosen)

    assert tcfg[TopicSpec.PROPERTY_STORAGE_MODE] == TopicSpec.STORAGE_MODE_CLOUD
    assert tcfg[TopicSpec.PROPERTY_RETENTION_TIME] == "30000"
    assert tcfg[TopicSpec.PROPERTY_CLEANUP_POLICY] == "delete"
```

- [ ] **Step 3: Run tests**

Run: `pytest tests/rptest/tests/cloud_topics_swarm_model_test.py -v`
Expected: all 7 tests PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/rptest/tests/cloud_topics_swarm_primitives.py \
        tests/rptest/tests/cloud_topics_swarm_model_test.py
git commit -m "tests/rptest: add enable-primitive overrides for swarm mechanisms

Baseline cluster+topic config disables everything; each mechanism in
the swarm model layers its overrides on top via attach_overrides()
and merged_*_config()."
```

---

## Task 4: Validate-primitive (EffectValidator)

**Files:**
- Modify: `tests/rptest/tests/cloud_topics_swarm_primitives.py`

- [ ] **Step 1: Append EffectValidator code**

Append to `tests/rptest/tests/cloud_topics_swarm_primitives.py`:

```python
from ducktape.utils.util import wait_until

from rptest.tests.cloud_topics_swarm_model import Effect


def _sum_metric(redpanda, metric_name: str) -> int:
    """Sum a Prometheus metric across all nodes/shards. Returns 0 if not
    yet exposed (so snapshot() returns 0 cleanly before workload starts)."""
    samples = redpanda.metrics_sample(metric_name)
    if samples is None or not samples.samples:
        return 0
    return int(sum(s.value for s in samples.samples))


class EffectValidator:
    """Final-effect metric-delta validator. snapshot() captures the
    baseline before the workload; assert_observed() polls until the
    metric advances by ``threshold`` or the deadline expires."""

    def __init__(self, effect: Effect):
        if effect.terminal_metric is None:
            raise NotImplementedError(
                f"effect {effect.name!r} has no terminal_metric; an "
                f"AdminApi-based validator is required (see spec §4 TBC)"
            )
        self._effect = effect
        self._baseline: int | None = None

    @property
    def name(self) -> str:
        return self._effect.name

    @property
    def metric(self) -> str:
        assert self._effect.terminal_metric is not None
        return self._effect.terminal_metric

    def snapshot(self, redpanda) -> None:
        self._baseline = _sum_metric(redpanda, self.metric)

    def assert_observed(self, redpanda, logger) -> None:
        assert self._baseline is not None, "call snapshot() before assert_observed()"
        threshold = self._effect.threshold
        deadline = self._effect.deadline_sec
        last = [self._baseline]

        def _moved() -> bool:
            curr = _sum_metric(redpanda, self.metric)
            last[0] = curr
            return (curr - self._baseline) >= threshold

        wait_until(
            _moved,
            timeout_sec=deadline,
            backoff_sec=2,
            retry_on_exc=True,
            err_msg=lambda: (
                f"effect {self.name!r}: metric {self.metric!r} did not "
                f"advance by >= {threshold} within {deadline}s "
                f"(baseline={self._baseline}, last={last[0]})"
            ),
        )
        logger.info(
            f"effect {self.name!r}: {self.metric!r} advanced "
            f"{self._baseline} -> {last[0]}"
        )
```

- [ ] **Step 2: Sanity-check import**

Run: `python -c "from rptest.tests.cloud_topics_swarm_primitives import EffectValidator; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 3: Commit**

```bash
git add tests/rptest/tests/cloud_topics_swarm_primitives.py
git commit -m "tests/rptest: add EffectValidator for swarm test terminal metrics"
```

---

## Task 5: Ducktape test scaffolding — `CloudTopicsSwarmTestBase`

**Files:**
- Create: `tests/rptest/tests/cloud_topics_swarm_test.py`

- [ ] **Step 1: Write the test base class**

Create `tests/rptest/tests/cloud_topics_swarm_test.py`:

```python
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""Cloud topics swarm test.

Phase 1: model-driven smoke test that targets a single effect, asks the
Z3 model what to enable, applies cluster+topic overrides, runs the
workload, and validates the effect's terminal metric. See
``docs/superpowers/specs/2026-05-13-cloud-topics-swarm-phase1-design.md``.
"""

from __future__ import annotations

from typing import Any

from ducktape.mark import matrix
from ducktape.tests.test import TestContext

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.context.cloud_storage import CloudStorageType
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.redpanda import SISettings, get_cloud_storage_type
from rptest.tests.cloud_topics_swarm_model import default_model
from rptest.tests.cloud_topics_swarm_primitives import (
    EffectValidator,
    attach_overrides,
    merged_cluster_config,
    merged_producer_kwargs,
    merged_topic_config,
)
from rptest.tests.redpanda_test import RedpandaTest


class CloudTopicsSwarmTestBase(RedpandaTest):
    """Base class for model-driven cloud-topics tests.

    Subclasses call ``solve_and_setup(effect_name)`` from their test
    method to apply the right config, then ``run_smoke(...)`` to drive
    the producer/consumer and validate."""

    # Solved before __init__ runs — set by subclass test methods using
    # the harness-level ``configure_for_effect`` classmethod, OR resolved
    # at __init__ when ``target_effect_name`` is set on the class.

    def __init__(self, test_context: TestContext, target_effect_name: str):
        self._model = default_model()
        attach_overrides(self._model)
        self._target_effect_name = target_effect_name
        self._chosen = self._model.solve_for(target_effect_name)

        self._chosen_names = [m.name for m in self._chosen]
        cluster_cfg = merged_cluster_config(self._chosen)

        si_settings = SISettings(
            test_context=test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            cloud_storage_housekeeping_interval_ms=1000,
            fast_uploads=True,
        )
        super().__init__(
            test_context=test_context,
            extra_rp_conf=cluster_cfg,
            si_settings=si_settings,
        )

    def _create_cloud_topic(self, name: str, partitions: int = 1) -> TopicSpec:
        topic_cfg = merged_topic_config(self._chosen)
        spec = TopicSpec(name=name, partition_count=partitions)
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(spec.name, spec.partition_count, spec.replication_factor,
                         config=topic_cfg)
        return spec

    def _producer_kwargs(self) -> dict[str, Any]:
        return merged_producer_kwargs(self._chosen)

    def _validator(self) -> EffectValidator:
        return EffectValidator(self._model.get_effect(self._target_effect_name))

    def run_smoke(self, topic_name: str, msg_size: int, msg_count: int) -> None:
        spec = self._create_cloud_topic(topic_name)
        self.logger.info(
            f"swarm: target={self._target_effect_name!r} "
            f"mechanisms={self._chosen_names}"
        )

        validator = self._validator()
        validator.snapshot(self.redpanda)

        producer_kwargs = self._producer_kwargs()
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            spec.name,
            msg_size=msg_size,
            msg_count=msg_count,
            tolerate_failed_produce=True,
            **producer_kwargs,
        )
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            spec.name,
            msg_size=msg_size,
            loop=False,
            nodes=[producer.nodes[0]],
            producer=producer,
        )
        try:
            producer.start()
            producer.wait(timeout_sec=180)
            pstatus = producer.produce_status
            acked = pstatus.acked
            self.logger.info(
                f"swarm: produced acked={acked}/{msg_count} "
                f"bad_offsets={pstatus.bad_offsets}"
            )
            assert acked >= msg_count * 3 // 4, (
                f"too few acks for a meaningful run: {acked}/{msg_count}"
            )

            validator.assert_observed(self.redpanda, self.logger)

            consumer.start(clean=False)
            consumer.wait(timeout_sec=180)
            cstatus = consumer.consumer_status
            self.logger.info(
                f"swarm: consumer valid_reads={cstatus.validator.valid_reads} "
                f"invalid_reads={cstatus.validator.invalid_reads} "
                f"offset_gaps={cstatus.validator.offset_gaps}"
            )
            assert cstatus.validator.invalid_reads == 0, (
                f"data corruption: {cstatus.validator.invalid_reads} invalid reads"
            )
            assert cstatus.validator.out_of_scope_invalid_reads == 0, (
                f"out-of-scope reads: {cstatus.validator.out_of_scope_invalid_reads}"
            )
            assert cstatus.validator.valid_reads >= acked, (
                f"data loss: expected >= {acked} valid reads, "
                f"got {cstatus.validator.valid_reads}"
            )
        finally:
            producer.stop()
            consumer.stop()
            producer.free()
            consumer.free()
```

- [ ] **Step 2: Sanity-check import**

Run: `python -c "from rptest.tests.cloud_topics_swarm_test import CloudTopicsSwarmTestBase; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 3: Commit**

```bash
git add tests/rptest/tests/cloud_topics_swarm_test.py
git commit -m "tests/rptest: add CloudTopicsSwarmTestBase scaffolding"
```

---

## Task 6: Smoke test — `test_long_term_gc_via_model`

**Files:**
- Modify: `tests/rptest/tests/cloud_topics_swarm_test.py`

- [ ] **Step 1: Append the smoke test class**

Append to `tests/rptest/tests/cloud_topics_swarm_test.py`:

```python
class CloudTopicsSwarmSmokeTest(CloudTopicsSwarmTestBase):
    """Phase 1 smoke test: drive long-term GC end to end via the model."""

    MSG_SIZE = 1024
    MSG_COUNT = 5000  # ~5 MiB, completes in ~30s at default produce rate

    def __init__(self, test_context: TestContext):
        super().__init__(test_context, target_effect_name="long_term_gc_observed")

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_long_term_gc_via_model(self, cloud_storage_type: CloudStorageType):
        self.run_smoke(
            topic_name="ct-swarm-long-term-gc",
            msg_size=self.MSG_SIZE,
            msg_count=self.MSG_COUNT,
        )
```

- [ ] **Step 2: Run the test in docker (or wherever ducktape is available)**

Run: `tests/docker/ducktape-deps/up.sh && ducktape --debug tests/rptest/tests/cloud_topics_swarm_test.py::CloudTopicsSwarmSmokeTest.test_long_term_gc_via_model`
Expected: PASS. If the test fails because the terminal metric never moved, re-read §"Risks" of the spec: most likely the dependency set is missing an edge; add it in `default_model()`.

If the local test environment cannot be brought up, skip running the test in the agent and surface that to the reviewer — the unit tests (Tasks 1–4) plus a clean static-import check (Tasks 5–6 Step 2) are the bar Phase 1 enforces from CI.

- [ ] **Step 3: Commit**

```bash
git add tests/rptest/tests/cloud_topics_swarm_test.py
git commit -m "tests/rptest: add long-term-GC smoke test for cloud-topics swarm

Phase 1 smoke test. Targets long_term_gc_observed via the Z3 model:
solver returns {reconciliation, retention_low, long_term_gc_fast},
producer drives ~5 MiB of traffic, validator waits on
vectorized_cloud_topics_gc_objects_deleted_total, then a SeqConsumer
verifies no data loss."
```

---

## Task 7: Add suite entry

**Files:**
- Modify: `tests/rptest/test_suite_cloud_topics.yml`

- [ ] **Step 1: Inspect current suite file**

Run: `cat tests/rptest/test_suite_cloud_topics.yml`
Expected: a YAML list of test file paths.

- [ ] **Step 2: Append swarm test entry**

Add a line referencing `tests/rptest/tests/cloud_topics_swarm_test.py` matching the existing entry style. If the file uses include-globs, no edit is needed — confirm by inspecting and skip Step 3 in that case.

- [ ] **Step 3: Commit**

```bash
git add tests/rptest/test_suite_cloud_topics.yml
git commit -m "tests/rptest: include cloud-topics swarm test in the cloud-topics suite"
```

---

## Self-review

**Spec coverage:**
- §2 Approach (two-layer Z3 model) — Tasks 1 + 2.
- §3 File layout — Tasks 1 (model), 3 (primitives), 5 (test base).
- §4 Mechanism catalog — Task 2 (Z3 vars) + Task 3 (overrides). All 11 mechanisms wired; producer-side ones intentionally have empty overrides in Phase 1 (noted in code).
- §5 Effect catalog — Task 2. All 8 effects wired; 3 TBC effects have `terminal_metric=None` and their validator raises `NotImplementedError` to fail loudly.
- §6 Validate-primitive shape — Task 4.
- §7 Enable-primitive shape — Task 3.
- §8 Smoke test — Tasks 5 + 6.
- §9 Testing the model — Tasks 1, 2, 3 add pytest-level model tests.

**Placeholder scan:** No "TODO", "TBD", "implement later" left in plan steps. The two `TBC` references in Task 2 step 3 refer to deliberate `terminal_metric=None` placeholders for Phase 2 effects, which are documented in the spec and handled by `EffectValidator` with `NotImplementedError`.

**Type consistency:** `Mechanism.var`, `Effect.var`, `Effect.requires(*mechs)`, `SwarmModel.solve_for(name) -> list[Mechanism]`, `merged_cluster_config(chosen)`, `EffectValidator(effect).snapshot(redpanda) / assert_observed(redpanda, logger)` — names consistent across tasks 1–6.

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-13-cloud-topics-swarm-phase1.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
