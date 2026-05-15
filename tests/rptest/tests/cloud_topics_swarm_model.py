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
of mechanisms that must be enabled to make the effect possible.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional, cast

# z3 ships no PEP-561 stubs, so under pyright strict mode every z3
# method call returns Unknown. Cast the module to Any so the rest of
# the file stays readable; the runtime import is unchanged.
import z3 as _z3  # type: ignore[import-untyped]

z3: Any = _z3


@dataclass
class Mechanism:
    """A test-controllable knob (Z3 boolean variable).

    ``disruption`` is an optional callable invoked when the mechanism is
    selected. It receives the running test and a stop-event that is set
    when the produce phase finishes. One-shot disruptions can ignore
    the event; looping disruptions should poll it.

    ``partition_count`` lets a mechanism request the target topic be
    created with more partitions than the default (1). When several
    mechanisms set it, the harness takes the max."""

    model: "SwarmModel"
    name: str
    cluster_config_overrides: dict[str, Any] = field(
        default_factory=lambda: cast(dict[str, Any], {})
    )
    topic_config_overrides: dict[str, str] = field(
        default_factory=lambda: cast(dict[str, str], {})
    )
    producer_overrides: dict[str, Any] = field(
        default_factory=lambda: cast(dict[str, Any], {})
    )
    needs_restart: bool = False
    disruption: Optional[Callable[..., None]] = None
    partition_count: int = 1

    def __post_init__(self) -> None:
        self.var: Any = z3.Bool(self.name)
        self.model._register_mechanism(self)


@dataclass
class Effect:
    """An observable outcome with a terminal Prometheus metric."""

    model: "SwarmModel"
    name: str
    terminal_metric: str | None
    threshold: int = 1
    deadline_sec: int = 120
    _requires: list[Mechanism] = field(
        default_factory=lambda: cast(list[Mechanism], [])
    )

    def __post_init__(self) -> None:
        self.var: Any = z3.Bool(self.name)
        self.model._register_effect(self)

    def requires(self, *mechs: Mechanism) -> None:
        self._requires.extend(mechs)
        self.model._add_implication(self.var, [m.var for m in mechs])


class SwarmModel:
    def __init__(self) -> None:
        self._mechs: dict[str, Mechanism] = {}
        self._effects: dict[str, Effect] = {}
        self._implications: list[Any] = []

    def _register_mechanism(self, m: Mechanism) -> None:
        assert m.name not in self._mechs, f"duplicate mechanism {m.name}"
        self._mechs[m.name] = m

    def _register_effect(self, e: Effect) -> None:
        assert e.name not in self._effects, f"duplicate effect {e.name}"
        self._effects[e.name] = e

    def _add_implication(self, effect_var: Any, mech_vars: list[Any]) -> None:
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
        solver.minimize(z3.Sum([z3.If(m.var, 1, 0) for m in self._mechs.values()]))
        if solver.check() != z3.sat:
            raise ValueError(f"unsatisfiable: cannot enable {effect_name}")
        model = solver.model()
        chosen: list[Mechanism] = []
        for _, mech in self._mechs.items():
            if z3.is_true(model.eval(mech.var, model_completion=True)):
                chosen.append(mech)
        return chosen


def default_model() -> SwarmModel:
    """Build the default mechanism + effect catalog for cloud-topics swarm
    testing. Catalog ordering matches the spec
    ``2026-05-13-cloud-topics-swarm-phase1-design.md``.

    Cluster/topic/producer overrides live in the primitives module so
    this module stays import-light. Other code may merge those in later
    by looking up ``model._mechs[name]``.
    """
    m = SwarmModel()

    # --- Mechanisms (L0-focused scope) ---
    reconciliation = Mechanism(m, "reconciliation", needs_restart=True)
    short_term_gc_fast = Mechanism(m, "short_term_gc_fast", needs_restart=True)
    compaction = Mechanism(m, "compaction", needs_restart=True)
    epoch_increment_fast = Mechanism(m, "epoch_increment_fast", needs_restart=True)
    transactional_producer = Mechanism(m, "transactional_producer")
    idempotent_producer = Mechanism(m, "idempotent_producer")
    multiple_producers = Mechanism(m, "multiple_producers")
    produce_inflight_limit_low = Mechanism(
        m, "produce_inflight_limit_low", needs_restart=True
    )
    psm_low_producer_limit = Mechanism(m, "psm_low_producer_limit")

    # Disruption mechanisms: fire a runtime action when the mechanism is
    # selected. Not required by any effect; layered on as "spice" by the
    # swarm test. Each one's start time is picked independently by the
    # harness from a seeded RNG. Constructed for the side effect of
    # registration in the model; the test addresses them by name.
    Mechanism(m, "inject_broker_restart")
    Mechanism(m, "inject_leadership_transfer")
    Mechanism(m, "inject_minio_block")
    Mechanism(m, "inject_node_maintenance")
    Mechanism(m, "inject_partition_movement")
    Mechanism(m, "inject_node_decommission")

    # Bumps the target topic's partition count from 1 to a very high
    # value (1000). Exercises multi-partition cloud-topic upload /
    # reconcile paths and gives the looping leadership-transfer
    # disruption a wide partition pool to randomise over.
    Mechanism(m, "high_partition_count", partition_count=1000)

    # transactional_producer => idempotent_producer
    m._implications.append(
        z3.Implies(transactional_producer.var, idempotent_producer.var)
    )

    # --- Effects (L0-focused scope) ---
    # Effects no longer carry terminal metrics: validation is done via
    # KgoVerifier content checks so the harness can tolerate restarts
    # (metric counters reset on restart, content does not).
    l1_upload = Effect(m, "l1_upload_observed", terminal_metric=None)
    l1_upload.requires(reconciliation)

    short_term_gc = Effect(m, "short_term_gc_observed", terminal_metric=None)
    short_term_gc.requires(reconciliation, short_term_gc_fast, epoch_increment_fast)

    compaction_eff = Effect(m, "compaction_observed", terminal_metric=None)
    compaction_eff.requires(reconciliation, compaction)

    epoch_increment = Effect(m, "epoch_increment_observed", terminal_metric=None)
    epoch_increment.requires(epoch_increment_fast)

    producer_eviction = Effect(m, "producer_eviction_observed", terminal_metric=None)
    producer_eviction.requires(
        multiple_producers,
        idempotent_producer,
        psm_low_producer_limit,
    )

    inflight_backpressure = Effect(
        m,
        "inflight_backpressure_observed",
        terminal_metric=None,
    )
    inflight_backpressure.requires(produce_inflight_limit_low)

    return m
