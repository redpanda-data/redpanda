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
from typing import Any

import z3


@dataclass
class Mechanism:
    """A test-controllable knob (Z3 boolean variable)."""

    model: "SwarmModel"
    name: str
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
    terminal_metric: str | None
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

    def _add_implication(self, effect_var: z3.BoolRef, mech_vars: list[z3.BoolRef]) -> None:
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
        solver.minimize(
            z3.Sum([z3.If(m.var, 1, 0) for m in self._mechs.values()])
        )
        if solver.check() != z3.sat:
            raise ValueError(f"unsatisfiable: cannot enable {effect_name}")
        model = solver.model()
        chosen = []
        for name, mech in self._mechs.items():
            if model.eval(mech.var, model_completion=True) == True:
                chosen.append(mech)
        return chosen
