# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import pytest
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
    with pytest.raises(KeyError):
        model.solve_for("does_not_exist")
