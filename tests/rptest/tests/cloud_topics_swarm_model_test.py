# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import pytest
import z3
from rptest.tests.cloud_topics_swarm_model import Effect, Mechanism, SwarmModel, default_model


def test_solver_returns_minimum_dependency_set():
    model = SwarmModel()
    m_recon = Mechanism(model, "reconciliation")
    m_ret = Mechanism(model, "retention_low")
    Mechanism(model, "unrelated")
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


def test_default_model_epoch_increment_does_not_require_reconciliation():
    """The epoch service runs independently of the reconciler."""
    model = default_model()
    chosen = {m.name for m in model.solve_for("epoch_increment_observed")}
    assert chosen == {"epoch_increment_fast"}


def test_high_partition_count_mechanism_present():
    """high_partition_count is a Phase 2 topology mechanism: not
    required by any effect, but layered on by the swarm matrix when
    its flag is true."""
    model = default_model()
    m = model._mechs["high_partition_count"]
    assert m.partition_count > 1
    # Default partition_count for other mechanisms remains 1.
    assert model._mechs["reconciliation"].partition_count == 1


def test_default_model_producer_eviction_dependencies():
    model = default_model()
    chosen = {m.name for m in model.solve_for("producer_eviction_observed")}
    assert chosen == {
        "multiple_producers",
        "idempotent_producer",
        "psm_low_producer_limit",
    }


def test_transactional_implies_idempotent():
    """transactional_producer => idempotent_producer (Kafka semantics)."""
    model = default_model()
    s = z3.Solver()
    txn = model._mechs["transactional_producer"].var
    idemp = model._mechs["idempotent_producer"].var
    s.add(z3.Implies(txn, idemp))
    s.add(txn == True)
    s.add(idemp == False)
    assert s.check() == z3.unsat


def test_merged_cluster_config_for_short_term_gc():
    from rptest.tests.cloud_topics_swarm_model import default_model
    from rptest.tests.cloud_topics_swarm_primitives import (
        attach_overrides, merged_cluster_config,
    )

    model = default_model()
    attach_overrides(model)
    chosen = model.solve_for("short_term_gc_observed")
    cfg = merged_cluster_config(chosen)

    # The three required mechanisms re-enable each loop they own.
    assert cfg["cloud_topics_disable_reconciliation_loop"] is False
    assert cfg["cloud_topics_reconciliation_min_interval"] == 2000
    assert cfg["cloud_topics_disable_level_zero_gc_for_tests"] is False
    assert cfg["cloud_topics_short_term_gc_interval"] == 2000
    assert cfg["cloud_topics_epoch_service_epoch_increment_interval"] == 5000


def test_merged_topic_config_for_short_term_gc():
    from rptest.clients.types import TopicSpec
    from rptest.tests.cloud_topics_swarm_model import default_model
    from rptest.tests.cloud_topics_swarm_primitives import (
        attach_overrides, merged_topic_config,
    )

    model = default_model()
    attach_overrides(model)
    chosen = model.solve_for("short_term_gc_observed")
    tcfg = merged_topic_config(chosen)

    # Baseline only: short_term_gc doesn't change topic config.
    assert tcfg[TopicSpec.PROPERTY_STORAGE_MODE] == TopicSpec.STORAGE_MODE_CLOUD
    assert tcfg[TopicSpec.PROPERTY_CLEANUP_POLICY] == "delete"
