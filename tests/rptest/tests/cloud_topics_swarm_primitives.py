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
            # Default target is ~64MB. Smoke tests produce only a few MB,
            # so make L1 objects small enough that the reconciler doesn't
            # wait to accumulate a default-sized batch.
            "cloud_topics_reconciliation_max_object_size": 1024 * 1024,
        },
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
        producer={},
    )
    set_overrides(
        "multiple_producers",
        # KgoVerifierProducer cycles to a new producer ID every
        # msgs_per_producer_id messages. Pair with a small
        # max_concurrent_producer_ids via psm_low_producer_limit to drive
        # rm_stm / producer_state_manager into LRU eviction. 5000 keeps
        # PID churn frequent enough to exercise eviction while letting
        # each kgo-verifier client cycle run long enough for the HTTP
        # status endpoint to be polled successfully between teardowns.
        producer={"msgs_per_producer_id": 5000},
    )
    set_overrides(
        "psm_low_producer_limit",
        # Default is 100000. With msgs_per_producer_id=5000 and a 100MiB
        # payload, the kgo-verifier cycles through ~20 PIDs per run.
        # cap=8 lands ~12 evictions over the run -- enough to exercise
        # the eviction path without stalling the idempotent producer.
        cluster={"max_concurrent_producer_ids": 8},
    )

    # --- Disruption mechanisms (Phase 2) ---
    model._mechs["inject_broker_restart"].disruption = _disrupt_broker_restart
    model._mechs["inject_leadership_transfer"].disruption = (
        _disrupt_leadership_transfer
    )
    model._mechs["inject_minio_block"].disruption = _disrupt_minio_block

    # produce_inflight_limit_low: not exercised by Phase 1 / 2 smoke
    # tests. Overrides intentionally left empty so the model stays a
    # faithful catalog.


# --- Disruption implementations ---

# MinIO listens on port 9000 in the docker test environment. Blocking
# OUTPUT to this port from a broker simulates a network partition between
# that broker and the object store while leaving Kafka traffic intact.
_MINIO_PORT = 9000
_MINIO_BLOCK_SECONDS = 15


def _disrupt_broker_restart(test) -> None:
    """Restart one randomly-chosen broker and wait for it to come back."""
    import random
    node = random.choice(test.redpanda.nodes)
    test.logger.info(f"swarm: disrupt: restarting broker {node.name}")
    test.redpanda.restart_nodes([node], start_timeout=60, stop_timeout=60)
    test.logger.info(f"swarm: disrupt: restart of {node.name} complete")


def _disrupt_leadership_transfer(test) -> None:
    """Force a leadership transfer on the target topic's partition 0."""
    from rptest.services.admin import Admin
    admin = Admin(test.redpanda)
    topic = test._smoke_topic_name
    test.logger.info(
        f"swarm: disrupt: transferring leadership of {topic}/0"
    )
    admin.partition_transfer_leadership("kafka", topic, 0)
    test.logger.info(f"swarm: disrupt: leadership transfer of {topic}/0 issued")


def _disrupt_minio_block(test) -> None:
    """Block outbound traffic to MinIO from one broker for a fixed window."""
    import random
    import time
    node = random.choice(test.redpanda.nodes)
    rule = f"iptables -A OUTPUT -p tcp --destination-port {_MINIO_PORT} -j DROP"
    undo = f"iptables -D OUTPUT -p tcp --destination-port {_MINIO_PORT} -j DROP"
    test.logger.info(
        f"swarm: disrupt: blocking MinIO traffic on {node.name} for "
        f"{_MINIO_BLOCK_SECONDS}s"
    )
    try:
        node.account.ssh(rule)
        time.sleep(_MINIO_BLOCK_SECONDS)
    finally:
        try:
            node.account.ssh(undo)
        except Exception as e:
            test.logger.warn(f"swarm: disrupt: failed to undo block on {node.name}: {e}")
    test.logger.info(f"swarm: disrupt: MinIO traffic restored on {node.name}")


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


