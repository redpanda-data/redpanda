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

from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.redpanda import CLOUD_TOPICS_CONFIG_STR
from rptest.tests.cloud_topics_swarm_model import Effect, Mechanism, SwarmModel

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
            # Default target is ~64MB. Smoke tests produce only a few MB,
            # so make L1 objects small enough that the reconciler doesn't
            # wait to accumulate a default-sized batch.
            "cloud_topics_reconciliation_max_object_size": 1024 * 1024,
        },
    )
    set_overrides(
        "retention_low",
        topic={TopicSpec.PROPERTY_RETENTION_TIME: "30000"},
    )
    set_overrides(
        "long_term_gc_fast",
        cluster={
            "cloud_topics_long_term_garbage_collection_interval": 5000,
            # Default is 1h; shrink so the GC loop can actually delete
            # newly-unreferenced L1 objects within the test deadline.
            "cloud_topics_long_term_file_deletion_delay": 1000,
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
    # multiple_producers / l1_reader_cache_evict_fast / produce_inflight_limit_low:
    # not exercised by the Phase 1 smoke test. Overrides intentionally left
    # empty so the model stays a faithful catalog. Phase 2 will wire them.


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
                f"AdminApi-based validator is required (see spec section 4 TBC)"
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
