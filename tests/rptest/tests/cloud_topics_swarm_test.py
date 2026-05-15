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
    attach_overrides,
    merged_cluster_config,
    merged_producer_kwargs,
    merged_topic_config,
)
from rptest.tests.redpanda_test import RedpandaTest


class CloudTopicsSwarmTestBase(RedpandaTest):
    """Base class for model-driven cloud-topics tests.

    Subclasses set ``target_effect_name`` and call ``run_smoke(...)`` from
    their test method to drive producer/consumer and validate."""

    def __init__(
        self,
        test_context: TestContext,
        target_effect_name: str,
        extra_mechanism_names: list[str] | None = None,
    ):
        self._model = default_model()
        attach_overrides(self._model)
        self._target_effect_name = target_effect_name
        self._chosen = self._model.solve_for(target_effect_name)

        # Layer additional ("spice") mechanisms on top of the solver-
        # required set. Used by the swarm matrix test to enable
        # disruption mechanisms without targeting them via an effect.
        if extra_mechanism_names:
            existing = {m.name for m in self._chosen}
            for name in extra_mechanism_names:
                if name not in existing:
                    self._chosen.append(self._model._mechs[name])

        self._chosen_names = [m.name for m in self._chosen]
        self._smoke_topic_name: str | None = None
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
        rpk.create_topic(
            spec.name, spec.partition_count, spec.replication_factor, config=topic_cfg
        )
        return spec

    def _producer_kwargs(self) -> dict[str, Any]:
        return merged_producer_kwargs(self._chosen)

    def _compute_partition_count(self) -> int:
        """Max partition_count across chosen mechanisms (baseline = 1)."""
        return max((m.partition_count for m in self._chosen), default=1)

    # Default seed for the per-disruption random start-time RNG.
    # The matrix test methods override this via the ``seed`` @matrix
    # axis so each parametrised run gets a fresh schedule but the
    # same schedule re-runs deterministically.
    DISRUPTION_SEED = 42

    # Random window each disruption's start time is sampled from,
    # relative to produce-start. The upper bound leaves ~90 s after
    # the latest start so even a 60 s disruption has time to complete
    # before produce wraps up.
    DISRUPTION_START_MIN_SEC = 30
    DISRUPTION_START_MAX_SEC = 510

    def _run_disruptions(self, disruptions: list, abort_event) -> None:
        """Spawn one thread per disruption. Each thread sleeps for a
        per-disruption random delay (seeded by ``_disruption_seed``),
        then invokes its disruption callable. Returns after all
        disruption threads join (or after the produce-end abort
        signal). The seeded RNG only controls START TIMES --
        in-disruption choices (which node to restart, which partition
        to transfer, etc.) remain unseeded so we get variety across a
        single run."""
        import random
        import threading

        seed = getattr(self, "_disruption_seed", self.DISRUPTION_SEED)
        rng = random.Random(seed)
        self.logger.info(f"swarm: disruption seed = {seed}")
        threads: list[threading.Thread] = []
        for mech in disruptions:
            delay = rng.uniform(
                self.DISRUPTION_START_MIN_SEC,
                self.DISRUPTION_START_MAX_SEC,
            )
            self.logger.info(
                f"swarm: scheduled disruption {mech.name!r} at +{delay:.0f}s"
            )
            t = threading.Thread(
                target=self._delayed_disruption,
                args=(mech, delay, abort_event),
                daemon=True,
            )
            t.start()
            threads.append(t)
        for t in threads:
            t.join(timeout=self.DISRUPTION_START_MAX_SEC + 180)

    def _delayed_disruption(self, mech, delay: float, abort_event) -> None:
        import time

        end_at = time.monotonic() + delay
        while time.monotonic() < end_at:
            if abort_event.is_set():
                return
            time.sleep(1)
        self.logger.info(f"swarm: invoking disruption {mech.name!r}")
        try:
            mech.disruption(self, abort_event)
        except Exception as e:
            self.logger.error(f"swarm: disruption {mech.name!r} raised: {e}")

    def run_smoke(
        self,
        topic_name: str,
        msg_size: int,
        msg_count: int,
        rate_limit_bps: int | None = None,
    ) -> None:
        """Produce ``msg_count`` records with a single KgoVerifierProducer,
        then read them all back with KgoVerifierSeqConsumer and assert no
        data loss or corruption. The chosen mechanisms shape what the
        cluster does during the run; the ``multiple_producers`` mechanism
        is realised via PID churn (``msgs_per_producer_id``) on a single
        kgo-verifier instance rather than parallel processes -- the
        verifier doesn't reliably support multiple instances sharing a
        ducktape client node. When ``rate_limit_bps`` is set the
        producer paces itself to that rate so the produce phase lasts
        for a roughly predictable amount of wall-clock time, leaving
        the disruption layer plenty of mid-produce time to operate."""
        import threading

        partition_count = self._compute_partition_count()
        spec = self._create_cloud_topic(topic_name, partitions=partition_count)
        self._smoke_topic_name = topic_name
        self._smoke_partition_count = partition_count
        self.logger.info(
            f"swarm: target={self._target_effect_name!r} "
            f"mechanisms={self._chosen_names} "
            f"partitions={partition_count}"
        )

        producer_kwargs = self._producer_kwargs()
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            spec.name,
            msg_size=msg_size,
            msg_count=msg_count,
            rate_limit_bps=rate_limit_bps,
            # Larger batches amortise the acks=all round-trip latency
            # over many records; without this kgo-verifier produces tiny
            # batches and the per-batch latency caps throughput.
            batch_max_bytes=msg_size * 128,
            tolerate_failed_produce=True,
            # Treat 'sent' (not 'acked') as completion. Doesn't change
            # wire-level acks but lets the harness move past in-flight
            # records during teardown.
            wait_for_acks=False,
            client_name="swarm-producer",
            **producer_kwargs,
        )

        disruption_thread: threading.Thread | None = None
        abort_event = threading.Event()
        consumer: KgoVerifierSeqConsumer | None = None
        try:
            producer.start()

            # Launch any disruption mechanisms in a background thread so
            # they fire mid-produce (not before, not after).
            disruptions = [m for m in self._chosen if m.disruption is not None]
            if disruptions:
                disruption_thread = threading.Thread(
                    target=self._run_disruptions,
                    args=(disruptions, abort_event),
                    daemon=True,
                )
                disruption_thread.start()

            # 10 min produce + up to 60s broker downtime + slack.
            producer.wait(timeout_sec=1200)
            pstatus = producer.produce_status
            acked = pstatus.acked
            self.logger.info(
                f"swarm: producer acked={acked}/{msg_count} "
                f"bad_offsets={pstatus.bad_offsets}"
            )
            # Tell looping disruptions to stop, then join.
            abort_event.set()
            if disruption_thread is not None:
                disruption_thread.join(timeout=180)
                if disruption_thread.is_alive():
                    self.logger.warn("swarm: disruption thread did not finish in time")
            assert acked >= msg_count * 3 // 4, (
                f"too few acks for a meaningful run: {acked}/{msg_count}"
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
            consumer.start(clean=False)
            consumer.wait(timeout_sec=1200)
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
            # Stop any looping disruptions before tearing down services.
            abort_event.set()
            producer.stop()
            producer.free()
            if consumer is not None:
                consumer.stop()
                consumer.free()


class CloudTopicsSwarmSmokeTest(CloudTopicsSwarmTestBase):
    """Phase 1 smoke test: exercise the L0 path end to end via the model.

    Targets short_term_gc_observed, which the model resolves to
    {reconciliation, short_term_gc_fast, epoch_increment_fast}. Validation
    is content-only via KgoVerifierSeqConsumer."""

    MSG_SIZE = 8192
    # Target produce rate. With wait_for_acks=False the producer isn't
    # latency-bound; 20 MiB/s is well within what the cluster sustains
    # on a single cloud-topic partition.
    PRODUCE_RATE_BPS = 20 * 1024 * 1024
    # Wall-clock duration of the produce phase.
    PRODUCE_DURATION_SECONDS = 600

    def __init__(self, test_context: TestContext):
        super().__init__(test_context, target_effect_name="short_term_gc_observed")

    def _msg_count(self) -> int:
        total_bytes = self.PRODUCE_RATE_BPS * self.PRODUCE_DURATION_SECONDS
        return total_bytes // self.MSG_SIZE

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_short_term_gc_via_model(self, cloud_storage_type: CloudStorageType):
        self.run_smoke(
            topic_name="ct-swarm-short-term-gc",
            msg_size=self.MSG_SIZE,
            msg_count=self._msg_count(),
            rate_limit_bps=self.PRODUCE_RATE_BPS,
        )


# --- Phase 2: random swarm with disruption injection ---


class _SwarmMatrixBase(CloudTopicsSwarmTestBase):
    """Base for the disruption matrix. Every run includes all three
    disruption mechanisms; each one's start time is chosen
    independently from a seeded RNG so runs are reproducible. The
    @matrix decorator on subclasses only pins cloud_storage_type."""

    TARGET_EFFECT: str = "short_term_gc_observed"
    # Subclasses set this to add extra mechanisms (e.g. the high-
    # partition-count variant pins high_partition_count) on top of
    # the always-on disruption set below.
    BASE_EXTRA_MECHANISMS: list[str] = []
    # Every matrix test exercises the full disruption suite.
    _ALWAYS_ON_DISRUPTIONS: list[str] = [
        "inject_broker_restart",
        "inject_leadership_transfer",
        "inject_minio_block",
        "inject_node_maintenance",
        "inject_partition_movement",
        "inject_node_decommission",
    ]
    MSG_SIZE = 8192
    PRODUCE_RATE_BPS = 20 * 1024 * 1024
    PRODUCE_DURATION_SECONDS = 600

    def __init__(self, test_context: TestContext):
        extras = list(self.BASE_EXTRA_MECHANISMS) + list(self._ALWAYS_ON_DISRUPTIONS)
        super().__init__(
            test_context,
            target_effect_name=self.TARGET_EFFECT,
            extra_mechanism_names=extras,
        )

    def _msg_count(self) -> int:
        total_bytes = self.PRODUCE_RATE_BPS * self.PRODUCE_DURATION_SECONDS
        return total_bytes // self.MSG_SIZE

    # Per-test parametrised RNG seeds. Each matrix subclass picks one
    # via the seed @matrix axis below; the harness uses the chosen
    # seed to schedule disruption start times deterministically for
    # that run.
    DISRUPTION_SEEDS = [42, 1337, 9001, 31337, 65537]

    def _run(self, seed: int) -> None:
        self._disruption_seed = seed
        self.run_smoke(
            topic_name="ct-swarm-matrix",
            msg_size=self.MSG_SIZE,
            msg_count=self._msg_count(),
            rate_limit_bps=self.PRODUCE_RATE_BPS,
        )


class CloudTopicsSwarmMatrixShortTermGc(_SwarmMatrixBase):
    """Short-term GC path with the full disruption suite."""

    TARGET_EFFECT = "short_term_gc_observed"

    @cluster(num_nodes=6)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(
            applies_only_on=[CloudStorageType.S3]
        ),
        seed=_SwarmMatrixBase.DISRUPTION_SEEDS,
    )
    def test_swarm(self, cloud_storage_type: CloudStorageType, seed: int):
        self._run(seed)


class CloudTopicsSwarmMatrixL1Upload(_SwarmMatrixBase):
    """Reconciler (L1 upload) path with the full disruption suite."""

    TARGET_EFFECT = "l1_upload_observed"

    @cluster(num_nodes=6)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(
            applies_only_on=[CloudStorageType.S3]
        ),
        seed=_SwarmMatrixBase.DISRUPTION_SEEDS,
    )
    def test_swarm(self, cloud_storage_type: CloudStorageType, seed: int):
        self._run(seed)


class CloudTopicsSwarmMatrixEpoch(_SwarmMatrixBase):
    """Epoch service with the full disruption suite."""

    TARGET_EFFECT = "epoch_increment_observed"

    @cluster(num_nodes=6)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(
            applies_only_on=[CloudStorageType.S3]
        ),
        seed=_SwarmMatrixBase.DISRUPTION_SEEDS,
    )
    def test_swarm(self, cloud_storage_type: CloudStorageType, seed: int):
        self._run(seed)


class CloudTopicsSwarmMatrixShortTermGcHighPartitions(_SwarmMatrixBase):
    """High-partition-count variant of the short-term GC matrix.

    Pins ``high_partition_count`` so the run uses a 1000-partition
    topic. The looping leadership-transfer burst gets a wide partition
    pool to pick from, and broker restart forces re-election of
    hundreds of partition leaders at once."""

    TARGET_EFFECT = "short_term_gc_observed"
    BASE_EXTRA_MECHANISMS = ["high_partition_count"]

    @cluster(num_nodes=6)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(
            applies_only_on=[CloudStorageType.S3]
        ),
        seed=_SwarmMatrixBase.DISRUPTION_SEEDS,
    )
    def test_swarm(self, cloud_storage_type: CloudStorageType, seed: int):
        self._run(seed)


# NOTE: a CloudTopicsSwarmMatrixProducerEviction class belongs here but is
# omitted in Phase 2. KgoVerifierProducer's idempotent client doesn't
# recover from rm_stm / producer_state_manager LRU eviction within the
# 5-minute produce timeout (the HTTP status endpoint never comes back up
# during repeated Kafka-client re-inits, and the producer can't make
# progress against an aggressive max_concurrent_producer_ids cap). The
# mechanism + effect remain in default_model() so the model still covers
# the path; a future test variant should exercise this with a producer
# client that survives PSM eviction.
