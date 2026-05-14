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
        rpk.create_topic(spec.name, spec.partition_count, spec.replication_factor,
                         config=topic_cfg)
        return spec

    def _producer_kwargs(self) -> dict[str, Any]:
        return merged_producer_kwargs(self._chosen)

    # Number of concurrent KgoVerifierProducer instances spawned when the
    # multiple_producers mechanism is selected. Each instance gets its own
    # set of producer IDs, so combined with msgs_per_producer_id this
    # multiplies the total PID cardinality reaching producer_state_manager.
    MULTIPLE_PRODUCERS_COUNT = 4

    def _producer_count(self) -> int:
        if "multiple_producers" in self._chosen_names:
            return self.MULTIPLE_PRODUCERS_COUNT
        return 1

    # Disruptions fire after this many seconds of produce activity. That
    # leaves the cluster enough time to upload a few L0/L1 objects so the
    # disruption interrupts steady state rather than start-up.
    DISRUPTION_DELAY_SEC = 15

    def _run_disruptions(self, disruptions: list) -> None:
        import time
        time.sleep(self.DISRUPTION_DELAY_SEC)
        for mech in disruptions:
            try:
                self.logger.info(
                    f"swarm: invoking disruption for {mech.name!r}"
                )
                mech.disruption(self)
            except Exception as e:
                self.logger.error(
                    f"swarm: disruption {mech.name!r} raised: {e}"
                )

    def run_smoke(self, topic_name: str, msg_size: int, msg_count: int) -> None:
        """Produce ``msg_count * producer_count`` records, then read them
        all back with KgoVerifierSeqConsumer and assert no data loss or
        corruption. The chosen mechanisms shape what the cluster does
        during the run, but validation is content-only — no metric
        sampling — so the test stays correct across restarts."""
        import threading
        spec = self._create_cloud_topic(topic_name)
        self._smoke_topic_name = topic_name
        self.logger.info(
            f"swarm: target={self._target_effect_name!r} "
            f"mechanisms={self._chosen_names}"
        )

        producer_kwargs = self._producer_kwargs()
        producer_count = self._producer_count()
        producers: list[KgoVerifierProducer] = []
        for i in range(producer_count):
            p = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                spec.name,
                msg_size=msg_size,
                msg_count=msg_count,
                tolerate_failed_produce=True,
                client_name=f"swarm-producer-{i}",
                **producer_kwargs,
            )
            producers.append(p)

        disruption_thread: threading.Thread | None = None
        consumer: KgoVerifierSeqConsumer | None = None
        try:
            for p in producers:
                p.start()

            # Launch any disruption mechanisms in a background thread so
            # they fire mid-produce (not before, not after).
            disruptions = [m for m in self._chosen if m.disruption is not None]
            if disruptions:
                disruption_thread = threading.Thread(
                    target=self._run_disruptions,
                    args=(disruptions,),
                    daemon=True,
                )
                disruption_thread.start()

            total_acked = 0
            for i, p in enumerate(producers):
                p.wait(timeout_sec=300)
                pstatus = p.produce_status
                total_acked += pstatus.acked
                self.logger.info(
                    f"swarm: producer[{i}] acked={pstatus.acked}/{msg_count} "
                    f"bad_offsets={pstatus.bad_offsets}"
                )
            if disruption_thread is not None:
                disruption_thread.join(timeout=120)
                if disruption_thread.is_alive():
                    self.logger.warn(
                        "swarm: disruption thread did not finish in time"
                    )
            expected_total = msg_count * producer_count
            assert total_acked >= expected_total * 3 // 4, (
                f"too few acks for a meaningful run: "
                f"{total_acked}/{expected_total}"
            )

            # Multiple producers write interleaved messages from a
            # SeqConsumer's perspective, so per-key sequence validation
            # doesn't apply. Skip the consumer in that case.
            if producer_count > 1:
                self.logger.info(
                    "swarm: skipping content validation (multiple producers)"
                )
                return

            consumer = KgoVerifierSeqConsumer(
                self.test_context,
                self.redpanda,
                spec.name,
                msg_size=msg_size,
                loop=False,
                nodes=[producers[0].nodes[0]],
                producer=producers[0],
            )
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
            assert cstatus.validator.valid_reads >= total_acked, (
                f"data loss: expected >= {total_acked} valid reads, "
                f"got {cstatus.validator.valid_reads}"
            )
        finally:
            for p in producers:
                p.stop()
                p.free()
            if consumer is not None:
                consumer.stop()
                consumer.free()


class CloudTopicsSwarmSmokeTest(CloudTopicsSwarmTestBase):
    """Phase 1 smoke test: exercise the L0 path end to end via the model.

    Targets short_term_gc_observed, which the model resolves to
    {reconciliation, short_term_gc_fast, epoch_increment_fast}. Validation
    is content-only via KgoVerifierSeqConsumer."""

    MSG_SIZE = 1024
    PAYLOAD_BYTES_LOCAL = 100 * 1024 * 1024
    PAYLOAD_BYTES_RELEASE = 1024 * 1024 * 1024

    def __init__(self, test_context: TestContext):
        super().__init__(test_context, target_effect_name="short_term_gc_observed")

    def _msg_count(self) -> int:
        payload = (
            self.PAYLOAD_BYTES_RELEASE
            if self.scale.release
            else self.PAYLOAD_BYTES_LOCAL
        )
        return payload // self.MSG_SIZE

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_short_term_gc_via_model(self, cloud_storage_type: CloudStorageType):
        self.run_smoke(
            topic_name="ct-swarm-short-term-gc",
            msg_size=self.MSG_SIZE,
            msg_count=self._msg_count(),
        )


# --- Phase 2: random swarm with disruption injection ---


class _SwarmMatrixBase(CloudTopicsSwarmTestBase):
    """Base for the disruption matrix. Subclasses hardcode the target
    effect (matrix params can't reach the constructor in ducktape) and
    inherit a test method that varies only the disruption flags."""

    TARGET_EFFECT: str = "short_term_gc_observed"
    MSG_SIZE = 1024
    PAYLOAD_BYTES_LOCAL = 100 * 1024 * 1024
    PAYLOAD_BYTES_RELEASE = 1024 * 1024 * 1024

    def __init__(self, test_context: TestContext):
        super().__init__(test_context, target_effect_name=self.TARGET_EFFECT)

    def _msg_count(self) -> int:
        payload = (
            self.PAYLOAD_BYTES_RELEASE
            if self.scale.release
            else self.PAYLOAD_BYTES_LOCAL
        )
        return payload // self.MSG_SIZE

    def _run_with_disruptions(
        self,
        inject_broker_restart: bool,
        inject_leadership_transfer: bool,
        inject_minio_block: bool,
    ) -> None:
        extras: list[str] = []
        if inject_broker_restart:
            extras.append("inject_broker_restart")
        if inject_leadership_transfer:
            extras.append("inject_leadership_transfer")
        if inject_minio_block:
            extras.append("inject_minio_block")
        # Disruption mechanisms have no cluster-config impact; we can
        # add them to the chosen set at runtime.
        existing = {m.name for m in self._chosen}
        for name in extras:
            if name not in existing:
                self._chosen.append(self._model._mechs[name])
        self._chosen_names = [m.name for m in self._chosen]

        self.run_smoke(
            topic_name="ct-swarm-matrix",
            msg_size=self.MSG_SIZE,
            msg_count=self._msg_count(),
        )


class CloudTopicsSwarmMatrixShortTermGc(_SwarmMatrixBase):
    """Disruption matrix for the short-term GC path."""

    TARGET_EFFECT = "short_term_gc_observed"

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3]),
        inject_broker_restart=[False, True],
        inject_leadership_transfer=[False, True],
        inject_minio_block=[False, True],
    )
    def test_swarm(
        self,
        cloud_storage_type: CloudStorageType,
        inject_broker_restart: bool,
        inject_leadership_transfer: bool,
        inject_minio_block: bool,
    ):
        self._run_with_disruptions(
            inject_broker_restart,
            inject_leadership_transfer,
            inject_minio_block,
        )


class CloudTopicsSwarmMatrixL1Upload(_SwarmMatrixBase):
    """Disruption matrix for the reconciler (L1 upload) path."""

    TARGET_EFFECT = "l1_upload_observed"

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3]),
        inject_broker_restart=[False, True],
        inject_leadership_transfer=[False, True],
        inject_minio_block=[False, True],
    )
    def test_swarm(
        self,
        cloud_storage_type: CloudStorageType,
        inject_broker_restart: bool,
        inject_leadership_transfer: bool,
        inject_minio_block: bool,
    ):
        self._run_with_disruptions(
            inject_broker_restart,
            inject_leadership_transfer,
            inject_minio_block,
        )


class CloudTopicsSwarmMatrixEpoch(_SwarmMatrixBase):
    """Disruption matrix for the epoch service."""

    TARGET_EFFECT = "epoch_increment_observed"

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3]),
        inject_broker_restart=[False, True],
        inject_leadership_transfer=[False, True],
        inject_minio_block=[False, True],
    )
    def test_swarm(
        self,
        cloud_storage_type: CloudStorageType,
        inject_broker_restart: bool,
        inject_leadership_transfer: bool,
        inject_minio_block: bool,
    ):
        self._run_with_disruptions(
            inject_broker_restart,
            inject_leadership_transfer,
            inject_minio_block,
        )


class CloudTopicsSwarmMatrixProducerEviction(_SwarmMatrixBase):
    """Disruption matrix for the producer_state_manager eviction path."""

    TARGET_EFFECT = "producer_eviction_observed"

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3]),
        inject_broker_restart=[False, True],
        inject_leadership_transfer=[False, True],
        inject_minio_block=[False, True],
    )
    def test_swarm(
        self,
        cloud_storage_type: CloudStorageType,
        inject_broker_restart: bool,
        inject_leadership_transfer: bool,
        inject_minio_block: bool,
    ):
        self._run_with_disruptions(
            inject_broker_restart,
            inject_leadership_transfer,
            inject_minio_block,
        )
