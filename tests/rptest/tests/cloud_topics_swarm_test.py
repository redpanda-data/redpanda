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

    Subclasses set ``target_effect_name`` and call ``run_smoke(...)`` from
    their test method to drive producer/consumer and validate."""

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
