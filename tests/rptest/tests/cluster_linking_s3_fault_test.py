# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import time

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.cluster_linking_test_base import (
    CLOUD_TOPICS_SHADOW_LINK_LOG_ALLOW_LIST,
    ShadowLinkPreAllocTestBase,
)
from rptest.util import firewall_blocked

# While the target's S3 endpoint is blackholed, the cloud-topics write
# path and the shadow-link replicators emit error-level logs that are
# expected for this scenario.
S3_FAULT_LOG_ALLOW_LIST = CLOUD_TOPICS_SHADOW_LINK_LOG_ALLOW_LIST + [
    # batcher.cc: exception path of the L0 upload
    re.compile(r".*Unexpected L0 upload error.*"),
    re.compile(r".*Unexpected batcher error.*"),
    # partition_replicator.cc: exception path of replicate_finished
    re.compile(r".*Exception during replication.*"),
    re.compile(r".*Error in fetch_and_replicate.*"),
    # cloud_io remote: upload/download retries exhausted
    re.compile(r".*backoff quota exceded.*"),
]


class ClusterLinkS3FaultTest(ShadowLinkPreAllocTestBase):
    """Reproducer for CORE-16884: a transient, silent S3 outage on the
    TARGET cluster of a shadow link wedges every partition replicator.
    In-flight L0 upload PUTs hang on established-but-dead sockets
    (http::client only bounds connection establishment with a timeout),
    upload slots and partition-data-queue units leak, and replication
    stays stalled until the kernel TCP retransmit limit (~15 min)
    fires -- regardless of when S3 connectivity returns.

    The test blackholes the target's S3 port (iptables DROP, sockets
    left alive), verifies replication actually stalled (precondition,
    so the run cannot pass vacuously), lifts the block, and requires
    replication to resume within RESUME_TIMEOUT_SEC and fully catch up
    within CATCH_UP_TIMEOUT_SEC.

    Expected on current dev: FAILS at the resume assertion with the
    "CORE-16884 stall" message. Passes once uploads observe deadlines.

    Not covered here (follow-up fix areas of the same ticket): lag
    mis-reporting during the stall (SRC-HWM=-1 / negative lag in
    rpk shadow status) and retry-liveness during the block.
    """

    LINK_NAME = "test-link"
    PARTITION_COUNT = 8
    MSG_SIZE = 1024
    # Effectively unbounded; the test stops the producer explicitly.
    MSG_COUNT = 2_000_000
    PRODUCE_RATE_BPS = 512 * 1024

    BLOCK_SEC = 120
    STALL_CHECK_DELAY_SEC = 30
    STALL_CHECK_INTERVAL_SEC = 20
    RESUME_TIMEOUT_SEC = 90
    CATCH_UP_TIMEOUT_SEC = 240

    def _hwms(self, rpk, topic: str) -> dict[int, int]:
        return {p.id: p.high_watermark for p in rpk.describe_topic(topic)}

    def _target_hwm_sum(self, topic: str) -> int:
        return sum(self._hwms(self.target_cluster_rpk, topic).values())

    @cluster(num_nodes=7, log_allow_list=S3_FAULT_LOG_ALLOW_LIST)
    @matrix(storage_mode=[TopicSpec.STORAGE_MODE_CLOUD])
    def test_replication_resumes_after_s3_blackhole(self, storage_mode: str):
        topic = TopicSpec(
            name="source-topic",
            partition_count=self.PARTITION_COUNT,
            replication_factor=3,
        )
        self.create_source_topic(topic, storage_mode)
        self.create_link(self.LINK_NAME)

        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        producer = KgoVerifierProducer(
            context=self.test_context,
            redpanda=self.source_cluster.service,
            topic=topic.name,
            msg_size=self.MSG_SIZE,
            msg_count=self.MSG_COUNT,
            rate_limit_bps=self.PRODUCE_RATE_BPS,
            custom_node=self.preallocated_nodes,
        )
        producer.start(clean=True)
        try:
            producer.wait_for_acks(1000, timeout_sec=60, backoff_sec=1)

            # Steady state: the shadow topic's HWM is advancing.
            base_hwm = self._target_hwm_sum(topic.name)
            wait_until(
                lambda: self._target_hwm_sum(topic.name) > base_hwm,
                timeout_sec=60,
                backoff_sec=2,
                err_msg="replication did not reach steady state",
            )

            s3_port = self.redpanda.si_settings.cloud_storage_api_endpoint_port
            self.logger.info(
                f"Blackholing S3 port {s3_port} on target nodes for "
                f"{self.BLOCK_SEC}s (sockets left alive)"
            )
            block_started = time.monotonic()
            stalled_hwm = None
            with firewall_blocked(self.redpanda.nodes, s3_port, kill_sockets=False):
                # Precondition: the wedge must actually trigger. If
                # replication keeps advancing the run must not proceed
                # to a vacuous pass. Compare per-partition HWMs (not
                # just the sum) so a partition dropped from one of the
                # two snapshots -- e.g. a transient NOT_LEADER during
                # `rpk describe topic` -- is self-diagnosing instead of
                # silently mimicking "HWM advanced".
                time.sleep(self.STALL_CHECK_DELAY_SEC)
                hwms_a = self._hwms(self.target_cluster_rpk, topic.name)
                time.sleep(self.STALL_CHECK_INTERVAL_SEC)
                hwms_b = self._hwms(self.target_cluster_rpk, topic.name)
                assert hwms_b == hwms_a, (
                    "reproducer precondition not met: target HWM advanced "
                    f"during the S3 block ({hwms_a} -> {hwms_b}); the "
                    "upload wedge did not trigger"
                )
                # Capture the frozen HWM here, inside the block window.
                # Sampling after unblock risks the backlog fully
                # draining before the sample lands, which would
                # plateau the resume check below and flake it red with
                # a misleading "CORE-16884 stall" message.
                stalled_hwm = sum(hwms_b.values())
                remaining = self.BLOCK_SEC - (time.monotonic() - block_started)
                if remaining > 0:
                    time.sleep(remaining)
        finally:
            producer.stop()

        self.logger.info("S3 unblocked; waiting for replication to resume")

        # (a) Replication resumes shortly after S3 recovery. On current
        # dev this fails: the stall persists until the kernel TCP
        # retransmit limit (~15 min from block start).
        wait_until(
            lambda: self._target_hwm_sum(topic.name) > stalled_hwm,
            timeout_sec=self.RESUME_TIMEOUT_SEC,
            backoff_sec=5,
            err_msg=(
                f"CORE-16884 stall: target HWM did not advance within "
                f"{self.RESUME_TIMEOUT_SEC}s of S3 recovery"
            ),
        )

        # (b) Full catch-up to the (now fixed) source HWMs.
        def caught_up() -> bool:
            src = self._hwms(self.source_cluster_rpk, topic.name)
            dst = self._hwms(self.target_cluster_rpk, topic.name)
            self.logger.debug(f"catch-up check: source={src} target={dst}")
            return src == dst

        wait_until(
            caught_up,
            timeout_sec=self.CATCH_UP_TIMEOUT_SEC,
            backoff_sec=5,
            err_msg=(
                f"target did not catch up to source within "
                f"{self.CATCH_UP_TIMEOUT_SEC}s of S3 recovery"
            ),
        )
