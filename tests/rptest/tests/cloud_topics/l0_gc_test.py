# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import TypeAlias, cast


from rptest.clients.admin.v2 import Admin, l0_pb, ntp_pb
from rptest.context.cloud_storage import CloudStorageType
from rptest.services.kgo_repeater_service import repeater_traffic
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from connectrpc.errors import ConnectError
from ducktape.cluster.cluster import ClusterNode
from ducktape.errors import TimeoutError
from ducktape.tests.test import TestContext
from rptest.archival.s3_client import S3Client
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.admin import Admin as RedpandaAdmin
from rptest.services.redpanda import (
    SISettings,
    get_cloud_storage_type,
    CLOUD_TOPICS_CONFIG_STR,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception, wait_until_with_progress_check
from rptest.utils.mode_checks import is_debug_mode


class CloudTopicsL0GCTestBase(RedpandaTest):
    def __init__(
        self,
        test_context: TestContext,
        housekeeping_interval_ms: int | None = None,
        extra_rp_conf_overrides: dict[str, int | bool] | None = None,
    ):
        self.test_context = test_context
        si_settings = SISettings(
            test_context=test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            cloud_storage_housekeeping_interval_ms=housekeeping_interval_ms
            if housekeeping_interval_ms is not None
            else 1000,
            fast_uploads=True,
        )
        extra_rp_conf = {
            CLOUD_TOPICS_CONFIG_STR: True,
            "cloud_topics_reconciliation_min_interval": 2000,
            "cloud_topics_reconciliation_max_interval": 2000,
            "cloud_topics_epoch_service_epoch_increment_interval": 5000,
            "cloud_topics_epoch_service_local_epoch_cache_duration": 5000,
            "cloud_topics_short_term_gc_minimum_object_age": 10000,
            "cloud_topics_short_term_gc_interval": 2000,
            "cloud_topics_short_term_gc_backoff_interval": 10000,
            "cloud_topics_gc_health_check_interval": 2000,
        }
        if extra_rp_conf_overrides:
            extra_rp_conf.update(extra_rp_conf_overrides)
        self._epoch_increment_interval_ms: int = extra_rp_conf[
            "cloud_topics_epoch_service_epoch_increment_interval"
        ]
        super().__init__(
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
            si_settings=si_settings,
        )

    def create_topics(self, topics: list[TopicSpec]):
        rpk = RpkTool(self.redpanda)
        for spec in topics:
            rpk.create_topic(
                spec.name,
                spec.partition_count,
                spec.replication_factor,
                config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD},
            )

    def get_num_objects_deleted(self, nodes: list[ClusterNode] | None = None):
        samples = self.redpanda.metrics_sample(
            "vectorized_cloud_topics_l0_gc_objects_deleted_total",
            nodes=nodes,
        )
        self.logger.info(samples)
        if samples is not None and samples.samples:
            deleted_total = int(sum(s.value for s in samples.samples))
            self.logger.debug(f"{deleted_total=}")
            return deleted_total
        return 0

    def get_bytes_deleted(self, nodes: list[ClusterNode] | None = None) -> int:
        return self._get_metric_total(
            "vectorized_cloud_topics_l0_gc_bytes_deleted_total", nodes=nodes
        )

    def _get_metric_total(
        self, name: str, nodes: list[ClusterNode] | None = None
    ) -> int:
        samples = self.redpanda.metrics_sample(name, nodes=nodes)
        if samples is not None and samples.samples:
            return int(sum(s.value for s in samples.samples))
        return 0

    def _get_metric_values(
        self, name: str, nodes: list[ClusterNode] | None = None
    ) -> list[int]:
        samples = self.redpanda.metrics_sample(name, nodes=nodes)
        if samples is not None and samples.samples:
            return [int(s.value) for s in samples.samples]
        return []

    def _get_metric_max(self, name: str, nodes: list[ClusterNode] | None = None) -> int:
        samples = self.redpanda.metrics_sample(name, nodes=nodes)
        if samples is not None and samples.samples:
            return int(max(s.value for s in samples.samples))
        return 0

    def produce_some(
        self,
        topics: list[str],
        min_runtime_s: int | None = None,
    ):
        """Run a repeater for long enough that the batcher spreads L0
        objects across multiple epoch intervals, which is required for
        GC to find epoch-eligible objects to collect. Make sure the
        repeater makes some progress during that time.

        :param min_runtime_s: Minimum time (seconds) to keep the
            repeater running.  Defaults to 6x the configured epoch
            increment interval.
        """
        epoch_interval_s = self._epoch_increment_interval_ms // 1000
        if min_runtime_s is None:
            min_runtime_s = 6 * epoch_interval_s

        with repeater_traffic(
            context=self.test_context,
            redpanda=self.redpanda,
            topics=topics,
            msg_size=1024,
            rate_limit_bps=2 * 1024 * 1024,
            workers=1,
        ) as repeater:
            start = time.monotonic()

            def produced_across_epochs():
                produced, _ = repeater.total_messages()
                elapsed = time.monotonic() - start
                return produced > 0 and elapsed >= min_runtime_s

            wait_until_with_progress_check(
                lambda: repeater.total_messages()[0],
                produced_across_epochs,
                timeout_sec=int(min_runtime_s) + 60,
                backoff_sec=2,
                progress_sec=epoch_interval_s * 3,
                logger=self.logger,
            )


class CloudTopicsL0GCTest(CloudTopicsL0GCTestBase):
    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_l0_gc(self, cloud_storage_type: CloudStorageType):
        self.topics = [TopicSpec(partition_count=2)]
        self.create_topics(self.topics)
        self.produce_some(topics=[spec.name for spec in self.topics])

        # TODO: we are only checking that deletes are happening here (and should
        # also be happening in parallel with the repeater's fetch/produce
        # workload), but we do want to add tests that check constraints
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=5,
            retry_on_exc=True,
        )

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_idle_housekeeping(self, cloud_storage_type: CloudStorageType):
        self.topics = [
            TopicSpec(partition_count=2),
            TopicSpec(
                # more partitions to show per-partition epoch bump
                partition_count=4
            ),
        ]
        self.create_topics(self.topics)
        self.logger.debug(
            "Produce to only one topic, so only the housekeeping loop can progress the max collectible epoch"
        )
        self.produce_some(topics=[self.topics[0].name])

        self.logger.debug(
            f"GC should still make progress because the housekeeping loop kicks in and bumps the epoch on each {self.topics[1].name} partition"
        )
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=5,
            retry_on_exc=True,
        )


GcStatus: TypeAlias = l0_pb.Status
StatusReport: TypeAlias = dict[int, dict[int, GcStatus] | str]
EpochInfo: TypeAlias = l0_pb.EpochInfo
EpochReport: TypeAlias = dict[str, dict[int, l0_pb.EpochInfo | str]]


class CloudTopicsL0GCAdminBase(CloudTopicsL0GCTestBase):
    """Shared admin API helpers for L0 GC tests."""

    @property
    def l0_client(self):
        return Admin(self.redpanda).l0()

    def gc_get_status(self, node: int | None = None) -> StatusReport:
        response = self.l0_client.get_status(l0_pb.GetStatusRequest(node_id=node))
        assert response is not None, "GetStatusResponse should not be None"
        expected_nodes = len(self.redpanda.nodes) if node is None else 1
        assert len(response.nodes) == expected_nodes, (
            f"{len(response.nodes)=} != {expected_nodes=}"
        )
        return {
            n.node_id: (
                {s.shard_id: cast(GcStatus, s.status) for s in n.shards}
                if n.error == ""
                else n.error
            )
            for n in response.nodes
        }

    def _gc_node_ids(self, node: int | None) -> list[int]:
        if node is not None:
            return [node]
        return [self.redpanda.node_id(n) for n in self.redpanda.nodes]

    def gc_pause(self, node: int | None = None):
        for nid in self._gc_node_ids(node):
            self.logger.debug(f"Pause L0 GC on node {nid}")
            self.l0_client.pause_gc(l0_pb.PauseGcRequest(node_id=nid))

    def gc_start(self, node: int | None = None):
        for nid in self._gc_node_ids(node):
            self.logger.debug(f"Start L0 GC on node {nid}")
            self.l0_client.start_gc(l0_pb.StartGcRequest(node_id=nid))

    def gc_reset(self, node: int | None = None):
        for nid in self._gc_node_ids(node):
            self.logger.debug(f"Reset L0 GC on node {nid}")
            self.l0_client.reset_gc(l0_pb.ResetGcRequest(node_id=nid))

    def gc_advance_epoch(self, topic: str, partition: int, new_epoch: int) -> EpochInfo:
        self.logger.debug(f"Advance epoch for '{topic}/{partition}'")
        response = self.l0_client.advance_epoch(
            l0_pb.AdvanceEpochRequest(
                partition=ntp_pb.TopicPartition(topic=topic, partition=partition),
                new_epoch=new_epoch,
            )
        )
        assert response is not None, "AdvanceEpochResponse should not be None"
        return response.epoch

    def gc_get_epoch_info(
        self,
        topic_partitions: list[tuple[str, int]] | None = None,
    ) -> EpochReport:
        if topic_partitions is None:
            topic_partitions = [
                (t.name, i) for t in self.topics for i in range(0, t.partition_count)
            ]
        self.logger.debug(f"Get epoch info for {topic_partitions=}")
        result: dict[str, dict[int, l0_pb.EpochInfo | str]] = {}
        for topic, partition in topic_partitions:
            if topic not in result:
                result[topic] = {}
            try:
                response = self.l0_client.get_epoch_info(
                    l0_pb.GetEpochInfoRequest(
                        partition=ntp_pb.TopicPartition(
                            topic=topic, partition=partition
                        )
                    )
                )
                result[topic][partition] = response.epoch
            except Exception as e:
                result[topic][partition] = str(e)
        return result

    def check_statuses(
        self,
        report: StatusReport,
        nodes: list[int] | None = None,
        status: GcStatus.ValueType | None = None,
        error: str | None = None,
        strict: bool = True,
    ):
        assert status is not None or error is not None, (
            "check_statuses usage: Set expected status or expected error"
        )
        if nodes is None or nodes == []:
            nodes = [self.redpanda.node_id(n) for n in self.redpanda.nodes]
        assert not strict or len(report) == len(nodes), (
            f"Expected report for exactly {nodes=}, got {report=}"
        )

        for node_id in nodes:
            assert node_id in report, f"Expected status for {node_id=}"
            shards = report[node_id]
            if error is not None:
                assert isinstance(shards, str), (
                    f"{node_id}: Expected error got {shards=}"
                )
                assert error in shards, f"{node_id=} Unexpected error body '{shards}'"
            else:
                assert isinstance(shards, dict), (
                    f"{node_id=}: Unexpected error '{shards}'"
                )
                assert len(shards) > 0, f"{node_id=}: Report unexpectedly empty"
                assert all(s == status for _, s in shards.items()), (
                    f"Expected {status=} on {node_id=}: {shards=}"
                )

    def _all_in_state(self, expected: GcStatus.ValueType) -> bool:
        try:
            self.check_statuses(self.gc_get_status(), status=expected)
            return True
        except AssertionError:
            return False

    def _all_running(self) -> bool:
        return self._all_in_state(GcStatus.L0_GC_STATUS_RUNNING)

    def _all_paused(self) -> bool:
        return self._all_in_state(GcStatus.L0_GC_STATUS_PAUSED)

    def wait_all_running(self, timeout_sec: int = 30):
        wait_until(
            self._all_running,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            retry_on_exc=True,
        )

    def setUp(self):
        super().setUp()
        self.logger.debug("Wait for safety monitor to clear initial health check")
        self.wait_all_running()


class CloudTopicsL0GCAdminTest(CloudTopicsL0GCAdminBase):
    """
    Integration: Admin API rpcs for starting and stopping level zero garbage collection.
    """

    def __init__(self, test_context: TestContext):
        # Use a long housekeeping interval so that the housekeeper does not
        # auto-advance epochs during the test; we want to observe the effect
        # of manually bumping a specific partition's epoch via Admin rpc.
        super().__init__(
            test_context=test_context, housekeeping_interval_ms=10 * 60 * 60 * 1000
        )

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_get_status(self, cloud_storage_type: CloudStorageType):
        self.logger.debug("Clusterwide status")
        statuses = self.gc_get_status()
        self.check_statuses(statuses, status=GcStatus.L0_GC_STATUS_RUNNING)

        target_node = self.redpanda.nodes[2]
        target_node_id = self.redpanda.node_id(target_node)

        self.logger.debug("Single-node status")
        statuses = self.gc_get_status(target_node_id)
        self.check_statuses(
            statuses, nodes=[target_node_id], status=GcStatus.L0_GC_STATUS_RUNNING
        )

        self.logger.debug("Kill a node and check partial failure")
        self.redpanda.stop_node(target_node, timeout=30)
        statuses = self.gc_get_status()
        self.check_statuses(
            statuses,
            nodes=[target_node_id],
            error="(Service unavailable)",
            strict=False,
        )

        alive_node_ids = [
            self.redpanda.node_id(n)
            for n in self.redpanda.nodes
            if n.name != target_node.name
        ]

        # With the safety monitor, the remaining nodes will detect the
        # cluster is unhealthy (one node down) and transition to
        # SAFETY_BLOCKED. This may take up to one health check interval.
        def _alive_nodes_safety_blocked():
            s = self.gc_get_status()
            self.check_statuses(
                s,
                nodes=alive_node_ids,
                status=GcStatus.L0_GC_STATUS_SAFETY_BLOCKED,
                strict=False,
            )
            return True

        wait_until(
            _alive_nodes_safety_blocked,
            timeout_sec=30,
            backoff_sec=2,
            retry_on_exc=True,
        )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_basic_pause_unpause(self, cloud_storage_type: CloudStorageType):
        self.topics = [
            TopicSpec(partition_count=2),
            TopicSpec(partition_count=2),
        ]
        self.create_topics(self.topics)
        self.logger.debug("Produce some")
        self.produce_some(topics=[spec.name for spec in self.topics])

        self.logger.debug("Wait until we've deleted something...")
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=5,
            retry_on_exc=True,
        )

        self.gc_pause()
        self.check_statuses(self.gc_get_status(), status=GcStatus.L0_GC_STATUS_PAUSED)

        n_deleted = self.get_num_objects_deleted()
        self.logger.debug(f"GC should be stopped now, so we won't exceed {n_deleted=}")
        with expect_exception(TimeoutError, lambda _: True):
            wait_until(
                lambda: self.get_num_objects_deleted() > n_deleted,
                timeout_sec=30,
                backoff_sec=5,
                retry_on_exc=True,
            )

        self.logger.debug(
            "Re-start garbage collection. We should see the deleted object count ticking up."
        )
        self.gc_start()
        self.wait_all_running()

        wait_until(
            lambda: self.get_num_objects_deleted() > n_deleted,
            timeout_sec=60,
            backoff_sec=5,
            retry_on_exc=True,
        )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_single_node_pause_unpause(self, cloud_storage_type: CloudStorageType):
        self.topics = [TopicSpec(partition_count=2)]
        self.create_topics(self.topics)

        pause_node = self.redpanda.nodes[0]
        pause_node_id = self.redpanda.node_id(pause_node)

        self.logger.debug(f"Pause GC on {pause_node.name} and produce some records")
        self.gc_pause(pause_node_id)
        self.check_statuses(
            self.gc_get_status(node=pause_node_id),
            nodes=[pause_node_id],
            status=GcStatus.L0_GC_STATUS_PAUSED,
        )
        other_nodes = [self.redpanda.node_id(n) for n in self.redpanda.nodes[1:]]

        self.check_statuses(
            self.gc_get_status(),
            nodes=other_nodes,
            status=GcStatus.L0_GC_STATUS_RUNNING,
            strict=False,
        )

        self.produce_some(topics=[spec.name for spec in self.topics])

        self.logger.debug("Wait for GC to kick in")
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=5,
            retry_on_exc=True,
        )

        self.logger.debug(
            f"Confirm that we're not deleting any objects on {pause_node.name}"
        )
        with expect_exception(TimeoutError, lambda e: True):
            wait_until(
                lambda: self.get_num_objects_deleted(nodes=[pause_node]) > 0,
                timeout_sec=15,
                backoff_sec=3,
                retry_on_exc=True,
            )

        self.logger.debug(f"Now unpause {pause_node.name} and wait for some deletes")
        self.gc_start(pause_node_id)
        self.check_statuses(self.gc_get_status(), status=GcStatus.L0_GC_STATUS_RUNNING)

        wait_until(
            lambda: self.get_num_objects_deleted(nodes=[pause_node]) > 0,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )

    @cluster(num_nodes=1)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_not_found(self, cloud_storage_type: CloudStorageType):
        nonexistent_node_id: int = 23
        with expect_exception(ConnectError, lambda e: "23 not found" in str(e)):
            self.gc_start(nonexistent_node_id)
        with expect_exception(ConnectError, lambda e: "23 not found" in str(e)):
            self.gc_pause(nonexistent_node_id)

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_partial_failure(self, cloud_storage_type: CloudStorageType):
        node_to_kill = self.redpanda.nodes[1]
        node_to_kill_id = self.redpanda.node_id(node_to_kill)

        self.logger.debug(f"Check that GC admin API is up and stop {node_to_kill.name}")
        self.gc_start()
        self.redpanda.stop_node(node_to_kill, timeout=30)

        self.logger.debug(
            f"Pause on dead node {node_to_kill.name} ({node_to_kill_id}) should fail"
        )
        with expect_exception(ConnectError, lambda e: "unavailable" in str(e).lower()):
            self.gc_pause(node_to_kill_id)

        self.logger.debug(f"Restart {node_to_kill.name} and pause GC there")
        self.redpanda.start_node(
            node_to_kill, timeout=30, node_id_override=node_to_kill_id
        )
        self.gc_pause(node_to_kill_id)

    def _epoch_report_to_str(self, epochs: EpochReport, indent: int = 1) -> str:
        def epoch_info_to_dict(info: l0_pb.EpochInfo) -> dict[str, int]:
            return {
                "estimated_inactive_epoch": info.estimated_inactive_epoch,
                "max_applied_epoch": info.max_applied_epoch,
                "last_reconciled_log_offset": info.last_reconciled_log_offset,
                "current_epoch_window_offset": info.current_epoch_window_offset,
            }

        serializable = {
            t: {
                p: (epoch_info_to_dict(e) if isinstance(e, l0_pb.EpochInfo) else e)
                for p, e in ps.items()
            }
            for t, ps in epochs.items()
        }
        return json.dumps(serializable, indent=indent)

    def check_epochs(
        self, epochs: EpochReport, active_topics: list[str], stalled_topics: list[str]
    ):
        self.logger.debug(self._epoch_report_to_str(epochs))
        for t in self.topics:
            assert t.name in epochs, f"Expected {t.name=} got {epochs=}"
            ps = epochs[t.name]
            assert all(p in ps for p in range(0, t.partition_count)), (
                f"Expected partitions [0..{t.partition_count}] got {ps=}"
            )
            if t.name in active_topics:
                # active topics should have EpochInfo with positive inactive epoch
                assert all(
                    isinstance(e, l0_pb.EpochInfo) and e.estimated_inactive_epoch > 0
                    for _, e in ps.items()
                ), f"Expected EpochInfo with positive epochs for {t.name=}"
            elif t.name in stalled_topics:
                # Stalled topics should have EpochInfo with nonexistent estimated_inactive_epoch
                # since no data has been reconciled
                assert all(
                    isinstance(e, l0_pb.EpochInfo) and e.estimated_inactive_epoch < 0
                    for _, e in ps.items()
                ), f"Expected EpochInfo with min epoch for stalled {t.name=}"
            else:
                assert False, f"{t.name} not in {(active_topics + stalled_topics)=}"

        return True

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_advance_epoch(self, cloud_storage_type: CloudStorageType):
        self.topics = [
            TopicSpec(partition_count=1),
            TopicSpec(partition_count=1),
        ]
        self.create_topics(self.topics)
        produce_topics = [t.name for t in self.topics[0:1]]
        stalled_topic = self.topics[1].name
        assert stalled_topic not in produce_topics, (
            f"{stalled_topic=} in {produce_topics=}"
        )

        self.produce_some(topics=produce_topics)

        # since we've produced nothing to stalled_topic, that partition will block GC
        # from progressing. this block checks that the epoch report has the right shape
        # and that it shows errors for all the partitions of stalled_topic
        # NOTE: wait_until here so we don't race against reconciliation of produce_topics data.
        # "monotonic epoch" invariant guarantees that epoch(stalled_topic) didn't advance then return to 0.
        wait_until(
            lambda: self.check_epochs(
                self.gc_get_epoch_info(), produce_topics, [stalled_topic]
            ),
            timeout_sec=15,
            backoff_sec=3,
            retry_on_exc=True,
        )
        epochs = self.gc_get_epoch_info()
        self.check_epochs(epochs, produce_topics, [stalled_topic])

        self.logger.debug(
            f"Check that GC doesn't progress despite reconciliation making progress on {produce_topics=}"
        )
        with expect_exception(TimeoutError, lambda _: True):
            wait_until(
                lambda: self.get_num_objects_deleted() > 0,
                timeout_sec=30,
                backoff_sec=5,
                retry_on_exc=True,
            )

        epochs = self.gc_get_epoch_info()
        self.check_epochs(epochs, produce_topics, [stalled_topic])

        # While stuck, min_partition_gc_epoch should be <= 0.
        min_epoch_metric = "vectorized_cloud_topics_l0_gc_min_partition_gc_epoch"
        min_epoch_while_stuck = self._get_metric_max(min_epoch_metric)
        self.logger.debug(f"While stuck: {min_epoch_while_stuck=}")
        assert min_epoch_while_stuck <= 0, (
            f"Expected min_partition_gc_epoch <= 0 while stalled "
            f"topic blocks, got {min_epoch_while_stuck}"
        )

        self.logger.debug(
            "Force the stalled topic's epoch up to the inactive epoch of the active topic. This should unstick GC"
        )
        target_epoch = cast(
            EpochInfo, epochs[produce_topics[0]][0]
        ).estimated_inactive_epoch

        new_epoch = self.gc_advance_epoch(
            topic=stalled_topic,
            partition=0,
            new_epoch=target_epoch,
        )
        self.logger.debug(f"New EpochInfo for {stalled_topic=}: {new_epoch}")

        gc_epoch = new_epoch.estimated_inactive_epoch
        max_epoch = new_epoch.max_applied_epoch
        epoch_offset = new_epoch.current_epoch_window_offset
        lrlo = new_epoch.last_reconciled_log_offset

        assert gc_epoch == (target_epoch - 1) and gc_epoch < max_epoch, (
            f"Expected {gc_epoch=} == {target_epoch-1=}"
        )
        assert epoch_offset == lrlo and epoch_offset > 0, (
            f"Expected 0 < ({epoch_offset=}) == ({lrlo=}) on {stalled_topic=}"
        )

        self.logger.debug(
            f"Now that we've advanced {stalled_topic=} epoch window, GC can make progress"
        )
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=5,
            retry_on_exc=True,
        )

        # After unstick, min_partition_gc_epoch should be positive.
        wait_until(
            lambda: self._get_metric_max(min_epoch_metric) > 0,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )
        self.logger.debug("After unstick: min_partition_gc_epoch > 0")

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_concurrent_pause_start(self, cloud_storage_type: CloudStorageType):
        """
        Integration: Rapidly toggle pause/start/reset while GC is running,
        including concurrent resets from a background thread. Verify no
        crashes, no stuck state, and that the last command wins.
        """

        topic = TopicSpec(partition_count=2, replication_factor=3)
        self.topics = [topic]
        self.create_topics(self.topics)

        self.produce_some(topics=[topic.name])

        self.logger.info("Waiting for GC to start deleting")
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )

        # Fire resets concurrently from a background thread while the main
        # thread rapidly toggles pause/start.
        reset_errors: list[Exception] = []

        def background_resets(rounds: int):
            for i in range(rounds):
                try:
                    self.gc_reset()
                except Exception as e:
                    self.logger.warning(f"Background reset {i} failed: {e}")
                    reset_errors.append(e)

        with ThreadPoolExecutor(max_workers=1) as executor:
            reset_fut = executor.submit(background_resets, 20)

            self.logger.info(
                "Starting rapid pause/start toggling (50 rounds) with concurrent resets"
            )
            for _ in range(50):
                self.gc_pause()
                self.gc_start()
            self.logger.info("Toggling complete, waiting for background resets")

            reset_fut.result(timeout=60)

        assert len(reset_errors) == 0, (
            f"Background resets had {len(reset_errors)} errors: {reset_errors}"
        )

        # End with start — verify GC reaches RUNNING and keeps working.
        self.gc_start()
        self.wait_all_running()
        deleted_after_toggle = self.get_num_objects_deleted()
        self.logger.info(
            f"After toggling (running): objects_deleted={deleted_after_toggle}"
        )

        # Verify GC continues to make progress.
        wait_until(
            lambda: self.get_num_objects_deleted() > deleted_after_toggle,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )
        deleted_running = self.get_num_objects_deleted()
        self.logger.info(f"GC still progressing: objects_deleted={deleted_running}")

        # End with pause — verify GC reaches PAUSED and stops.
        self.gc_pause()
        wait_until(
            lambda: self._all_paused(),
            timeout_sec=15,
            backoff_sec=2,
            retry_on_exc=True,
        )
        deleted_at_pause = self.get_num_objects_deleted()

        # Verify objects_deleted does not advance while paused.
        with expect_exception(TimeoutError, lambda _: True):
            wait_until(
                lambda: self.get_num_objects_deleted() > deleted_at_pause,
                timeout_sec=10,
                backoff_sec=2,
                retry_on_exc=True,
            )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_reset_while_paused(self, cloud_storage_type: CloudStorageType):
        """
        Integration: Reset GC while paused, then restart. Verify that GC
        resumes and makes progress.
        """
        topic = TopicSpec(partition_count=2, replication_factor=3)
        self.topics = [topic]
        self.create_topics(self.topics)

        self.produce_some(topics=[topic.name])

        self.logger.debug("Wait for GC to start deleting")
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )

        self.logger.debug("Pause GC")
        self.gc_pause()
        self.check_statuses(self.gc_get_status(), status=GcStatus.L0_GC_STATUS_PAUSED)

        deleted_before_reset = self.get_num_objects_deleted()
        self.logger.debug(f"Deleted before reset: {deleted_before_reset}")

        self.logger.debug("Reset GC while paused")
        self.gc_reset()

        # GC should still be paused after reset (was paused before)
        self.check_statuses(self.gc_get_status(), status=GcStatus.L0_GC_STATUS_PAUSED)

        self.logger.debug("Start GC after reset — should resume and keep deleting")
        self.gc_start()
        self.wait_all_running()

        wait_until(
            lambda: self.get_num_objects_deleted() > deleted_before_reset,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )
        self.logger.debug(
            f"GC progressed after reset: {self.get_num_objects_deleted()} > {deleted_before_reset}"
        )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_reset_while_running(self, cloud_storage_type: CloudStorageType):
        """
        Integration: Reset GC while it is actively running. Verify that it
        auto-resumes without getting stuck.
        """
        topic = TopicSpec(partition_count=2, replication_factor=3)
        self.topics = [topic]
        self.create_topics(self.topics)

        self.produce_some(topics=[topic.name])

        self.logger.debug("Wait for GC to start deleting")
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )

        self.logger.debug("Reset GC while running")
        self.gc_reset()

        deleted_after_reset = self.get_num_objects_deleted()
        self.logger.debug(f"Deleted after reset: {deleted_after_reset}")

        # GC should auto-resume to running state after reset
        self.wait_all_running()

        self.logger.debug("Verify GC continues making progress after reset")
        wait_until(
            lambda: self.get_num_objects_deleted() > deleted_after_reset,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )
        self.logger.debug(
            f"GC progressed after reset: {self.get_num_objects_deleted()} > {deleted_after_reset}"
        )


class CloudTopicsL0GCMetricsTest(CloudTopicsL0GCTestBase):
    """
    Integration: Basic semantics for some GC metrics
    """

    def _get_int_metric(self, name: str) -> list[int]:
        samples = self.redpanda.metrics_sample(name)
        self.logger.debug(samples)
        assert samples is not None, "samples unexpectedly None"
        vals = [int(s.value) for s in samples.samples]
        return vals

    def get_epoch_lag(self) -> list[int]:
        return self._get_int_metric("vectorized_cloud_topics_l0_gc_epoch_lag")

    def get_max_deleted_epoch(self) -> list[int]:
        return self._get_int_metric("vectorized_cloud_topics_l0_gc_max_deleted_epoch")

    def get_collection_rounds(self) -> list[int]:
        return self._get_int_metric(
            "vectorized_cloud_topics_l0_gc_collection_rounds_total"
        )

    def get_objects_listed(self) -> list[int]:
        return self._get_int_metric(
            "vectorized_cloud_topics_l0_gc_objects_listed_total"
        )

    def get_delete_requests(self) -> list[int]:
        return self._get_int_metric(
            "vectorized_cloud_topics_l0_gc_delete_requests_total"
        )

    @cluster(num_nodes=5)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_gc_metrics(self, cloud_storage_type: CloudStorageType):
        self.topics = [TopicSpec(partition_count=1)]
        self.create_topics(self.topics)
        self.produce_some(topics=[spec.name for spec in self.topics])

        # GC should be completing collection rounds and scanning objects
        wait_until(
            lambda: sum(self.get_collection_rounds()) > 0,
            timeout_sec=60,
            backoff_sec=2,
            retry_on_exc=True,
        )
        self.logger.info(
            f"GC running, collection_rounds={self.get_collection_rounds()}"
        )

        wait_until(
            lambda: sum(self.get_objects_listed()) > 0,
            timeout_sec=60,
            backoff_sec=2,
            retry_on_exc=True,
        )
        self.logger.info(
            f"GC scanning objects, objects_listed={self.get_objects_listed()}"
        )

        # Wait for GC to start deleting - max_deleted_epoch should become >= 0 on some shard
        wait_until(
            lambda: any(e >= 0 for e in self.get_max_deleted_epoch()),
            timeout_sec=60,
            backoff_sec=2,
            retry_on_exc=True,
        )

        initial_max_deleted = max(self.get_max_deleted_epoch())
        self.logger.info(f"GC started deleting, {initial_max_deleted=}")

        # Produce more to create new epochs while GC is running
        self.produce_some(topics=[spec.name for spec in self.topics])

        # max_deleted_epoch should increase as GC makes progress
        wait_until(
            lambda: max(self.get_max_deleted_epoch()) > initial_max_deleted,
            timeout_sec=60,
            backoff_sec=2,
            retry_on_exc=True,
        )
        self.logger.info(
            f"GC progressing, max_deleted_epoch increased to "
            f"{max(self.get_max_deleted_epoch())}"
        )

        # Verify batching: delete requests should be fewer than objects deleted,
        # indicating multiple objects per batch request.
        total_deleted = self.get_num_objects_deleted()
        total_delete_requests = sum(self.get_delete_requests())
        self.logger.info(f"Batching check: {total_deleted=}, {total_delete_requests=}")
        assert total_delete_requests > 0, "Expected at least one delete request"
        assert total_deleted > total_delete_requests, (
            f"Expected batching: {total_deleted=} should be greater than "
            f"{total_delete_requests=}"
        )


class CloudTopicsL0GCDataIntegrityTest(CloudTopicsL0GCTestBase):
    """
    Integration: Verify GC does not cause data loss.

    Produces a known number of records, waits for GC to delete L0 objects
    (confirming reconciliation has moved data to L1), then consumes and
    verifies every record is intact.
    """

    MSG_COUNT = 5000
    MSG_SIZE = 1024

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_no_data_loss_under_gc(self, cloud_storage_type: CloudStorageType):
        """
        Produce records, let GC delete L0 objects, then consume and verify
        that every record is still readable.
        """
        topic = TopicSpec(partition_count=2)
        self.topics = [topic]
        self.create_topics(self.topics)

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size=self.MSG_SIZE,
            msg_count=self.MSG_COUNT,
        )
        producer.start()
        producer.wait(timeout_sec=120)

        pstatus = producer.produce_status
        self.logger.info(
            f"Produced {pstatus.acked} records, bad_offsets={pstatus.bad_offsets}"
        )
        assert pstatus.acked == self.MSG_COUNT, (
            f"Producer did not ack all messages: {pstatus.acked} != {self.MSG_COUNT}"
        )
        assert pstatus.bad_offsets == 0, (
            f"Producer saw {pstatus.bad_offsets} bad offsets"
        )

        self.logger.info("Waiting for GC to delete L0 objects")
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=5,
            retry_on_exc=True,
        )
        objects_deleted = self.get_num_objects_deleted()
        self.logger.info(f"GC has deleted {objects_deleted} L0 objects, consuming now")

        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size=self.MSG_SIZE,
            loop=False,
            nodes=[producer.nodes[0]],
            producer=producer,
        )
        consumer.start(clean=False)
        consumer.wait(timeout_sec=120)

        cstatus = consumer.consumer_status
        self.logger.info(
            f"Consumer: valid_reads={cstatus.validator.valid_reads}, "
            f"invalid_reads={cstatus.validator.invalid_reads}, "
            f"offset_gaps={cstatus.validator.offset_gaps}, "
            f"out_of_scope_invalid_reads={cstatus.validator.out_of_scope_invalid_reads}"
        )

        assert cstatus.validator.invalid_reads == 0, (
            f"Data corruption: {cstatus.validator.invalid_reads} invalid reads"
        )
        assert cstatus.validator.out_of_scope_invalid_reads == 0, (
            f"Out-of-scope reads: {cstatus.validator.out_of_scope_invalid_reads}"
        )
        assert cstatus.validator.valid_reads == self.MSG_COUNT, (
            f"Data loss: expected {self.MSG_COUNT} reads, "
            f"got {cstatus.validator.valid_reads}"
        )

        producer.stop()
        consumer.stop()


class CloudTopicsL0GCGracePeriodTest(CloudTopicsL0GCTestBase):
    """
    Integration: Verify the deletion grace period is enforced.

    Objects that are epoch-eligible but younger than the configured
    minimum age must not be deleted. Once they age past the grace
    period they should be collected.
    """

    GRACE_PERIOD_S = 30

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context=test_context,
            extra_rp_conf_overrides={
                "cloud_topics_short_term_gc_minimum_object_age": self.GRACE_PERIOD_S
                * 1000,
                # Keep GC polling frequently so we observe skips quickly.
                "cloud_topics_short_term_gc_interval": 1000,
                "cloud_topics_short_term_gc_backoff_interval": 2000,
            },
        )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_grace_period_enforcement(self, cloud_storage_type: CloudStorageType):
        """
        Produce a burst of records as fast as possible so all L0 objects
        have similar creation times. With a 30s grace period, GC should
        list and skip them as 'too young' for a long window after epochs
        advance, then delete them once they age out.
        """
        topic = TopicSpec(partition_count=1)
        self.topics = [topic]
        self.create_topics(self.topics)

        # Use KgoVerifierProducer for a fast, synchronous burst so that
        # all L0 objects are created in a narrow time window.
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size=1024,
            msg_count=500,
        )
        producer.start()
        producer.wait(timeout_sec=60)
        self.logger.info(f"Produced {producer.produce_status.acked} records")
        producer.stop()

        skipped_metric = "vectorized_cloud_topics_l0_gc_objects_skipped_too_young_total"

        self.logger.info(
            "Waiting for GC to skip epoch-eligible objects as too young "
            "while no objects have been deleted yet"
        )
        wait_until(
            lambda: self._get_metric_total(skipped_metric) > 0,
            timeout_sec=60,
            backoff_sec=1,
            retry_on_exc=True,
        )

        assert self.get_num_objects_deleted() == 0, (
            f"Expected objects_delete=0, got {self.get_num_objects_deleted()=}"
        )

        self.logger.info(f"Waiting for grace period ({self.GRACE_PERIOD_S}s) to expire")
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=self.GRACE_PERIOD_S + 10,
            backoff_sec=3,
            retry_on_exc=True,
        )


class CloudTopicsL0GCNodeFailureTest(CloudTopicsL0GCTestBase):
    """
    Integration: Verify GC survives a node failure mid-collection and
    resumes cluster-wide progress after the node restarts.
    """

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_node_failure_mid_gc(self, cloud_storage_type: CloudStorageType):
        """
        Produce data, let GC make progress, kill a node, verify surviving
        nodes continue collecting, then restart and confirm cluster-wide
        GC resumes.
        """
        topic = TopicSpec(partition_count=3)
        self.topics = [topic]
        self.create_topics(self.topics)

        self.produce_some(topics=[topic.name])

        self.logger.info("Waiting for GC kick in")
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )

        # Pick the node to kill. Use the last node so that the admin API
        # (which targets the first node by default) stays available.
        kill_node = self.redpanda.nodes[-1]
        kill_node_id = self.redpanda.node_id(kill_node)
        surviving_nodes = [n for n in self.redpanda.nodes if n != kill_node]

        deleted_on_survivors_before = self.get_num_objects_deleted(
            nodes=surviving_nodes
        )

        self.logger.info(f"Killing node {kill_node.name} (id={kill_node_id})")
        self.redpanda.stop_node(kill_node, timeout=30)

        self.logger.info("Verifying surviving nodes continue GC")
        wait_until(
            lambda: self.get_num_objects_deleted(nodes=surviving_nodes)
            > deleted_on_survivors_before,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )

        self.logger.info(f"Restarting node {kill_node.name} (id={kill_node_id})")
        self.redpanda.start_node(kill_node, timeout=30, node_id_override=kill_node_id)

        self.logger.info("Waiting for restarted node GC to resume")
        wait_until(
            lambda: self.get_num_objects_deleted(nodes=[kill_node]) > 0,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )


class CloudTopicsL0GCLeadershipTransferTest(CloudTopicsL0GCTestBase):
    """
    Integration: Verify GC continues making progress through leadership
    transfers.
    """

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_leadership_transfer_during_gc(self, cloud_storage_type: CloudStorageType):
        """
        Produce records, let GC make progress, transfer partition
        leadership, then verify GC continues deleting objects.
        """
        topic = TopicSpec(partition_count=1, replication_factor=3)
        self.topics = [topic]
        self.create_topics(self.topics)

        self.produce_some(topics=[topic.name])

        self.logger.info("Waiting for GC to start deleting")
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )

        # Find current leader and pick a different target.
        admin = RedpandaAdmin(self.redpanda)
        leader_id = admin.get_partition_leader(
            namespace="kafka", topic=topic.name, partition=0
        )
        target_id = next(
            self.redpanda.node_id(n)
            for n in self.redpanda.nodes
            if self.redpanda.node_id(n) != leader_id
        )
        self.logger.info(f"Transferring leadership from {leader_id} to {target_id}")
        admin.partition_transfer_leadership("kafka", topic.name, 0, target_id)

        wait_until(
            lambda: admin.get_partition_leader(
                namespace="kafka", topic=topic.name, partition=0
            )
            == target_id,
            timeout_sec=30,
            backoff_sec=2,
            retry_on_exc=True,
        )
        self.logger.info(f"Leadership transferred to {target_id}")

        # Snapshot after transfer so we know progress is post-transfer.
        deleted_before = self.get_num_objects_deleted()

        # Verify GC continues to make progress post-transfer.
        wait_until(
            lambda: self.get_num_objects_deleted() > deleted_before,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )


class CloudTopicsL0GCTopicDeletionTest(CloudTopicsL0GCTestBase):
    """
    Integration: Delete a cloud topic while GC is actively running.
    Verify GC handles it gracefully — no crashes, no hangs, and
    max_deleted_epoch continues advancing for the remaining topic.
    """

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_topic_deletion_during_gc(self, cloud_storage_type: CloudStorageType):
        """
        Create two topics, produce to both, wait for GC to start,
        delete one topic, and verify GC continues for the survivor.
        """
        topic_a = TopicSpec(partition_count=1, replication_factor=3)
        topic_b = TopicSpec(partition_count=1, replication_factor=3)
        self.topics = [topic_a, topic_b]
        self.create_topics(self.topics)

        self.produce_some(topics=[topic_a.name, topic_b.name])

        self.logger.info("Waiting for GC to start deleting")
        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )

        epoch_before = self._get_metric_max(
            "vectorized_cloud_topics_l0_gc_max_deleted_epoch"
        )

        # Delete topic B while GC is running.
        rpk = RpkTool(self.redpanda)
        self.logger.info(f"Deleting topic {topic_b.name}")
        rpk.delete_topic(topic_b.name)

        # Verify max_deleted_epoch advances (GC is making real progress).
        self.logger.info("Waiting for GC to continue after deletion")
        wait_until(
            lambda: self._get_metric_max(
                "vectorized_cloud_topics_l0_gc_max_deleted_epoch"
            )
            > epoch_before,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )


class CloudTopicsL0GCEpochLagTest(CloudTopicsL0GCTestBase):
    """
    Integration: Configure slow GC so it falls behind ingress, verify
    epoch_lag grows, then hot-reconfigure to fast GC and verify it
    catches up.
    """

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context,
            extra_rp_conf_overrides={
                # Slow GC: 10s between rounds (5x the default, enough to
                # build measurable backpressure without wasting test time).
                "cloud_topics_short_term_gc_interval": 10000,
                "cloud_topics_short_term_gc_backoff_interval": 10000,
            },
        )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_epoch_lag_and_catchup(self, cloud_storage_type: CloudStorageType):
        """
        Produce data continuously while GC is slow (10s interval), observe
        epoch_lag building up, then hot-reconfigure to fast GC (1s) and
        verify epoch_lag decreases significantly.

        Production must stay active through both phases so that new objects
        keep arriving across successive epochs. Without continuous ingress
        GC can finish all work in a single round, after which
        min_deletion_epoch freezes while max_gc_eligible_epoch keeps
        advancing — making epoch_lag grow instead of shrink.
        """
        topic = TopicSpec(partition_count=1, replication_factor=3)
        self.topics = [topic]
        self.create_topics(self.topics)

        with repeater_traffic(
            context=self.test_context,
            redpanda=self.redpanda,
            topics=[topic.name],
            msg_size=1024,
            rate_limit_bps=2 * 1024 * 1024,
            workers=1,
        ) as repeater:
            repeater.await_group_ready()
            repeater.await_progress(200, timeout_sec=120)

            # Wait for epoch_lag > 10: GC is running but falling behind
            # because epochs advance faster than the 10s GC interval.
            wait_until(
                lambda: self._get_metric_max("vectorized_cloud_topics_l0_gc_epoch_lag")
                > 10,
                timeout_sec=60,
                backoff_sec=5,
                retry_on_exc=True,
            )

            lag_while_slow = self._get_metric_max(
                "vectorized_cloud_topics_l0_gc_epoch_lag"
            )
            self.logger.info(f"Slow-GC phase: epoch_lag={lag_while_slow}")
            assert lag_while_slow > 0

            # Hot-reconfigure: speed up GC dramatically.
            admin = RedpandaAdmin(self.redpanda)
            admin.patch_cluster_config(
                upsert={
                    "cloud_topics_short_term_gc_interval": 1000,
                    "cloud_topics_short_term_gc_backoff_interval": 1000,
                }
            )
            self.logger.info("Hot-reconfigured GC to 1s interval")

            # Verify GC catches up: epoch_lag should decrease by at least 50%.
            # It may not reach 0 because not every epoch has objects to
            # delete, so max_deleted_epoch can't close the gap completely.
            wait_until(
                lambda: self._get_metric_max("vectorized_cloud_topics_l0_gc_epoch_lag")
                < lag_while_slow // 2,
                timeout_sec=60,
                backoff_sec=3,
                retry_on_exc=True,
            )
            lag_final = self._get_metric_max("vectorized_cloud_topics_l0_gc_epoch_lag")
            self.logger.info(
                f"Fast-GC phase: epoch_lag={lag_final} (was {lag_while_slow} while slow)"
            )


class CloudTopicsL0GCOrphanedObjectsTest(CloudTopicsL0GCTestBase):
    """
    Integration: Inject well-formed orphaned L0 objects into the bucket
    and verify GC deletes them since their epoch is below the watermark.
    """

    NUM_ORPHANS = 10

    def _make_l0_object_key(self, prefix: int, epoch: int) -> str:
        """Build a well-formed L0 data object key."""
        return f"level_zero/data/{prefix:03d}/{epoch:018d}/{uuid.uuid4()}"

    def _inject_orphaned_objects(
        self, count: int, prefix: int, epoch: int
    ) -> list[str]:
        bucket = self.si_settings.cloud_storage_bucket
        client = self.redpanda.cloud_storage_client
        keys: list[str] = []
        for _ in range(count):
            key = self._make_l0_object_key(prefix, epoch)
            client.put_object(bucket, key, b"orphaned-l0-data")
            keys.append(key)
        return keys

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_orphaned_objects_cleaned(self, cloud_storage_type: CloudStorageType):
        """
        Produce real data so GC establishes an epoch watermark, then
        inject orphaned L0 objects with a very low epoch. Verify GC
        deletes them.
        """
        topic = TopicSpec(partition_count=1, replication_factor=3)
        self.topics = [topic]
        self.create_topics(self.topics)

        self.produce_some(topics=[topic.name])

        # Wait for the GC watermark to be well above the epoch we'll
        # use for fake objects (epoch=1), and for GC to be actively
        # deleting.
        self.logger.info("Waiting for GC watermark to advance past 10")
        wait_until(
            lambda: self._get_metric_max(
                "vectorized_cloud_topics_l0_gc_min_partition_gc_epoch"
            )
            > 10
            and self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
        )

        # Inject orphaned objects with epoch=1. The watermark is well
        # above this, so they are immediately epoch-eligible. They just
        # need to age past the grace period (10s default).
        self.logger.info(f"Injecting {self.NUM_ORPHANS} orphaned objects")
        injected_keys = self._inject_orphaned_objects(
            count=self.NUM_ORPHANS, prefix=0, epoch=1
        )
        self.logger.info(
            f"Injected keys: {injected_keys[:3]}... ({len(injected_keys)} total)"
        )

        # Poll the bucket directly until every injected key is gone.
        bucket = self.si_settings.cloud_storage_bucket
        client = cast(S3Client, self.redpanda.cloud_storage_client)

        def orphans_gone() -> bool:
            for key in injected_keys:
                try:
                    client.get_object_meta(bucket, key)
                    return False
                except Exception:
                    pass
            return True

        self.logger.info("Waiting for orphaned objects to disappear from bucket")
        wait_until(
            orphans_gone,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=False,
        )
        self.logger.info("All orphaned objects deleted")


class CloudTopicsL0GCStressTest(CloudTopicsL0GCTestBase):
    """
    Stress: Push ~3 GiB through 12 partitions, wait for a significant
    volume of L0 data to be garbage-collected, then consume and verify
    that no records were lost and some reads were served from L1.

    In debug/sanitizer builds the data volume is reduced to 40 MiB to
    stay within the framework's per-test write budget.
    """

    PARTITION_COUNT = 12
    MSG_SIZE = 4096

    if is_debug_mode():
        data_volume = 40 * 1024 * 1024  # 40 MiB
        timeout_s = 180
    else:
        data_volume = 3 * 1024 * 1024 * 1024  # 3 GiB
        timeout_s = 600

    MSG_COUNT = data_volume // MSG_SIZE
    # Wait for GC to collect at least 2/3 of the produced volume.
    GC_BYTES_TARGET = data_volume * 2 // 3
    # Expect at least 1/3 of produced volume to be read from L1.
    L1_READ_BYTES_TARGET = data_volume // 3

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type(docker_use_arbitrary=True))
    def test_gc_stress(self, cloud_storage_type: CloudStorageType):
        """
        Produce ~3 GiB across 12 partitions (40 MiB in debug mode),
        wait for GC to collect roughly 2/3, then consume all records
        and verify nothing was lost and some reads came from L1.
        """
        topic = TopicSpec(partition_count=self.PARTITION_COUNT)
        self.topics = [topic]
        self.create_topics(self.topics)

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size=self.MSG_SIZE,
            msg_count=self.MSG_COUNT,
        )
        producer.start()

        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size=self.MSG_SIZE,
            loop=True,
            nodes=[producer.nodes[0]],
            producer=producer,
        )
        consumer.start(clean=False)

        self.logger.info(
            f"Waiting for GC to delete >= {self.GC_BYTES_TARGET / (1024**3):.1f} GiB"
        )
        wait_until_with_progress_check(
            check=lambda: self.get_bytes_deleted(),
            condition=lambda: self.get_bytes_deleted() >= self.GC_BYTES_TARGET,
            timeout_sec=self.timeout_s,
            progress_sec=60,
            backoff_sec=5,
            logger=self.logger,
        )
        bytes_deleted = self.get_bytes_deleted()
        self.logger.info(
            f"GC deleted {bytes_deleted / (1024**3):.2f} GiB "
            f"({self.get_num_objects_deleted()} objects)"
        )

        producer.wait(timeout_sec=self.timeout_s)

        pstatus = producer.produce_status
        self.logger.info(
            f"Produced {pstatus.acked} records "
            f"(~{pstatus.acked * self.MSG_SIZE / (1024**3):.2f} GiB), "
            f"bad_offsets={pstatus.bad_offsets}"
        )
        assert pstatus.acked == self.MSG_COUNT, (
            f"Producer did not ack all messages: {pstatus.acked} != {self.MSG_COUNT}"
        )
        assert pstatus.bad_offsets == 0, (
            f"Producer saw {pstatus.bad_offsets} bad offsets"
        )

        consumer.wait(timeout_sec=self.timeout_s)

        cstatus = consumer.consumer_status
        self.logger.info(
            f"Consumer: valid_reads={cstatus.validator.valid_reads}, "
            f"invalid_reads={cstatus.validator.invalid_reads}, "
            f"offset_gaps={cstatus.validator.offset_gaps}, "
            f"out_of_scope_invalid_reads="
            f"{cstatus.validator.out_of_scope_invalid_reads}"
        )

        assert cstatus.validator.invalid_reads == 0, (
            f"Data corruption: {cstatus.validator.invalid_reads} invalid reads"
        )
        assert cstatus.validator.out_of_scope_invalid_reads == 0, (
            f"Out-of-scope reads: {cstatus.validator.out_of_scope_invalid_reads}"
        )
        assert cstatus.validator.valid_reads >= self.MSG_COUNT, (
            f"Data loss: expected at least {self.MSG_COUNT} reads, "
            f"got {cstatus.validator.valid_reads}"
        )

        l1_read_bytes = self._get_metric_total(
            "vectorized_cloud_topics_level_one_reader_read_bytes"
        )
        self.logger.info(
            f"L1 reader bytes: {l1_read_bytes / (1024**2):.1f} MiB "
            f"(target: {self.L1_READ_BYTES_TARGET / (1024**2):.1f} MiB)"
        )
        assert l1_read_bytes >= self.L1_READ_BYTES_TARGET, (
            f"Expected >= {self.L1_READ_BYTES_TARGET} bytes read from L1, "
            f"got {l1_read_bytes}"
        )

        producer.stop()
        consumer.stop()


class CloudTopicsL0GCSafetyBlockTest(CloudTopicsL0GCAdminBase):
    """
    Integration: the cluster_safety_monitor blocks L0 GC when the cluster
    health overview reports an unhealthy state and unblocks it when health
    is restored.
    """

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context=test_context,
            extra_rp_conf_overrides={
                "cloud_topics_short_term_gc_backoff_interval": 2000,
                "health_monitor_max_metadata_age": 1000,
            },
        )

    def _all_safety_blocked(self) -> bool:
        try:
            report = self.gc_get_status()
            self.logger.debug(f"GC status report: {report}")
            self.check_statuses(report, status=GcStatus.L0_GC_STATUS_SAFETY_BLOCKED)
            return True
        except AssertionError as e:
            self.logger.debug(f"Not all safety_blocked yet: {e}")
            return False

    @cluster(
        num_nodes=4,
        log_allow_list=[
            ".*cluster - storage space alert: free space.*",
        ],
    )
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_safety_block_on_unhealthy_cluster(
        self, cloud_storage_type: CloudStorageType
    ):
        self.topics = [TopicSpec(partition_count=1)]
        self.create_topics(self.topics)
        self.produce_some(topics=[self.topics[0].name])

        wait_until(
            lambda: self.get_num_objects_deleted() > 0,
            timeout_sec=60,
            backoff_sec=5,
            retry_on_exc=True,
        )

        self.logger.debug("Trigger cluster unhealthy via disk space alert")
        one_tb = 1024 * 1024 * 1024 * 1024
        self.redpanda.set_cluster_config(
            {"storage_space_alert_free_threshold_bytes": one_tb}
        )

        self.logger.debug("Wait for all nodes to report safety_blocked")
        wait_until(
            self._all_safety_blocked,
            timeout_sec=60,
            backoff_sec=2,
            retry_on_exc=True,
        )

        blocked_metric = "vectorized_cloud_topics_l0_gc_safety_blocked_rounds_total"
        baseline_total = self._get_metric_total(blocked_metric)
        num_shards = len(self._get_metric_values(blocked_metric))
        assert num_shards > 0, "Expected at least one shard reporting"

        self.logger.debug(
            "Verify GC stays blocked: all shards must skip multiple rounds"
        )

        def _blocked_rounds_advancing():
            total = self._get_metric_total(blocked_metric)
            advanced = total - baseline_total >= num_shards * 3
            if advanced:
                self.check_statuses(
                    self.gc_get_status(),
                    status=GcStatus.L0_GC_STATUS_SAFETY_BLOCKED,
                )
            return advanced

        wait_until(
            _blocked_rounds_advancing,
            timeout_sec=60,
            backoff_sec=2,
            retry_on_exc=True,
        )

        self.logger.debug("Restore cluster health")
        self.redpanda.set_cluster_config(
            {"storage_space_alert_free_threshold_bytes": 0}
        )

        self.logger.debug("Wait for all nodes to return to running")
        self.wait_all_running()

        self.logger.debug("Verify blocked rounds stop advancing after recovery")
        stable_count = 0
        last_total = self._get_metric_total(blocked_metric)

        def _blocked_rounds_stable():
            nonlocal stable_count, last_total
            total = self._get_metric_total(blocked_metric)
            if total != last_total:
                last_total = total
                stable_count = 0
                return False
            stable_count += 1
            return stable_count >= 3

        wait_until(
            _blocked_rounds_stable,
            timeout_sec=30,
            backoff_sec=2,
            retry_on_exc=True,
        )
