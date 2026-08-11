# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from concurrent.futures import Future, ThreadPoolExecutor
from logging import Logger
import math
import threading
from typing import Any


from rptest.clients.rpk import RpkTool
from rptest.services.cluster import TestContext, cluster
from rptest.services.multi_cluster_services import (
    Cluster,
    RedpandaCluster,
    SecondaryClusterArgs,
)
from rptest.services.redpanda import (
    PandaproxyConfig,
    SISettings,
    SchemaRegistryConfig,
)
from rptest.tests.cluster_linking_test_base import (
    ClusterLinkingProgressVerifier,
    ShadowLinkTestBase,
)
from rptest.tests.idempotency_stress_test import matrix
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer
from rptest.utils.node_operations import NodeOpsExecutor
from rptest.utils.mode_checks import is_debug_mode
from rptest.utils.node_operations import (
    FailureInjectorBackgroundThread,
    generate_random_workload,
)
from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until

SKIPPED_TOPIC_PROPERTIES = {
    "redpanda.remote.allowgaps",
    "redpanda.leaders.preference",
}


class ClusterLinkingWorkloadSpec:
    def __init__(
        self,
        topic: str,
        topic_properties: dict[str, Any] = {},
        replication_factor: int = 3,
        partition_count: int = 3,
        producer_properties: dict[str, Any] = {},
        consumer_properties: dict[str, Any] = {},
        msg_count: int = 40000,
        msg_size: int = 4 * 1024,
        validate_number_of_messages_on_target: bool = True,
        use_transactions: bool = False,
        use_compaction: bool = False,
    ):
        self.topic = topic
        self.topic_properties = topic_properties
        self.replication_factor = replication_factor
        self.partition_count = partition_count
        self.producer_properties = producer_properties
        self.consumer_properties = consumer_properties
        self.msg_count = msg_count
        self.msg_size = msg_size
        self.validate_number_of_messages_on_target = (
            validate_number_of_messages_on_target
        )
        self.use_transactions = use_transactions
        self.use_compaction = use_compaction

    def __str__(self) -> str:
        return (
            f"ClusterLinkingWorkloadSpec(topic={self.topic}, "
            f"replication_factor={self.replication_factor}, "
            f"partition_count={self.partition_count}, "
            f"producer_properties={self.producer_properties}, "
            f"consumer_properties={self.consumer_properties}, "
            f"msg_count={self.msg_count}, "
            f"msg_size={self.msg_size}, "
            f"validate_number_of_messages_on_target={self.validate_number_of_messages_on_target}, "
            f"use_transactions={self.use_transactions}, "
            f"use_compaction={self.use_compaction})"
        )


class ClusterLinkingWorkloadResult:
    def __init__(self, topic: str, success: bool, error: str | None = None):
        self.topic = topic
        self.success = success
        self.error = error

    @property
    def is_success(self):
        return self.success


class ClusterLinkingWorkloadWorker:
    def __init__(
        self,
        test_context: TestContext,
        source_cluster: Cluster,
        target_cluster: RedpandaCluster,
        spec: ClusterLinkingWorkloadSpec,
        preallocated_nodes: list[ClusterNode],
        logger: Logger,
    ):
        self.test_context: TestContext = test_context
        self.spec: ClusterLinkingWorkloadSpec = spec
        self.preallocated_nodes: list[ClusterNode] = preallocated_nodes
        self.logger: Logger = logger
        self.source_cluster: Cluster = source_cluster
        self.target_cluster: RedpandaCluster = target_cluster

        self.source_rpk = RpkTool(self.source_cluster.service)
        self.target_rpk = RpkTool(self.target_cluster.service)

        self.verifier = ClusterLinkingProgressVerifier(
            test_context=self.test_context,
            source_cluster=self.source_cluster,
            target_cluster=self.target_cluster,
            topic=self.spec.topic,
            preallocated_nodes=self.preallocated_nodes,
            logger=self.logger,
            use_transactions=self.spec.use_transactions,
            use_compaction=self.spec.use_compaction,
            msg_count=self.spec.msg_count,
            msg_size=self.spec.msg_size,
            producer_properties=self.spec.producer_properties,
            consumer_properties=self.spec.consumer_properties,
            validate_number_of_messages_on_target=self.spec.validate_number_of_messages_on_target,
        )

    def start_and_verify(self, progress_timeout: int = 60):
        self.logger.info(f"Starting workload: {self.spec}")
        try:
            self.source_rpk.create_topic(
                self.spec.topic,
                partitions=self.spec.partition_count,
                replicas=self.spec.replication_factor,
                config=self.spec.topic_properties,
            )
            wait_until(
                lambda: self.spec.topic in self.target_rpk.list_topics(),
                timeout_sec=progress_timeout,
                backoff_sec=5,
                err_msg=f"Topic {self.spec.topic} did not appear on target cluster within {progress_timeout} seconds",
            )
            self.verifier.start()
            success, error = self.verifier.wait_and_verify(
                progress_timeout=progress_timeout
            )
        except Exception as e:
            self.logger.error(f"Workload for topic: {self.spec.topic} failed: {e}")
            success = False
            error = str(e)
        return ClusterLinkingWorkloadResult(self.spec.topic, success, error)


class ClusterLinkingWorkloadManager:
    def __init__(
        self,
        test_context: TestContext,
        source_cluster: Cluster,
        target_cluster: RedpandaCluster,
        workload_specs: list[ClusterLinkingWorkloadSpec],
        preallocated_nodes: list[ClusterNode],
        logger: Logger,
    ):
        self.test_context = test_context
        self.source_cluster = source_cluster
        self.target_cluster = target_cluster
        self.logger: Logger = logger
        self.workload_specs: list[ClusterLinkingWorkloadSpec] = workload_specs
        self.workers: list[ClusterLinkingWorkloadWorker] = []
        self.futures: list[Future[ClusterLinkingWorkloadResult]] = []
        self.executor = ThreadPoolExecutor(max_workers=len(workload_specs))
        self.preallocated_nodes: list[ClusterNode] = preallocated_nodes

    def start_all(self, progress_timeout: int = 60):
        for i, spec in enumerate(self.workload_specs):
            worker = ClusterLinkingWorkloadWorker(
                self.test_context,
                self.source_cluster,
                self.target_cluster,
                spec,
                [self.preallocated_nodes[i]],
                self.logger,
            )
            self.workers.append(worker)

            result = self.executor.submit(
                worker.start_and_verify, progress_timeout=progress_timeout
            )
            result.add_done_callback(
                lambda f: self.logger.info(
                    f"Workload for topic: {f.result().topic} completed. Result: (success: {f.result().is_success}, error: {f.result().error})"
                )
            )
            self.futures.append(result)

    def wait_and_verify_all(self) -> list[ClusterLinkingWorkloadResult]:
        return [f.result() for f in self.futures]


ALL_TOPIC_PROPERTIES = [
    "cleanup.policy",
    "compression.type",
    "delete.retention.ms",
    "flush.bytes",
    "flush.ms",
    "initial.retention.local.target.bytes",
    "initial.retention.local.target.ms",
    "max.compaction.lag.ms",
    "max.message.bytes",
    "message.timestamp.after.max.ms",
    "message.timestamp.before.max.ms",
    "message.timestamp.type",
    "min.cleanable.dirty.ratio",
    "min.compaction.lag.ms",
    "redpanda.iceberg.mode",
    "redpanda.remote.delete",
    "redpanda.remote.read",
    "redpanda.remote.write",
    "retention.bytes",
    "retention.local.target.bytes",
    "retention.local.target.ms",
    "retention.ms",
    "segment.bytes",
    "segment.ms",
    "write.caching",
]


class ShadowLinkingRandomOpsTest(ShadowLinkTestBase):
    def __init__(self, test_ctx: TestContext):
        self.test_context = test_ctx
        self.fi = None
        self.admin_fuzz = None
        super().__init__(
            test_ctx,
            num_brokers=5,
            num_prealloc_nodes=3,
            secondary_cluster_args=SecondaryClusterArgs(
                # TODO: enable when DR of schemas is supported
                # schema_registry_config=SchemaRegistryConfig(),
                # pandaproxy_config=PandaproxyConfig(),
                si_settings=SISettings(
                    self.test_context,
                    cloud_storage_enable_remote_read=True,
                    cloud_storage_enable_remote_write=True,
                    fast_uploads=True,
                ),
                extra_rp_conf={
                    "group_new_member_join_timeout": 3000,
                },
            ),
            extra_rp_conf={
                "default_topic_replications": 3,
                "raft_learner_recovery_rate": 512 * (1024 * 1024),
                "partition_autobalancing_mode": "node_add",
                # set disk timeout to value greater than max suspend time
                # not to emit spurious errors
                "raft_io_timeout_ms": 20000,
                "compacted_log_segment_size": 1024 * 1024,
                "log_segment_size": 2 * 1024 * 1024,
                # to speed up th test we set the retention trim interval to 5 seconds,
                # this way the disk size information will be updated more frequently
                "retention_local_trim_interval": 5000,
                "partition_autobalancing_tick_interval_ms": 2000,
                "group_new_member_join_timeout": 3000,
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            si_settings=SISettings(
                self.test_context,
                cloud_storage_enable_remote_read=True,
                cloud_storage_enable_remote_write=True,
                fast_uploads=True,
                skip_end_of_test_scrubbing=True,
            ),
        )

    def teardown(self) -> None:
        if self.fi:
            self.fi.stop()
            self.fi = None
        if self.admin_fuzz:
            self.admin_fuzz.stop()
            self.admin_fuzz = None
        return super().teardown()

    def setup_scale(self):
        # TODO: adjust scale for CDT tests
        if is_debug_mode():
            self.bytes_per_topic = 20 * 1024 * 1024
            self.msg_size = 256
            self.msg_count = self.bytes_per_topic // self.msg_size
            self.total_node_ops = 3
            self.partition_count = 3
        else:
            self.bytes_per_topic = 100 * 1024 * 1024
            self.msg_size = 1024
            self.msg_count = self.bytes_per_topic // self.msg_size
            self.total_node_ops = 5
            self.partition_count = 12

    @cluster(num_nodes=11)
    @matrix(failures=[False, True])
    def test_node_operations(self, failures: bool):
        self.setup_scale()

        req = self.create_default_link_request("rnot-link")
        for prop in ALL_TOPIC_PROPERTIES:
            req.shadow_link.configurations.topic_metadata_sync_options.synced_shadow_topic_properties.append(
                prop
            )
        self.create_link_with_request(req)

        lock = threading.Lock()
        if failures:
            self.fi = FailureInjectorBackgroundThread(
                self.redpanda,
                self.logger,
                max_suspend_duration_seconds=4,
                lock=lock,
                min_inter_failure_time=20,
                max_inter_failure_time=40,
            )
            self.fi.start()

        manager = ClusterLinkingWorkloadManager(
            self.test_context,
            self.source_cluster,
            self.target_cluster,
            [
                ClusterLinkingWorkloadSpec(
                    topic="si-topic",
                    topic_properties={
                        "cleanup.policy": "delete",
                        "retention.bytes": "1024000",
                        "segment.bytes": f"{1024 * 1024}",
                    },
                    partition_count=self.partition_count,
                    msg_count=self.msg_count,
                    msg_size=self.msg_size,
                ),
                ClusterLinkingWorkloadSpec(
                    topic="compacted-topic",
                    topic_properties={
                        "cleanup.policy": "compact",
                        "segment.bytes": f"{1024 * 1024}",
                    },
                    partition_count=self.partition_count,
                    msg_count=self.msg_count,
                    msg_size=self.msg_size,
                    producer_properties={
                        "key_set_cardinality": 600,
                        "tombstone_probability": 0.4,
                    },
                    consumer_properties={
                        "compacted": True,
                    },
                    use_compaction=True,
                ),
                ClusterLinkingWorkloadSpec(
                    topic="topic-txns",
                    # transactions, use a smaller topic to avoid long test times
                    msg_count=math.floor(self.msg_count / 10),
                    msg_size=self.msg_size,
                    partition_count=1,
                    use_transactions=True,
                    producer_properties={
                        "transaction_abort_rate": 0.1,
                        "msgs_per_transaction": 50,
                        "debug_logs": True,
                    },
                    consumer_properties={},
                    validate_number_of_messages_on_target=True,
                ),
            ],
            self.preallocated_nodes,
            self.logger,
        )

        manager.start_all(progress_timeout=120)

        self.admin_fuzz = AdminOperationsFuzzer(
            self.source_cluster.service,
            min_replication=3,
            operations_interval=3,
            retries_interval=10,
            retries=10,
        )

        self.admin_fuzz.start()
        active_node_idxs = {self.redpanda.idx(n) for n in self.redpanda.nodes}

        # main workload loop
        executor = NodeOpsExecutor(
            self.redpanda,
            self.logger,
            lock,
            progress_timeout=120,
        )

        for i, op in enumerate(
            generate_random_workload(available_nodes=active_node_idxs)
        ):
            if i >= self.total_node_ops:
                break
            self.logger.info(f"starting operation {i + 1}/{self.total_node_ops}")
            executor.execute_operation(op)

        self.admin_fuzz.wait(20, 180)
        self.admin_fuzz.stop()
        self.admin_fuzz = None

        results = manager.wait_and_verify_all()

        if self.fi:
            self.fi.stop()
            self.fi = None

        for r in results:
            assert r.is_success, f"Workload manager failed: {r.error}"

        def topics_are_in_sync():
            source_topics = self.source_cluster_rpk.list_topics()

            for topic in source_topics:
                src_cfg = self.source_cluster_rpk.describe_topic_configs(topic)
                target_cfg = self.target_cluster_rpk.describe_topic_configs(topic)
                for property, value in src_cfg.items():
                    if property in SKIPPED_TOPIC_PROPERTIES:
                        continue
                    if value[0] != target_cfg[property][0]:
                        self.logger.warning(
                            f"Topic config mismatch for topic {topic} {property} = {value[0]} != {property} = {target_cfg[property][0]}"
                        )
                        return False
                src_partitions = self.source_cluster_rpk.describe_topic(topic)
                target_partitions = self.target_cluster_rpk.describe_topic(topic)
                for src, target in zip(src_partitions, target_partitions):
                    if src.high_watermark != target.high_watermark:
                        self.logger.warning(
                            f"High watermark mismatch for topic {topic} partition {src.id}"
                        )
                        return False
            return True

        wait_until(
            topics_are_in_sync,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="Topic properties not in sync after completion",
        )
