# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import math
import random
import re
import threading
from enum import Enum
from typing import Any
from logging import Logger

from ducktape.cluster.cluster import ClusterNode
from ducktape.tests.test import TestContext

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.default import DefaultClient
from rptest.clients.offline_log_viewer import OfflineLogViewer
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer
from rptest.services.apache_iceberg_catalog import IcebergRESTCatalog
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierConsumerGroupConsumer,
    KgoVerifierProducer,
)
from rptest.services.catalog_service import CatalogService
from rptest.services.redpanda import (
    CHAOS_LOG_ALLOW_LIST,
    PREV_VERSION_LOG_ALLOW_LIST,
    CloudStorageType,
    LoggingConfig,
    PandaproxyConfig,
    RedpandaService,
    SISettings,
    SchemaRegistryConfig,
    get_cloud_storage_type,
    CLOUD_TOPICS_CONFIG_STR,
)
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.mode_checks import (
    cleanup_on_early_exit,
)
from rptest.utils.node_operations import (
    FailureInjectorBackgroundThread,
    NodeOpsExecutor,
    generate_random_workload,
    verify_offset_translator_state_consistent,
)

TS_LOG_ALLOW_LIST = [
    re.compile(".*archival_metadata_stm.*Replication wait for archival STM timed out"),
    # v23.2.x specific log
    re.compile(".*archival_metadata_stm.*Can't apply override"),
    # topic deletion may happen before data were uploaded
    re.compile(".*cloud_storage.*Failed to fetch manifest during finalize().*"),
    re.compile(".*archival -.* System error during SSL read:.*"),
    # When Redpanda is suspended manifest download may fail with timeout
    # ERROR 2025-06-03 09:48:14,461 [shard 0:main] raft - [group_id:316, {kafka/tp-workload-fast/27}] state_machine_manager.cc:385 - error applying raft snapshot - std::runtime_error (couldn't download manifest: cloud_storage::error_outcome:2)
    re.compile(
        """.*state_machine_manager.* error applying raft snapshot - std::runtime_error \\(couldn't download manifest: cloud_storage::error_outcome:2\\).*"""
    ),
]


class CompactionMode(str, Enum):
    SLIDING_WINDOW = "sliding_window"
    CHUNKED_SLIDING_WINDOW = "chunked_sliding_window"
    ADJACENT_MERGE = "adjacent_merge"


RNOT_ALLOW_LIST = CHAOS_LOG_ALLOW_LIST + PREV_VERSION_LOG_ALLOW_LIST + TS_LOG_ALLOW_LIST


class RandomNodeOperationsBase(PreallocNodesTest):
    def __init__(
        self,
        test_context: TestContext,
        *args: Any,
        is_smoke_test: bool,
        **kwargs: Any,
    ):
        super().__init__(
            test_context=test_context,
            num_brokers=5,
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
            },
            # 2 nodes for kgo producer/consumer workloads
            node_prealloc_count=3,
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            *args,
            **kwargs,
        )

        self.admin_fuzz = None
        self.should_skip = False
        self.is_smoke_test = is_smoke_test
        self.nodes_with_prev_version = []
        self.installer = self.redpanda._installer
        self.previous_version = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD
        )
        self._si_settings = SISettings(
            self.test_context,
            cloud_storage_enable_remote_read=True,
            cloud_storage_enable_remote_write=True,
            fast_uploads=True,
        )
        self.catalog_service = IcebergRESTCatalog(
            test_context,
            cloud_storage_bucket=self._si_settings.cloud_storage_bucket,
            filesystem_wrapper_mode=False,
        )

    def min_producer_records(self):
        return 20 * self.producer_throughput

    def _create_topics(self, count: int):
        def rand_topic():
            return TopicSpec(
                partition_count=random.randint(1, self.max_partitions),
                replication_factor=3,
            )

        for spec in [rand_topic() for _ in range(count)]:
            DefaultClient(self.redpanda).create_topic(spec)

    def tearDown(self):
        if self.admin_fuzz is not None:
            self.admin_fuzz.stop()

        return super().tearDown()

    def early_exit_hook(self):
        """
        Hook for `skip_debug_mode` decorator
        """
        if self.redpanda:
            self.redpanda.set_skip_if_no_redpanda_log(True)

    def setUp(self):
        self.catalog_service.start()

    def _setup_test_scale(self):
        # test setup
        self.producer_timeout = 180
        self.consumer_timeout = 180

        self.topic_count = 10
        self.max_partitions = 32

        if self.redpanda.dedicated_nodes:
            # scale test setup
            self.producer_throughput = 20000
            self.node_operations = 30
            self.msg_size = 1024  # 1KiB
            self.rate_limit = 100 * 1024 * 1024  # 100 MBps
            self.total_data = 5 * 1024 * 1024 * 1024
        else:
            self.producer_throughput = 1000 if self.debug_mode else 10000
            self.node_operations = 10
            self.msg_size = 128
            self.rate_limit = 1024 * 1024
            self.total_data = 50 * 1024 * 1024

        # if we are in smoke_test mode, reduce the scale of the test by a lot
        # to make it go fast, primarily by dropping the number of partitions
        if self.is_smoke_test:
            self.max_partitions = 2
            # for an even faster test, while iterating, reduce the number of
            # operations to 1
            self.node_operations = 10

        # Tip off the end-of-test controller log validation that we will
        # create a large number of records, scaling with partition count
        # and operation count.
        self.redpanda.set_expected_controller_records(
            self.max_partitions * self.node_operations * self.topic_count
        )

        self.consumers_count = math.ceil(self.max_partitions / 4)
        assert self.consumers_count > 0
        self.msg_count = int(self.total_data / self.msg_size)
        assert self.msg_count > 0

        self.logger.info(
            f"running test with: [message_size {self.msg_size},  total_bytes: {self.total_data}, message_count: {self.msg_count}, rate_limit: {self.rate_limit}, cluster_operations: {self.node_operations}]"
        )

        self.logger.info(f"RandomNodeOperationsBase object after setup: {self}")

    def _start_redpanda(
        self,
        mixed_versions: bool,
        with_iceberg: bool,
        compaction_mode: CompactionMode,
        with_cloud_topics: bool,
    ):
        # since this test is deleting topics we must tolerate missing manifests
        self._si_settings.set_expected_damage(
            {"ntr_no_topic_manifest", "ntpr_no_manifest"}
        )
        self.redpanda.set_si_settings(self._si_settings)

        if with_iceberg:
            self.redpanda.add_extra_rp_conf(
                {
                    "iceberg_enabled": "true",
                    "iceberg_catalog_type": "rest",
                    "iceberg_rest_catalog_endpoint": self.catalog_service.iceberg_rest_url,
                    "iceberg_rest_catalog_client_id": "panda-user",
                    "iceberg_rest_catalog_client_secret": "panda-secret",
                }
            )

        if with_cloud_topics:
            self.redpanda.add_extra_rp_conf(
                {
                    CLOUD_TOPICS_CONFIG_STR: True,
                    # Set both compaction intervals for cloud topics compaction tests
                    "log_compaction_interval_ms": 5000,
                    "cloud_topics_compaction_interval_ms": 5000,
                },
            )

        if compaction_mode == CompactionMode.CHUNKED_SLIDING_WINDOW:
            # This may not be recognized on certain nodes in mixed-version run.
            environment = {"__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"}
            self.redpanda.set_environment(environment)
            # Use 3 KiB of memory for compaction map, which should force chunked compaction.
            self.redpanda.add_extra_rp_conf(
                {"storage_compaction_key_map_memory": 3 * 1024}
            )
        elif compaction_mode == CompactionMode.ADJACENT_MERGE:
            self.redpanda.add_extra_rp_conf(
                {"log_compaction_use_sliding_window": False}
            )

        self.redpanda.set_seed_servers(self.redpanda.nodes)
        if mixed_versions:
            node_count = len(self.redpanda.nodes)
            with_prev_version = math.ceil(node_count / 2.0)
            self.logger.info(
                f"Using cluster with mixed versions with {node_count - with_prev_version} nodes using current HEAD version {RedpandaInstaller.HEAD} and {with_prev_version} using previous version: {self.previous_version}"
            )

            self.nodes_with_prev_version = self.redpanda.nodes[:with_prev_version]
            self.installer.install(self.nodes_with_prev_version, self.previous_version)

            self.redpanda.set_seed_servers(self.nodes_with_prev_version)
            self.redpanda.start(auto_assign_node_id=True, omit_seeds_on_idx_one=False)
        else:
            self.redpanda.start(auto_assign_node_id=True, omit_seeds_on_idx_one=False)

        self.redpanda.await_feature(
            "membership_change_controller_cmds", "active", timeout_sec=30
        )

        if with_cloud_topics:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

    def _alter_local_topic_retention_bytes(self, topic: str, retention_bytes: int):
        rpk = RpkTool(self.redpanda)

        def alter_and_verify():
            try:
                rpk.alter_topic_config(
                    topic,
                    TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
                    retention_bytes,
                )

                cfgs = rpk.describe_topic_configs(topic)
                retention = int(
                    cfgs[TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES][0]
                )
                return retention == retention_bytes
            except Exception:
                return False

        wait_until(alter_and_verify, 15, 0.5)

    class producer_consumer:
        def __init__(
            self,
            test_context: TestContext,
            logger: Logger,
            topic_name: str,
            redpanda: RedpandaService,
            nodes: list[ClusterNode],
            msg_size: int,
            rate_limit_bps: int,
            msg_count: int,
            consumers_count: int,
            compaction_enabled: bool = False,
            key_set_cardinality: int | None = None,
            tolerate_data_loss: bool = False,
            iceberg_enabled: bool = False,
            catalog_service: CatalogService | None = None,
        ):
            self.test_context = test_context
            self.logger = logger
            self.topic = topic_name
            self.redpanda = redpanda
            self.nodes = nodes
            self.msg_size = msg_size
            self.rate_limit_bps = rate_limit_bps
            self.msg_count = msg_count
            self.consumer_count = consumers_count
            self.compaction_enabled = compaction_enabled
            self.key_set_cardinality = key_set_cardinality
            self.tolerate_data_loss = tolerate_data_loss
            self.iceberg_enabled = iceberg_enabled
            self.catalog_service = catalog_service

        def _start_producer(self, clean: bool):
            self.producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic,
                self.msg_size,
                self.msg_count,
                custom_node=self.nodes,
                rate_limit_bps=self.rate_limit_bps,
                key_set_cardinality=self.key_set_cardinality,
                tolerate_data_loss=self.tolerate_data_loss,
            )

            self.producer.start(clean=clean)

            wait_until(
                lambda: self.producer.produce_status.acked > 10,
                timeout_sec=120,
                backoff_sec=1,
            )
            self.producer.wait_for_offset_map()

        def _start_consumer(self, with_logs: bool = False):
            self.consumer = KgoVerifierConsumerGroupConsumer(
                self.test_context,
                self.redpanda,
                self.topic,
                self.msg_size,
                readers=self.consumer_count,
                nodes=self.nodes,
                debug_logs=with_logs,
                trace_logs=with_logs,
                compacted=self.compaction_enabled,
                tolerate_data_loss=self.tolerate_data_loss,
            )

            self.consumer.start(clean=False)

        def start(self, clean: bool):
            self.logger.info(
                f"starting workload: topic: {self.topic}, with [rate_limit: {self.rate_limit_bps}, message size: {self.msg_size}, message count: {self.msg_count}]"
            )

            self._start_producer(clean=clean)
            self._start_consumer()

        def verify(self):
            self.logger.info(
                f"verifying workload: topic: {self.topic}, with [rate_limit: {self.rate_limit_bps}, message size: {self.msg_size}, message count: {self.msg_count}]"
            )
            self.producer.wait()

            assert (
                self.producer.produce_status.bad_offsets == 0 or self.tolerate_data_loss
            )
            # Await the consumer that is reading only the subset of data that
            # was written before it started.
            self.consumer.wait()

            assert self.consumer.consumer_status.validator.invalid_reads == 0, (
                f"Invalid reads in topic: {self.topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"
            )
            del self.consumer

            # Start a new consumer to read all data written
            self._start_consumer(with_logs=True)
            self.consumer.wait()
            if not self.compaction_enabled:
                assert (
                    self.consumer.consumer_status.validator.valid_reads
                    >= self.producer.produce_status.acked
                ), (
                    f"Missing messages from topic: {self.topic}. valid reads: {self.consumer.consumer_status.validator.valid_reads}, acked messages: {self.producer.produce_status.acked}"
                )

            assert self.consumer.consumer_status.validator.invalid_reads == 0, (
                f"Invalid reads in topic: {self.topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"
            )

            if self.iceberg_enabled:
                # Perform basic iceberg verification: ensure the table exists and has translated data
                assert self.catalog_service, (
                    "Expected catalog service to have a value when iceberg is enabled for verification purposes"
                )
                client = self.catalog_service.client()
                namespace = "redpanda"

                def verify_iceberg_table() -> bool:
                    try:
                        tables = client.list_tables(namespace)
                        if (namespace, self.topic) not in tables:
                            self.logger.debug(f"Table {self.topic} not yet in catalog")
                            return False

                        table = client.load_table(f"{namespace}.{self.topic}")
                        df = table.scan().to_pandas()

                        if df.empty:
                            self.logger.debug(f"Table {self.topic} has no rows yet")
                            return False

                        max_offset = df["redpanda"].str["offset"].max()

                        self.logger.info(
                            f"Iceberg table {self.topic}: rows={len(df)}, max_offset={max_offset}"
                        )
                        return max_offset is not None and max_offset > 0
                    except Exception as e:
                        self.logger.debug(
                            f"Error verifying iceberg table {self.topic}: {e}"
                        )
                        return False

                wait_until(
                    verify_iceberg_table,
                    timeout_sec=120,
                    backoff_sec=1,
                    err_msg=f"Iceberg verification failed for topic {self.topic}",
                )

    def maybe_enable_iceberg_for_topic(self, topic_name: str, iceberg_enabled: bool):
        if iceberg_enabled:
            client = DefaultClient(self.redpanda)
            client.alter_topic_config(
                topic_name, TopicSpec.PROPERTY_ICEBERG_MODE, "key_value"
            )

    def _do_test_node_operations(
        self,
        enable_failures: bool,
        mixed_versions: bool,
        with_iceberg: bool,
        compaction_mode: CompactionMode,
        cloud_storage_type: CloudStorageType,
    ):
        # In order to reduce the number of parameters and at the same time cover
        # as many use cases as possible this test uses 3 topics which 3 separate
        # producer/consumer pairs:
        #
        # tp-workload-deletion   - topic with delete cleanup policy
        # tp-workload-compaction - topic with compaction
        # tp-workload-fast       - topic with fast partition movements enabled
        if with_iceberg:
            if mixed_versions:
                self.should_skip = True
                self.logger.info(
                    "Skipping test with iceberg and mixed versions as it is not supported"
                )
            cloud_storage_types = supported_storage_types()
            if cloud_storage_type not in cloud_storage_types:
                self.should_skip = True
                self.logger.info(
                    "Skipping test with iceberg and unsupported cloud storage type"
                )

        with_cloud_topics = True
        if mixed_versions:
            with_cloud_topics = False
            self.logger.info("Disabling cloud topics in mixed version test")

        def enable_write_caching_testing():
            if not mixed_versions:
                return True
            # Write caching feature is available 24.x and later.
            pre_upgrade_version = (
                self.redpanda._installer.highest_from_prior_feature_version(
                    RedpandaInstaller.HEAD
                )
            )
            return pre_upgrade_version[0] >= 24

        lock = threading.Lock()
        default_segment_size = 1024 * 1024

        # setup test case scale parameters
        self._setup_test_scale()

        if self.should_skip:
            cleanup_on_early_exit(self)
            return

        # start redpanda process
        self._start_redpanda(
            mixed_versions,
            with_iceberg=with_iceberg,
            compaction_mode=compaction_mode,
            with_cloud_topics=with_cloud_topics,
        )

        self.redpanda.set_cluster_config({"controller_snapshot_max_age_sec": 1})

        if with_iceberg:
            self.redpanda.set_cluster_config(
                {"iceberg_catalog_commit_interval_ms": 10000}
            )

        client = DefaultClient(self.redpanda)

        # wait until schema registry topic is created
        # to avoid topic creation errors during restarts
        def schema_registry_topic_created():
            rpk = RpkTool(self.redpanda)
            try:
                rpk.list_schemas()
            except Exception:
                return False
            return True

        wait_until(
            schema_registry_topic_created,
            180,
            backoff_sec=2,
            err_msg="Error waiting for schema registry topic to be created",
        )

        # create some initial topics
        self._create_topics(self.topic_count)
        regular_topic = TopicSpec(
            name="tp-workload-deletion",
            partition_count=self.max_partitions,
            replication_factor=3,
            cleanup_policy=TopicSpec.CLEANUP_DELETE,
            segment_bytes=default_segment_size,
            # disable remote reads/writes in this topic as
            # the fast partition movement one has them enabled
            redpanda_remote_read=False,
            redpanda_remote_write=False,
        )
        client.create_topic(regular_topic)
        self.maybe_enable_iceberg_for_topic(regular_topic.name, with_iceberg)

        # change local retention policy to make some local segments will be deleted during the test
        self._alter_local_topic_retention_bytes(
            regular_topic.name, 3 * default_segment_size
        )

        regular_producer_consumer = RandomNodeOperationsBase.producer_consumer(
            test_context=self.test_context,
            logger=self.logger,
            topic_name=regular_topic.name,
            redpanda=self.redpanda,
            nodes=[self.preallocated_nodes[0]],
            msg_size=self.msg_size,
            rate_limit_bps=self.rate_limit,
            msg_count=self.msg_count,
            consumers_count=self.consumers_count,
            compaction_enabled=False,
            iceberg_enabled=with_iceberg,
            catalog_service=self.catalog_service,
        )

        compacted_topic = TopicSpec(
            name="tp-workload-compaction",
            partition_count=self.max_partitions,
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
            segment_bytes=default_segment_size,
            redpanda_remote_read=True,
            redpanda_remote_write=True,
        )
        client.create_topic(compacted_topic)
        self.maybe_enable_iceberg_for_topic(compacted_topic.name, with_iceberg)

        compacted_producer_consumer = RandomNodeOperationsBase.producer_consumer(
            test_context=self.test_context,
            logger=self.logger,
            topic_name=compacted_topic.name,
            redpanda=self.redpanda,
            nodes=[self.preallocated_nodes[1]],
            msg_size=self.msg_size,
            rate_limit_bps=self.rate_limit,
            msg_count=self.msg_count,
            consumers_count=self.consumers_count,
            key_set_cardinality=500,
            compaction_enabled=True,
            iceberg_enabled=with_iceberg,
            catalog_service=self.catalog_service,
        )

        regular_producer_consumer.start(clean=True)
        compacted_producer_consumer.start(clean=True)

        # if running with tiered storage create a topic with fast partition
        # moves enabled
        fast_topic = TopicSpec(
            name="tp-workload-fast",
            partition_count=self.max_partitions,
            cleanup_policy=TopicSpec.CLEANUP_DELETE,
            segment_bytes=default_segment_size,
            redpanda_remote_read=True,
            redpanda_remote_write=True,
        )

        client.create_topic(fast_topic)

        client.alter_topic_config(
            fast_topic.name,
            "initial.retention.local.target.bytes",
            default_segment_size,
        )
        self._alter_local_topic_retention_bytes(
            fast_topic.name, 8 * default_segment_size
        )
        self.maybe_enable_iceberg_for_topic(fast_topic.name, with_iceberg)
        fast_producer_consumer = RandomNodeOperationsBase.producer_consumer(
            test_context=self.test_context,
            logger=self.logger,
            topic_name=fast_topic.name,
            redpanda=self.redpanda,
            nodes=[self.preallocated_nodes[2]],
            msg_size=self.msg_size,
            rate_limit_bps=self.rate_limit,
            msg_count=self.msg_count,
            consumers_count=self.consumers_count,
            compaction_enabled=False,
            iceberg_enabled=with_iceberg,
            catalog_service=self.catalog_service,
        )
        fast_producer_consumer.start(clean=True)

        cloud_topics_delete_consumer = RandomNodeOperationsBase.producer_consumer(
            test_context=self.test_context,
            logger=self.logger,
            topic_name="tp-workload-ct-delete",
            redpanda=self.redpanda,
            nodes=[self.preallocated_nodes[2]],
            msg_size=self.msg_size,
            rate_limit_bps=self.rate_limit,
            msg_count=self.msg_count,
            consumers_count=self.consumers_count,
            compaction_enabled=False,
            iceberg_enabled=with_iceberg,
            catalog_service=self.catalog_service,
        )

        cloud_topics_compact_consumer = RandomNodeOperationsBase.producer_consumer(
            test_context=self.test_context,
            logger=self.logger,
            topic_name="tp-workload-ct-compact",
            redpanda=self.redpanda,
            nodes=[self.preallocated_nodes[2]],
            msg_size=self.msg_size,
            rate_limit_bps=self.rate_limit,
            msg_count=self.msg_count,
            consumers_count=self.consumers_count,
            compaction_enabled=True,
            iceberg_enabled=with_iceberg,
            catalog_service=self.catalog_service,
        )

        tiered_cloud_delete_consumer = RandomNodeOperationsBase.producer_consumer(
            test_context=self.test_context,
            logger=self.logger,
            topic_name="tp-workload-tc-delete",
            redpanda=self.redpanda,
            nodes=[self.preallocated_nodes[2]],
            msg_size=self.msg_size,
            rate_limit_bps=self.rate_limit,
            msg_count=self.msg_count,
            consumers_count=self.consumers_count,
            compaction_enabled=False,
            iceberg_enabled=with_iceberg,
            catalog_service=self.catalog_service,
        )

        tiered_cloud_compact_consumer = RandomNodeOperationsBase.producer_consumer(
            test_context=self.test_context,
            logger=self.logger,
            topic_name="tp-workload-tc-compact",
            redpanda=self.redpanda,
            nodes=[self.preallocated_nodes[2]],
            msg_size=self.msg_size,
            rate_limit_bps=self.rate_limit,
            msg_count=self.msg_count,
            consumers_count=self.consumers_count,
            compaction_enabled=True,
            iceberg_enabled=with_iceberg,
            catalog_service=self.catalog_service,
        )

        if with_cloud_topics:
            rpk = RpkTool(self.redpanda)
            rpk.create_topic(
                topic=cloud_topics_delete_consumer.topic,
                partitions=self.max_partitions,
                replicas=3,
                config={
                    "segment.bytes": default_segment_size,
                    "cleanup.policy": "delete",
                    "redpanda.remote.read": "false",
                    "redpanda.remote.write": "false",
                    TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
                },
            )
            self.maybe_enable_iceberg_for_topic(
                cloud_topics_delete_consumer.topic, with_iceberg
            )
            cloud_topics_delete_consumer.start(clean=False)

            rpk.create_topic(
                topic=cloud_topics_compact_consumer.topic,
                partitions=self.max_partitions,
                replicas=3,
                config={
                    "segment.bytes": default_segment_size,
                    "cleanup.policy": "compact",
                    "redpanda.remote.read": "false",
                    "redpanda.remote.write": "false",
                    TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
                },
            )
            self.maybe_enable_iceberg_for_topic(
                cloud_topics_compact_consumer.topic, with_iceberg
            )
            cloud_topics_compact_consumer.start(clean=False)

            rpk.create_topic(
                topic=tiered_cloud_delete_consumer.topic,
                partitions=self.max_partitions,
                replicas=3,
                config={
                    "segment.bytes": default_segment_size,
                    "cleanup.policy": "delete",
                    "redpanda.remote.read": "false",
                    "redpanda.remote.write": "false",
                    TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED_CLOUD,
                },
            )
            self.maybe_enable_iceberg_for_topic(
                tiered_cloud_delete_consumer.topic, with_iceberg
            )
            tiered_cloud_delete_consumer.start(clean=False)

            rpk.create_topic(
                topic=tiered_cloud_compact_consumer.topic,
                partitions=self.max_partitions,
                replicas=3,
                config={
                    "segment.bytes": default_segment_size,
                    "cleanup.policy": "compact",
                    "redpanda.remote.read": "false",
                    "redpanda.remote.write": "false",
                    TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED_CLOUD,
                },
            )
            self.maybe_enable_iceberg_for_topic(
                tiered_cloud_compact_consumer.topic, with_iceberg
            )
            tiered_cloud_compact_consumer.start(clean=False)

        write_caching_enabled = enable_write_caching_testing()
        write_caching_producer_consumer = None
        if write_caching_enabled:
            cleanup_policy = TopicSpec._random_cleanup_policy()
            tp_suffix = cleanup_policy.replace(",", "-")
            write_caching_topic = TopicSpec(
                name=f"tp-workload-writecaching-{tp_suffix}",
                partition_count=self.max_partitions,
                cleanup_policy=cleanup_policy,
                segment_bytes=default_segment_size,
                redpanda_remote_read=True,
                redpanda_remote_write=True,
            )

            client.create_topic(write_caching_topic)
            client.alter_topic_config(
                write_caching_topic.name, TopicSpec.PROPERTY_WRITE_CACHING, "true"
            )
            self.maybe_enable_iceberg_for_topic(write_caching_topic.name, with_iceberg)
            write_caching_producer_consumer = (
                RandomNodeOperationsBase.producer_consumer(
                    test_context=self.test_context,
                    logger=self.logger,
                    topic_name=write_caching_topic.name,
                    redpanda=self.redpanda,
                    nodes=[self.preallocated_nodes[2]],
                    msg_size=self.msg_size,
                    rate_limit_bps=self.rate_limit,
                    msg_count=self.msg_count,
                    consumers_count=self.consumers_count,
                    compaction_enabled=(cleanup_policy is not TopicSpec.CLEANUP_DELETE),
                    tolerate_data_loss=True,
                    iceberg_enabled=with_iceberg,
                    catalog_service=self.catalog_service,
                )
            )
            write_caching_producer_consumer.start(clean=False)

        # start admin operations fuzzer, it will provide a stream of
        # admin day 2 operations executed during the test
        self.admin_fuzz = AdminOperationsFuzzer(
            self.redpanda,
            min_replication=3,
            operations_interval=3,
            retries_interval=10,
            retries=10,
        )

        self.admin_fuzz.start()
        self.active_node_idxs = {self.redpanda.idx(n) for n in self.redpanda.nodes}

        fi = None
        if enable_failures:
            fi = FailureInjectorBackgroundThread(
                self.redpanda,
                self.logger,
                max_suspend_duration_seconds=4,
                lock=lock,
                min_inter_failure_time=45,
                max_inter_failure_time=90,
            )
            fi.start()

        # main workload loop
        executor = NodeOpsExecutor(
            self.redpanda,
            self.logger,
            lock,
            progress_timeout=120 if enable_failures else 60,
        )
        if with_iceberg:
            executor.override_config_params = {"iceberg_enabled": True}
        for i, op in enumerate(
            generate_random_workload(available_nodes=self.active_node_idxs)
        ):
            if i >= self.node_operations:
                break
            self.logger.info(f"starting operation {i + 1}/{self.node_operations}")
            executor.execute_operation(op)

        self.admin_fuzz.wait(20, 180)
        self.admin_fuzz.stop()

        if fi:
            fi.stop()

        # stop producer and consumer and verify results
        regular_producer_consumer.verify()
        compacted_producer_consumer.verify()

        if write_caching_enabled:
            assert write_caching_producer_consumer
            write_caching_producer_consumer.verify()

        fast_producer_consumer.verify()

        if with_cloud_topics:
            cloud_topics_delete_consumer.verify()
            cloud_topics_compact_consumer.verify()
            tiered_cloud_delete_consumer.verify()
            tiered_cloud_compact_consumer.verify()

        if mixed_versions:
            self.logger.info("Upgrading cluster with current Redpanda version")
            for n in self.nodes_with_prev_version:
                self.redpanda.stop_node(n)
            self.installer.install(self.nodes_with_prev_version, RedpandaInstaller.HEAD)
            for n in self.nodes_with_prev_version:
                self.redpanda.start_node(
                    n, omit_seeds_on_idx_one=True, auto_assign_node_id=True
                )

            def cluster_version_updated():
                admin = Admin(self.redpanda)
                admin.get_brokers()
                node_features = [
                    admin.get_features(n) for n in self.redpanda.started_nodes()
                ]
                self.logger.info(
                    f"Reported cluster versions: {[f['cluster_version'] for f in node_features]}"
                )
                return all(
                    f["cluster_version"] == f["node_latest_version"]
                    for f in node_features
                )

            wait_until(
                cluster_version_updated,
                180,
                backoff_sec=2,
                err_msg="Error waiting for cluster to report consistent version",
            )

        verify_offset_translator_state_consistent(self.redpanda)
        # Validate that the controller log written during the test is readable by offline log viewer
        log_viewer = OfflineLogViewer(self.redpanda)
        for node in self.redpanda.started_nodes():
            # stop node before reading controller log to make sure it is stable
            self.redpanda.stop_node(node)
            controller_records = log_viewer.read_controller(node=node)
            self.logger.info(
                f"Read {len(controller_records)} controller records from node {node.name} successfully"
            )
            if log_viewer.has_controller_snapshot(node):
                controller_snapshot = log_viewer.read_controller_snapshot(node)
                self.logger.info(f"Read controller snapshot: {controller_snapshot} ")

    def __str__(self):
        fields = [
            f"test_context={self.test_context!r}",
            f"admin_fuzz={self.admin_fuzz!r}",
            f"should_skip={self.should_skip!r}",
            f"is_smoke_test={self.is_smoke_test!r}",
            f"nodes_with_prev_version={self.nodes_with_prev_version!r}",
            f"previous_version={self.previous_version!r}",
            f"catalog_service={self.catalog_service!r}",
            f"producer_timeout={getattr(self, 'producer_timeout', None)!r}",
            f"consumer_timeout={getattr(self, 'consumer_timeout', None)!r}",
            f"topic_count={getattr(self, 'topic_count', None)!r}",
            f"max_partitions={getattr(self, 'max_partitions', None)!r}",
            f"producer_throughput={getattr(self, 'producer_throughput', None)!r}",
            f"node_operations={getattr(self, 'node_operations', None)!r}",
            f"msg_size={getattr(self, 'msg_size', None)!r}",
            f"rate_limit={getattr(self, 'rate_limit', None)!r}",
            f"total_data={getattr(self, 'total_data', None)!r}",
            f"consumers_count={getattr(self, 'consumers_count', None)!r}",
            f"msg_count={getattr(self, 'msg_count', None)!r}",
            f"redpanda={getattr(self, 'redpanda', None)!r}",
        ]
        return f"<RandomNodeOperationsBase {', '.join(fields)}>"


class RedpandaNodeOperationsSmokeTest(RandomNodeOperationsBase):
    """
    Smoke test for RNOT.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(
            *args,
            **kwargs,
            is_smoke_test=True,
            log_config=LoggingConfig(
                "info",
                {
                    "storage-resources": "warn",
                    "storage-gc": "trace",
                    "raft": "trace",
                    "cluster": "debug",
                    "datalake": "trace",
                    "cloud_storage": "debug",
                    "cloud_io": "debug",
                    "kafka": "debug",
                    "reconciler": "debug",
                    "cloud_topics": "debug",
                    "cloud_topics_compaction": "debug",
                    "offset_translator": "trace",
                },
            ),
        )

    @cluster(num_nodes=9, log_allow_list=RNOT_ALLOW_LIST)
    @matrix(
        cloud_storage_type=get_cloud_storage_type()[:1], mixed_versions=[True, False]
    )
    def test_node_ops_smoke_test(
        self, cloud_storage_type: CloudStorageType, mixed_versions: bool
    ):
        """
        Smoke test for node operations, run with a reduced workload and only two
        parameterizations, that we think is the most useful.
        """

        # iceberg and mixed versions are mutually incompatible, so run two
        # flavors of the smoke test, one with iceberg and one with mixed versions
        with_iceberg = not mixed_versions
        self._do_test_node_operations(
            enable_failures=True,
            mixed_versions=mixed_versions,
            with_iceberg=with_iceberg,
            # https://redpandadata.slack.com/archives/C02BDN76HUK/p1750974997157089?thread_ts=1750974846.673979&cid=C02BDN76HUK
            compaction_mode=CompactionMode.SLIDING_WINDOW,
            cloud_storage_type=cloud_storage_type,
        )
