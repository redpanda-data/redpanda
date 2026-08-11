# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import (
    Admin,
    InboundTopic,
    MigrationAction,
    NamespacedTopic,
    OutboundDataMigration,
)
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.utils.data_migrations import DataMigrationTestMixin
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.rpcn_utils import counter_stream_config
from ducktape.utils.util import wait_until


class MountUnmountIcebergTest(DataMigrationTestMixin):
    TOPIC_NAME = "ducky_topic"
    PARTITION_COUNT = 5
    FAST_COMMIT_INTVL_S = 5
    SLOW_COMMIT_INTVL_S = 60

    # Produce for as long as unmount takes until partitions get writes blocked.
    # It may take a couple of minutes, so we limit messages count to avoid
    # congestion in the verifier.
    VERY_MANY_MESSAGES = 1000000
    LOW_PRODUCTION_INTERVAL_MS = 1

    verifier_schema_avro = """
{
    "type": "record",
    "name": "VerifierRecord",
    "fields": [
        {
            "name": "verifier_string",
            "type": "string"
        },
        {
            "name": "ordinal",
            "type": "long"
        },
        {
            "name": "timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        }
    ]
}
    """

    def __init__(self, test_context):
        self._topic = None
        super(MountUnmountIcebergTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=SISettings(test_context),
            extra_rp_conf={
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": self.FAST_COMMIT_INTVL_S * 1000,
            },
            schema_registry_config=SchemaRegistryConfig(),
        )
        self.dl = DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
        )

    def avro_stream_config(self, topic, subject, cnt=3000, interval_ms=None):
        mapping = dict(
            ordinal="this",
            timestamp="timestamp_unix_milli()",
            verifier_string="uuid_v4()",
        )
        return counter_stream_config(
            self.redpanda, topic, subject, mapping, cnt, interval_ms
        )

    def setUp(self):
        self.dl.setUp()
        rpk = RpkTool(self.redpanda)
        rpk.create_schema_from_str("verifier_schema", self.verifier_schema_avro)

    def tearDown(self):
        self.dl.tearDown()

    def wait_all_partitions_consuming(self, verifier, timeout_sec=60):
        def all_partitions_started_consuming():
            consumed = verifier.max_consumed_offsets
            self.redpanda.logger.debug(f"Consumed offsets: {consumed}")
            return (
                all(offset > 0 for offset in consumed.values())
                and len(consumed) == self.PARTITION_COUNT
            )

        wait_until(
            all_partitions_started_consuming, timeout_sec=timeout_sec, backoff_sec=1
        )

    @cluster(num_nodes=6)
    @skip_debug_mode
    @matrix(cloud_storage_type=supported_storage_types())
    def test_simple_unmount(self, cloud_storage_type):
        self.dl.create_iceberg_enabled_topic(
            self.TOPIC_NAME,
            partitions=self.PARTITION_COUNT,
            replicas=3,
            iceberg_mode="value_schema_id_prefix",
            config={"redpanda.iceberg.delete": "false"},
        )
        connect = RedpandaConnectService(self.test_context, self.redpanda)
        connect.start()
        verifier = DatalakeVerifier(
            self.redpanda, self.TOPIC_NAME, self.dl.spark(), max_buffered_msgs=50000
        )

        connect.start_stream(
            name="ducky_stream",
            config=self.avro_stream_config(
                self.TOPIC_NAME,
                "verifier_schema",
                self.VERY_MANY_MESSAGES,
                self.LOW_PRODUCTION_INTERVAL_MS,
            ),
        )
        # todo make reasonable or just make sure something went through
        self.redpanda.set_cluster_config(
            {"iceberg_catalog_commit_interval_ms": self.SLOW_COMMIT_INTVL_S * 1000}
        )
        verifier.start(wait_first_iceberg_msg=True)
        self.wait_all_partitions_consuming(verifier, timeout_sec=60)

        self.admin = Admin(self.redpanda)
        ns_topic = NamespacedTopic(self.TOPIC_NAME)

        out_migration = OutboundDataMigration([ns_topic], consumer_groups=[])
        out_migration_id = self.create_and_wait(out_migration)
        self.admin.execute_data_migration_action(
            out_migration_id, MigrationAction.prepare
        )
        self.wait_for_migration_states(out_migration_id, ["prepared"])
        self.admin.execute_data_migration_action(
            out_migration_id, MigrationAction.execute
        )
        # the topic goes read-only during this wait
        self.wait_for_migration_states(out_migration_id, ["executed"])
        connect.stop_stream("ducky_stream", should_finish=False)
        # go_offline waits for consuming till migration blocking offset,
        # consume thread waits for query thread as comparison buffer size is
        # limited, and querying may lag due to translation lag
        verifier.go_offline(600)

        self.admin.execute_data_migration_action(
            out_migration_id, MigrationAction.finish
        )
        self.wait_for_migration_states(out_migration_id, ["finished"])
        self.wait_partitions_disappear([self.TOPIC_NAME])

        verifier.wait(progress_timeout_sec=10 * self.SLOW_COMMIT_INTVL_S)

    @cluster(num_nodes=6)
    @skip_debug_mode
    @matrix(cloud_storage_type=supported_storage_types())
    def test_simple_remount(self, cloud_storage_type):
        # create topic
        self.dl.create_iceberg_enabled_topic(
            self.TOPIC_NAME,
            partitions=self.PARTITION_COUNT,
            replicas=3,
            iceberg_mode="value_schema_id_prefix",
        )

        # translation to lag significantly
        self.redpanda.set_cluster_config(
            {"iceberg_catalog_commit_interval_ms": self.SLOW_COMMIT_INTVL_S * 1000}
        )

        # start ducky_stream1
        connect = RedpandaConnectService(self.test_context, self.redpanda)
        connect.start()
        connect.start_stream(
            name="ducky_stream1",
            config=self.avro_stream_config(
                self.TOPIC_NAME,
                "verifier_schema",
                self.VERY_MANY_MESSAGES,
                self.LOW_PRODUCTION_INTERVAL_MS,
            ),
        )

        wait_until(
            lambda: connect.total_records_sent("ducky_stream1") > 300,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="Timeout waiting for ducky_stream1 to produce messages",
        )

        # unmount
        self.admin = Admin(self.redpanda)
        ns_topic = NamespacedTopic(self.TOPIC_NAME)
        self.logger.info(f"unmounting {self.TOPIC_NAME}")
        self.admin.unmount_topics([ns_topic])
        self.wait_partitions_disappear([self.TOPIC_NAME])
        self.logger.info(f"unmounted {self.TOPIC_NAME}")

        # stop ducky_stream1
        connect.stop_stream("ducky_stream1", should_finish=False)

        # remount
        self.logger.info(f"remounting {self.TOPIC_NAME}")
        self.admin.mount_topics([InboundTopic(ns_topic)])
        self.wait_partitions_appear(
            [TopicSpec(name=self.TOPIC_NAME, partition_count=self.PARTITION_COUNT)]
        )
        self.logger.info(f"remounted {self.TOPIC_NAME}")

        # start verifier
        verifier = DatalakeVerifier(
            self.redpanda, self.TOPIC_NAME, self.dl.spark(), max_buffered_msgs=50000
        )
        verifier.start()

        # translation to lag less
        self.redpanda.set_cluster_config(
            {"iceberg_catalog_commit_interval_ms": self.FAST_COMMIT_INTVL_S * 1000}
        )

        # produce more data
        connect.start_stream(
            name="ducky_stream2",
            config=self.avro_stream_config(self.TOPIC_NAME, "verifier_schema", 300),
        )
        connect.stop_stream("ducky_stream2")

        # verify consistency
        verifier.wait(progress_timeout_sec=2 * self.SLOW_COMMIT_INTVL_S)
