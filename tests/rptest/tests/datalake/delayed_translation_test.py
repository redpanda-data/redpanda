# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.tests.datalake.catalog_service_factory import supported_catalog_types
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineBase, QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result
from rptest.utils.parallel import execute_in_parallel
from rptest.utils.rpcn_utils import counter_stream_config


class DatalakeDelayedTranslationTest(RedpandaTest):
    AVRO_SCHEMA_STR = """
{
    "type": "record",
    "name": "EmptyEvent",
    "fields": [
        {
            "name": "weight",
            "type": {
                "type": "array",
                "items": "int"
            }
        }
    ]
}
    """
    TOPIC_NAME = "topic"
    SCHEMA_NAME = "schema"

    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeDelayedTranslationTest, self).__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(
                test_context=test_ctx,
                log_segment_size=10 * 1024,
                cloud_storage_max_connections=5,
                cloud_storage_enable_remote_read=True,
                cloud_storage_enable_remote_write=True,
            ),
            extra_rp_conf={"iceberg_enabled": False},
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )

    def setUp(self):
        pass  # redpanda will be started by DatalakeServices

    def _partition_count(self):
        return 1000 if self.redpanda.dedicated_nodes else 100

    def _for_each_partition(self, fn) -> list:
        return execute_in_parallel(range(self._partition_count()), fn)

    def _get_partition_local_starts(self, pids) -> list[int]:
        admin = Admin(self.redpanda)
        return [
            admin.get_partition_cloud_storage_status(self.TOPIC_NAME, pid)[
                "local_log_start_offset"
            ]
            for pid in pids
        ]

    def _query_minmax(self, query_engine: QueryEngineBase):
        with query_engine.run_query(f"""
            SELECT redpanda.partition pid,
                    min(redpanda.offset) min_offset,
                    max(redpanda.offset) max_offset
            FROM redpanda.{query_engine.escape_identifier(self.TOPIC_NAME)}
            GROUP BY 1
        """) as cursor:
            last_query_result = {
                row[0]: dict(min_offset=row[1], max_offset=row[2]) for row in cursor
            }
            self.redpanda.logger.debug(f"{last_query_result=}")
            return last_query_result

    def produce_until_result(self, *args, **kwargs):
        try:
            self._connect.start_stream(
                name="ducky_stream",
                config=counter_stream_config(
                    self.redpanda,
                    self.TOPIC_NAME,
                    self.SCHEMA_NAME,
                    {"weight": "range(0, 10000)"},
                    0,
                ),
            )
            return wait_until_result(*args, **kwargs)
        finally:
            self._connect.stop_stream("ducky_stream", should_finish=False)

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK, QueryEngineType.TRINO],
        catalog_type=supported_catalog_types(),
    )
    def test_basic(
        self, cloud_storage_type, query_engine: QueryEngineType, catalog_type
    ):
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            catalog_type=catalog_type,
            include_query_engines=[query_engine],
        ) as dl:
            query_engine_inst = dl.service(engine_type=query_engine)
            assert query_engine_inst is not None

            wait_until(
                self.redpanda.all_up,
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Failed waiting for redpanda to become available",
            )

            rpk = RpkTool(self.redpanda)
            rpk.create_topic(
                self.TOPIC_NAME,
                self._partition_count(),
                config={
                    "redpanda.remote.read": "true",
                    "redpanda.remote.write": "true",
                    # in hope there is some kafka data in 10k worth of logs
                    "retention.local.target.bytes": 10 * 1024,
                    "cleanup.policy": "delete",
                },
            )
            rpk.create_schema_from_str(self.SCHEMA_NAME, self.AVRO_SCHEMA_STR)

            self._connect = RedpandaConnectService(self.test_context, self.redpanda)
            self._connect.start()

            # produce until all partitions are GCed locally

            def is_truncated():
                log_starts = self._for_each_partition(self._get_partition_local_starts)
                self.redpanda.logger.debug(f"{log_starts=}")
                return all(s > 0 for s in log_starts), log_starts

            log_starts = self.produce_until_result(
                is_truncated,
                timeout_sec=600,
                backoff_sec=15,
                err_msg="Failed waiting for all partitions to be GCed",
            )

            # enable iceberg

            self.redpanda.set_cluster_config(
                {
                    "iceberg_enabled": True,
                    "iceberg_catalog_commit_interval_ms": 5000,
                    "iceberg_target_lag_ms": 5000,
                },
                expect_restart=True,
            )

            for k, v in {
                TopicSpec.PROPERTY_ICEBERG_MODE: "key_value",
                TopicSpec.PROPERTY_ICEBERG_TARGET_LAG_MS: 10000,
            }.items():
                rpk.alter_topic_config(self.TOPIC_NAME, k, v)

            # check translation starts for all partitions

            iceberg_max_offsets = []

            def translation_started():
                try:
                    last_query_results = self._query_minmax(query_engine_inst)
                except Exception as e:
                    # can happen if no data has made its way there yet
                    self.redpanda.logger.info(f"Error querying: {e}")
                    return False

                for pid, query_result in last_query_results.items():
                    assert query_result["min_offset"] >= log_starts[pid]

                n_translated = len(last_query_results)
                n_total = self._partition_count()
                self.redpanda.logger.info(
                    f"{n_translated} of {n_total} partitions translated"
                )
                return n_translated == n_total, last_query_results

            query_results_when_translated = wait_until_result(
                translation_started,
                timeout_sec=600,
                backoff_sec=1,
                err_msg="Failed waiting for all partitions to be translated",
            )
            iceberg_max_offsets = {
                pid: res["max_offset"]
                for pid, res in query_results_when_translated.items()
            }
            self.redpanda.logger.debug(f"{iceberg_max_offsets=}")

            # check it carries on if we produce more

            def translation_continues():
                # must work exception-free this time
                last_query_result = self._query_minmax(query_engine_inst)
                state = (
                    {
                        "pid": pid,
                        "old": old_max_offset,
                        "new": last_query_result[pid]["max_offset"],
                    }
                    for pid, old_max_offset in iceberg_max_offsets.items()
                )
                stuck = [d for d in state if d["new"] <= d["old"]]
                self.redpanda.logger.info(f"partitions not progressed: {stuck}")
                return stuck == []

            self.produce_until_result(
                translation_continues,
                timeout_sec=600,
                backoff_sec=1,
                err_msg="Failed waiting for partitions translation continuing",
            )

            # need more time to shut down

            for node in self.redpanda.nodes:
                self.redpanda.stop_node(node, timeout=120)
