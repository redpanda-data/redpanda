# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from typing import Any

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    PandaproxyConfig,
    SISettings,
    SchemaRegistryConfig,
)
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.tests.datalake.catalog_service_factory import supported_catalog_types
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.rpcn_utils import counter_stream_config
from rptest.utils.si_utils import NTP, BucketView

KiB = 1024
MiB = KiB * KiB

DISABLED_WRITES_ALLOWED_LOGS = [
    "Adjacent segment merging refusing to run on topic with remote.write disabled",
]


class DatalakeClusterRestoreTest(RedpandaTest):
    segment_size = 1 * MiB

    def __init__(self, test_context: TestContext):
        extra_rp_conf = {
            # Frequent cluster metadata uploads.
            "controller_snapshot_max_age_sec": 1,
            "cloud_storage_cluster_metadata_upload_interval_ms": 1000,
            # Frequent Iceberg uploads.
            "iceberg_enabled": "true",
            "iceberg_catalog_commit_interval_ms": 1000,
            "iceberg_target_lag_ms": 1000,
            # Allow tests to alter _schemas (e.g. to disable uploads).
            "kafka_nodelete_topics": [],
        }
        si_settings = SISettings(
            test_context,
            log_segment_size=self.segment_size,
            fast_uploads=True,
        )
        self.s3_bucket = si_settings.cloud_storage_bucket
        super(DatalakeClusterRestoreTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=si_settings,
            extra_rp_conf=extra_rp_conf,
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
        )
        self.rpcn = RedpandaConnectService(self.test_context, self.redpanda)

    def setUp(self):
        # NOTE: defer cluster startup to DatalakeServices.
        self.rpcn.start()

    def n_field_names(self, n: int):
        return [f"field_{n}" for n in range(n)]

    def n_avro_schema(self, n: int):
        assert n > 0, "Don't call this with no fields!"
        avro_fields = [
            f'{{"name": "{f_name}", "type": "int"}}' for f_name in self.n_field_names(n)
        ]
        avro_schema = f"""
        {{
            "type": "record",
            "name": "RecordWith{n}Fields",
            "fields": [ {",".join(avro_fields)} ]
        }}
        """
        self.redpanda.logger.info(avro_schema)
        return avro_schema

    def get_num_schemas_records(self) -> int:
        """
        Gets the number of records in the schemas topic, or 0 if the schemas
        topic does not exist.
        """
        rpk: RpkTool = RpkTool(self.redpanda)
        if "_schemas" not in rpk.list_topics():
            return 0
        admin = self.redpanda._admin
        admin.await_stable_leader("_schemas")

        total_records = 0
        for p in rpk.describe_topic("_schemas"):
            total_records += p.high_watermark
        return total_records

    def wipe_and_restart_cluster(self):
        """
        Stops all nodes in the cluster, wipes them, and restarts, effectively
        creating a brand new cluster.
        """
        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)
        self.redpanda.restart_nodes(
            self.redpanda.nodes, auto_assign_node_id=True, omit_seeds_on_idx_one=False
        )
        self.redpanda._admin.await_stable_leader(
            "controller", partition=0, namespace="redpanda", timeout_s=60, backoff_s=2
        )

    def restore_cluster_and_wait(self, topics: list[str]):
        """
        Performs a cluster restore on the cluster, and waits until all of the
        given topics appear in the cluster.
        """
        rpk: RpkTool = RpkTool(self.redpanda)
        rpk.cluster_recovery_start(wait=True)
        wait_until(
            lambda: self.has_all_topics(topics),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="Not all topics restored",
        )

    def start_structured_topic_streams(
        self,
        dl: DatalakeServices,
        create_topics: bool = True,
        topic_config: dict[str, Any] = dict(),
        create_schemas: bool = True,
    ) -> tuple[list[str], list[str]]:
        """
        Creates RPCN streams ingesting to structured structured Iceberg topics,
        optionally creating the topics and their schemas. Returns the topic
        names and stream names.
        """
        topics = []
        streams = []

        # Stream a modest volume of records. We don't want to overwhelm the
        # cluster.
        rpcn_modest_interval_ms = 50
        rpk = RpkTool(self.redpanda)
        for i in range(5):
            topic = f"structured_tp_{i}"
            n = i + 10
            n_schema = self.n_avro_schema(n)
            n_subj = f"records_w_{n}"
            if create_schemas:
                rpk.create_schema_from_str(n_subj, n_schema)

            # Create a counting stream that has the counter value in each of
            # the n fields.
            field_bloblang_map = dict()
            for field in self.n_field_names(n):
                field_bloblang_map[field] = "this"

            if create_topics:
                dl.create_iceberg_enabled_topic(
                    topic,
                    replicas=3,
                    iceberg_mode="value_schema_id_prefix",
                    config=topic_config,
                )
            cfg = counter_stream_config(
                self.redpanda,
                topic,
                n_subj,
                field_bloblang_map,
                cnt=0,  # indefinite count
                interval_ms=rpcn_modest_interval_ms,
            )
            stream = f"{topic}_stream"
            self.rpcn.start_stream(name=stream, config=cfg)
            topics.append(topic)
            streams.append(stream)
        return topics, streams

    def start_unstructured_topic_streams(
        self,
        dl: DatalakeServices,
        create_topics: bool = True,
        topic_config: dict[str, Any] = dict(),
    ) -> tuple[list[str], list[str]]:
        """
        Creates RPCN streams ingesting to non-structured Iceberg topics,
        optionally creating the topics. Returns the topic names and stream
        names.
        """
        topics = []
        streams = []

        # Send a low volume of records. We don't want to overwhelm the cluster.
        rpcn_modest_interval_ms = 100
        for i in range(5):
            topic = f"unstructured_tp_{i}"
            cfg = counter_stream_config(
                self.redpanda,
                topic,
                "",  # subject
                cnt=0,  # indefinite count
                interval_ms=rpcn_modest_interval_ms,
            )
            if create_topics:
                dl.create_iceberg_enabled_topic(
                    topic, replicas=3, iceberg_mode="key_value", config=topic_config
                )
            stream = f"{topic}_stream"
            self.rpcn.start_stream(name=stream, config=cfg)
            topics.append(topic)
            streams.append(stream)
        return topics, streams

    def start_all_topic_streams(
        self,
        dl: DatalakeServices,
        create_topics: bool = True,
        create_schemas: bool = True,
        topic_config: dict[str, Any] = dict(),
    ) -> tuple[list[str], list[str]]:
        """
        Creates a mix of structured and unstructured RPCN streams.
        """
        all_topics, all_streams = self.start_structured_topic_streams(
            dl,
            create_topics=create_topics,
            create_schemas=create_schemas,
            topic_config=topic_config,
        )
        t, s = self.start_unstructured_topic_streams(
            dl, create_topics=create_topics, topic_config=topic_config
        )
        all_topics.extend(t)
        all_streams.extend(s)
        return all_topics, all_streams

    def has_all_topics(self, topics: list[str]) -> bool:
        """
        Returns whether the cluster contains all of 'topics'.
        """
        rpk = RpkTool(self.redpanda)
        current_topics = set(rpk.list_topics())
        self.logger.info(f"Current topics: {list(current_topics)}")
        return all([t in current_topics for t in topics])

    def offsets_per_table(
        self, dl: DatalakeServices, tables: list[str]
    ) -> dict[str, int]:
        """
        Returns a map from table name to max translated offset.
        """
        offsets = dict()
        for t in tables:
            offsets[t] = int(dl.spark().max_translated_offset("redpanda", t, 0))
        self.logger.info(f"Translated offsets: {offsets}")
        return offsets

    def counts_per_table(
        self, dl: DatalakeServices, tables: list[str]
    ) -> dict[str, int]:
        """
        Returns a map from table name to count.
        """
        offsets = dict()
        for t in tables:
            offsets[t] = int(dl.spark().count_table("redpanda", t))
        self.logger.info(f"Num records: {offsets}")
        return offsets

    def tables_translated_more(
        self, dl: DatalakeServices, counts_before: dict[str, int], at_least_n: int
    ):
        """
        Return true if every table has translated at_least_n records more than
        counts_before.
        """
        topics = list(counts_before.keys())
        offsets_now = self.counts_per_table(dl, topics)
        for t, o_before in counts_before.items():
            if t not in offsets_now:
                return False
            if offsets_now[t] <= o_before + at_least_n:
                return False
        return True

    def tables_translated_higher(
        self, dl: DatalakeServices, offsets_before: dict[str, int], at_least_n: int
    ):
        """
        Return true if every table has translated at_least_n offsets higher than
        offsets_before.
        """
        topics = list(offsets_before.keys())
        offsets_now = self.offsets_per_table(dl, topics)
        for t, o_before in offsets_before.items():
            if t not in offsets_now:
                return False
            if offsets_now[t] <= o_before + at_least_n:
                return False
        return True

    def has_uploaded_schemas(self):
        schemas_hwm = self.get_num_schemas_records()
        bucket_view = BucketView(self.redpanda)
        ntp = NTP("kafka", "_schemas", 0)
        manifest = bucket_view.get_partition_manifest(ntp)
        last_offset = BucketView.kafka_last_offset(manifest)
        if last_offset is None:
            return False
        return last_offset == schemas_hwm - 1

    @cluster(num_nodes=6)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=supported_catalog_types(),
    )
    def test_basic(self, cloud_storage_type, catalog_type) -> None:
        """
        Basic test that exercises the happy path for cluster restore. Our
        assumption here is that tiered storage is keeping up with translation,
        so that after restoring we have restored schemas.
        """
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=catalog_type,
        ) as dl:
            all_topics, all_streams = self.start_all_topic_streams(dl)
            for t in all_topics:
                dl.wait_for_translation_until_offset(t, 50)
            wait_until(
                self.has_uploaded_schemas,
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Timed out waiting for schemas upload",
            )

            # Stop our streams, as we would expect a user going through a
            # restore. Otherwise, they might create schemas, etc.
            for s in all_streams:
                self.rpcn.stop_stream(s, should_finish=None)
            self.wipe_and_restart_cluster()
            self.restore_cluster_and_wait(all_topics)

            num_schemas_records = self.get_num_schemas_records()
            assert num_schemas_records > 0, (
                "Expected schemas topic to have been restored"
            )

            # Restart our workloads without creating the schemas or topics
            # explicitly.
            self.start_all_topic_streams(dl, create_schemas=False, create_topics=False)

            # Our tables should continue to be ingested to.
            offsets_snap = self.offsets_per_table(dl, all_topics)
            wait_until(
                lambda: self.tables_translated_higher(dl, offsets_snap, 50),
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Translation not progressing after restore",
            )
            assert dl.num_tables() == len(all_topics), (
                "Expected one table per topic (i.e. no DLQ tables)"
            )

    @cluster(num_nodes=6)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=supported_catalog_types(),
    )
    def test_restore_partition_spec(self, cloud_storage_type, catalog_type) -> None:
        """
        Test that upon restoring, any custom partition specs also get restored.
        """
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=catalog_type,
        ) as dl:
            pspec_prop_name = "redpanda.iceberg.partition.spec"
            partition_pspec = "(redpanda.partition)"
            topic_config = {pspec_prop_name: partition_pspec}
            all_topics, all_streams = self.start_all_topic_streams(
                dl, topic_config=topic_config
            )
            for t in all_topics:
                dl.wait_for_translation_until_offset(t, 50)

            spark = dl.spark()

            def check_expected_psec():
                for t in all_topics:
                    expected_partition_line = ("redpanda.partition", "int", None)
                    spark_describe_out = spark.run_query_fetch_all(
                        f"describe redpanda.{t}"
                    )
                    assert expected_partition_line in spark_describe_out, str(
                        spark_describe_out
                    )

            check_expected_psec()

            # Stop our streams and restore.
            for s in all_streams:
                self.rpcn.stop_stream(s, should_finish=None)
            self.wipe_and_restart_cluster()
            self.restore_cluster_and_wait(all_topics)

            # Check that after restoring, the topic properties look sane.
            rpk: RpkTool = RpkTool(self.redpanda)
            for t in all_topics:
                topic_configs = rpk.describe_topic_configs(t)
                assert pspec_prop_name in topic_configs, topic_configs
                pspec_val = topic_configs[pspec_prop_name]
                assert partition_pspec in pspec_val, f"{pspec_val} vs {partition_pspec}"

            # Sanity check that we can continue translating, and that the table
            # maintains the correct partition spec.
            offsets_snap = self.offsets_per_table(dl, all_topics)
            self.start_all_topic_streams(dl, create_schemas=False, create_topics=False)
            wait_until(
                lambda: self.tables_translated_higher(dl, offsets_snap, 50),
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Translation not progressing after restore",
            )
            check_expected_psec()

            # Anoth sanity check, nothing bad happened while translating.
            assert dl.num_tables() == len(all_topics), (
                "Expected one table per topic (i.e. no DLQ tables)"
            )

    @cluster(num_nodes=6, log_allow_list=DISABLED_WRITES_ALLOWED_LOGS)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=supported_catalog_types(),
    )
    def test_slow_tiered_storage_dlq(self, cloud_storage_type, catalog_type) -> None:
        """
        Test when tiered storage is explicitly slower than the translation
        interval, and we end up with schemas that didn't end up getting
        restored, so data that continues to be written with the original schema
        IDs get DLQed
        """
        # Reset the Redpanda tiered storage upload configs to not be so fast.
        self.redpanda.si_settings.cloud_storage_segment_max_upload_interval_sec = 600
        self.redpanda.si_settings.cloud_storage_manifest_max_upload_interval_sec = 600
        self.redpanda.set_si_settings(self.redpanda.si_settings)
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=catalog_type,
        ) as dl:
            # To make this test more robust, on top of lowering the upload
            # rate, we'll just avoid uploading the schemas topic to allow us to
            # reliably check that the schemas topic is not restored below
            # (otherwise a leadership transfer could trigger an upload).
            rpk: RpkTool = RpkTool(self.redpanda)
            rpk.create_topic(
                "_schemas", partitions=1, config={"redpanda.remote.write": "false"}
            )

            all_topics, all_streams = self.start_all_topic_streams(dl)
            for t in all_topics:
                dl.wait_for_translation_until_offset(t, 50)

            # To simulate slow tiered storage, disable writes and then
            # push more data to Iceberg.
            for t in all_topics:
                rpk.alter_topic_config(t, "redpanda.remote.write", "false")
            offsets_snap = self.counts_per_table(dl, all_topics)
            wait_until(
                lambda: self.tables_translated_more(dl, offsets_snap, 50),
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Translation not progressing after disabling uploads",
            )

            for s in all_streams:
                self.rpcn.stop_stream(s, should_finish=None)
            self.wipe_and_restart_cluster()
            self.restore_cluster_and_wait(all_topics)

            num_schemas_records = self.get_num_schemas_records()
            assert num_schemas_records == 0, (
                "Expected schemas topic to not have been restored"
            )

            # Restart our workloads without creating the schemas or topics
            # explicitly.
            structured_tables, structured_streams = self.start_structured_topic_streams(
                dl, create_topics=False, create_schemas=False
            )
            structured_counts_snap = self.counts_per_table(dl, structured_tables)

            unstructured_tables, _ = self.start_unstructured_topic_streams(
                dl, create_topics=False
            )
            unstructured_counts_snap = self.counts_per_table(dl, unstructured_tables)

            # Key-value topics should continue to write with no issues since
            # they don't depend on restored schemas.
            wait_until(
                lambda: self.tables_translated_more(dl, unstructured_counts_snap, 50),
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Translation not progressing after restore",
            )

            # Structured topics should make no progress to their main tables,
            # and instead should write to their DLQs.
            for t in structured_tables:
                dl.wait_for_translation_until_offset(f"{t}~dlq", 50)

            structured_counts_snap_after = self.counts_per_table(dl, structured_tables)
            assert structured_counts_snap == structured_counts_snap_after, (
                "Expected no translation in structured main tables:\n"
                "{structured_offsets_snap}\nvs\n{structured_offsets_snap_after}"
            )

            assert dl.num_tables() == 2 * len(structured_tables) + len(
                unstructured_tables
            ), "Expected structured tables to have DLQ tables"

            # Recreate our streams, but this time also recreate out schemas.
            # This should allow new data to be translated.
            for s in structured_streams:
                self.rpcn.stop_stream(s, should_finish=None)
            self.start_structured_topic_streams(
                dl, create_topics=False, create_schemas=True
            )

            wait_until(
                lambda: self.tables_translated_more(dl, structured_counts_snap, 50),
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Translation not progressing after restore",
            )

            # Reenable tiered storage writes to allow for end-of-test
            # scrubbing.
            for t in all_topics:
                rpk.alter_topic_config(t, "redpanda.remote.write", "true")
            rpk.alter_topic_config("_schemas", "redpanda.remote.write", "true")

    @cluster(num_nodes=6, log_allow_list=DISABLED_WRITES_ALLOWED_LOGS)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=supported_catalog_types(),
    )
    def test_slow_tiered_storage_dupe_records(
        self, cloud_storage_type, catalog_type
    ) -> None:
        """
        Test when tiered storage is explicitly slower than the translation
        interval, and we end up with repeated offsets, since we may translate a
        given offset before it makes its way to object storage, in which case
        after restoring the same offset will be used again
        """
        # Reset the Redpanda tiered storage upload configs to not be so fast.
        self.redpanda.si_settings.cloud_storage_segment_max_upload_interval_sec = 600
        self.redpanda.si_settings.cloud_storage_manifest_max_upload_interval_sec = 600
        self.redpanda.set_si_settings(self.redpanda.si_settings)
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=catalog_type,
        ) as dl:
            # To make this test more robust, on top of lowering the upload
            # rate, we'll just avoid uploading the schemas topic to allow us to
            # reliably check that the schemas topic is not restored below
            # (otherwise a leadership transfer could trigger an upload).
            rpk: RpkTool = RpkTool(self.redpanda)
            rpk.create_topic(
                "_schemas", partitions=1, config={"redpanda.remote.write": "false"}
            )

            all_topics, all_streams = self.start_all_topic_streams(dl)
            for t in all_topics:
                dl.wait_for_translation_until_offset(t, 50)

            # To simulate slow tiered storage, disable writes and then
            # push more data to Iceberg.
            for t in all_topics:
                rpk.alter_topic_config(t, "redpanda.remote.write", "false")
            counts_snap = self.counts_per_table(dl, all_topics)
            wait_until(
                lambda: self.tables_translated_more(dl, counts_snap, 50),
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Translation not progressing after disabling uploads",
            )

            for s in all_streams:
                self.rpcn.stop_stream(s, should_finish=None)

            self.wipe_and_restart_cluster()
            self.restore_cluster_and_wait(all_topics)

            num_schemas_records = self.get_num_schemas_records()
            assert num_schemas_records == 0, (
                "Expected schemas topic to not have been restored"
            )

            # Recreate the our streams and their necessary schemas.
            all_topics, all_streams = self.start_all_topic_streams(
                dl, create_topics=False, create_schemas=True
            )
            counts_snap = self.counts_per_table(dl, all_topics)

            # All of our topics should continue to write with no issues since
            # we have recreated our schemas.
            wait_until(
                lambda: self.tables_translated_more(dl, counts_snap, 50),
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Translation not progressing after restore",
            )

            spark = dl.spark()
            for t in all_topics:
                dupes_query = (
                    "select redpanda.offset, count(*) as counts "
                    f"from redpanda.`{t}` group by redpanda.offset "
                    "having count(*) > 1 order by counts desc"
                )
                res = spark.run_query_fetch_all(dupes_query)
                self.redpanda.logger.debug(f"Duplicate offsets for {t}: {res}")
                assert len(res) > 0, f"Expected some duplicate offsets for table {t}"
            assert dl.num_tables() == len(all_topics), (
                "Expected one table per topic (i.e. no DLQ tables)"
            )

            # Reenable tiered storage writes to allow for end-of-test
            # scrubbing.
            for t in all_topics:
                rpk.alter_topic_config(t, "redpanda.remote.write", "true")
            rpk.alter_topic_config("_schemas", "redpanda.remote.write", "true")
