# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""End-to-end test: PostgreSQL -> Debezium -> Redpanda -> Iceberg

Verifies that Debezium CDC events (inserts, updates, deletes) are
correctly translated to Iceberg tables with proper upsert and delete
semantics.
"""

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.debezium_server_service import DebeziumServerService
from rptest.services.postgres_service import PostgresService
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest


class DebeziumCdcIcebergTest(RedpandaTest):
    """End-to-end test: PostgreSQL -> Debezium -> Redpanda -> Iceberg"""

    def __init__(self, test_ctx):
        super().__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            schema_registry_config=SchemaRegistryConfig(),
            extra_rp_conf={
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": 5000,
            },
        )
        self.postgres = PostgresService(test_ctx)
        self.debezium = None
        self.dl = DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
        )

    def setUp(self):
        self.postgres.start()
        self.dl.setUp()

    def tearDown(self):
        if self.debezium:
            self.debezium.stop()
        self.dl.tearDown()
        self.postgres.stop()
        super().tearDown()

    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_debezium_cdc_to_iceberg(self, cloud_storage_type):
        """Test insert, update, and delete via Debezium CDC to Iceberg."""
        # Create test table with REPLICA IDENTITY FULL
        self.postgres.exec_sql(
            sql="CREATE TABLE users ("
            "id SERIAL PRIMARY KEY, "
            "name TEXT NOT NULL, "
            "email TEXT"
            ")"
        )
        self.postgres.exec_sql(sql="ALTER TABLE users REPLICA IDENTITY FULL")

        # Insert initial rows before starting Debezium so the snapshot
        # has data to produce (which creates the Kafka topic).
        self.postgres.exec_sql(
            sql="INSERT INTO users (name, email) VALUES ('alice', 'alice@example.com')"
        )
        self.postgres.exec_sql(
            sql="INSERT INTO users (name, email) VALUES ('bob', 'bob@example.com')"
        )
        self.postgres.exec_sql(
            sql="INSERT INTO users (name, email) VALUES "
            "('charlie', 'charlie@example.com')"
        )
        self.logger.info("PostgreSQL table created with 3 initial rows")

        # Wait for Schema Registry to be ready (Debezium's Avro
        # converter needs to register schemas on first produce).
        def _sr_ready():
            try:
                import requests as req

                r = req.get(
                    f"{self.redpanda.schema_reg().split(',')[0]}/subjects",
                    timeout=5,
                )
                self.logger.debug(f"Schema Registry status: {r.status_code}")
                return r.status_code == 200
            except Exception:
                return False

        wait_until(
            _sr_ready,
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Schema Registry not ready",
        )
        self.logger.info("Schema Registry is ready")

        # Pre-create the Debezium topic since Redpanda may not have
        # auto-topic creation enabled.
        topic_name = "dbserver1.public.users"
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic_name)
        self.logger.info(f"Pre-created topic '{topic_name}'")

        # Start Debezium Server
        self.debezium = DebeziumServerService(
            self.test_context,
            self.redpanda,
            self.postgres,
            database_name=PostgresService.DB_NAME,
            table_include_list="public.users",
            server_name="dbserver1",
        )
        self.logger.info("Starting Debezium Server...")
        self.debezium.start()
        self.logger.info("Debezium Server started")

        # Set Iceberg mode on the pre-created topic
        self.dl.set_iceberg_mode_on_topic(topic_name, "debezium")
        self.logger.info(f"Set iceberg mode 'debezium' on topic '{topic_name}'")

        self.logger.info("Waiting for initial inserts to appear in Iceberg...")

        # Check that Redpanda received messages
        def _topic_has_messages():
            partitions = rpk.describe_topic(topic_name)
            total = sum(p.high_watermark for p in partitions)
            self.logger.info(f"Topic '{topic_name}' high watermark: {total}")
            return total > 0

        wait_until(
            _topic_has_messages,
            timeout_sec=30,
            backoff_sec=2,
            err_msg="No messages appeared in Redpanda topic",
        )

        # Wait for table to appear in Iceberg catalog
        self.logger.info("Waiting for Iceberg table to be created in catalog...")

        def _table_exists():
            exists = self.dl.table_exists(topic_name)
            self.logger.info(f"Iceberg table exists: {exists}")
            return exists

        wait_until(
            _table_exists,
            timeout_sec=90,
            backoff_sec=5,
            err_msg="Iceberg table was never created by translation",
        )
        self.logger.info("Iceberg table created, checking row count...")

        # Wait for initial inserts to appear
        spark = self.dl.query_engine(QueryEngineType.SPARK)

        def _inserts_visible():
            try:
                count = spark.count_table("redpanda", topic_name)
                self.logger.info(f"Iceberg row count: {count}")
                return count >= 3
            except Exception as e:
                self.logger.info(f"Iceberg query failed: {e}")
                return False

        wait_until(
            _inserts_visible,
            timeout_sec=60,
            backoff_sec=5,
            err_msg="Initial inserts not visible in Iceberg",
        )
        self.logger.info("All 3 inserts visible in Iceberg")

        # Update and delete
        self.logger.info("Performing UPDATE (id=1) and DELETE (id=2)...")
        self.postgres.exec_sql(sql="UPDATE users SET name = 'alicia' WHERE id = 1")
        self.postgres.exec_sql(sql="DELETE FROM users WHERE id = 2")
        self.logger.info("Waiting for CDC updates to propagate...")

        def _final_state():
            try:
                rows = spark.run_query_fetch_all(
                    f"SELECT id, name FROM redpanda.{spark.escape_identifier(topic_name)} ORDER BY id"
                )
                self.logger.info(f"Current Iceberg rows: {rows}")
                if len(rows) != 2:
                    return False
                return rows[0][1] == "alicia" and rows[1][1] == "charlie"
            except Exception as e:
                self.logger.info(f"Iceberg query failed: {e}")
                return False

        wait_until(
            _final_state,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="Final CDC state not reflected in Iceberg",
        )

        # Final verification
        rows = spark.run_query_fetch_all(
            f"SELECT id, name, email FROM redpanda.{spark.escape_identifier(topic_name)} ORDER BY id"
        )
        assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}: {rows}"
        assert rows[0][0] == 1, f"Expected id=1, got {rows[0][0]}"
        assert rows[0][1] == "alicia", f"Expected 'alicia', got {rows[0][1]}"
        assert rows[1][0] == 3, f"Expected id=3, got {rows[1][0]}"
        assert rows[1][1] == "charlie", f"Expected 'charlie', got {rows[1][1]}"

        self.logger.info(f"Debezium CDC -> Iceberg test passed: final state = {rows}")
