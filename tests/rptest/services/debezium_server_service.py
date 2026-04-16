# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import requests

from ducktape.services.service import Service
from ducktape.utils.util import wait_until


class DebeziumServerService(Service):
    """Debezium Server service for ducktape tests.

    Runs a standalone Debezium Server (Quarkus-based) that captures
    CDC events from a PostgreSQL source database and writes them to
    Redpanda using Avro + Schema Registry serialization.
    """

    INSTALL_DIR = "/opt/debezium-server"
    PERSISTENT_ROOT = "/var/lib/debezium"
    LOG_FILE = f"{PERSISTENT_ROOT}/debezium.log"
    HEALTH_PORT = 8080
    logs = {"debezium_logs": {"path": LOG_FILE, "collect_default": True}}

    def __init__(
        self,
        ctx,
        redpanda,
        postgres_service,
        database_name="testdb",
        table_include_list="public.*",
        server_name="dbserver1",
        num_nodes=1,
    ):
        super().__init__(ctx, num_nodes=num_nodes)
        self.redpanda = redpanda
        self.postgres = postgres_service
        self.database_name = database_name
        self.table_include_list = table_include_list
        self.server_name = server_name

    def start_node(self, node, timeout_sec=120):
        node.account.ssh(f"mkdir -p {self.PERSISTENT_ROOT}")

        pg_host = self.postgres.hostname()
        bootstrap = self.redpanda.brokers()
        schema_reg = self.redpanda.schema_reg()

        props = self._build_properties(pg_host, bootstrap, schema_reg)
        config_path = f"{self.INSTALL_DIR}/config/application.properties"
        node.account.create_file(config_path, props)

        # Detect architecture for Java path
        arch = (
            node.account.ssh_output("dpkg-architecture -q DEB_BUILD_ARCH")
            .decode()
            .strip()
        )
        java_home = f"/usr/lib/jvm/java-21-openjdk-{arch}"

        runner_jar = (
            node.account.ssh_output(
                f"ls {self.INSTALL_DIR}/debezium-server-*runner.jar"
            )
            .decode()
            .strip()
        )
        # Use -cp with lib/* glob (not -jar) so that additional JARs
        # we added to lib/ (e.g. Confluent Avro serializer) are on
        # the classpath. Use semicolon before &, not &&: the pattern
        # `cd dir && cmd &` backgrounds the entire compound command as
        # a subshell that holds the SSH channel open, while
        # `cd dir; cmd &` only backgrounds cmd.
        cp = f"{runner_jar}:{self.INSTALL_DIR}/config:{self.INSTALL_DIR}/lib/*"
        cmd = (
            f"cd {self.INSTALL_DIR}; "
            f"nohup {java_home}/bin/java -cp '{cp}'"
            f" io.debezium.server.Main"
            f" >> {self.LOG_FILE} 2>&1 &"
        )
        self.logger.info(f"Starting Debezium with: {cmd}")
        node.account.ssh(cmd)
        self.logger.info("Debezium SSH command returned")

        wait_until(
            lambda: self._is_ready(node),
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg="Debezium Server did not become ready",
        )

    def stop_node(self, node):
        node.account.ssh("pkill -f 'debezium.server.Main'", allow_fail=True)

    def clean_node(self, node):
        self.stop_node(node)
        node.account.ssh(f"rm -rf {self.PERSISTENT_ROOT}", allow_fail=True)

    def _is_ready(self, node):
        try:
            url = f"http://{node.account.hostname}:{self.HEALTH_PORT}/q/health/ready"
            r = requests.get(url, timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def _build_properties(self, pg_host, bootstrap_servers, schema_reg_url):
        first_sr = schema_reg_url.split(",")[0]
        return f"""# Source: PostgreSQL
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.offset.storage.file.filename={self.PERSISTENT_ROOT}/offsets.dat
debezium.source.offset.flush.interval.ms=0
debezium.source.database.hostname={pg_host}
debezium.source.database.port={self.postgres.PG_PORT}
debezium.source.database.user={self.postgres.DB_USER}
debezium.source.database.password={self.postgres.DB_PASSWORD}
debezium.source.database.dbname={self.database_name}
debezium.source.topic.prefix={self.server_name}
debezium.source.table.include.list={self.table_include_list}
debezium.source.plugin.name=pgoutput
debezium.source.slot.name=debezium_test
debezium.source.tombstones.on.delete=false
debezium.source.snapshot.mode=initial

# Sink: Kafka/Redpanda
debezium.sink.type=kafka
debezium.sink.kafka.producer.bootstrap.servers={bootstrap_servers}
debezium.sink.kafka.producer.acks=all
debezium.sink.kafka.producer.key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
debezium.sink.kafka.producer.value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer

# Avro format via Confluent AvroConverter (produces SR wire format)
debezium.format.value=avro
debezium.format.value.schema.registry.url={first_sr}
debezium.format.key=avro
debezium.format.key.schema.registry.url={first_sr}
"""

    def topic_name(self, schema="public", table=""):
        """Return the Debezium topic name for a given table."""
        return f"{self.server_name}.{schema}.{table}"
