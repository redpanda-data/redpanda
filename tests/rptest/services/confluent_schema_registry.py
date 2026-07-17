# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import tempfile

import requests
from ducktape.services.service import Service
from ducktape.utils.util import wait_until

CP_DIR = "/opt/confluent"
SR_BIN = os.path.join(CP_DIR, "bin", "schema-registry-start")
SR_LOG = "/var/log/schema-registry.log"
SR_PROPS = "/tmp/schema-registry.properties"
SR_DEFAULT_PORT = 8081


class ConfluentSchemaRegistryService(Service):
    """
    Single-node Confluent Schema Registry, used for migration / cross-vendor
    compatibility tests. The registry stores schemas in the `_schemas` topic
    of whichever Kafka-compatible cluster is passed as `bootstrap_provider`.

    `bootstrap_provider` is anything that exposes a `bootstrap_servers()`
    method returning a comma-separated host:port list — e.g. a
    `KafkaServiceAdapter` wrapping `kafkatest.services.kafka.KafkaService`,
    or a `RedpandaService`.
    """

    logs = {
        "schema_registry_log": {
            "path": SR_LOG,
            "collect_default": True,
        },
    }

    def __init__(self, context, bootstrap_provider, port: int = SR_DEFAULT_PORT):
        super().__init__(context, num_nodes=1)
        self._bootstrap_provider = bootstrap_provider
        self.port = port

    def _bootstrap_servers(self) -> str:
        bs = self._bootstrap_provider.bootstrap_servers()
        # Confluent SR expects PLAINTEXT://host:port form for plaintext.
        return ",".join(
            f"PLAINTEXT://{ep}" if "://" not in ep else ep for ep in bs.split(",")
        )

    def _properties(self) -> str:
        return (
            f"listeners=http://0.0.0.0:{self.port}\n"
            f"kafkastore.bootstrap.servers={self._bootstrap_servers()}\n"
            "kafkastore.topic=_schemas\n"
            "schema.compatibility.level=none\n"
            # This is a test-only registry; RF=1 keeps `_schemas` writable on a
            # small source cluster regardless of its broker count.
            "kafkastore.topic.replication.factor=1\n"
        )

    def url(self, node=None) -> str:
        node = node or self.nodes[0]
        return f"http://{node.account.hostname}:{self.port}"

    def alive(self, node) -> bool:
        try:
            r = requests.get(f"{self.url(node)}/subjects", timeout=2)
            return r.status_code == 200
        except Exception:
            return False

    def pids(self, node):
        # The registry's JVM main class is
        # io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain; match on
        # the (unambiguous) simple class name so stop_node actually finds it.
        return node.account.java_pids("SchemaRegistryMain")

    def start_node(self, node, **kwargs):
        if not node.account.exists(SR_BIN):
            raise RuntimeError(
                f"Confluent SR binary not found at {SR_BIN} on "
                f"{node.account.hostname}. The Confluent Platform install "
                f"({CP_DIR}) is missing or incomplete on this node's image"
            )

        props = self._properties()
        self.logger.info(f"Starting Confluent SR on {node.account.hostname}\n{props}")
        with tempfile.NamedTemporaryFile(mode="w") as f:
            f.write(props)
            f.flush()
            node.account.copy_to(f.name, SR_PROPS)

        cmd = f"nohup {SR_BIN} {SR_PROPS} > {SR_LOG} 2>&1 &"
        node.account.ssh(cmd, allow_fail=False)

        wait_until(
            lambda: self.alive(node),
            timeout_sec=90,
            backoff_sec=2,
            err_msg=f"Confluent SR did not become reachable at {self.url(node)}",
        )
        self.logger.info(f"Confluent SR is up at {self.url(node)}")

    def stop_node(self, node, **kwargs):
        for pid in self.pids(node):
            node.account.signal(pid, 15, allow_fail=True)
        wait_until(
            lambda: len(self.pids(node)) == 0,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Confluent SR did not stop",
        )

    def clean_node(self, node, **kwargs):
        node.account.ssh(f"rm -f {SR_PROPS} {SR_LOG}", allow_fail=True)
