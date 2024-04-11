# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import logging

from typing import Optional

from ducktape.cluster.cluster import ClusterNode
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.tests.test import TestContext
from rptest.clients.types import TopicSpec
from rptest.clients.serde_client_utils import SchemaType, SerdeClientType
from rptest.util import inject_remote_script

from uuid import uuid4

PYTHON_EXEC = "python3"
JAVA_EXEC = "java -cp"


class SerdeClient(BackgroundThreadService):
    EXES = dict({
        SerdeClientType.Python:
        f'{PYTHON_EXEC} /opt/remote/python_librdkafka_serde_client.py',
        SerdeClientType.Java:
        f'{JAVA_EXEC} /opt/kafka-serde/kafka-serde.jar com.redpanda.JavaKafkaSerdeClientMain',
        SerdeClientType.Golang:
        '/opt/redpanda-tests/go/go-kafka-serde/go-kafka-serde'
    })

    def __init__(
            self,
            context: TestContext,
            brokers: str,
            schema_registry_url: str,
            schema_type: SchemaType,
            serde_client_type: SerdeClientType,
            count: int,
            *,
            nodes: Optional[list[ClusterNode]] = None,
            num_nodes: Optional[int] = None,
            topic=str(uuid4()),
            group=str(uuid4()),
            security_config: Optional[dict] = None,
            skip_known_types: Optional[bool] = None,
            subject_name_strategy: Optional[str] = None,
            payload_class: Optional[str] = None,
            compression_type: Optional[TopicSpec.CompressionTypes] = None):

        if num_nodes is None and nodes is None:
            num_nodes = 1

        super().__init__(context, num_nodes=0 if nodes else num_nodes)

        if nodes is not None:
            assert len(nodes) > 0
            self.nodes = nodes

        self._serde_client_type = serde_client_type

        self._cmd_args = f" --brokers {brokers}"
        self._cmd_args += f" --topic {topic}"
        self._cmd_args += f" --schema-registry {schema_registry_url}"
        self._cmd_args += f" --consumer-group {group}"
        self._cmd_args += f" --protocol {schema_type}"
        self._cmd_args += f" --count {count}"

        if skip_known_types is not None:
            if self._serde_client_type != SerdeClientType.Golang:
                self._cmd_args += " --skip-known-types"
            else:
                assert False

        if subject_name_strategy is not None:
            assert self._serde_client_type == SerdeClientType.Python
            self._cmd_args += f" --subject-name-strategy {subject_name_strategy}"

        if payload_class is not None:
            assert self._serde_client_type == SerdeClientType.Python
            self._cmd_args += f" --payload-class {payload_class}"

        if compression_type is not None:
            assert self._serde_client_type == SerdeClientType.Python
            self._cmd_args += f" --compression-type {compression_type}"

        if self._serde_client_type == SerdeClientType.Golang:
            self._cmd_args += f" --debug"

        if security_config is not None:
            security_string = json.dumps(security_config)
            self._cmd_args += f" --security \'{security_string}\'"

        self.logger.debug(f"Command to run: {self._cmd_args}")

    def _worker(self, idx, node):

        if self._serde_client_type == SerdeClientType.Python:
            inject_remote_script(node, "payload_pb2.py")
            script_path = inject_remote_script(
                node, "python_librdkafka_serde_client.py")
            script_path = f"{PYTHON_EXEC} {script_path}"
        else:
            script_path = self.EXES[self._serde_client_type]

        script_path += self._cmd_args

        self.logger.debug(
            f"Starting serde client with command '{script_path}'")
        ssh_output = node.account.ssh_capture(script_path)

        for line in ssh_output:
            self.logger.debug(line)

    def __enter__(self):
        self.run()

    def __exit__(self, *args):
        self.reset()

    def reset(self):
        self.worker_errors.clear()
        self.errors = ''
