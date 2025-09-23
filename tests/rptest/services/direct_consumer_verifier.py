# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import dataclasses
import json
from dataclasses import dataclass, fields
from enum import Enum
from re import sub
from typing import List, Optional

import requests
from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until


def to_json_case(input_str: str):
    s = sub(r"(_|-)+", " ", input_str).title().replace(" ", "")
    return "".join([s[0].lower(), s[1:]])


def as_dict(obj):
    """
    Convert a dataclass instance to a dictionary, excluding None values.
    Handles nested dataclasses, lists, and enums properly.
    """
    if dataclasses.is_dataclass(obj):
        # Handle dataclass objects
        result = {}
        for field in fields(obj):
            value = getattr(obj, field.name)
            if value is not None:
                result[to_json_case(field.name)] = as_dict(value)
        return result
    elif isinstance(obj, list):
        # Handle lists recursively
        return [as_dict(item) for item in obj if item is not None]
    elif hasattr(obj, "value"):
        # Handle enum objects
        return obj.value
    else:
        # Return primitive values as-is
        return obj


class OffsetResetPolicy(Enum):
    EARLIEST = 0
    LATEST = 1


class IsolationLevel(Enum):
    READ_UNCOMMITTED = 0
    READ_COMMITTED = 1


@dataclass
class BrokerAddress:
    host: str
    port: int


@dataclass
class DirectConsumerConfiguration:
    min_bytes: int
    max_fetch_size: int
    partition_max_bytes: int
    reset_policy: OffsetResetPolicy
    max_wait_time_ms: int
    isolation_level: IsolationLevel
    max_buffered_bytes: int
    max_buffered_elements: int


@dataclass
class CreateDirectConsumerRequest:
    client_id: str
    initial_brokers: List[BrokerAddress]
    consumer_configuration: DirectConsumerConfiguration


@dataclass
class PartitionAssignment:
    partition_id: int
    offset: Optional[int] = None


@dataclass
class TopicAssignment:
    topic: str
    partitions: List[PartitionAssignment]


@dataclass
class AssignPartitionsRequest:
    client_id: str
    topic_assignments: List[TopicAssignment]


@dataclass
class TopicPartitions:
    topic: str
    partitions: List[int]


@dataclass
class UnassignPartitionsRequest:
    client_id: str
    topics: List[TopicPartitions]


@dataclass
class PartitionState:
    partition_id: int
    last_fetch_offset: int
    last_fetch_timestamp: int
    fetched_records: int
    fetched_bytes: int
    error_code: int


@dataclass
class TopicState:
    topic: str
    partitions: List[PartitionState]


@dataclass
class GetConsumerStateRequest:
    client_id: str
    include_partition_states: bool


@dataclass
class ConsumerStateResponse:
    client_id: str
    assigned_topics: List[TopicState]
    total_consumed_bytes: int
    total_consumed_messages: int
    last_error_code: int
    non_monotonic_fetches: int


@dataclass
class StatusRequest:
    pass


class DirectConsumerVerifier(Service):
    EXE = "direct_consumer_verifier"
    PERSISTENT_ROOT = "/var/lib/direct_consumer_verifier"
    LOG_PATH = f"{PERSISTENT_ROOT}/direct_consumer_verifier.log"

    logs = {"verifier_log": {"path": LOG_PATH, "collect_default": True}}

    def __init__(
        self,
        context: TestContext,
        log_level: str = "DEBUG",
        listener_port: int = 6321,
    ):
        super().__init__(context, num_nodes=1)

        self._log_level = log_level

        self._node = None
        self._listener_port = listener_port
        self._listener_host = "0.0.0.0"
        self._memory_limit = "2G"
        self._cores = 1

    def clean_node(self, node):
        self._node = None
        node.account.kill_process(self.EXE, clean_shutdown=True)

        if node.account.exists(self.PERSISTENT_ROOT):
            node.account.remove(self.PERSISTENT_ROOT, recursive=True)

    def _root_path(self):
        """
        Returns the root path for the verifier binary.
        This is used to construct the full path to the executable.
        """
        self.logger.info("globals: %s", self.context.globals)
        return self.context.globals.get(
            "direct_consumer_verifier_root", "/opt/direct_consumer_verifier"
        )

    def start_node(self, node, clean=None):
        node.account.ssh(
            "mkdir -p %s" % DirectConsumerVerifier.PERSISTENT_ROOT, allow_fail=False
        )
        assert self._node is None or self._node == node, (
            f"started on more than one node? {self._node} {node}"
        )
        self._node = node

        cmd = f"{self._root_path()}/bin/{self.EXE}"
        cmd += f" --hostname {self._listener_host}"
        cmd += f" --port {self._listener_port}"
        cmd += f" --default-log-level {self._log_level.lower()}"
        cmd += f" -c {self._cores}"
        cmd += f" -m {self._memory_limit}"

        final_cmd = f"nohup {cmd} > {self.LOG_PATH} 2>&1 &"
        node.account.ssh(final_cmd)
        wait_until(
            lambda: self.is_alive(),
            timeout_sec=15,
            backoff_sec=2,
            err_msg="Direct Consumer Verifier did not start",
            retry_on_exc=True,
        )

    def is_alive(self):
        self.status()
        return True

    def wait_node(self, node, timeout_sec=600) -> bool:
        assert self._node == node
        try:
            wait_until(
                lambda: not self.is_alive(), timeout_sec=timeout_sec, backoff_sec=5
            )
        except TimeoutError:
            return False
        return True

    def stop_node(self, node):
        node.account.kill_process(self.EXE, clean_shutdown=True)

    def request(self, path, req):
        """
        Send a request to the verifier service.

        :param path: The path to the API endpoint.
        :param message: The message to send in the request body.
        :return: The response from the verifier service.
        """
        url = self._url(path)
        message = as_dict(req)
        self.logger.info(f"Sending request to {url} with message: {message}")

        headers = {"Content-Type": "application/json"}
        response = requests.post(url, data=json.dumps(message), headers=headers)
        response.raise_for_status()
        resp = response.json()
        self.logger.debug(f"Response from {url}: {resp}")
        return resp

    def status(self):
        return self.request("status", StatusRequest())

    def create_consumer(self, request: CreateDirectConsumerRequest):
        return self.request("consumers", request)

    def assign_partitions(self, request: AssignPartitionsRequest):
        return self.request("consumers/partitions/assign", request)

    def unassign_partitions(self, request: UnassignPartitionsRequest):
        return self.request("consumers/partitions/unassign", request)

    def get_consumer_state(
        self, request: GetConsumerStateRequest
    ) -> ConsumerStateResponse:
        response = self.request("consumers/state", request)

        assigned_topics = []
        for topic_data in response.get("assignedTopics", []):
            partitions = []
            for partition_data in topic_data.get("partitions", []):
                partitions.append(
                    PartitionState(
                        partition_id=partition_data["partitionId"],
                        last_fetch_timestamp=partition_data["lastFetchTimestamp"],
                        last_fetch_offset=partition_data["lastFetchOffset"],
                        fetched_bytes=partition_data["fetchedBytes"],
                        fetched_records=partition_data["fetchedRecords"],
                        error_code=partition_data["errorCode"],
                    )
                )
            assigned_topics.append(
                TopicState(topic=topic_data["topic"], partitions=partitions)
            )

        return ConsumerStateResponse(
            client_id=response.get("client_id", ""),
            assigned_topics=assigned_topics,
            total_consumed_bytes=int(response.get("totalConsumedBytes", 0)),
            total_consumed_messages=int(response.get("totalConsumedMessages", 0)),
            last_error_code=response.get("lastErrorCode", 0),
            non_monotonic_fetches=response.get("nonMonotonicFetches", 0),
        )

    def _url(self, path) -> str:
        return f"http://{self._node.account.hostname}:{self._listener_port}/{path}"
