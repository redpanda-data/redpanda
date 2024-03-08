# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import requests
import signal
import typing

from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from ducktape.cluster.remoteaccount import RemoteCommandError
from rptest.services.redpanda import RedpandaService
from ducktape.utils.util import wait_until


class TransformVerifierProduceStatus(typing.NamedTuple):
    # The number of bytes sent by the producer
    bytes_sent: int
    # The number of records sent by the producer by partition
    latest_seqnos: dict[int, int]
    # The number of errors producing
    error_count: int
    # If all the records have been produced
    done: bool

    @classmethod
    def deserialize(cls, buf: str | bytes | bytearray):
        data = json.loads(buf)
        return cls(
            bytes_sent=data["bytes_sent"],
            latest_seqnos=data["latest_seqnos"],
            error_count=data["error_count"],
            done=data["done"],
        )

    def merge(self, other: 'TransformVerifierProduceStatus'):
        combined = self.latest_seqnos.copy()
        for k, v in other.latest_seqnos.items():
            combined[k] = max(combined.get(k, 0), v)
        return TransformVerifierProduceStatus(
            bytes_sent=self.bytes_sent + other.bytes_sent,
            latest_seqnos=combined,
            error_count=self.error_count + other.error_count,
            done=self.done and other.done,
        )


class TransformVerifierProduceConfig(typing.NamedTuple):
    # The topic name to produce to
    topic: str
    # Total amount of bytes to send.
    #
    # expected values are something like: 10MB or 1GB
    max_bytes: str
    # Bytes to send per second, determines how long the test runs for
    # expected values are something like: 512KB or 1MB
    bytes_per_second: str
    # How big a single message is in bytes
    # i.e. 10KB or 1MB
    message_size: str
    # Maximum size to batch records on the client
    # i.e. 1MB
    max_batch_size: str
    transactional: bool = False

    def serialize_cmd(self) -> str:
        cmd = [
            "produce",
            f"--topic={self.topic}",
            f"--max-bytes={self.max_bytes}",
            f"--bytes-per-second={self.bytes_per_second}",
            f"--message-size={self.message_size}",
            f"--max-batch-size={self.max_batch_size}",
        ]
        if self.transactional:
            cmd.append("--transactional")
        return " ".join(cmd)

    def deserialize_status(self, buf: str | bytes | bytearray):
        return TransformVerifierProduceStatus.deserialize(buf)

    def is_done(self, status: TransformVerifierProduceStatus) -> bool:
        return status.done


class TransformVerifierConsumeStatus(typing.NamedTuple):
    # The number of records recieved by consumers by partition
    latest_seqnos: dict[int, int]
    # The number of invalid records
    invalid_records: int
    # The number of errors
    error_count: int

    @classmethod
    def deserialize(cls, buf: str | bytes | bytearray):
        data = json.loads(buf)
        return cls(
            latest_seqnos=data["latest_seqnos"],
            invalid_records=data["invalid_records"],
            error_count=data["error_count"],
        )

    def merge(self, other: 'TransformVerifierConsumeStatus'):
        combined = self.latest_seqnos.copy()
        for k, v in other.latest_seqnos.items():
            combined[k] = max(combined.get(k, 0), v)
        return TransformVerifierConsumeStatus(
            latest_seqnos=combined,
            invalid_records=self.invalid_records + other.invalid_records,
            error_count=self.error_count + other.error_count,
        )


class TransformVerifierConsumeConfig(typing.NamedTuple):
    # The topic name to consume from
    topic: str
    # Bytes to send per second, determines how long the test runs for
    # expected values are something like: 512KB or 1MB
    bytes_per_second: str
    # The producer status to validate.
    # Will validate that the correct number of records are on the output topic
    # and all seqno are valid
    validate: TransformVerifierProduceStatus

    def serialize_cmd(self) -> str:
        return " ".join([
            "consume",
            f"--topic={self.topic}",
            f"--bytes-per-second={self.bytes_per_second}",
        ])

    def deserialize_status(self, buf: str | bytes | bytearray):
        return TransformVerifierConsumeStatus.deserialize(buf)

    def is_done(self, status: TransformVerifierConsumeStatus) -> bool:
        if status.invalid_records > 0:
            return True
        for partition, amt in self.validate.latest_seqnos.items():
            if status.latest_seqnos.get(partition, 0) < amt:
                return False
        return True


class TransformVerifierService(Service):
    """
    A verifier for transforms.

    General usage looks like the following in a test:

    status = TransformVerifierService.oneshot(
      context=self.test_context,
      redpanda=self.redpanda,
      config=TransformVerifierProduceConfig(...),
    )

    """
    EXE = "transform-verifier"
    LOG_PATH = "/tmp/transform-verifier.log"

    logs = {
        "transform_verifier_log": {
            "path": LOG_PATH,
            "collect_default": True
        }
    }

    redpanda: RedpandaService
    report_port: int
    config: TransformVerifierConsumeConfig | TransformVerifierProduceConfig
    _pid: typing.Optional[int]

    @classmethod
    def oneshot(cls,
                context: TestContext,
                redpanda: RedpandaService,
                config: TransformVerifierConsumeConfig
                | TransformVerifierProduceConfig,
                timeout_sec=300):
        """
        Common pattern to use the service
        """
        service = cls(context, redpanda, config)
        service.start()
        try:
            service.wait(timeout_sec=timeout_sec)
            final_status = service.get_status()
            service.stop()
            service.free()
            return final_status
        except:
            service.stop()
            raise

    def __init__(self, context: TestContext, redpanda: RedpandaService,
                 config: TransformVerifierConsumeConfig
                 | TransformVerifierProduceConfig):
        super().__init__(context, num_nodes=1)
        self.redpanda = redpanda

        # Note: using a port that happens to already be in test environment
        # firewall rules from other use cases.  If changing this, update
        # terraform for test clusters.
        self.remote_port = 8080
        self.config = config
        self._pid = None

    def clean_node(self, node):
        self.redpanda.logger.debug(f"{self.__class__.__name__}.clean_node")
        node.account.kill_process(self.EXE, clean_shutdown=False)
        if node.account.exists(self.LOG_PATH):
            node.account.remove(self.LOG_PATH)

    def start_node(self, node):
        cmd = " ".join([
            f"/opt/redpanda-tests/go/transform-verifier/{self.EXE}",
            self.config.serialize_cmd(),
            f"--brokers={self.redpanda.brokers()}",
            f"--port={self.remote_port}",
        ])
        cmd = f"nohup {cmd} >> {self.LOG_PATH} 2>&1 & echo $!"
        self.logger.info(f"start_node[{node.name}]: {cmd}")
        pid_str = node.account.ssh_output(cmd, timeout_sec=10)
        self.logger.debug(
            f"spawned {self.who_am_i()} node={node.name} pid={pid_str} port={self.remote_port}"
        )
        self._pid = int(pid_str.strip())

        # Wait for status endpoint to respond.
        self._await_ready(node)

        # Because the above command was run with `nohup` we can't be sure that
        # it is the one who actually replied to the `await_ready` calls.
        # Check that the PID we just launched is still running as a confirmation
        # that it is the one.
        self._assert_running(node)

    def wait_node(self, node, timeout_sec=None):
        """
        Wait until the status endpoint reports that it is done
        """
        latest_status = None

        def poll_for_complete():
            nonlocal latest_status
            latest_status = self._get_status_for_node(node)
            return self.config.is_done(latest_status)

        wait_until(
            poll_for_complete,
            timeout_sec=timeout_sec or 5,
            backoff_sec=0.5,
            err_msg=
            f"Timed out for transform verifier to complete {self.who_am_i()}, latest status: {latest_status}"
        )
        return True

    def stop_node(self, node):
        """
        Stop the process on this node
        """
        self.redpanda.logger.info(f"{self.__class__.__name__}.stop")
        if self._pid is None:
            return

        # Attempt a graceful stop
        try:
            self._execute_cmd(node, "stop")
        except Exception as e:
            self.logger.warn("unable to request /stop {self.who_am_i()}: {e}")

        try:
            wait_until(lambda: not node.account.exists(f"/proc/{self._pid}"),
                       timeout_sec=10,
                       backoff_sec=0.5)
            self._pid = None
            return
        except TimeoutError as e:
            self.logger.warn(
                "gracefully stopping {self.who_am_i()} failed: {e}")

        # Gracefully stop did not work, try a hard kill
        self.logger.debug(f"Killing pid for {self.who_am_i()}")
        try:
            node.account.signal(self._pid, signal.SIGKILL, allow_fail=False)
        except RemoteCommandError as e:
            if b"No such process" not in e.msg:
                raise
        self._pid = None

    def get_status(self):
        status = None
        for node in self.nodes:
            node_status = self._get_status_for_node(node)
            if not status:
                status = node_status
            else:
                status = status.merge(node_status)
        return status

    def _await_ready(self, node):
        """
        Wait for the remote processes http endpoint to come up
        """
        wait_until(
            lambda: self._is_ready(node),
            timeout_sec=20,
            backoff_sec=.25,
            err_msg=
            f"Timed out waiting for status endpoint {self.who_am_i()} to be available"
        )

    def _execute_cmd(self, node, cmd):
        """
        Execute a command on a given node.

        Supported Commands:
        - "stop"
        """
        self.logger.debug(f"Invoking remote path {cmd} on {node.name}")
        r = requests.get(self._remote_url(node, cmd), timeout=10)
        r.raise_for_status()

    def _remote_url(self, node, path):
        return f"http://{node.account.hostname}:{self.remote_port}/{path}"

    def _is_ready(self, node):
        try:
            self._get_status_for_node(node)
        except Exception as e:
            # Broad exception handling for any lower level connection errors etc
            # that might not be properly classed as `requests` exception.
            self.logger.debug(
                f"Status endpoint {self.who_am_i()} not ready: {e}")
            return False
        else:
            return True

    def _assert_running(self, node):
        assert self._pid, "missing pid for node"
        node.account.alive(self._pid)

    def _get_status_for_node(self, node):
        r = requests.get(self._remote_url(node, "status"), timeout=10)
        r.raise_for_status()
        status = self.config.deserialize_status(r.text)
        self.logger.debug(
            f"Status endpoint {self.who_am_i()} response: {status}")
        return status
