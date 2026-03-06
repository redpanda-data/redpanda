# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from __future__ import annotations

import enum
import json
import os
import signal
import statistics
from dataclasses import asdict, dataclass
from typing import Any, Sequence, overload

from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.services.redpanda_types import (
    RedpandaServiceForClients,
    SaslCredentials,
)


@dataclass
class RdkafkaPerformanceMetrics:
    """Metrics parsed from the rdkafka_performance ``-u`` table output.

    The table is emitted periodically by the tool.  Each row is a
    cumulative snapshot.  The :meth:`RdkafkaPerformanceService.metrics`
    method returns the **last** row (i.e. the final totals).

    Column mapping (producer mode)::

        elapsed | msgs | bytes | rtt | dr | dr_m/s | dr_MB/s |
        dr_err  | tx_err | outq | offset

    Column mapping (consumer mode)::

        elapsed | msgs | bytes | rtt | m/s | MB/s | rx_err | offset

    """

    elapsed_ms: int
    """Cumulative wall-clock time in milliseconds."""

    msgs: int
    """Total messages produced or consumed."""

    bytes: int
    """Total bytes produced or consumed."""

    rtt: int
    """Average round-trip time in milliseconds."""

    # -- Producer-specific (None in consumer mode) --
    dr: int | None = None
    """Messages delivered (delivery reports OK, producer only)."""

    dr_msgs_per_sec: int | None = None
    """Delivery rate in msgs/s (producer only)."""

    dr_mb_per_sec: float | None = None
    """Delivery rate in MB/s (producer only)."""

    dr_err: int | None = None
    """Delivery report errors (producer only)."""

    tx_err: int | None = None
    """Produce call failures (producer only)."""

    outq: int | None = None
    """Current output queue length (producer only)."""

    # -- Consumer-specific (None in producer mode) --
    msgs_per_sec: int | None = None
    """Consume rate in msgs/s (consumer only)."""

    mb_per_sec: float | None = None
    """Consume rate in MB/s (consumer only)."""

    rx_err: int | None = None
    """Receive errors (consumer only)."""


@overload
def _sum_optional(values: Sequence[int | None]) -> int | None: ...


@overload
def _sum_optional(values: Sequence[float | None]) -> float | None: ...


def _sum_optional(values: Sequence[int | float | None]) -> int | float | None:
    present_values = [value for value in values if value is not None]
    if not present_values:
        return None
    return sum(present_values)


def _parse_table(output: str) -> list[dict[str, str]]:
    """Parse the ``-u`` pipe-delimited table output into a list of dicts.

    Each dict maps a column header name to its stripped string value.
    Header rows (re-printed every 20 data rows) are detected and used
    to key subsequent data rows.
    """
    headers: list[str] = []
    rows: list[dict[str, str]] = []

    for line in output.splitlines():
        # Every row starts with "| ".
        if not line.startswith("|"):
            continue

        cells = [c.strip() for c in line.split("|") if c.strip()]

        # A header row contains non-numeric tokens like "elapsed".
        if cells and not cells[0].lstrip("-").replace(".", "").isdigit():
            headers = cells
        elif headers:
            row = dict(zip(headers, cells))
            rows.append(row)

    return rows


class RdkafkaPerformanceMode(enum.Enum):
    PRODUCE = "P"
    CONSUME = "G"


class RdkafkaPerformanceService(Service):
    """
    Service to run rdkafka_performance
    """

    PROCESS_NAME = "rdkafka_performance"
    EXE = f"/opt/librdkafka/examples/{PROCESS_NAME}"
    LOG_PATH = f"/tmp/{PROCESS_NAME}.log"
    RESULT_FILE_NAME = "result.json"

    logs: dict[str, dict[str, str | bool]] = {}

    # message count is divided across all service instances
    def __init__(
        self,
        context: TestContext,
        redpanda: RedpandaServiceForClients,
        topic: str,
        msg_count: int,
        msg_size: int = 1024,
        mode: RdkafkaPerformanceMode = RdkafkaPerformanceMode.PRODUCE,
        *,
        warmup_msg_count: int = 0,
        group_name: str | None = None,
        num_nodes: int = 1,
        clients_per_node: int = 3,
        acks: int | None = None,
        compression: str | None = None,
        key: str | None = None,
        rate: float | None = None,
        partition: int | None = None,
        # Arbitrary librdkafka -X properties
        extra_config: dict[str, str] | None = None,
        sasl_options: SaslCredentials | None = None,
        enable_tls: bool = False,
        custom_node: list[ClusterNode] | None = None,
    ):
        if num_nodes < 1:
            raise ValueError(f"num_nodes must be >= 1, got {num_nodes}")
        if clients_per_node < 1:
            raise ValueError(f"clients_per_node must be >= 1, got {clients_per_node}")

        nodes_to_allocate = 0 if custom_node else num_nodes
        super().__init__(context, num_nodes=nodes_to_allocate)

        if custom_node is not None:
            assert not self.nodes
            self.nodes = custom_node

        if mode == RdkafkaPerformanceMode.CONSUME:
            assert group_name is not None, "group_name is required in CONSUME mode"

        self._redpanda = redpanda
        self._topic = topic
        self._msg_count = msg_count
        self._msg_size = msg_size
        self._warmup_msg_count = warmup_msg_count
        self._mode = mode
        self._group_name = group_name
        self._clients_per_node = clients_per_node
        self._acks = acks
        self._compression = compression
        self._key = key
        self._rate = rate
        self._partition = partition
        self._extra_config: dict[str, str] = dict(extra_config or {})
        self._sasl_options = sasl_options
        self._enable_tls = enable_tls
        self._instances: dict[str, list[tuple[int, str]]] = {}

        self.logs = {
            f"rdkafka_performance_output_{client_idx}": {
                "path": self._instance_log_path(client_idx),
                "collect_default": True,
            }
            for client_idx in range(self._clients_per_node)
        }

        # Auto-detect cloud cluster credentials.
        if hasattr(redpanda, "GLOBAL_CLOUD_CLUSTER_CONFIG"):
            security = redpanda.kafka_client_security()
            if security.sasl_enabled and self._sasl_options is None:
                self._sasl_options = security.simple_credentials()
            self._enable_tls = self._enable_tls or security.tls_enabled

    def _build_cmd(self, msg_count: int, warmup_count: int) -> str:
        """Build the full rdkafka_performance command line."""
        parts: list[str] = [self.EXE]

        # Mode flag
        parts.append(f"-{self._mode.value}")

        if self._mode == RdkafkaPerformanceMode.CONSUME:
            assert self._group_name is not None
            parts += [self._group_name]

        # Required args
        parts += ["-b", self._redpanda.brokers()]
        parts += ["-t", self._topic]
        parts += ["-c", str(msg_count)]
        parts += ["-s", str(self._msg_size)]

        # Optional native flags
        if self._acks is not None:
            parts += ["-a", str(self._acks)]
        if self._compression is not None:
            parts += ["-z", self._compression]
        if self._key is not None:
            parts += ["-k", self._key]
        if self._rate is not None:
            parts += ["-r", str(self._rate)]
        if self._partition is not None:
            parts += ["-p", str(self._partition)]
        if warmup_count > 0:
            parts += ["-w", str(warmup_count)]

        # Always use table output for machine-parseable metrics.
        parts.append("-u")

        # Security via -X properties
        security_props = self._build_security_config()
        all_extra = {**security_props, **self._extra_config}
        for prop, val in all_extra.items():
            parts += ["-X", f"{prop}={val}"]

        return " ".join(parts)

    def _instance_log_path(self, client_idx: int) -> str:
        return f"/tmp/{self.PROCESS_NAME}_{client_idx}.log"

    def _instance_message_count(
        self, node: ClusterNode, client_idx: int, count: int
    ) -> int:
        total_instances = len(self.nodes) * self._clients_per_node
        base_count = count // total_instances
        remainder = count % total_instances

        global_instance_idx = (
            self.nodes.index(node) * self._clients_per_node + client_idx
        )
        if global_instance_idx < remainder:
            return base_count + 1
        return base_count

    def _build_security_config(self) -> dict[str, str]:
        """Return librdkafka -X security properties derived from the
        authentication state."""
        props: dict[str, str] = {}

        if self._sasl_options is not None:
            if self._enable_tls:
                props["security.protocol"] = "sasl_ssl"
            else:
                props["security.protocol"] = "sasl_plaintext"
            props["sasl.mechanism"] = self._sasl_options.mechanism
            props["sasl.username"] = self._sasl_options.username
            props["sasl.password"] = self._sasl_options.password

        elif self._enable_tls:
            props["security.protocol"] = "ssl"

        return props

    def start_node(self, node: ClusterNode, **kwargs: Any):
        self.clean_node(node, **kwargs)

        assert node.name not in self._instances

        self._instances[node.name] = []

        for client_idx in range(self._clients_per_node):
            msg_count = self._instance_message_count(node, client_idx, self._msg_count)
            warmup_count = self._instance_message_count(
                node, client_idx, self._warmup_msg_count
            )
            cmd = self._build_cmd(msg_count, warmup_count)
            log_path = self._instance_log_path(client_idx)
            wrapped_cmd = f"nohup {cmd} >> {log_path} 2>&1 & echo $!"

            pid_str = node.account.ssh_output(wrapped_cmd, timeout_sec=10)
            pid = int(pid_str.strip())
            self._instances[node.name].append((pid, log_path))
            self.logger.debug(
                f"Spawned rdkafka_performance node={node.name} client={client_idx} "
                f"pid={pid} msg_count={msg_count}"
            )

    def wait_node(self, node: ClusterNode, timeout_sec: float | None = None) -> bool:
        timeout = timeout_sec or 600
        for pid, _ in self._instances[node.name]:
            wait_until(
                lambda: not node.account.exists(f"/proc/{pid}"),
                timeout_sec=timeout,
                backoff_sec=2,
                err_msg=(
                    f"rdkafka_performance did not finish within {timeout}s (pid={pid})"
                ),
            )
        del self._instances[node.name]
        return True

    def stop_node(self, node: ClusterNode, **kwargs: Any):
        instances = self._instances.get(node.name)
        if instances is None:
            return

        for pid, _ in instances:
            self.logger.debug(f"Killing pid {pid}")
            try:
                node.account.signal(pid, signal.SIGKILL, allow_fail=False)
            except RemoteCommandError as e:
                if "No such process" not in str(e.msg):
                    raise
        del self._instances[node.name]

    def clean_node(self, node: ClusterNode, **kwargs: Any):
        node.account.kill_process(self.PROCESS_NAME, clean_shutdown=False)
        node.account.remove(self.LOG_PATH, allow_fail=True)
        for client_idx in range(self._clients_per_node):
            node.account.remove(self._instance_log_path(client_idx), allow_fail=True)

    def _result_file_path(self) -> str:
        result_dir = TestContext.results_dir(self.context, self.context.test_index)
        os.makedirs(result_dir, exist_ok=True)
        return os.path.join(result_dir, self.RESULT_FILE_NAME)

    def write_metrics_result(self, metrics: RdkafkaPerformanceMetrics) -> None:
        with open(self._result_file_path(), "w", encoding="utf-8") as result_file:
            json.dump(asdict(metrics), result_file, indent=2, sort_keys=True)
            result_file.write("\n")

    def _metrics_for_node_instance(
        self, node: ClusterNode, log_path: str
    ) -> RdkafkaPerformanceMetrics:
        """Parse the last row of the ``-u`` table output.

        Call this after ``wait()`` has returned.  Reads the log file on
        *node* and returns a :class:`RdkafkaPerformanceMetrics` with the
        values from the last data row emitted by rdkafka_performance.
        """
        output = node.account.ssh_output(f"cat {log_path}", timeout_sec=10).decode(
            "utf-8"
        )

        rows = _parse_table(output)
        if not rows:
            raise RuntimeError(
                f"No table rows found in rdkafka_performance output for "
                f"mode={self._mode.name}, log={log_path}. Log content:\n{output}"
            )

        r = rows[-1]

        # We drop offset. It's not useful at this point

        if self._mode == RdkafkaPerformanceMode.PRODUCE:
            return RdkafkaPerformanceMetrics(
                elapsed_ms=int(r["elapsed"]),
                msgs=int(r["msgs"]),
                bytes=int(r["bytes"]),
                rtt=int(r["rtt"]),
                dr=int(r["dr"]),
                dr_msgs_per_sec=int(r["dr_m/s"]),
                dr_mb_per_sec=float(r["dr_MB/s"]),
                dr_err=int(r["dr_err"]),
                tx_err=int(r["tx_err"]),
                outq=int(r["outq"]),
            )
        else:
            return RdkafkaPerformanceMetrics(
                elapsed_ms=int(r["elapsed"]),
                msgs=int(r["msgs"]),
                bytes=int(r["bytes"]),
                rtt=int(r["rtt"]),
                msgs_per_sec=int(r["m/s"]),
                mb_per_sec=float(r["MB/s"]),
                rx_err=int(r["rx_err"]),
            )

    def metrics(self) -> RdkafkaPerformanceMetrics:
        """Return metrics across all nodes."""

        instance_metrics = [
            self._metrics_for_node_instance(
                service_node, self._instance_log_path(client_idx)
            )
            for service_node in self.nodes
            for client_idx in range(self._clients_per_node)
        ]
        if not instance_metrics:
            raise RuntimeError("No nodes are configured for rdkafka_performance")

        return RdkafkaPerformanceMetrics(
            elapsed_ms=int(
                statistics.median(
                    [current_metrics.elapsed_ms for current_metrics in instance_metrics]
                )
            ),
            msgs=sum(current_metrics.msgs for current_metrics in instance_metrics),
            bytes=sum(current_metrics.bytes for current_metrics in instance_metrics),
            rtt=int(
                statistics.median(
                    [current_metrics.rtt for current_metrics in instance_metrics]
                )
            ),
            dr=_sum_optional(
                [current_metrics.dr for current_metrics in instance_metrics]
            ),
            dr_msgs_per_sec=_sum_optional(
                [
                    current_metrics.dr_msgs_per_sec
                    for current_metrics in instance_metrics
                ]
            ),
            dr_mb_per_sec=_sum_optional(
                [current_metrics.dr_mb_per_sec for current_metrics in instance_metrics]
            ),
            dr_err=_sum_optional(
                [current_metrics.dr_err for current_metrics in instance_metrics]
            ),
            tx_err=_sum_optional(
                [current_metrics.tx_err for current_metrics in instance_metrics]
            ),
            outq=_sum_optional(
                [current_metrics.outq for current_metrics in instance_metrics]
            ),
            msgs_per_sec=_sum_optional(
                [current_metrics.msgs_per_sec for current_metrics in instance_metrics]
            ),
            mb_per_sec=_sum_optional(
                [current_metrics.mb_per_sec for current_metrics in instance_metrics]
            ),
            rx_err=_sum_optional(
                [current_metrics.rx_err for current_metrics in instance_metrics]
            ),
        )
