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
import signal
from dataclasses import dataclass
from typing import Any

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

    logs = {
        "rdkafka_performance_output": {
            "path": LOG_PATH,
            "collect_default": True,
        },
    }

    def __init__(
        self,
        context: TestContext,
        redpanda: RedpandaServiceForClients,
        topic: str,
        msg_count: int,
        msg_size: int = 1024,
        mode: RdkafkaPerformanceMode = RdkafkaPerformanceMode.PRODUCE,
        *,
        group_name: str | None = None,
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
        nodes_to_allocate = 0 if custom_node else 1
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
        self._mode = mode
        self._group_name = group_name
        self._acks = acks
        self._compression = compression
        self._key = key
        self._rate = rate
        self._partition = partition
        self._extra_config: dict[str, str] = dict(extra_config or {})
        self._sasl_options = sasl_options
        self._enable_tls = enable_tls
        self._pid: int | None = None

        # Auto-detect cloud cluster credentials.
        if hasattr(redpanda, "GLOBAL_CLOUD_CLUSTER_CONFIG"):
            security = redpanda.kafka_client_security()
            if security.sasl_enabled and self._sasl_options is None:
                self._sasl_options = security.simple_credentials()
            self._enable_tls = self._enable_tls or security.tls_enabled

    def _build_cmd(self) -> str:
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
        parts += ["-c", str(self._msg_count)]
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

        # Always use table output for machine-parseable metrics.
        parts.append("-u")

        # Security via -X properties
        security_props = self._build_security_config()
        all_extra = {**security_props, **self._extra_config}
        for prop, val in all_extra.items():
            parts += ["-X", f"{prop}={val}"]

        return " ".join(parts)

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

        assert self._pid is None

        cmd = self._build_cmd()
        wrapped_cmd = f"nohup {cmd} >> {self.LOG_PATH} 2>&1 & echo $!"

        self.logger.debug(f"Starting rdkafka_performance: {wrapped_cmd}")
        pid_str = node.account.ssh_output(wrapped_cmd, timeout_sec=10)
        self._pid = int(pid_str.strip())
        self.logger.debug(
            f"Spawned rdkafka_performance node={node.name} pid={self._pid}"
        )

    def wait_node(self, node: ClusterNode, timeout_sec: float | None = None) -> bool:
        timeout = timeout_sec or 600
        wait_until(
            lambda: not node.account.exists(f"/proc/{self._pid}"),
            timeout_sec=timeout,
            backoff_sec=2,
            err_msg=(
                f"rdkafka_performance did not finish within {timeout}s "
                f"(pid={self._pid})"
            ),
        )
        self._pid = None
        return True

    def stop_node(self, node: ClusterNode, **kwargs: Any):
        if self._pid is None:
            return
        self.logger.debug(f"Killing pid {self._pid}")
        try:
            node.account.signal(self._pid, signal.SIGKILL, allow_fail=False)
        except RemoteCommandError as e:
            if "No such process" not in str(e.msg):
                raise
        self._pid = None

    def clean_node(self, node: ClusterNode, **kwargs: Any):
        node.account.kill_process(self.PROCESS_NAME, clean_shutdown=False)
        node.account.remove(self.LOG_PATH, allow_fail=True)

    def metrics(self, node: ClusterNode) -> RdkafkaPerformanceMetrics:
        """Parse the last row of the ``-u`` table output.

        Call this after ``wait()`` has returned.  Reads the log file on
        *node* and returns a :class:`RdkafkaPerformanceMetrics` with the
        values from the last data row emitted by rdkafka_performance.
        """
        output = node.account.ssh_output(f"cat {self.LOG_PATH}", timeout_sec=10).decode(
            "utf-8"
        )

        rows = _parse_table(output)
        if not rows:
            raise RuntimeError(
                f"No table rows found in rdkafka_performance output for "
                f"mode={self._mode.name}. Log content:\n{output}"
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
