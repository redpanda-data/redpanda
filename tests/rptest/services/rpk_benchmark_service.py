# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from __future__ import annotations

import json
import os
import signal
from dataclasses import asdict, dataclass
from typing import Any

from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.services.redpanda import RedpandaService


@dataclass
class RpkBenchmarkMetrics:
    requests_per_sec: float
    mb_per_sec: float
    errors: int


class RpkBenchmarkService(Service):
    PROCESS_NAME = "rpk"
    LOG_PATH = "/tmp/rpk_benchmark.log"
    METRICS_PATH = "/tmp/rpk_benchmark_metrics.json"
    RESULT_FILE_NAME = "result.json"

    logs = {
        "rpk_benchmark_output": {
            "path": LOG_PATH,
            "collect_default": True,
        }
    }

    def __init__(
        self,
        context: TestContext,
        redpanda: RedpandaService,
        topic: str,
        *,
        mode: str = "produce",
        partitions: int = 18,
        replicas: int = 3,
        clients: int = 1,
        record_size: int = 100,
        warmup_s: int = 5,
        duration_s: int = 30,
        wait_for_stable_leadership: bool = True,
    ):
        super().__init__(context, num_nodes=1)
        if mode != "produce":
            raise ValueError(f"unsupported rpk benchmark mode: {mode}")
        self._redpanda = redpanda
        self._topic = topic
        self._mode = mode
        self._partitions = partitions
        self._replicas = replicas
        self._clients = clients
        self._record_size = record_size
        self._warmup_s = warmup_s
        self._duration_s = duration_s
        self._wait_for_stable_leadership = wait_for_stable_leadership
        self._pids: dict[str, int] = {}

    def _build_cmd(self) -> str:
        return (
            f"{self._redpanda.find_binary('rpk')} -X brokers={self._redpanda.brokers()} benchmark {self._mode} "
            f"--topic {self._topic} --partitions {self._partitions} --replicas {self._replicas} "
            f"--clients {self._clients} --record-size {self._record_size} "
            f"--warmup {self._warmup_s} --duration {self._duration_s} "
            f"--metrics-json {self.METRICS_PATH} --wait-leadership-balanced={str(self._wait_for_stable_leadership).lower()}"
        )

    def start_node(self, node: ClusterNode, **kwargs: Any) -> None:
        self.clean_node(node, **kwargs)
        wrapped_cmd = f"nohup {self._build_cmd()} >> {self.LOG_PATH} 2>&1 & echo $!"
        pid = int(node.account.ssh_output(wrapped_cmd, timeout_sec=10).strip())
        self._pids[node.name] = pid
        self.logger.debug(f"Spawned rpk benchmark node={node.name} pid={pid}")

    def wait_node(self, node: ClusterNode, timeout_sec: float | None = None) -> bool:
        pid = self._pids[node.name]
        timeout = timeout_sec or 600
        wait_until(
            lambda: not node.account.exists(f"/proc/{pid}"),
            timeout_sec=timeout,
            backoff_sec=2,
            err_msg=f"rpk benchmark did not finish in {timeout}s (pid={pid})",
        )

        return True

    def stop_node(self, node: ClusterNode, **kwargs: Any) -> None:
        pid = self._pids.get(node.name)
        if pid is None:
            return
        try:
            node.account.signal(pid, signal.SIGKILL, allow_fail=False)
        except RemoteCommandError as e:
            if "No such process" not in str(e.msg):
                raise

    def clean_node(self, node: ClusterNode, **kwargs: Any) -> None:
        node.account.kill_process(self.PROCESS_NAME, clean_shutdown=False)
        node.account.remove(self.LOG_PATH, allow_fail=True)
        node.account.remove(self.METRICS_PATH, allow_fail=True)

    def _result_file_path(self) -> str:
        result_dir = TestContext.results_dir(self.context, self.context.test_index)
        os.makedirs(result_dir, exist_ok=True)
        return os.path.join(result_dir, self.RESULT_FILE_NAME)

    def write_metrics_result(self, metrics: RpkBenchmarkMetrics) -> None:
        with open(self._result_file_path(), "w", encoding="utf-8") as result_file:
            json.dump(asdict(metrics), result_file, indent=2, sort_keys=True)
            result_file.write("\n")

    def metrics(self) -> RpkBenchmarkMetrics:
        # We could just make rpk write the result.json directly. This way we
        # support checking the metrics though and we will need this if we ever
        # add multi node support for this service.
        if not self.nodes[0].account.exists(self.METRICS_PATH):
            raise RuntimeError(f"metrics json not found at {self.METRICS_PATH}")

        output = (
            self.nodes[0]
            .account.ssh_output(f"cat {self.METRICS_PATH}", timeout_sec=10)
            .decode("utf-8")
        )
        metrics = json.loads(output)

        return RpkBenchmarkMetrics(
            requests_per_sec=float(metrics["requests_per_sec"]),
            mb_per_sec=float(metrics["mb_per_sec"]),
            errors=int(metrics["errors"]),
        )
