# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import json
import collections
from typing import Optional

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from rptest.services.utils import BadLogLines, NodeCrash
from rptest.services.openmessaging_benchmark_configs import OMBSampleConfigurations

LOG_ALLOW_LIST = [
    "No such file or directory", "cannot be started once stopped"
]


# Benchmark worker that is used by benchmark process to run consumers and producers
class OpenMessagingBenchmarkWorkers(Service):
    PERSISTENT_ROOT = "/var/lib/openmessaging"
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "worker.log")
    PORT = 9090
    STATS_PORT = 9091

    logs = {
        "open_messaging_benchmark_worker_stdout_stderr": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True
        }
    }

    def __init__(self, ctx, num_workers=None, nodes: Optional[list] = None):
        """
        :param num_workers: allocate this many nodes as workers (mutually exclusive with `nodes`)
        :param nodes: use these pre-allocated nodes as workers (mutually exclusive with `num_workers`)
        """
        if nodes is None and num_workers is None:
            num_workers = 3

        super(OpenMessagingBenchmarkWorkers,
              self).__init__(ctx, num_nodes=0 if nodes else num_workers)

        if nodes is not None:
            assert len(nodes) > 0
            self.nodes = nodes

    def start_node(self, node):
        self.logger.info("Starting Open Messaging Benchmark worker node on %s",
                         node.account.hostname)

        self.clean_node(node)

        node.account.mkdirs(OpenMessagingBenchmarkWorkers.PERSISTENT_ROOT)

        start_cmd = f"cd /opt/openmessaging-benchmark; \
                      HEAP_OPTS=\" \" \
                      bin/benchmark-worker \
                      --port {OpenMessagingBenchmarkWorkers.PORT} \
                      --stats-port {OpenMessagingBenchmarkWorkers.STATS_PORT} \
                      >> {OpenMessagingBenchmarkWorkers.STDOUT_STDERR_CAPTURE} 2>&1 & disown"

        with node.account.monitor_log(OpenMessagingBenchmarkWorkers.
                                      STDOUT_STDERR_CAPTURE) as monitor:
            node.account.ssh(start_cmd)
            monitor.wait_until(
                "Javalin has started",
                timeout_sec=60,
                backoff_sec=4,
                err_msg=
                "Open Messaging Benchmark worker service didn't finish startup"
            )
        self.logger.info(
            f"Open Messaging Benchmark worker is successfully started on node {node.account.hostname}"
        )

    def raise_on_bad_log_lines(self, node):
        """
        Check if benchmark worker logfile contains errors

        TimeoutException - means that redpanda node is responding too slow,
        so some records can't be produced or consumed
        """
        bad_lines = collections.defaultdict(list)
        self.logger.info(
            f"Scanning node {node.account.hostname} log for errors...")

        for line in node.account.ssh_capture(
                f"grep -e TimeoutException {OpenMessagingBenchmarkWorkers.STDOUT_STDERR_CAPTURE} || true"
        ):
            allowed = False
            for a in LOG_ALLOW_LIST:
                if a in line:
                    allowed = True
                    break

            if not allowed:
                bad_lines[node].append(line)

        if bad_lines:
            raise BadLogLines(bad_lines)

    def check_has_errors(self):
        for node in self.nodes:
            self.raise_on_bad_log_lines(node)

    def stop_node(self, node, allow_fail=False):
        self.logger.info(
            f"Stopping Open Messaging Benchmark worker node on {node.account.hostname}"
        )
        node.account.kill_process("openmessaging-benchmark",
                                  allow_fail=allow_fail)

    def clean_node(self, node):
        self.logger.info(
            f"Cleaning Open Messaging Benchmark worker node on {node.account.hostname}"
        )
        self.stop_node(node, allow_fail=True)
        node.account.remove(OpenMessagingBenchmarkWorkers.PERSISTENT_ROOT,
                            allow_fail=True)

    def get_adresses(self):
        nodes = ""
        for node in self.nodes:
            nodes += f"http://{node.account.hostname}:{OpenMessagingBenchmarkWorkers.PORT},"
        return nodes


# Benchmark process service
class OpenMessagingBenchmark(Service):
    PERSISTENT_ROOT = "/var/lib/openmessaging"
    RESULTS_DIR = os.path.join(PERSISTENT_ROOT, "results")
    RESULT_FILE = os.path.join(RESULTS_DIR, "result.json")
    CHARTS_DIR = os.path.join(PERSISTENT_ROOT, "charts")
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "benchmark.log")
    OPENMESSAGING_DIR = "/opt/openmessaging-benchmark"
    DRIVER_FILE = os.path.join(OPENMESSAGING_DIR,
                               "driver-redpanda/redpanda-ducktape.yaml")
    WORKLOAD_FILE = os.path.join(OPENMESSAGING_DIR, "workloads/ducktape.yaml")
    NUM_WORKERS = 2

    logs = {
        # Includes charts/ and results/ directories along with benchmark.log
        "open_messaging_benchmark_root": {
            "path": PERSISTENT_ROOT,
            "collect_default": True
        },
    }

    def __init__(self,
                 ctx,
                 redpanda,
                 driver="SIMPLE_DRIVER",
                 workload="SIMPLE_WORKLOAD",
                 node=None,
                 worker_nodes=None):
        """
        Creates a utility that can run OpenMessagingBenchmark (OMB) tests in ducktape. See OMB
        documentation for definitions of driver/workload files.

        :param workload: either a string referencing an entry in OMBSampleConfiguration.WORKLOADS,
                         or a dict of workload parameters.
        :param nodes: optional, pre-allocated node to run the benchmark from (by default allocate one)
        :param worker_nodes: optional, list of pre-allocated nodes to run workers on (by default allocate NUM_WORKERS)
        """
        super(OpenMessagingBenchmark,
              self).__init__(ctx, num_nodes=0 if node else 1)

        if node:
            self.nodes = [node]

        self._ctx = ctx
        self.redpanda = redpanda
        self.worker_nodes = worker_nodes
        self.workers = None
        self.driver = OMBSampleConfigurations.DRIVERS[driver]
        if isinstance(workload, str):
            self.workload = OMBSampleConfigurations.WORKLOADS[workload][0]
            self.validator = OMBSampleConfigurations.WORKLOADS[workload][1]
        else:
            self.workload = workload[0]
            self.validator = workload[1]

        self.logger.info("Using driver: %s, workload: %s", self.driver["name"],
                         self.workload["name"])

    def _create_benchmark_workload_file(self, node):
        conf = self.render("omb_workload.yaml", **self.workload)
        self.logger.info("Rendered workload config: \n %s", conf)
        node.account.create_file(OpenMessagingBenchmark.WORKLOAD_FILE, conf)

    def _create_benchmark_driver_file(self, node):
        # if testing redpanda cloud, override with default superuser
        if hasattr(self.redpanda, 'GLOBAL_CLOUD_CLUSTER_CONFIG'):
            u, p, m = self.redpanda._superuser
            self.driver['sasl_username'] = u
            self.driver['sasl_password'] = p
            self.driver['sasl_mechanism'] = m
            self.driver['security_protocol'] = 'SASL_SSL'
            self.driver["redpanda_node"] = self.redpanda.brokers().split(
                ':')[0]
        else:
            self.driver["redpanda_node"] = self.redpanda.nodes[
                0].account.hostname
        conf = self.render("omb_driver.yaml", **self.driver)
        self.logger.info("Rendered driver config: \n %s", conf)
        node.account.create_file(OpenMessagingBenchmark.DRIVER_FILE, conf)

    def _create_workers(self):
        self.workers = OpenMessagingBenchmarkWorkers(
            self._ctx,
            num_workers=OpenMessagingBenchmark.NUM_WORKERS,
            nodes=self.worker_nodes)
        self.workers.start()

    def start_node(self, node):
        idx = self.idx(node)
        self.logger.info("Open Messaging Benchmark: benchmark node - %d on %s",
                         idx, node.account.hostname)

        self.clean_node(node)

        self._create_workers()

        self._create_benchmark_workload_file(node)
        self._create_benchmark_driver_file(node)

        worker_nodes = self.workers.get_adresses()

        self.logger.info(
            f"Starting Open Messaging Benchmark with workers: {worker_nodes}")

        rp_node = None
        if self.redpanda.num_nodes > 0:
            rp_node = self.redpanda.nodes[0]
        rp_version = "unknown_version"
        try:
            rp_version = self.redpanda.get_version(rp_node)
        except AssertionError:
            # In some builds (particularly in dev), version string may not be populated
            pass

        start_cmd = f"cd {OpenMessagingBenchmark.OPENMESSAGING_DIR}; \
                    bin/benchmark \
                    --drivers {OpenMessagingBenchmark.DRIVER_FILE} \
                    --workers {worker_nodes} \
                    --output {OpenMessagingBenchmark.RESULT_FILE} \
                    --service-version {rp_version} \
                    -t swarm \
                    {OpenMessagingBenchmark.WORKLOAD_FILE} >> {OpenMessagingBenchmark.STDOUT_STDERR_CAPTURE} 2>&1 \
                    & disown"

        # This command generates charts and returns some metrics data like latency quantiles and throughput that
        # we can use to determine if they fall in the expected range.
        self.chart_cmd = f"cd {OpenMessagingBenchmark.OPENMESSAGING_DIR} && \
            bin/generate_charts.py --results {OpenMessagingBenchmark.RESULTS_DIR} --output {OpenMessagingBenchmark.CHARTS_DIR}"

        node.account.mkdirs(OpenMessagingBenchmark.RESULTS_DIR)
        node.account.mkdirs(OpenMessagingBenchmark.CHARTS_DIR)
        self.node = node

        with node.account.monitor_log(
                OpenMessagingBenchmark.STDOUT_STDERR_CAPTURE) as monitor:
            node.account.ssh(start_cmd)
            monitor.wait_until(
                "Starting warm-up traffic",
                timeout_sec=60,
                backoff_sec=4,
                err_msg="Open Messaging Benchmark service didn't start")

    def raise_on_bad_log_lines(self, node):
        """
        Check if benchmark logfile contains errors

        Here we expect that log doesn't contain java Exceptions
        """
        bad_lines = collections.defaultdict(list)
        self.logger.info(
            f"Scanning node {node.account.hostname} log for errors...")

        for line in node.account.ssh_capture(
                f"grep -e Exception {OpenMessagingBenchmark.STDOUT_STDERR_CAPTURE} || true"
        ):
            allowed = False
            for a in LOG_ALLOW_LIST:
                if a in line:
                    allowed = True
                    break

            if not allowed:
                bad_lines[node].append(line)

        if bad_lines:
            raise BadLogLines(bad_lines)

    def check_succeed(self, validate_metrics=True):
        self.workers.check_has_errors()
        for node in self.nodes:
            # Here we check that OMB finished and put result in file
            assert node.account.exists(\
                OpenMessagingBenchmark.RESULT_FILE), f"{node.account.hostname} OMB is not finished"
            self.raise_on_bad_log_lines(node)
        # Generate charts from the result
        self.logger.info(f"Generating charts with command {self.chart_cmd}")
        metrics = json.loads(self.node.account.ssh_output(self.chart_cmd))
        if validate_metrics:
            OMBSampleConfigurations.validate_metrics(metrics, self.validator)

    def wait_node(self, node, timeout_sec):
        process_pid = node.account.java_pids("benchmark")
        if len(process_pid) == 0:
            return True
        process_pid = process_pid[0]
        try:
            wait_until(lambda: not node.account.alive(process_pid),
                       timeout_sec=timeout_sec,
                       backoff_sec=10,
                       err_msg="Open Messaging Benchmark reached timeout")
            return True
        except Exception:
            return False

    def stop_node(self, node, allow_fail=False):
        if self.workers is not None:
            self.workers.stop()
        self.logger.info(
            f"Stopping Open Messaging Benchmark node on {node.account.hostname}"
        )
        node.account.kill_process("openmessaging-benchmark",
                                  allow_fail=allow_fail)

    def clean_node(self, node):
        self.logger.info(
            f"Cleaning Open Messaging Benchmark node on {node.account.hostname}"
        )
        self.stop_node(node, allow_fail=True)
        node.account.remove(OpenMessagingBenchmark.PERSISTENT_ROOT,
                            allow_fail=True)

    def benchmark_time(self):
        return self.workload["test_duration_minutes"] + self.workload[
            "warmup_duration_minutes"]
