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
from typing import Optional, Any

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.node_container import NodeContainer

from rptest.services.redpanda import RedpandaService, RedpandaServiceBase, RedpandaServiceCloud
from rptest.services.utils import BadLogLines
from rptest.services.openmessaging_benchmark_configs import OMBSampleConfigurations, ValidatorDict

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

    def __init__(self,
                 ctx,
                 num_workers=None,
                 nodes: Optional[list[ClusterNode]] = None):
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

    def start_node(self, node, timeout_sec=60, **kwargs):
        self.logger.info("Starting Open Messaging Benchmark worker node on %s",
                         node.account.hostname)

        self.clean_node(node)

        node.account.mkdirs(OpenMessagingBenchmarkWorkers.PERSISTENT_ROOT)

        start_cmd = (
            f"cd /opt/openmessaging-benchmark; "
            f"HEAP_OPTS=\" \" "
            f"KAFKA_OPTS=\" \" "
            f"bin/benchmark-worker "
            f"--port {OpenMessagingBenchmarkWorkers.PORT} "
            f"--stats-port {OpenMessagingBenchmarkWorkers.STATS_PORT} "
            f">> {OpenMessagingBenchmarkWorkers.STDOUT_STDERR_CAPTURE} 2>&1 & disown"
        )

        with node.account.monitor_log(OpenMessagingBenchmarkWorkers.
                                      STDOUT_STDERR_CAPTURE) as monitor:
            node.account.ssh(start_cmd)
            monitor.wait_until(
                "Javalin has started",
                timeout_sec=timeout_sec,
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

    def stop_node(self, node, allow_fail=False, **_):
        self.logger.info(
            f"Stopping Open Messaging Benchmark worker node on {node.account.hostname}"
        )
        node.account.kill_process("openmessaging-benchmark",
                                  allow_fail=allow_fail)

    def clean_node(self, node, **_):
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


WorkloadDict = dict[str, Any]
WorkloadTuple = tuple[WorkloadDict, ValidatorDict]


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

    nodes: list[ClusterNode]

    def __init__(self,
                 ctx,
                 redpanda: RedpandaService | RedpandaServiceCloud,
                 driver: str | dict[str, Any] = "SIMPLE_DRIVER",
                 workload: str | WorkloadTuple = "SIMPLE_WORKLOAD",
                 node: ClusterNode | None = None,
                 worker_nodes=None,
                 topology="swarm",
                 num_workers=NUM_WORKERS):
        """
        Creates a utility that can run OpenMessagingBenchmark (OMB) tests in ducktape. See OMB
        documentation for definitions of driver/workload files.

        :param workload: either a string referencing an entry in OMBSampleConfiguration.WORKLOADS,
                         or tuple of (workload_dict, validator_dict) (see OMBSampleConfiguration.WORKLOADS for the
                         structure of the dicts)
        :param nodes: optional, pre-allocated node to run the benchmark from (by default allocate one)
        :param worker_nodes: optional, list of pre-allocated nodes to run workers on (by default allocate NUM_WORKERS)
        """
        super(OpenMessagingBenchmark,
              self).__init__(ctx, num_nodes=0 if node else 1)

        if node:
            self.nodes = [node]

        self._metrics: dict[str, Any] = {}
        self._ctx = ctx
        self.topology = topology
        self.redpanda = redpanda
        self.worker_nodes = worker_nodes
        self.num_workers = num_workers
        self.workers = None
        if isinstance(driver, str):
            self.driver = OMBSampleConfigurations.DRIVERS[driver]
        else:
            self.driver = driver
        if isinstance(workload, str):
            self.workload = OMBSampleConfigurations.WORKLOADS[workload][0]
            self.validator = OMBSampleConfigurations.WORKLOADS[workload][1]
        else:
            self.workload = workload[0]
            self.validator = workload[1]

        assert int(
            self.workload.get("warmup_duration_minutes", '0')
        ) >= 1, "must use non-zero warmup time as we rely on warm-up message to detect test start"

        self.logger.info("Using driver: %s, workload: %s", self.driver["name"],
                         self.workload["name"])

    def _create_benchmark_workload_file(self, node):
        conf = self.render("omb_workload.yaml", **self.workload)
        self.logger.info("Rendered workload config: \n %s", conf)
        node.account.create_file(OpenMessagingBenchmark.WORKLOAD_FILE, conf)

    def _create_benchmark_driver_file(self, node):
        # if testing redpanda cloud, override with default superuser
        if isinstance(self.redpanda, RedpandaServiceCloud):
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
            self._ctx, num_workers=self.num_workers, nodes=self.worker_nodes)
        self.workers.start()

    @property
    def metrics(self):
        """Metrics from the results of an OMB run.
        """
        return self._metrics

    def start_node(self, node, timeout_sec=5 * 60, **kwargs):
        idx = self.idx(node)
        self.logger.info("Open Messaging Benchmark: benchmark node - %d on %s",
                         idx, node.account.hostname)

        self.clean_node(node)

        self._create_workers()

        self._create_benchmark_workload_file(node)
        self._create_benchmark_driver_file(node)

        assert self.workers
        worker_nodes = self.workers.get_adresses()

        self.logger.info(
            f"Starting Open Messaging Benchmark with workers: {worker_nodes}")

        # This version is used for the charts, in the cloud we use the
        # install pack version, otherwise the redpanda version
        if isinstance(self.redpanda, RedpandaServiceCloud):
            rp_version = self.redpanda.install_pack_version()
        else:
            try:
                rp_version = self.redpanda.get_version(self.redpanda.nodes[0])
            except AssertionError:
                # In some builds (particularly in dev), version string may not be populated
                rp_version = "unknown_version"

        start_cmd = f"cd {OpenMessagingBenchmark.OPENMESSAGING_DIR}; \
                    bin/benchmark \
                    --drivers {OpenMessagingBenchmark.DRIVER_FILE} \
                    --workers {worker_nodes} \
                    --output {OpenMessagingBenchmark.RESULT_FILE} \
                    --service-version {rp_version} \
                    -t {self.topology} \
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
                timeout_sec=timeout_sec,
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

    def check_succeed(self, validate_metrics=True, raise_exceptions=True):
        """
        Evaluates the success of a benchmark test based on various metrics and conditions.

        Parameters:
        - validate_metrics (bool): If True, performs validation checks on the benchmark metrics to determine if
        the test results are within expected limits. Default is True.

        - raise_exceptions (bool): If True, the method raises an exception if the benchmark test fails based
        on the validation of the metrics. Default is True.

        Returns:
        - A tuple (is_successful, results) where `is_successful` is a boolean indicating the success of the test,
        and `results` is validation results

        Raises:
        - Exception: If `raise_exceptions` is True and the test is determined to fail based on the metrics validation.
        """
        assert self.workers
        self.workers.check_has_errors()
        for node in self.nodes:
            # Here we check that OMB finished and put result in file
            assert node.account.exists(\
                OpenMessagingBenchmark.RESULT_FILE), f"{node.account.hostname} OMB is not finished"
            self.raise_on_bad_log_lines(node)
        # Generate charts from the result
        self.logger.info(f"Generating charts with command {self.chart_cmd}")
        self.node.account.ssh_output(self.chart_cmd)
        metrics = json.loads(
            self.node.account.ssh_output(
                f'cat {OpenMessagingBenchmark.RESULT_FILE}'))

        # Previously we were using generate_charts.py to get the metrics which
        # calculated this additional metric. Hence we do it here for backwards
        # compatibility.
        metrics['throughputMBps'] = (
            sum(metrics['publishRate']) / len(metrics['publishRate']) *
            metrics['messageSize']) / (1024.0 * 1024.0)
        metrics['publishLatencyMin'] = min(metrics['publishLatencyMin'])
        metrics['endToEndLatencyMin'] = min(metrics['endToEndLatencyMin'])

        self._metrics = metrics

        if validate_metrics and raise_exceptions:
            OMBSampleConfigurations.validate_metrics(self._metrics,
                                                     self.validator)

        if validate_metrics and not raise_exceptions:
            is_valid, results = OMBSampleConfigurations.validate_metrics(
                self._metrics, self.validator, raise_exceptions=False)
            return is_valid, results

    def detect_spikes_by_percentile(self,
                                    results: dict[str, Any],
                                    expected_max_latencies: dict[str, float],
                                    max_spike_width=1):
        """
        Detects and evaluates latency spikes in multiple series of latency data based on predefined thresholds.
        This function analyzes each series to determine if there are isolated spikes or sustained high latency events that exceed the allowed maximum thresholds.

        Why?
            There are cases where we may have latency spikes due to disk activity.
            By detecting these spikes, we can retry the test to help ensure that the results are not influenced by temporary anomalies.
            Additionally, this allows some degree of latency during specified periods which is controlled by the max_spike_width parameter.
            Related GH issue: https://github.com/redpanda-data/core-internal/issues/1180

        Parameters:
            results (dict): A dictionary where each key is a series identifier and each value is a list of latency measurements for that series.
            max_spike_width (int): The maximum number of consecutive measurements that can exceed the expected maximum before being considered a sustained high latency rather than an isolated spike.
            expected_max_latencies (dict): A dictionary where each key is a series identifier and each value is the maximum allowed latency for that series.

        Returns:
            bool: A boolean indicating whether a retry is advised based on the spike detection analysis.

        Notes:
            - "Isolated spike" is defined as a spike where the number of consecutive measurements exceeding the threshold does not surpass `max_spike_width`.
            - "Sustained high latency" is defined as a situation where more than `max_spike_width` consecutive measurements exceed the allowed maximum, suggesting a more serious issue that a retry might not resolve.
            - If any sustained high latency is detected, the function advises against a retry regardless of other findings.
        """
        def detect_spikes_in_series(latency_series, expected_max):
            high_latency_start = None
            consecutive_high_latency_count = 0
            isolated_spikes = []
            sustained_detected = False  # Track if sustained high latency is detected

            for i, value in enumerate(latency_series):
                if value > expected_max:
                    if high_latency_start is None:
                        high_latency_start = i
                    consecutive_high_latency_count += 1
                    if consecutive_high_latency_count > max_spike_width:
                        self.logger.debug(
                            f"Sustained high latency detected in series {key}, not considered a spike. Sequence starts at index {high_latency_start} and ends at index {i}. Values: {latency_series[high_latency_start:i+1]}"
                        )
                        sustained_detected = True
                else:
                    if high_latency_start is not None and 0 < consecutive_high_latency_count <= max_spike_width:
                        if high_latency_start == i - 1:
                            self.logger.debug(
                                f"Isolated spike detected at index {high_latency_start}. Value: {latency_series[high_latency_start]}"
                            )
                        else:
                            self.logger.debug(
                                f"Isolated spike detected between indices {high_latency_start} and {i-1}. Values: {latency_series[high_latency_start:i]}"
                            )
                        isolated_spikes.append((high_latency_start, i - 1))
                    consecutive_high_latency_count = 0
                    high_latency_start = None

            if 0 < consecutive_high_latency_count <= max_spike_width:
                self.logger.debug(
                    f"Ending isolated spike detected from index {high_latency_start} to {len(latency_series)-1}. Values: {latency_series[high_latency_start:]}"
                )
                isolated_spikes.append(
                    (high_latency_start, len(latency_series) - 1))

            return isolated_spikes, sustained_detected

        should_retry = False
        sustained_anywhere = False  # Flag to track sustained high latency across different series
        for key, series in results.items():
            if key in expected_max_latencies:
                expected_max = expected_max_latencies[key]
                self.logger.debug(f"Checking for spikes in series: {key}")
                isolated_spikes, sustained_detected = detect_spikes_in_series(
                    series, expected_max)
                if isolated_spikes:
                    self.logger.debug(f"Spikes detected in series: {key}.")
                    should_retry = True
                if sustained_detected:
                    self.logger.debug(
                        f"Sustained high latency detected in series: {key}. Advising against retry."
                    )
                    sustained_anywhere = True
                else:
                    self.logger.debug(
                        f"Latency data for {key} series is within allowed max values"
                    )
            else:
                self.logger.debug(
                    f"Series {key} not in expected max latencies mapping, skipping."
                )

        if sustained_anywhere:
            self.logger.info(
                "No retry advised due to sustained high latency in one or more series."
            )
            should_retry = False
        elif should_retry:
            self.logger.info("Advising retry due to isolated spikes.")
        else:
            self.logger.info(
                "Retry is not needed. Data is within allowed max values")

        return should_retry

    def wait_node(self, node, timeout_sec=None):
        assert timeout_sec is not None
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

    def stop_node(self, node, allow_fail=False, **_):
        if self.workers is not None:
            self.workers.stop()
        self.logger.info(
            f"Stopping Open Messaging Benchmark node on {node.account.hostname}"
        )
        node.account.kill_process("openmessaging-benchmark",
                                  allow_fail=allow_fail)

    def clean_node(self, node, **_):
        self.logger.info(
            f"Cleaning Open Messaging Benchmark node on {node.account.hostname}"
        )
        self.stop_node(node, allow_fail=True)
        node.account.remove(OpenMessagingBenchmark.PERSISTENT_ROOT,
                            allow_fail=True)

    def get_workload_int(self, key: str) -> int:
        """Get the workload property specified by key: it must exist and be an int."""
        v = self.workload[key]
        assert isinstance(v, int), f"value {v} for {key} was not an int"
        return v

    def benchmark_time(self) -> int:
        """An estimate of the runtime of the test in minutes. This simply sums the warmup and test runtime so
        is always an underestimate (unless the run fails early), so you should add a few minutes of
        buffer to the returned value to set timeouts."""
        return self.get_workload_int(
            "test_duration_minutes") + self.get_workload_int(
                "warmup_duration_minutes")
