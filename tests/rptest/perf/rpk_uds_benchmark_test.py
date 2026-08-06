# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# End-to-end validation of the AF_UNIX Kafka listener via rpk, comparing a
# `unix://` seed against a TCP seed (produce, consume, rate-limited).
#
# Starts a single-node Redpanda with both a TCP listener (port 9092) and
# a UDS listener (/var/lib/redpanda/kafka.sock), then runs rpk benchmark
# through each seed on the broker node.
#
# IMPORTANT — what these tests do and do NOT measure:
#   rpk's franz client uses the UDS transport ONLY for the initial metadata
#   fetch against a `unix://` seed. UDS listeners are non-advertisable by
#   design, so once kgo has metadata it keys its per-broker connection pool
#   on the TCP address returned in that metadata, and ALL produce/consume
#   payload flows over TCP — including to the same broker that served the
#   metadata. See src/go/rpk/pkg/kafka/client_franz.go (Scope of UDS in the
#   connection lifecycle). Consequently the MB/s and latency figures below
#   are NOT a UDS-vs-TCP data-plane comparison: they characterize the whole
#   rpk client experience over each seed, and the two transports are expected
#   to be ~equal. Their value is functional (the UDS bootstrap path works and
#   carries a real workload end-to-end), not throughput.
#
#   Raw AF_UNIX-vs-TCP socket throughput / connect / round-trip latency — the
#   actual ~2x UDS win — is measured by the C++ seastar microbenchmarks in
#   src/v/net/tests: uds_throughput_bench, uds_connect_bench, uds_rtt_bench,
#   uds_sustained_bench, uds_concurrency_bench.
#
# Both seeds are exercised from the same machine to eliminate network
# variability. UDS is AF_UNIX (local-only), so the benchmark client must
# run on the broker node.

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until

from rptest.services.cluster import cluster
from rptest.services.redpanda import ResourceSettings
from rptest.perf.redpanda_perf_test import RedpandaPerfTest

UDS_SOCKET_PATH = "/var/lib/redpanda/kafka.sock"
METRICS_PATH = "/tmp/rpk_uds_bench_metrics.json"
LOG_PATH = "/tmp/rpk_uds_bench.log"


@dataclass
class BenchResult:
    transport: str
    record_size: int
    clients: int
    requests_per_sec: float
    mb_per_sec: float
    errors: int
    mode: str = "produce"
    target_rate: float = 0
    p99_latency_us: float = 0
    cpu_user_sec: float = 0
    cpu_sys_sec: float = 0


class RpkUdsBenchmarkPerf(RedpandaPerfTest):
    """Validate the UDS Kafka listener end-to-end via rpk over a `unix://`
    seed vs a TCP seed on the same broker.

    Note: rpk uses UDS only to bootstrap metadata; produce/consume payload
    rides the advertised TCP listener by design (see the module docstring),
    so the throughput/latency numbers are a whole-client comparison expected
    to be ~equal, not a UDS data-plane speedup. Raw UDS-vs-TCP socket perf
    lives in the C++ net/tests/uds_*_bench microbenchmarks.
    """

    PARTITIONS = 6
    REPLICAS = 1
    WARMUP_S = 10
    DURATION_S = 30

    # (record_size_bytes, num_clients)
    MATRIX = [
        (100, 1),
        (100, 10),
        (1024, 1),
        (1024, 10),
        (10240, 1),
        (10240, 10),
    ]

    # Fixed-volume (1 GiB) transfer per (transport, record size). This is the
    # "1 GB of input through Kafka" comparison, driven by
    # `rpk benchmark produce --max-records`.
    ONE_GIB = 1 << 30
    ONE_GB_SIZES = [1024, 16384, 131072]  # 1 KiB, 16 KiB, 128 KiB
    ONE_GB_CLIENTS = 8
    ONE_GB_SAFETY_DURATION_S = 600

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        resource_settings = ResourceSettings(num_cpus=2)
        super().__init__(
            *args,
            num_brokers=1,
            resource_settings=resource_settings,
            extra_node_conf={
                "kafka_api": [
                    {
                        "name": "dnslistener",
                        "address": "0.0.0.0",
                        "port": 9092,
                    },
                    {
                        "name": "udslistener",
                        "unix_path": UDS_SOCKET_PATH,
                    },
                ],
            },
            **kwargs,
        )

    def _run_bench_on_node(
        self,
        node: ClusterNode,
        transport: str,
        brokers: str,
        record_size: int,
        clients: int,
        mode: str = "produce",
        target_rate: float = 0,
        topic: str | None = None,
        max_records: int = 0,
        duration_s: int | None = None,
    ) -> BenchResult:
        if topic is None:
            topic = f"bench-{transport}-{record_size}b-{clients}c"
        rpk = self.redpanda.find_binary("rpk")

        node.account.remove(METRICS_PATH, allow_fail=True)
        node.account.remove(LOG_PATH, allow_fail=True)

        # In fixed-volume (--max-records) mode, --duration is only a safety
        # timeout, so the caller passes a generous duration_s.
        duration = duration_s if duration_s is not None else self.DURATION_S

        cmd = (
            f"{rpk} -X brokers={brokers} benchmark {mode} "
            f"--topic {topic} "
            f"--clients {clients} "
            f"--warmup {self.WARMUP_S} "
            f"--duration {duration} "
            f"--metrics-json {METRICS_PATH} "
            f"--wait-leadership-balanced={'true' if mode == 'produce' else 'false'}"
        )
        if mode == "produce":
            cmd += (
                f" --partitions {self.PARTITIONS}"
                f" --replicas {self.REPLICAS}"
                f" --record-size {record_size}"
            )
            if target_rate > 0:
                cmd += f" --target-rate {target_rate}"
        elif mode == "consume":
            cmd += " --use-existing-topic"
        if max_records > 0:
            cmd += f" --max-records {max_records}"

        wrapped = f"nohup {cmd} >> {LOG_PATH} 2>&1 & echo $!"
        pid = int(
            node.account.ssh_output(wrapped, timeout_sec=10).strip()
        )
        self.logger.debug(
            f"Spawned rpk benchmark {mode} ({transport}) pid={pid}"
        )

        timeout = self.WARMUP_S + duration + 120
        wait_until(
            lambda: not node.account.exists(f"/proc/{pid}"),
            timeout_sec=timeout,
            backoff_sec=2,
            err_msg=(
                f"rpk benchmark {mode} ({transport}) did not finish "
                f"in {timeout}s (pid={pid})"
            ),
        )

        raw = node.account.ssh_output(
            f"cat {METRICS_PATH}", timeout_sec=10
        ).decode("utf-8")
        metrics = json.loads(raw)

        return BenchResult(
            transport=transport,
            record_size=record_size,
            clients=clients,
            requests_per_sec=float(metrics["requests_per_sec"]),
            mb_per_sec=float(metrics["mb_per_sec"]),
            errors=int(metrics["errors"]),
            mode=mode,
            target_rate=target_rate,
            p99_latency_us=float(metrics.get("p99_latency_us", 0)),
            cpu_user_sec=float(metrics.get("cpu_user_sec", 0)),
            cpu_sys_sec=float(metrics.get("cpu_sys_sec", 0)),
        )

    @cluster(num_nodes=3)
    def test_uds_vs_tcp_1gb(self) -> None:
        """Push a fixed 1 GiB per seed/record-size and confirm parity.

        The fixed-volume counterpart to the duration-based produce test: each
        run produces exactly 1 GiB of record payload (via
        `rpk benchmark produce --max-records`) over a TCP seed and a `unix://`
        seed. --duration acts only as a safety timeout here.

        This is a FUNCTIONAL test: it proves the UDS bootstrap path carries a
        real 1 GiB workload end-to-end without errors. It is NOT a UDS
        throughput win — the payload rides TCP in both runs (see the module
        docstring), so the two MB/s numbers are expected to be ~equal
        (ratio ~1.0). For the raw UDS-vs-TCP socket throughput, see the C++
        net/tests/uds_throughput_bench / uds_sustained_bench microbenchmarks.
        """
        node = self.redpanda.nodes[0]
        tcp_brokers = f"{node.account.hostname}:9092"
        uds_brokers = f"unix://{UDS_SOCKET_PATH}"

        results: list[tuple[int, int, BenchResult, BenchResult]] = []
        for record_size in self.ONE_GB_SIZES:
            max_records = self.ONE_GIB // record_size
            label = (
                f"{self._format_size(record_size)} x {max_records} records "
                f"= 1 GiB"
            )
            self.logger.info(f"--- {label} ---")

            tcp = self._run_bench_on_node(
                node, "tcp", tcp_brokers, record_size, self.ONE_GB_CLIENTS,
                max_records=max_records,
                duration_s=self.ONE_GB_SAFETY_DURATION_S,
            )
            assert tcp.errors == 0, f"TCP bench errors: {tcp.errors}"
            self.logger.info(
                f"TCP:  {tcp.mb_per_sec:.2f} MB/s, p99 {tcp.p99_latency_us:.0f}us"
            )

            uds = self._run_bench_on_node(
                node, "uds", uds_brokers, record_size, self.ONE_GB_CLIENTS,
                max_records=max_records,
                duration_s=self.ONE_GB_SAFETY_DURATION_S,
            )
            assert uds.errors == 0, f"UDS bench errors: {uds.errors}"
            self.logger.info(
                f"UDS:  {uds.mb_per_sec:.2f} MB/s, p99 {uds.p99_latency_us:.0f}us"
            )

            results.append((record_size, max_records, tcp, uds))

        # Summary: throughput, derived wall-time for the 1 GiB, p99, ratios.
        # Ratios are uds/tcp. Because payload rides TCP in both runs (UDS is
        # bootstrap-only), the MB ratio is expected to be ~1.0; a large
        # deviation signals measurement noise or a regression, NOT a UDS
        # data-plane effect.
        one_gib_mb = self.ONE_GIB / (1024 * 1024)
        self.logger.info("")
        self.logger.info("=" * 104)
        self.logger.info(
            " UDS vs TCP — fixed 1 GiB produce (rpk --max-records)"
        )
        self.logger.info("=" * 104)
        self.logger.info(
            f"{'MsgSize':>8} "
            f"{'TCP MB/s':>10} {'UDS MB/s':>10} {'MB Ratio':>10} "
            f"{'TCP t(s)':>10} {'UDS t(s)':>10} "
            f"{'TCP p99':>10} {'UDS p99':>10} {'p99 Ratio':>10}"
        )
        self.logger.info("-" * 104)
        for record_size, _max_records, tcp, uds in results:
            mb_ratio = (
                uds.mb_per_sec / tcp.mb_per_sec if tcp.mb_per_sec > 0 else 0
            )
            tcp_time = one_gib_mb / tcp.mb_per_sec if tcp.mb_per_sec > 0 else 0
            uds_time = one_gib_mb / uds.mb_per_sec if uds.mb_per_sec > 0 else 0
            p99_ratio = (
                uds.p99_latency_us / tcp.p99_latency_us
                if tcp.p99_latency_us > 0
                else 0
            )
            self.logger.info(
                f"{self._format_size(record_size):>8} "
                f"{tcp.mb_per_sec:>10.2f} "
                f"{uds.mb_per_sec:>10.2f} "
                f"{mb_ratio:>9.2f}x "
                f"{tcp_time:>10.2f} "
                f"{uds_time:>10.2f} "
                f"{tcp.p99_latency_us:>10.0f} "
                f"{uds.p99_latency_us:>10.0f} "
                f"{p99_ratio:>9.2f}x"
            )
        self.logger.info("=" * 104)

    def _format_size(self, size: int) -> str:
        if size >= 1024:
            return f"{size // 1024} kB"
        return f"{size} B"

    @cluster(num_nodes=3)
    def test_uds_vs_tcp_produce(self) -> None:
        node = self.redpanda.nodes[0]
        tcp_brokers = f"{node.account.hostname}:9092"
        uds_brokers = f"unix://{UDS_SOCKET_PATH}"

        results: list[tuple[BenchResult, BenchResult]] = []

        for record_size, clients in self.MATRIX:
            label = f"{self._format_size(record_size)}, {clients} client(s)"
            self.logger.info(f"--- {label} ---")

            tcp = self._run_bench_on_node(
                node, "tcp", tcp_brokers, record_size, clients
            )
            assert tcp.errors == 0, f"TCP bench errors: {tcp.errors}"
            self.logger.info(
                f"TCP:  {tcp.requests_per_sec:.0f} req/s, "
                f"{tcp.mb_per_sec:.2f} MB/s"
            )

            uds = self._run_bench_on_node(
                node, "uds", uds_brokers, record_size, clients
            )
            assert uds.errors == 0, f"UDS bench errors: {uds.errors}"
            self.logger.info(
                f"UDS:  {uds.requests_per_sec:.0f} req/s, "
                f"{uds.mb_per_sec:.2f} MB/s"
            )

            if tcp.requests_per_sec > 0:
                req_ratio = uds.requests_per_sec / tcp.requests_per_sec
                mb_ratio = uds.mb_per_sec / tcp.mb_per_sec
                self.logger.info(
                    f"UDS/TCP: {req_ratio:.2f}x req/s, {mb_ratio:.2f}x MB/s"
                )

            results.append((tcp, uds))

        # Summary table
        self.logger.info("")
        self.logger.info("=" * 95)
        self.logger.info(
            " UDS vs TCP Produce Benchmark — rpk end-to-end"
        )
        self.logger.info("=" * 95)
        self.logger.info(
            f"{'MsgSize':>8} {'Clients':>8} "
            f"{'TCP req/s':>12} {'UDS req/s':>12} {'req Ratio':>10} "
            f"{'TCP MB/s':>10} {'UDS MB/s':>10} {'MB Ratio':>10}"
        )
        self.logger.info("-" * 95)
        for tcp, uds in results:
            req_ratio = (
                uds.requests_per_sec / tcp.requests_per_sec
                if tcp.requests_per_sec > 0
                else 0
            )
            mb_ratio = (
                uds.mb_per_sec / tcp.mb_per_sec
                if tcp.mb_per_sec > 0
                else 0
            )
            self.logger.info(
                f"{self._format_size(tcp.record_size):>8} "
                f"{tcp.clients:>8} "
                f"{tcp.requests_per_sec:>12.0f} "
                f"{uds.requests_per_sec:>12.0f} "
                f"{req_ratio:>9.2f}x "
                f"{tcp.mb_per_sec:>10.2f} "
                f"{uds.mb_per_sec:>10.2f} "
                f"{mb_ratio:>9.2f}x"
            )
        self.logger.info("=" * 95)

    @cluster(num_nodes=3)
    def test_uds_vs_tcp_consume(self) -> None:
        """Pre-populate topics via produce, then consume and compare."""
        node = self.redpanda.nodes[0]
        tcp_brokers = f"{node.account.hostname}:9092"
        uds_brokers = f"unix://{UDS_SOCKET_PATH}"

        # Phase 1: populate topics with produce
        for record_size, clients in self.MATRIX:
            for transport, brokers in [("tcp", tcp_brokers),
                                       ("uds", uds_brokers)]:
                self.logger.info(
                    f"Populating {transport} "
                    f"size={record_size} clients={clients}"
                )
                result = self._run_bench_on_node(
                    node, transport, brokers, record_size, clients,
                )
                assert result.errors == 0, (
                    f"Populate errors: {result.errors}"
                )

        # Phase 2: consume from populated topics
        results: list[tuple[BenchResult, BenchResult]] = []
        for record_size, clients in self.MATRIX:
            topic = f"bench-tcp-{record_size}b-{clients}c"
            label = (
                f"{self._format_size(record_size)}, {clients} client(s)"
            )
            self.logger.info(f"--- consume {label} ---")

            tcp = self._run_bench_on_node(
                node, "tcp", tcp_brokers, record_size, clients,
                mode="consume", topic=topic,
            )
            assert tcp.errors == 0, f"TCP consume errors: {tcp.errors}"
            self.logger.info(
                f"TCP:  {tcp.requests_per_sec:.0f} req/s, "
                f"{tcp.mb_per_sec:.2f} MB/s, "
                f"p99={tcp.p99_latency_us:.0f} us"
            )

            uds_topic = f"bench-uds-{record_size}b-{clients}c"
            uds = self._run_bench_on_node(
                node, "uds", uds_brokers, record_size, clients,
                mode="consume", topic=uds_topic,
            )
            assert uds.errors == 0, f"UDS consume errors: {uds.errors}"
            self.logger.info(
                f"UDS:  {uds.requests_per_sec:.0f} req/s, "
                f"{uds.mb_per_sec:.2f} MB/s, "
                f"p99={uds.p99_latency_us:.0f} us"
            )

            results.append((tcp, uds))

        self.logger.info("")
        self.logger.info("=" * 110)
        self.logger.info(
            " UDS vs TCP Consume Benchmark — rpk end-to-end"
        )
        self.logger.info("=" * 110)
        self.logger.info(
            f"{'MsgSize':>8} {'Clients':>8} "
            f"{'TCP req/s':>12} {'UDS req/s':>12} {'req Ratio':>10} "
            f"{'TCP MB/s':>10} {'UDS MB/s':>10} {'MB Ratio':>10} "
            f"{'TCP p99':>10} {'UDS p99':>10}"
        )
        self.logger.info("-" * 110)
        for tcp, uds in results:
            req_ratio = (
                uds.requests_per_sec / tcp.requests_per_sec
                if tcp.requests_per_sec > 0
                else 0
            )
            mb_ratio = (
                uds.mb_per_sec / tcp.mb_per_sec
                if tcp.mb_per_sec > 0
                else 0
            )
            self.logger.info(
                f"{self._format_size(tcp.record_size):>8} "
                f"{tcp.clients:>8} "
                f"{tcp.requests_per_sec:>12.0f} "
                f"{uds.requests_per_sec:>12.0f} "
                f"{req_ratio:>9.2f}x "
                f"{tcp.mb_per_sec:>10.2f} "
                f"{uds.mb_per_sec:>10.2f} "
                f"{mb_ratio:>9.2f}x "
                f"{tcp.p99_latency_us:>10.0f} "
                f"{uds.p99_latency_us:>10.0f}"
            )
        self.logger.info("=" * 110)

    @cluster(num_nodes=3)
    def test_uds_vs_tcp_rate_limited(self) -> None:
        """Produce at fixed rate targets and compare latency/CPU."""
        node = self.redpanda.nodes[0]
        tcp_brokers = f"{node.account.hostname}:9092"
        uds_brokers = f"unix://{UDS_SOCKET_PATH}"

        rate_matrix = [
            (100, 10),
            (100, 50),
            (1024, 10),
            (1024, 50),
        ]

        results: list[tuple[BenchResult, BenchResult]] = []
        for record_size, target_rate in rate_matrix:
            label = (
                f"{self._format_size(record_size)}, "
                f"{target_rate} MB/s"
            )
            self.logger.info(f"--- rated {label} ---")

            tcp = self._run_bench_on_node(
                node, "tcp", tcp_brokers, record_size, 1,
                target_rate=target_rate,
                topic=f"rated-tcp-{record_size}b-{target_rate}mbps",
            )
            assert tcp.errors == 0, f"TCP rated errors: {tcp.errors}"
            self.logger.info(
                f"TCP:  {tcp.mb_per_sec:.2f} MB/s, "
                f"p99={tcp.p99_latency_us:.0f} us, "
                f"cpu={tcp.cpu_user_sec + tcp.cpu_sys_sec:.2f}s"
            )

            uds = self._run_bench_on_node(
                node, "uds", uds_brokers, record_size, 1,
                target_rate=target_rate,
                topic=f"rated-uds-{record_size}b-{target_rate}mbps",
            )
            assert uds.errors == 0, f"UDS rated errors: {uds.errors}"
            self.logger.info(
                f"UDS:  {uds.mb_per_sec:.2f} MB/s, "
                f"p99={uds.p99_latency_us:.0f} us, "
                f"cpu={uds.cpu_user_sec + uds.cpu_sys_sec:.2f}s"
            )

            results.append((tcp, uds))

        self.logger.info("")
        self.logger.info("=" * 110)
        self.logger.info(
            " UDS vs TCP Rate-Limited Produce — rpk end-to-end"
        )
        self.logger.info("=" * 110)
        self.logger.info(
            f"{'MsgSize':>8} {'Rate':>8} "
            f"{'TCP MB/s':>10} {'UDS MB/s':>10} "
            f"{'TCP p99':>10} {'UDS p99':>10} {'p99 Ratio':>10} "
            f"{'TCP CPU':>10} {'UDS CPU':>10} {'CPU Ratio':>10}"
        )
        self.logger.info("-" * 110)
        for tcp, uds in results:
            p99_ratio = (
                uds.p99_latency_us / tcp.p99_latency_us
                if tcp.p99_latency_us > 0
                else 0
            )
            tcp_cpu = tcp.cpu_user_sec + tcp.cpu_sys_sec
            uds_cpu = uds.cpu_user_sec + uds.cpu_sys_sec
            cpu_ratio = uds_cpu / tcp_cpu if tcp_cpu > 0 else 0
            self.logger.info(
                f"{self._format_size(tcp.record_size):>8} "
                f"{tcp.target_rate:>7.0f} "
                f"{tcp.mb_per_sec:>10.2f} "
                f"{uds.mb_per_sec:>10.2f} "
                f"{tcp.p99_latency_us:>10.0f} "
                f"{uds.p99_latency_us:>10.0f} "
                f"{p99_ratio:>9.2f}x "
                f"{tcp_cpu:>10.2f} "
                f"{uds_cpu:>10.2f} "
                f"{cpu_ratio:>9.2f}x"
            )
        self.logger.info("=" * 110)
