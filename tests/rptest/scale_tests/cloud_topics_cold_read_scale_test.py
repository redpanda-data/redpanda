# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierConsumerGroupConsumer,
)
from rptest.services.admin import Admin
from rptest.services.redpanda import SISettings, MetricsEndpoint
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.scale_parameters import ScaleParameters


class CloudTopicsColdReadScaleTest(PreallocNodesTest):
    """Scale gate + A/B: produce-latency sensitivity to cold-read load, under
    each cloud_io admission-control policy.

    A producer writes a cloud topic at a modest, constant rate; its data exceeds
    the cloud cache, so reads miss and fetch L1 cold, contending the per-shard S3
    pool against the produce path's L0 uploads. With the shipped reservation,
    producer_upload keeps a floor of 2 of the pool's 8 connections.

    Each admission-control-policy arm runs two cold-read stages on the same cluster and
    backlog -- a low-load reference (N_LOW readers, below saturation) and a heavy
    load that saturates the pool (N_HIGH) -- and gates the produce-p99 ratio
    between them. The reservation floor should hold produce p99 nearly flat
    (ratio ~1); without it (passthrough) produce competes unprotected and p99
    climbs. Measuring within an arm makes each its own control: the two policies
    run as separate @matrix clusters and can't be compared in one method, so each
    asserts its own ratio. Guards confirm the run mattered -- both stages read
    cold, and stage 2 saturated the pool.

    Teardown SIGKILLs the brokers: under sustained saturation a few reconcilers
    wedge on orphaned multipart uploads (CORE-16648 -- no abort/timeout on that
    path) and graceful shutdown hangs. The verdict is already decided, so we
    abandon the cluster rather than block on the wedge.

    Scope: a coarse CDT gate. The precise reservation-vs-passthrough latency A/B
    lives in the bench-runner tier-9 configs; the floor is unit-tested in
    cloud_io/tests/admission_control_test.cc.
    """

    topics = ()

    NUM_BROKERS = 9
    NUM_CLIENT_NODES = 2  # producer on [0], reader on [1]
    MSG_SIZE = 16 * 1024

    # High partition count so cold reads contend the pool. Capped by the
    # cluster's partition limit at runtime.
    MAX_PARTITIONS = 6000

    # Small enough that cold reads contend it at ducktape scale, but above the
    # pool=4 collapse (reads cascade into cloud_op_timeouts and the reconciler
    # wedges). The shipped reservation floors producer_upload at 2 of the 8.
    POOL_CONNECTIONS = 8

    # Small cache so the backlog can't fit -- looping readers keep missing it and
    # fetch cold from object storage.
    CLOUD_CACHE_SIZE = 1 * 1024**3  # 1 GiB

    # Modest produce that the floor's reserved connections easily sustain. We do
    # NOT crank produce to force contention -- high produce pool-binds the produce
    # path itself (256 MiB/s timed out the backlog build). Contention comes from
    # read pressure instead (N_LOW/N_HIGH).
    PRODUCE_RATE_BPS = 20 * 1024**2  # 20 MiB/s
    BACKLOG_SEC = 3 * 60
    SAMPLE_INTERVAL_SEC = 30

    # Keep N_LOW well below saturation so the ratio's denominator is the
    # uncontended case. N_HIGH saturates the pool and still fits one client node;
    # ~256 readers would cap the client before the pool.
    N_LOW = 16  # stage 1: low-load reference (unsaturated)
    N_HIGH = 128  # stage 2: heavy cold-read load (saturates the pool)

    # Per-stage duration, and the settle window skipped at each stage start
    # (consumer-group rebalance + ramp) before sampling steady-state produce p99.
    STAGE_SEC = 6 * 60
    STAGE_SETTLE_SEC = 2 * 60

    # Gate on the stage2/stage1 median-p99 ratio. Observed: reservation ~0.8x
    # (floor holds -- produce p99 is flat-to-better under the load step) vs
    # passthrough ~2.8x (no floor). R_LO/R_HI sit in that gap with margin;
    # passthrough clearing R_HI also confirms stage 2 built real contention.
    # Median, not worst-window (the worst is noisier).
    R_LO = 1.1  # reservation: produce p99 barely moves under load
    R_HI = 2.0  # passthrough: produce p99 degrades sharply

    # Stage 2 must actually saturate the pool for the ratio to mean anything.
    # Arbitrary, well below observed depths (peak waiters run ~1500-3400) -- only
    # asserts "the pool saturated", not a tuned threshold.
    MIN_STAGE2_WAITERS = 500

    def __init__(self, test_context: TestContext):
        # Admission-control policy is parametrized (@matrix); read it here so the cluster
        # starts under the right policy (cloud_io_admission_control_policy is
        # restart-only, so it can't be flipped mid-test).
        self._admission_control_policy = (test_context.injected_args or {}).get(
            "admission_control_policy", "reservation"
        )
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=self.POOL_CONNECTIONS,
            cloud_storage_cache_size=self.CLOUD_CACHE_SIZE,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
            # End-of-test scrub reads every partition's manifest back; at this
            # partition count it overruns the runner timeout. Skip it as other
            # scale tests do -- orthogonal to what this gates.
            skip_end_of_test_scrubbing=True,
        )
        extra_rp_conf = {
            "enable_cluster_metadata_upload_loop": False,
            # The reservation arm reads an internal (vectorized_) metric, the
            # cloud_io admission-control waiter gauge, so internal metrics must be on.
            "disable_metrics": False,
            # Per @matrix arm: 'reservation' (the floor under test) vs
            # 'passthrough' (control -- no floor, admission control bypassed).
            "cloud_io_admission_control_policy": self._admission_control_policy,
        }
        super().__init__(
            test_context,
            num_brokers=self.NUM_BROKERS,
            node_prealloc_count=self.NUM_CLIENT_NODES,
            si_settings=si_settings,
            extra_rp_conf=extra_rp_conf,
        )
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    def _cluster_healthy(self) -> bool:
        overview = self.admin.get_cluster_health_overview()
        healthy = overview.get("is_healthy", False)
        if not healthy:
            self.logger.warning(f"Cluster unhealthy: {overview}")
        return healthy

    def _total_pool_waiters(self) -> float:
        # Cluster-wide fibers queued for an S3 pool slot. >0 means the pool is
        # the binding resource somewhere, i.e. cold reads are contending it.
        # Registered only by the reservation policy -- absent under passthrough.
        return self.redpanda.metric_sum(
            "vectorized_cloud_io_admission_control_total_waiters",
            metrics_endpoint=MetricsEndpoint.METRICS,
            expect_metric=True,
        )

    @cluster(num_nodes=11)
    @skip_debug_mode
    @matrix(admission_control_policy=["reservation", "passthrough"])
    def test_produce_under_cold_reads(self, admission_control_policy: str):
        # __init__ configured the cluster from injected_args; the method branches
        # on the @matrix param. Assert they match so a non-matrix invocation can't
        # configure one policy while the assertions use the other.
        assert admission_control_policy == self._admission_control_policy, (
            f"admission_control_policy param {admission_control_policy!r} != configured "
            f"{self._admission_control_policy!r}"
        )
        scale = ScaleParameters(self.redpanda, replication_factor=3)
        partitions = min(self.MAX_PARTITIONS, max(1, scale.partition_limit))

        topic = "cloud_topics_cold_read"
        self.rpk.create_topic(
            topic=topic,
            partitions=partitions,
            replicas=3,
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD},
        )
        self.logger.info(
            f"Created cloud topic {topic} with {partitions} partitions "
            f"(partition_limit={scale.partition_limit})"
        )

        rate = int(min(self.PRODUCE_RATE_BPS, scale.expect_bandwidth))
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=self.MSG_SIZE,
            msg_count=1_000_000_000,  # ceiling; the producer is stopped by time
            rate_limit_bps=rate,
            custom_node=[self.preallocated_nodes[0]],
            tolerate_failed_produce=True,
        )

        def run_stage(n_readers: int) -> tuple[float, float, int, float]:
            """Run one cold-read stage at n_readers for STAGE_SEC; return (p99
            median, p99 worst, reads, peak waiters) over the post-settle samples.
            loop=True re-reads the backlog from offset 0 (kgo-verifier auto-names
            a fresh group per instance and forbids loop + an explicit name, so
            sequential stages stay independent). Single-process, one node."""
            reader = KgoVerifierConsumerGroupConsumer(
                self.test_context,
                self.redpanda,
                topic,
                self.MSG_SIZE,
                readers=n_readers,
                loop=True,
                nodes=[self.preallocated_nodes[1]],
            )
            reader.start()
            try:
                stage_start = time.time()
                p99s: list[float] = []
                peak = 0.0
                while time.time() - stage_start < self.STAGE_SEC:
                    time.sleep(self.SAMPLE_INTERVAL_SEC)
                    elapsed = time.time() - stage_start
                    p99_us = producer.produce_status.latency["p99"]
                    if admission_control_policy == "reservation":
                        peak = max(peak, self._total_pool_waiters())
                    settled = elapsed >= self.STAGE_SETTLE_SEC
                    if settled:
                        p99s.append(p99_us)
                    self.logger.info(
                        f"[{admission_control_policy}] stage n={n_readers} t={elapsed:.0f}s "
                        f"{'steady' if settled else 'settle'}: "
                        f"produce_p99={p99_us / 1000:.0f}ms, "
                        f"reads={reader.consumer_status.validator.valid_reads}, "
                        f"pool_waiters={peak:.0f}"
                    )
                reads = reader.consumer_status.validator.valid_reads
            finally:
                reader.stop()
            p99_median = sorted(p99s)[len(p99s) // 2] if p99s else 0.0
            p99_worst = max(p99s) if p99s else 0.0
            return p99_median, p99_worst, reads, peak

        producer.start()
        try:
            # ── Build a backlog larger than the cloud cache ──────────────
            backlog_target = int(rate * self.BACKLOG_SEC / self.MSG_SIZE)
            backlog_bytes = backlog_target * self.MSG_SIZE
            # Cold-read premise: the backlog must exceed the cache, else looping
            # readers serve warm -- and the byte-based guard below can't tell (a
            # small looped backlog still reads far more than the cache, warm).
            # rate can clamp to expect_bandwidth, so fail fast if it undershoots.
            assert backlog_bytes > self.CLOUD_CACHE_SIZE, (
                f"planned backlog {backlog_bytes} B (rate {rate} B/s x "
                f"{self.BACKLOG_SEC}s) <= cache {self.CLOUD_CACHE_SIZE} B -- reads "
                f"would be warm; raise BACKLOG_SEC or use a less rate-limited cluster"
            )
            self.logger.info(
                f"Building backlog: ~{backlog_target} msgs "
                f"(~{backlog_bytes / 1024**3:.0f} GiB) vs "
                f"{self.CLOUD_CACHE_SIZE / 1024**3:.0f} GiB cache, at {rate} B/s"
            )
            producer.wait_for_acks(
                count=backlog_target, timeout_sec=self.BACKLOG_SEC * 2, backoff_sec=5
            )

            # ── Two cold-read stages: low-load reference, then heavy load ──
            # kgo-verifier reports produce ack latency in microseconds over a
            # rolling window. Capture the uncontended baseline before any readers.
            baseline_p99_us = producer.produce_status.latency["p99"]
            self.logger.info(
                f"[{admission_control_policy}] baseline produce p99 = "
                f"{baseline_p99_us / 1000:.0f} ms; stage 1 (low load) n={self.N_LOW}"
            )
            s1_median, s1_worst, s1_reads, _ = run_stage(self.N_LOW)
            self.logger.info(
                f"[{admission_control_policy}] stage 2 (heavy load) n={self.N_HIGH}"
            )
            s2_median, s2_worst, s2_reads, s2_waiters = run_stage(self.N_HIGH)
        finally:
            producer.stop()

        ratio_median = s2_median / s1_median if s1_median else 0.0
        ratio_worst = s2_worst / s1_worst if s1_worst else 0.0
        s1_read_bytes = s1_reads * self.MSG_SIZE
        s2_read_bytes = s2_reads * self.MSG_SIZE
        self.logger.info(
            f"[{admission_control_policy}] produce p99 vs cold-read load: "
            f"n={self.N_LOW} median/worst={s1_median / 1000:.0f}/{s1_worst / 1000:.0f}ms, "
            f"n={self.N_HIGH} median/worst={s2_median / 1000:.0f}/{s2_worst / 1000:.0f}ms; "
            f"load-sensitivity ratio (stage2/stage1) median={ratio_median:.2f}x "
            f"worst={ratio_worst:.2f}x (baseline p99={baseline_p99_us / 1000:.0f}ms); "
            f"stage reads ~{s1_read_bytes / 1024**3:.0f}/{s2_read_bytes / 1024**3:.0f} GiB; "
            f"stage2 pool_waiters_peak={s2_waiters:.0f}"
        )

        try:
            # Both arms: both stages must have read genuinely cold (more than the
            # cache), else the comparison is moot.
            assert (
                s1_read_bytes > self.CLOUD_CACHE_SIZE
                and s2_read_bytes > self.CLOUD_CACHE_SIZE
            ), (
                f"reads weren't cold: stage1 {s1_read_bytes} B, stage2 "
                f"{s2_read_bytes} B vs {self.CLOUD_CACHE_SIZE} B cache"
            )

            # Non-degenerate p99 series: a zero s1_median makes ratio_median 0.0,
            # which would pass the reservation gate (0.0 <= R_LO) trivially.
            assert s1_median > 0 and s2_median > 0, (
                f"degenerate produce-p99 series (stage1 median={s1_median}, "
                f"stage2 median={s2_median}) -- no meaningful latency measured"
            )

            if admission_control_policy == "reservation":
                # Stage 2 must have saturated the pool, else the ratio is trivial
                # (produce is fast when nothing competes).
                assert s2_waiters >= self.MIN_STAGE2_WAITERS, (
                    f"the high stage only reached {s2_waiters:.0f} pool waiters "
                    f"(expected >= {self.MIN_STAGE2_WAITERS}) -- it didn't saturate "
                    f"the pool, so the latency ratio is trivial. Raise N_HIGH or "
                    f"MAX_PARTITIONS."
                )
                # The floor must keep produce p99 nearly flat across the load step.
                assert ratio_median <= self.R_LO, (
                    f"reservation floor failed to protect produce: p99 inflated "
                    f"{ratio_median:.2f}x from n={self.N_LOW} to n={self.N_HIGH} "
                    f"under cold-read load (limit {self.R_LO}x)"
                )
            else:
                # No floor: produce p99 must degrade sharply across the load step
                # (which also confirms stage 2 built real contention).
                assert ratio_median >= self.R_HI, (
                    f"passthrough produce p99 only moved {ratio_median:.2f}x from "
                    f"n={self.N_LOW} to n={self.N_HIGH} (expected >= {self.R_HI}x) "
                    f"-- the high stage didn't build enough contention; raise "
                    f"N_HIGH or MAX_PARTITIONS"
                )
            wait_until(
                self._cluster_healthy,
                timeout_sec=60,
                backoff_sec=5,
                err_msg="Cluster has unavailable partitions after the test",
            )
            self.logger.info(
                f"Cluster healthy -- {admission_control_policy} arm passed"
            )
        finally:
            # SIGKILL rather than block on the reconciler-wedge hang (CORE-16648,
            # see the class docstring); the verdict is already decided above.
            #
            # HACK: when CORE-16648 is fixed, drop this force-stop and let the
            # graceful teardown run -- a clean shutdown then becomes a real check.
            # self.logger.info(
            #     "Force-stopping brokers to avoid the reconciler-wedge shutdown "
            #     "hang (CORE-16648)"
            # )
            # self.redpanda.stop(forced=True)
            self.logger.info("Let brokers tear down gracefully")
            self.redpanda.stop()
