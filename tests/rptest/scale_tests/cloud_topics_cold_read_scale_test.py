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
    each cloud_io scheduler policy.

    A producer writes a cloud topic at a modest, constant rate. Its data exceeds
    the cloud cache, so any read misses and fetches L1 cold, contending the
    per-shard S3 pool against the produce-path L0 uploads. The pool is
    deliberately small (8) so cold reads contend it at ducktape scale, but not so
    small that the cluster collapses (at pool=4 the reads cascade into
    cloud_op_timeouts and the reconciler wedges); with the shipped 2+2+2
    reservation, producer_upload keeps a floor of 2.

    Each arm runs TWO cold-read stages on the same cluster and backlog -- stage 1
    at N_LOW readers (a low-load reference, deliberately below pool saturation)
    and stage 2 at N_HIGH (heavy load that saturates the pool) -- and measures
    how much produce p99 degrades from one to the other. With the reservation
    floor, producer_upload keeps reserved connections, so produce p99 stays nearly
    flat across the stages (load-insensitive); without it (passthrough) produce
    competes unprotected and p99 climbs sharply. Measuring within an arm (same
    cluster, same backlog, only the reader count changes) makes each arm its own
    control and sidesteps cross-cluster variance -- the assertion is on its own
    stage2/stage1 ratio, since the two policies run as separate @matrix clusters
    and can't be compared in one method. Hard guards assert the run was
    meaningful: stage 2 saturated the pool (had waiters, reservation arm) and both
    stages read genuinely cold (pulled back more than the cache).

    Sustained pool saturation wedges a few partitions' reconcilers on orphaned
    multipart uploads (CORE-16648: the multipart path has no abort or timeout),
    so a graceful shutdown hangs forever. The arm's verdict is decided by the
    assertions regardless, so we SIGKILL the brokers at teardown rather than
    block on the wedge.

    Scope: a coarse CDT gate + A/B. The precise reservation-vs-passthrough
    latency A/B lives in the locked bench-runner tier-9 configs; the floor
    mechanism is unit-tested in cloud_io/tests/scheduler_test.cc.
    """

    topics = ()

    NUM_BROKERS = 9
    NUM_CLIENT_NODES = 3
    MSG_SIZE = 16 * 1024

    # High partition count so cold reads contend the pool. Capped by the
    # cluster's partition limit at runtime.
    MAX_PARTITIONS = 6000

    # Small enough to contend at ducktape scale, large enough to avoid the
    # pool=4 collapse (cascading cold_op_timeouts + a wedged reconciler). At >=6
    # the shipped 2+2+2 reservation fits (producer_upload floor = 2). The waiter
    # guard fails the reservation arm if 8 leaves too much headroom to contend.
    POOL_CONNECTIONS = 8

    # Keep the cloud cache small so the backlog can't fit; the looping readers
    # then keep missing it and fetch cold from object storage.
    CLOUD_CACHE_SIZE = 1 * 1024**3  # 1 GiB

    # Modest, constant produce: it builds fine and the floor's reserved
    # connections easily sustain it. We do NOT crank produce to force contention
    # -- high produce pool-binds the produce path itself (256 MiB/s timed out the
    # backlog build, build 86035). The contention comes from READ pressure
    # (N_LOW/N_HIGH): cold readers monopolize the pool, so producer_upload loses
    # its connection turnover under passthrough but keeps its floor under
    # reservation.
    PRODUCE_RATE_BPS = 20 * 1024**2  # 20 MiB/s
    BACKLOG_SEC = 3 * 60
    SAMPLE_INTERVAL_SEC = 30

    # Two cold-read stages per arm; the A/B is how much produce p99 degrades from
    # the low stage to the high stage. The reservation floor should keep it
    # nearly flat (load-insensitive); passthrough should degrade sharply. Stage 1
    # (N_LOW) is the low-load REFERENCE, deliberately below pool saturation, so
    # the passthrough stage2/stage1 ratio stays large -- the saturation that
    # drives the gap is stage 2 (N_HIGH). n_high=128 fits one client node; 256
    # would cap it (build 86068: 128 -> 2112 pool waiters, the client kept up).
    N_LOW = 16  # stage 1: low-load reference (unsaturated)
    N_HIGH = 128  # stage 2: heavy cold-read load (saturates the pool)

    # Per-stage duration, and the settle window skipped at each stage start
    # (consumer-group rebalance + ramp) before sampling steady-state produce p99.
    STAGE_SEC = 6 * 60
    STAGE_SETTLE_SEC = 2 * 60

    # Load-sensitivity gate on the stage2/stage1 median-p99 ratio, calibrated from
    # build 86072: reservation 0.78x (floor holds -- produce p99 is flat to better
    # under 8x the read load) vs passthrough 2.77x (no floor -- p99 ~triples).
    # Reservation must stay at/under R_LO; passthrough must clear R_HI (which also
    # confirms the high stage built real contention). Gated on the median
    # (robust); the worst-window ratios were 0.84x vs 7.75x.
    R_LO = 1.1  # reservation: produce p99 barely moves under load
    R_HI = 2.0  # passthrough: produce p99 degrades sharply

    # Stage 2 must actually saturate the pool for the ratio to mean anything. The
    # value is arbitrary -- 3-5x below the queue depths we observe (stage-2 peak
    # waiters ran ~1500-3400 across runs) -- and means only "the pool saturated",
    # not a tuned threshold.
    MIN_STAGE2_WAITERS = 500

    def __init__(self, test_context: TestContext):
        # Scheduler policy is parametrized (@matrix); read it here so the cluster
        # starts under the right policy (cloud_io_scheduler_policy is
        # restart-only, so it can't be flipped mid-test).
        self._scheduler_policy = (test_context.injected_args or {}).get(
            "scheduler_policy", "reservation"
        )
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=self.POOL_CONNECTIONS,
            cloud_storage_cache_size=self.CLOUD_CACHE_SIZE,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
            # End-of-test scrub reads every partition's manifest back from cloud
            # storage; at this partition count it overruns the runner timeout.
            # Orthogonal to what this test gates; skip it as other scale tests do.
            skip_end_of_test_scrubbing=True,
        )
        extra_rp_conf = {
            "enable_cluster_metadata_upload_loop": False,
            # The reservation arm reads an internal (vectorized_) metric, the
            # cloud_io scheduler waiter gauge, so internal metrics must be on.
            "disable_metrics": False,
            # Per @matrix arm: 'reservation' (the floor under test) vs
            # 'passthrough' (control -- no floor, scheduler bypassed).
            "cloud_io_scheduler_policy": self._scheduler_policy,
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
            "vectorized_cloud_io_scheduler_total_waiters",
            metrics_endpoint=MetricsEndpoint.METRICS,
            expect_metric=True,
        )

    @cluster(num_nodes=12)
    @skip_debug_mode
    @matrix(scheduler_policy=["reservation", "passthrough"])
    def test_produce_under_cold_reads(self, scheduler_policy: str):
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
            """One cold-read stage at n_readers. Samples produce p99 each
            interval for STAGE_SEC, discarding the settle window, and returns
            (p99 median, p99 worst, reads, peak waiters) over the steady-state
            samples. loop=True re-reads the cold backlog from offset 0 each loop;
            kgo-verifier auto-names a fresh random group per instance (and forbids
            loop + an explicit group name), so sequential stages get distinct
            groups for free. Single-process service, so exactly one node."""
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
                    if scheduler_policy == "reservation":
                        peak = max(peak, self._total_pool_waiters())
                    settled = elapsed >= self.STAGE_SETTLE_SEC
                    if settled:
                        p99s.append(p99_us)
                    self.logger.info(
                        f"[{scheduler_policy}] stage n={n_readers} t={elapsed:.0f}s "
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
            self.logger.info(
                f"Building backlog: ~{backlog_target} msgs "
                f"(~{rate * self.BACKLOG_SEC / 1024**3:.0f} GiB) vs "
                f"{self.CLOUD_CACHE_SIZE / 1024**3:.0f} GiB cache, at {rate} B/s"
            )
            producer.wait_for_acks(
                count=backlog_target, timeout_sec=self.BACKLOG_SEC * 2, backoff_sec=5
            )

            # ── Two cold-read stages: low-load reference, then heavy load ──
            # The A/B is how much produce p99 degrades from the low stage to the
            # high stage. kgo-verifier reports produce ack latency (produce call
            # -> ack) in microseconds over a rolling window. Capture the
            # uncontended baseline before any readers, for context.
            baseline_p99_us = producer.produce_status.latency["p99"]
            self.logger.info(
                f"[{scheduler_policy}] baseline produce p99 = "
                f"{baseline_p99_us / 1000:.0f} ms; stage 1 (low load) n={self.N_LOW}"
            )
            s1_median, s1_worst, s1_reads, _ = run_stage(self.N_LOW)
            self.logger.info(
                f"[{scheduler_policy}] stage 2 (heavy load) n={self.N_HIGH}"
            )
            s2_median, s2_worst, s2_reads, s2_waiters = run_stage(self.N_HIGH)
        finally:
            producer.stop()

        ratio_median = s2_median / s1_median if s1_median else 0.0
        ratio_worst = s2_worst / s1_worst if s1_worst else 0.0
        s1_read_bytes = s1_reads * self.MSG_SIZE
        s2_read_bytes = s2_reads * self.MSG_SIZE
        self.logger.info(
            f"[{scheduler_policy}] produce p99 vs cold-read load: "
            f"n={self.N_LOW} median/worst={s1_median / 1000:.0f}/{s1_worst / 1000:.0f}ms, "
            f"n={self.N_HIGH} median/worst={s2_median / 1000:.0f}/{s2_worst / 1000:.0f}ms; "
            f"load-sensitivity ratio (stage2/stage1) median={ratio_median:.2f}x "
            f"worst={ratio_worst:.2f}x (baseline p99={baseline_p99_us / 1000:.0f}ms); "
            f"stage reads ~{s1_read_bytes / 1024**3:.0f}/{s2_read_bytes / 1024**3:.0f} GiB; "
            f"stage2 pool_waiters_peak={s2_waiters:.0f}"
        )

        try:
            # Regime guard (both arms): both stages must have read genuinely cold
            # (pulled back more than the cache), else the comparison is moot.
            assert (
                s1_read_bytes > self.CLOUD_CACHE_SIZE
                and s2_read_bytes > self.CLOUD_CACHE_SIZE
            ), (
                f"reads weren't cold: stage1 {s1_read_bytes} B, stage2 "
                f"{s2_read_bytes} B vs {self.CLOUD_CACHE_SIZE} B cache"
            )

            if scheduler_policy == "reservation":
                # The high stage must have saturated the pool, else the ratio is
                # trivial (produce is fast when nothing competes).
                assert s2_waiters >= self.MIN_STAGE2_WAITERS, (
                    f"the high stage only reached {s2_waiters:.0f} pool waiters "
                    f"(expected >= {self.MIN_STAGE2_WAITERS}) -- it didn't saturate "
                    f"the pool, so the latency ratio is trivial. Raise N_HIGH or "
                    f"MAX_PARTITIONS."
                )
                # The floor must keep produce p99 nearly flat as read load scales
                # 8x (n=16 -> n=128) -- the A/B's whole point.
                assert ratio_median <= self.R_LO, (
                    f"reservation floor failed to protect produce: p99 inflated "
                    f"{ratio_median:.2f}x from n={self.N_LOW} to n={self.N_HIGH} "
                    f"under cold-read load (limit {self.R_LO}x)"
                )
            else:
                # Passthrough has no floor: produce p99 must degrade sharply under
                # the same load step -- which also confirms the high stage built
                # real contention (else the comparison would be meaningless).
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
            self.logger.info(f"Cluster healthy -- {scheduler_policy} arm passed")
        finally:
            # Sustained pool saturation wedges a few partitions' reconcilers on
            # orphaned multipart uploads (CORE-16648: no abort/timeout on the
            # multipart path), which hangs a graceful shutdown forever. The arm's
            # verdict is decided above, so SIGKILL the brokers to keep teardown
            # bounded rather than block on the wedge.
            #
            # HACK: when CORE-16648 is fixed, drop this force-stop and let the
            # normal graceful teardown run -- a clean shutdown then becomes a
            # real extra check instead of something we dodge.
            self.logger.info(
                "Force-stopping brokers to avoid the reconciler-wedge shutdown "
                "hang (CORE-16648)"
            )
            for node in self.redpanda.nodes:
                self.redpanda.stop_node(node, forced=True)
