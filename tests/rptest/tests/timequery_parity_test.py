# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
5-way timequery parity test.

Kafka's ListOffsets-by-timestamp contract is "the earliest offset whose
record timestamp is >= the requested timestamp". Redpanda answers that
query through a different index implementation per storage mode (raft-local
time index, classic tiered-storage remote index, tiered_v2/cloud L1 object
footer index), each of which has its own way of getting the "first record
at-or-after" vs "running max timestamp" distinction wrong.

This test produces a byte-identical workload - including non-monotonic
timestamp phases where the correct answer is subtle and Apache Kafka's
behavior is the spec - to one topic per Redpanda storage mode (local,
tiered_v1, tiered_v2, cloud) and to an Apache Kafka cluster, then sweeps
timequeries and asserts that all five topics agree, and that Kafka agrees
with the analytically computed answer. Sweeps run at three lifecycle
points: while all data is local, after data has moved to remote/L1
storage, and after a full Redpanda restart.

Retention is infinite everywhere (only *local* trimming is enabled on the
tiered topics, which does not delete data) so answers cannot shift under
the test's feet.
"""

import concurrent.futures
import random
import time
from collections.abc import Callable
from typing import cast

from confluent_kafka import KafkaError, Message, Producer
from ducktape.tests.test import Test, TestContext
from kafkatest.services.kafka import KafkaService  # type: ignore[import-untyped]
from kafkatest.services.zookeeper import (  # type: ignore[import-untyped]
    ZookeeperService,
)
from kafkatest.version import V_3_0_0  # type: ignore[import-untyped]

from rptest.clients.admin.v2 import Admin as AdminV2, metastore_pb, ntp_pb
from rptest.clients.default import DefaultClient
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kafka import KafkaServiceAdapter
from rptest.services.redpanda import SISettings, make_redpanda_service
from rptest.services.redpanda_types import RedpandaServiceForClients
from rptest.util import wait_for_local_storage_truncate, wait_until


class TimeQueryParityTest(Test):
    """
    Differential timequery test: Apache Kafka as the spec oracle against
    Redpanda's four storage modes, all fed the identical record/timestamp
    sequence.
    """

    record_size = 1024
    segment_size = 1024 * 1024
    local_target_bytes = 4 * segment_size
    ct_object_size = 1024 * 1024
    # Small enough that L1 objects carry several index entries with
    # multiple unindexed batches between consecutive entries; this is the
    # regime in which running-max-vs-first-match seek bugs are visible.
    ct_indexing_interval = 64 * 1024
    # Producing in fixed-size flushed batches keeps batch composition (and
    # therefore each batch's max_timestamp) identical across all topics.
    records_per_flush = 16

    kafka_topic = "tq-parity-kafka"
    rp_topics = {
        "tq-parity-local": TopicSpec.STORAGE_MODE_LOCAL,
        "tq-parity-tiered-v1": TopicSpec.STORAGE_MODE_IMPL_TIERED_V1,
        "tq-parity-tiered-v2": TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
        "tq-parity-cloud": TopicSpec.STORAGE_MODE_CLOUD,
    }
    tiered_v1_topic = "tq-parity-tiered-v1"
    tiered_v2_topic = "tq-parity-tiered-v2"
    cloud_topic = "tq-parity-cloud"

    def __init__(self, test_context: TestContext):
        super().__init__(test_context=test_context)

        self.zk = ZookeeperService(test_context, num_nodes=1, version=V_3_0_0)
        self.kafka = KafkaServiceAdapter(
            test_context,
            KafkaService(test_context, num_nodes=1, zk=self.zk, version=V_3_0_0),
        )

        extra_rp_conf = {
            "enable_cluster_metadata_upload_loop": False,
            "enable_leader_balancer": False,
            "disable_batch_cache": True,
            "log_retention_ms": -1,
            "log_segment_size": self.segment_size,
            "log_segment_size_min": self.segment_size,
            # Fast L0 -> L1 movement for the tiered_v2/cloud topics.
            "cloud_topics_long_term_flush_interval": 1000,
            "cloud_topics_reconciliation_min_interval": 1000,
            "cloud_topics_reconciliation_max_interval": 2000,
            "cloud_topics_reconciliation_max_object_size": self.ct_object_size,
            "cloud_topics_indexing_interval": self.ct_indexing_interval,
            # Engage strict local retention so the tiered_v2 topic actually
            # trims its local log to retention.local.target.bytes and reads
            # below the local start pivot to L1.
            "retention_local_strict": True,
            "retention_local_strict_override": True,
            "retention_local_trim_interval": 2000,
            "cloud_storage_housekeeping_interval_ms": 2000,
            "log_compaction_interval_ms": 2000,
        }
        self.si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )
        self.si_settings.load_context(self.logger, test_context)
        self.redpanda = make_redpanda_service(
            context=test_context,
            num_brokers=1,
            si_settings=self.si_settings,
            extra_rp_conf=extra_rp_conf,
        )

        self.base_ts = int(time.time() - 600) * 1000
        self.timestamps, self.phases = self._generate_workload()
        self.msg_count = len(self.timestamps)
        self.queries = self._generate_queries()
        self.expected = self._expected_offsets()

    def setUp(self):
        self.zk.start()
        self.kafka.start()
        self.redpanda.start()
        self.redpanda.set_feature_active("tiered_cloud_topics", True, timeout_sec=30)

    def tearDown(self):
        # ducktape handles service teardown automatically, but stopping
        # Kafka and zookeeper explicitly with logging makes hangs easier
        # to attribute.
        self.logger.info("Stopping Kafka...")
        self.kafka.stop()
        self.logger.info("Stopping zookeeper...")
        self.zk.stop()

    # ------------------------------------------------------------------
    # Workload
    # ------------------------------------------------------------------

    def _generate_workload(self) -> tuple[list[int], dict[str, tuple[int, int]]]:
        """
        Build the deterministic timestamp sequence, one entry per record.
        Returns the sequence plus a phase name -> [start, end) offset-range
        map used to spread query sampling across every timestamp regime.
        """
        rng = random.Random(0x5EED)
        timestamps: list[int] = []
        phases: dict[str, tuple[int, int]] = {}

        def phase(name: str, count: int, gen: Callable[[], int]) -> None:
            start = len(timestamps)
            for _ in range(count):
                timestamps.append(gen())
            phases[name] = (start, len(timestamps))

        # Monotonic ramp: the baseline well-behaved regime.
        cur = self.base_ts

        def ramp() -> int:
            nonlocal cur
            cur += 1
            return cur

        phase("ramp", 4096, ramp)

        # Duplicate run: thousands of records sharing one timestamp; the
        # answer for that timestamp is the *first* of them.
        plateau_ts = cur
        phase("plateau", 512, lambda: plateau_ts)

        # Local jitter: timestamps trend upward but are out of order
        # within and across batches, so a batch's max_timestamp routinely
        # exceeds timestamps of records that follow it.
        def jitter() -> int:
            nonlocal cur
            cur += 3
            return cur + rng.randint(-10, 10)

        phase("jitter", 4096, jitter)

        # Spike: a single record far in the future. Queries between the
        # pre-spike max and the spike must answer with the spike's offset,
        # not with a later backfill record that first re-crosses them.
        spike_ts = max(timestamps) + 60_000
        phase("spike", 1, lambda: spike_ts)

        # Backfill: a long stretch entirely below the running max (the
        # spike), rising back past the pre-spike level. This is the
        # regime where seeking to "first index entry whose running max
        # reaches the target" instead of its predecessor gives wrong
        # answers.
        back = max(t for t in timestamps if t != spike_ts) - 100

        def backfill() -> int:
            nonlocal back
            back += 1
            return back

        phase("backfill", 2048, backfill)

        # Recover: monotonic again from above everything, so the log ends
        # with a clean global maximum and queries past it answer -1.
        cur = spike_ts
        phase("recover", 1536, ramp)

        return timestamps, phases

    def _generate_queries(self) -> list[int]:
        rng = random.Random(0xFACE)
        queries: set[int] = set()
        for start, end in self.phases.values():
            for _ in range(8):
                t = self.timestamps[rng.randrange(start, end)]
                queries.update((t - 1, t, t + 1))
        lo, hi = min(self.timestamps), max(self.timestamps)
        queries.update((lo - 1000, lo, hi, hi + 1, hi + 10_000))
        return sorted(queries)

    def _expected_offsets(self) -> dict[int, int]:
        """
        The ListOffsets contract: earliest offset with ts >= query, for
        every query timestamp. The answer is non-decreasing in the query
        timestamp, so walking the sorted queries alongside a single
        forward scan of the log resolves all of them in one pass.
        """
        expected: dict[int, int] = {}
        i = 0
        for query_ts in self.queries:
            while i < len(self.timestamps) and self.timestamps[i] < query_ts:
                i += 1
            expected[query_ts] = i if i < len(self.timestamps) else -1
        return expected

    # ------------------------------------------------------------------
    # Setup helpers
    # ------------------------------------------------------------------

    def _create_topics(self):
        rpk = RpkTool(self.redpanda)
        common = {
            "message.timestamp.type": "CreateTime",
            "retention.ms": "-1",
            "segment.bytes": str(self.segment_size),
        }
        # Local trimming starts effectively disabled (target far above the
        # produced data) so the first sweep genuinely runs against local
        # storage on every topic; _engage_local_trim() lowers the target
        # before the remote sweep.
        local_trim = {
            "retention.local.target.bytes": str(1024 * self.segment_size),
            "retention.local.target.ms": "-1",
        }
        for name, mode in self.rp_topics.items():
            config = {**TopicSpec.storage_mode_config(mode), **common}
            if mode in (
                TopicSpec.STORAGE_MODE_IMPL_TIERED_V1,
                TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
            ):
                config.update(local_trim)
            rpk.create_topic(topic=name, partitions=1, replicas=1, config=config)

        kafka_client = DefaultClient(self.kafka)
        kafka_client.create_topic(
            TopicSpec(name=self.kafka_topic, partition_count=1, replication_factor=1)
        )
        for k, v in common.items():
            kafka_client.alter_topic_config(self.kafka_topic, k, v)

    def _produce_all(self):
        def make_producer(brokers: str) -> Producer:
            return Producer(
                {
                    "bootstrap.servers": brokers,
                    # Idempotence pins retries to in-order delivery, so
                    # record i lands at offset i on every topic.
                    "enable.idempotence": True,
                    "linger.ms": 50,
                }
            )

        rp_producer = make_producer(self.redpanda.brokers())
        kafka_producer = make_producer(cast(str, self.kafka.brokers()))

        errors: list[str] = []

        def on_delivery(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                errors.append(f"{msg.topic()}: {err}")

        for i, ts in enumerate(self.timestamps):
            value = f"{i:016d}".encode().ljust(self.record_size, b"x")
            for topic in self.rp_topics:
                rp_producer.produce(
                    topic,
                    value=value,
                    partition=0,
                    timestamp=ts,
                    on_delivery=on_delivery,
                )
            kafka_producer.produce(
                self.kafka_topic,
                value=value,
                partition=0,
                timestamp=ts,
                on_delivery=on_delivery,
            )
            if (i + 1) % self.records_per_flush == 0:
                rp_producer.flush()
                kafka_producer.flush()
        rp_producer.flush()
        kafka_producer.flush()
        assert not errors, f"produce failures: {errors[:10]}"

    def _all_topics(self) -> list[tuple[str, KafkaCat]]:
        rp_kcat = KafkaCat(self.redpanda)
        kafka_kcat = KafkaCat(self.kafka)
        topics: list[tuple[str, KafkaCat]] = [
            (name, rp_kcat) for name in self.rp_topics
        ]
        topics.append((self.kafka_topic, kafka_kcat))
        return topics

    def _verify_hwm(self, retry_timeout_sec: int = 0):
        def hwm_of(svc: RedpandaServiceForClients, topic: str) -> int:
            partition = next(RpkTool(svc).describe_topic(topic))
            return partition.high_watermark

        def all_match() -> bool:
            hwms = {t: hwm_of(self.redpanda, t) for t in self.rp_topics}
            hwms[self.kafka_topic] = hwm_of(self.kafka, self.kafka_topic)
            self.logger.info(f"high watermarks: {hwms} (want {self.msg_count})")
            return all(h == self.msg_count for h in hwms.values())

        if retry_timeout_sec > 0:
            wait_until(
                all_match,
                timeout_sec=retry_timeout_sec,
                backoff_sec=3,
                err_msg="high watermarks did not converge",
                retry_on_exc=True,
            )
        else:
            assert all_match(), "unexpected high watermark on some topic"

    # ------------------------------------------------------------------
    # Lifecycle waits
    # ------------------------------------------------------------------

    def _wait_reconciled(self, topic: str):
        """Wait until the L1 metastore covers the whole partition."""
        metastore = AdminV2(self.redpanda).metastore()

        def is_reconciled() -> bool:
            req = metastore_pb.GetOffsetsRequest(
                partition=ntp_pb.TopicPartition(topic=topic, partition=0)
            )
            next_offset = metastore.get_offsets(req=req).offsets.next_offset
            self.logger.info(
                f"{topic} metastore next_offset={next_offset}/{self.msg_count}"
            )
            return next_offset >= self.msg_count

        wait_until(
            is_reconciled,
            timeout_sec=120,
            backoff_sec=5,
            err_msg=f"{topic} not reconciled to metastore",
            retry_on_exc=True,
        )

    # ------------------------------------------------------------------
    # The sweep
    # ------------------------------------------------------------------

    def _sweep(self, label: str):
        """
        Run every query timestamp against all five topics and require
        (a) 5-way agreement and (b) Kafka matching the analytic answer.
        """
        topics = self._all_topics()
        self.logger.info(
            f"sweep '{label}': {len(self.queries)} query timestamps "
            f"across {len(topics)} topics"
        )

        results: dict[int, dict[str, int]] = {q: {} for q in self.queries}
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            futures = {
                pool.submit(kcat.query_offset, topic, 0, q): (q, topic)
                for q in self.queries
                for topic, kcat in topics
            }
            for future in concurrent.futures.as_completed(futures):
                q, topic = futures[future]
                results[q][topic] = future.result()

        mismatches: list[str] = []
        for q in self.queries:
            answers = results[q]
            expected = self.expected[q]
            if len(set(answers.values())) != 1 or answers[self.kafka_topic] != expected:
                mismatches.append(
                    f"ts={q} expected={expected} "
                    + " ".join(f"{t}={o}" for t, o in sorted(answers.items()))
                )

        assert not mismatches, (
            f"sweep '{label}': {len(mismatches)} timequery mismatches "
            f"(expected = analytic first-offset-with-ts>=query):\n"
            + "\n".join(mismatches[:20])
        )
        self.logger.info(f"sweep '{label}': all {len(self.queries)} queries agree")

    # ------------------------------------------------------------------
    # Test
    # ------------------------------------------------------------------

    @cluster(num_nodes=3)
    def test_timequery_parity(self):
        self._create_topics()
        self._produce_all()
        self._verify_hwm()

        # Sweep 1: every topic still serves entirely from local storage.
        self._sweep("all-local")

        # Move data to remote: lower the tiered topics' local retention
        # target so classic tiered storage trims its local log after
        # upload; tiered_v2/cloud reconcile into L1 objects and the
        # tiered_v2 topic additionally trims its local log so reads pivot
        # to L1.
        rpk = RpkTool(self.redpanda)
        for topic in (self.tiered_v1_topic, self.tiered_v2_topic):
            rpk.alter_topic_config(
                topic, "retention.local.target.bytes", str(self.local_target_bytes)
            )
        self._wait_reconciled(self.tiered_v2_topic)
        self._wait_reconciled(self.cloud_topic)
        for topic in (self.tiered_v1_topic, self.tiered_v2_topic):
            wait_for_local_storage_truncate(
                redpanda=self.redpanda,
                topic=topic,
                target_bytes=self.local_target_bytes,
                timeout_sec=180,
            )

        # Sweep 2: early timestamps now resolve via remote indexes.
        self._sweep("remote")

        # Sweep 3: cold caches and freshly recovered in-memory state.
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._verify_hwm(retry_timeout_sec=60)
        self._sweep("post-restart")
