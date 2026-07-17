# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from contextlib import contextmanager, nullcontext
import re
import time
import socket
import random
from typing import Any, Callable, Optional

import google.protobuf.duration_pb2
import google.protobuf.field_mask_pb2
from ducktape.cluster.cluster import ClusterNode
from ducktape.services.service import Service
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.internal.shadow_link_internal.v1 import (
    shadow_link_internal_pb2,
    shadow_link_internal_pb2_connect,
)
from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    shadow_link_pb2,
    shadow_link_pb2_connect,
)
from rptest.clients.admin.proto.redpanda.core.common.v1 import acl_pb2
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import TestContext
from rptest.services.kgo_verifier_services import (
    KgoVerifierConsumerGroupConsumer,
    KgoVerifierProducer,
)
from rptest.services.multi_cluster_services import (
    Cluster,
    MultiClusterServices,
    RedpandaCluster,
    RedpandaService,
    SecondaryClusterArgs,
    ServiceType,
    SecondaryClusterSpec,
)
from rptest.services.redpanda import (
    LoggingConfig,
    SISettings,
    TLSProvider,
)
from rptest.services.tls import CertificateAuthority, Certificate, TLSCertManager
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.util import bg_thread_cm, wait_until_result
from rptest.utils.node_operations import FailureInjectorBackgroundThread
import threading
from logging import Logger
from threading import Lock
from urllib3.exceptions import ProtocolError


SOURCE_CLUSTER_SPEC = "source_cluster_spec"


DEFAULT_SOURCE_CLUSTER_SPEC = SecondaryClusterSpec(ServiceType.REDPANDA)


# Topic properties that are always synced
REQUIRED_SYNCED_TOPIC_PROPERTIES = [
    "max.message.bytes",
    "cleanup.policy",
    "message.timestamp.type",
]

# Topic properties that are synced by default
DEFAULT_SYNCED_TOPIC_PROPERTIES = [
    "compression.type",
    "retention.bytes",
    "retention.ms",
    "replication.factor",
    "delete.retention.ms",
    "max.compaction.lag.ms",
    "min.compaction.lag.ms",
    "redpanda.storage.mode",
    "redpanda.storage.mode.impl",
]

DISALLOWED_SYNCED_TOPIC_PROPERTIES = [
    "redpanda.remote.readreplica",
    "redpanda.remote.recovery",
    "redpanda.remote.allowgaps",
    "redpanda.virtual.cluster.id",
    "redpanda.leaders.preference",
]

CONTROLLER_LOCKED_TASKS = [
    "Source Topic Sync",
    "Security Migrator Task",
    "Roles Migrator Task",
]

# Matches mirroring_task::task_name in
# src/v/cluster_link/schema_registry_sync/mirroring_task.h
SCHEMA_REGISTRY_SYNC_TASK_NAME = "Schema Registry Shadowing"

ALL_STORAGE_MODES = [
    TopicSpec.STORAGE_MODE_LOCAL,
    TopicSpec.STORAGE_MODE_IMPL_TIERED_V1,
    TopicSpec.STORAGE_MODE_CLOUD,
    TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
]

# Log messages that are expected when running shadow link tests with
# cloud / tiered_cloud storage modes.
CLOUD_TOPICS_SHADOW_LINK_LOG_ALLOW_LIST = [
    # Cloud-topics subsystem may not be initialized immediately after a
    # node restart; the replicator retries until it becomes available.
    re.compile(r".*cloud-topics subsystem is not initialized"),
    # The cloud-topics STM may time out during epoch fencing under load
    # or immediately after leadership changes.
    re.compile(r".*ctp_stm\.cc.*Sync timeout"),
]


class StorageModeFlipper:
    """Background thread that periodically rotates `redpanda.storage.mode` of
    a topic between a list of modes. Useful for stress-testing transitions
    between cloud / tiered_cloud / disk storage modes while a workload is
    running. Transient errors during the alter call (e.g. leadership changes
    or partition movement) are logged and retried on the next tick.
    """

    def __init__(
        self,
        rpk: RpkTool,
        topic: str,
        modes: list[str],
        interval_seconds: float,
        logger: Logger,
    ):
        self._rpk = rpk
        self._topic = topic
        self._modes = modes
        self._interval = interval_seconds
        self._logger = logger
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if not self._modes:
            return
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"flipper-{self._topic}",
        )
        self._thread.start()

    def stop(self, timeout: float = 30) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def _run(self) -> None:
        idx = 0
        while not self._stop.wait(self._interval):
            mode = self._modes[idx % len(self._modes)]
            idx += 1
            try:
                self._rpk.alter_topic_config(self._topic, "redpanda.storage.mode", mode)
                self._logger.debug(f"Flipped storage mode of {self._topic} to {mode}")
            except Exception as e:
                self._logger.warning(
                    f"Failed to flip storage mode of {self._topic} to {mode}: {e}"
                )


class ClusterLinkingTLSProvider(TLSProvider):
    def __init__(self, tls: TLSCertManager):
        self.tls: TLSCertManager = tls

    @property
    def ca(self) -> CertificateAuthority:
        return self.tls.ca

    def create_broker_cert(self, service: Service, node: ClusterNode) -> Certificate:
        assert node in service.nodes
        return self.tls.create_cert(node.name, common_name=node.name)

    def create_service_client_cert(self, service: Service, name: str) -> Certificate:
        return self.tls.create_cert(socket.gethostname(), name=name, common_name=name)


class ClusterLinkingProgressVerifier:
    instance_lock = Lock()
    instance_count = 0

    def __init__(
        self,
        test_context,
        source_cluster: Cluster,
        target_cluster: RedpandaCluster,
        topic: str,
        preallocated_nodes: list,
        logger,
        use_transactions: bool = False,
        use_compaction: bool = False,
        msg_count: int = 40000,
        msg_size: int = 4 * 1024,
        timeout_sec: int = 600,
        producer_properties: dict[str, Any] | None = None,
        consumer_properties: dict[str, Any] | None = None,
        validate_number_of_messages_on_target: bool = True,
    ):
        self.test_context = test_context
        self.source_cluster = source_cluster
        self.target_cluster = target_cluster

        self.topic = topic
        self.source_rpk = RpkTool(self.source_cluster.service)
        self.target_rpk = RpkTool(self.target_cluster.service)
        self.preallocated_nodes = preallocated_nodes
        self.logger = logger
        self.use_transactions = use_transactions
        self.use_compaction = use_compaction

        self.msg_count = msg_count
        self.msg_size = msg_size
        self.producer_properties: dict[str, Any] = (
            producer_properties if producer_properties else {}
        )
        self.consumer_properties: dict[str, Any] = (
            consumer_properties if consumer_properties else {}
        )
        # When using compaction, the completion criteria examines per-partition
        # offsets, which may be at odds with having a max_msgs set.
        assert not (self.use_compaction and "max_msgs" in self.consumer_properties), (
            "max_msgs is incompatible with use_compaction: completion requires "
            "per-partition offset parity, which a bounded read may never reach. "
            "Let the consumer tail (continuous) instead."
        )
        self.timeout_sec = timeout_sec
        self.validate_number_of_messages_on_target = (
            validate_number_of_messages_on_target
        )
        self._instance_id = ClusterLinkingProgressVerifier.instance_id()

    @staticmethod
    def instance_id() -> int:
        with ClusterLinkingProgressVerifier.instance_lock:
            id = ClusterLinkingProgressVerifier.instance_count
            ClusterLinkingProgressVerifier.instance_count += 1
            return id

    def start(self):
        self.producer = KgoVerifierProducer(
            context=self.test_context,
            redpanda=self.source_cluster.service,
            topic=self.topic,
            msg_size=self.msg_size,
            msg_count=self.msg_count,
            use_transactions=self.use_transactions,
            custom_node=self.preallocated_nodes,
            **self.producer_properties,
        )
        self.producer.start(clean=True)
        self.producer.wait_for_acks(10, 40, 1)
        self.producer.wait_for_offset_map()
        readers = 8

        self.source_consumer = KgoVerifierConsumerGroupConsumer(
            context=self.test_context,
            redpanda=self.source_cluster.service,
            topic=self.topic,
            msg_size=self.msg_size,
            readers=readers,
            use_transactions=self.use_transactions,
            group_name=f"source-cg-{self._instance_id}",
            nodes=self.preallocated_nodes,
            continuous=True,
            **self.consumer_properties,
        )
        self.source_consumer.start(clean=False)

        # NOTE: when using compaction, the completion criteria examines
        # per-partition offsets, which is at odds with having a max_msgs.
        self.target_consumer = KgoVerifierConsumerGroupConsumer(
            context=self.test_context,
            redpanda=self.target_cluster.service,
            topic=self.topic,
            msg_size=self.msg_size,
            max_msgs=None if self.use_compaction else self.msg_count,
            readers=readers,
            use_transactions=self.use_transactions,
            group_name=f"target-cg-{self._instance_id}",
            nodes=self.preallocated_nodes,
            continuous=True,
            **self.consumer_properties,
        )

        self.target_consumer.start(clean=False)

    def producer_finished(self):
        return self.producer.produce_status.acked >= self.msg_count

    def expected_read_messages(self):
        return (
            self.producer.produce_status.acked
            - self.producer.produce_status.aborted_transaction_messages
        )

    def max_offsets_match(
        self, consumer: KgoVerifierConsumerGroupConsumer, producer: KgoVerifierProducer
    ) -> bool:
        return (
            consumer.consumer_status.validator.max_offsets_consumed
            == producer.produce_status.max_offsets_produced
        )

    def source_consumer_finished(self):
        if not self.producer_finished():
            return False
        elif self.use_compaction:
            return self.max_offsets_match(self.source_consumer, self.producer)
        else:
            return (
                self.source_consumer.consumer_status.validator.total_reads
                >= self.expected_read_messages()
            )

    def target_consumer_finished(self):
        if not self.producer_finished():
            return False
        elif not self.validate_number_of_messages_on_target:
            return True
        elif self.use_compaction:
            return self.max_offsets_match(self.target_consumer, self.producer)
        else:
            return (
                self.target_consumer.consumer_status.validator.total_reads
                >= self.expected_read_messages()
            )

    def workload_finished(self):
        return (
            self.producer_finished()
            and self.source_consumer_finished()
            and self.target_consumer_finished()
        )

    def check_topic_hwms(self, timeout: int = 120, debug_only: bool = False):
        """Verify the target topic's high watermarks have caught up to the source.

        Describes the topic on both clusters and compares the per-partition high
        watermark; any partition whose watermarks differ is counted as lagging.
        With debug_only=True discrepancies are only logged (used to dump
        diagnostics when the workload stalls); otherwise a lagging or mismatched
        partition fails verification.

        describe_topics() retries until source and target agree on the partition
        set and every partition has a readable high watermark on both sides, so
        the comparison never races against transient leadership churn. See
        CORE-16414 for the failure this guards against.
        """

        def describe_topics():
            last_log = None

            def log_once(message):
                # Throttle to distinct states: a persistent condition leaves one
                # line per transition rather than spamming on every backoff.
                nonlocal last_log
                if message != last_log:
                    last_log = message
                    self.logger.warning(message)

            def describe_once():
                # tolerant=True keeps momentarily-leaderless partitions in the
                # result (with high_watermark=None) instead of dropping them,
                # which separates two concerns the default describe conflates:
                #
                #   1. Structural: the set of partition ids is fixed for a
                #      provisioned topic and is reported in metadata regardless
                #      of leadership. Source and target must agree on it; a
                #      persistent mismatch is a real (shadow-link) bug, surfaced
                #      via the timeout below.
                #   2. Transient: an individual partition may briefly have no
                #      leader (e.g. during failure injection) and thus no
                #      readable high watermark. We wait for liveness rather than
                #      silently skipping the partition.
                #
                # Logging both (throttled) keeps the signal alive even when a
                # transient condition is waited out and the test passes.
                #
                # Target is described before source so that, for a partition
                # still replicating, target_hw <= source_hw and the lag computed
                # below stays non-negative.
                target = {
                    p.id: p
                    for p in self.target_rpk.describe_topic(self.topic, tolerant=True)
                }
                source = {
                    p.id: p
                    for p in self.source_rpk.describe_topic(self.topic, tolerant=True)
                }

                if not source or source.keys() != target.keys():
                    log_once(
                        f"Partition set mismatch (structural) for {self.topic} "
                        f"while computing lag: source={sorted(source.keys())} "
                        f"target={sorted(target.keys())}; retrying"
                    )
                    return False, None

                not_ready = [
                    pid
                    for pid in sorted(source.keys())
                    if source[pid].high_watermark is None
                    or target[pid].high_watermark is None
                ]
                if not_ready:
                    log_once(
                        f"Partitions without a readable high watermark for "
                        f"{self.topic}: {not_ready}; retrying"
                    )
                    return False, None

                return True, (target, source)

            return wait_until_result(
                describe_once,
                timeout_sec=timeout,
                backoff_sec=0.5,
                err_msg=f"Source and target did not converge on a comparable set of partitions for lag calculation in {timeout} seconds",
            )

        try:
            (target, source) = describe_topics()
            # Postcondition of describe_topics(): the partition sets match and
            # every high watermark is non-None on both sides, so partitions line
            # up by id and the subtraction below cannot see None. The assert
            # documents (and defensively re-checks) that invariant.
            assert source.keys() == target.keys(), (
                "Verification failed, Topic partitions mismatch between source and target"
            )
            partitions_with_lag = 0
            for pid in sorted(source.keys()):
                source_partition = source[pid]
                target_partition = target[pid]
                if target_partition.high_watermark != source_partition.high_watermark:
                    lag = (
                        source_partition.high_watermark
                        - target_partition.high_watermark
                    )
                    self.logger.debug(
                        f"Partition {self.topic}/{pid} - source: ({source_partition}), target: ({target_partition}) lag: {lag}"
                    )
                    partitions_with_lag += 1
            assert debug_only or partitions_with_lag == 0, (
                f"Verification failed, {partitions_with_lag} partitions do not have synced high watermarks"
            )
        except Exception as e:
            # debug_only callers (e.g. the workload-stall diagnostic dump) want a
            # best-effort snapshot, so swallow failures; real verification
            # propagates them.
            self.logger.warning(f"Verification failed: {e}")
            if not debug_only:
                raise

    def stop_kgo_services(self):
        self.source_consumer.stop()
        self.target_consumer.stop()
        self.producer.stop()

    def _raise_if_worker_crashed(self):
        """Fail fast if any kgo-verifier worker's status thread has errored.

        A worker whose process dies (e.g. a client-library crash such as the
        franz-go produce-path panic) stops answering status polls and can never
        recover. Its StatusThread records the failure -- naming the worker that
        exited -- and raise_on_error() re-raises it here, so validate_progress
        surfaces that descriptive error immediately instead of waiting out
        progress_timeout and reporting an opaque "Workload stalled".
        """
        for svc in (self.producer, self.source_consumer, self.target_consumer):
            if svc._status_thread is not None:
                svc._status_thread.raise_on_error()

    def validate_progress(self, progress_timeout=60, backoff_delay=5):
        workload_last_progress = time.time()
        source_consumer_last_reads = 0
        target_consumer_last_reads = 0
        producer_last_acked = 0

        while not self.workload_finished():
            now = time.time()
            producer_acked = self.producer.produce_status.acked
            source_reads = self.source_consumer.consumer_status.validator.total_reads
            target_reads = self.target_consumer.consumer_status.validator.total_reads

            # A crashed kgo-verifier worker can never make progress, so fail
            # fast with the descriptive error its status thread recorded rather
            # than blaming a "Workload stalled" after progress_timeout elapses.
            self._raise_if_worker_crashed()

            # track workload progress
            if (
                source_reads > source_consumer_last_reads
                or target_reads > target_consumer_last_reads
                or producer_acked > producer_last_acked
            ):
                workload_last_progress = now
                source_consumer_last_reads = source_reads
                target_consumer_last_reads = target_reads
                producer_last_acked = producer_acked

            if now - workload_last_progress > progress_timeout:
                self.logger.error(
                    f"No workload progress for {progress_timeout}s, source reads: {source_reads} (last: {source_consumer_last_reads}), target reads: {target_reads} (last: {target_consumer_last_reads}), producer acks: {producer_acked} (last: {producer_last_acked})"
                )
                self.check_topic_hwms(debug_only=True)
                raise Exception("Workload stalled")

            if not self.workload_finished():
                time.sleep(backoff_delay)

        self.check_topic_hwms()

    def consumer_groups_state_consistent(self):
        source_groups = self.source_rpk.group_list()
        self.logger.debug(f"Source consumer groups: {source_groups}")
        for g in source_groups:
            source_d = self.source_rpk.group_describe(g.group)
            target_d = self.target_rpk.group_describe(g.group)
            target_partitions = {(p.topic, p.partition): p for p in target_d.partitions}
            errors = []
            for p in source_d.partitions:
                key = (p.topic, p.partition)
                if key not in target_partitions:
                    self.logger.error(
                        f"Group {g.group} partition {key} offset commit not found in target"
                    )
                    errors.append((key, "missing in target"))
                    continue

                if p.current_offset != target_partitions[key].current_offset:
                    self.logger.debug(
                        f"Partition {key} offset mismatch: {p.current_offset} != {target_partitions[key].current_offset}"
                    )
                    errors.append(
                        (
                            key,
                            "offset mismatch current: {} target: {}".format(
                                p.current_offset, target_partitions[key].current_offset
                            ),
                        )
                    )
        if len(errors) > 0:
            for e in errors:
                self.logger.debug(f"Consumer group inconsistency: {e}")
            return False

        return True

    def wait_and_verify(self, progress_timeout=60) -> tuple[bool, str | None]:
        try:
            self.validate_progress(progress_timeout=progress_timeout)
        except Exception as e:
            self.logger.error(f"Replication progress validation failed: {e}")
            return (False, str(e))

        wait_until(
            lambda: self.consumer_groups_state_consistent(),
            timeout_sec=3 * progress_timeout,
            backoff_sec=3,
            retry_on_exc=True,
        )

        return (True, None)


# Will retry to send the request if there was a connection aborted
# error, after a short backoff period
def retry_request(func: Callable[..., Any]) -> Any:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except ProtocolError as e:
            if "Connection aborted" not in str(e):
                raise
            self = args[0]
            self.logger.debug(f"Received {e} while executing {str(func)}. Retrying...")
            time.sleep(0.1)
            return func(*args, **kwargs)

    return wrapper


class ShadowLinkTestBase(PreallocNodesTest):
    """
    Base class for Shadow Link tests. This base is responsible
    for setting up the test environment for shadow link testing,
    the test uses a primary service from MultiClusterServices as
    the target cluster. Secondary service is used as the source cluster.
    """

    # Pause Schema Registry API-mode sync and wait for the sync task to park
    # before the target cluster is stopped, so shutdown never overlaps an
    # in-flight sync (an overlap can stall shutdown long enough to time the
    # test out). Set False (class- or instance-level) to opt out, e.g. for
    # tests that deliberately exercise shutdown during a sync.
    pause_sr_sync_before_shutdown: bool = True
    # The drain must outlast a mid-flight sync's destination write retries
    # (~70s worst case) on a loaded debug machine.
    sr_sync_pause_timeout_sec: int = 120

    def __init__(
        self,
        test_context: TestContext,
        num_prealloc_nodes: int = 0,
        secondary_cluster_args: SecondaryClusterArgs = SecondaryClusterArgs(),
        num_brokers=3,
        *args: Any,
        **kwargs: Any,
    ):
        # Detect storage mode from @matrix injected args and configure
        # SI settings / cloud topics config when a non-local mode is
        # requested.  This keeps individual test methods free from
        # boilerplate cluster-setup logic.
        storage_mode = (test_context.injected_args or {}).get("storage_mode")
        needs_si = storage_mode in (
            TopicSpec.STORAGE_MODE_TIERED,
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V1,
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
        )
        needs_cloud_topics = storage_mode in (
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
        )

        if needs_si and "si_settings" not in kwargs:
            kwargs["si_settings"] = SISettings(
                test_context,
                cloud_storage_max_connections=10,
                cloud_storage_enable_remote_read=True,
                cloud_storage_enable_remote_write=True,
                fast_uploads=True,
            )

        kwargs.setdefault("extra_rp_conf", {}).update(
            {
                "enable_shadow_linking": True,
                "group_initial_rebalance_delay": 1000,
            }
        )

        if needs_cloud_topics:
            kwargs["extra_rp_conf"].update(
                {
                    "enable_cluster_metadata_upload_loop": False,
                    # Background compaction for cloud topics runs on this
                    # interval (default 30s). Tests that wait ~30s for
                    # compaction / tombstone removal to make progress race
                    # with the default tick; shrink it so removal lands well
                    # within the wait window. The shadow topic is compacted on
                    # this (target) cluster.
                    "cloud_topics_compaction_interval_ms": 1000,
                }
            )

        # Propagate SI / cloud-topics config to the secondary (source)
        # cluster, creating a fresh SecondaryClusterArgs to avoid
        # mutating the shared default instance.
        if needs_si:
            sec_kwargs = dict(secondary_cluster_args.kwargs)
            if "si_settings" not in sec_kwargs:
                sec_kwargs["si_settings"] = kwargs.get("si_settings")
            if needs_cloud_topics:
                sec_extra = dict(sec_kwargs.get("extra_rp_conf", {}))
                sec_extra.update(
                    {
                        "enable_cluster_metadata_upload_loop": False,
                        # Produce acks block until the L0 upload completes,
                        # so the default 250ms upload interval caps produce
                        # throughput at ~1 msg/interval and makes these
                        # workloads time out. The producer writes to this
                        # (source) cluster, so shrink it here for faster
                        # test runs.
                        "cloud_topics_produce_upload_interval": 25,
                        # The final compaction-consistency check also expects
                        # the source to have removed its tombstones, so shrink
                        # the compaction tick here too (see the primary cluster
                        # config above).
                        "cloud_topics_compaction_interval_ms": 1000,
                    }
                )
                sec_kwargs["extra_rp_conf"] = sec_extra
            secondary_cluster_args = SecondaryClusterArgs(
                secondary_cluster_args.num_brokers,
                *secondary_cluster_args.args,
                **sec_kwargs,
            )

        kwargs.setdefault(
            "log_config",
            LoggingConfig(
                "info",
                logger_levels={
                    "cluster": "trace",
                    "shadow_link": "trace",
                    "kafka/client": "trace",
                    "kafka": "trace",
                    "archival": "trace",
                    "tx": "trace",
                    "shadow_link_service": "trace",
                },
            ),
        )

        super().__init__(
            test_context=test_context,
            # For running kgo producer/consumer
            node_prealloc_count=num_prealloc_nodes,
            num_brokers=num_brokers,
            *args,
            **kwargs,
        )

        self.test_context = test_context
        self.admin_v2: AdminV2
        self.services: MultiClusterServices
        self.service_client: shadow_link_pb2_connect.ShadowLinkServiceClient
        self.internal_service_client: (
            shadow_link_internal_pb2_connect.ShadowLinkServiceClient
        )
        self.secondary_cluster_args: SecondaryClusterArgs = secondary_cluster_args
        self.source_cluster_spec: SecondaryClusterSpec = self.get_source_cluster_spec()

    def get_source_cluster_spec(self) -> SecondaryClusterSpec:
        if not self.test_context.injected_args:
            return DEFAULT_SOURCE_CLUSTER_SPEC

        return self.test_context.injected_args.get(
            SOURCE_CLUSTER_SPEC, DEFAULT_SOURCE_CLUSTER_SPEC
        )

    def leadership_shuffler(
        self, redpanda, topic: str, enabled: bool, namespace: str = "kafka"
    ):
        if not enabled:
            return nullcontext()

        @bg_thread_cm
        def leadership_transfer_thread(redpanda, topic: str, namespace: str):
            admin = Admin(redpanda, retry_codes=[503, 504])
            while (yield):
                try:
                    partitions = admin.get_partitions(namespace=namespace, topic=topic)
                    partition = random.choice(partitions)
                    p_id = partition["partition_id"]
                    admin.partition_transfer_leadership(
                        namespace=namespace, topic=topic, partition=p_id
                    )
                except Exception as e:
                    redpanda.logger.info(f"error transferring leadership: {e}")

        return leadership_transfer_thread(redpanda, topic, namespace)

    def setUp(self):
        self.services = MultiClusterServices(
            self.test_context,
            self.logger,
            self.redpanda,
            secondary_spec=self.source_cluster_spec,
            secondary_args=self.secondary_cluster_args,
        )
        self.services.setUp()
        self.admin_v2 = AdminV2(self.target_cluster_service)
        self.service_client = self.admin_v2.shadow_link()
        self.internal_service_client = self.admin_v2.internal_shadow_link()
        self._install_sr_sync_pause_hook()

    def _install_sr_sync_pause_hook(self) -> None:
        """Make the target service's stop() pause SR sync first. The @cluster
        decorator stops the target cluster inside the test wrapper, before
        tearDown runs, so a tearDown hook would fire only after the cluster is
        already gone; wrapping stop() runs the pause right before any stop of
        a live cluster, on both the passed and failed paths, while both
        clusters are still up."""
        target = self.target_cluster_service
        original_stop = target.stop

        def stop_with_sr_sync_pause(**kwargs: Any) -> None:
            # Skip when nothing is running: ducktape's teardown stops services
            # again after the decorator already stopped this one, and a pause
            # attempt against a stopped cluster would only log a spurious
            # warning. Any stop of a live cluster (including a mid-test one)
            # pauses first.
            if self.pause_sr_sync_before_shutdown and target.started_nodes():
                try:
                    self._pause_schema_registry_sync(self.sr_sync_pause_timeout_sec)
                except Exception:
                    # Best effort: a failed pause must not replace the test's
                    # own result; shutdown just proceeds with the usual
                    # overlap odds.
                    self.logger.warn(
                        "failed to pause Schema Registry sync before shutdown",
                        exc_info=True,
                    )
            original_stop(**kwargs)

        target.stop = stop_with_sr_sync_pause

    def _pause_schema_registry_sync(self, timeout_sec: int) -> None:
        """Pause Schema Registry API-mode sync on every link and wait until
        the sync task is parked, so no sync is running when the clusters
        shut down."""
        for link in self.list_links():
            sr = link.configurations.schema_registry_sync_options
            if not sr.HasField("shadow_schema_registry_api"):
                continue
            link_name = link.name
            if not sr.shadow_schema_registry_api.paused:
                self.logger.info(
                    f"Pausing Schema Registry sync on link {link_name} before shutdown"
                )
                sr.shadow_schema_registry_api.paused = True
                self.update_link(
                    shadow_link=link,
                    update_mask=google.protobuf.field_mask_pb2.FieldMask(
                        paths=[
                            "configurations.schema_registry_sync_options"
                            ".shadow_schema_registry_api.paused"
                        ]
                    ),
                )

            def sr_task_parked() -> bool:
                status = self.get_link(link_name).status
                for task in status.task_statuses:
                    if task.name == SCHEMA_REGISTRY_SYNC_TASK_NAME:
                        if task.state not in (
                            shadow_link_pb2.TASK_STATE_PAUSED,
                            shadow_link_pb2.TASK_STATE_NOT_RUNNING,
                        ):
                            return False
                        # The task reports PAUSED before its in-flight run
                        # has drained (task::pause flips the state, then
                        # closes the runner gate), so PAUSED alone is not
                        # proof the sync stopped. current_sync clears only
                        # when the run actually exits; require that too.
                        return not status.schema_registry_sync_status.HasField(
                            "current_sync"
                        )
                # No task entry: nothing is running, so nothing to drain.
                return True

            wait_until(
                sr_task_parked,
                timeout_sec=timeout_sec,
                backoff_sec=1,
                err_msg=(
                    f"Schema Registry sync task on link {link_name} "
                    "did not pause before shutdown"
                ),
            )

    @property
    def source_cluster(self) -> Cluster:
        return self.services.secondary

    @property
    def source_cluster_service(self) -> RedpandaService:
        return self.services.secondary.service

    @property
    def source_cluster_rpk(self) -> RpkTool:
        return self.source_cluster.rpk

    @property
    def target_cluster_service(self) -> RedpandaService:
        return self.services.primary.service

    @property
    def target_cluster(self) -> RedpandaCluster:
        return self.services.primary

    @property
    def target_cluster_rpk(self) -> RpkTool:
        return self.target_cluster.rpk

    def create_default_link_request(
        self,
        link_name: str,
        mirror_all_topics: bool = True,
        mirror_all_groups: bool = True,
        mirror_all_acls: bool = True,
    ) -> shadow_link_pb2.CreateShadowLinkRequest:
        topic_sync_options: shadow_link_pb2.TopicMetadataSyncOptions = (
            shadow_link_pb2.TopicMetadataSyncOptions(
                interval=google.protobuf.duration_pb2.Duration(seconds=1)
            )
        )
        group_sync_options: shadow_link_pb2.ConsumerOffsetSyncOptions = (
            shadow_link_pb2.ConsumerOffsetSyncOptions(
                interval=google.protobuf.duration_pb2.Duration(seconds=1)
            )
        )
        security_sync_options: shadow_link_pb2.SecuritySettingsSyncOptions = (
            shadow_link_pb2.SecuritySettingsSyncOptions(
                interval=google.protobuf.duration_pb2.Duration(seconds=1)
            )
        )

        if mirror_all_topics:
            topic_sync_options = shadow_link_pb2.TopicMetadataSyncOptions(
                interval=google.protobuf.duration_pb2.Duration(seconds=1),
                auto_create_shadow_topic_filters=[
                    shadow_link_pb2.NameFilter(
                        pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                        filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                        name="*",
                    )
                ],
            )

        if mirror_all_groups:
            group_sync_options = shadow_link_pb2.ConsumerOffsetSyncOptions(
                interval=google.protobuf.duration_pb2.Duration(seconds=1),
                group_filters=[
                    shadow_link_pb2.NameFilter(
                        pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                        filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                        name="*",
                    )
                ],
            )

        if mirror_all_acls:
            security_sync_options = shadow_link_pb2.SecuritySettingsSyncOptions(
                interval=google.protobuf.duration_pb2.Duration(seconds=1),
                acl_filters=[
                    shadow_link_pb2.ACLFilter(
                        resource_filter=shadow_link_pb2.ACLResourceFilter(
                            resource_type=acl_pb2.ACL_RESOURCE_ANY,
                            pattern_type=acl_pb2.ACL_PATTERN_ANY,
                        ),
                        access_filter=shadow_link_pb2.ACLAccessFilter(
                            permission_type=acl_pb2.ACL_PERMISSION_TYPE_ANY,
                            operation=acl_pb2.ACL_OPERATION_ANY,
                        ),
                    ),
                    shadow_link_pb2.ACLFilter(
                        resource_filter=shadow_link_pb2.ACLResourceFilter(
                            resource_type=acl_pb2.ACL_RESOURCE_SR_ANY,
                            pattern_type=acl_pb2.ACL_PATTERN_ANY,
                        ),
                        access_filter=shadow_link_pb2.ACLAccessFilter(
                            permission_type=acl_pb2.ACL_PERMISSION_TYPE_ANY,
                            operation=acl_pb2.ACL_OPERATION_ANY,
                        ),
                    ),
                ],
            )

        client_options = shadow_link_pb2.ShadowLinkClientOptions(
            bootstrap_servers=self.source_cluster.service.brokers_list()
        )

        link_cfg = shadow_link_pb2.ShadowLinkConfigurations(
            client_options=client_options,
            topic_metadata_sync_options=topic_sync_options,
            consumer_offset_sync_options=group_sync_options,
            security_sync_options=security_sync_options,
        )

        link_resource = shadow_link_pb2.ShadowLink(configurations=link_cfg)
        link_resource.name = link_name

        req = shadow_link_pb2.CreateShadowLinkRequest()
        req.shadow_link.CopyFrom(link_resource)
        return req

    def delete_link_request(
        self, link_name: str, force: bool = False
    ) -> shadow_link_pb2.DeleteShadowLinkRequest:
        req = shadow_link_pb2.DeleteShadowLinkRequest(name=link_name, force=force)
        return req

    def create_link(
        self, link_name: str, *args: Any, **kwargs: Any
    ) -> shadow_link_pb2.ShadowLink:
        req = self.create_default_link_request(link_name=link_name, *args, **kwargs)
        return self.create_link_with_request(req=req)

    @retry_request
    def create_link_with_request(
        self, req: shadow_link_pb2.CreateShadowLinkRequest
    ) -> shadow_link_pb2.ShadowLink:
        return self.service_client.create_shadow_link(req=req).shadow_link

    def delete_link(
        self, link_name: str, force: bool = False, *args: Any, **kwargs: Any
    ) -> shadow_link_pb2.DeleteShadowLinkResponse:
        req = self.delete_link_request(
            link_name=link_name, force=force, *args, **kwargs
        )
        return self.delete_link_with_request(req=req)

    @retry_request
    def failover_link(self, name: str) -> shadow_link_pb2.ShadowLink:
        req = shadow_link_pb2.FailOverRequest(name=name)
        return self.service_client.fail_over(req=req).shadow_link

    @retry_request
    def failover_link_topic(
        self, link_name: str, topic: str
    ) -> shadow_link_pb2.ShadowLink:
        req = shadow_link_pb2.FailOverRequest(name=link_name, shadow_topic_name=topic)
        return self.service_client.fail_over(req=req).shadow_link

    @retry_request
    def delete_link_with_request(
        self, req: shadow_link_pb2.DeleteShadowLinkRequest
    ) -> shadow_link_pb2.DeleteShadowLinkResponse:
        return self.service_client.delete_shadow_link(req=req)

    @retry_request
    def list_links(self) -> list[shadow_link_pb2.ShadowLink]:
        resp = self.service_client.list_shadow_links(
            req=shadow_link_pb2.ListShadowLinksRequest()
        )
        return resp.shadow_links

    @retry_request
    def update_link(
        self,
        shadow_link: shadow_link_pb2.ShadowLink,
        update_mask: google.protobuf.field_mask_pb2.FieldMask | None = None,
    ) -> shadow_link_pb2.ShadowLink:
        resp = self.service_client.update_shadow_link(
            req=shadow_link_pb2.UpdateShadowLinkRequest(
                shadow_link=shadow_link, update_mask=update_mask
            )
        )

        return resp.shadow_link

    @retry_request
    def get_link(self, name: str) -> shadow_link_pb2.ShadowLink:
        resp = self.service_client.get_shadow_link(
            req=shadow_link_pb2.GetShadowLinkRequest(name=name)
        )
        return resp.shadow_link

    @retry_request
    def get_shadow_topic(
        self, shadow_link_name: str, shadow_topic_name: str
    ) -> shadow_link_pb2.ShadowTopic:
        resp = self.service_client.get_shadow_topic(
            req=shadow_link_pb2.GetShadowTopicRequest(
                shadow_link_name=shadow_link_name, name=shadow_topic_name
            )
        )
        return resp.shadow_topic

    @retry_request
    def list_shadow_topics(
        self, shadow_link_name: str
    ) -> list[shadow_link_pb2.ShadowTopic]:
        resp = self.service_client.list_shadow_topics(
            req=shadow_link_pb2.ListShadowTopicsRequest(
                shadow_link_name=shadow_link_name
            )
        )
        return resp.shadow_topics

    def remove_shadow_topic(
        self, shadow_link_name: str, shadow_topic_name: str
    ) -> shadow_link_internal_pb2.RemoveShadowTopicResponse:
        return self.internal_service_client.remove_shadow_topic(
            req=shadow_link_internal_pb2.RemoveShadowTopicRequest(
                shadow_link_name=shadow_link_name, shadow_topic_name=shadow_topic_name
            )
        )

    def force_update_shadow_topic_state(
        self,
        shadow_link_name: str,
        shadow_topic_name: str,
        new_state: shadow_link_pb2.ShadowTopicState.ValueType,
    ) -> shadow_link_internal_pb2.ForceUpdateShadowTopicStateResponse:
        return self.internal_service_client.force_update_shadow_topic_state(
            req=shadow_link_internal_pb2.ForceUpdateShadowTopicStateRequest(
                shadow_link_name=shadow_link_name,
                shadow_topic_name=shadow_topic_name,
                new_state=new_state,
            )
        )

    def source_default_client(self):
        return DefaultClient(self.source_cluster.service)

    def target_default_client(self):
        return DefaultClient(self.target_cluster.service)

    @staticmethod
    def _topic_config_from_spec(spec: TopicSpec) -> dict[str, str]:
        """Extract topic-level config from a TopicSpec for rpk creation."""
        config: dict[str, str] = {}
        if spec.cleanup_policy:
            config["cleanup.policy"] = spec.cleanup_policy
        if spec.segment_bytes:
            config["segment.bytes"] = str(spec.segment_bytes)
        if spec.retention_bytes:
            config["retention.bytes"] = str(spec.retention_bytes)
        if spec.retention_ms is not None:
            config["retention.ms"] = str(spec.retention_ms)
        if spec.max_message_bytes:
            config["max.message.bytes"] = str(spec.max_message_bytes)
        if spec.delete_retention_ms:
            config["delete.retention.ms"] = str(spec.delete_retention_ms)
        if spec.min_cleanable_dirty_ratio is not None:
            config["min.cleanable.dirty.ratio"] = str(spec.min_cleanable_dirty_ratio)
        if spec.message_timestamp_type is not None:
            config["message.timestamp.type"] = spec.message_timestamp_type
        if spec.max_compaction_lag_ms is not None:
            config["max.compaction.lag.ms"] = str(spec.max_compaction_lag_ms)
        if spec.min_compaction_lag_ms is not None:
            config["min.compaction.lag.ms"] = str(spec.min_compaction_lag_ms)
        if spec.compression_type is not None:
            config["compression.type"] = str(spec.compression_type)
        return config

    def create_source_topic(self, topic: TopicSpec, storage_mode: str | None = None):
        """Create a topic on the source cluster with the given storage mode.

        For local / None delegates to DefaultClient (existing behaviour).
        For tiered / cloud / tiered_cloud uses rpk so the storage mode
        property can be set, and activates feature flags when necessary.
        """
        if storage_mode is None or storage_mode == TopicSpec.STORAGE_MODE_LOCAL:
            self.source_default_client().create_topic(topic)
            return

        if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2:
            self.source_cluster_service.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )
            self.target_cluster.service.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        config = self._topic_config_from_spec(topic)
        config.update(TopicSpec.storage_mode_config(storage_mode))

        source_rpk = RpkTool(self.source_cluster.service)

        def try_create():
            try:
                source_rpk.create_topic(
                    topic=topic.name,
                    partitions=topic.partition_count,
                    replicas=topic.replication_factor,
                    config=config,
                )
                return True
            except Exception as e:
                if "INVALID_CONFIG" in str(e):
                    return False
                raise

        wait_until(
            try_create,
            timeout_sec=30,
            backoff_sec=2,
            err_msg=f"Failed to create source topic {topic.name} "
            f"with storage_mode={storage_mode}",
        )

    def topic_exists_in_source(self, topic: str) -> bool:
        topics = RpkTool(self.source_cluster_service).list_topics()
        return topic in topics

    def topic_partitions_exists_in_target(
        self,
        topic: TopicSpec,
        rpk: Optional[RpkTool] = None,
    ) -> bool:
        return self.topic_exists_in_target(
            topic=topic.name, partition_count=topic.partition_count, rpk=rpk
        )

    def topic_exists_in_target(
        self,
        topic: str,
        partition_count: Optional[int] = None,
        rpk: Optional[RpkTool] = None,
    ) -> bool:
        rpk = rpk or RpkTool(self.target_cluster.service)
        topics = rpk.list_topics()
        topic_exists = topic in topics

        if partition_count is None:
            return topic_exists

        partitions = list(rpk.describe_topic(topic))
        return topic_exists and len(partitions) == partition_count

    def wait_for_topic_status(
        self,
        link: str,
        topic: str,
        target_status: shadow_link_pb2.ShadowTopicState.ValueType,
        timeout_sec: int = 60,
    ):
        def topic_reached_status():
            try:
                metadata = self.get_link(name=link)
                topic_status = [
                    s.status.state
                    for s in metadata.status.shadow_topics
                    if s.name == topic
                ]
                self.target_cluster_service.logger.debug(
                    f"Topic {topic} status: {topic_status}"
                )
                return next(iter(topic_status), None) == target_status
            except Exception as e:
                self.target_cluster_service.logger.debug(
                    f"Exception while fetching topic status: {e}"
                )
                return False

        self.target_cluster.service.wait_until(
            topic_reached_status,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"Topic {topic} has not reached {target_status} in {timeout_sec} seconds",
        )

    def wait_for_link_status(
        self,
        link: str,
        target_status: shadow_link_pb2.ShadowLinkState.ValueType,
        timeout_sec: int = 60,
    ):
        def link_reached_status():
            try:
                metadata = self.get_link(name=link)
                self.target_cluster_service.logger.debug(
                    f"Link {link} status: {metadata.status.state}"
                )
                return metadata.status.state == target_status
            except Exception as e:
                self.target_cluster_service.logger.debug(
                    f"Exception while fetching link status: {e}"
                )
                return False

        self.target_cluster.service.wait_until(
            link_reached_status,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"Link {link} has not reached {target_status} in {timeout_sec} seconds",
        )

    def wait_for_link_failover(self, link: str, timeout_sec: int = 60):
        def link_failed_over():
            try:
                metadata = self.get_link(name=link)
                self.target_cluster_service.logger.debug(
                    f"Link {link} status: {metadata.status.state}"
                )
                return all(
                    [
                        s.status.state
                        == shadow_link_pb2.ShadowTopicState.SHADOW_TOPIC_STATE_FAILED_OVER
                        for s in metadata.status.shadow_topics
                    ]
                )
            except Exception as e:
                self.target_cluster_service.logger.debug(
                    f"Exception while fetching link status: {e}"
                )
                return False

        self.target_cluster.service.wait_until(
            link_failed_over,
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=f"Link {link} has not completed failover in {timeout_sec} seconds",
        )

    @contextmanager
    def _nop_context_manager(self):
        try:
            yield
        finally:
            pass

    @contextmanager
    def create_source_failure_injector(self, **kwargs):
        fi = FailureInjectorBackgroundThread(
            self.source_cluster.service, self.logger, **kwargs
        )
        fi.start()
        try:
            yield
        finally:
            fi.stop()

    @contextmanager
    def create_target_failure_injector(self, **kwargs):
        fi = FailureInjectorBackgroundThread(
            self.target_cluster.service, self.logger, **kwargs
        )
        fi.start()
        try:
            yield
        finally:
            fi.stop()

    @contextmanager
    def superuser_access(self):
        self.admin_v2 = AdminV2(
            self.target_cluster_service,
            auth=(
                self.redpanda.SUPERUSER_CREDENTIALS.username,
                self.redpanda.SUPERUSER_CREDENTIALS.password,
            ),
        )
        self.service_client = self.admin_v2.shadow_link()
        try:
            yield
        finally:
            self.admin_v2 = AdminV2(self.target_cluster_service)
            self.service_client = self.admin_v2.shadow_link()


class ShadowLinkPreAllocTestBase(ShadowLinkTestBase):
    """
    Same as ShadowLinkTestBase but with preallocated nodes for running
    kgo producer/consumer pair on a preallocated node.
    """

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(test_context, num_prealloc_nodes=1, *args, **kwargs)

        self.verifier: ClusterLinkingProgressVerifier
        self.started = False

    def _start_producer_consumer(
        self,
        topic: str = "test-topic",
        msg_size: int = 128,
        msg_cnt: int = 10000,
        use_transactions: bool = False,
        use_compaction: bool = False,
        producer_properties: dict[str, Any] | None = None,
    ):
        self.verifier = ClusterLinkingProgressVerifier(
            self.test_context,
            self.source_cluster,
            self.target_cluster,
            topic,
            self.preallocated_nodes,
            self.logger,
            msg_count=msg_cnt,
            msg_size=msg_size,
            use_transactions=use_transactions,
            use_compaction=use_compaction,
            producer_properties=producer_properties or {},
            timeout_sec=180,
        )
        self.verifier.start()
        self.started = True

    @contextmanager
    def producer_consumer(self, **kwargs: Any):
        self._start_producer_consumer(**kwargs)
        try:
            yield
        finally:
            self.verifier.stop_kgo_services()

    def verify(self, progress_timeout: int = 60):
        success, error = self.verifier.wait_and_verify(
            progress_timeout=progress_timeout
        )

        assert success, f"Verification failed: {error}"
