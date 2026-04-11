# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from contextlib import contextmanager, nullcontext
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
from rptest.services.redpanda import LoggingConfig, TLSProvider
from rptest.services.tls import CertificateAuthority, Certificate, TLSCertManager
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.util import bg_thread_cm, wait_until_result
from rptest.utils.node_operations import FailureInjectorBackgroundThread
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
]


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

        self.target_consumer = KgoVerifierConsumerGroupConsumer(
            context=self.test_context,
            redpanda=self.target_cluster.service,
            topic=self.topic,
            msg_size=self.msg_size,
            max_msgs=self.msg_count,
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
        # describe target first to make sure the lag is always greater than or equal to 0
        def describe_topics():
            def describe_once():
                target = list(self.target_rpk.describe_topic(self.topic))
                source = list(self.source_rpk.describe_topic(self.topic))
                if len(source) != len(target):
                    return False, None
                return True, (target, source)

            return wait_until_result(
                describe_once,
                timeout_sec=timeout,
                backoff_sec=0.5,
                err_msg=f"Failed to describe topics for lag calculation in {timeout} seconds",
            )

        try:
            (target, source) = describe_topics()
            assert len(target) == len(source), (
                "Verification failed, Topic partitions count mismatch between source and target"
            )
            partitions_with_lag = 0
            for source_partition, target_partition in zip(source, target):
                assert source_partition.id == target_partition.id, (
                    f"Partition id mismatch {source_partition.id} != {target_partition.id}"
                )
                if target_partition.high_watermark != source_partition.high_watermark:
                    lag = (
                        source_partition.high_watermark
                        - target_partition.high_watermark
                    )
                    self.logger.debug(
                        f"Partition {self.topic}/{source_partition.id} - source: ({source_partition}), target: ({target_partition}) lag: {lag}"
                    )
                    partitions_with_lag += 1
                assert debug_only or partitions_with_lag == 0, (
                    f"Verification failed, {partitions_with_lag} partitions do not have synced high watermarks"
                )
        except Exception as e:
            self.logger.warning(f"Verification failed: {e}")
            if not debug_only:
                raise

    def stop_kgo_services(self):
        self.source_consumer.stop()
        self.target_consumer.stop()
        self.producer.stop()

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

    def __init__(
        self,
        test_context: TestContext,
        num_prealloc_nodes: int = 0,
        secondary_cluster_args: SecondaryClusterArgs = SecondaryClusterArgs(),
        num_brokers=3,
        *args: Any,
        **kwargs: Any,
    ):
        kwargs.setdefault("extra_rp_conf", {}).update(
            {
                "enable_shadow_linking": True,
                "group_initial_rebalance_delay": 1000,
            }
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
            num_brokers=3,
            secondary_args=self.secondary_cluster_args,
        )
        self.services.setUp()
        self.admin_v2 = AdminV2(self.target_cluster_service)
        self.service_client = self.admin_v2.shadow_link()
        self.internal_service_client = self.admin_v2.internal_shadow_link()

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

    def verify(self):
        success, error = self.verifier.wait_and_verify()

        assert success, f"Verification failed: {error}"
