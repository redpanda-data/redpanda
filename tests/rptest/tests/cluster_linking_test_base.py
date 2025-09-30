# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
from contextlib import contextmanager
import time
from typing import Any

import google.protobuf.duration_pb2
import google.protobuf.field_mask_pb2
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    shadow_link_pb2,
    shadow_link_pb2_connect,
)
from rptest.clients.admin.proto.redpanda.core.common import acl_pb2
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool
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
)
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.node_operations import FailureInjectorBackgroundThread


SOURCE_CLUSTER_SPEC = "source_cluster_spec"


DEFAULT_SOURCE_CLUSTER_SPEC = SecondaryClusterSpec(ServiceType.REDPANDA)


class ClusterLinkingProgressVerifier:
    def __init__(
        self,
        test_context,
        source_cluster: Cluster,
        target_cluster: RedpandaCluster,
        topic: str,
        preallocated_nodes: list,
        logger,
        msg_count: int = 40000,
        msg_size: int = 4 * 1024,
        timeout_sec: int = 600,
    ):
        self.test_context = test_context
        self.source_cluster = source_cluster
        self.target_cluster = target_cluster

        self.topic = topic
        self.source_rpk = RpkTool(self.source_cluster.service)
        self.target_rpk = RpkTool(self.target_cluster.service)
        self.preallocated_nodes = preallocated_nodes
        self.logger = logger
        self.msg_count = msg_count
        self.msg_size = msg_size

        self.timeout_sec = timeout_sec

    def start(self):
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.source_cluster.service,
            topic=self.topic,
            msg_size=self.msg_size,
            msg_count=self.msg_count,
            custom_node=self.preallocated_nodes,
        )
        self.producer.start(clean=False)
        self.producer.wait_for_acks(10, 40, 1)
        readers = 16

        self.source_consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.source_cluster.service,
            topic=self.topic,
            msg_size=self.msg_size,
            readers=readers,
            group_name="source-cg",
            continuous=True,
        )
        self.source_consumer.start(clean=False)

        self.target_consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.target_cluster.service,
            topic=self.topic,
            msg_size=self.msg_size,
            max_msgs=self.msg_count,
            readers=readers,
            group_name="test-kgo-consumer-group",
            nodes=self.preallocated_nodes,
            continuous=True,
        )

        self.target_consumer.start(clean=False)

    def producer_finished(self):
        return self.producer.produce_status.acked >= self.msg_count

    def source_consumer_finished(self):
        return (
            self.source_consumer.consumer_status.validator.total_reads >= self.msg_count
        )

    def target_consumer_finished(self):
        return (
            self.target_consumer.consumer_status.validator.total_reads >= self.msg_count
        )

    def workload_finished(self):
        return (
            self.producer_finished()
            and self.source_consumer_finished()
            and self.target_consumer_finished()
        )

    def _calculate_partition_lag(self):
        ret = defaultdict(dict)

        # describe target first to make sure the lag is always greater than or equal to 0
        target = list(self.target_rpk.describe_topic(self.topic))
        source = list(self.source_rpk.describe_topic(self.topic))

        if len(source) != len(target):
            return None
        for source_partition, target_partition in zip(source, target):
            if source_partition.id != target_partition.id:
                return None
            assert source_partition.high_watermark >= target_partition.high_watermark, (
                f"Source partition high watermark must be greater than or equal to target partition high watermark (source: {source_partition.high_watermark}, target: {target_partition.high_watermark})"
            )
            lag = source_partition.high_watermark - target_partition.high_watermark
            self.logger.debug(
                f"Partition {self.topic}/{source_partition.id} - source: ({source_partition}), target: ({target_partition}) lag: {lag}"
            )
            ret[source_partition.id] = lag

        return ret

    def total_lag(self, partition_lags) -> float:
        if partition_lags is None:
            return float("inf")
        total = 0
        for lag in partition_lags.values():
            total += lag
        return total

    def validate_progress(self, progress_timeout=60, backoff_delay=5):
        total_last_lag = float("inf")
        replication_last_progress = time.time()

        workload_last_progress = time.time()
        source_consumer_last_reads = 0
        target_consumer_last_reads = 0
        producer_last_acked = 0

        while True:
            # Check replication progress
            current_lag = self._calculate_partition_lag()
            total_current_lag = self.total_lag(current_lag)
            now = time.time()
            # track replication progress
            if total_current_lag < total_last_lag or total_current_lag == 0:
                self.logger.debug(
                    f"Replication making progress - current_lag: {total_current_lag}, last_lag: {total_last_lag}"
                )
                replication_last_progress = now

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

            if not self.workload_finished() and (
                now - workload_last_progress > progress_timeout
            ):
                self.logger.error(
                    f"No workload progress for {progress_timeout}s, source reads: {source_reads} (last: {source_consumer_last_reads}), target reads: {target_reads} (last: {target_consumer_last_reads}), producer acks: {producer_acked} (last: {producer_last_acked})"
                )
                self.source_consumer.stop()
                self.target_consumer.stop()
                self.producer.stop()
                raise Exception("Workload stalled")

            total_last_lag = total_current_lag
            if time.time() - replication_last_progress > progress_timeout:
                self.logger.error(
                    f"No replication progress for {progress_timeout}s, last lag: {total_last_lag}, current lag: {total_current_lag}"
                )
                raise Exception("Replication stalled")

            if self.workload_finished() and total_current_lag == 0:
                self.logger.info("Replication finished")
                break

            time.sleep(backoff_delay)

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
                    self.logger.error(
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
                self.logger.error(f"Consumer group inconsistency: {e}")
            return False

        return True

    def wait_and_verify(self, progress_timeout=60):
        try:
            self.validate_progress(progress_timeout=progress_timeout)
        except Exception as e:
            self.logger.error(f"Replication progress validation failed: {e}")
            return (False, e)

        wait_until(
            lambda: self.consumer_groups_state_consistent(),
            timeout_sec=3 * progress_timeout,
            backoff_sec=3,
        )

        return (True, None)


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
        *args: Any,
        **kwargs: Any,
    ):
        kwargs.setdefault("extra_rp_conf", {}).update(
            {
                "enable_shadow_linking": True,
            }
        )

        super().__init__(
            test_context=test_context,
            # For running kgo producer/consumer
            node_prealloc_count=num_prealloc_nodes,
            num_brokers=3,
            log_config=LoggingConfig(
                "info",
                logger_levels={
                    "cluster_link": "trace",
                    "kafka/client": "trace",
                    "kafka": "trace",
                },
            ),
            *args,
            **kwargs,
        )

        self.test_context = test_context
        self.admin_v2: AdminV2
        self.services: MultiClusterServices
        self.service_client: shadow_link_pb2_connect.ShadowLinkServiceClient
        self.secondary_cluster_args: SecondaryClusterArgs = secondary_cluster_args
        self.source_cluster_spec: SecondaryClusterSpec = self.get_source_cluster_spec()

    def get_source_cluster_spec(self) -> SecondaryClusterSpec:
        if not self.test_context.injected_args:
            return DEFAULT_SOURCE_CLUSTER_SPEC

        return self.test_context.injected_args.get(
            SOURCE_CLUSTER_SPEC, DEFAULT_SOURCE_CLUSTER_SPEC
        )

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
                shadowed_topic_properties=["replication.factor"],
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
        self, link_name: str
    ) -> shadow_link_pb2.DeleteShadowLinkRequest:
        req = shadow_link_pb2.DeleteShadowLinkRequest(name=link_name)
        return req

    def create_link(
        self, link_name: str, *args: Any, **kwargs: Any
    ) -> shadow_link_pb2.ShadowLink:
        req = self.create_default_link_request(link_name=link_name, *args, **kwargs)
        return self.create_link_with_request(req=req)

    def create_link_with_request(
        self, req: shadow_link_pb2.CreateShadowLinkRequest
    ) -> shadow_link_pb2.ShadowLink:
        return self.service_client.create_shadow_link(req=req).shadow_link

    def delete_link(
        self, link_name: str, *args, **kwargs
    ) -> shadow_link_pb2.DeleteShadowLinkResponse:
        req = self.delete_link_request(link_name=link_name, *args, **kwargs)
        return self.delete_link_with_request(req=req)

    def delete_link_with_request(
        self, req: shadow_link_pb2.DeleteShadowLinkRequest
    ) -> shadow_link_pb2.DeleteShadowLinkResponse:
        return self.service_client.delete_shadow_link(req=req)

    def list_links(self) -> list[shadow_link_pb2.ShadowLink]:
        resp = self.service_client.list_shadow_links(
            req=shadow_link_pb2.ListShadowLinksRequest()
        )
        return resp.shadow_links

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

    def get_link(self, name: str) -> shadow_link_pb2.ShadowLink:
        resp = self.service_client.get_shadow_link(
            req=shadow_link_pb2.GetShadowLinkRequest(name=name)
        )
        return resp.shadow_link

    def source_default_client(self):
        return DefaultClient(self.source_cluster.service)

    def topic_exists_in_source(self, topic: str) -> bool:
        topics = RpkTool(self.source_cluster_service).list_topics()
        return topic in topics

    def topic_exists_in_target(self, topic: str) -> bool:
        topics = RpkTool(self.target_cluster.service).list_topics()
        return topic in topics

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


class ShadowLinkPreAllocTestBase(ShadowLinkTestBase):
    """
    Same as ShadowLinkTestBase but with preallocated nodes for running
    kgo producer/consumer pair on a preallocated node.
    """

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(test_context, num_prealloc_nodes=1, *args, **kwargs)

        self.verifier: ClusterLinkingProgressVerifier
        self.started = False

    def start_producer_consumer(
        self,
        topic: str = "test-topic",
        msg_size: int = 128,
        msg_cnt: int = 10000,
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
            timeout_sec=180,
        )
        self.verifier.start()
        self.started = True

    def verify(self):
        success, error = self.verifier.wait_and_verify()

        assert success, f"Verification failed: {error}"
