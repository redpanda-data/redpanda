# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from contextlib import contextmanager

import google.protobuf.duration_pb2
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    shadow_link_pb2,
    shadow_link_pb2_connect,
)
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool
from rptest.services.kgo_verifier_services import (
    KgoVerifierConsumerGroupConsumer,
    KgoVerifierProducer,
)
from rptest.services.multi_cluster_services import (
    Cluster,
    MultiClusterServices,
    RedpandaCluster,
    RedpandaService,
    ServiceType,
)
from rptest.services.redpanda import LoggingConfig
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.node_operations import FailureInjectorBackgroundThread


class ShadowLinkTestBase(PreallocNodesTest):
    """
    Base class for Shadow Link tests. This base is responsible
    for setting up the test environment for shadow link testing,
    the test uses a primary service from MultiClusterServices as
    the target cluster. Secondary service is used as the source cluster.
    """

    def __init__(self, test_context, num_prealloc_nodes=0, *args, **kwargs):
        super().__init__(
            test_context=test_context,
            # For running kgo producer/consumer
            node_prealloc_count=num_prealloc_nodes,
            num_brokers=3,
            extra_rp_conf={
                "enable_developmental_unrecoverable_data_corrupting_features": True,
                "development_enable_cluster_link": True,
            },
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
        self.client: shadow_link_pb2_connect.ShadowLinkServiceClient

    def setUp(self):
        self.services = MultiClusterServices(
            self.test_context,
            self.logger,
            self.redpanda,
            secondary_type=ServiceType.REDPANDA,
            num_brokers=3,
        )
        self.services.setUp()
        self.admin_v2 = AdminV2(self.target_cluster_service)
        self.client = self.admin_v2.shadow_link()

    @property
    def source_cluster(self) -> Cluster:
        return self.services.secondary

    @property
    def source_cluster_service(self) -> RedpandaService:
        return self.services.secondary.service

    @property
    def target_cluster_service(self) -> RedpandaService:
        return self.services.primary.service

    @property
    def target_cluster(self) -> RedpandaCluster:
        return self.services.primary

    def create_link(
        self,
        link_name: str,
        mirror_all_topics: bool = True,
        mirror_all_groups: bool = True,
    ):
        if mirror_all_topics:
            topic_sync_options = shadow_link_pb2.TopicMetadataSyncOptions(
                interval=google.protobuf.duration_pb2.Duration(seconds=1),
                topic_filters=[
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
        client_options = shadow_link_pb2.ShadowLinkClientOptions(
            bootstrap_servers=self.source_cluster.service.brokers_list()
        )

        link_cfg = shadow_link_pb2.ShadowLinkConfigurations(
            client_options=client_options,
            topic_metadata_sync_options=topic_sync_options,
            consumer_offset_sync_options=group_sync_options,
        )

        link_resource = shadow_link_pb2.ShadowLink(configurations=link_cfg)
        link_resource.name = link_name

        req = shadow_link_pb2.CreateShadowLinkRequest()
        req.shadow_link.CopyFrom(link_resource)
        return self.client.create_shadow_link(req=req)

    def list_links(self) -> list[shadow_link_pb2.ShadowLink]:
        resp = self.client.list_shadow_links(
            req=shadow_link_pb2.ListShadowLinksRequest()
        )
        return resp.shadow_links

    def get_link(self, name: str) -> shadow_link_pb2.ShadowLink:
        resp = self.client.get_shadow_link(
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

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context, num_prealloc_nodes=1, *args, **kwargs)
        self.producer: KgoVerifierProducer = None
        self.consumer: KgoVerifierConsumerGroupConsumer = None
        self.started = False

    def start_producer_consumer(
        self,
        topic: str = "test-topic",
        msg_size: int = 128,
        msg_cnt: int = 10000,
        with_logs: bool = True,
    ) -> KgoVerifierProducer:
        assert self.producer is None, "Producer already started"
        assert self.consumer is None, "Consumer already started"
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.source_cluster.service,
            topic,
            msg_size,
            msg_cnt,
            custom_node=self.preallocated_nodes,
            rate_limit_bps=1024 * 1024,
            debug_logs=with_logs,
            trace_logs=with_logs,
        )
        self.producer.start(clean=False)
        wait_until(
            lambda: self.producer.produce_status.acked > 10,
            timeout_sec=120,
            backoff_sec=0.1,
        )
        self.consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.target_cluster.service,
            nodes=self.preallocated_nodes,
            topic=topic,
            msg_size=msg_size,
            max_msgs=msg_cnt,
            readers=1,
            continuous=True,
            debug_logs=with_logs,
            trace_logs=with_logs,
        )
        self.consumer.start(clean=False)
        self.started = True

    def verify(self):
        try:
            assert self.started, "Producer/Consumer not started"
            self.producer.wait()
            self.consumer.wait()
            assert self.producer.produce_status.bad_offsets == 0, (
                "Producer bad offsets detected"
            )
            assert self.consumer.consumer_status.validator.invalid_reads == 0, (
                "Consumer invalid reads detected"
            )
            # Todo factor in compaction in future tests.
            assert self.consumer.consumer_status.validator.offset_gaps == 0, (
                "Consumption gaps detected, check consumer logs for gap offsets"
            )
        finally:
            if self.producer is not None:
                self.producer.stop()
            if self.consumer is not None:
                self.consumer.stop()
