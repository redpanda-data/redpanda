# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from dataclasses import dataclass
import math
import operator
import random
from pathlib import Path
import string
from typing import Any

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from rptest.context.cloud_storage import CloudStorageType
from rptest.services.cluster import cluster
from rptest.services.consumer_swarm import ConsumerSwarm
from rptest.services.producer_swarm import ProducerSwarm
from rptest.services.redpanda import (
    LoggingConfig,
    PandaproxyConfig,
    SISettings,
    SchemaRegistryConfig,
    RESTART_LOG_ALLOW_LIST,
)
from rptest.tests.datalake.catalog_service_factory import filesystem_catalog_type
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.schemas import linear_pb2
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.utils import LocalPayloadDirectory
from rptest.utils.scale_parameters import ScaleParameters


@dataclass
class LocalPayloadInfo:
    payload_size: int
    payloads_per_topic: dict[str, LocalPayloadDirectory]


@dataclass
class SwarmNodeConfig:
    num_clients: int
    num_messages_per_client: int


class DatalakeScaleTest(RedpandaTest):
    # The maximum response size a client will tell RP its willing to receive.
    FETCH_MAX_BYTES_MIB: int = 90

    def __init__(self, test_ctx: TestContext, *args: Any, **kwargs: Any):
        self._ctx = test_ctx
        super(DatalakeScaleTest, self).__init__(
            test_ctx,
            num_brokers=3,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                # Raise the broker-imposed fetch max bytes to allow for fetches
                # to return more than just the obligatory read.
                "fetch_max_bytes": self.FETCH_MAX_BYTES_MIB * 2**20,
                # Similar to above, is deprecated in later versions of RP.
                "kafka_max_bytes_per_fetch": self.FETCH_MAX_BYTES_MIB * 2**20,
                "iceberg_enabled": "true",
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            log_config=LoggingConfig("info", {"datalake": "debug"}),
            *args,
            **kwargs,
        )

    def setUp(self) -> None:
        # Defer start-up to DatalakeServices
        pass

    def _get_schema_registry_client(self) -> SchemaRegistryClient:
        schema_registry_url = self.redpanda.schema_reg().split(",", 1)[0]
        schema_registry_conf: dict[str, str] = {"url": schema_registry_url}

        return SchemaRegistryClient(schema_registry_conf)

    def _random_linear_msg(self, msg_type: Any, str_len: int) -> Any:
        """For a protobuf message of `msg_type` which is just a linear list of string fields generate a random message."""
        msg_descriptor = msg_type.DESCRIPTOR
        msg = msg_type()
        fields: list[str] = [field for field in msg_descriptor.fields_by_name]
        for field in fields:
            random_str = "".join(random.choices(string.ascii_letters, k=str_len))
            setattr(msg, field, random_str)

        return msg

    def _create_local_payloads(
        self,
        topics: list[str],
        msg_type: Any,
        msg_field_size_bytes: int,
        unique_messages_per_topic: int,
    ) -> LocalPayloadInfo:
        schema_registry_client = self._get_schema_registry_client()
        # Note that `ps` is used to serialize the message and register it's schema with Redpanda's schema registry.
        ps = ProtobufSerializer(
            msg_type, schema_registry_client, {"use.deprecated.format": False}
        )

        payload_info = LocalPayloadInfo(payload_size=0, payloads_per_topic={})

        for topic_name in topics:
            payloads = LocalPayloadDirectory(
                path=Path(f"/tmp/custom_payloads_{topic_name}")
            )

            payload_size: int = 0
            for i in range(0, unique_messages_per_topic):
                msg = self._random_linear_msg(msg_type, msg_field_size_bytes)
                msg_bytes: bytes = ps(
                    msg, SerializationContext(topic_name, MessageField.VALUE)
                )
                payload_size = len(msg_bytes)
                payloads.add_payload(f"message_{i}", msg_bytes)

            payload_info.payloads_per_topic[topic_name] = payloads
            assert (
                payload_info.payload_size == 0
                or payload_info.payload_size == payload_size
            ), "all messages must be of the same size"
            payload_info.payload_size = payload_size

        return payload_info

    def _create_topics(
        self,
        topics: list[str],
        partitions_per_topic: int,
        replication_factor: int,
        max_message_size: int,
        scale: ScaleParameters,
        dl: DatalakeServices,
    ):
        for tn in topics:
            config = {
                "segment.bytes": scale.segment_size,
                "retention.bytes": scale.retention_bytes,
                "cleanup.policy": "delete",
                "max.message.bytes": max_message_size + 1000,
            }

            if scale.local_retention_bytes:
                config["retention.local.target.bytes"] = scale.local_retention_bytes

            dl.create_iceberg_enabled_topic(
                name=tn,
                partitions=partitions_per_topic,
                replicas=replication_factor,
                iceberg_mode="value_schema_id_prefix",
                config=config,
            )

    def _create_producer_swarm_nodes(
        self, topics: list[str], local_payloads: LocalPayloadInfo, cfg: SwarmNodeConfig
    ) -> list[ProducerSwarm]:
        assert len(topics) == len(local_payloads.payloads_per_topic.keys()), (
            "must have one set of payloads per topic"
        )

        producer_nodes: list[ProducerSwarm] = []
        for topic in topics:
            producer_nodes.append(
                ProducerSwarm(
                    self.test_context,
                    self.redpanda,
                    topic,
                    cfg.num_clients,
                    cfg.num_messages_per_client,
                    unique_topics=False,
                    messages_per_second_per_producer=0,
                    local_payload_dir=local_payloads.payloads_per_topic[topic],
                )
            )

        return producer_nodes

    def _create_consumer_swarm_nodes(
        self, topics: list[str], cfg: SwarmNodeConfig
    ) -> list[ConsumerSwarm]:
        consumer_nodes: list[ConsumerSwarm] = []
        max_fetch_bytes = self.FETCH_MAX_BYTES_MIB * 2**20
        # Set properties to allow for more than just the obligatory read to be returned.
        properties: dict[str, Any] = {
            "fetch.max.bytes": max_fetch_bytes,
            "max.partition.fetch.bytes": max_fetch_bytes,
        }
        for topic in topics:
            consumer_nodes.append(
                ConsumerSwarm(
                    self.test_context,
                    self.redpanda,
                    topic,
                    f"{topic}_group",
                    cfg.num_clients,
                    int(0.95 * cfg.num_clients * cfg.num_messages_per_client),
                    properties=properties,
                )
            )
        return consumer_nodes

    def _datalake_staging_usage(self):
        # returns number of bytes in datalake staging directory
        metric_name = "vectorized_space_management_datalake_disk_usage_bytes"
        total_usage = self.redpanda.metric_sum(metric_name, expect_metric=True)

        self.logger.info(f"datalake_staging_usage={total_usage}")
        return total_usage

    @cluster(num_nodes=10, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(
        msg_size_bytes=[32 * 1024 * 1024],
        msg_field_count=[20, 80],
        cloud_storage_type=supported_storage_types(),
    )
    def test_iceberg_large_messages_throughput(
        self,
        msg_size_bytes: int,
        msg_field_count: int,
        cloud_storage_type: CloudStorageType,
    ):
        assert not self.debug_mode
        assert self.redpanda.dedicated_nodes

        match msg_field_count:
            case 20:
                msg_type = linear_pb2.Linear20
            case 80:
                msg_type = linear_pb2.Linear80
            case _:
                assert False, "invalid value for field count"
        msg_field_size_bytes = msg_size_bytes // msg_field_count

        swarm_nodes = 2
        replication_factor = 3

        unique_messages_per_topic = 10
        # Account for formatting metadata size in message
        estimated_max_message_size = int(1.1 * msg_size_bytes)

        scale = ScaleParameters(
            self.redpanda,
            replication_factor,
            tiered_storage_enabled=True,
        )

        topic_names = [f"datalake_large_messages_{i}" for i in range(swarm_nodes)]
        partitions_per_topic = scale.node_cpus * scale.node_count * 5

        clients_per_swarm_node = scale.node_cpus * scale.node_count * 4
        bytes_per_message_count = (
            clients_per_swarm_node * estimated_max_message_size * swarm_nodes
        )
        # Use the non-TS estimate from ScaleParameters
        expected_throughput: float = (
            (scale.node_count / replication_factor)
            * (scale.node_cpus / 24.0)
            * 1e9
            * 0.5
        )
        target_runtime_sec = 60
        total_bytes = target_runtime_sec * expected_throughput
        message_count_per_client = math.ceil(total_bytes / bytes_per_message_count) + 1
        swarm_node_cfg = SwarmNodeConfig(
            num_clients=clients_per_swarm_node,
            num_messages_per_client=message_count_per_client,
        )

        self.logger.info(
            f"expected throughput: {scale.expect_bandwidth} message_count_per_client: {message_count_per_client} total_bytes: {total_bytes} bytes_per_message_count: {bytes_per_message_count}"
        )

        with DatalakeServices(
            self._ctx,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=filesystem_catalog_type(),
        ) as dl:
            self._create_topics(
                topics=topic_names,
                partitions_per_topic=partitions_per_topic,
                replication_factor=replication_factor,
                max_message_size=estimated_max_message_size,
                scale=scale,
                dl=dl,
            )
            payload_info = self._create_local_payloads(
                topics=topic_names,
                msg_type=msg_type,
                msg_field_size_bytes=msg_field_size_bytes,
                unique_messages_per_topic=unique_messages_per_topic,
            )
            producer_nodes = self._create_producer_swarm_nodes(
                topics=topic_names, local_payloads=payload_info, cfg=swarm_node_cfg
            )
            consumer_nodes = self._create_consumer_swarm_nodes(
                topics=topic_names, cfg=swarm_node_cfg
            )

            for p in producer_nodes:
                p.start()

            for c in consumer_nodes:
                c.start()

            for p in producer_nodes:
                p.wait()

            for c in consumer_nodes:
                c.wait()

            for topic_name in topic_names:
                dl.wait_for_translation(
                    topic_name,
                    msg_count=int(
                        0.95
                        * swarm_node_cfg.num_messages_per_client
                        * swarm_node_cfg.num_clients
                    ),
                    op=operator.gt,
                    progress_sec=2 * 60,
                    timeout=5 * 60,
                    backoff_sec=10,
                )

            # Currently the datalake scratch space is not accounted for in the
            # storage usage admin api result. This can result in a storage
            # usage inconsistency error on exit. Hence we wait here for the
            # scratch space to be cleared before ending the test
            wait_until(
                lambda: self._datalake_staging_usage() == 0,
                timeout_sec=5 * 60,
                backoff_sec=30,
                err_msg="datalake_staging_usage != 0",
            )
