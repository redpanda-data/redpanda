# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from enum import Enum
from typing import Any, Type, TypeVar

import kafkatest.version
from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.kafka import KafkaServiceAdapter
from rptest.services.redpanda import RedpandaService

KAFKA_VERSION = kafkatest.version.KafkaVersion("3.7.0")


class ServiceType(str, Enum):
    REDPANDA = "redpanda"
    KAFKA = "kafka"


Service = RedpandaService | KafkaServiceAdapter
C = TypeVar("C", bound="Cluster")
KC = TypeVar("KC", bound="KafkaCluster")
RC = TypeVar("RC", bound="RedpandaCluster")


class Cluster:
    _service: Service

    def __init__(self, service: Service):
        self._service: Service = service

    @property
    def service(self) -> Service:
        return self._service

    @property
    def is_kafka(self) -> bool:
        return False

    @property
    def is_redpanda(self) -> bool:
        return False

    @property
    def admin(self) -> Admin:
        raise NotImplementedError("Cluster.admin")

    @property
    def rpk(self) -> RpkTool:
        return RpkTool(self._service)

    def start(self):
        self.service.start()

    def stop(self):
        self.service.stop()

    def __str__(self):
        return f"{self._service_type} cluster of {len(self._service.nodes)} nodes"


class KafkaCluster(Cluster):
    _zk: ZookeeperService

    def __init__(self, service: KafkaServiceAdapter, zk: ZookeeperService):
        super().__init__(service)
        self._zk = zk

    @classmethod
    def create(cls: Type[KC], test_ctx, num_brokers) -> KC:
        zk = ZookeeperService(test_ctx, num_nodes=1, version=KAFKA_VERSION)
        svc = KafkaServiceAdapter(
            test_ctx,
            KafkaService(test_ctx, num_nodes=num_brokers, zk=zk, version=KAFKA_VERSION),
        )
        return cls(svc, zk)

    @property
    def is_kafka(self) -> bool:
        return True

    def start(self):
        self._zk.start()
        super().start()

    def stop(self):
        super().stop()
        self._zk.stop()

    def __str__(self):
        return f"Kafka cluster of {len(self.service.nodes)} nodes"


class RedpandaCluster(Cluster):
    def __init__(self, service: RedpandaService):
        super().__init__(service)

    @classmethod
    def create(cls: Type[RC], test_ctx, num_brokers) -> RC:
        return cls(RedpandaService(test_ctx, num_brokers=num_brokers))

    @property
    def is_redpanda(self) -> bool:
        return True

    @property
    def admin(self) -> Admin:
        return Admin(self.service)

    def __str__(self):
        return f"Redpanda cluster of {len(self.service.nodes)} nodes"


class MultiClusterServices:
    def __init__(
        self,
        test_ctx,
        logger,
        redpanda: RedpandaService,
        secondary_type: ServiceType = ServiceType.REDPANDA,
        num_brokers=3,
    ):
        self.test_ctx = test_ctx
        self.logger = logger
        self._clusters: list[Cluster] = [RedpandaCluster(redpanda)]
        if secondary_type is ServiceType.REDPANDA:
            self._clusters.append(RedpandaCluster.create(self.test_ctx, num_brokers))
        elif secondary_type is ServiceType.KAFKA:
            self._clusters.append(KafkaCluster.create(self.test_ctx, num_brokers))
        assert len(self._clusters) == 2, f"Expected two clusters, got {self._clusters=}"

    def setUp(self):
        assert len(self.primary.service.started_nodes()) == 0, (
            f"{str(c)}: MultiClusterServices expects to start itself"
        )

        # TODO: extra configs?

        for c in self._clusters:
            c.start()

    def tearDown(self):
        # NOTE: expect the primary service to be shut down by the test framework
        self.secondary.stop()

    @property
    def primary(self):
        return self._clusters[0]

    @property
    def secondary(self):
        return self._clusters[1]

    def create_topic(
        self,
        cluster: Cluster,
        name: str,
        partitions: int = 1,
        replicas: int = 1,
        config: dict[str, Any] = dict(),
    ):
        cluster.rpk.create_topic(
            topic=name, partitions=partitions, replicas=replicas, config=config
        )

    def list_topics(self, cluster: Cluster, detailed: bool = False):
        return list(cluster.rpk.list_topics(detailed))

    def __enter__(self):
        self.setUp()
        return self

    def __exit__(self, *args, **kwargs):
        self.tearDown()
