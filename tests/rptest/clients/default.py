# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import typing
from collections.abc import Sequence
from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.kcl import KCL
from rptest.clients.rpk import RpkTool
from kafka import KafkaAdminClient


class PartitionDescription(typing.NamedTuple):
    id: int
    leader: int
    replicas: typing.List[int]


class TopicDescription(typing.NamedTuple):
    name: str
    partitions: typing.List[PartitionDescription]


class TopicConfigValue(typing.NamedTuple):
    value: typing.Union[str, int]
    source: str

    def __eq__(self, other):
        if isinstance(other, TopicConfigValue):
            return self.value == other.value and self.source == other.source
        assert isinstance(other, str) or isinstance(other, int)
        return self.value == other


class DefaultClient:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def create_topic(self, specs):
        if isinstance(specs, TopicSpec):
            specs = [specs]
        client = KafkaCliTools(self._redpanda)
        for spec in specs:
            client.create_topic(spec)

    def create_topic_with_assignment(self, name: str,
                                     assignments: Sequence[Sequence[int]]):
        client = KafkaCliTools(self._redpanda)
        client.create_topic_with_assignment(name, assignments)

    def delete_topic(self, name):
        client = KafkaCliTools(self._redpanda)
        client.delete_topic(name)

    def describe_topics(self, topics=None):
        """
        Describe topics. Pass topics=None to describe all topics, or a pass a
        list of topic names to restrict the call to a set of specific topics.
        """
        def make_partition_desc(d):
            return PartitionDescription(id=d['partition'],
                                        leader=d['leader'],
                                        replicas=d['replicas'])

        def make_topic_desc(d):
            partitions = [make_partition_desc(d) for d in d['partitions']]
            return TopicDescription(name=d['topic'], partitions=partitions)

        client = KafkaAdminClient(
            bootstrap_servers=self._redpanda.brokers_list(),
            **self._redpanda.security_config())
        res = client.describe_topics(topics)
        return [make_topic_desc(d) for d in res]

    def describe_topic(self, topic: str):
        td = self.describe_topics([topic])
        assert len(td) == 1, f"Received {len(td)} topics expected 1: {td}"
        assert td[
            0].name == topic, f"Received topic {td[0].name} expected {topic}: {td}"
        return td[0]

    def alter_topic_partition_count(self, topic: str, count: int):
        client = KafkaCliTools(self._redpanda)
        client.create_topic_partitions(topic, count)

    def alter_topic_config(self, topic: str, key: str,
                           value: typing.Union[str, int]):
        """
        Alter a topic configuration property.
        """
        rpk = RpkTool(self._redpanda)
        rpk.alter_topic_config(topic, key, value)

    def alter_topic_configs(self, topic: str,
                            props: dict[str, typing.Union[str, int]]):
        """
        Alter multiple topic configuration properties.
        """
        kafka_tools = KafkaCliTools(self._redpanda)
        kafka_tools.alter_topic_config(topic, props)

    def describe_topic_configs(self, topic: str):
        rpk = RpkTool(self._redpanda)
        configs = rpk.describe_topic_configs(topic)
        return {
            key: TopicConfigValue(value=value[0], source=value[1])
            for key, value in configs.items()
        }

    def delete_topic_config(self, topic: str, key: str):
        rpk = RpkTool(self._redpanda)
        rpk.delete_topic_config(topic, key)

    def alter_broker_config(self,
                            values: dict[str, typing.Any],
                            incremental: bool,
                            *,
                            broker: typing.Optional[int] = None):
        kcl = KCL(self._redpanda)
        return kcl.alter_broker_config(values, incremental, broker)

    def delete_broker_config(self, keys: list[str], incremental: bool):
        kcl = KCL(self._redpanda)
        return kcl.delete_broker_config(keys, incremental)
