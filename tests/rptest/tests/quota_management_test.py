# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from enum import Enum
from functools import total_ordering
import json
from typing import NamedTuple
from rptest.clients.kafka_cli_tools import KafkaCliTools, KafkaCliToolsError
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception


def expect_kafka_cli_error_msg(error_msg: str):
    return expect_exception(KafkaCliToolsError,
                            lambda e: error_msg in e.output)


class QuotaEntityType(Enum):
    CLIENT_ID = "client-id"
    CLIENT_ID_PREFIX = "client-id-prefix"


class QuotaEntityPart(NamedTuple):
    name: str
    type: QuotaEntityType

    @classmethod
    def from_dict(cls, d: dict):
        return cls(name=d['name'], type=QuotaEntityType(d['type']))


class QuotaEntity(NamedTuple):
    parts: list[QuotaEntityPart]

    @staticmethod
    def client_id_default():
        return QuotaEntity(
            [QuotaEntityPart("<default>", QuotaEntityType.CLIENT_ID)])

    @staticmethod
    def client_id(name):
        return QuotaEntity([QuotaEntityPart(name, QuotaEntityType.CLIENT_ID)])

    @staticmethod
    def client_id_prefix(name):
        return QuotaEntity(
            [QuotaEntityPart(name, QuotaEntityType.CLIENT_ID_PREFIX)])

    @classmethod
    def from_list(cls, l: list):
        parts = [QuotaEntityPart.from_dict(part) for part in l]
        # sorted for determinism
        return cls(parts=sorted(parts))


@total_ordering
class QuotaValueType(Enum):
    PRODUCER_BYTE_RATE = "producer_byte_rate"
    CONSUMER_BYTE_RATE = "consumer_byte_rate"
    CONTROLLER_MUTATION_RATE = "controller_mutation_rate"

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


class QuotaValue(NamedTuple):
    key: QuotaValueType
    values: str

    @staticmethod
    def producer_byte_rate(value):
        return QuotaValue(QuotaValueType.PRODUCER_BYTE_RATE, value)

    @staticmethod
    def consumer_byte_rate(value):
        return QuotaValue(QuotaValueType.CONSUMER_BYTE_RATE, value)

    @staticmethod
    def controller_mutation_rate(value):
        return QuotaValue(QuotaValueType.CONTROLLER_MUTATION_RATE, value)

    @classmethod
    def from_dict(cls, d: dict):
        return cls(key=QuotaValueType(d['key']), values=d['values'])


class Quota(NamedTuple):
    entity: QuotaEntity
    values: list[QuotaValue]

    @classmethod
    def from_dict(cls, d: dict):
        entity = QuotaEntity.from_list(d['entity'])
        # sorted for determinism
        values = sorted([QuotaValue.from_dict(value) for value in d['values']])
        return cls(entity=entity, values=values)


class QuotaOutput(NamedTuple):
    quotas: list[Quota]

    @classmethod
    def from_dict(cls, d: dict):
        if not d.get('quotas'):
            return cls(quotas=[])
        # sorted for determinism
        quotas = sorted([Quota.from_dict(quota) for quota in d['quotas']])
        return cls(quotas=quotas)

    @classmethod
    def from_json(cls, out: str):
        return cls.from_dict(json.loads(out))


class QuotaManagementTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.kafka_cli = KafkaCliTools(self.redpanda)

    @cluster(num_nodes=1)
    def test_alter(self):
        self.kafka_cli.alter_quota_config(
            "--entity-type clients --entity-name client_1",
            to_add={"consumer_byte_rate": 10240})

    @cluster(num_nodes=1)
    def test_describe(self):
        self.kafka_cli.describe_quota_config("--client-defaults")
