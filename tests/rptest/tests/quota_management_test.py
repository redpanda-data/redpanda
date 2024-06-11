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
from ducktape.mark import parametrize, ignore
from rptest.clients.rpk import RpkException, RpkTool


def expect_kafka_cli_error_msg(error_msg: str):
    return expect_exception(KafkaCliToolsError,
                            lambda e: error_msg in e.output)


def expect_rpk_error_msg(error_msg: str):
    return expect_exception(RpkException, lambda e: error_msg in e.stderr)


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

        self.rpk = RpkTool(self.redpanda)
        self.kafka_cli = KafkaCliTools(self.redpanda)

    # TODO: write a small test suite against kafka-configs.sh
    @ignore
    @cluster(num_nodes=1)
    def test_kafka_configs(self):
        self.kafka_cli.alter_quota_config(
            "--entity-type clients --entity-name client_1",
            to_add={"consumer_byte_rate": 10240})
        self.kafka_cli.describe_quota_config("--client-defaults")

    def describe(self, *args, **kwargs) -> QuotaOutput:
        res = self.rpk.describe_cluster_quotas(*args, **kwargs)
        return QuotaOutput.from_dict(res)

    def alter(self, *args, **kwargs) -> QuotaOutput:
        res = self.rpk.alter_cluster_quotas(*args, **kwargs)
        assert res['status'] == 'OK', f'Alter failed with result: {res}'

    @staticmethod
    def _assert_equal(got, expected):
        assert got == expected, f"Mismatch.\n\tGot:\t\t{got}\n\tExpected:\t{expected}"

    @cluster(num_nodes=1)
    def test_describe_default(self):
        self.redpanda.logger.debug(
            "Check that initially describe with default returns no results")
        got = self.describe(default=["client-id"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

        self.redpanda.logger.debug(
            "Add a default quota and verify that describe returns it")
        self.alter(default=["client-id"], add=["producer_byte_rate=1111"])
        got = self.describe(default=["client-id"])
        expected = QuotaOutput([
            Quota(entity=QuotaEntity.client_id_default(),
                  values=[QuotaValue.producer_byte_rate("1111")])
        ])
        self._assert_equal(got, expected)

        self.redpanda.logger.debug(
            "Add two exact match quotas and verify that describe with default match type doesn't return them"
        )
        self.alter(name=["client-id=a-consumer"],
                   add=["consumer_byte_rate=2222"])
        self.alter(name=["client-id-prefix=admins-"],
                   add=["controller_mutation_rate=3333"])
        got = self.describe(default=["client-id"])
        expected = expected
        self._assert_equal(got, expected)

        self.redpanda.logger.debug(
            "Delete the default quota and verify that describe doesn't return it anymore"
        )
        self.alter(default=["client-id"], delete=["producer_byte_rate"])
        got = self.describe(default=["client-id"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

        self.redpanda.logger.debug(
            "Delete the non-default quotas and verify that describe still returns nothing"
        )
        self.alter(name=["client-id=a-consumer"],
                   delete=["consumer_byte_rate"])
        self.alter(name=["client-id-prefix=admins-"],
                   delete=["controller_mutation_rate"])
        got = self.describe(default=["client-id"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

    @cluster(num_nodes=1)
    def test_describe_any(self):
        self.redpanda.logger.debug(
            "Check that initially describe with any returns no results")
        got = self.describe(any=["client-id"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

        self.redpanda.logger.debug(
            "Add some client-id and client-id-prefix quotas and verify that any with client-id only returns client-id quotas, and any with client-id-prefix only returns client-id-prefix quotas."
        )
        self.alter(name=["client-id=a-consumer"],
                   add=["consumer_byte_rate=2222"])
        self.alter(name=["client-id-prefix=admins-"],
                   add=["controller_mutation_rate=3333"])
        self.alter(name=["client-id=a-producer"],
                   add=["producer_byte_rate=4444"])
        got = self.describe(any=["client-id"])
        expected = QuotaOutput([
            Quota(entity=QuotaEntity.client_id("a-consumer"),
                  values=[QuotaValue.consumer_byte_rate("2222")]),
            Quota(entity=QuotaEntity.client_id("a-producer"),
                  values=[QuotaValue.producer_byte_rate("4444")])
        ])
        self._assert_equal(got, expected)

        got = self.describe(any=["client-id-prefix"])
        expected = QuotaOutput([
            Quota(entity=QuotaEntity.client_id_prefix("admins-"),
                  values=[QuotaValue.controller_mutation_rate("3333")])
        ])
        self._assert_equal(got, expected)

        self.redpanda.logger.debug(
            "Delete the client-id quotas and verify that any no longer returns them"
        )
        self.alter(name=["client-id=a-consumer"],
                   delete=["consumer_byte_rate"])
        self.alter(name=["client-id=a-producer"],
                   delete=["producer_byte_rate"])
        got = self.describe(default=["client-id"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

    @cluster(num_nodes=1)
    def test_describe_name(self):
        self.redpanda.logger.debug(
            "Check that initially describe with name returns no results")
        got = self.describe(name=["client-id=a-consumer"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

        self.redpanda.logger.debug(
            "Add an exact match client id and check that filtering for it with name returns it"
        )
        self.alter(name=["client-id=a-consumer"],
                   add=["consumer_byte_rate=2222"])
        got = self.describe(name=["client-id=a-consumer"])
        expected = QuotaOutput([
            Quota(entity=QuotaEntity.client_id("a-consumer"),
                  values=[QuotaValue.consumer_byte_rate("2222")])
        ])
        self._assert_equal(got, expected)

        self.redpanda.logger.debug(
            "Add quotas with other names and entity types and verify that we can search for each with name independently"
        )
        self.alter(name=["client-id-prefix=admins-"],
                   add=["controller_mutation_rate=3333"])
        self.alter(name=["client-id=a-producer"],
                   add=["producer_byte_rate=4444"])
        got = self.describe(name=["client-id=a-consumer"])
        expected = expected  # Same as before
        self._assert_equal(got, expected)

        got = self.describe(name=["client-id=a-producer"])
        expected = QuotaOutput([
            Quota(entity=QuotaEntity.client_id("a-producer"),
                  values=[QuotaValue.producer_byte_rate("4444")])
        ])
        self._assert_equal(got, expected)

        got = self.describe(name=["client-id-prefix=admins-"])
        expected = QuotaOutput([
            Quota(entity=QuotaEntity.client_id_prefix("admins-"),
                  values=[QuotaValue.controller_mutation_rate("3333")])
        ])
        self._assert_equal(got, expected)

        self.redpanda.logger.debug(
            "Remove all the quotas and verify that none of the previous describes with name return anything"
        )
        self.alter(name=["client-id=a-consumer"],
                   delete=["consumer_byte_rate"])
        self.alter(name=["client-id=a-producer"],
                   delete=["producer_byte_rate"])
        self.alter(name=["client-id-prefix=admins-"],
                   delete=["controller_mutation_rate"])
        got = self.describe(name=["client-id=a-consumer"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

        got = self.describe(name=["client-id=a-producer"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

        got = self.describe(name=["client-id-prefix=admins-"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

    @cluster(num_nodes=1)
    @parametrize(strict=False)
    @parametrize(strict=True)
    def test_multiple_quotas_same_key(self, strict):
        self.redpanda.logger.debug(
            "Verify that alter and describe work with multiple quota values for the same key (regardless of strict mode)"
        )
        self.alter(default=["client-id"],
                   add=["consumer_byte_rate=1111", "producer_byte_rate=2222"])
        got = self.describe(default=["client-id"], strict=strict)
        expected = QuotaOutput([
            Quota(entity=QuotaEntity.client_id_default(),
                  values=[
                      QuotaValue.consumer_byte_rate("1111"),
                      QuotaValue.producer_byte_rate("2222")
                  ])
        ])
        self._assert_equal(got, expected)

    @cluster(num_nodes=1)
    @parametrize(strict=False)
    @parametrize(strict=True)
    def test_describe_multiple_components(self, strict):
        self.redpanda.logger.debug(
            "Verify that describe rejects multiple filter components of the same type (client/user/ip)"
        )
        with expect_rpk_error_msg("INVALID_REQUEST"):
            self.describe(any=["client-id", "client-id-prefix"], strict=strict)

        # TODO: uncomment this once (user, client) compound keys are supported
        # For now, this is just included to document the expected behaviour of
        # compound keys and the strict field
        # self.alter(default=["client-id", "user"],
        #            add=["producer_byte_rate=2222"])
        #
        # got = self.describe(any=["client-id", "user"], strict=strict)
        # compound_output = QuotaOutput([
        #     Quota(entity=QuotaEntity.client_id_default_and_user_default(),
        #           values=[QuotaValue.producer_byte_rate("2222")])
        # ])
        # self._assert_equal(got, compound_output)
        #
        # got = self.describe(any=["client-id"], strict=strict)
        # expected = QuotaOutput() if strict else compound_output
        # self._assert_equal(got, expected)
        #
        # got = self.describe(any=["user"], strict=strict)
        # expected = QuotaOutput() if strict else compound_output
        # self._assert_equal(got, expected)
