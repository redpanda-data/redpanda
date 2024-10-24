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
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception
from ducktape.mark import parametrize, ignore
from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.kcl import RawKCL
from ducktape.utils.util import wait_until


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
        return cls(key=QuotaValueType(d['key']), values=d['value'])


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
        self.kcl = RawKCL(self.redpanda)
        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=1)
    def test_kafka_configs(self):
        def normalize(output):
            return [line.strip() for line in output.strip().split('\n')]

        def assert_outputs_equal(out, expected):
            self._assert_equal(normalize(out), normalize(expected))

        self.redpanda.logger.debug("Create a config for client default")
        self.kafka_cli.alter_quota_config(
            "--entity-type clients --entity-default",
            to_add={"consumer_byte_rate": 10240.0})

        self.redpanda.logger.debug("Create a config for a specific client")
        self.kafka_cli.alter_quota_config(
            "--entity-type clients --entity-name custom-producer",
            to_add={"producer_byte_rate": 20480.0})

        self.redpanda.logger.debug(
            "Check describe filtering works for a default match")
        out = self.kafka_cli.describe_quota_config("--client-defaults")
        expected = "Quota configs for the default client-id are consumer_byte_rate=10240.0"
        assert_outputs_equal(out, expected)

        self.redpanda.logger.debug("Check specific match filtering works")
        out = self.kafka_cli.describe_quota_config(
            "--entity-type clients --entity-name custom-producer")
        expected = "Quota configs for client-id 'custom-producer' are producer_byte_rate=20480.0"
        assert_outputs_equal(out, expected)

        out = self.kafka_cli.describe_quota_config(
            "--entity-type clients --entity-name unknown-producer")
        expected = ""
        assert_outputs_equal(out, expected)

        self.redpanda.logger.debug("Check any match filtering works")
        out = self.kafka_cli.describe_quota_config("--entity-type clients")
        expected = """Quota configs for the default client-id are consumer_byte_rate=10240.0
Quota configs for client-id 'custom-producer' are producer_byte_rate=20480.0"""
        assert_outputs_equal(out, expected)

        self.redpanda.logger.debug("Check deleting quotas works")
        self.kafka_cli.alter_quota_config(
            "--entity-type clients --entity-default",
            to_remove=["consumer_byte_rate"])
        self.kafka_cli.alter_quota_config(
            "--entity-type clients --entity-name custom-producer",
            to_remove=["producer_byte_rate"])

        out = self.kafka_cli.describe_quota_config("--entity-type clients")
        expected = ""
        assert_outputs_equal(out, expected)

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

    @cluster(num_nodes=1)
    def test_error_handling(self):
        # rpk has client-side validation for the supported types,
        # so use other clients to exercise unsupported types
        self.redpanda.logger.debug(
            "Verify the error message of user quotas (not yet supported)")
        with expect_kafka_cli_error_msg(
                "Entity type 'user' not yet supported"):
            self.kafka_cli.describe_quota_config("--user-defaults")

        self.redpanda.logger.debug(
            "Verify that the default for client-id-prefix is not supported with alter"
        )
        alter_body = {
            "Entries": [{
                "Entity": [{
                    "Type": "client-id-prefix",
                }],
                "Ops": [{
                    "Key": "producer_byte_rate",
                    "Value": 10.0,
                }],
            }],
        }
        res = self.kcl.raw_alter_quotas(alter_body)
        assert len(res['Entries']) == 1, f"Unexpected entries: {res}"
        entry = res['Entries'][0]
        assert entry['ErrorCode'] == 42, \
            f"Unexpected entry: {entry}"
        assert entry['ErrorMessage'] == "Invalid quota entity type, client-id-prefix entity should not be used at the default level (use client-id default instead).", \
            f"Unexpected entry: {entry}"

        self.redpanda.logger.debug(
            "Verify that the default for client-id-prefix is not supported with describe"
        )
        describe_body = {
            "Components": [{
                "EntityType": "client-id-prefix",
                "MatchType": 1,  # default
            }],
        }
        res = self.kcl.raw_describe_quotas(describe_body)
        assert res['ErrorCode'] == 42, \
            f"Unexpected response: {res}"
        assert res['ErrorMessage'] == "Invalid quota entity type, client-id-prefix entity should not be used at the default level (use client-id default instead).", \
            f"Unexpected response: {res}"

        self.redpanda.logger.debug(
            "Verify that Exact match without a match field results in an error"
        )
        describe_body = {
            "Components": [{
                "EntityType": "client-id",
                "MatchType": 0,  # exact match
                # "Match": "missing"
            }],
        }
        res = self.kcl.raw_describe_quotas(describe_body)
        assert res['ErrorCode'] == 42, \
            f"Unexpected response: {res}"
        assert res['ErrorMessage'] == "Unspecified match field for exact_name match type", \
            f"Unexpected response: {res}"

        self.redpanda.logger.debug(
            "Verify that it is possible for alter to partially succeed")
        alter_body = {
            "Entries": [
                {
                    "Entity": [{
                        "Type": "client-id",
                    }],
                    "Ops": [{
                        "Key": "producer_byte_rate",
                        "Value": 10.0,
                    }],
                },
                {
                    "Entity": [{
                        "Type": "user",  # Not yet supported
                    }],
                    "Ops": [{
                        "Key": "producer_byte_rate",
                        "Value": 10.0,
                    }],
                },
            ],
        }
        res = self.kcl.raw_alter_quotas(alter_body)
        assert len(res['Entries']) == 2, f"Unexpected entries: {res}"
        assert res['Entries'][0]['ErrorCode'] == 0, \
            f"Unexpected response: {res}"
        assert res['Entries'][1]['ErrorCode'] == 35, \
            f"Unexpected response: {res}"
        got = self.describe(default=["client-id"])
        expected = QuotaOutput([
            Quota(entity=QuotaEntity.client_id_default(),
                  values=[QuotaValue.producer_byte_rate("10")])
        ])
        self._assert_equal(got, expected)

    @cluster(num_nodes=3)
    def test_multi_node(self):
        self.redpanda.logger.debug(
            "Wait for controller leader to be ready and select a non-leader node"
        )
        leader_node = self.redpanda.get_node(
            self.admin.await_stable_leader(topic="controller",
                                           partition=0,
                                           namespace="redpanda",
                                           timeout_s=30))
        non_leader_node = next(
            filter(lambda node: node != leader_node, self.redpanda.nodes))

        self.redpanda.logger.debug(f"Found leader node: {leader_node.name}")
        self.redpanda.logger.debug(
            f"Issuing an alter request to a non-leader ({non_leader_node.name}) and "
            "expecting that it is redirected to the leader internally")
        self.alter(default=["client-id"],
                   add=["producer_byte_rate=1111"],
                   node=non_leader_node)

        def describe_shows_default():
            got = self.describe(default=["client-id"])
            expected = QuotaOutput([
                Quota(entity=QuotaEntity.client_id_default(),
                      values=[QuotaValue.producer_byte_rate("1111")])
            ])
            self._assert_equal(got, expected)
            return True

        self.redpanda.logger.debug(
            "Waiting until describe shows the newly added quota")
        wait_until(describe_shows_default,
                   timeout_sec=30,
                   retry_on_exc=True,
                   err_msg=f"Describe did not succeed in time")
