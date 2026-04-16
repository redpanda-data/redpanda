# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
from enum import Enum
from functools import total_ordering
from typing import Any, NamedTuple

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from typing_extensions import Self

from rptest.clients.kafka_cli_tools import KafkaCliTools, KafkaCliToolsError
from rptest.clients.kcl import RawKCL
from rptest.clients.rpk import RpkException, RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    RESTART_LOG_ALLOW_LIST,
    ClusterNode,
    LoggingConfig,
    SISettings,
)
from rptest.services.redpanda_installer import (
    InstallOptions,
    RedpandaInstaller,
    RedpandaVersionTriple,
    wait_for_num_versions,
)
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception, wait_until_result

log_config = LoggingConfig(
    "info",
    logger_levels={
        "kafka": "trace",
        "kafka_quotas": "trace",
    },
)


def expect_kafka_cli_error_msg(error_msg: str):
    return expect_exception(KafkaCliToolsError, lambda e: error_msg in e.output)


def expect_rpk_error_msg(error_msg: str):
    return expect_exception(RpkException, lambda e: error_msg in e.stderr)


class QuotaEntityType(Enum):
    USER = "user"
    CLIENT_ID = "client-id"
    CLIENT_ID_PREFIX = "client-id-prefix"

    def _get_ordering(self):
        return {name: idx for idx, name in enumerate(self.__class__)}

    def __lt__(self, other: Self):
        ordering = self._get_ordering()
        return ordering[self] < ordering[other]


class QuotaEntityPart(NamedTuple):
    type: QuotaEntityType
    name: str

    Dict = dict[str, str]

    @classmethod
    def from_dict(cls, d: Dict):
        return cls(name=d["name"], type=QuotaEntityType(d["type"]))


class QuotaEntity(NamedTuple):
    parts: list[QuotaEntityPart]

    @staticmethod
    def user_default():
        return QuotaEntity(
            [QuotaEntityPart(name="<default>", type=QuotaEntityType.USER)]
        )

    @staticmethod
    def user(name: str):
        return QuotaEntity([QuotaEntityPart(name=name, type=QuotaEntityType.USER)])

    @staticmethod
    def client_id_default():
        return QuotaEntity(
            [QuotaEntityPart(name="<default>", type=QuotaEntityType.CLIENT_ID)]
        )

    @staticmethod
    def client_id(name: str):
        return QuotaEntity([QuotaEntityPart(name=name, type=QuotaEntityType.CLIENT_ID)])

    @staticmethod
    def client_id_prefix(name: str):
        return QuotaEntity(
            [QuotaEntityPart(name=name, type=QuotaEntityType.CLIENT_ID_PREFIX)]
        )

    @staticmethod
    def client_id_default_and_user_default():
        return QuotaEntity(
            [
                QuotaEntityPart(name="<default>", type=QuotaEntityType.USER),
                QuotaEntityPart(name="<default>", type=QuotaEntityType.CLIENT_ID),
            ]
        )

    List = list[QuotaEntityPart.Dict]

    @classmethod
    def from_list(cls, l: List):
        parts = [QuotaEntityPart.from_dict(part) for part in l]
        # sorted for determinism
        return cls(parts=sorted(parts))


@total_ordering
class QuotaValueType(Enum):
    PRODUCER_BYTE_RATE = "producer_byte_rate"
    CONSUMER_BYTE_RATE = "consumer_byte_rate"
    CONTROLLER_MUTATION_RATE = "controller_mutation_rate"

    def __lt__(self, other: Self):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


class QuotaValue(NamedTuple):
    key: QuotaValueType
    values: str

    @staticmethod
    def producer_byte_rate(value: str):
        return QuotaValue(QuotaValueType.PRODUCER_BYTE_RATE, value)

    @staticmethod
    def consumer_byte_rate(value: str):
        return QuotaValue(QuotaValueType.CONSUMER_BYTE_RATE, value)

    @staticmethod
    def controller_mutation_rate(value: str):
        return QuotaValue(QuotaValueType.CONTROLLER_MUTATION_RATE, value)

    @classmethod
    def from_dict(cls, d: dict[Any, Any]):
        return cls(key=QuotaValueType(d["key"]), values=d["value"])


class Quota(NamedTuple):
    entity: QuotaEntity
    values: list[QuotaValue]

    Dict = dict[str, QuotaEntity.List]

    @classmethod
    def from_dict(cls, d: Dict) -> Self:
        entity = QuotaEntity.from_list(d["entity"])
        # sorted for determinism
        values = sorted([QuotaValue.from_dict(value) for value in d["values"]])
        return cls(entity=entity, values=values)


class QuotaOutput(NamedTuple):
    quotas: list[Quota]

    @classmethod
    def from_dict(cls, d: dict[str, list[Quota.Dict]]) -> Self:
        if not d.get("quotas"):
            return cls(quotas=[])
        # sorted for determinism
        quotas = sorted([Quota.from_dict(quota) for quota in d["quotas"]])
        return cls(quotas=quotas)

    @classmethod
    def from_json(cls, out: str) -> Self:
        return cls.from_dict(json.loads(out))


class QuotaManagementUtils:
    def describe(self, *args: Any, **kwargs: Any) -> QuotaOutput:
        res = self.rpk.describe_cluster_quotas(*args, **kwargs)
        return QuotaOutput.from_dict(res)

    def alter(self, *args: Any, with_retries: bool = False, **kwargs: Any):
        if with_retries:
            wait_until(
                lambda: self.rpk.alter_cluster_quotas(*args, **kwargs)["status"]
                == "OK",
                timeout_sec=30,
                backoff_sec=1,
                err_msg="failed to run rpk.alter_cluster_quotas",
            )
        else:
            res = self.rpk.alter_cluster_quotas(*args, **kwargs)
            assert res["status"] == "OK", f"Alter failed with result: {res}"

    @staticmethod
    def _assert_equal(got: Any, expected: Any):
        assert got == expected, f"Mismatch.\n\tGot:\t\t{got}\n\tExpected:\t{expected}"


class QuotaManagementTest(RedpandaTest, QuotaManagementUtils):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, log_config=log_config, **kwargs)

        self.rpk = RpkTool(self.redpanda)
        self.kafka_cli = KafkaCliTools(self.redpanda)
        self.kcl = RawKCL(self.redpanda)
        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=1)
    def test_kafka_configs(self):
        def normalize(output: str):
            return [line.strip() for line in output.strip().split("\n")]

        def assert_outputs_equal(out: str, expected: str):
            self._assert_equal(normalize(out), normalize(expected))

        self.logger.debug("Create a config for client default")
        self.kafka_cli.alter_quota_config(
            "--entity-type clients --entity-default",
            to_add={"consumer_byte_rate": 10240.0},
        )

        self.logger.debug("Create a config for a specific client")
        self.kafka_cli.alter_quota_config(
            "--entity-type clients --entity-name custom-producer",
            to_add={"producer_byte_rate": 20480.0},
        )

        self.logger.debug("Check describe filtering works for a default match")
        out = self.kafka_cli.describe_quota_config("--client-defaults")
        expected = (
            "Quota configs for the default client-id are consumer_byte_rate=10240.0"
        )
        assert_outputs_equal(out, expected)

        self.logger.debug("Check specific match filtering works")
        out = self.kafka_cli.describe_quota_config(
            "--entity-type clients --entity-name custom-producer"
        )
        expected = "Quota configs for client-id 'custom-producer' are producer_byte_rate=20480.0"
        assert_outputs_equal(out, expected)

        out = self.kafka_cli.describe_quota_config(
            "--entity-type clients --entity-name unknown-producer"
        )
        expected = ""
        assert_outputs_equal(out, expected)

        self.logger.debug("Check any match filtering works")
        out = self.kafka_cli.describe_quota_config("--entity-type clients")
        expected = """Quota configs for the default client-id are consumer_byte_rate=10240.0
Quota configs for client-id 'custom-producer' are producer_byte_rate=20480.0"""
        assert_outputs_equal(out, expected)

        self.logger.debug("Check deleting quotas works")
        self.kafka_cli.alter_quota_config(
            "--entity-type clients --entity-default", to_remove=["consumer_byte_rate"]
        )
        self.kafka_cli.alter_quota_config(
            "--entity-type clients --entity-name custom-producer",
            to_remove=["producer_byte_rate"],
        )

        out = self.kafka_cli.describe_quota_config("--entity-type clients")
        expected = ""
        assert_outputs_equal(out, expected)

        self.logger.debug("Create a config for user default")
        self.kafka_cli.alter_quota_config(
            "--entity-type users --entity-default",
            to_add={"consumer_byte_rate": 10241.0},
        )

        self.logger.debug("Create a config for a specific user")
        self.kafka_cli.alter_quota_config(
            "--entity-type users --entity-name custom-user",
            to_add={"producer_byte_rate": 20481.0},
        )

        self.logger.debug("Check describe filtering works for a default user match")
        out = self.kafka_cli.describe_quota_config("--user-defaults")
        expected = "Quota configs for the default user-principal are consumer_byte_rate=10241.0"
        assert_outputs_equal(out, expected)

        self.logger.debug("Check specific match filtering works")
        out = self.kafka_cli.describe_quota_config(
            "--entity-type users --entity-name custom-user"
        )
        expected = "Quota configs for user-principal 'custom-user' are producer_byte_rate=20481.0"
        assert_outputs_equal(out, expected)

        self.logger.debug("Create a config for specific client, specific user")
        self.kafka_cli.alter_quota_config(
            "--user custom-user-2 --client test-client",
            to_add={"producer_byte_rate": 20482.0},
        )

        self.logger.debug("Check specific match filtering works")
        out = self.kafka_cli.describe_quota_config(
            "--user custom-user-2 --client test-client"
        )
        expected = "Quota configs for user-principal 'custom-user-2', client-id 'test-client' are producer_byte_rate=20482.0"
        assert_outputs_equal(out, expected)

        self.logger.debug("Create a config for default client, specific user")
        self.kafka_cli.alter_quota_config(
            "--entity-type users --entity-name custom-user-3 --entity-type clients --entity-default",
            to_add={"producer_byte_rate": 20483.0},
        )

        self.logger.debug("Check specific match filtering works")
        out = self.kafka_cli.describe_quota_config(
            "--entity-type users --entity-name custom-user-3 --entity-type clients --entity-default"
        )
        expected = "Quota configs for user-principal 'custom-user-3', the default client-id are producer_byte_rate=20483.0"
        assert_outputs_equal(out, expected)

    @cluster(num_nodes=1)
    def test_describe_default(self):
        self.logger.debug(
            "Check that initially describe with default returns no results"
        )
        got = self.describe(default=["client-id"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

        self.logger.debug("Add a default quota and verify that describe returns it")
        self.alter(default=["client-id"], add=["producer_byte_rate=1111"])
        got = self.describe(default=["client-id"])
        expected = QuotaOutput(
            [
                Quota(
                    entity=QuotaEntity.client_id_default(),
                    values=[QuotaValue.producer_byte_rate("1111")],
                )
            ]
        )
        self._assert_equal(got, expected)

        self.logger.debug(
            "Add two exact match quotas and verify that describe with default match type doesn't return them"
        )
        self.alter(name=["client-id=a-consumer"], add=["consumer_byte_rate=2222"])
        self.alter(
            name=["client-id-prefix=admins-"], add=["controller_mutation_rate=3333"]
        )
        got = self.describe(default=["client-id"])
        expected = expected
        self._assert_equal(got, expected)

        self.logger.debug(
            "Delete the default quota and verify that describe doesn't return it anymore"
        )
        self.alter(default=["client-id"], delete=["producer_byte_rate"])
        got = self.describe(default=["client-id"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

        self.logger.debug(
            "Delete the non-default quotas and verify that describe still returns nothing"
        )
        self.alter(name=["client-id=a-consumer"], delete=["consumer_byte_rate"])
        self.alter(
            name=["client-id-prefix=admins-"], delete=["controller_mutation_rate"]
        )
        got = self.describe(default=["client-id"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

    @cluster(num_nodes=1)
    def test_describe_any(self):
        self.logger.debug("Check that initially describe with any returns no results")
        got = self.describe(any=["client-id"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

        self.logger.debug(
            "Add some client-id and client-id-prefix quotas and verify that any with client-id only returns client-id quotas, and any with client-id-prefix only returns client-id-prefix quotas."
        )
        self.alter(name=["client-id=a-consumer"], add=["consumer_byte_rate=2222"])
        self.alter(
            name=["client-id-prefix=admins-"], add=["controller_mutation_rate=3333"]
        )
        self.alter(name=["client-id=a-producer"], add=["producer_byte_rate=4444"])
        got = self.describe(any=["client-id"])
        expected = QuotaOutput(
            [
                Quota(
                    entity=QuotaEntity.client_id("a-consumer"),
                    values=[QuotaValue.consumer_byte_rate("2222")],
                ),
                Quota(
                    entity=QuotaEntity.client_id("a-producer"),
                    values=[QuotaValue.producer_byte_rate("4444")],
                ),
            ]
        )
        self._assert_equal(got, expected)

        got = self.describe(any=["client-id-prefix"])
        expected = QuotaOutput(
            [
                Quota(
                    entity=QuotaEntity.client_id_prefix("admins-"),
                    values=[QuotaValue.controller_mutation_rate("3333")],
                )
            ]
        )
        self._assert_equal(got, expected)

        self.logger.debug(
            "Delete the client-id quotas and verify that any no longer returns them"
        )
        self.alter(name=["client-id=a-consumer"], delete=["consumer_byte_rate"])
        self.alter(name=["client-id=a-producer"], delete=["producer_byte_rate"])
        got = self.describe(default=["client-id"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

    @cluster(num_nodes=1)
    def test_describe_name(self):
        self.logger.debug("Check that initially describe with name returns no results")
        got = self.describe(name=["client-id=a-consumer"])
        expected = QuotaOutput([])
        self._assert_equal(got, expected)

        self.logger.debug(
            "Add an exact match client id and check that filtering for it with name returns it"
        )
        self.alter(name=["client-id=a-consumer"], add=["consumer_byte_rate=2222"])
        got = self.describe(name=["client-id=a-consumer"])
        expected = QuotaOutput(
            [
                Quota(
                    entity=QuotaEntity.client_id("a-consumer"),
                    values=[QuotaValue.consumer_byte_rate("2222")],
                )
            ]
        )
        self._assert_equal(got, expected)

        self.logger.debug(
            "Add quotas with other names and entity types and verify that we can search for each with name independently"
        )
        self.alter(
            name=["client-id-prefix=admins-"], add=["controller_mutation_rate=3333"]
        )
        self.alter(name=["client-id=a-producer"], add=["producer_byte_rate=4444"])
        got = self.describe(name=["client-id=a-consumer"])
        expected = expected  # Same as before
        self._assert_equal(got, expected)

        got = self.describe(name=["client-id=a-producer"])
        expected = QuotaOutput(
            [
                Quota(
                    entity=QuotaEntity.client_id("a-producer"),
                    values=[QuotaValue.producer_byte_rate("4444")],
                )
            ]
        )
        self._assert_equal(got, expected)

        got = self.describe(name=["client-id-prefix=admins-"])
        expected = QuotaOutput(
            [
                Quota(
                    entity=QuotaEntity.client_id_prefix("admins-"),
                    values=[QuotaValue.controller_mutation_rate("3333")],
                )
            ]
        )
        self._assert_equal(got, expected)

        self.logger.debug(
            "Remove all the quotas and verify that none of the previous describes with name return anything"
        )
        self.alter(name=["client-id=a-consumer"], delete=["consumer_byte_rate"])
        self.alter(name=["client-id=a-producer"], delete=["producer_byte_rate"])
        self.alter(
            name=["client-id-prefix=admins-"], delete=["controller_mutation_rate"]
        )
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
    def test_multiple_quotas_same_key(self, strict: bool):
        self.logger.debug(
            "Verify that alter and describe work with multiple quota values for the same key (regardless of strict mode)"
        )
        self.alter(
            default=["client-id"],
            add=["consumer_byte_rate=1111", "producer_byte_rate=2222"],
        )
        got = self.describe(default=["client-id"], strict=strict)
        expected = QuotaOutput(
            [
                Quota(
                    entity=QuotaEntity.client_id_default(),
                    values=[
                        QuotaValue.consumer_byte_rate("1111"),
                        QuotaValue.producer_byte_rate("2222"),
                    ],
                )
            ]
        )
        self._assert_equal(got, expected)

    @cluster(num_nodes=1)
    @parametrize(strict=False)
    @parametrize(strict=True)
    def test_describe_multiple_components(self, strict: bool):
        self.logger.debug(
            "Verify that describe rejects multiple filter components of the same type (client/user/ip)"
        )

        with expect_rpk_error_msg("INVALID_REQUEST"):
            self.describe(any=["client-id", "client-id-prefix"], strict=strict)

        with expect_rpk_error_msg("INVALID_REQUEST"):
            self.describe(any=["user", "user"], strict=strict)

        with expect_rpk_error_msg("INVALID_REQUEST"):
            self.describe(any=["user", "client-id", "client-id"], strict=strict)

        self.alter(default=["client-id", "user"], add=["producer_byte_rate=2222"])

        got = self.describe(any=["client-id", "user"], strict=strict)
        compound_output = QuotaOutput(
            [
                Quota(
                    entity=QuotaEntity.client_id_default_and_user_default(),
                    values=[QuotaValue.producer_byte_rate("2222")],
                )
            ]
        )
        self._assert_equal(got, compound_output)

        got = self.describe(any=["client-id"], strict=strict)
        expected = QuotaOutput([]) if strict else compound_output
        self._assert_equal(got, expected)

        got = self.describe(any=["user"], strict=strict)
        expected = QuotaOutput([]) if strict else compound_output
        self._assert_equal(got, expected)

    @cluster(num_nodes=1)
    def test_error_handling(self):
        # rpk has client-side validation for the supported types,
        # so use other clients to exercise unsupported types
        self.logger.debug(
            "Verify that the default for client-id-prefix is not supported with alter"
        )
        alter_body = {
            "Entries": [
                {
                    "Entity": [
                        {
                            "Type": "client-id-prefix",
                        }
                    ],
                    "Ops": [
                        {
                            "Key": "producer_byte_rate",
                            "Value": 10.0,
                        }
                    ],
                }
            ],
        }
        res = self.kcl.raw_alter_quotas(alter_body)
        assert len(res["Entries"]) == 1, f"Unexpected entries: {res}"
        entry = res["Entries"][0]
        assert entry["ErrorCode"] == 42, f"Unexpected entry: {entry}"
        assert (
            entry["ErrorMessage"]
            == "Invalid quota entity type, client-id-prefix entity should not be used at the default level (use client-id default instead)."
        ), f"Unexpected entry: {entry}"

        self.logger.debug(
            "Verify that the default for client-id-prefix is not supported with describe"
        )
        describe_body = {
            "Components": [
                {
                    "EntityType": "client-id-prefix",
                    "MatchType": "DEFAULT",
                }
            ],
        }
        res = self.kcl.raw_describe_quotas(describe_body)
        assert res["ErrorCode"] == 42, f"Unexpected response: {res}"
        assert (
            res["ErrorMessage"]
            == "Invalid quota entity type, client-id-prefix entity should not be used at the default level (use client-id default instead)."
        ), f"Unexpected response: {res}"

        self.logger.debug(
            "Verify that Exact match without a match field results in an error"
        )
        describe_body = {
            "Components": [
                {
                    "EntityType": "client-id",
                    "MatchType": "EXACT",
                    # "Match": "missing"
                }
            ],
        }
        res = self.kcl.raw_describe_quotas(describe_body)
        assert res["ErrorCode"] == 42, f"Unexpected response: {res}"
        assert (
            res["ErrorMessage"] == "Unspecified match field for exact_name match type"
        ), f"Unexpected response: {res}"

        self.logger.debug("Verify that it is possible for alter to partially succeed")
        alter_body = {
            "Entries": [
                {
                    "Entity": [
                        {
                            "Type": "client-id",
                        }
                    ],
                    "Ops": [
                        {
                            "Key": "producer_byte_rate",
                            "Value": 10.0,
                        }
                    ],
                },
                {
                    "Entity": [
                        {
                            "Type": "role",  # Not a valid entity type
                        }
                    ],
                    "Ops": [
                        {
                            "Key": "producer_byte_rate",
                            "Value": 10.0,
                        }
                    ],
                },
            ],
        }
        res = self.kcl.raw_alter_quotas(alter_body)
        assert len(res["Entries"]) == 2, f"Unexpected entries: {res}"
        assert res["Entries"][0]["ErrorCode"] == 0, f"Unexpected response: {res}"
        assert res["Entries"][1]["ErrorCode"] == 42, f"Unexpected response: {res}"
        got = self.describe(default=["client-id"])
        expected = QuotaOutput(
            [
                Quota(
                    entity=QuotaEntity.client_id_default(),
                    values=[QuotaValue.producer_byte_rate("10")],
                )
            ]
        )
        self._assert_equal(got, expected)

        self.logger.debug("Verify that a describe on ip results in an error")
        describe_body = {
            "Components": [
                {
                    "EntityType": "ip",
                    "MatchType": "DEFAULT",
                }
            ],
        }
        res = self.kcl.raw_describe_quotas(describe_body)
        assert res["ErrorCode"] == 35, f"Unexpected response: {res}"
        assert res["ErrorMessage"] == "Entity type 'ip' not yet supported", (
            f"Unexpected response: {res}"
        )

        self.logger.debug(
            "Verify that a describe on a custom entity type results in an error"
        )
        describe_body = {
            "Components": [
                {
                    "EntityType": "bad-entity-type",
                    "MatchType": "DEFAULT",
                }
            ],
        }
        res = self.kcl.raw_describe_quotas(describe_body)
        assert res["ErrorCode"] == 35, f"Unexpected response: {res}"
        assert (
            res["ErrorMessage"] == "Custom entity type 'bad-entity-type' not supported"
        ), f"Unexpected response: {res}"

    @cluster(num_nodes=3)
    def test_multi_node(self):
        self.logger.debug(
            "Wait for controller leader to be ready and select a non-leader node"
        )
        leader_node = self.redpanda.get_node(
            self.admin.await_stable_leader(
                topic="controller", partition=0, namespace="redpanda", timeout_s=30
            )
        )
        non_leader_node = next(
            filter(lambda node: node != leader_node, self.redpanda.nodes)
        )

        self.logger.debug(f"Found leader node: {leader_node.name}")
        self.logger.debug(
            f"Issuing an alter request to a non-leader ({non_leader_node.name}) and "
            "expecting that it is redirected to the leader internally"
        )
        self.alter(
            default=["client-id"], add=["producer_byte_rate=1111"], node=non_leader_node
        )

        def describe_shows_default():
            got = self.describe(default=["client-id"])
            expected = QuotaOutput(
                [
                    Quota(
                        entity=QuotaEntity.client_id_default(),
                        values=[QuotaValue.producer_byte_rate("1111")],
                    )
                ]
            )
            self._assert_equal(got, expected)
            return True

        self.logger.debug("Waiting until describe shows the newly added quota")
        wait_until(
            describe_shows_default,
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Describe did not succeed in time",
        )


class QuotaManagementUpgradeTest(EndToEndTest, QuotaManagementUtils):
    """
    Verify that user quota clients work as expected during an upgrade
    """

    def __init__(self, test_context):
        super().__init__(test_context=test_context)

    def alter_quotas(self, body: dict[str, Any], node: ClusterNode | None = None):
        return wait_until_result(
            lambda: (True, self.kcl.raw_alter_quotas(body, node=node)),
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="Failed to get a non_throwing alter quotas request",
        )

    def transfer_leadership(self, new_leader: ClusterNode):
        """
        Request leadership transfer of the controller and check that it completes successfully.
        """
        self.logger.debug(
            f"Request transferring leadership to {new_leader.account.hostname}"
        )
        target_id = self.redpanda.node_id(new_leader, force_refresh=True)

        admin = Admin(self.redpanda)

        leader_id = admin.await_stable_leader(
            namespace="redpanda", topic="controller", partition=0
        )
        self.logger.debug(f"Current leader {leader_id}")

        if leader_id == target_id:
            return

        self.logger.debug(f"Transferring leadership to {new_leader.account.hostname}")
        admin.transfer_leadership_to(
            namespace="redpanda", topic="controller", partition=0, target_id=target_id
        )
        admin.await_stable_leader(
            namespace="redpanda", topic="controller", check=lambda id: id == target_id
        )

    @cluster(num_nodes=2, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_upgrade(self):
        # user_based_client_quota feature introduced in v26.1, start on v25.3
        from_version = (25, 3, 1)
        to_version = RedpandaInstaller.next_major_version(from_version[0:2])

        install_opts = InstallOptions(version=RedpandaVersionTriple(from_version))
        self.start_redpanda(
            num_nodes=2,
            si_settings=SISettings(test_context=self.test_context),
            install_opts=install_opts,
        )

        self.kcl = RawKCL(self.redpanda)

        first_node = self.redpanda.nodes[0]
        second_node = self.redpanda.nodes[1]

        alter_user_quota_body = {
            "Entries": [
                {
                    "Entity": [
                        {
                            "Type": "user",
                        }
                    ],
                    "Ops": [
                        {
                            "Key": "producer_byte_rate",
                            "Value": 10.0,
                        }
                    ],
                }
            ],
        }

        # Sanity check: v25.3 doesn't support user quotas
        self.logger.debug("Verify that user quotas are not supported in older version")
        res = self.alter_quotas(alter_user_quota_body)
        assert len(res["Entries"]) == 1, f"Unexpected entries: {res}"
        entry = res["Entries"][0]
        assert entry["ErrorCode"] == 35, f"Unexpected entry: {entry}"
        assert entry["ErrorMessage"] == "Entity type 'user' not yet supported", (
            f"Unexpected entry: {entry}"
        )

        # Upgrade one node to the next version.
        self.redpanda._installer.install(self.redpanda.nodes, to_version)
        self.redpanda.restart_nodes([first_node])
        wait_for_num_versions(self.redpanda, 2)

        # Ensure the controller is on the upgraded node so that we can verify
        # the behavior of user quotas during the upgrade
        self.transfer_leadership(first_node)

        self.logger.debug("Verify that during upgrade user quotas are disabled")
        res = self.alter_quotas(alter_user_quota_body)
        assert len(res["Entries"]) == 1, f"Unexpected entries: {res}"
        entry = res["Entries"][0]
        assert entry["ErrorCode"] == 35, f"Unexpected entry: {entry}"
        assert (
            entry["ErrorMessage"] == "user-based client quotas are not yet available"
        ), f"Unexpected entry: {entry}"

        self.redpanda.restart_nodes([second_node])
        wait_for_num_versions(self.redpanda, 1)
        self.redpanda.await_feature("user_based_client_quota", "active", timeout_sec=30)

        self.logger.debug("Verify that user quotas are now enabled")
        res = self.alter_quotas(alter_user_quota_body, node=second_node)
        assert len(res["Entries"]) == 1, f"Unexpected entries: {res}"
        entry = res["Entries"][0]
        assert entry["ErrorCode"] == 0, f"Unexpected entry: {entry}"

    @cluster(num_nodes=2, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_quotas_during_upgrade(self):
        # client-id quotas predate v25.3, test they survive the upgrade
        from_version = (25, 3, 1)
        to_version = RedpandaInstaller.next_major_version(from_version[0:2])

        install_opts = InstallOptions(version=RedpandaVersionTriple(from_version))
        self.start_redpanda(
            num_nodes=2,
            si_settings=SISettings(test_context=self.test_context),
            install_opts=install_opts,
        )

        self.rpk = RpkTool(self.redpanda)

        first_node = self.redpanda.nodes[0]
        second_node = self.redpanda.nodes[1]

        self.logger.debug("Add a default quota and two exact ones as a base setup")
        self.alter(default=["client-id"], add=["producer_byte_rate=1111"])
        self.alter(name=["client-id=a-consumer"], add=["consumer_byte_rate=2222"])
        self.alter(
            name=["client-id-prefix=admins-"], add=["controller_mutation_rate=3333"]
        )

        def check_all_nodes(entity_type: str, expected_quota: Quota):
            for n in self.redpanda.nodes:
                self.logger.debug(f"Waiting for quota '{expected_quota}' in node {n}")

                def quota_in_node():
                    got = self.describe(any=[entity_type])
                    self.logger.debug(f"describe result: {got}")
                    return expected_quota in got.quotas

                wait_until(
                    quota_in_node,
                    timeout_sec=5,
                    err_msg=f"Could not find expected quota '{expected_quota}'",
                )

        default_quota = Quota(
            entity=QuotaEntity.client_id_default(),
            values=[QuotaValue.producer_byte_rate("1111")],
        )
        consumer_a_quota = Quota(
            entity=QuotaEntity.client_id("a-consumer"),
            values=[QuotaValue.consumer_byte_rate("2222")],
        )
        admins_quota = Quota(
            entity=QuotaEntity.client_id_prefix("admins-"),
            values=[QuotaValue.controller_mutation_rate("3333")],
        )

        all_quotas = {
            "default": ("client-id", default_quota),
            "consumer_a": ("client-id", consumer_a_quota),
            "admins": ("client-id-prefix", admins_quota),
        }

        # Check all quotas exist on every node
        for etype, quota in all_quotas.values():
            check_all_nodes(etype, quota)

        # Upgrade one node to the next version.
        self.redpanda._installer.install(self.redpanda.nodes, to_version)
        self.redpanda.restart_nodes([first_node])
        wait_for_num_versions(self.redpanda, 2)

        # Make some changes in quotas in the new version
        self.transfer_leadership(first_node)

        # First call after restart is run with retries because it takes some time for everything to stabilize
        self.alter(
            with_retries=True,
            default=["client-id"],
            add=["producer_byte_rate=1112"],
            node=first_node,
        )
        self.alter(
            name=["client-id=b-consumer"],
            add=["consumer_byte_rate=2223"],
            node=first_node,
        )
        self.alter(
            name=["client-id-prefix=superuser-"],
            add=["controller_mutation_rate=3334"],
            node=first_node,
        )

        # Make some changes in quotas in the old version
        self.transfer_leadership(second_node)

        self.alter(
            name=["client-id=c-consumer"],
            add=["producer_byte_rate=2224"],
            node=second_node,
        )

        new_default_quota = Quota(
            entity=QuotaEntity.client_id_default(),
            values=[QuotaValue.producer_byte_rate("1112")],
        )
        consumer_b_quota = Quota(
            entity=QuotaEntity.client_id("b-consumer"),
            values=[QuotaValue.consumer_byte_rate("2223")],
        )
        superusers_quota = Quota(
            entity=QuotaEntity.client_id_prefix("superuser-"),
            values=[QuotaValue.controller_mutation_rate("3334")],
        )

        # Overwrite old default
        all_quotas["default"] = ("client-id", new_default_quota)
        # Add new quotas
        all_quotas["consumer_b"] = ("client-id", consumer_b_quota)
        all_quotas["superusers"] = ("client-id-prefix", superusers_quota)

        # Check all quotas exist on every node
        for etype, quota in all_quotas.values():
            check_all_nodes(etype, quota)

        # Upgrade the second node to the head version, as well.
        self.redpanda.restart_nodes([second_node])
        wait_for_num_versions(self.redpanda, 1)

        self.transfer_leadership(second_node)
        # First call after restart is run with retries because it takes some time for everything to stabilize
        self.alter(
            with_retries=True,
            default=["user"],
            add=["producer_byte_rate=1113"],
            node=second_node,
        )

        default_user_quota = Quota(
            entity=QuotaEntity.user_default(),
            values=[QuotaValue.producer_byte_rate("1113")],
        )

        # Add user quota
        all_quotas["default_user"] = ("user", default_user_quota)

        # Check all quotas exist on every node
        for etype, quota in all_quotas.values():
            check_all_nodes(etype, quota)
