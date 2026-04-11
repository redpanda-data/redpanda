# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import datetime
import json
import random
import re
import socket
import string
import time
import urllib.parse
from enum import IntEnum
from typing import List

import requests
from confluent_kafka import KafkaError, KafkaException
from connectrpc.errors import ConnectError, ConnectErrorCode
from ducktape.cluster.cluster import ClusterNode
from ducktape.errors import TimeoutError
from ducktape.mark import matrix, parametrize
from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from requests.exceptions import HTTPError
from typing_extensions import assert_never

from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    security_pb2,
)
from rptest.clients.admin.proto.redpanda.core.common.v1 import (
    security_types_pb2,
)
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.kafka_cli_tools import KafkaCliTools, KafkaCliToolsError
from rptest.clients.kcl import RawKCL
from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    SaslCredentials,
    SecurityConfig,
    TLSProvider,
)
from rptest.services.tls import Certificate, CertificateAuthority, TLSCertManager
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.sasl_reauth_test import (
    EXPIRATION_METRIC,
    REAUTH_METRIC,
    get_sasl_metrics,
)
from rptest.util import expect_exception, expect_http_error
from rptest.utils.log_utils import wait_until_nag_is_set
from rptest.utils.mode_checks import in_fips_environment
from rptest.utils.utf8 import (
    generate_string_with_control_character,
)

SCRAM_MECHANISM_MAP = {
    "SCRAM_MECHANISM_UNSPECIFIED": security_types_pb2.SCRAM_MECHANISM_UNSPECIFIED,
    "SCRAM-SHA-256": security_types_pb2.SCRAM_MECHANISM_SCRAM_SHA_256,
    "SCRAM-SHA-512": security_types_pb2.SCRAM_MECHANISM_SCRAM_SHA_512,
}


def scram_mechanism_from_string(
    name: str,
) -> security_types_pb2.ScramMechanism:
    try:
        return SCRAM_MECHANISM_MAP[name]
    except KeyError:
        return security_types_pb2.SCRAM_MECHANISM_UNSPECIFIED


class BaseScramTest(RedpandaTest):
    def __init__(self, test_context, **kwargs):
        super(BaseScramTest, self).__init__(test_context, **kwargs)
        self.admin = AdminV2(
            self.redpanda,
            auth=(
                self.redpanda.SUPERUSER_CREDENTIALS.username,
                self.redpanda.SUPERUSER_CREDENTIALS.password,
            ),
        )

    def gen_random_password(self, length):
        return "".join(random.choice(string.ascii_letters) for _ in range(length))

    def update_user(
        self,
        username,
        quote: bool = True,
        password=None,
        expected_status_code=200,
        err_msg=None,
    ):
        if quote:
            username = urllib.parse.quote(username, safe="")
        if password is None:
            password = self.gen_random_password(20)

        controller = self.redpanda.nodes[0]
        url = f"http://{controller.account.hostname}:9644/v1/security/users/{username}"
        data = dict(
            username=username,
            password=password,
            algorithm="SCRAM-SHA-256",
        )
        res = requests.put(url, json=data)

        assert res.status_code == expected_status_code, (
            f"Expected {expected_status_code}, got {res.status_code}: {res.content}"
        )

        if err_msg is not None:
            assert res.json()["message"] == err_msg, (
                f"{res.json()['message']} != {err_msg}"
            )

        return password

    def update_scram_credential_v2(
        self,
        name: str,
        mechanism: security_types_pb2.ScramMechanism,
        password: str,
        expected_error: ConnectErrorCode | None = None,
    ) -> security_pb2.ScramCredential:
        req = security_pb2.UpdateScramCredentialRequest(
            scram_credential=security_pb2.ScramCredential(
                name=name,
                password=password,
                mechanism=mechanism,
            )
        )

        if expected_error is None:
            res = self.admin.security().update_scram_credential(req)
            return res.scram_credential
        else:
            with expect_exception(
                ConnectError,
                lambda e: e.code == expected_error,
            ):
                self.admin.security().update_scram_credential(req)

    def delete_user(self, username, quote: bool = True):
        if quote:
            username = urllib.parse.quote(username, safe="")
        controller = self.redpanda.nodes[0]
        url = f"http://{controller.account.hostname}:9644/v1/security/users/{username}"
        res = requests.delete(url)
        assert res.status_code == 200, (
            f"Status code: {res.status_code} for DELETE user {username}"
        )

    def delete_scram_credential_v2(self, name: str) -> None:
        _ = self.admin.security().delete_scram_credential(
            security_pb2.DeleteScramCredentialRequest(name=name)
        )

    def list_users(self):
        controller = self.redpanda.nodes[0]
        url = f"http://{controller.account.hostname}:9644/v1/security/users"
        res = requests.get(url)
        assert res.status_code == 200
        return res.json()

    def list_scram_credentials_v2(self) -> list[security_pb2.ScramCredential]:
        res = self.admin.security().list_scram_credentials(
            security_pb2.ListScramCredentialsRequest()
        )
        return res.scram_credentials

    def create_user(
        self, username, algorithm, password=None, expected_status_code=200, err_msg=None
    ):
        if password is None:
            password = self.gen_random_password(15)

        controller = self.redpanda.nodes[0]
        url = f"http://{controller.account.hostname}:9644/v1/security/users"
        data = dict(
            username=username,
            password=password,
            algorithm=algorithm,
        )
        self.logger.debug(f"User Creation Arguments: {data}")
        res = requests.post(url, json=data)

        assert res.status_code == expected_status_code, (
            f"Expected {expected_status_code}, got {res.status_code}: {res.content}"
        )

        if err_msg is not None:
            assert res.json()["message"] == err_msg, (
                f"{res.json()['message']} != {err_msg}"
            )

        return password

    def create_scram_credential_v2(
        self,
        name: str,
        mechanism: security_types_pb2.ScramMechanism,
        password: str,
        expected_error: ConnectErrorCode | None = None,
    ) -> security_pb2.ScramCredential | None:
        req = security_pb2.CreateScramCredentialRequest(
            scram_credential=security_pb2.ScramCredential(
                name=name,
                password=password,
                mechanism=mechanism,
            )
        )

        if expected_error is None:
            res = self.admin.security().create_scram_credential(req)
            return res.scram_credential
        else:
            with expect_exception(
                ConnectError,
                lambda e: e.code == expected_error,
            ):
                self.admin.security().create_scram_credential(req)

            return None

    def make_superuser_client(self, password_override=None, algorithm_override=None):
        username, password, algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        password = password_override or password
        algorithm = algorithm_override or algorithm
        return PythonLibrdkafka(
            self.redpanda, username=username, password=password, algorithm=algorithm
        )

    def check_credential_on_all_nodes(
        self, username: str, expected_password_set_at: datetime.datetime | None
    ) -> bool:
        """
        Check that a credential with the given username and expected password_set_at exists on all nodes.

        :returns: true if the credential exists on all nodes
        """
        for node in self.redpanda.nodes:
            self.logger.debug(f"Checking for credential {username} on node {node.name}")
            try:
                cred = self.admin.security(node=node).get_scram_credential(
                    security_pb2.GetScramCredentialRequest(name=username)
                )
                self.logger.debug(f"Got credential {username} on node {node.name}")
                if expected_password_set_at is not None:
                    password_set_at = cred.scram_credential.password_set_at.ToDatetime(
                        tzinfo=datetime.timezone.utc
                    )
                    if password_set_at != expected_password_set_at:
                        self.logger.info(
                            f"Credential {username} on node {node.name} has unexpected password_set_at {password_set_at}, expected {expected_password_set_at}"
                        )
                        return False
            except Exception as e:
                self.logger.warning(
                    f"Error getting credential {username} on node {node.name}: {e}"
                )
                return False

        self.logger.debug(f"Credential {username} found on all nodes")
        return True


class ScramTest(BaseScramTest):
    def __init__(self, test_context):
        security = SecurityConfig()
        security.enable_sasl = True
        super(ScramTest, self).__init__(
            test_context,
            num_brokers=3,
            security=security,
            extra_node_conf={"developer_mode": True},
        )

    @cluster(num_nodes=3)
    @parametrize(alternate_listener=False)
    @parametrize(alternate_listener=True)
    def test_redirects(self, alternate_listener):
        """
        This test is for Admin API redirection functionality in general, but is in this
        test class because managing SCRAM/SASL users is one of the key areas that relies
        on redirection working, so it's a natural fit.
        """

        controller_id = None
        t1 = time.time()
        while controller_id is None:
            node = self.redpanda.nodes[0]
            controller_id = r = requests.get(
                f"http://{node.account.hostname}:9644/v1/partitions/redpanda/controller/0",
            )
            if r.status_code != 200:
                time.sleep(1)
                continue

            controller_id = r.json()["leader_id"]
            if controller_id == -1:
                time.sleep(1)
                controller_id = None
                continue

            if time.time() - t1 > 10:
                raise RuntimeError("Timed out waiting for a leader")

        leader_node = self.redpanda.get_node(controller_id)

        # Request to all nodes, with redirect-following disabled.  Expect success
        # from the leader, and redirect responses from followers.
        for i, node in enumerate(self.redpanda.nodes):
            # Redpanda config in ducktape has two listeners, one by IP and one by DNS (this simulates
            # nodes that have internal and external addresses).  The admin API redirects should
            # redirect us to the leader's IP for requests sent by IP, by DNS for requests sent by DNS.
            if alternate_listener:
                hostname = socket.gethostbyname(node.account.hostname)
                port = self.redpanda.ADMIN_ALTERNATE_PORT
                leader_name = socket.gethostbyname(leader_node.account.hostname)

            else:
                hostname = node.account.hostname
                port = 9644
                leader_name = leader_node.account.hostname

            resp = requests.request(
                "post",
                f"http://{hostname}:{port}/v1/security/users",
                json={
                    "username": f"user_a_{i}",
                    "password": "password012345",
                    "algorithm": "SCRAM-SHA-256",
                },
                allow_redirects=False,
            )
            self.logger.info(f"Response: {resp.status_code} {resp.headers} {resp.text}")

            if node == leader_node:
                assert resp.status_code == 200
            else:
                # Check we were redirected to the proper listener of the leader node
                self.logger.info(
                    f"Response (redirect): {resp.status_code} {resp.headers.get('location', None)} {resp.text} {resp.history}"
                )
                assert resp.status_code == 307

                location = resp.headers.get("location", None)
                assert location is not None
                assert location.startswith(f"http://{leader_name}:{port}/")

                # Again, this time let requests follow the redirect
                resp = requests.request(
                    "post",
                    f"http://{hostname}:{port}/v1/security/users",
                    json={
                        "username": f"user_a_{i}",
                        "password": "password012345",
                        "algorithm": "SCRAM-SHA-256",
                    },
                    allow_redirects=True,
                )

                self.logger.info(
                    f"Response (follow redirect): {resp.status_code} {resp.text} {resp.history}"
                )
                assert resp.status_code == 200

    @cluster(num_nodes=3)
    @matrix(use_v2_api=[False, True])
    def test_scram(self, use_v2_api):
        topic = TopicSpec()

        client = self.make_superuser_client()
        client.create_topic(topic)

        try:
            # with incorrect password
            client = self.make_superuser_client("xxx")
            client.topics()
            assert False, "Listing topics should fail"
        except AssertionError as e:
            raise e
        except Exception as e:
            self.redpanda.logger.debug(e)
            pass

        # but it works with correct password
        client = self.make_superuser_client()
        topics = client.topics()
        print(topics)
        assert topic.name in topics

        # again!
        client = self.make_superuser_client()
        topics = client.topics()
        print(topics)
        assert topic.name in topics

        username = self.redpanda.SUPERUSER_CREDENTIALS.username
        if use_v2_api:
            self.delete_scram_credential_v2(username)
        else:
            self.delete_user(username)

        try:
            # now listing should fail because the user has been deleted. add
            # some delay to give user deletion time to propogate
            for _ in range(5):
                client = self.make_superuser_client()
                topics = client.topics()
                time.sleep(1)
            assert False, "Listing topics should fail"
        except AssertionError as e:
            raise e
        except Exception as e:
            self.redpanda.logger.debug(e)
            pass

        # recreate user
        if use_v2_api:
            password = self.gen_random_password(15)
            self.create_scram_credential_v2(
                username,
                scram_mechanism_from_string(
                    self.redpanda.SUPERUSER_CREDENTIALS.algorithm
                ),
                password,
            )
        else:
            algorithm = self.redpanda.SUPERUSER_CREDENTIALS.algorithm
            password = self.create_user(username, algorithm)

        # works ok again
        client = self.make_superuser_client(password_override=password)
        topics = client.topics()
        print(topics)
        assert topic.name in topics

        # update password
        if use_v2_api:
            new_password = self.gen_random_password(15)
            self.update_scram_credential_v2(
                username,
                security_types_pb2.SCRAM_MECHANISM_SCRAM_SHA_256,
                new_password,
            )
        else:
            new_password = self.update_user(username)

        try:
            # now listing should fail because the password is different
            for _ in range(5):
                client = self.make_superuser_client(password_override=password)
                client.topics()
                time.sleep(1)
            assert False, "Listing topics should fail"
        except AssertionError as e:
            raise e
        except Exception as e:
            self.redpanda.logger.debug(e)
            pass

        # but works ok with new password
        client = self.make_superuser_client(password_override=new_password)
        topics = client.topics()
        print(topics)
        assert topic.name in topics

        if use_v2_api:
            users = [cred.name for cred in self.list_scram_credentials_v2()]
        else:
            users = self.list_users()
        assert username in users

    @cluster(num_nodes=3)
    @matrix(scram_algo=["SCRAM-SHA-256", "SCRAM-SHA-512"], use_v2_api=[False, True])
    def test_scram_kafka_api_describe(self, scram_algo, use_v2_api):
        """
        This test validates the KIP-554 implementation of Redpanda
        """
        test_username = "test-user"
        test_password = "test-password0"
        test_algorithm = scram_algo
        if use_v2_api:
            test_mechanism = scram_mechanism_from_string(scram_algo)

        if use_v2_api:
            self.create_scram_credential_v2(
                name=test_username, mechanism=test_mechanism, password=test_password
            )
        else:
            self.create_user(
                username=test_username, algorithm=test_algorithm, password=test_password
            )

        kcli = KafkaCliTools(self.redpanda)
        (username, algo, iterations) = kcli.describe_user_scram(user=test_username)
        assert username == test_username, f"Expected {test_username}, got {username}"
        assert algo == test_algorithm, f"Expected {test_algorithm}, got {algo}"
        assert iterations == 4096, f"Expected 4096, got {iterations}"

    @cluster(num_nodes=3)
    @matrix(use_v2_api=[False, True])
    def test_scram_kafka_api_create_user(self, use_v2_api):
        test_username = "test-user"
        test_password = "test-password0"
        test_algorithm = "SCRAM-SHA-256"
        iteration_count = 8192

        try:
            RpkTool(
                self.redpanda,
                username=test_username,
                password=test_password,
                sasl_mechanism=test_algorithm,
            ).list_topics()
            assert False, f"Expected {test_username} to not exist"
        except RpkException:
            pass

        kcli = KafkaCliTools(self.redpanda)
        kcli.create_alter_scram_user(
            user=test_username,
            password=test_password,
            algorithm=test_algorithm,
            iteration_count=iteration_count,
        )

        if use_v2_api:
            users = [cred.name for cred in self.list_scram_credentials_v2()]
        else:
            users = self.list_users()
        assert test_username in users, f"Expected {test_username} to be in {users}"

        # Validate that we can use RPK to list topics with the new user
        RpkTool(
            self.redpanda,
            username=test_username,
            password=test_password,
            sasl_mechanism=test_algorithm,
        ).list_topics()

    @cluster(num_nodes=3)
    @matrix(use_v2_api=[False, True])
    def test_scram_kafka_api_modify_user(self, use_v2_api):
        test_username = "test-user"
        orig_password = "test-password0"
        new_password = "new-password"
        orig_algorithm = "SCRAM-SHA-256"
        new_algorithm = "SCRAM-SHA-512"
        if use_v2_api:
            orig_mechanism = scram_mechanism_from_string(orig_algorithm)

        if use_v2_api:
            self.create_scram_credential_v2(
                test_username,
                orig_mechanism,
                orig_password,
            )
        else:
            self.create_user(
                username=test_username, algorithm=orig_algorithm, password=orig_password
            )

        kcli = KafkaCliTools(self.redpanda)
        kcli.create_alter_scram_user(
            user=test_username, password=new_password, algorithm=new_algorithm
        )

        # Validate that we can use RPK to list topics with the new user
        RpkTool(
            self.redpanda,
            username=test_username,
            password=new_password,
            sasl_mechanism=new_algorithm,
        ).list_topics()

    @cluster(num_nodes=3)
    @matrix(use_v2_api=[False, True])
    def test_scram_kafka_api_delete_user(self, use_v2_api):
        test_username = "test-user"
        test_password = "test-password0"
        test_algorithm = "SCRAM-SHA-256"
        if use_v2_api:
            test_mechanism = scram_mechanism_from_string(test_algorithm)

        if use_v2_api:
            self.create_scram_credential_v2(
                test_username, test_mechanism, test_password
            )
        else:
            self.create_user(
                username=test_username, algorithm=test_algorithm, password=test_password
            )

        kcli = KafkaCliTools(self.redpanda)
        kcli.delete_scram_user(user=test_username, algorithm=test_algorithm)

        try:
            RpkTool(
                self.redpanda,
                username=test_username,
                password=test_password,
                sasl_mechanism=test_algorithm,
            ).list_topics()
            assert False, f"Expected {test_username} to not exist"
        except RpkException:
            pass

    @cluster(num_nodes=3)
    def test_scram_password_set_at_v2(self):
        """
        Comprehensive test for password_set_at timestamp behavior in v2 API.

        This test verifies that:
        1. password_set_at is populated when creating a new credential
        2. getting the credential returns the correct password_set_at
        3. password_set_at is updated when the password is changed
        4. password_set_at persists correctly across cluster restarts
        """
        username = "test-user-v2"
        password = "test-password0123456"
        mechanism = security_types_pb2.SCRAM_MECHANISM_SCRAM_SHA_256

        self.logger.info("Creating credential and verifying password_set_at")
        created_cred = self.create_scram_credential_v2(username, mechanism, password)

        created_at = created_cred.password_set_at.ToDatetime(
            tzinfo=datetime.timezone.utc
        )

        self.logger.info(f"Created credential with password_set_at={created_at}")

        self.logger.info("Wait for created credential to be available on all nodes")
        wait_until(
            lambda: self.check_credential_on_all_nodes(username, created_at),
            timeout_sec=30,
            backoff_sec=0.5,
            err_msg=f"Timeout waiting for {username} to propagate to all nodes",
        )

        self.logger.info(
            "Got expected credential on every node; created credential propagated to all nodes"
        )

        self.logger.info("Updating password and verifying password_set_at changes")

        # Sleep to ensure timestamp difference (at least 1 second)
        time.sleep(1)

        new_password = "new-password0123456"
        updated_cred = self.update_scram_credential_v2(
            username, mechanism, new_password
        )

        updated_at = updated_cred.password_set_at.ToDatetime(
            tzinfo=datetime.timezone.utc
        )

        assert updated_at > created_at, (
            f"password_set_at should be updated: {updated_at} < {created_at}"
        )

        # Wait for updated credential to be available on all nodes (propagation delay)
        wait_until(
            lambda: self.check_credential_on_all_nodes(username, updated_at),
            timeout_sec=30,
            backoff_sec=0.5,
            err_msg=f"Timeout waiting for update to {username} to propagate to all nodes",
        )

        self.logger.info(
            "Got expected credential on every node; updated credential propagated to all nodes"
        )

        self.logger.info("Restarting cluster and verifying password_set_at persistence")
        self.redpanda.restart_nodes(self.redpanda.nodes)

        wait_until(
            lambda: self.check_credential_on_all_nodes(username, updated_at),
            timeout_sec=30,
            backoff_sec=0.5,
            err_msg=f"Timeout waiting for credential {username} to be available on all nodes after restart",
        )

        self.logger.info(
            "Got expected credential on every node after restart; password_set_at persisted correctly"
        )


class SaslPlainTest(BaseScramTest):
    """
    These tests validate the functionality of the SASL/PLAIN
    authentication mechanism.
    """

    class ClientType(IntEnum):
        KCL = 1
        RPK = 2
        PYTHON_RDKAFKA = 3
        KCLI = 4

    class ScramType(IntEnum):
        SCRAM_SHA_256 = 1
        SCRAM_SHA_512 = 2

        def __str__(self):
            return self.name.replace("_", "-")

    def __init__(self, test_context):
        security = SecurityConfig()
        security.enable_sasl = True
        super(SaslPlainTest, self).__init__(
            test_context,
            num_brokers=3,
            security=security,
            extra_node_conf={"developer_mode": True},
        )

    class SaslPlainConfig(IntEnum):
        ON = 1
        OFF = 2
        OVERRIDE_ON = 3
        OVERRIDE_OFF = 4

    def _config_plain_authn(self, sasl_plain_config):
        scram_plain = ["PLAIN", "SCRAM"]
        match sasl_plain_config:
            case self.SaslPlainConfig.OFF:
                pass
            case self.SaslPlainConfig.ON:
                self.logger.debug("Enabling SASL PLAIN")
                self.redpanda.set_cluster_config({"sasl_mechanisms": scram_plain})
            case self.SaslPlainConfig.OVERRIDE_ON:
                self.logger.debug("Enabling SASL PLAIN through override")
                self.redpanda.set_cluster_config(
                    {
                        "sasl_mechanisms_overrides": [
                            {"listener": "dnslistener", "sasl_mechanisms": scram_plain},
                        ]
                    }
                )
            case self.SaslPlainConfig.OVERRIDE_OFF:
                self.logger.debug("Enabling SASL PLAIN by default")
                self.redpanda.set_cluster_config({"sasl_mechanisms": scram_plain})
                self.logger.debug("Disabling SASL PLAIN through override")
                self.redpanda.set_cluster_config(
                    {
                        "sasl_mechanisms_overrides": [
                            {"listener": "dnslistener", "sasl_mechanisms": []},
                        ]
                    }
                )

    def _make_client(
        self,
        client_type: ClientType,
        username_override: str | None = None,
        password_override: str | None = None,
        algorithm_override: str | None = None,
    ) -> PythonLibrdkafka | RawKCL | RpkTool | KafkaCliTools:
        username, password, algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        username = username_override or username
        password = password_override or password
        algorithm = algorithm_override or algorithm

        if client_type == self.ClientType.PYTHON_RDKAFKA:
            return PythonLibrdkafka(
                self.redpanda, username=username, password=password, algorithm=algorithm
            )
        elif client_type == self.ClientType.KCL:
            return RawKCL(
                self.redpanda,
                username=username,
                password=password,
                sasl_mechanism=algorithm,
            )
        elif client_type == self.ClientType.RPK:
            return RpkTool(
                self.redpanda,
                username=username,
                password=password,
                sasl_mechanism=algorithm,
            )
        elif client_type == self.ClientType.KCLI:
            return KafkaCliTools(
                self.redpanda, user=username, passwd=password, algorithm=algorithm
            )
        else:
            assert_never(client_type)  # pyright: ignore[reportUnreachable]

    def _make_topic(
        self,
        client: PythonLibrdkafka | RawKCL | RpkTool | KafkaCliTools,
        expect_success: bool,
    ) -> TopicSpec:
        topic_name = "test-topic"
        topic = TopicSpec(name=topic_name)
        try:
            if isinstance(client, PythonLibrdkafka):
                client.create_topic(topic)
            elif isinstance(client, RawKCL):
                resp = client.create_topics(6, [{"name": topic_name}])
                self.logger.info(f"RESP: {resp}")
                if expect_success:
                    assert len(resp) != 0, (
                        "Should have received response with SASL/PLAIN enabled"
                    )
                    assert resp[0]["ErrorCode"] == 0, (
                        f"Expected error code 0, got {resp[0]['ErrorCode']}"
                    )
                    return
                else:
                    assert len(resp) == 0, (
                        "Should not have received response with SASL/PLAIN disabled"
                    )
                    return
            elif isinstance(client, RpkTool):
                client.create_topic(topic=topic_name)
            elif isinstance(client, KafkaCliTools):
                client.create_topic(topic)
            else:
                assert False, f"Unknown client type: {client} ({type(client)})"  # pyright: ignore[reportUnreachable]
            assert expect_success, "Should have failed with SASL/PLAIN disabled"
        except RpkException as e:
            assert isinstance(client, RpkTool), (
                f"Should not have received an RPK exception from {client} ({type(client)})"
            )
            assert not expect_success, (
                f"Should not have failed with SASL/PLAIN enabled: {e}"
            )
            assert "UNSUPPORTED_SASL_MECHANISM" in str(e), (
                f"Expected UNSUPPORTED_SASL_MECHANISM, got {e}"
            )
        except KafkaException as e:
            assert isinstance(client, PythonLibrdkafka), (
                f"Should not have received a KafkaException from {client} ({type(client)})"
            )
            assert not expect_success, (
                f"Should not have failed with SASL/PLAIN enabled: {e}"
            )
            assert e.args[0].code() == KafkaError._TIMED_OUT, (
                f"Expected KafkaError._TIMED_OUT, got {e.args[0].code()}"
            )
        except KafkaCliToolsError as e:
            assert isinstance(client, KafkaCliTools), (
                f"Should not have received a KafkaCliToolsError from {client} ({type(client)})"
            )
            assert not expect_success, (
                f"Should not have failed with SASL/PLAIN enabled: {e}"
            )
            assert "UnsupportedSaslMechanismException" in str(e), (
                f"Expected to see UnsupportedSaslMechanismException, got {e}"
            )

    @cluster(num_nodes=3)
    @matrix(
        client_type=list(ClientType),
        scram_type=list(ScramType),
        sasl_plain_config=list(SaslPlainConfig),
        use_v2_api=[False, True],
    )
    def test_plain_authn(self, client_type, scram_type, sasl_plain_config, use_v2_api):
        """
        This test validates that SASL/PLAIN works with common kafka client
        libraries:
        - Python librdkafka
        - Raw KCL
        - RPK
        - Kafka CLI tools

        This test will validate that SASL/PLAIN works with both SCRAM-SHA-256
        and SCRAM-SHA-512 users.
        """
        username = "test-user"
        password = "test-password0"
        RpkTool(
            self.redpanda,
            username=self.redpanda.SUPERUSER_CREDENTIALS.username,
            password=self.redpanda.SUPERUSER_CREDENTIALS.password,
            sasl_mechanism=self.redpanda.SUPERUSER_CREDENTIALS.algorithm,
        ).sasl_allow_principal(
            principal=username, operations=["all"], resource="topic", resource_name="*"
        )
        if use_v2_api:
            self.create_scram_credential_v2(
                username, scram_mechanism_from_string(str(scram_type)), password
            )
        else:
            self.create_user(
                username=username, algorithm=str(scram_type), password=password
            )

        self._config_plain_authn(sasl_plain_config)

        client = self._make_client(
            client_type,
            username_override=username,
            password_override=password,
            algorithm_override="PLAIN",
        )
        sasl_plain_enabled = sasl_plain_config in [
            self.SaslPlainConfig.ON,
            self.SaslPlainConfig.OVERRIDE_ON,
        ]
        self._make_topic(client, sasl_plain_enabled)

    @cluster(num_nodes=3)
    @matrix(use_v2_api=[False, True])
    def test_plain_authn_short_password(self, use_v2_api):
        """
        This test validates that SASL/PLAIN in fips mode fails gracefully when the user
        provides a short password.
        """
        username = "test-user"
        good_password = "sufficiently_long_password"
        bad_password = "short-pwd"
        algorithm = str(self.ScramType.SCRAM_SHA_256)
        RpkTool(
            self.redpanda,
            username=self.redpanda.SUPERUSER_CREDENTIALS.username,
            password=self.redpanda.SUPERUSER_CREDENTIALS.password,
            sasl_mechanism=self.redpanda.SUPERUSER_CREDENTIALS.algorithm,
        ).sasl_allow_principal(
            principal=username, operations=["all"], resource="topic", resource_name="*"
        )
        if use_v2_api:
            self.create_scram_credential_v2(
                username,
                scram_mechanism_from_string(algorithm),
                good_password,
            )
        else:
            self.create_user(
                username=username,
                algorithm=algorithm,
                password=good_password,
            )
        self._config_plain_authn(self.SaslPlainConfig.ON)

        # We create the user with a good password to not have to worry about short password error
        # there. This test simulates a user created before an update to a fips 140-3 version, that
        # might have a short password. The length of the password check is done early, before checking
        # the validity of the password. Since the password is wrong, this create_topic request will
        # fail. We are interested to see how it will fail
        client = RpkTool(
            self.redpanda,
            username=username,
            password=bad_password,
            sasl_mechanism="PLAIN",
        )
        with expect_exception(RpkException, lambda e: True):
            client.create_topic("test-topic")

        log_msg = "password length less than 14 characters"
        if in_fips_environment():
            wait_until(
                lambda: self.redpanda.search_log_any(log_msg),
                timeout_sec=10,
                err_msg="Expected to find warning about short password in logs",
            )
        else:
            assert not self.redpanda.search_log_any(log_msg), (
                "Warning about password length should not be present"
            )


class SaslPlainTLSProvider(TLSProvider):
    def __init__(self, tls: TLSCertManager):
        self._tls = tls

    @property
    def ca(self) -> CertificateAuthority:
        return self._tls.ca

    def create_broker_cert(self, service: Service, node: ClusterNode) -> Certificate:
        assert node in service.nodes
        return self._tls.create_cert(node.name)

    def create_service_client_cert(self, _: Service, name: str) -> Certificate:
        return self._tls.create_cert(socket.gethostname(), name=name)


class SaslPlainConfigTest(BaseScramTest):
    """
    These tests verify the behavior of Redpanda in different
    configurations with SASL/PLAIN enabled
    """

    LICENSE_CHECK_INTERVAL_SEC = 1

    def __init__(self, test_context):
        self.security = SecurityConfig()
        self.security.enable_sasl = True
        super(SaslPlainConfigTest, self).__init__(
            test_context, num_brokers=3, security=self.security
        )
        self.redpanda.set_environment(
            {
                "__REDPANDA_PERIODIC_REMINDER_INTERVAL_SEC": f"{self.LICENSE_CHECK_INTERVAL_SEC}"
            }
        )
        self.tls = TLSCertManager(self.logger)

    def setUp(self):
        pass

    def _start_cluster(self, enable_tls: bool):
        if enable_tls:
            self.security.tls_provider = SaslPlainTLSProvider(tls=self.tls)
            self.redpanda.set_security_settings(self.security)
        super().setUp()

    @cluster(num_nodes=3)
    def test_cannot_enable_plain_without_scram(self):
        """
        This test verifies that when enabling PLAIN you must also enable SCRAM
        """
        self._start_cluster(enable_tls=False)

        def validate_sasl_plain_mech(mechs: List[str]):
            try:
                self.redpanda.set_cluster_config({"sasl_mechanisms": mechs})
                assert False, f"Should not have been able to set {mechs}"
            except HTTPError as e:
                assert e.response.status_code == 400, (
                    f"Expected 400, got {e.response.status_code}"
                )
                response = json.loads(e.response.text)
                assert "sasl_mechanisms" in response, (
                    f'Response missing "sasl_mechanisms": {response}'
                )
                assert (
                    "SCRAM mechanism must be enabled if PLAIN is enabled"
                    == response["sasl_mechanisms"]
                ), f"Invalid message in response: {response['sasl_mechansisms']}"

        validate_sasl_plain_mech(["PLAIN"])
        validate_sasl_plain_mech(["PLAIN", "GSSAPI"])

    @cluster(num_nodes=3, log_allow_list=[re.compile("SASL/PLAIN is enabled")])
    @parametrize(enable_tls=True)
    @parametrize(enable_tls=False)
    def test_sasl_plain_log(self, enable_tls: bool):
        """
        This test verifies that a log message is emitted when SASL/PLAIN is enabled
        """
        self._start_cluster(enable_tls=enable_tls)
        wait_until_nag_is_set(
            redpanda=self.redpanda, check_interval_sec=self.LICENSE_CHECK_INTERVAL_SEC
        )
        self.redpanda.set_cluster_config({"sasl_mechanisms": ["SCRAM", "PLAIN"]})

        self.logger.debug("Waiting for SASL/PLAIN message")

        def has_sasl_plain_log():
            # There is always at least one Kafka API with TLS disabled meaning
            # this will always log at the error level
            return self.redpanda.search_log_all(
                r"^ERROR.*SASL/PLAIN is enabled\. This is insecure and not recommended for production\.$"
            )

        wait_until(
            has_sasl_plain_log,
            timeout_sec=self.LICENSE_CHECK_INTERVAL_SEC * 2,
            err_msg="Failed to find SASL/PLAIN log message",
        )


class ScramLiveUpdateTest(RedpandaTest):
    def __init__(self, test_context):
        super(ScramLiveUpdateTest, self).__init__(test_context, num_brokers=1)

    @cluster(num_nodes=1)
    def test_enable_sasl_live(self):
        """
        Verify that when enable_sasl is set to true at runtime, subsequent
        unauthenticated kafka clients are rejected.
        """

        unauthenticated_client = PythonLibrdkafka(self.redpanda)
        topic = TopicSpec(replication_factor=1)
        unauthenticated_client.create_topic(topic)
        assert len(unauthenticated_client.topics()) == 1

        # Switch on authentication
        self.redpanda.set_cluster_config({"enable_sasl": True})

        # An unauthenticated client should be rejected
        try:
            unauthenticated_client.topics()
        except Exception as e:
            self.logger.exception(f"Unauthenticated: {e}")
        else:
            self.logger.error("Unauthenticated client should have been rejected")
            assert False

        # Switch off authentication
        self.redpanda.set_cluster_config({"enable_sasl": False})

        # An unauthenticated client should be accepted again
        assert len(unauthenticated_client.topics()) == 1


class ScramBootstrapUserTest(RedpandaTest):
    BOOTSTRAP_USERNAME = "bob"
    BOOTSTRAP_PASSWORD = "sekrit0123456789"

    # BOOTSTRAP_MECHANISM = 'SCRAM-SHA-512'

    def __init__(self, test_context, *args, **kwargs):
        # Configure the cluster as a user might configure it for secure
        # bootstrap: i.e. all auth turned on from moment of creation.

        self.mechanism = test_context.injected_args["mechanism"]
        self.expect_fail = test_context.injected_args.get("expect_fail", False)

        security_config = SecurityConfig()
        security_config.enable_sasl = True

        super().__init__(
            test_context,
            *args,
            environment={
                "RP_BOOTSTRAP_USER": f"{self.BOOTSTRAP_USERNAME}:{self.BOOTSTRAP_PASSWORD}:{self.mechanism}"
            },
            extra_rp_conf={
                "enable_sasl": True,
                "admin_api_require_auth": True,
                "superusers": ["bob"],
            },
            security=security_config,
            superuser=SaslCredentials(
                self.BOOTSTRAP_USERNAME, self.BOOTSTRAP_PASSWORD, self.mechanism
            ),
            **kwargs,
        )

    def setUp(self):
        self.redpanda.start(expect_fail=self.expect_fail)
        if not self.expect_fail:
            self._create_initial_topics()

    def _check_http_status_everywhere(self, expect_status, callable):
        """
        Check that the callback results in an HTTP error with the
        given status code from all nodes in the cluster.  This enables
        checking that auth state has propagated as expected.

        :returns: true if all nodes throw an error with the expected status code
        """

        for n in self.redpanda.nodes:
            try:
                callable(n)
            except HTTPError as e:
                if e.response.status_code != expect_status:
                    return False
            else:
                return False

        return True

    def _check_connect_err_everywhere(self, expected_code: ConnectErrorCode, callable):
        """
        Check that the callback results in an HTTP error with the
        given status code from all nodes in the cluster.  This enables
        checking that auth state has propagated as expected.

        :returns: true if all nodes throw an error with the expected status code
        """

        for n in self.redpanda.nodes:
            try:
                callable(n)
            except ConnectError as e:
                if e.code != expected_code:
                    return False
            else:
                return False

        return True

    @cluster(num_nodes=3)
    @parametrize(mechanism="SCRAM-SHA-512")
    @parametrize(mechanism="SCRAM-SHA-256")
    def test_bootstrap_user(self, mechanism):
        # Anonymous access should be refused
        admin = Admin(self.redpanda)
        new_password = "newpassword0123456789"
        with expect_http_error(403):
            admin.list_users()

        # Access using the bootstrap credentials should succeed
        admin = Admin(
            self.redpanda, auth=(self.BOOTSTRAP_USERNAME, self.BOOTSTRAP_PASSWORD)
        )
        assert self.BOOTSTRAP_USERNAME in admin.list_users()

        # Modify the bootstrap user's credential
        admin.update_user(self.BOOTSTRAP_USERNAME, new_password, "SCRAM-SHA-256")

        # Getting 401 with old credentials everywhere will show that the
        # credential update has propagated to all nodes
        wait_until(
            lambda: self._check_http_status_everywhere(
                401, lambda n: admin.list_users(node=n)
            ),
            timeout_sec=10,
            backoff_sec=0.5,
        )

        # Using old password should fail
        with expect_http_error(401):
            admin.list_users()

        # Using new credential should succeed
        admin = Admin(self.redpanda, auth=(self.BOOTSTRAP_USERNAME, new_password))
        admin.list_users()

        # Modified credential should survive a restart: this verifies that
        # the RP_BOOTSTRAP_USER setting does not fight with changes made
        # by other means.
        self.redpanda.restart_nodes(self.redpanda.nodes)
        admin.list_users()

    @cluster(num_nodes=3)
    @parametrize(mechanism="SCRAM-SHA-512")
    @parametrize(mechanism="SCRAM-SHA-256")
    def test_bootstrap_user_v2(self, mechanism):
        # Anonymous access should be refused
        admin = AdminV2(self.redpanda)
        new_password = "newpassword0123456789"

        with expect_exception(
            ConnectError,
            lambda e: e.code == ConnectErrorCode.PERMISSION_DENIED,
        ):
            admin.security().list_scram_credentials(
                security_pb2.ListScramCredentialsRequest()
            )

        # Access using the bootstrap credentials should succeed
        admin = AdminV2(
            self.redpanda, auth=(self.BOOTSTRAP_USERNAME, self.BOOTSTRAP_PASSWORD)
        )

        users = [
            cred.name
            for cred in admin.security()
            .list_scram_credentials(security_pb2.ListScramCredentialsRequest())
            .scram_credentials
        ]
        assert self.BOOTSTRAP_USERNAME in users

        # Modify the bootstrap user's credential
        admin.security().update_scram_credential(
            security_pb2.UpdateScramCredentialRequest(
                scram_credential=security_pb2.ScramCredential(
                    name=self.BOOTSTRAP_USERNAME,
                    mechanism=scram_mechanism_from_string(mechanism),
                    password=new_password,
                )
            )
        )

        # Getting UNAUTHENTICATED with old credentials everywhere will show that the
        # credential update has propagated to all nodes
        wait_until(
            lambda: self._check_connect_err_everywhere(
                ConnectErrorCode.UNAUTHENTICATED,
                lambda n: admin.security(node=n).list_scram_credentials(
                    security_pb2.ListScramCredentialsRequest()
                ),
            ),
            timeout_sec=10,
            backoff_sec=0.5,
        )

        # Using old password should fail
        with expect_exception(
            ConnectError,
            lambda e: e.code == ConnectErrorCode.UNAUTHENTICATED,
        ):
            admin.security().list_scram_credentials(
                security_pb2.ListScramCredentialsRequest()
            )

        # Using new credential should succeed
        admin = AdminV2(self.redpanda, auth=(self.BOOTSTRAP_USERNAME, new_password))
        admin.security().list_scram_credentials(
            security_pb2.ListScramCredentialsRequest()
        )

        # Modified credential should survive a restart: this verifies that
        # the RP_BOOTSTRAP_USER setting does not fight with changes made
        # by other means.
        self.redpanda.restart_nodes(self.redpanda.nodes)
        admin.security().list_scram_credentials(
            security_pb2.ListScramCredentialsRequest()
        )

    @cluster(
        num_nodes=1,
        log_allow_list=[re.compile(r"std::invalid_argument.*Invalid SCRAM mechanism")],
    )
    @parametrize(mechanism="sCrAm-ShA-512", expect_fail=True)
    def test_invalid_scram_mechanism(self, mechanism, expect_fail):
        assert expect_fail
        assert self.redpanda.count_log_node(
            self.redpanda.nodes[0], "Invalid SCRAM mechanism"
        )


class InvalidNewUserStrings(BaseScramTest):
    """
    Tests used to validate that strings with control characters are rejected
    when attempting to create users
    """

    def __init__(self, test_context):
        security = SecurityConfig()
        security.enable_sasl = False
        super(InvalidNewUserStrings, self).__init__(
            test_context,
            num_brokers=3,
            security=security,
            extra_node_conf={"developer_mode": True},
        )

    @cluster(num_nodes=3)
    @matrix(use_v2_api=[False, True])
    def test_invalid_user_name(self, use_v2_api):
        """
        Validates that usernames that contain control characters and usernames which
        do not match the SCRAM regex are properly rejected
        """
        username = generate_string_with_control_character(15)
        algorithm = "SCRAM-SHA-256"

        if use_v2_api:
            self.create_scram_credential_v2(
                name=username,
                mechanism=scram_mechanism_from_string(algorithm),
                password=self.gen_random_password(15),
                expected_error=ConnectErrorCode.INVALID_ARGUMENT,
            )
        else:
            self.create_user(
                username=username,
                algorithm=algorithm,
                expected_status_code=400,
                err_msg="Parameter 'username' contained invalid control characters",
            )

        # Two ordinals (corresponding to ',' and '=') are explicitly excluded from SASL usernames
        for ordinal in [0x2C, 0x3D]:
            username = f"john{chr(ordinal)}doe"
            if use_v2_api:
                self.create_scram_credential_v2(
                    name=username,
                    mechanism=scram_mechanism_from_string(algorithm),
                    password=self.gen_random_password(15),
                    expected_error=ConnectErrorCode.INVALID_ARGUMENT,
                )
            else:
                self.create_user(
                    username=username,
                    algorithm=algorithm,
                    expected_status_code=400,
                    err_msg=f"Invalid SCRAM username {'{' + username + '}'}",
                )

    @cluster(num_nodes=3)
    def test_invalid_alg(self):
        """
        (V1 Only) Validates that algorithms that contain control characters are properly rejected
        """
        algorithm = generate_string_with_control_character(10)
        username = "test"

        self.create_user(
            username=username,
            algorithm=algorithm,
            expected_status_code=400,
            err_msg="Parameter 'algorithm' contained invalid control characters",
        )

    @cluster(num_nodes=3)
    @matrix(use_v2_api=[False, True])
    def test_invalid_password(self, use_v2_api):
        """
        Validates that passwords that contain control characters are properly rejected
        """
        username = "test"
        password = generate_string_with_control_character(15)
        algorithm = "SCRAM-SHA-256"

        if use_v2_api:
            self.create_scram_credential_v2(
                name=username,
                mechanism=scram_mechanism_from_string(algorithm),
                password=password,
                expected_error=ConnectErrorCode.INVALID_ARGUMENT,
            )
        else:
            self.create_user(
                username=username,
                algorithm=algorithm,
                password=password,
                expected_status_code=400,
                err_msg="Parameter 'password' contained invalid control characters",
            )

    @cluster(num_nodes=3)
    @matrix(use_v2_api=[False, True])
    def test_short_password(self, use_v2_api):
        """
        Validate that in fips mode, short scram passwords (<14 chars) are rejected with a clean error.
        In non-fips mode, they should be accepted.
        """
        username = "test-user"
        algorithm = "SCRAM-SHA-256"
        short_password = "short_pwd"

        if in_fips_environment():
            expected_status_code = 400
            expected_v2_error = ConnectErrorCode.INVALID_ARGUMENT
            err_msg = "Password length less than 14 characters"
        else:
            expected_status_code = 200
            expected_v2_error = None
            err_msg = None

        # Validate failure in fips mode and success in non-fips
        if use_v2_api:
            self.create_scram_credential_v2(
                name=username,
                mechanism=scram_mechanism_from_string(algorithm),
                password=short_password,
                expected_error=expected_v2_error,
            )
        else:
            self.create_user(
                username=username,
                algorithm=algorithm,
                password=short_password,
                expected_status_code=expected_status_code,
                err_msg=err_msg,
            )

        # Validate success in both - password is long enough
        username = "test-user-2"
        long_password = "sufficiently_long_password"
        if use_v2_api:
            self.create_scram_credential_v2(
                name=username,
                mechanism=scram_mechanism_from_string(algorithm),
                password=long_password,
            )
        else:
            self.create_user(
                username=username,
                algorithm=algorithm,
                password=long_password,
            )

        # Validate failure in fips mode and success in non-fips
        if use_v2_api:
            self.update_scram_credential_v2(
                name=username,
                mechanism=scram_mechanism_from_string(algorithm),
                password=short_password,
                expected_error=expected_v2_error,
            )
        else:
            self.update_user(
                username=username,
                password=short_password,
                expected_status_code=expected_status_code,
                err_msg=err_msg,
            )

        # Validate success in both - password is long enough
        other_long_password = "other_sufficiently_long_password"
        if use_v2_api:
            self.update_scram_credential_v2(
                name=username,
                mechanism=scram_mechanism_from_string(algorithm),
                password=other_long_password,
            )
        else:
            self.update_user(
                username=username,
                password=other_long_password,
            )


class EscapedNewUserStrings(BaseScramTest):
    # All of the non-control characters that need escaping
    NEED_ESCAPE = [
        "!",
        '"',
        "#",
        "$",
        "%",
        "&",
        "'",
        "(",
        ")",
        "+",
        # ',', Excluded by SASLNAME regex
        "/",
        ":",
        ";",
        "<",
        # '=', Excluded by SASLNAME regex
        ">",
        "?",
        "[",
        "\\",
        "]",
        "^",
        "`",
        "{",
        "}",
        "~",
    ]

    @cluster(num_nodes=3)
    @matrix(use_v2_api=[False, True])
    def test_update_delete_user(self, use_v2_api):
        """
        Verifies that users whose names contain characters which require URL escaping can be subsequently deleted.
        i.e. that the username included with a delete request is properly unescaped by the admin server.
        """

        su_username = self.redpanda.SUPERUSER_CREDENTIALS.username

        users = []

        self.logger.debug("Create some users with names that will require URL escaping")

        password = "passwd01234567"
        algorithm = "SCRAM-SHA-256"

        for ch in self.NEED_ESCAPE:
            username = f"john{ch}doe"
            if use_v2_api:
                self.create_scram_credential_v2(
                    name=username,
                    mechanism=scram_mechanism_from_string(algorithm),
                    password=password,
                    expected_error=None,
                )
            else:
                self.create_user(
                    username=username,
                    algorithm=algorithm,
                    password=password,
                    expected_status_code=200,
                )
            users.append(username)

        admin = Admin(self.redpanda)

        def _users_match(expected: list[str]):
            if use_v2_api:
                live_users = [cred.name for cred in self.list_scram_credentials_v2()]
            else:
                live_users = admin.list_users()

            live_users.remove(su_username)
            return len(expected) == len(live_users) and set(expected) == set(live_users)

        wait_until(lambda: _users_match(users), timeout_sec=5, backoff_sec=0.5)

        self.logger.debug(
            "We should be able to update and delete these users without issue"
        )
        for username in users:
            if use_v2_api:
                self.update_scram_credential_v2(
                    name=username,
                    mechanism=security_types_pb2.SCRAM_MECHANISM_SCRAM_SHA_256,
                    password=self.gen_random_password(15),
                )
                self.delete_scram_credential_v2(name=username)
            else:
                self.update_user(username=username)
                self.delete_user(username=username)

        try:
            wait_until(lambda: _users_match([]), timeout_sec=5, backoff_sec=0.5)
        except TimeoutError:
            if use_v2_api:
                live_users = [cred.name for cred in self.list_scram_credentials_v2()]
            else:
                live_users = admin.list_users()
            live_users.remove(su_username)
            assert len(live_users) == 0, (
                f"Expected no users, got {len(live_users)}: {live_users}"
            )


class SCRAMReauthTest(BaseScramTest):
    EXAMPLE_TOPIC = "foo"

    MAX_REAUTH_MS = 2000
    PRODUCE_DURATION_S = MAX_REAUTH_MS * 2 / 1000
    PRODUCE_INTERVAL_S = 0.1
    PRODUCE_ITER = int(PRODUCE_DURATION_S / PRODUCE_INTERVAL_S)

    def __init__(self, test_context, **kwargs):
        security = SecurityConfig()
        security.enable_sasl = True
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            security=security,
            extra_rp_conf={"kafka_sasl_max_reauth_ms": self.MAX_REAUTH_MS},
            **kwargs,
        )

        username, password, algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        self.rpk = RpkTool(
            self.redpanda,
            username=username,
            password=password,
            sasl_mechanism=algorithm,
        )

    @cluster(num_nodes=3)
    def test_scram_reauth(self):
        self.rpk.create_topic(self.EXAMPLE_TOPIC)
        su_client = self.make_superuser_client()
        producer = su_client.get_producer()
        producer.poll(1.0)

        expected_topics = set([self.EXAMPLE_TOPIC])
        wait_until(
            lambda: set(producer.list_topics(timeout=5).topics.keys())
            == expected_topics,
            timeout_sec=5,
        )

        for i in range(0, self.PRODUCE_ITER):
            producer.poll(0.0)
            producer.produce(topic=self.EXAMPLE_TOPIC, key="bar", value=str(i))
            time.sleep(self.PRODUCE_INTERVAL_S)

        producer.flush(timeout=2)

        metrics = get_sasl_metrics(self.redpanda)
        self.redpanda.logger.debug(f"SASL metrics: {metrics}")
        assert EXPIRATION_METRIC in metrics.keys()
        assert metrics[EXPIRATION_METRIC] == 0, (
            "Client should reauth before session expiry"
        )
        assert REAUTH_METRIC in metrics.keys()
        assert metrics[REAUTH_METRIC] > 0, "Expected client reauth on some broker..."
