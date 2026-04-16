# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import datetime
from enum import Enum
import json
import socket
import threading
import time
from urllib.parse import urlparse

import requests
from confluent_kafka import KafkaError
from connectrpc.errors import ConnectError, ConnectErrorCode
from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix, parametrize
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from keycloak import KeycloakOpenID

from rptest.clients.admin.proto.redpanda.core.admin.v2 import security_pb2
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.kafka_cli_tools import AuthorizationError, KafkaCliTools
from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.rpk import AclList, RpkTool
from rptest.services.cluster import cluster
from rptest.services.keycloak import (
    DEFAULT_AT_LIFESPAN_S,
    DEFAULT_REALM,
    KeycloakService,
    OAuthConfig,
)
from rptest.services.redpanda import (
    LoggingConfig,
    MetricsEndpoint,
    PandaproxyConfig,
    SchemaRegistryConfig,
    SecurityConfig,
    make_redpanda_service,
)
from rptest.services.tls import TLSCertManager
from rptest.tests.sasl_reauth_test import (
    EXPIRATION_METRIC,
    REAUTH_METRIC,
    get_sasl_metrics,
)
from rptest.tests.tls_metrics_test import FaketimeTLSProvider
from rptest.util import expect_exception, wait_until_result
from rptest.utils.log_utils import wait_until_nag_is_set
from rptest.utils.mode_checks import skip_fips_mode

CLIENT_ID = "myapp"
TOKEN_AUDIENCE = "account"
EXAMPLE_TOPIC = "foo"

log_config = LoggingConfig(
    "info",
    logger_levels={
        "security": "trace",
        "pandaproxy": "trace",
        "schemaregistry": "trace",
        "kafka/client": "trace",
        "kafka": "debug",
        "http": "trace",
        "request_auth": "trace",
    },
)


class NestedGroupType(str, Enum):
    """
    How nested identity provider group names are mapped to Redpanda principals.
    The enum values are used directly as string configuration values.
    NONE
        No special handling for nested groups. The full group name as provided
        by the identity provider (including any nesting or path components) is
        used when deriving principals.
    SUFFIX
        Use only the leaf (suffix) component of a nested group name when
        deriving principals. For example, a group like ``/team/platform/admins``
        is treated as ``admins``.
    """

    NONE = "none"
    SUFFIX = "suffix"


def get_nested_group_types() -> list[NestedGroupType]:
    return [NestedGroupType.NONE, NestedGroupType.SUFFIX]


class RedpandaOIDCTestBase(Test):
    """
    Base class for tests that use the Redpanda service with OIDC
    """

    def __init__(
        self,
        test_context,
        num_nodes=4,
        sasl_mechanisms=["SCRAM", "OAUTHBEARER"],
        http_authentication=["BASIC", "OIDC"],
        sasl_max_reauth_ms=None,
        access_token_lifespan=DEFAULT_AT_LIFESPAN_S,
        use_ssl=False,
        **kwargs,
    ):
        super(RedpandaOIDCTestBase, self).__init__(test_context, **kwargs)
        self.tls = None
        provider = None
        if use_ssl:
            self.tls = TLSCertManager(self.logger)
            provider = FaketimeTLSProvider(self.tls)

        num_brokers = num_nodes - 1
        self.keycloak = KeycloakService(test_context, tls=provider)
        kc_node = self.keycloak.nodes[0]
        try:
            self.keycloak.start_node(
                kc_node, access_token_lifespan_s=access_token_lifespan
            )
        except Exception as e:
            self.logger.error(f"{e}")
            self.keycloak.clean_node(kc_node)
            assert False, "Keycloak failed to start"

        security = SecurityConfig()
        security.enable_sasl = True
        security.sasl_mechanisms = sasl_mechanisms
        security.http_authentication = http_authentication
        security.tls_provider = provider

        pandaproxy_config = PandaproxyConfig()
        pandaproxy_config.authn_method = "http_basic"

        schema_reg_config = SchemaRegistryConfig()
        schema_reg_config.authn_method = "http_basic"

        self.redpanda = make_redpanda_service(
            test_context,
            num_brokers,
            extra_rp_conf={
                "oidc_discovery_url": self.keycloak.get_discovery_url(
                    kc_node, use_ssl=use_ssl
                ),
                "oidc_token_audience": TOKEN_AUDIENCE,
                "kafka_sasl_max_reauth_ms": sasl_max_reauth_ms,
                "group_initial_rebalance_delay": 0,
            },
            security=security,
            pandaproxy_config=pandaproxy_config,
            schema_registry_config=schema_reg_config,
            log_config=log_config,
        )

        self.client_cert = None
        if use_ssl:
            assert self.tls is not None
            self.client_cert = self.tls.create_cert(
                socket.gethostname(), common_name="user", name="user"
            )
            schema_reg_config.client_key = self.client_cert.key
            schema_reg_config.client_crt = self.client_cert.crt
            pandaproxy_config.client_key = self.client_cert.key
            pandaproxy_config.client_crt = self.client_cert.crt

            self.redpanda.set_schema_registry_settings(schema_reg_config)
            self.redpanda.set_pandaproxy_settings(pandaproxy_config)

        self.su_username, self.su_password, self.su_algorithm = (
            self.redpanda.SUPERUSER_CREDENTIALS
        )

        self.rpk = RpkTool(
            self.redpanda,
            username=self.su_username,
            password=self.su_password,
            sasl_mechanism=self.su_algorithm,
            tls_cert=self.client_cert,
            tls_enabled=use_ssl,
        )

    def setUp(self):
        self.redpanda.logger.info("Starting Redpanda")
        self.redpanda.start()

    def create_service_user(self, client_id=CLIENT_ID):
        kc_node = self.keycloak.nodes[0]

        self.keycloak.admin.create_user(
            "norma", "desmond", realm_admin=True, email="10086@sunset.blvd"
        )
        self.keycloak.login_admin_user(kc_node, "norma", "desmond")
        self.keycloak.admin.create_client(client_id)

        service_user = f"service-account-{client_id}"
        # add an email address to myapp client's service user. this should
        # appear alongside the access token.
        self.keycloak.admin.update_user(service_user, email="myapp@customer.com")
        return self.keycloak.admin_ll.get_user_id(service_user)

    def get_client_credentials_token(self, cfg) -> dict:
        token_endpoint_url = urlparse(cfg.token_endpoint)
        openid = KeycloakOpenID(
            server_url=f"{token_endpoint_url.scheme}://{token_endpoint_url.netloc}",
            client_id=cfg.client_id,
            client_secret_key=cfg.client_secret,
            realm_name=DEFAULT_REALM,
            verify=False,
        )
        return openid.token(grant_type="client_credentials")

    def get_idp_request_count(self, nodes: list[ClusterNode]):
        metrics = [
            "security_idp_latency_seconds_count",
        ]
        samples = self.redpanda.metrics_samples(metrics, nodes, MetricsEndpoint.METRICS)

        result = {}
        for k in samples.keys():
            result[k] = result.get(k, 0) + sum(
                [int(s.value) for s in samples[k].samples]
            )
        return result["security_idp_latency_seconds_count"]

    def get_sasl_session_revoked_total(self):
        metrics = [
            "kafka_rpc_sasl_session_revoked_total",
        ]
        samples = self.redpanda.metrics_samples(
            metrics, self.redpanda.nodes, MetricsEndpoint.METRICS
        )
        result = {}
        for k in samples.keys():
            result[k] = result.get(k, 0) + sum(
                [int(s.value) for s in samples[k].samples]
            )
        return result["kafka_rpc_sasl_session_revoked_total"]


class RedpandaOIDCTestMethods(RedpandaOIDCTestBase):
    def __init__(self, test_context, **kwargs):
        super(RedpandaOIDCTestMethods, self).__init__(test_context, **kwargs)

    def _get_visible_topics(self, cfg: OAuthConfig) -> set[str]:
        """Get a fresh token and list visible topics."""
        k_client = PythonLibrdkafka(
            self.redpanda,
            algorithm="OAUTHBEARER",
            oauth_config=cfg,
            tls_cert=self.client_cert,
        )
        producer = k_client.get_producer()
        producer.poll(0.0)
        return set(producer.list_topics(timeout=5).topics.keys())

    def _setup_gbac_group(self, group_name: str) -> OAuthConfig:
        """Create a Keycloak group with the service user and return the
        OAuth config for the CLIENT_ID application."""
        client_id = CLIENT_ID
        self.create_service_user()

        self.keycloak.admin.create_group_mapper(client_id, use_full_path=False)
        self.keycloak.admin.create_group(group_name)
        self.keycloak.admin.add_service_user_to_group(client_id, group_name)

        cfg = self.keycloak.generate_oauth_config(self.keycloak.nodes[0], client_id)
        assert cfg.client_secret is not None
        assert cfg.token_endpoint is not None
        return cfg

    def _tls_config(
        self,
    ) -> tuple[str, tuple[str, str] | None, str | bool]:
        """Return (scheme, cert, ca_cert) for HTTP requests, handling
        both TLS and non-TLS configurations."""
        if self.client_cert is not None:
            return (
                "https",
                (self.client_cert.crt, self.client_cert.key),
                self.client_cert.ca.crt,
            )
        return ("http", None, True)

    @cluster(num_nodes=4)
    def test_init(self):
        kc_node = self.keycloak.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user()

        self.rpk.create_topic(EXAMPLE_TOPIC)
        self.rpk.sasl_allow_principal(
            f"User:{service_user_id}",
            ["all"],
            "topic",
            EXAMPLE_TOPIC,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )

        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        assert cfg.client_secret is not None
        assert cfg.token_endpoint is not None
        k_client = PythonLibrdkafka(
            self.redpanda,
            algorithm="OAUTHBEARER",
            oauth_config=cfg,
            tls_cert=self.client_cert,
        )
        producer = k_client.get_producer()

        # Explicit poll triggers OIDC token flow. Required for librdkafka
        # metadata requests to behave nicely.
        producer.poll(0.0)

        expected_topics = set([EXAMPLE_TOPIC])
        print(f"expected_topics: {expected_topics}")

        wait_until(
            lambda: set(producer.list_topics(timeout=5).topics.keys())
            == expected_topics,
            timeout_sec=5,
        )

        token = self.get_client_credentials_token(cfg)

        cert = None
        ca_cert = True
        scheme = "http"
        if self.client_cert is not None:
            scheme = "https"
            cert = (self.client_cert.crt, self.client_cert.key)
            ca_cert = self.client_cert.ca.crt

        def check_pp_topics():
            response = requests.get(
                url=f"{scheme}://{self.redpanda.nodes[0].account.hostname}:8082/topics",
                headers={
                    "Accept": "application/vnd.kafka.v2+json",
                    "Content-Type": "application/vnd.kafka.v2+json",
                    "Authorization": f"Bearer {token['access_token']}",
                },
                timeout=10,
                cert=cert,
                verify=ca_cert,
            )
            return (
                response.status_code == requests.codes.ok
                and set(response.json()) == expected_topics
            )

        def check_sr_subjects():
            response = requests.get(
                url=f"{scheme}://{self.redpanda.nodes[0].account.hostname}:8081/subjects",
                headers={
                    "Accept": "application/vnd.schemaregistry.v1+json",
                    "Authorization": f"Bearer {token['access_token']}",
                },
                timeout=10,
                cert=cert,
                verify=ca_cert,
            )
            return response.status_code == requests.codes.ok and response.json() == []

        wait_until(check_pp_topics, timeout_sec=10)

        wait_until(check_sr_subjects, timeout_sec=10)

    @cluster(num_nodes=4)
    def test_admin_whoami(self):
        kc_node = self.keycloak.nodes[0]
        rp_node = self.redpanda.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user(client_id)
        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        token = self.get_client_credentials_token(cfg)

        whoami_url = (
            f"http://{self.redpanda.admin_endpoint(rp_node)}/v1/security/oidc/whoami"
        )
        auth_header = {"Authorization": f"Bearer {token['access_token']}"}

        def request_whoami(with_auth: bool):
            response = requests.get(
                url=whoami_url, headers=auth_header if with_auth else None, timeout=5
            )
            self.redpanda.logger.info(
                f"response.status_code: {response.status_code}, response.content: {response.content}"
            )
            return response

        # At this point, admin API does not require auth and service_user_id is not a superuser

        response = request_whoami(with_auth=False)
        assert response.status_code == requests.codes.unauthorized

        response = request_whoami(with_auth=True)
        assert response.status_code == requests.codes.ok
        assert response.json()["id"] == service_user_id
        assert response.json()["expire"] > time.time()

        # Require Auth for Admin
        self.redpanda.set_cluster_config({"admin_api_require_auth": True})

        response = request_whoami(with_auth=False)
        assert response.status_code == requests.codes.unauthorized

        response = request_whoami(with_auth=True)
        assert response.status_code == requests.codes.ok
        assert response.json()["id"] == service_user_id
        assert response.json()["expire"] > time.time()

    @cluster(num_nodes=4)
    def test_admin_v2_resolve_oidc_identity(self):
        """
        Test the v2 admin API for resolving OIDC identities.
        This is the v2 equivalent of test_admin_whoami.
        """

        kc_node = self.keycloak.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user(client_id)
        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        token = self.get_client_credentials_token(cfg)

        def resolve_oidc_identity(
            with_auth: bool,
        ) -> security_pb2.ResolveOidcIdentityResponse:
            admin_v2 = AdminV2(self.redpanda)
            req = security_pb2.ResolveOidcIdentityRequest()
            return admin_v2.security().resolve_oidc_identity(
                req,
                extra_headers={"Authorization": f"Bearer {token['access_token']}"}
                if with_auth
                else None,
            )

        def verify_response(response: security_pb2.ResolveOidcIdentityResponse):
            assert response.principal == service_user_id, (
                f"Unexpected principal: {response.principal} != {service_user_id}"
            )

            now = datetime.datetime.now(datetime.timezone.utc)
            expire = response.expire.ToDatetime(tzinfo=datetime.timezone.utc)
            assert expire > now, f"Unexpected expire: {expire} <= {now}"

        # At this point, admin API does not require auth and service_user_id is not a superuser
        with expect_exception(
            ConnectError,
            lambda e: e.code == ConnectErrorCode.FAILED_PRECONDITION,
        ):
            _ = resolve_oidc_identity(with_auth=False)

        verify_response(resolve_oidc_identity(with_auth=True))

        # Require Auth for Admin
        self.redpanda.set_cluster_config({"admin_api_require_auth": True})

        with expect_exception(
            ConnectError,
            lambda e: e.code == ConnectErrorCode.UNAUTHENTICATED,
        ):
            _ = resolve_oidc_identity(with_auth=False)

        verify_response(resolve_oidc_identity(with_auth=True))

    @cluster(num_nodes=4)
    def test_admin_invalidate_keys(self):
        kc_node = self.keycloak.nodes[0]
        rp_node = self.redpanda.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user(client_id)
        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        token = self.get_client_credentials_token(cfg)

        invalidate_keys_url = f"http://{self.redpanda.admin_endpoint(rp_node)}/v1/security/oidc/keys/cache_invalidate"
        auth_header = {"Authorization": f"Bearer {token['access_token']}"}

        def request_cache_invalidate(with_auth: bool):
            response = requests.post(
                url=invalidate_keys_url,
                headers=auth_header if with_auth else None,
                timeout=5,
            )
            self.redpanda.logger.info(f"response.status_code: {response.status_code}")
            return response.status_code

        # At this point, admin API does not require auth and service_user_id is not a superuser

        assert request_cache_invalidate(with_auth=False) == requests.codes.ok
        assert request_cache_invalidate(with_auth=True) == requests.codes.ok

        # Require Auth for Admin
        self.redpanda.set_cluster_config({"admin_api_require_auth": True})

        assert request_cache_invalidate(with_auth=False) == requests.codes.forbidden
        assert request_cache_invalidate(with_auth=True) == requests.codes.forbidden

        # Add service_user_id as a superuser
        self.redpanda.set_cluster_config(
            {
                "superusers": [
                    self.redpanda.SUPERUSER_CREDENTIALS.username,
                    service_user_id,
                ]
            }
        )

        id_requests = self.get_idp_request_count([rp_node])

        assert request_cache_invalidate(with_auth=True) == requests.codes.ok

        assert id_requests < self.get_idp_request_count([rp_node])

    @cluster(num_nodes=4)
    def test_admin_v2_refresh_oidc_keys(self):
        """
        Test the v2 admin API for refreshing OIDC keys.
        This is the v2 equivalent of test_admin_invalidate_keys.
        """

        kc_node = self.keycloak.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user(client_id)
        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        token = self.get_client_credentials_token(cfg)

        def refresh_oidc_keys(
            with_auth: bool,
        ) -> security_pb2.RefreshOidcKeysResponse:
            admin_v2 = AdminV2(self.redpanda)
            req = security_pb2.RefreshOidcKeysRequest()
            return admin_v2.security().refresh_oidc_keys(
                req,
                extra_headers={"Authorization": f"Bearer {token['access_token']}"}
                if with_auth
                else None,
            )

        # At this point, admin API does not require auth and service_user_id is not a superuser
        # Both calls should succeed
        refresh_oidc_keys(with_auth=False)
        refresh_oidc_keys(with_auth=True)

        # Require Auth for Admin
        self.redpanda.set_cluster_config({"admin_api_require_auth": True})

        with expect_exception(
            ConnectError,
            lambda e: e.code == ConnectErrorCode.PERMISSION_DENIED,
        ):
            refresh_oidc_keys(with_auth=False)

        with expect_exception(
            ConnectError,
            lambda e: e.code == ConnectErrorCode.PERMISSION_DENIED,
        ):
            refresh_oidc_keys(with_auth=True)

        # Add service_user_id as a superuser
        self.redpanda.set_cluster_config(
            {
                "superusers": [
                    self.redpanda.SUPERUSER_CREDENTIALS.username,
                    service_user_id,
                ]
            }
        )

        idp_request_counts_before = {
            node: self.get_idp_request_count(nodes=[node])
            for node in self.redpanda.nodes
        }

        refresh_oidc_keys(with_auth=True)

        idp_request_counts_after = {
            node: self.get_idp_request_count(nodes=[node])
            for node in self.redpanda.nodes
        }

        for node in self.redpanda.nodes:
            before = idp_request_counts_before[node]
            after = idp_request_counts_after[node]
            assert before < after, (
                f"Expected more IdP requests on node {node.account.hostname}: before refresh: {before}, after refresh: {after}"
            )

    @cluster(num_nodes=4)
    def test_admin_revoke(self):
        FETCH_TIMEOUT_SEC = 10
        GROUP_ID = "test_admin_revoke"

        kc_node = self.keycloak.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user(client_id)
        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        token = self.get_client_credentials_token(cfg)
        auth_header = {"Authorization": f"Bearer {token['access_token']}"}

        def request_revoke(with_auth: bool, expected):
            for node in self.redpanda.nodes:
                revoke_url = f"http://{self.redpanda.admin_endpoint(node)}/v1/security/oidc/revoke"

                response = requests.post(
                    url=revoke_url,
                    headers=auth_header if with_auth else None,
                    timeout=5,
                )
                self.redpanda.logger.info(
                    f"response.status_code: {response.status_code}"
                )
                assert response.status_code == expected

        # At this point, admin API does not require auth and service_user_id is not a superuser

        request_revoke(with_auth=False, expected=requests.codes.ok)
        request_revoke(with_auth=True, expected=requests.codes.ok)

        # Require Auth for Admin
        self.redpanda.set_cluster_config({"admin_api_require_auth": True})

        request_revoke(with_auth=False, expected=requests.codes.forbidden)
        request_revoke(with_auth=True, expected=requests.codes.forbidden)

        # Add service_user_id as a superuser
        self.redpanda.set_cluster_config(
            {
                "superusers": [
                    self.redpanda.SUPERUSER_CREDENTIALS.username,
                    service_user_id,
                ]
            }
        )

        self.rpk.create_topic(EXAMPLE_TOPIC)
        expected_topics = set([EXAMPLE_TOPIC])
        wait_until(
            lambda: set(self.rpk.list_topics()) == expected_topics, timeout_sec=10
        )

        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        k_client = PythonLibrdkafka(
            self.redpanda,
            algorithm="OAUTHBEARER",
            oauth_config=cfg,
            tls_cert=self.client_cert,
        )

        consumer = k_client.get_consumer(extra_config={"group.id": GROUP_ID})
        producer = k_client.get_producer()

        self.redpanda.logger.debug("starting producer")
        producer.poll(1.0)
        wait_until(
            lambda: set(producer.list_topics(timeout=10).topics.keys())
            == expected_topics,
            timeout_sec=5,
        )

        def consume_one():
            self.redpanda.logger.debug("starting consumer")
            rec = consumer.poll(FETCH_TIMEOUT_SEC)
            self.redpanda.logger.debug(f"consumed: {rec}")
            return rec

        def has_group():
            groups = self.rpk.group_describe(group=GROUP_ID, summary=True)
            return groups.members == 1 and groups.state == "Stable"

        self.redpanda.logger.debug("starting consumer.subscribe")
        consumer.subscribe([EXAMPLE_TOPIC])
        self.redpanda.logger.debug("consumer.subscribed")
        rec = consumer.poll(1.0)
        assert rec is None

        wait_until(has_group, timeout_sec=30, backoff_sec=1)

        t1 = threading.Thread(target=consume_one)
        t1.start()

        revoked_total = self.get_sasl_session_revoked_total()

        time.sleep(5)

        self.redpanda.logger.debug("starting final revoke")
        request_revoke(with_auth=True, expected=requests.codes.ok)

        self.redpanda.logger.debug("starting producer")
        producer.produce(topic=EXAMPLE_TOPIC, key="bar", value="23")
        producer.flush(timeout=5)
        self.redpanda.logger.debug("produced 1")

        self.redpanda.logger.debug("joining consumer thread")
        t1.join()
        self.redpanda.logger.debug("joined consumer thread")

        wait_until(
            lambda: revoked_total < self.get_sasl_session_revoked_total(),
            timeout_sec=10,
            backoff_sec=1,
        )

        consumer.close()

    @cluster(num_nodes=4)
    def test_admin_v2_revoke_oidc_sessions(self):
        FETCH_TIMEOUT_SEC = 10
        GROUP_ID = "test_admin_revoke"

        kc_node = self.keycloak.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user(client_id)
        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        token = self.get_client_credentials_token(cfg)

        def request_revoke_oidc_sessions(
            with_auth: bool,
        ) -> security_pb2.RevokeOidcSessionsResponse:
            admin_v2 = AdminV2(self.redpanda)
            req = security_pb2.RevokeOidcSessionsRequest()
            return admin_v2.security().revoke_oidc_sessions(
                req,
                extra_headers={"Authorization": f"Bearer {token['access_token']}"}
                if with_auth
                else None,
            )

        # At this point, admin API does not require auth and service_user_id is not a superuser
        # Both calls should succeed
        request_revoke_oidc_sessions(with_auth=False)
        request_revoke_oidc_sessions(with_auth=True)

        # Require Auth for Admin
        self.redpanda.set_cluster_config({"admin_api_require_auth": True})

        with expect_exception(
            ConnectError,
            lambda e: e.code == ConnectErrorCode.PERMISSION_DENIED,
        ):
            request_revoke_oidc_sessions(with_auth=False)

        with expect_exception(
            ConnectError,
            lambda e: e.code == ConnectErrorCode.PERMISSION_DENIED,
        ):
            request_revoke_oidc_sessions(with_auth=True)

        # Add service_user_id as a superuser
        self.redpanda.set_cluster_config(
            {
                "superusers": [
                    self.redpanda.SUPERUSER_CREDENTIALS.username,
                    service_user_id,
                ]
            }
        )

        self.rpk.create_topic(EXAMPLE_TOPIC)
        expected_topics = set([EXAMPLE_TOPIC])
        wait_until(
            lambda: set(self.rpk.list_topics()) == expected_topics,
            timeout_sec=10,
            err_msg=f"Expected topics: {expected_topics}, got: {self.rpk.list_topics()}",
        )

        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        k_client = PythonLibrdkafka(
            self.redpanda,
            algorithm="OAUTHBEARER",
            oauth_config=cfg,
            tls_cert=self.client_cert,
        )

        consumer = k_client.get_consumer(extra_config={"group.id": GROUP_ID})
        producer = k_client.get_producer()

        self.redpanda.logger.debug("starting producer")
        producer.poll(1.0)
        wait_until(
            lambda: set(producer.list_topics(timeout=10).topics.keys())
            == expected_topics,
            timeout_sec=5,
            err_msg=f"Producer topics do not match expected topics: {expected_topics}",
        )

        def consume_one():
            self.redpanda.logger.debug("starting consumer")
            rec = consumer.poll(FETCH_TIMEOUT_SEC)
            self.redpanda.logger.debug(f"consumed: {rec}")
            return rec

        def has_group():
            groups = self.rpk.group_describe(group=GROUP_ID, summary=True)
            return groups.members == 1 and groups.state == "Stable"

        self.redpanda.logger.debug("starting consumer.subscribe")
        consumer.subscribe([EXAMPLE_TOPIC])
        self.redpanda.logger.debug("consumer.subscribed")
        rec = consumer.poll(1.0)
        assert rec is None, f"Expected no record, got: {rec}"

        wait_until(
            has_group,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Consumer group did not reach expected state",
        )

        t1 = threading.Thread(target=consume_one)
        t1.start()

        revoked_total = self.get_sasl_session_revoked_total()

        time.sleep(5)

        self.redpanda.logger.debug("starting final revoke")
        request_revoke_oidc_sessions(with_auth=True)

        self.redpanda.logger.debug("starting producer")
        producer.produce(topic=EXAMPLE_TOPIC, key="bar", value="23")
        producer.flush(timeout=5)
        self.redpanda.logger.debug("produced 1")

        self.redpanda.logger.debug("joining consumer thread")
        t1.join()
        self.redpanda.logger.debug("joined consumer thread")

        wait_until(
            lambda: revoked_total < self.get_sasl_session_revoked_total(),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Expected sasl_session_revoked_total to increase after OIDC sessions revoke",
        )

        consumer.close()

    @cluster(num_nodes=4)
    @matrix(
        full_group=[True, False],
        nested_group_mode=get_nested_group_types(),
    )
    def test_group_claim(self, full_group: bool, nested_group_mode: NestedGroupType):
        """
        Test that group claim mapping works as expected for topic authorization.

        This test verifies that OIDC group claims from the identity provider (Keycloak)
        are correctly mapped to Redpanda ACL principals, allowing group-based authorization.

        Parameters:
            full_group: When True, Keycloak includes the full group path (e.g., "/test-group")
                       in the token. When False, only the group name is included.
            nested_group_mode: Controls how Redpanda handles nested group paths:
                              - NONE: Group paths are used as-is
                              - SUFFIX: Only the leaf group name is used

        Test flow:
        1. Configure Redpanda's nested_group_behavior setting
        2. Create a service user in Keycloak with a group mapper
        3. Create a group and add the service user to it
        4. Create a topic and grant access via a Group:* ACL principal
        5. Authenticate using OIDC and verify the user can access the topic
        6. Verify the resolved OIDC identity includes the expected group
        """
        kc_node = self.keycloak.nodes[0]

        # Determine the qualified group name based on test parameters.
        # When full_group=True and nested_group_mode=NONE, Keycloak returns "/test-group"
        # Otherwise, just "test-group" is used.
        group_name = "test-group"
        qualified_group_name = f"{'/' if full_group and nested_group_mode == NestedGroupType.NONE else ''}{group_name}"
        group_acl = f"Group:{qualified_group_name}"

        self.logger.debug(f'Qualified group name: "{qualified_group_name}"')
        self.logger.debug(f'Group ACL: "{group_acl}"')

        # Configure how Redpanda handles nested group paths from OIDC tokens
        self.redpanda.set_cluster_config(
            {"nested_group_behavior": nested_group_mode.value}
        )

        # Set up the OIDC client and service user in Keycloak
        client_id = CLIENT_ID
        self.create_service_user()

        # Create a group mapper that includes group membership in the access token.
        # use_full_path determines whether the full path ("/group") or just name ("group") is included.
        self.keycloak.admin.create_group_mapper(client_id, full_group)

        # Create the group and add the service account to it
        self.keycloak.admin.create_group(group_name)
        self.keycloak.admin.add_service_user_to_group(client_id, group_name)

        # Create a topic and grant access to it via the group ACL principal.
        # This allows any user with the matching group claim to access the topic.
        self.rpk.create_topic(EXAMPLE_TOPIC)
        self.rpk.sasl_allow_principal(
            group_acl,
            ["all"],
            "topic",
            EXAMPLE_TOPIC,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )

        # Create a Kafka client that authenticates using OIDC
        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        token = self.get_client_credentials_token(cfg)
        assert cfg.client_secret is not None
        assert cfg.token_endpoint is not None
        k_client = PythonLibrdkafka(
            self.redpanda,
            algorithm="OAUTHBEARER",
            oauth_config=cfg,
            tls_cert=self.client_cert,
        )
        producer = k_client.get_producer()

        # Explicit poll triggers OIDC token flow. Required for librdkafka
        # metadata requests to behave nicely.
        producer.poll(0.0)

        # Verify the user can see the topic (proving group-based authorization works)
        expected_topics = set([EXAMPLE_TOPIC])
        self.logger.debug(f"expected_topics: {expected_topics}")

        wait_until(
            lambda: set(producer.list_topics(timeout=5).topics.keys())
            == expected_topics,
            timeout_sec=5,
            err_msg="Failed to list topics using group claim for authorization",
        )

        # Verify the resolved OIDC identity includes the expected group claim
        def resolve_oidc_identity(
            token: dict,
        ) -> security_pb2.ResolveOidcIdentityResponse:
            admin_v2 = AdminV2(self.redpanda)
            req = security_pb2.ResolveOidcIdentityRequest()
            self.logger.debug(f'Using access token "{token["access_token"]}"')
            return admin_v2.security().resolve_oidc_identity(
                req,
                extra_headers={"Authorization": f"Bearer {token['access_token']}"},
            )

        resp = resolve_oidc_identity(token=token)
        assert resp.groups == [qualified_group_name], (
            f"Unexpected groups: {resp.groups}, did not match {[qualified_group_name]}"
        )

    @cluster(num_nodes=4)
    def test_group_membership_change(self):
        """
        Test that changing group membership dynamically changes topic access permissions.

        This test:
        1. Creates two topics (topic1 and topic2) and three groups (group1, group2, group3)
        2. Sets up ACLs so group1 can access topic1 and group2 can access topic2 (group3 has no permissions)
        3. Adds service user to group1, verifies it can see topic1 but not topic2
        4. Removes service user from group1, adds to group2
        5. Verifies service user can now see topic2 but not topic1
        6. Removes service user from group2, adds to group3 (no permissions)
        7. Verifies service user cannot see any topics
        8. Adds service user to all groups, verifies it can see both topics
        """
        kc_node = self.keycloak.nodes[0]

        topic1 = "topic1"
        topic2 = "topic2"
        group1 = "group1"
        group2 = "group2"
        group3 = "group3"

        client_id = CLIENT_ID
        self.create_service_user()

        # Create group mapper (use full path = False for simpler group names)
        self.keycloak.admin.create_group_mapper(client_id, use_full_path=False)

        # Create all groups in Keycloak
        self.keycloak.admin.create_group(group1)
        self.keycloak.admin.create_group(group2)
        self.keycloak.admin.create_group(group3)

        # Create both topics
        self.rpk.create_topic(topic1)
        self.rpk.create_topic(topic2)

        # Set up ACLs: group1 can describe topic1, group2 can describe topic2
        # group3 has no permissions
        self.rpk.sasl_allow_principal(
            f"Group:{group1}",
            ["describe"],
            "topic",
            topic1,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )
        self.rpk.sasl_allow_principal(
            f"Group:{group2}",
            ["describe"],
            "topic",
            topic2,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )

        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        assert cfg.client_secret is not None
        assert cfg.token_endpoint is not None

        # Phase 1: Add service user to group1
        self.logger.info("Phase 1: Adding service user to group1")
        self.keycloak.admin.add_service_user_to_group(client_id, group1)

        # Verify service user can see topic1 but not topic2
        wait_until(
            lambda: self._get_visible_topics(cfg) == {topic1},
            timeout_sec=10,
            backoff_sec=1,
            err_msg=f"Expected to see only {topic1} when in group1, got: {self._get_visible_topics(cfg)}",
        )
        self.logger.info("Verified: service user in group1 can see topic1 only")

        # Phase 2: Remove from group1, add to group2
        self.logger.info("Phase 2: Removing service user from group1, adding to group2")
        self.keycloak.admin.remove_service_user_from_group(client_id, group1)
        self.keycloak.admin.add_service_user_to_group(client_id, group2)

        # Verify service user can now see topic2 but not topic1
        wait_until(
            lambda: self._get_visible_topics(cfg) == {topic2},
            timeout_sec=10,
            backoff_sec=1,
            err_msg=f"Expected to see only {topic2} when in group2, got: {self._get_visible_topics(cfg)}",
        )
        self.logger.info("Verified: service user in group2 can see topic2 only")

        # Phase 3: Remove from group2, add to group3 (no permissions)
        self.logger.info(
            "Phase 3: Removing service user from group2, adding to group3 (no permissions)"
        )
        self.keycloak.admin.remove_service_user_from_group(client_id, group2)
        self.keycloak.admin.add_service_user_to_group(client_id, group3)

        # Verify service user cannot see any topics
        wait_until(
            lambda: self._get_visible_topics(cfg) == set(),
            timeout_sec=10,
            backoff_sec=1,
            err_msg=f"Expected to see no topics when in group3, got: {self._get_visible_topics(cfg)}",
        )
        self.logger.info(
            "Verified: service user in group3 (no permissions) cannot see any topics"
        )

        # Phase 4: Add service user to all groups
        self.logger.info("Phase 4: Adding service user to all groups")
        self.keycloak.admin.add_service_user_to_group(client_id, group1)
        self.keycloak.admin.add_service_user_to_group(client_id, group2)

        # Verify service user can see both topics
        wait_until(
            lambda: self._get_visible_topics(cfg) == {topic1, topic2},
            timeout_sec=10,
            backoff_sec=1,
            err_msg=f"Expected to see both topics when in all groups, got: {self._get_visible_topics(cfg)}",
        )
        self.logger.info("Verified: service user in all groups can see both topics")

        # Verify the resolved OIDC identity includes all three groups
        token = self.get_client_credentials_token(cfg)
        admin_v2 = AdminV2(self.redpanda)
        req = security_pb2.ResolveOidcIdentityRequest()
        resp = admin_v2.security().resolve_oidc_identity(
            req,
            extra_headers={"Authorization": f"Bearer {token['access_token']}"},
        )
        assert set(resp.groups) == {group1, group2, group3}, (
            f"Expected groups {[group1, group2, group3]}, got {resp.groups}"
        )
        self.logger.info(
            f"Verified: resolved OIDC identity includes all three groups: {resp.groups}"
        )

    @cluster(num_nodes=4)
    def test_group_role_authorization(self):
        """
        Test that group-role membership is honored for authorization.

        This test verifies that when a group is added as a member of a role,
        users in that group receive the permissions granted to the role.

        Test flow:
        1. Create a service user in Keycloak and add it to a group
        2. Create a role in Redpanda and add the group as a member
        3. Grant topic access to the role via ACLs
        4. Verify the user (via group -> role path) can access the topic
        """
        kc_node = self.keycloak.nodes[0]

        role_name = "test-role"
        group_name = "test-group"
        topic_name = "group-role-topic"

        # Set up the OIDC client and service user
        client_id = CLIENT_ID
        self.create_service_user()

        # Create group mapper (use full path = False for simpler group names)
        self.keycloak.admin.create_group_mapper(client_id, use_full_path=False)

        # Create the group and add the service account to it
        self.keycloak.admin.create_group(group_name)
        self.keycloak.admin.add_service_user_to_group(client_id, group_name)

        # Create a topic
        self.rpk.create_topic(topic_name)

        # Create a role and add the group as a member using the v2 Admin API
        admin_v2 = AdminV2(
            self.redpanda,
            auth=(self.su_username, self.su_password),
        )

        # Create role with group as member
        role = security_pb2.Role(
            name=role_name,
            members=[
                security_pb2.RoleMember(group=security_pb2.RoleGroup(name=group_name))
            ],
        )
        admin_v2.security().create_role(security_pb2.CreateRoleRequest(role=role))
        self.logger.info(
            f"Created role '{role_name}' with group '{group_name}' as member"
        )

        # Grant describe permission to the role via ACL
        self.rpk.sasl_allow_role(
            role_name,
            ["describe"],
            "topic",
            topic_name,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )
        self.logger.info(
            f"Granted describe permission to RedpandaRole:{role_name} on {topic_name}"
        )

        # Create a Kafka client that authenticates using OIDC
        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        assert cfg.client_secret is not None
        assert cfg.token_endpoint is not None

        # Verify user can see the topic via the group -> role authorization path
        wait_until(
            lambda: topic_name in self._get_visible_topics(cfg),
            timeout_sec=10,
            backoff_sec=1,
            err_msg=f"Expected to see {topic_name} via group->role authorization, got: {self._get_visible_topics(cfg)}",
        )
        self.logger.info(
            f"Verified: user in group '{group_name}' can access topic via role '{role_name}'"
        )

    @cluster(num_nodes=4)
    def test_group_role_deny_takes_precedence(self):
        """
        Test that deny permissions via group-role path take precedence.

        This test verifies that when a group is added as a member of a role
        with deny permissions, the deny takes precedence over direct group allows.

        Test flow:
        1. Create a service user in Keycloak and add it to a group
        2. Create a role with deny permission and add the group as a member
        3. Grant allow permission directly to the group
        4. Verify the user is denied access (deny via role takes precedence)
        """
        kc_node = self.keycloak.nodes[0]

        role_name = "deny-role"
        group_name = "deny-test-group"
        topic_name = "deny-test-topic"

        # Set up the OIDC client and service user
        client_id = CLIENT_ID
        self.create_service_user()

        # Create group mapper
        self.keycloak.admin.create_group_mapper(client_id, use_full_path=False)

        # Create the group and add the service account to it
        self.keycloak.admin.create_group(group_name)
        self.keycloak.admin.add_service_user_to_group(client_id, group_name)

        # Create a topic
        self.rpk.create_topic(topic_name)

        # Create a role with the group as a member
        admin_v2 = AdminV2(
            self.redpanda,
            auth=(self.su_username, self.su_password),
        )

        role = security_pb2.Role(
            name=role_name,
            members=[
                security_pb2.RoleMember(group=security_pb2.RoleGroup(name=group_name))
            ],
        )
        admin_v2.security().create_role(security_pb2.CreateRoleRequest(role=role))
        self.logger.info(
            f"Created role '{role_name}' with group '{group_name}' as member"
        )

        # Grant allow permission directly to the group
        group_principal = f"Group:{group_name}"
        self.rpk.sasl_allow_principal(
            group_principal,
            ["describe"],
            "topic",
            topic_name,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )
        self.logger.info(
            f"Granted describe permission to {group_principal} on {topic_name}"
        )

        # Grant deny permission to the role
        self.rpk.sasl_deny_role(
            role_name,
            ["describe"],
            "topic",
            topic_name,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )
        self.logger.info(
            f"Denied describe permission to RedpandaRole:{role_name} on {topic_name}"
        )

        # Create a Kafka client that authenticates using OIDC
        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)

        # Wait a bit to ensure ACLs propagate, then verify user cannot see the topic
        # (deny via role should take precedence over group allow)
        time.sleep(3)
        visible = self._get_visible_topics(cfg)
        assert topic_name not in visible, (
            f"Expected topic '{topic_name}' to NOT be visible due to role deny, "
            f"but it was visible. Deny via group->role should take precedence."
        )
        self.logger.info(
            f"Verified: deny via role '{role_name}' takes precedence over group allow"
        )

    @cluster(num_nodes=4)
    def test_group_role_multiple_groups_in_role(self):
        """
        Test authorization when multiple groups are members of the same role.

        This test verifies that users from different groups that are all members
        of the same role get the permissions granted to that role.

        Test flow:
        1. Create two groups and add them both to the same role
        2. Grant permissions to the role
        3. Verify users in either group can access the resource
        """
        kc_node = self.keycloak.nodes[0]

        role_name = "multi-group-role"
        group1_name = "multi-group-1"
        group2_name = "multi-group-2"
        topic_name = "multi-group-topic"

        # Set up the OIDC client and service user
        client_id = CLIENT_ID
        self.create_service_user()

        # Create group mapper
        self.keycloak.admin.create_group_mapper(client_id, use_full_path=False)

        # Create both groups
        self.keycloak.admin.create_group(group1_name)
        self.keycloak.admin.create_group(group2_name)

        # Create a topic
        self.rpk.create_topic(topic_name)

        # Create a role with both groups as members
        admin_v2 = AdminV2(
            self.redpanda,
            auth=(self.su_username, self.su_password),
        )

        role = security_pb2.Role(
            name=role_name,
            members=[
                security_pb2.RoleMember(group=security_pb2.RoleGroup(name=group1_name)),
                security_pb2.RoleMember(group=security_pb2.RoleGroup(name=group2_name)),
            ],
        )
        admin_v2.security().create_role(security_pb2.CreateRoleRequest(role=role))
        self.logger.info(
            f"Created role '{role_name}' with groups '{group1_name}' and '{group2_name}' as members"
        )

        # Grant permission to the role
        self.rpk.sasl_allow_role(
            role_name,
            ["describe"],
            "topic",
            topic_name,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )

        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)

        # Test with user in group1
        self.logger.info(f"Testing with user in {group1_name}")
        self.keycloak.admin.add_service_user_to_group(client_id, group1_name)

        wait_until(
            lambda: topic_name in self._get_visible_topics(cfg),
            timeout_sec=10,
            backoff_sec=1,
            err_msg=f"User in {group1_name} should see {topic_name} via role",
        )
        self.logger.info(f"Verified: user in {group1_name} can access topic via role")

        # Remove from group1, add to group2
        self.logger.info(f"Switching user to {group2_name}")
        self.keycloak.admin.remove_service_user_from_group(client_id, group1_name)
        self.keycloak.admin.add_service_user_to_group(client_id, group2_name)

        wait_until(
            lambda: topic_name in self._get_visible_topics(cfg),
            timeout_sec=10,
            backoff_sec=1,
            err_msg=f"User in {group2_name} should see {topic_name} via role",
        )
        self.logger.info(f"Verified: user in {group2_name} can access topic via role")

    @cluster(num_nodes=4)
    def test_group_in_multiple_roles(self):
        """
        Test authorization when a group is a member of multiple roles.

        This test verifies that when a group is added to multiple roles,
        users in that group get permissions from all those roles.

        Test flow:
        1. Create a group and add it to two different roles
        2. Grant different permissions to each role (role1 -> topic1, role2 -> topic2)
        3. Verify the user can access both topics via the different roles
        """
        kc_node = self.keycloak.nodes[0]

        role1_name = "role-for-topic1"
        role2_name = "role-for-topic2"
        group_name = "multi-role-group"
        topic1_name = "multi-role-topic1"
        topic2_name = "multi-role-topic2"

        # Set up the OIDC client and service user
        client_id = CLIENT_ID
        self.create_service_user()

        # Create group mapper
        self.keycloak.admin.create_group_mapper(client_id, use_full_path=False)

        # Create the group and add the service user to it
        self.keycloak.admin.create_group(group_name)
        self.keycloak.admin.add_service_user_to_group(client_id, group_name)

        # Create both topics
        self.rpk.create_topic(topic1_name)
        self.rpk.create_topic(topic2_name)

        # Create both roles with the group as a member
        admin_v2 = AdminV2(
            self.redpanda,
            auth=(self.su_username, self.su_password),
        )

        group_member = security_pb2.RoleMember(
            group=security_pb2.RoleGroup(name=group_name)
        )

        role1 = security_pb2.Role(name=role1_name, members=[group_member])
        admin_v2.security().create_role(security_pb2.CreateRoleRequest(role=role1))

        role2 = security_pb2.Role(name=role2_name, members=[group_member])
        admin_v2.security().create_role(security_pb2.CreateRoleRequest(role=role2))

        self.logger.info(
            f"Created roles '{role1_name}' and '{role2_name}' with group '{group_name}' as member"
        )

        # Grant role1 access to topic1, role2 access to topic2
        self.rpk.sasl_allow_role(
            role1_name,
            ["describe"],
            "topic",
            topic1_name,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )
        self.rpk.sasl_allow_role(
            role2_name,
            ["describe"],
            "topic",
            topic2_name,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )

        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)

        # Verify user can see both topics via the different role paths
        wait_until(
            lambda: {topic1_name, topic2_name}.issubset(self._get_visible_topics(cfg)),
            timeout_sec=10,
            backoff_sec=1,
            err_msg=f"Expected to see both topics via different roles, got: {self._get_visible_topics(cfg)}",
        )
        self.logger.info(
            f"Verified: user in group '{group_name}' can access both topics via different roles"
        )

    @cluster(num_nodes=4)
    def test_group_kafka_api_coverage(self):
        """
        Test group-based access control across core Kafka API operations.

        Covers:
        - Metadata request (authorized group) -> visible
        - Metadata request (unauthorized group) -> not visible
        - Produce to allowed topic -> succeeds
        - Produce to denied topic -> denied
        - Consume from allowed topic -> succeeds
        - Consume from denied topic -> denied

        Setup:
        - Two topics: allowed-topic (group has all permissions) and
          denied-topic (group has explicit deny ACL)
        - Service user is a member of kafka-api-group
        - Consumer group resource kafka-api-consumer-group is allowed
        """
        allowed_topic = "allowed-topic"
        denied_topic = "denied-topic"
        group_name = "kafka-api-group"
        consumer_group_id = "kafka-api-consumer-group"

        cfg = self._setup_gbac_group(group_name)

        self.rpk.create_topic(allowed_topic)
        self.rpk.create_topic(denied_topic)

        # Produce a record to denied-topic via superuser so there is data
        # to attempt to consume later in Phase 3.
        self.rpk.produce(denied_topic, "setup-key", "setup-value")

        group_principal = f"Group:{group_name}"

        # Allow all on allowed-topic
        self.rpk.sasl_allow_principal(group_principal, ["all"], "topic", allowed_topic)

        # Explicit deny all on denied-topic
        self.rpk.sasl_deny_principal(group_principal, ["all"], "topic", denied_topic)

        # Allow consumer group resource for consume phase
        self.rpk.sasl_allow_principal(
            group_principal, ["all"], "group", consumer_group_id
        )

        self.logger.info("Phase 1: Metadata visibility")

        def check_topic_visibility():
            visible = self._get_visible_topics(cfg)
            return allowed_topic in visible and denied_topic not in visible, visible

        visible = wait_until_result(
            check_topic_visibility,
            timeout_sec=10,
            backoff_sec=1,
            err_msg=(
                f"Expected {allowed_topic} to be visible and "
                f"{denied_topic} to be invisible"
            ),
        )
        self.logger.info(f"Phase 1 passed: visible topics = {visible}")

        self.logger.info("Phase 2: Produce authorization")

        k_client = PythonLibrdkafka(
            self.redpanda,
            algorithm="OAUTHBEARER",
            oauth_config=cfg,
            tls_cert=self.client_cert,
        )
        producer = k_client.get_producer()
        producer.poll(0.0)

        # Produce to allowed topic should succeed
        def produce_to_allowed():
            errors: list[str] = []

            def on_delivery(err, msg):
                if err is not None:
                    errors.append(str(err))

            producer.produce(
                topic=allowed_topic,
                key="test-key",
                value="test-value",
                on_delivery=on_delivery,
            )
            producer.flush(timeout=10)
            return len(errors) == 0

        wait_until(
            produce_to_allowed,
            timeout_sec=10,
            backoff_sec=1,
            err_msg=f"Failed to produce to {allowed_topic}",
        )
        self.logger.info("Produce to allowed topic succeeded")

        # Produce to denied topic should fail
        denied_produce_errors: list = []

        def on_delivery_denied(err, msg):
            denied_produce_errors.append(err)

        producer.produce(
            topic=denied_topic,
            key="test-key",
            value="test-value",
            on_delivery=on_delivery_denied,
        )
        producer.flush(timeout=10)
        assert len(denied_produce_errors) == 1, (
            f"Expected exactly one delivery callback, got {len(denied_produce_errors)}"
        )
        assert denied_produce_errors[0] is not None, (
            "Expected delivery error for denied topic, got None"
        )
        assert (
            denied_produce_errors[0].code() == KafkaError.TOPIC_AUTHORIZATION_FAILED
        ), f"Expected TOPIC_AUTHORIZATION_FAILED, got {denied_produce_errors[0]}"
        self.logger.info(
            f"Produce to denied topic failed with {denied_produce_errors[0]}"
        )

        self.logger.info("Phase 3: Consume authorization")

        # Consume from allowed topic should succeed
        k_client_consumer = PythonLibrdkafka(
            self.redpanda,
            algorithm="OAUTHBEARER",
            oauth_config=cfg,
            tls_cert=self.client_cert,
        )
        consumer = k_client_consumer.get_consumer(
            extra_config={
                "group.id": consumer_group_id,
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([allowed_topic])

        def poll_allowed():
            rec = consumer.poll(timeout=5)
            return rec is not None and rec.error() is None

        wait_until(
            poll_allowed,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Failed to consume record from allowed topic",
        )
        consumer.close()
        self.logger.info("Consumed from allowed topic")

        # Consume from denied topic should fail
        denied_consumer = k_client_consumer.get_consumer(
            extra_config={"group.id": consumer_group_id}
        )
        denied_consumer.subscribe([denied_topic])

        rec = denied_consumer.poll(timeout=5)
        denied_consumer.close()

        assert rec is not None, "Expected error when consuming from denied topic"
        err = rec.error()
        assert err is not None, "Expected error when consuming from denied topic"
        assert err.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED, (
            f"Expected TOPIC_AUTHORIZATION_FAILED, got {err}"
        )
        self.logger.info("Denied topic consumption correctly denied with auth error")

    @cluster(num_nodes=4)
    def test_group_pandaproxy_api_coverage(self):
        """
        Test group-based access control through the Pandaproxy HTTP API.

        Covers:
        - Produce to allowed topic -> succeeds (200, offsets returned)
        - Consume from allowed topic -> succeeds (200, records returned)

        TODO(andrew): Exercise the deny path (produce/consume on a denied
        topic) once CORE-15764 is resolved.

        Setup:
        - Keycloak group "proxy-api-group" with service user as member
        - Topic "allowed-topic-pp" with ACL granting all permissions to
          Group:proxy-api-group
        """
        allowed_topic = "allowed-topic-pp"
        group_name = "proxy-api-group"

        cfg = self._setup_gbac_group(group_name)

        self.rpk.create_topic(allowed_topic)

        group_principal = f"Group:{group_name}"

        # Allow all on allowed-topic-pp
        self.rpk.sasl_allow_principal(
            group_principal,
            ["all"],
            "topic",
            allowed_topic,
        )

        token = self.get_client_credentials_token(cfg)

        scheme, cert, ca_cert = self._tls_config()

        hostname = self.redpanda.nodes[0].account.hostname
        base_url = f"{scheme}://{hostname}:8082"

        auth_header = {"Authorization": f"Bearer {token['access_token']}"}

        produce_headers = {
            "Accept": "application/vnd.kafka.v2+json",
            "Content-Type": "application/vnd.kafka.json.v2+json",
            **auth_header,
        }

        fetch_headers = {
            "Accept": "application/vnd.kafka.json.v2+json",
            **auth_header,
        }

        produce_data = json.dumps(
            {"records": [{"value": "test-value", "partition": 0}]}
        )

        self.logger.info("Phase 1: Produce authorization")

        # Produce to allowed topic should succeed
        def produce_to_allowed():
            res = requests.post(
                f"{base_url}/topics/{allowed_topic}",
                produce_data,
                headers=produce_headers,
                timeout=10,
                cert=cert,
                verify=ca_cert,
            )
            if res.status_code != 200:
                return False
            offsets = res.json().get("offsets", [])
            return len(offsets) > 0 and offsets[0].get("offset", -1) >= 0

        wait_until(
            produce_to_allowed,
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Failed to produce to allowed topic via Pandaproxy",
        )
        self.logger.info("Produce to allowed topic succeeded")

        self.logger.info("Phase 2: Consume authorization")

        # Fetch from allowed topic should succeed
        def fetch_from_allowed():
            res = requests.get(
                f"{base_url}/topics/{allowed_topic}/partitions/0/records"
                f"?offset=0&max_bytes=1024&timeout=1000",
                headers=fetch_headers,
                timeout=10,
                cert=cert,
                verify=ca_cert,
            )
            if res.status_code != 200:
                return False
            records = res.json()
            return isinstance(records, list) and len(records) > 0

        wait_until(
            fetch_from_allowed,
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Failed to fetch from allowed topic via Pandaproxy",
        )
        self.logger.info("Fetch from allowed topic succeeded")

    @cluster(num_nodes=4)
    def test_group_schema_registry_api_coverage(self):
        """
        Test group-based access control through the Schema Registry HTTP API.

        Covers:
        - Register schema on allowed subject -> succeeds (200, schema ID)
        - Register schema on denied subject -> denied (403)
        - Read schema on allowed subject -> succeeds (200, versions returned)
        - Read schema on denied subject -> denied (403)

        Setup:
        - Two subjects: allowed-subject (group has WRITE+READ) and
          denied-subject (group has explicit deny WRITE+READ)
        - Service user is a member of sr-api-group
        - ACLs are managed via SR /security/acls endpoint
        """
        allowed_subject = "allowed-subject"
        denied_subject = "denied-subject"
        group_name = "sr-api-group"

        cfg = self._setup_gbac_group(group_name)
        token = self.get_client_credentials_token(cfg)

        scheme, cert, ca_cert = self._tls_config()

        hostname = self.redpanda.nodes[0].account.hostname
        base_url = f"{scheme}://{hostname}:8081"

        super_auth = (self.su_username, self.su_password)

        # Enable Schema Registry authorization so ACLs are enforced
        self.redpanda.set_cluster_config(
            {"schema_registry_enable_authorization": "True"}
        )

        # --- Create SR ACLs via /security/acls ---

        group_principal = f"Group:{group_name}"

        acls = [
            # Allow write+read on allowed-subject
            {
                "principal": group_principal,
                "resource": allowed_subject,
                "resource_type": "SUBJECT",
                "pattern_type": "LITERAL",
                "host": "*",
                "operation": "WRITE",
                "permission": "ALLOW",
            },
            {
                "principal": group_principal,
                "resource": allowed_subject,
                "resource_type": "SUBJECT",
                "pattern_type": "LITERAL",
                "host": "*",
                "operation": "READ",
                "permission": "ALLOW",
            },
            # Deny write+read on denied-subject
            {
                "principal": group_principal,
                "resource": denied_subject,
                "resource_type": "SUBJECT",
                "pattern_type": "LITERAL",
                "host": "*",
                "operation": "WRITE",
                "permission": "DENY",
            },
            {
                "principal": group_principal,
                "resource": denied_subject,
                "resource_type": "SUBJECT",
                "pattern_type": "LITERAL",
                "host": "*",
                "operation": "READ",
                "permission": "DENY",
            },
        ]

        res = requests.post(
            f"{base_url}/security/acls",
            json=acls,
            headers={"Content-Type": "application/json"},
            auth=super_auth,
            timeout=10,
            cert=cert,
            verify=ca_cert,
        )
        assert res.status_code == 201, (
            f"Failed to create SR ACLs: {res.status_code} {res.text}"
        )
        self.logger.info(f"Created SR ACLs: {res.text}")

        # Wait for ACLs to propagate to all nodes
        def acls_propagated():
            for node in self.redpanda.nodes:
                node_url = f"{scheme}://{node.account.hostname}:8081"
                resp = requests.get(
                    f"{node_url}/security/acls",
                    auth=super_auth,
                    timeout=10,
                    cert=cert,
                    verify=ca_cert,
                )
                if resp.status_code != 200:
                    return False
                node_acls = resp.json()
                for acl in acls:
                    if acl not in node_acls:
                        return False
            return True

        wait_until(
            acls_propagated,
            timeout_sec=30,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="SR ACLs did not propagate to all nodes",
        )
        self.logger.info("SR ACLs propagated to all nodes")

        auth_header = {"Authorization": f"Bearer {token['access_token']}"}

        sr_headers = {
            "Accept": "application/vnd.schemaregistry.v1+json",
            "Content-Type": "application/vnd.schemaregistry.v1+json",
            **auth_header,
        }

        sr_read_headers = {
            "Accept": "application/vnd.schemaregistry.v1+json",
            **auth_header,
        }

        schema_data = json.dumps(
            {
                "schema": '{"type":"record","name":"Test","fields":[{"name":"f","type":"string"}]}',
                "schemaType": "AVRO",
            }
        )

        self.logger.info("Phase 1: Register schema (authorized group)")

        def register_allowed():
            res = requests.post(
                f"{base_url}/subjects/{allowed_subject}/versions",
                data=schema_data,
                headers=sr_headers,
                timeout=10,
                cert=cert,
                verify=ca_cert,
            )
            if res.status_code != 200:
                self.logger.debug(f"Register allowed: {res.status_code} {res.text}")
                return False
            body = res.json()
            return "id" in body and body["id"] > 0

        wait_until(
            register_allowed,
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Failed to register schema on allowed subject",
        )
        self.logger.info("Register schema on allowed subject succeeded")

        self.logger.info("Phase 2: Register schema (unauthorized group)")

        res = requests.post(
            f"{base_url}/subjects/{denied_subject}/versions",
            data=schema_data,
            headers=sr_headers,
            timeout=10,
            cert=cert,
            verify=ca_cert,
        )
        assert res.status_code == 403, (
            f"Expected 403 for denied subject register, got {res.status_code}: {res.text}"
        )
        self.logger.info(
            f"Register on denied subject denied: {res.status_code} {res.text}"
        )

        self.logger.info("Phase 3: Read schema (authorized group)")

        def read_allowed():
            res = requests.get(
                f"{base_url}/subjects/{allowed_subject}/versions",
                headers=sr_read_headers,
                timeout=10,
                cert=cert,
                verify=ca_cert,
            )
            if res.status_code != 200:
                self.logger.debug(f"Read allowed: {res.status_code} {res.text}")
                return False
            versions = res.json()
            return isinstance(versions, list) and len(versions) > 0

        wait_until(
            read_allowed,
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Failed to read schema from allowed subject",
        )
        self.logger.info("Read schema from allowed subject succeeded")

        self.logger.info("Phase 4: Read schema (unauthorized group)")

        res = requests.get(
            f"{base_url}/subjects/{denied_subject}/versions",
            headers=sr_read_headers,
            timeout=10,
            cert=cert,
            verify=ca_cert,
        )
        assert res.status_code == 403, (
            f"Expected 403 for denied subject read, got {res.status_code}: {res.text}"
        )
        self.logger.info(
            f"Read from denied subject denied: {res.status_code} {res.text}"
        )


class RedpandaOIDCTest(RedpandaOIDCTestMethods):
    def __init__(self, test_context, **kwargs):
        super(RedpandaOIDCTest, self).__init__(test_context, **kwargs)


class RedpandaOIDCTlsTest(RedpandaOIDCTestMethods):
    def __init__(self, test_context, **kwargs):
        super(RedpandaOIDCTlsTest, self).__init__(test_context, use_ssl=True, **kwargs)


class JavaClientOIDCTest(RedpandaOIDCTestBase):
    def __init__(self, test_context, **kwargs):
        super(JavaClientOIDCTest, self).__init__(test_context, use_ssl=False, **kwargs)

    @cluster(num_nodes=4)
    def test_java_client(self):
        kc_node = self.keycloak.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user(client_id)

        self.rpk.create_topic(EXAMPLE_TOPIC)
        expected_topics = set([EXAMPLE_TOPIC])
        wait_until(
            lambda: set(self.rpk.list_topics()) == expected_topics, timeout_sec=5
        )

        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        cli = KafkaCliTools(self.redpanda, oauth_cfg=cfg)

        self.redpanda.logger.debug(
            "Without an appropriate ACL, topic list empty, produce fails with an authZ error."
        )

        with expect_exception(AuthorizationError, lambda _: True):
            cli.oauth_produce(EXAMPLE_TOPIC, 1)

        assert len(cli.list_topics()) == 0

        self.redpanda.logger.debug(
            "Grant access to service user. We can see it in the list and produce now."
        )

        self.rpk.sasl_allow_principal(
            f"User:{service_user_id}",
            ["all"],
            "topic",
            EXAMPLE_TOPIC,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )

        assert set(cli.list_topics()) == set(expected_topics)

        N_REC = 10
        cli.oauth_produce(EXAMPLE_TOPIC, N_REC)
        records = []
        for i in range(0, N_REC):
            rec = self.rpk.consume(EXAMPLE_TOPIC, n=1, offset=i)
            records.append(json.loads(rec))

        self.redpanda.logger.debug(json.dumps(records, indent=1))

        assert len(records) == N_REC, f"Expected {N_REC} records, got {len(records)}"

        values = set([r["value"] for r in records])

        assert len(values) == len(records), (
            f"Expected {len(records)} unique records, got {len(values)}"
        )


class OIDCReauthTest(RedpandaOIDCTestBase):
    MAX_REAUTH_MS = 8000
    PRODUCE_DURATION_S = MAX_REAUTH_MS * 2 / 1000
    PRODUCE_INTERVAL_S = 0.1
    PRODUCE_ITER = int(PRODUCE_DURATION_S / PRODUCE_INTERVAL_S)

    TOKEN_LIFESPAN_S = int(PRODUCE_DURATION_S)

    def __init__(self, test_context, **kwargs):
        super().__init__(
            test_context,
            sasl_max_reauth_ms=self.MAX_REAUTH_MS,
            access_token_lifespan=self.TOKEN_LIFESPAN_S,
            use_ssl=False,
            **kwargs,
        )

    @cluster(num_nodes=4)
    def test_oidc_reauth(self):
        kc_node = self.keycloak.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user(client_id)

        self.redpanda.logger.info("Creating ACL")
        self.rpk.sasl_allow_principal(
            f"User:{service_user_id}",
            ["all"],
            "topic",
            EXAMPLE_TOPIC,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )

        self.redpanda.logger.info("Creating topic")
        self.rpk.create_topic(EXAMPLE_TOPIC)

        self.redpanda.logger.info("Creating producer")
        cfg = self.keycloak.generate_oauth_config(kc_node, CLIENT_ID)
        assert cfg.client_secret is not None
        assert cfg.token_endpoint is not None
        k_client = PythonLibrdkafka(
            self.redpanda, algorithm="OAUTHBEARER", oauth_config=cfg
        )
        producer = k_client.get_producer()
        producer.poll(0.0)

        def has_leader():
            topics = producer.list_topics(topic=EXAMPLE_TOPIC, timeout=5).topics
            topic = topics.get(EXAMPLE_TOPIC, None)
            has_leader = (
                topic is not None
                and len(topic.partitions) == 1
                and topic.partitions[0].error is None
                and topic.partitions[0].leader != -1
            )
            if not has_leader:
                self.redpanda.logger.debug(
                    f"has_leader: topic={topic}, parts: {topic.partitions[0] if topic else None}"
                )
            return has_leader

        self.redpanda.logger.info("Waiting for topic")
        wait_until(
            has_leader,
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
        )

        def has_acl(node: ClusterNode):
            lst = AclList.parse_raw(self.rpk.acl_list(node=node))
            return lst.has_permission(
                f"{service_user_id}", "all", "topic", EXAMPLE_TOPIC
            )

        self.redpanda.logger.info("Waiting for ACL")
        wait_until(
            lambda: all(has_acl(node) for node in self.redpanda.nodes),
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=False,
        )

        self.redpanda.logger.info("Producing to topic")
        for _ in range(0, self.PRODUCE_ITER):
            producer.produce(topic=EXAMPLE_TOPIC, key="bar", value="23")
            time.sleep(self.PRODUCE_INTERVAL_S)
            producer.flush(5)

        self.redpanda.logger.info("Produced to topic")

        metrics = get_sasl_metrics(self.redpanda)
        self.redpanda.logger.debug(f"SASL metrics: {metrics}")
        assert EXPIRATION_METRIC in metrics.keys()
        assert metrics[EXPIRATION_METRIC] == 0, (
            "Client should reauth before session expiry"
        )
        assert REAUTH_METRIC in metrics.keys()
        assert metrics[REAUTH_METRIC] > 0, "Expected client reauth on some broker..."

        assert k_client.oauth_count > 1, (
            f"Expected at least 2 OAUTH challenges, got {k_client.oauth_count}"
        )


class OIDCLicenseTest(RedpandaOIDCTestBase):
    LICENSE_CHECK_INTERVAL_SEC = 1

    def __init__(self, test_context, num_nodes=3, **kwargs):
        super(OIDCLicenseTest, self).__init__(
            test_context,
            num_nodes=num_nodes,
            sasl_mechanisms=["SCRAM"],
            http_authentication=["BASIC"],
            **kwargs,
        )
        self.redpanda.set_environment(
            {
                "__REDPANDA_PERIODIC_REMINDER_INTERVAL_SEC": f"{self.LICENSE_CHECK_INTERVAL_SEC}",
            }
        )

    @cluster(num_nodes=3)
    @skip_fips_mode  # See NOTE below
    @parametrize(authn_config={"sasl_mechanisms": ["OAUTHBEARER", "SCRAM"]})
    @parametrize(authn_config={"http_authentication": ["OIDC", "BASIC"]})
    def test_license_nag(self, authn_config):
        wait_until_nag_is_set(
            redpanda=self.redpanda, check_interval_sec=self.LICENSE_CHECK_INTERVAL_SEC
        )

        self.logger.debug("Ensuring no license nag")
        time.sleep(self.LICENSE_CHECK_INTERVAL_SEC * 2)
        # NOTE: This assertion will FAIL if running in FIPS mode because
        # being in FIPS mode will trigger the license nag
        assert not self.redpanda.has_license_nag()

        self.logger.debug("Setting cluster config")
        self.redpanda.set_cluster_config(authn_config)

        self.redpanda.set_environment({"__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE": "1"})

        self.redpanda.rolling_restart_nodes(
            self.redpanda.nodes, use_maintenance_mode=False
        )
        wait_until_nag_is_set(
            redpanda=self.redpanda, check_interval_sec=self.LICENSE_CHECK_INTERVAL_SEC
        )

        self.logger.debug("Waiting for license nag")
        wait_until(
            self.redpanda.has_license_nag,
            timeout_sec=self.LICENSE_CHECK_INTERVAL_SEC * 2,
            err_msg="License nag failed to appear",
        )
