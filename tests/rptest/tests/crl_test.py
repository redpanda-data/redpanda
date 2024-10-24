# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import socket
import tempfile
import json
import requests

from ducktape.utils.util import wait_until
from ducktape.cluster.cluster import ClusterNode

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster

from rptest.services.admin import Admin
from rptest.services.redpanda import RedpandaService
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.redpanda import (SecurityConfig, TLSProvider,
                                      SchemaRegistryConfig, PandaproxyConfig)
from rptest.services import tls
from rptest.util import expect_exception


class CachingTLSProvider(TLSProvider):
    broker_certs: list[tls.Certificate] = []
    service_client_certs: list[tls.Certificate] = []

    def __init__(self, tls):
        self.tls = tls

    @property
    def ca(self):
        return self.tls.ca

    def create_broker_cert(self, redpanda, node):
        assert node in redpanda.nodes
        self.broker_certs.append(self.tls.create_cert(node.name))
        return self.broker_certs[-1]

    def create_service_client_cert(self, _, name):
        self.service_client_certs.append(
            self.tls.create_cert(socket.gethostname(),
                                 name=name,
                                 common_name=name))
        return self.service_client_certs[-1]


class CertificateRevocationTest(RedpandaTest):
    # TODO(oren): check for this somehow:
    VERIFY_EXC = "seastar::tls::verification_error"
    VERIFY_ERROR = f'''
{VERIFY_EXC} (The certificate is NOT trusted. The certificate chain is revoked. (Issuer=[O=Redpanda,CN=Redpanda Test CA], Subject=[O=Redpanda,CN={{common_name}}]))">
        '''
    VERIFICATION_ERROR_LOG = [VERIFY_EXC]

    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         num_brokers=3,
                         skip_if_no_redpanda_log=True,
                         **kwargs)

        self.user, self.password, self.algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        self.admin = Admin(self.redpanda)

        self.tls = tls.TLSCertManager(self.logger)
        self.provider = CachingTLSProvider(self.tls)
        self.user_cert = self.tls.create_cert(socket.gethostname(),
                                              common_name="walterP",
                                              name="user")
        self.sr_client_cert = self.tls.create_cert(socket.gethostname(),
                                                   common_name="sr_client",
                                                   name="sr_client")
        self.pp_api_cert = self.tls.create_cert(socket.gethostname(),
                                                common_name="pp_api",
                                                name="pp_api")

        self.security = SecurityConfig()
        self.security.endpoint_authn_method = "mtls_identity"
        self.security.tls_provider = self.provider
        self.security.require_client_auth = True
        self.security.enable_sasl = False

        self.schema_registry_config = SchemaRegistryConfig()
        self.schema_registry_config.require_client_auth = True
        self.schema_registry_config.client_key = self.sr_client_cert.key
        self.schema_registry_config.client_crt = self.sr_client_cert.crt

        self.pandaproxy_config = PandaproxyConfig()
        self.pandaproxy_config.require_client_auth = True
        self.pandaproxy_config.client_key = self.sr_client_cert.key
        self.pandaproxy_config.client_crt = self.sr_client_cert.crt

        self.redpanda.set_security_settings(self.security)
        self.redpanda.set_schema_registry_settings(self.schema_registry_config)
        self.redpanda.set_pandaproxy_settings(self.pandaproxy_config)

        self.redpanda.add_extra_rp_conf({
            'kafka_mtls_principal_mapping_rules':
            [self.security.principal_mapping_rules]
        })

    def setUp(self):
        super().setUp()
        self.admin.create_user("walterP", self.password, self.algorithm)
        self.rpk = RpkTool(
            self.redpanda,
            tls_cert=self.user_cert,
        )

        self.no_auth_rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=3, log_allow_list=VERIFICATION_ERROR_LOG)
    def test_kafka(self):
        TOPIC_NAME = "foo"
        self.rpk.create_topic(TOPIC_NAME)
        topics = [t for t in self.rpk.list_topics()]
        assert TOPIC_NAME in topics, f"Missing topic '{TOPIC_NAME}': {topics}"

        self.tls.revoke_cert(self.user_cert)
        for n in self.redpanda.nodes:
            self.redpanda.write_crl_file(n, self.tls.ca)

        with expect_exception(
                RpkException,
                lambda e: "connection initialization failed" in str(e)):
            self.rpk.list_topics()

    @cluster(num_nodes=3, log_allow_list=VERIFICATION_ERROR_LOG)
    def test_sr_client(self):
        def create_schema(subject, schema):
            with tempfile.NamedTemporaryFile(suffix='.avro') as tf:
                tf.write(bytes(schema, 'UTF-8'))
                tf.seek(0)
                self.rpk.create_schema(subject, tf.name)

        schema = {"type": "record", "name": "foo", "fields": []}

        create_schema(
            'foo',
            json.dumps(schema),
        )

        self.tls.revoke_cert(self.sr_client_cert)
        for n in self.redpanda.nodes:
            self.redpanda.write_crl_file(n, self.tls.ca)

        self.logger.debug(
            "Restart schema store to force cert verification on kclient reconnect"
        )
        for n in self.redpanda.nodes:
            self.admin.restart_service(rp_service='schema-registry', node=n)

        self.logger.debug(
            "List schemas should now time out as SR tries and fails to fetch _schemas"
        )
        with expect_exception(RpkException,
                              lambda e: 'deadline exceeded' in str(e)):
            self.rpk.list_schemas()

    @cluster(num_nodes=3)
    def test_pp_api(self):
        def get_topics(node: ClusterNode):
            return requests.get(
                f"https://{node.account.hostname}:8082/topics",
                headers={
                    "Accept": "application/vnd.kafka.v2+json",
                    "Content-Type": "application/vnd.kafka.v2+json"
                },
                verify=self.user_cert.ca.crt,
                cert=(self.user_cert.crt, self.user_cert.key),
                timeout=10,
            )

        def expect_connection_exception(node: ClusterNode) -> bool:
            with expect_exception((requests.exceptions.SSLError,
                                   requests.exceptions.ConnectionError),
                                  lambda e: "certificate revoked" in str(
                                      e) or "Connection aborted" in str(e)):
                get_topics(node)
            return True

        node = self.redpanda.nodes[0]

        with get_topics(node) as res:
            assert res.status_code == 200, f"Bad status: {res.status_code}"

        self.tls.revoke_cert(self.user_cert)
        self.redpanda.write_crl_file(node, self.tls.ca)

        wait_until(lambda: expect_connection_exception(node),
                   timeout_sec=5,
                   backoff_sec=0.5,
                   err_msg="Did not receive expected SSL exception",
                   retry_on_exc=True)

        with get_topics(self.redpanda.nodes[1]) as res:
            assert res.status_code == 200, f"Bad status: {res.status_code}"

    @cluster(num_nodes=3,
             log_allow_list=[
                 "certificate revoked", "The certificate is NOT trusted",
                 "certificate verify failed"
             ])
    def test_rpc(self):
        node = self.redpanda.nodes[0]

        self.logger.debug(
            "Everything should work w/ or w/o TLS on internal RPC")

        ch = self.admin.get_cluster_health_overview(node)
        assert ch['is_healthy'], f"Cluster not healthy: {json.dumps(ch)}"

        RPC_TLS_CONFIG = dict(
            enabled=True,
            require_client_auth=True,
            key_file=RedpandaService.TLS_SERVER_KEY_FILE,
            cert_file=RedpandaService.TLS_SERVER_CRT_FILE,
            truststore_file=RedpandaService.TLS_CA_CRT_FILE,
            crl_file=RedpandaService.TLS_CA_CRL_FILE,
        )

        self.redpanda.stop()
        self.redpanda.start(node_config_overrides={
            n: dict(rpc_server_tls=RPC_TLS_CONFIG)
            for n in self.redpanda.nodes
        })

        def cluster_health(node: ClusterNode,
                           healthy: bool = True,
                           noisy: bool = False,
                           reasons: list[str] = []):
            assert not (healthy and len(reasons)
                        > 0), "Reasons apply only to unhealthy clusters"
            ch = self.admin.get_cluster_health_overview(node)
            if noisy:
                self.logger.debug(f"health overview: {json.dumps(ch)}")
            return ch['is_healthy'] is healthy and all(
                r in ch['unhealthy_reasons'] for r in reasons)

        wait_until(lambda: cluster_health(node),
                   timeout_sec=10,
                   backoff_sec=0.2,
                   err_msg="Cluster did not become healthy")

        self.logger.debug(
            f"Now revoke the broker cert and push to {node.account.hostname}")

        broker_cert = self.provider.broker_certs[0]

        assert node.account.hostname in broker_cert.crt, f"Cert order mismatch: {broker_cert.crt}"

        self.tls.revoke_cert(broker_cert)

        self.redpanda.write_crl_file(self.redpanda.nodes[1], self.tls.ca)
        self.redpanda.write_crl_file(self.redpanda.nodes[2], self.tls.ca)

        self.redpanda.restart_nodes(
            [node], override_cfg_params={"rpc_server_tls": RPC_TLS_CONFIG})
        other_node = self.redpanda.nodes[1]

        self.logger.debug(
            f"{node.account.hostname} should appear 'down' to the rest of the cluster"
        )

        wait_until(lambda: cluster_health(
            other_node, healthy=False, noisy=True, reasons=['nodes_down']),
                   timeout_sec=10,
                   backoff_sec=0.2,
                   err_msg="Cluster did not become unhealthy")

    # Ignore all logs since we can expect a variety of errors with noncogent certs, eg:
    # kafka - server.cc:159 - Error[applying protocol] remote address: 172.31.37.188:59640 - seastar::timed_out_error (timedout)
    @cluster(num_nodes=3, log_allow_list=[re.compile(".*")])
    def test_noncogent(self):
        node = self.redpanda.nodes[0]
        self.rpk.list_schemas()

        other_tls = tls.TLSCertManager(self.logger)
        some_cert = other_tls.create_cert(socket.gethostname(),
                                          common_name='foo',
                                          name='foo')
        other_tls.revoke_cert(some_cert)
        for n in self.redpanda.nodes:
            self.redpanda.write_crl_file(n, other_tls.ca)

        self.logger.debug(
            "TODO: As implemented, this quietly breaks everything. Behavior should be both graceful and configurable."
        )
        try:
            self.rpk.list_schemas()
        except:
            pass
