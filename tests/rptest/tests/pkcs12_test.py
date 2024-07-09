# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import socket

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.services.service import Service
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import TLSProvider, SecurityConfig
from rptest.services.tls import Certificate, CertificateAuthority, TLSCertManager
from rptest.tests.redpanda_test import RedpandaTest


class P12TLSProvider(TLSProvider):
    def __init__(self, tls: TLSCertManager, use_pkcs12: bool):
        self.tls = tls
        self.use_pkcs12 = use_pkcs12

    @property
    def ca(self) -> CertificateAuthority:
        return self.tls.ca

    def create_broker_cert(self, service: Service,
                           node: ClusterNode) -> Certificate:
        assert node in service.nodes
        return self.tls.create_cert(node.name)

    def create_service_client_cert(self, _: Service, name: str) -> Certificate:
        return self.tls.create_cert(socket.gethostname(),
                                    name=name,
                                    common_name=name)

    def use_pkcs12_file(self) -> bool:
        return self.use_pkcs12

    def p12_password(self, node: ClusterNode) -> str:
        assert node.name in self.tls.certs, f"No certificate associated with node {node.name}"
        return self.tls.certs[node.name].p12_password


class PKCS12Test(RedpandaTest):
    """
    Tests used to validate the functionality of using a PKCS#12 file
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         num_brokers=3,
                         skip_if_no_redpanda_log=True,
                         **kwargs)
        self.user, self.password, self.algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        self.admin = Admin(self.redpanda)

    def setUp(self):
        # Skip set up to allow test to control how Redpanda's TLS settings are configured
        pass

    def _prepare_cluster(self, use_pkcs12: bool):
        self.tls = TLSCertManager(self.logger)
        self.provider = P12TLSProvider(self.tls, use_pkcs12)
        self.user_cert = self.tls.create_cert(socket.gethostname(),
                                              common_name="walterP",
                                              name="user")

        self.security = SecurityConfig()
        self.security.endpoint_authn_method = "mtls_identity"
        self.security.tls_provider = self.provider
        self.security.require_client_auth = True
        self.security.enable_sasl = False

        self.redpanda.set_security_settings(self.security)
        self.redpanda.add_extra_rp_conf({
            'kafka_mtls_principal_mapping_rules':
            [self.security.principal_mapping_rules]
        })

        super().setUp()
        self.admin.create_user("walterP", self.password, self.algorithm)
        self.rpk = RpkTool(self.redpanda, tls_cert=self.user_cert)

    @cluster(num_nodes=3)
    @matrix(use_pkcs12=[True, False])
    def test_smoke(self, use_pkcs12: bool):
        """
        Simple smoke test to verify that the PKCS12 file is being used
        """
        self._prepare_cluster(use_pkcs12)
        TOPIC_NAME = "foo"
        self.rpk.create_topic(TOPIC_NAME)
        topics = [t for t in self.rpk.list_topics()]
        assert TOPIC_NAME in topics, f"Missing topic '{TOPIC_NAME}': {topics}"
