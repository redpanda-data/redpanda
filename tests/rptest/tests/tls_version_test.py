# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import socket
import subprocess

from enum import IntEnum

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.services.service import Service

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import TLSProvider, SecurityConfig
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.services.tls import Certificate, CertificateAuthority, TLSCertManager
from rptest.tests.redpanda_test import RedpandaTest


class TLSVersionTestProvider(TLSProvider):
    def __init__(self, tls: TLSCertManager):
        self._tls = tls

    @property
    def ca(self) -> CertificateAuthority:
        return self._tls.ca

    def create_broker_cert(self, service: Service,
                           node: ClusterNode) -> Certificate:
        assert node in service.nodes
        return self._tls.create_cert(node.name)

    def create_service_client_cert(self, _: Service, name: str) -> Certificate:
        return self._tls.create_cert(socket.gethostname(), name=name)


PERMITTED_ERROR_MESSAGE = [
    "seastar::tls::verification_error", "SSL routines::unsupported protocol"
]


class TLSVersion(IntEnum):
    v1_0 = 0
    v1_1 = 1
    v1_2 = 2
    v1_3 = 3


def tls_version_to_openssl(ver: TLSVersion) -> str:
    if ver == TLSVersion.v1_0:
        return "-tls1"
    elif ver == TLSVersion.v1_1:
        return "-tls1_1"
    elif ver == TLSVersion.v1_2:
        return "-tls1_2"
    elif ver == TLSVersion.v1_3:
        return "-tls1_3"
    else:
        raise ValueError(f"Unknown TLS Version: {ver}")


def tls_version_to_config(ver: TLSVersion) -> str:
    if ver == TLSVersion.v1_0:
        return "v1.0"
    elif ver == TLSVersion.v1_1:
        return "v1.1"
    elif ver == TLSVersion.v1_2:
        return "v1.2"
    elif ver == TLSVersion.v1_3:
        return "v1.3"
    else:
        raise ValueError(f"Unknown TLS Version: {ver}")


class TLSVersionTestBase(RedpandaTest):
    """
    Base test class that sets up TLS on the Kafka API interface
    """
    def __init__(self, test_context):
        super(TLSVersionTestBase, self).__init__(test_context=test_context)
        self.security = SecurityConfig()
        self.tls = TLSCertManager(self.logger)
        self.admin = Admin(self.redpanda)
        self.installer = self.redpanda._installer

    def setUp(self):
        self.security.tls_provider = TLSVersionTestProvider(tls=self.tls)
        self.redpanda.set_security_settings(self.security)

        super().setUp()

    def _output_good(self, output: str) -> bool:
        return "Verify return code: 0" in output or "Verify return code: 19" in output

    def _output_error(self, output: str) -> bool:
        return "no protocols available" in output or "tlsv1 alert protocol version" in output

    def verify_tls_version(self, node: ClusterNode, tls_version: TLSVersion,
                           expect_fail: bool):
        tls_version_str = tls_version_to_openssl(tls_version)
        try:
            cmd = f"openssl s_client {tls_version_str} -CAfile {self.tls.ca.crt} -connect {node.name}:9092"
            self.logger.debug(f"Running: {cmd}")
            _ = subprocess.check_output(cmd.split(),
                                        stderr=subprocess.STDOUT,
                                        stdin=subprocess.DEVNULL)
            if expect_fail:
                assert False, f"Expected openssl s_client to fail with TLS version string {tls_version_str}"
        except subprocess.CalledProcessError as e:
            if not expect_fail:
                assert self._output_good(
                    e.output.decode()
                ), f"Invalid output for good case detected: {e.output.decode()}"
            else:
                assert self._output_error(e.output.decode(
                )), f"Output not expected for failure: {e.output.decode()}"


class TLSVersionTest(TLSVersionTestBase):
    @cluster(num_nodes=3, log_allow_list=PERMITTED_ERROR_MESSAGE)
    @matrix(version=[0, 1, 2, 3])
    def test_change_version(self, version: int):
        """
        This test steps through each valid setting of tls_min_version and uses
        OpenSSL s_client to verify that any versions before the minimum selected version
        are rejected during handshake.
        """

        # Validate that by default it's v1.2
        cluster_cfg = self.admin.get_cluster_config()
        assert cluster_cfg["tls_min_version"] == tls_version_to_config(
            TLSVersion.v1_2
        ), f"Invalid cluster config: {cluster_cfg['tls_min_version']} != {tls_version_to_config(TLSVersion.v1_2)}"
        ver = TLSVersion(version)
        # Change the version
        self.admin.patch_cluster_config(
            {"tls_min_version": tls_version_to_config(ver)})
        self.redpanda.restart_nodes(self.redpanda.nodes)
        # Step through each node and each version and verify whether or not we get a failure
        for n in self.redpanda.nodes:
            for v in TLSVersion:
                expect_failure = v < ver
                self.verify_tls_version(node=n,
                                        tls_version=v,
                                        expect_fail=expect_failure)
