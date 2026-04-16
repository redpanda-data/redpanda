# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import socket
import subprocess

from ducktape.cluster.cluster import ClusterNode
from ducktape.services.service import Service
from ducktape.tests.test import TestContext

from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    SecurityConfig,
    TLSProvider,
)
from rptest.services.tls import (
    Certificate,
    CertificateAuthority,
    TLSCertManager,
)
from rptest.tests.redpanda_test import RedpandaTest


class OpenSSLConfigTestProvider(TLSProvider):
    """Simple TLS provider for the OpenSSL config isolation test."""

    def __init__(self, tls: TLSCertManager):
        self._tls = tls

    @property
    def ca(self) -> CertificateAuthority:
        return self._tls.ca

    def create_broker_cert(self, service: Service, node: ClusterNode) -> Certificate:
        assert node in service.nodes
        return self._tls.create_cert(node.name)

    def create_service_client_cert(self, service: Service, name: str) -> Certificate:
        return self._tls.create_cert(socket.gethostname(), name=name)


# OpenSSL config that restricts TLS to version 1.2 maximum
# If this config is loaded by Redpanda, TLS 1.3 handshakes will fail
RESTRICTIVE_OPENSSL_CNF = """
# OpenSSL config that blocks TLS 1.3
openssl_conf = openssl_init

[openssl_init]
ssl_conf = ssl_sect

[ssl_sect]
system_default = system_default_sect

[system_default_sect]
# This setting would prevent TLS 1.3 connections if loaded
MaxProtocol = TLSv1.2
"""


class OpenSSLConfigIsolationTest(RedpandaTest):
    """
    Test that Redpanda does not load the system's openssl.cnf file.

    This test verifies the fix for the OpenSSL initialization ordering bug
    where Redpanda could unintentionally pick up the OS's OPENSSL_CONF-configured
    openssl.cnf file, causing wrong crypto behavior.

    The test works by:
    1. Creating a custom openssl.cnf that sets MaxProtocol = TLSv1.2
    2. Setting OPENSSL_CONF to point to this restrictive config
    3. Starting Redpanda with TLS enabled
    4. Attempting a TLS 1.3 handshake

    If Redpanda incorrectly loads the system config, TLS 1.3 would be blocked
    and the handshake would fail. If Redpanda correctly ignores the system
    config (using OPENSSL_INIT_NO_LOAD_CONFIG), TLS 1.3 should succeed.
    """

    # Path on the node where we'll write the restrictive OpenSSL config
    RESTRICTIVE_CONFIG_PATH = "/tmp/restrictive_openssl.cnf"

    # Same as the default value of tls_v1_3_cipher_suites, but reordered to trigger config validation
    REORDERED_DEFAULT_CIPHERS = "TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384:TLS_AES_128_CCM_SHA256:TLS_CHACHA20_POLY1305_SHA256"

    def __init__(self, test_context: TestContext):
        super(OpenSSLConfigIsolationTest, self).__init__(
            test_context,
            extra_rp_conf={"tls_v1_3_cipher_suites": self.REORDERED_DEFAULT_CIPHERS},
        )
        self.security = SecurityConfig()
        self.tls = TLSCertManager(self.logger)

    def setUp(self):
        # Configure TLS
        self.security.tls_provider = OpenSSLConfigTestProvider(tls=self.tls)
        self.redpanda.set_security_settings(self.security)

        # Create a client certificate for testing TLS connections
        # The Kafka listener requires client certificate authentication
        self._client_cert = self.tls.create_cert(
            socket.gethostname(), name="test_client"
        )

        # Write the restrictive OpenSSL config to each node
        for node in self.redpanda.nodes:
            node.account.create_file(
                self.RESTRICTIVE_CONFIG_PATH, RESTRICTIVE_OPENSSL_CNF
            )

        # Set OPENSSL_CONF environment variable to point to the restrictive config
        # If Redpanda loads this config, TLS 1.3 will be blocked
        self.redpanda.set_environment({"OPENSSL_CONF": self.RESTRICTIVE_CONFIG_PATH})

        # Start Redpanda with TLS enabled
        super().setUp()

    def _verify_tls_version_works(
        self, node: ClusterNode, tls_version: str, port: int
    ) -> bool:
        """
        Attempt a TLS handshake with the specified version.

        Returns True if the handshake succeeds, False if it fails.
        """
        # Include client certificate since Kafka listener requires client auth
        cmd = (
            f"openssl s_client {tls_version} "
            f"-CAfile {self.tls.ca.crt} "
            f"-cert {self._client_cert.crt} "
            f"-key {self._client_cert.key} "
            f"-connect {node.name}:{port}"
        )
        self.logger.debug(f"Running: {cmd}")

        try:
            output = subprocess.check_output(
                cmd.split(),
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                timeout=10,
            )
            output_str = output.decode()
            # Check for successful verification
            return (
                "Verify return code: 0" in output_str
                or "Verify return code: 19" in output_str
            )
        except subprocess.CalledProcessError as e:
            output_str = e.output.decode()
            self.logger.debug(f"TLS command output: {output_str}")

            # Check for TLS version-specific errors FIRST - these indicate the
            # protocol version was rejected. Note: "Verify return code: 0" can
            # be misleading when the handshake fails before certificate exchange
            # (nothing to verify means verification "succeeds" vacuously)
            tls_version_errors = [
                "no protocols available",
                "tlsv1 alert protocol version",
                "wrong version number",
                "unsupported protocol",
            ]
            if any(err in output_str for err in tls_version_errors):
                self.logger.debug("TLS version negotiation failed")
                return False

            # Check that a cipher was actually negotiated (not "(NONE)")
            # If no cipher was negotiated, the TLS handshake failed
            if "Cipher is (NONE)" in output_str:
                self.logger.debug("TLS handshake failed - no cipher negotiated")
                return False

            # Check for successful verification - openssl s_client may return
            # non-zero even when TLS handshake succeeded (e.g., server closes
            # connection after handshake)
            if (
                "Verify return code: 0" in output_str
                or "Verify return code: 19" in output_str
            ):
                self.logger.debug("TLS verification succeeded")
                return True

            # For other errors (like generic handshake failures), log and fail
            self.logger.debug("TLS handshake failed for non-version reason")
            return False
        except subprocess.TimeoutExpired:
            self.logger.error("TLS handshake timed out")
            return False

    @cluster(num_nodes=1)
    def test_system_openssl_config_not_loaded(self):
        """
        Verify that Redpanda ignores the system's OPENSSL_CONF setting.

        This test sets OPENSSL_CONF to a config that blocks TLS 1.3, then
        verifies that TLS 1.3 handshakes still succeed. This proves Redpanda
        is using OPENSSL_INIT_NO_LOAD_CONFIG to prevent loading the system
        config.
        """
        node = self.redpanda.nodes[0]
        kafka_port = 9092

        # First verify TLS 1.2 works (baseline - should work regardless)
        self.logger.info("Verifying TLS 1.2 works (baseline check)")
        tls12_works = self._verify_tls_version_works(node, "-tls1_2", kafka_port)
        assert tls12_works, "TLS 1.2 should work - this is a baseline check"

        # Now verify TLS 1.3 works - this is the actual test
        # If OPENSSL_CONF was loaded, TLS 1.3 would be blocked by MaxProtocol=TLSv1.2
        self.logger.info("Verifying TLS 1.3 works (proves system config not loaded)")
        tls13_works = self._verify_tls_version_works(node, "-tls1_3", kafka_port)

        assert tls13_works, (
            "TLS 1.3 handshake failed! This suggests Redpanda loaded the "
            "system's OPENSSL_CONF which has MaxProtocol=TLSv1.2. "
            "Redpanda should use OPENSSL_INIT_NO_LOAD_CONFIG to prevent this."
        )

        self.logger.info(
            "SUCCESS: TLS 1.3 works despite OPENSSL_CONF setting MaxProtocol=TLSv1.2. "
            "This proves Redpanda correctly ignores the system OpenSSL config."
        )
