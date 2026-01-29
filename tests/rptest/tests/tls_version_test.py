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
from typing import List, Optional

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.services.service import Service

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    PandaproxyConfig,
    RedpandaService,
    SchemaRegistryConfig,
    SecurityConfig,
    TLSProvider,
)
from rptest.services.tls import (
    Certificate,
    CertificateAuthority,
    TLSCertManager,
    TLSKeyType,
)
from rptest.tests.redpanda_test import RedpandaTest


class TLSVersionTestProvider(TLSProvider):
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


PERMITTED_ERROR_MESSAGE = [
    "seastar::tls::verification_error",
    "SSL routines::no shared cipher",
    "SSL routines::unsupported protocol",
    # Error as reported before openssl-3.2.0 (needed for fips mode)
    "sslv3 alert handshake failure",
    # Error as reported since openssl-3.2.0
    "ssl/tls alert handshake failure",
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


def tls_version_to_config(ver: TLSVersion) -> str:
    if ver == TLSVersion.v1_0:
        return "v1.0"
    elif ver == TLSVersion.v1_1:
        return "v1.1"
    elif ver == TLSVersion.v1_2:
        return "v1.2"
    elif ver == TLSVersion.v1_3:
        return "v1.3"


class TLSVersionTestBase(RedpandaTest):
    """
    Base test class that sets up TLS on the Kafka API interface
    """

    def __init__(self, test_context, key_type: TLSKeyType):
        super(TLSVersionTestBase, self).__init__(test_context=test_context)
        self.security = SecurityConfig()
        self.tls = TLSCertManager(self.logger, key_type=key_type)
        self.key_type = key_type
        self.admin = Admin(self.redpanda)
        self.installer = self.redpanda._installer

    def setUp(self):
        self.security.tls_provider = TLSVersionTestProvider(tls=self.tls)
        self.redpanda.set_security_settings(self.security)

        self.schema_registry_config = SchemaRegistryConfig()
        self.schema_registry_config.require_client_auth = True
        self.redpanda.set_schema_registry_settings(self.schema_registry_config)

        self.pandaproxy_config = PandaproxyConfig()
        self.pandaproxy_config.require_client_auth = True
        self.redpanda.set_pandaproxy_settings(self.pandaproxy_config)

        tls = dict(
            enabled=True,
            require_client_auth=True,
            key_file=RedpandaService.TLS_SERVER_KEY_FILE,
            cert_file=RedpandaService.TLS_SERVER_CRT_FILE,
            truststore_file=RedpandaService.TLS_CA_CRT_FILE,
        )
        admin_api_tls = tls.copy()
        admin_api_tls.update(
            name="iplistener",
        )

        cfg_overrides = {}

        def set_cfg(node):
            cfg_overrides[node] = dict(
                admin_api_tls=admin_api_tls, rpc_server_tls=admin_api_tls
            )

        self.redpanda.for_nodes(self.redpanda.nodes, set_cfg)

        self.redpanda.start(node_config_overrides=cfg_overrides)

    def _output_good(self, output: str) -> bool:
        return "Verify return code: 0" in output or "Verify return code: 19" in output

    def _output_error(self, output: str) -> bool:
        return (
            "no protocols available" in output
            or "tlsv1 alert protocol version" in output
            or "handshake failure" in output
            or "wrong version number" in output
            or ("CONNECTED(" in output and "unexpected eof while reading" in output)
        )

    def verify_tls_version(
        self,
        node: ClusterNode,
        tls_version: TLSVersion,
        expect_fail: bool,
        port: int = 9092,
        cipher: Optional[str] = None,
    ):
        tls_version_str = tls_version_to_openssl(tls_version)
        cipher_arg = "ciphersuites" if tls_version == TLSVersion.v1_3 else "cipher"
        cipher_opt = f"-{cipher_arg} {cipher}" if cipher else ""
        try:
            cmd = f"openssl s_client {tls_version_str} {cipher_opt} -CAfile {self.tls.ca.crt} -connect {node.name}:{port}"
            self.logger.debug(f"Running: {cmd}")
            _ = subprocess.check_output(
                cmd.split(), stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL
            )
            if expect_fail:
                assert False, (
                    f"Expected openssl s_client to fail with TLS version string {tls_version_str}, cipher {cipher}, port {port}"
                )
        except subprocess.CalledProcessError as e:
            if not expect_fail:
                assert self._output_good(e.output.decode()), (
                    f"Invalid output for good case detected: {e.output.decode()}"
                )
            else:
                assert self._output_error(e.output.decode()), (
                    f"Output not expected for failure: {e.output.decode()}"
                )

    # Default ciphersuites for TLS 1.2 and 1.3
    TLSV1_2_CIPHERS = [
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
    ]
    TLSV1_2_CIPHERS_WEAK = TLSV1_2_CIPHERS + [
        "TLS_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_DHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
        "TLS_RSA_WITH_AES_128_CBC_SHA",
        "TLS_RSA_WITH_AES_128_CCM",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_RSA_WITH_AES_256_CCM",
    ]
    TLSV1_3_CIPHERS = [
        "TLS_AES_128_GCM_SHA256",
        "TLS_AES_256_GCM_SHA384",
        "TLS_CHACHA20_POLY1305_SHA256",
        "TLS_AES_128_CCM_SHA256",
    ]

    TLSV1_3_CIPHERS_STRICT = [
        "TLS_AES_256_GCM_SHA384",
        "TLS_CHACHA20_POLY1305_SHA256",
        "TLS_AES_128_GCM_SHA256",
    ]

    def _filter_by_key_type(self, key_type: TLSKeyType, ciphers: List) -> List:
        if key_type == TLSKeyType.RSA:
            # DHE-RSA and ECDHE-RSA require an RSA certificate
            return [
                c
                for c in ciphers
                if c.startswith("DHE-RSA") or c.startswith("ECDHE-RSA")
            ]
        elif key_type == TLSKeyType.ECDSA:
            return [c for c in ciphers if c.startswith("ECDHE-ECDSA")]

    def _get_openssl_ciphers(self, key_type: TLSKeyType) -> List:
        # Get the list of ciphers supported by the installed version of OpenSSL
        tls_version_str = tls_version_to_openssl(TLSVersion.v1_2)
        process = subprocess.run(
            ["openssl", "ciphers", "-s", tls_version_str],
            text=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.logger.debug(f"stdout: {process.stdout}")
        tls1_2_ciphers = [
            (TLSVersion.v1_2, c)
            for c in self._filter_by_key_type(
                key_type=self.key_type, ciphers=process.stdout.strip().split(":")
            )
        ]

        return tls1_2_ciphers + [(TLSVersion.v1_3, c) for c in self.TLSV1_3_CIPHERS]

    def _get_default_ciphers(self, key_type: TLSKeyType, strict: bool) -> List:
        # TLS 1.2 ciphers (only if not strict)
        tls12_ciphers = []
        if not strict:
            tls12_ciphers = [
                (TLSVersion.v1_2, c)
                for c in self._filter_by_key_type(key_type, self.TLSV1_2_CIPHERS)
            ]

        # TLS 1.3 ciphers (strict or normal)
        tls13_ciphers = [
            (TLSVersion.v1_3, c)
            for c in (self.TLSV1_3_CIPHERS_STRICT if strict else self.TLSV1_3_CIPHERS)
        ]

        ciphers = tls12_ciphers + tls13_ciphers
        assert ciphers, f"No ciphers found for key type {key_type} strict {strict}"

        return ciphers

    @cluster(num_nodes=1, log_allow_list=PERMITTED_ERROR_MESSAGE)
    def test_ciphersuite_support(self):
        """
        Test all ciphers for TLS 1.2+ across all interfaces filtered by key_type.
        """
        interfaces = [
            (9092, False),
            (9644, False),
            (8082, False),
            (8081, False),
            (33145, True),
        ]

        openssl_ciphers = self._get_openssl_ciphers(key_type=self.key_type)

        def check_ciphers(expected_ciphers):
            for port, strict in interfaces:
                expected = expected_ciphers(strict)
                valid_test = len(expected) < len(openssl_ciphers) and len(expected) > 0
                assert valid_test, (
                    f"Expecting some expected failures and expected successes for port {port} strict {strict}"
                )
                for v, c in openssl_ciphers:
                    expect_fail = (v, c) not in expected
                    self.logger.info(
                        f"Testing port: {port} tls: {v} cipher: {c}, cert: {self.key_type}, expect_fail: {expect_fail}"
                    )

                    self.verify_tls_version(
                        node=self.redpanda.nodes[0],
                        tls_version=v,
                        expect_fail=expect_fail,
                        port=port,
                        cipher=c,
                    )

        # Check default ciphers
        def default_ciphers(strict: bool):
            return self._get_default_ciphers(key_type=self.key_type, strict=strict)

        check_ciphers(default_ciphers)

        # Change ciphers
        self.redpanda.set_cluster_config(
            values={
                "tls_v1_2_cipher_suites": ":".join(self.TLSV1_2_CIPHERS_WEAK),
                "tls_v1_3_cipher_suites": ":".join(self.TLSV1_3_CIPHERS_STRICT),
            },
            expect_restart=True,
        )

        def expected_ciphers(strict: bool):
            return [(TLSVersion.v1_3, c) for c in self.TLSV1_3_CIPHERS_STRICT] + [
                (TLSVersion.v1_2, c)
                for c in self._filter_by_key_type(
                    self.key_type, (self.TLSV1_2_CIPHERS_WEAK if not strict else [])
                )
            ]

        check_ciphers(expected_ciphers)

        self.redpanda.set_cluster_config(
            values={
                "tls_min_version": "v1.3",
            },
            expect_restart=True,
        )

        def expected_ciphers_tls1_3(strict: bool):
            return [(TLSVersion.v1_3, c) for c in self.TLSV1_3_CIPHERS_STRICT]

        check_ciphers(expected_ciphers_tls1_3)

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
        ), (
            f"Invalid cluster config: {cluster_cfg['tls_min_version']} != {tls_version_to_config(TLSVersion.v1_2)}"
        )
        ver = TLSVersion(version)
        # Change the version
        self.admin.patch_cluster_config({"tls_min_version": tls_version_to_config(ver)})
        self.redpanda.restart_nodes(self.redpanda.nodes)
        # Step through each node and each version and verify whether or not we get a failure
        for n in self.redpanda.nodes:
            for v in TLSVersion:
                expect_failure = v < ver
                self.verify_tls_version(
                    node=n, tls_version=v, expect_fail=expect_failure
                )


class TLSVersionTestRSA(TLSVersionTestBase):
    def __init__(self, test_context):
        super(TLSVersionTestRSA, self).__init__(
            test_context=test_context, key_type=TLSKeyType.RSA
        )


class TLSVersionTestECDSA(TLSVersionTestBase):
    def __init__(self, test_context):
        super(TLSVersionTestECDSA, self).__init__(
            test_context=test_context, key_type=TLSKeyType.ECDSA
        )


class TLSRenegotiationTest(RedpandaTest):
    def __init__(self, test_context):
        super(TLSRenegotiationTest, self).__init__(test_context=test_context)
        self.security = SecurityConfig()
        self.tls = TLSCertManager(self.logger)
        self.admin = Admin(self.redpanda)

    def setUp(self):
        self.security.tls_provider = TLSVersionTestProvider(tls=self.tls)
        self.redpanda.set_security_settings(self.security)
        super().setUp()

    def verify_tls_renegotiation(self, node: ClusterNode, expect_fail: bool):
        cmd = f"openssl s_client -tls1_2 -CAfile {self.tls.ca.crt} -cert {self.tls.ca.crt} -key {self.tls.ca.key} -connect {node.name}:9092"
        self.logger.debug(f"Running: {cmd}")
        # Run command send send "R\n".  This tells s_client to attempt a client initiated renegotiation
        process = subprocess.run(
            cmd.split(),
            input="R\n",
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        self.logger.debug(f"stdout: {process.stdout}")
        self.logger.debug(f"stderr: {process.stderr}")
        if expect_fail:
            assert process.returncode != 0, (
                f"Expected openssl s_client to fail {process.returncode}"
            )
        else:
            assert process.returncode == 0, (
                f"Expected openssl s_client to succeed {process.returncode}"
            )

    @cluster(num_nodes=3, log_allow_list=PERMITTED_ERROR_MESSAGE)
    @matrix(enable_renegotiation=[True, False])
    def test_tls_renegotiation(self, enable_renegotiation: bool):
        cluster_cfg = self.admin.get_cluster_config(key="tls_enable_renegotiation")
        # It should always start false
        assert cluster_cfg["tls_enable_renegotiation"] == False, (
            f"Invalid cluster config: {cluster_cfg['tls_enable_renegotiation']} != False"
        )
        self.redpanda.set_cluster_config(
            admin_client=self.admin,
            values={"tls_enable_renegotiation": enable_renegotiation},
            expect_restart=True,
        )
        cluster_cfg = self.admin.get_cluster_config(key="tls_enable_renegotiation")
        assert cluster_cfg["tls_enable_renegotiation"] == enable_renegotiation, (
            f"Invalid cluster config: {cluster_cfg['tls_enable_renegotiation']} != {enable_renegotiation}"
        )
        for n in self.redpanda.nodes:
            self.verify_tls_renegotiation(node=n, expect_fail=not enable_renegotiation)
