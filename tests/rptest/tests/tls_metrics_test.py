# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import socket
import time
from datetime import datetime, timedelta
from typing import Optional, Callable
import crc32c

from ducktape.cluster.cluster import ClusterNode

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SecurityConfig, TLSProvider, SchemaRegistryConfig, PandaproxyConfig
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.services.redpanda import MetricSamples, MetricsEndpoint, RedpandaService
from rptest.services import tls
from rptest.tests.pandaproxy_test import User, PandaProxyTLSProvider
from rptest.util import wait_until_result

# Basic configs to enable TLS for internal RPC and Admin API
RPC_TLS_CONFIG = dict(enabled=True,
                      require_client_auth=True,
                      key_file=RedpandaService.TLS_SERVER_KEY_FILE,
                      cert_file=RedpandaService.TLS_SERVER_CRT_FILE,
                      truststore_file=RedpandaService.TLS_CA_CRT_FILE,
                      crl_file=RedpandaService.TLS_CA_CRL_FILE)

ADMIN_TLS_CONFIG = dict(name='iplistener',
                        enabled=True,
                        require_client_auth=True,
                        key_file=RedpandaService.TLS_SERVER_KEY_FILE,
                        cert_file=RedpandaService.TLS_SERVER_CRT_FILE,
                        truststore_file=RedpandaService.TLS_CA_CRT_FILE,
                        crl_file=RedpandaService.TLS_CA_CRL_FILE)


class FaketimeTLSProvider(TLSProvider):
    def __init__(self,
                 tls: tls.TLSCertManager,
                 broker_faketime='-0d',
                 client_faketime='-0d'):
        self.tls = tls
        self.broker_faketime = broker_faketime
        self.client_faketime = client_faketime

    @property
    def ca(self):
        return self.tls.ca

    def create_broker_cert(self, redpanda, node):
        assert node in redpanda.nodes
        return self.tls.create_cert(node.name, faketime=self.broker_faketime)

    def create_service_client_cert(self, _, name):
        return self.tls.create_cert(socket.gethostname(),
                                    name=name,
                                    common_name=name,
                                    faketime=self.client_faketime)


class TLSMetricsTestBase(RedpandaTest):
    CERT_METRICS: list[str] = [
        'truststore_expires_at_timestamp_seconds',
        'certificate_expires_at_timestamp_seconds',
        'loaded_at_timestamp_seconds',
        'certificate_valid',
        'certificate_serial',
        'trust_file_crc32c',
    ]

    EXPECTED_LABELS: list[str] = [
        'area',
        'detail',
        'shard',
    ]

    def __init__(self,
                 *args,
                 broker_faketime='-0d',
                 client_faketime='-0d',
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.broker_faketime = broker_faketime
        self.client_faketime = client_faketime

        self.security = SecurityConfig()
        su_username, su_password, su_algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        self.admin_user = User(0)
        self.admin_user.username = su_username
        self.admin_user.password = su_password
        self.admin_user.algorithm = su_algorithm

        self.schema_registry_config = SchemaRegistryConfig()
        self.schema_registry_config.require_client_auth = True
        self.pandaproxy_config = PandaproxyConfig()
        self.pandaproxy_config.require_client_auth = True

        self.tls = None

    def setUp(self):
        assert self.tls is not None

        self.security.require_client_auth = True
        self.security.kafka_enable_authorization = True
        self.security.enable_sasl = True
        self.schema_registry_config.authn_method = 'http_basic'
        self.pandaproxy_config.authn_method = 'http_basic'

        client_cert = self.tls.create_cert(
            socket.gethostname(),
            common_name=self.admin_user.username,
            name='test_client_tls')

        self.security.tls_provider = FaketimeTLSProvider(
            self.tls,
            broker_faketime=self.broker_faketime,
            client_faketime=self.client_faketime)
        self.schema_registry_config.client_key = client_cert.key
        self.schema_registry_config.client_crt = client_cert.crt
        self.pandaproxy_config.client_key = client_cert.key
        self.pandaproxy_config.client_crt = client_cert.crt

        self.redpanda.set_security_settings(self.security)
        self.redpanda.set_schema_registry_settings(self.schema_registry_config)
        self.redpanda.set_pandaproxy_settings(self.pandaproxy_config)

        super().setUp()

    def _get_metrics_from_node(
        self,
        node: ClusterNode,
        patterns: list[str],
        endpoint=MetricsEndpoint.METRICS
    ) -> Optional[dict[str, MetricSamples]]:
        def get_metrics_from_node_sync(patterns: list[str]):
            samples = self.redpanda.metrics_samples(
                patterns,
                [node],
                endpoint,
            )
            success = samples is not None
            return success, samples

        try:
            return wait_until_result(
                lambda: get_metrics_from_node_sync(patterns),
                timeout_sec=2,
                backoff_sec=.1)
        except TimeoutError as e:
            return None

    def _unpack_samples(self, metric_samples):
        return {
            k: [{
                'value': s.value,
                'labels': s.labels
            } for s in metric_samples[k].samples]
            for k in metric_samples.keys()
        }

    def _days_from_now(self, days):
        return (datetime.now() + timedelta(days=days)).timestamp()


class TLSMetricsTest(TLSMetricsTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tls = tls.TLSCertManager(self.logger)

    def setUp(self):
        super().setUp()

    @cluster(num_nodes=3)
    def test_metrics(self):
        """
        Test presence of certificate metrics
        """
        node = self.redpanda.nodes[0]
        metrics_samples = self._get_metrics_from_node(node, self.CERT_METRICS)
        assert metrics_samples is not None
        assert sorted(metrics_samples.keys()) == sorted(self.CERT_METRICS)

    @cluster(num_nodes=3)
    def test_public_metrics(self):
        """
        Test presense of certificate metrics on public endpoint
        """
        node = self.redpanda.nodes[0]
        metrics_samples = self._get_metrics_from_node(
            node,
            self.CERT_METRICS,
            endpoint=MetricsEndpoint.PUBLIC_METRICS,
        )
        assert metrics_samples is not None
        assert sorted(metrics_samples.keys()) == sorted(self.CERT_METRICS)

    @cluster(num_nodes=3)
    def test_labels(self):
        """
        Test presense of expected labels on metrics
        """
        node = self.redpanda.nodes[1]
        metrics_samples = self._get_metrics_from_node(node, self.CERT_METRICS)
        assert metrics_samples is not None
        metrics = self._unpack_samples(metrics_samples)
        for name in metrics.keys():
            assert (all([
                sorted(m['labels'].keys()) == sorted(self.EXPECTED_LABELS)
                for m in metrics[name]
            ]))

        # expect metrics to be emitted exclusively from shard0
        for name in metrics.keys():
            assert (all(
                [int(m['labels']['shard']) == 0 for m in metrics[name]]))

    @cluster(num_nodes=3)
    def test_services(self):
        """
        Test that metrics are successfully enabled for various services.
        """
        self.redpanda.stop()

        # Set up TLS for RPC and Admin API (iplistener)
        cfg_overrides = {}

        def set_cfg(node):
            cfg_overrides[node] = dict(rpc_server_tls=RPC_TLS_CONFIG,
                                       admin_api_tls=ADMIN_TLS_CONFIG)

        self.redpanda.for_nodes(self.redpanda.nodes, set_cfg)
        self.redpanda.start(node_config_overrides=cfg_overrides)

        node = self.redpanda.nodes[0]
        metric = self.CERT_METRICS[0]
        metrics_samples = self._get_metrics_from_node(
            node,
            [metric],
            endpoint=MetricsEndpoint.PUBLIC_METRICS,
        )

        assert metrics_samples is not None
        vals = self._unpack_samples(metrics_samples)
        areas = [v['labels']['area'] for v in vals[metric]]
        self.logger.debug(f"Areas w/ TLS enabled: {areas}")

        assert 'kafka' in areas
        assert 'schema_registry' in areas
        assert 'rpc' in areas
        assert 'rest_proxy' in areas
        assert 'admin' in areas

    @cluster(num_nodes=3)
    def test_expiry_reload(self):
        """
        Verify that when replacing certificat X by certificate Y s.t.
        expiry(Y) > expiry(X), the new expiry is reflected in the metrics.
        """
        node = self.redpanda.nodes[0]

        metrics_samples = self._get_metrics_from_node(node, self.CERT_METRICS)
        assert metrics_samples is not None, "Failed to get metrics"
        vals = self._unpack_samples(metrics_samples)

        status_before = dict(
            expiry=vals['certificate_expires_at_timestamp_seconds'][0]
            ['value'],
            loaded=vals['loaded_at_timestamp_seconds'][0]['value'])
        self.logger.debug(
            f"Before reload: {json.dumps(status_before, indent=1)}")

        time.sleep(5)

        self.security.tls_provider = FaketimeTLSProvider(
            tls=tls.TLSCertManager(self.logger, cert_expiry_days=10))

        self.redpanda.set_security_settings(self.security)
        self.redpanda.write_tls_certs()

        metrics_samples = self._get_metrics_from_node(node, self.CERT_METRICS)
        assert metrics_samples is not None, "Failed to get metrics"
        vals = self._unpack_samples(metrics_samples)

        status_after = dict(
            expiry=vals['certificate_expires_at_timestamp_seconds'][0]
            ['value'],
            loaded=vals['loaded_at_timestamp_seconds'][0]['value'])
        self.logger.debug(
            f"After reload: {json.dumps(status_after, indent=1)}")

        five_days = 5 * 24 * 60 * 60

        assert status_before['loaded'] < status_after[
            'loaded'], f"Unexpected status after reload: {json.dumps(status_after)}"
        assert status_before['expiry'] + five_days < status_after[
            'expiry'], f"Unexpected status after reload: {json.dumps(status_after)}"

    @cluster(num_nodes=3)
    def test_crc32c(self):
        node = self.redpanda.nodes[0]

        def check_crc():
            metrics_samples = self._get_metrics_from_node(
                node, ['trust_file_crc32c'])
            assert metrics_samples is not None, "Failed to get metrics"
            vals = self._unpack_samples(metrics_samples)['trust_file_crc32c']

            assert len(vals) > 0, "Missing crc metrics for some reason"

            expected = crc32c.crc32c(
                open(self.security.tls_provider.ca.crt, 'rb').read())

            for v in vals:
                got = int(v['value'])
                assert got == expected, f"Expected {expected}; Got {got}"

            return expected

        original = check_crc()

        self.security.tls_provider = FaketimeTLSProvider(
            tls=tls.TLSCertManager(self.logger, cert_expiry_days=10))

        self.redpanda.set_security_settings(self.security)
        self.redpanda.write_tls_certs()

        reloaded = check_crc()

        assert original != reloaded, f"Checksums unexpectedly equal"


class TLSMetricsTestChain(TLSMetricsTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tls = tls.TLSChainCACertManager(self.logger, chain_len=4)

    def setUp(self):
        super().setUp()

    @cluster(num_nodes=3)
    def test_cert_chain_metrics(self):
        """
        Test various behaviors given a longer chained truststore
        """
        node = self.redpanda.nodes[0]

        metrics_samples = self._get_metrics_from_node(node, self.CERT_METRICS)
        assert metrics_samples is not None
        assert sorted(metrics_samples.keys()) == sorted(
            self.CERT_METRICS), f"Missing metrics: {metrics_samples.keys()}"

        metric_values = self._unpack_samples(metrics_samples)

        assert len(metric_values['loaded_at_timestamp_seconds']
                   ) > 0, "loaded_at is not present"
        assert all([
            int(v['value']) == 1 for v in metric_values['certificate_valid']
        ]), "Cert not valid"
        assert all([
            int(time.time()) < v['value']
            and v['value'] <= self._days_from_now(self.tls.ca_expiry_days)
            for v in metric_values['truststore_expires_at_timestamp_seconds']
        ]), "Trustore expiry should be that of shortest-lived CA in the chain"
        assert all([
            int(time.time()) < v['value']
            and v['value'] <= self._days_from_now(self.tls.cert_expiry_days)
            for v in metric_values['certificate_expires_at_timestamp_seconds']
        ]), "Certificate expiry should reflect configured value"
        assert all([
            int(v['value']) == 3 for v in metric_values['certificate_serial']
        ])


class TLSMetricsTestExpiring(TLSMetricsTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, broker_faketime='-23.995h', **kwargs)
        self.tls = tls.TLSCertManager(self.logger, cert_expiry_days=1)

    def setUp(self):
        super().setUp()

    @cluster(num_nodes=3)
    def test_detect_expired_cert(self):
        """
        Test that metrics detect an expired certificate
        """
        node = self.redpanda.nodes[0]

        def certs_expired():
            metric_values = self._unpack_samples(
                self._get_metrics_from_node(node, ['certificate_valid']))
            success = all(not v['value']
                          for v in metric_values['certificate_valid'])
            return success, metric_values

        metric_values = wait_until_result(certs_expired,
                                          timeout_sec=30,
                                          backoff_sec=4)

        assert metric_values is not None, "metric_values unexpectedly None"

        assert all(not v['value'] for v in metric_values['certificate_valid']
                   ), "Cert(s) unexpectedly still valid"
