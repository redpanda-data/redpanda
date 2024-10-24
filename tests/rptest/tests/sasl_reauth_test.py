# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import json
from typing import Optional

from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import SecurityConfig
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import MetricSamples, MetricsEndpoint, SecurityConfig
from rptest.util import wait_until_result

from ducktape.utils.util import wait_until
from ducktape.cluster.cluster import ClusterNode

import typing

EXAMPLE_TOPIC = 'foo'

REAUTH_METRIC = "sasl_session_reauth_attempts_total"
EXPIRATION_METRIC = "sasl_session_expiration_total"


def get_metrics_from_node(
    redpanda: RedpandaService,
    node: ClusterNode,
    patterns: list[str],
    endpoint: MetricsEndpoint = MetricsEndpoint.METRICS
) -> Optional[dict[str, MetricSamples]]:
    def get_metrics_from_node_sync(patterns: list[str]):
        samples = redpanda.metrics_samples(patterns, [node], endpoint)
        success = samples is not None
        return success, samples

    try:
        return wait_until_result(lambda: get_metrics_from_node_sync(patterns),
                                 timeout_sec=2,
                                 backoff_sec=.1)
    except TimeoutError as e:
        return None


def get_sasl_metrics(redpanda: RedpandaService,
                     endpoint: MetricsEndpoint = MetricsEndpoint.METRICS):
    sasl_metrics = [
        EXPIRATION_METRIC,
        REAUTH_METRIC,
    ]
    node_samples = []
    for node in redpanda.nodes:
        samples = get_metrics_from_node(redpanda, node, sasl_metrics, endpoint)
        node_samples.append(samples)

    result = {}
    for samples in node_samples:
        for k in samples.keys():
            result[k] = result.get(k, 0) + sum(
                [int(s.value) for s in samples[k].samples])
    return result


class SASLReauthBase(RedpandaTest):
    def __init__(self,
                 conn_max_reauth: typing.Union[int, None] = 8000,
                 **kwargs):
        security = SecurityConfig()
        security.enable_sasl = True
        super().__init__(
            num_brokers=3,
            security=security,
            extra_rp_conf={'kafka_sasl_max_reauth_ms': conn_max_reauth},
            **kwargs)

        self.su_username, self.su_password, self.su_algorithm = self.redpanda.SUPERUSER_CREDENTIALS

        self.rpk = RpkTool(self.redpanda,
                           username=self.su_username,
                           password=self.su_password,
                           sasl_mechanism=self.su_algorithm)

    def make_su_client(self):
        return PythonLibrdkafka(self.redpanda,
                                username=self.su_username,
                                password=self.su_password,
                                algorithm=self.su_algorithm)


class ReauthConfigTest(SASLReauthBase):
    """
    Tests that kafka_sasl_max_reauth_ms{null} produces original behavior
    i.e. no reauth, no session expiry

    Also tests reauth when enabled after cluster startup
    """

    MAX_REAUTH_MS = 2000
    PRODUCE_DURATION_S = MAX_REAUTH_MS * 2 / 1000
    PRODUCE_INTERVAL_S = 0.1
    PRODUCE_ITER = int(PRODUCE_DURATION_S / PRODUCE_INTERVAL_S)

    def __init__(self, test_context, **kwargs):
        super().__init__(test_context=test_context,
                         conn_max_reauth=None,
                         **kwargs)

    @cluster(num_nodes=3)
    def test_reauth_disabled(self):
        self.rpk.create_topic(EXAMPLE_TOPIC)
        su_client = self.make_su_client()
        producer = su_client.get_producer()
        producer.poll(1.0)

        expected_topics = set([EXAMPLE_TOPIC])
        wait_until(lambda: set(producer.list_topics(timeout=5).topics.keys())
                   == expected_topics,
                   timeout_sec=5)

        for i in range(0, self.PRODUCE_ITER):
            producer.poll(0.0)
            producer.produce(topic=EXAMPLE_TOPIC, key='bar', value=str(i))
            time.sleep(self.PRODUCE_INTERVAL_S)

        producer.flush(timeout=5)

        metrics = get_sasl_metrics(self.redpanda)
        self.redpanda.logger.debug(f"SASL metrics: {metrics}")
        assert (EXPIRATION_METRIC in metrics.keys())
        assert (metrics[EXPIRATION_METRIC] == 0
                ), "SCRAM sessions should not expire"
        assert (REAUTH_METRIC in metrics.keys())
        assert (metrics[REAUTH_METRIC] == 0
                ), "Expected no reauths with max_reauth = NULL"

    @cluster(num_nodes=3)
    def test_enable_after_start(self):
        '''
        We should be able to enable reauthentication on a live cluster
        '''
        new_cfg = {'kafka_sasl_max_reauth_ms': self.MAX_REAUTH_MS}
        self.redpanda.set_cluster_config(new_cfg)

        self.rpk.create_topic(EXAMPLE_TOPIC)
        su_client = self.make_su_client()
        producer = su_client.get_producer()
        producer.poll(1.0)

        expected_topics = set([EXAMPLE_TOPIC])
        wait_until(lambda: set(producer.list_topics(timeout=5).topics.keys())
                   == expected_topics,
                   timeout_sec=5)

        for i in range(0, self.PRODUCE_ITER):
            producer.poll(0.0)
            producer.produce(topic=EXAMPLE_TOPIC, key='bar', value=str(i))
            time.sleep(self.PRODUCE_INTERVAL_S)

        producer.flush(timeout=5)

        metrics = get_sasl_metrics(self.redpanda)
        self.redpanda.logger.debug(f"SASL metrics: {metrics}")
        assert (EXPIRATION_METRIC in metrics.keys())
        assert (metrics[EXPIRATION_METRIC] == 0
                ), "Client should reauth before session expiry"
        assert (REAUTH_METRIC in metrics.keys())
        assert (metrics[REAUTH_METRIC]
                > 0), "Expected client reauth on some broker..."
