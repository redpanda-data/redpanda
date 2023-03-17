# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from requests.exceptions import HTTPError
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.services.rpk_consumer import RpkConsumer

from datetime import datetime
from functools import reduce

from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.utils.functional import flat_map


class UsageTest(RedpandaTest):
    """
    Tests that the usage endpoint is tracking kafka metrics
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context):
        extra_conf = {
            'enable_usage': True,
            'usage_num_windows': 60,
            'usage_window_width_interval_sec': 1,
            'usage_disk_persistance_interval_sec': 5
        }
        super(UsageTest, self).__init__(test_context=test_context,
                                        extra_rp_conf=extra_conf)
        self._ctx = test_context
        self._admin = Admin(self.redpanda)

    def _get_all_usage(self, include_open=True):
        """
        Performs an additional check that results are correctly ordered
        """
        def validate(node_response):
            prev_begin = datetime.now()
            prev_end = datetime.now()
            for e in node_response:
                begin = datetime.fromtimestamp(e['begin_timestamp'])
                if e['open'] is True:
                    # Open windows have a value of 0 for end timestamp
                    prev_begin = begin
                    continue
                end = datetime.fromtimestamp(e['end_timestamp'])
                assert begin < end, f"Begin: {begin}, End: {end}"
                assert begin < prev_begin, f"Begin: {begin}, PrevBegin: {prev_begin}"
                assert end < prev_end, f"End: {end}, PrevEnd: {prev_end}"
                prev_begin = begin
                prev_end = end

            return node_response

        # validate() checks results are ordered newest to oldest
        return flat_map(
            lambda x: validate(self._admin.get_usage(x, include_open)),
            self.redpanda.nodes)

    def _calculate_total_usage(self, results=None):
        # Total number of ingress/egress bytes across entire cluster
        def all_bytes(x):
            kafka_ingress = x['kafka_bytes_sent_count']
            kafka_egress = x['kafka_bytes_received_count']
            return kafka_ingress + kafka_egress

        if results is None:
            results = self._get_all_usage()

        # Some traffic over the kafka port should be expected at startup
        # but not a large amount
        return reduce(lambda acc, x: acc + all_bytes(x), results, 0)

    def _produce_and_consume_data(self, records=10240, size=512):
        # Test some data is recorded as activity over kafka port begins
        producer = KafkaCliTools(self.redpanda)
        producer.produce(self.topic, records, size, acks=1)
        total_produced = records * size

        consumer = RpkConsumer(self._ctx, self.redpanda, self.topic)
        consumer.start()
        self._bytes_received = 0

        def bytes_observed():
            for msg in consumer.messages:
                value = msg["value"]
                if value is not None:
                    self._bytes_received += len(value)
            return self._bytes_received >= total_produced

        wait_until(bytes_observed, timeout_sec=30, backoff_sec=1)
        consumer.stop()
        return total_produced

    @cluster(num_nodes=4)
    def test_usage_metrics_collection(self):
        # Assert windows are closing
        time.sleep(2)
        response = self._get_all_usage()
        assert len(response) >= 2, f"Not enough windows observed, {response}"

        # Some traffic over the kafka port should be expected at startup
        # but not a large amount
        total_data = self._calculate_total_usage()
        assert total_data < 4096, f"More then 4k traffic observed: {total_data}"

        # Produce / consume test data, should observe usage numbers increase
        total_produced = self._produce_and_consume_data()

        # Assert that more then data the data produced has been recorded, responses
        # and the initial non 0 recorded data are also included in the total recorded amt
        total_data = self._calculate_total_usage()
        assert total_data > total_produced, f"Expected {total_produced} observed: {total_data}"

    @cluster(num_nodes=4)
    def test_usage_collection_restart(self):
        self._admin.patch_cluster_config(
            upsert={'usage_disk_persistance_interval_sec': 1})
        # Ensure the restarted accounting fiber is up before data begins to be produced
        time.sleep(2)

        # Produce / consume test data, should observe usage numbers increase
        _ = self._produce_and_consume_data()
        time.sleep(3)

        # Query usage of node to restart before restart
        usage_pre_restart = self._calculate_total_usage(
            self._admin.get_usage(node=self.redpanda.nodes[0]))

        self.redpanda.restart_nodes([self.redpanda.nodes[0]])

        # Compare values pre/post restart to ensure data was persisted to disk
        usage_post_restart = self._calculate_total_usage(
            self._admin.get_usage(node=self.redpanda.nodes[0]))
        assert usage_post_restart >= usage_pre_restart, f"Usage post restart: {usage_post_restart} Usage pre restart: {usage_pre_restart}"

    @cluster(num_nodes=3)
    def test_usage_settings_changed(self):
        # Should expect maximum of 2 windows per broker
        self._admin.patch_cluster_config(upsert={'usage_num_windows': 2})
        time.sleep(2)
        response = self._get_all_usage()
        assert len(response) == 6

        # Should expect a 500 from the cluster
        self._admin.patch_cluster_config(upsert={'enable_usage': False})
        try:
            _ = self._get_all_usage()
            assert False, "Expecting v1/usage to return 400"
        except HTTPError as e:
            assert e.response.status_code == 400

        # Should expect windows to have been resized correctly
        self._admin.patch_cluster_config(
            upsert={
                'enable_usage': True,
                'usage_num_windows': 3,
                'usage_window_width_interval_sec': 2
            })
        time.sleep(3)
        response = self._get_all_usage(False)
        for r in response:
            begin = datetime.fromtimestamp(r['begin_timestamp'])
            end = datetime.fromtimestamp(r['end_timestamp'])
            total = end - begin
            assert total.seconds == 2 or total.seconds == 3, total.seconds
