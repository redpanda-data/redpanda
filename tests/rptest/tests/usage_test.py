# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import time
import random
import operator
from rptest.services.redpanda import RedpandaService
from rptest.clients.rpk import RpkTool
from requests.exceptions import HTTPError
from rptest.services.cluster import cluster
from rptest.utils.si_utils import BucketView
from rptest.services.redpanda import SISettings
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.kgo_verifier_services import KgoVerifierProducer

from datetime import datetime
from functools import reduce

from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.utils.functional import flat_map, flatten


class UsageWindow:
    def __init__(self, begin, end, is_open, bytes_sent, bytes_received,
                 bytes_in_cloud_storage):
        self.begin = begin
        self.end = end
        self.is_open = is_open
        self.bytes_sent = bytes_sent
        self.bytes_received = bytes_received
        self.bytes_in_cloud_storage = bytes_in_cloud_storage

    @property
    def is_closed(self):
        return not self.is_open

    def __repr__(self):
        r = {
            'begin': self.begin,
            'end': self.end,
            'is_open': self.is_open,
            'bytes_sent': self.bytes_sent,
            'bytes_received': self.bytes_received,
            'bytes_in_cloud_storage': self.bytes_in_cloud_storage
        }
        return str(r)


def parse_usage_response(usage_response):
    def parse_usage_window(e):
        return UsageWindow(e['begin_timestamp'], e['end_timestamp'], e['open'],
                           e['kafka_bytes_sent_count'],
                           e['kafka_bytes_received_count'],
                           e['cloud_storage_bytes_gauge'])

    return [parse_usage_window(e) for e in usage_response]


def assert_usage_consistency(node, a, b):
    """
    This method returns true if windows with matching timestamps contain the same values

    node: String (For debugging if assertions fires)
    a: List[UsageWindow]
    b: List[UsageWindow]

    Windows from list b are assumed to be from a newer response, so this method will verify
    that all items from b, if they exist in a, are equivalent.

    One excepton if the window is considered an 'open' window, there would be at max 1
    per request, if there is a matching open window in both inputs, their values
    are checked so that the value of b's open window would be >= the value in a's.
    """
    def group_windows(ws):
        return {(x.begin, x.end): x for x in ws}

    def compare(x, y, fn):
        return fn(x.bytes_sent, y.bytes_sent) and fn(x.bytes_received,
                                                     y.bytes_received)

    # Windows from group 'a' are expected to be from a previous request
    grp_a = group_windows(a)
    for x in b:
        key = (x.begin, x.end)
        a_value = grp_a.get(key, None)
        if a_value is not None:
            fn = operator.le if a_value.is_open else operator.eq
            assert compare(
                a_value, x, fn
            ), f"Failed to compare historical values, node: {node} a: {str(a_value)} b:{str(x)}"

            # Can only assert on value of cloud storage if the windows are closed, since across
            # the time of an open window, leadership may change and the value of the cs_bytes field
            # depends on if the broker is the controller leader or not
            if a_value.is_closed and x.is_closed:
                assert a_value.bytes_in_cloud_storage == x.bytes_in_cloud_storage, f"Failed bytes_in_cs compare node: {node} a: {str(a_value)} b:{str(x)}"


class UsageTest(RedpandaTest):
    """
    Tests that the usage endpoint is tracking kafka metrics
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context):
        self._settings = self._usage_conf(enable_usage=True,
                                          num_windows=30,
                                          window_interval=3,
                                          disk_write_interval=5)
        super(UsageTest, self).__init__(test_context=test_context,
                                        log_level='debug',
                                        extra_rp_conf=self._settings)
        self._ctx = test_context
        self._admin = Admin(self.redpanda)
        # Dict[NodeName, v1/usage_response] cached history of responses
        # used to validate future usage requests against historical ones to
        # detect for any inconsistencies
        self._previous_response = {}

    @property
    def usage_enabled(self):
        return self._settings.get('enable_usage', False)

    @property
    def num_windows(self):
        return self._settings.get('usage_num_windows', 0)

    @property
    def window_interval(self):
        return self._settings.get('usage_window_width_interval_sec', 0)

    @property
    def disk_write_interval(self):
        return self._settings.get('usage_disk_persistance_interval_sec', 0)

    def _usage_conf(self,
                    enable_usage=None,
                    num_windows=None,
                    window_interval=None,
                    disk_write_interval=None):
        changes = {}
        if enable_usage is not None:
            changes['enable_usage'] = enable_usage
        if num_windows is not None:
            changes['usage_num_windows'] = num_windows
        if window_interval is not None:
            changes['usage_window_width_interval_sec'] = window_interval
        if disk_write_interval is not None:
            changes[
                'usage_disk_persistance_interval_sec'] = disk_write_interval

        return changes

    def _modify_settings(self,
                         enable_usage=None,
                         num_windows=None,
                         window_interval=None,
                         disk_write_interval=None):
        conf = self._usage_conf(enable_usage=enable_usage,
                                num_windows=num_windows,
                                window_interval=window_interval,
                                disk_write_interval=disk_write_interval)
        self._admin.patch_cluster_config(upsert=conf)
        self._settings = self._settings | conf
        return conf

    def _get_usage(self, node, include_open, retries=3):
        exc = None
        while retries > 0:
            try:
                response = self._admin.get_usage(node, include_open)
                if response == []:
                    raise RuntimeError("Empty response received")
                return response
            except Exception as e:
                self.redpanda.logger.error(
                    f"Error making v1/usage request: {e}")
                retries -= 1
                time.sleep(1)
                exc = e
        raise exc

    def _get_all_usage(self,
                       include_open=True,
                       allow_gaps=False,
                       only_nodes=None):
        """
        Performs an additional check that results are correctly ordered

        include_open: Make usage requests with the include_open_window option
        allow_gaps: Assert all closed windows end timestamps match with proceeding begin
        only_nodes: Make request to a subset of rp nodes
        """
        def assert_history(node, node_response):
            # Ensure no historical results were lost or modified
            prev = self._previous_response.get(node, None)
            if prev is not None:
                assert_usage_consistency(node, prev, node_response)
            self._previous_response[node] = node_response

        def validate_ts(ts):
            # All timestamps must be a multiple of the interval in seconds
            return ts % self.window_interval == 0

        def validate_no_duplicates(response):
            # Ensure no two entires contain matching begin & end timestamps
            to_tss = [(x.begin, x.end) for x in response]
            return len(to_tss) == len(set(to_tss))

        def validate(node, node_response):
            # Ensure all windows in in chronological order with end timestamp
            # matching next beginning
            node_response = parse_usage_response(node_response)
            assert validate_no_duplicates(node_response), f"{node_response}"
            prev_begin = float('inf')
            prev_end = float('inf')
            for e in node_response:
                assert validate_ts(e.begin), e.begin
                if e.is_open is True:
                    # Open windows have a value of time::now() for end
                    prev_begin = e.begin
                    continue
                assert validate_ts(e.end), e.end
                # A window may be dropped which would mean this difference
                # being a multiple of the interval but not less than it
                assert e.end - e.begin >= self.window_interval, f"Expected interval: {self.window_interval} observed {e.end - e.begin}"
                assert e.begin < e.end, f"Begin: {e.begin}, End: {e.end}"
                assert e.begin < prev_begin, f"Begin: {e.begin}, PrevBegin: {prev_begin}"
                assert e.end < prev_end, f"End: {e.end}, PrevEnd: {prev_end}"
                if prev_begin != float('inf'):
                    if not allow_gaps:
                        assert e.end == prev_begin, f"End: {e.end} PrevEnd: {prev_begin}"
                    else:
                        assert e.end <= prev_begin, f"End: {e.end}, PrevBegin: {prev_begin}"
                prev_begin = e.begin
                prev_end = e.end

            assert_history(node, node_response)
            return node_response

        # validate() checks results are ordered newest to oldest and the timestamps
        # are multiples of the window interval
        nodes = self.redpanda.nodes if only_nodes is None else only_nodes
        return flat_map(
            lambda x: validate(x.name, self._get_usage(x, include_open)),
            nodes)

    def _calculate_total_usage(self, results=None):
        # Total number of ingress/egress bytes across entire cluster
        def all_bytes(x):
            kafka_ingress = x.bytes_sent
            kafka_egress = x.bytes_received
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

    def _grab_log_lines(self, pattern):
        def search_log_lines(node):
            lines = []
            for line in node.account.ssh_capture(
                    f"grep \"{pattern}\" {RedpandaService.STDOUT_STDERR_CAPTURE} || true",
                    timeout_sec=60):
                lines.append(line.strip())
            return lines

        return [search_log_lines(node) for node in self.redpanda.nodes]

    def _validate_timer_interval(self):
        log_lines = self._grab_log_lines("Usage based billing window_close*")

        assert len(log_lines) > 0, "Debug logging not enabled"
        self.redpanda.logger.debug(f"Log lines: {log_lines}")

        # Remove the first element as it is expected to not be aligned
        log_lines = flatten([xs[1:] for xs in log_lines])

        regex = re.compile(r".*delta: (?P<delta>\d*)")

        # Parse the value from the log line for lines across all nodes
        def delta_parse(x):
            v = regex.match(x)
            if v is None:
                raise RuntimeError(f"Unexpected log line: {x}")
            return int(v[1])

        # Parse the contents to just grab the value of `delta` within the log
        fire_times = [delta_parse(x) for x in log_lines]

        # Ensure all deltas are 0 meaning there is no skew
        # Make an exception for 1 second delta to account for late timers
        # in debug builds
        return all([x == 0 or x == 1 for x in fire_times])

    @cluster(num_nodes=3)
    def test_usage_metrics_collection(self):
        # Assert windows are closing
        time.sleep(2)
        response = self._get_all_usage()
        assert len(response) >= 2, f"Not enough windows observed, {response}"

        # Some traffic over the kafka port should be expected at startup
        # but not a large amount
        total_data = self._calculate_total_usage()

        iterations = 1
        prev_usage = self._get_all_usage()
        producer = KafkaCliTools(self.redpanda)
        while iterations < (self.num_windows + 7):
            producer.produce(self.topic, (512 * iterations), 512, acks=1)
            time.sleep(1)

            usage = self._get_all_usage()

            # 3 node cluster * 30 max windows == 90 windows total
            assert len(usage) <= 90, f"iterations: {iterations}"
            if len(usage) == len(prev_usage):
                # Theres been no new window closed
                pass
            else:
                # Assert that more then data the data produced has been recorded, responses
                # and the initial non 0 recorded data are also included in the total recorded amt
                total_data = self._calculate_total_usage(usage)
                total_prev = self._calculate_total_usage(prev_usage)
                assert total_data > 0, "no data observed"
                assert total_data >= total_prev, f"Expected {total_data} >= {total_prev} itr: {iterations}"

            prev_usage = usage
            iterations += 1

        # Additional validation to ensure there were no gaps and the timer fired exactly on time
        assert self._validate_timer_interval(
        ), "A timer skew of greater then 1s has been detected within a usage fiber"

    @cluster(num_nodes=4)
    def test_usage_collection_restart(self):
        # Produce / consume test data, should observe usage numbers increase
        _ = self._produce_and_consume_data()
        time.sleep(3)

        # Query usage of node to restart before restart
        usage_pre_restart = self._calculate_total_usage(
            self._get_all_usage(include_open=True,
                                allow_gaps=True,
                                only_nodes=[self.redpanda.nodes[0]]))

        self.redpanda.restart_nodes([self.redpanda.nodes[0]])

        # Compare values pre/post restart to ensure data was persisted to disk
        usage_post_restart = self._calculate_total_usage(
            self._get_all_usage(include_open=True,
                                allow_gaps=True,
                                only_nodes=[self.redpanda.nodes[0]]))
        assert usage_post_restart >= usage_pre_restart, f"Usage post restart: {usage_post_restart} Usage pre restart: {usage_pre_restart}"

    @cluster(num_nodes=3)
    def test_usage_settings_changed(self):
        # Should expect maximum of 2 windows per broker (including open)
        self._modify_settings(num_windows=2)
        # Wait 3 intervals
        time.sleep(self.window_interval * 3)
        response = self._get_all_usage()
        assert len(response) == 6, f"{len(response)}"

        # Should expect a 500 from the cluster
        self._modify_settings(enable_usage=False)
        try:
            _ = self._get_all_usage()
            assert False, "Expecting v1/usage to return 400"
        except HTTPError as e:
            assert e.response.status_code == 400

        # Should expect windows to have been resized correctly
        self._modify_settings(enable_usage=True,
                              num_windows=3,
                              window_interval=2,
                              disk_write_interval=5)
        time.sleep(self.window_interval * 5)
        response = self._get_all_usage(include_open=False, allow_gaps=True)
        for r in response:
            if r.is_closed:
                assert (r.end - r.begin) == self.window_interval


class UsageTestCloudStorageMetrics(RedpandaTest):
    message_size = 32 * 1024  # 32KiB
    log_segment_size = 256 * 1024  # 256KiB
    produce_byte_rate_per_ntp = 512 * 1024  # 512 KiB
    target_runtime = 20  # seconds
    check_interval = 5  # seconds

    topics = [
        TopicSpec(name="test-topic-1",
                  partition_count=3,
                  replication_factor=3,
                  retention_bytes=3 * log_segment_size),
        TopicSpec(name="test-topic-2",
                  partition_count=1,
                  replication_factor=1,
                  retention_bytes=3 * log_segment_size,
                  cleanup_policy=TopicSpec.CLEANUP_COMPACT)
    ]

    def __init__(self, test_context):
        # Parameters to ensure timely reporting of cloud usage stats via
        # the kafka::usage_manager
        extra_rp_conf = dict(health_monitor_max_metadata_age=2000,
                             enable_usage=True,
                             usage_num_windows=30,
                             usage_window_width_interval_sec=1,
                             log_compaction_interval_ms=2000,
                             compacted_log_segment_size=self.log_segment_size)

        super(UsageTestCloudStorageMetrics, self).__init__(
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
            si_settings=SISettings(test_context,
                                   log_segment_size=self.log_segment_size,
                                   cloud_storage_housekeeping_interval_ms=2000,
                                   fast_uploads=True))

        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)
        self.s3_port = self.si_settings.cloud_storage_api_endpoint_port

    def _create_producers(self) -> list[KgoVerifierProducer]:
        producers = []

        for topic in self.topics:
            bps = self.produce_byte_rate_per_ntp * topic.partition_count
            bytes_count = bps * self.target_runtime
            msg_count = bytes_count // self.message_size

            self.logger.info(f"Will produce {bytes_count / 1024}KiB at"
                             f"{bps / 1024}KiB/s on topic={topic.name}")
            producers.append(
                KgoVerifierProducer(self.test_context,
                                    self.redpanda,
                                    topic,
                                    msg_size=self.message_size,
                                    msg_count=msg_count,
                                    rate_limit_bps=bps))

        return producers

    @cluster(num_nodes=5)
    def test_usage_manager_cloud_storage(self):
        """
        """
        assert self.admin.cloud_storage_usage() == 0

        # Produce some data to a cloud_storage enabled topic
        producers = self._create_producers()
        for p in producers:
            p.start()

        wait_until(lambda: all([p.is_complete() for p in producers]),
                   timeout_sec=30,
                   backoff_sec=1)
        for p in producers:
            p.wait()

        bucket_view = BucketView(self.redpanda)

        def check_usage():
            # Check that the usage reporting system has reported correct values
            manifest_usage = bucket_view.cloud_log_sizes_sum().total(
                no_archive=True)
            # Only active leader will have latest value of cs_bytes
            reported_usages = flat_map(lambda node: self.admin.get_usage(node),
                                       self.redpanda.nodes)
            reported_usages = [
                x['cloud_storage_bytes_gauge'] for x in reported_usages
            ]

            self.logger.info(
                f"Expected {manifest_usage} bytes of cloud storage usage")
            self.logger.info(
                f"Max reported usages via kafka/usage_manager: {max(reported_usages)}"
            )
            return manifest_usage in reported_usages

        wait_until(
            check_usage,
            timeout_sec=30,
            backoff_sec=1,
            err_msg=
            "Reported cloud storage usage (via usage endpoint) did not match the manifest inferred usage"
        )
