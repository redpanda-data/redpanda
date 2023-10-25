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
import json
from functools import partial, reduce
from typing import Optional
from rptest.services.rpk_consumer import RpkConsumer
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster import ClusterNode
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool
from rptest.tests.cluster_config_test import wait_for_version_sync
from rptest.util import wait_until_result
from rptest.utils.audit_schemas import validate_audit_schema
from rptest.services.redpanda import LoggingConfig, MetricSamples, MetricsEndpoint


class AuditLogTests(RedpandaTest):
    audit_log = "__audit_log"

    def __init__(self, test_context):
        self._extra_conf = {
            'audit_enabled': True,
            'audit_log_num_partitions': 8,
            'audit_enabled_event_types': ['management'],
        }
        super(AuditLogTests,
              self).__init__(test_context=test_context,
                             extra_rp_conf=self._extra_conf,
                             log_config=LoggingConfig('info',
                                                      logger_levels={
                                                          'auditing':
                                                          'trace',
                                                          'admin_api_server':
                                                          'trace'
                                                      }))
        self._ctx = test_context
        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    def _wait_for_audit_log(self, timeout_sec):
        wait_until(lambda: self.audit_log in self.rpk.list_topics(),
                   timeout_sec=timeout_sec,
                   backoff_sec=2)

    def _audit_log_total_number_records(self):
        audit_partitions = self.rpk.describe_topic(self.audit_log)
        partition_totals = [
            partition.high_watermark for partition in audit_partitions
        ]
        assert len(partition_totals) == self._extra_conf[
            'audit_log_num_partitions'], \
                f"Expected: {self._extra_conf['audit_log_num_partitions']}, Result: {len(partition_totals)}"
        return sum(partition_totals)

    def _read_all_from_audit_log(self,
                                 filter_fn,
                                 stop_cond,
                                 timeout_sec=30,
                                 backoff_sec=1):
        class MessageMapper():
            def __init__(self, logger, filter_fn, stop_cond):
                self.logger = logger
                self.records = []
                self.filter_fn = filter_fn
                self.stop_cond = stop_cond
                self.next_offset_ingest = 0

            def ingest(self, records):
                new_records = records[self.next_offset_ingest:]
                self.next_offset_ingest = len(records)
                new_records = [json.loads(msg['value']) for msg in new_records]
                self.logger.info(f"Ingested: {len(new_records)} records")
                self.records += [r for r in new_records if self.filter_fn(r)]

            def is_finished(self):
                return stop_cond(self.records)

        mapper = MessageMapper(self.redpanda.logger, filter_fn, stop_cond)
        n = self._audit_log_total_number_records()

        consumer = RpkConsumer(self._ctx, self.redpanda, self.audit_log)
        consumer.start()

        def predicate():
            mapper.ingest(consumer.messages)
            return consumer.message_count >= n and mapper.is_finished()

        wait_until(predicate, timeout_sec=timeout_sec, backoff_sec=backoff_sec)
        consumer.stop()
        consumer.free()
        return mapper.records

    @cluster(num_nodes=4)
    def test_audit_log_functioning(self):
        """
        Ensures that the audit log can be produced to when the audit_enabled()
        configuration option is set, and that the same actions do nothing
        when the option is unset. Furthermore verifies that the internal duplicate
        aggregation feature is working.
        """
        def is_api_match(matches, record):
            self.logger.debug(f'{record}')
            if record['class_uid'] == 6003:
                regex = re.compile(
                    "http:\/\/(?P<address>.*):(?P<port>\d+)\/v1\/(?P<handler>.*)"
                )
                match = regex.match(
                        record['http_request']['url']['url_string'])
                if match is None:
                    raise RuntimeError(f'Record out of spec: {record}')
                return match.group('handler') in matches
            else:
                return False

        def number_of_records_matching(filter_by, n_expected):
            filter_fn = partial(is_api_match, filter_by)

            def aggregate_count(records):
                # Duplicate records are combined with the 'count' field
                def combine(acc, x):
                    return acc + (1 if 'count' not in x else x['count'])

                return reduce(combine, records, 0)

            stop_cond = lambda records: aggregate_count(records) >= n_expected
            records = self._read_all_from_audit_log(filter_fn, stop_cond)
            assert aggregate_count(
                records
            ) == n_expected, f"Expected: {n_expected}, Actual: {aggregate_count(records)}"
            return records

        self._wait_for_audit_log(timeout_sec=10)

        # The test override the default event type to 'heartbeat', therefore
        # any actions on the admin server should not result in audit msgs
        api_calls = {
            'features/license': self.admin.get_license,
            'cluster/health_overview': self.admin.get_cluster_health_overview
        }
        api_keys = api_calls.keys()
        call_apis = lambda: [fn() for fn in api_calls.values()]
        self.logger.debug("Starting 500 api calls with management enabled")
        for _ in range(0, 500):
            call_apis()
        self.logger.debug("Finished 500 api calls with management enabled")

        # Wait 1 second because asserting that 0 records of a type occur
        # will not continuously loop for the records we are expected not to
        # see. 1 second is long enough for the audit fibers to run and push
        # data to the audit topic.
        time.sleep(1)
        records = number_of_records_matching(api_keys, 1000)
        self.redpanda.logger.debug(f"records: {records}")

        # Raises if validation fails on any records
        [validate_audit_schema(record) for record in records]

        # Remove management setting
        patch_result = self.admin.patch_cluster_config(
            upsert={'audit_enabled_event_types': ['heartbeat']})
        wait_for_version_sync(self.admin, self.redpanda,
                              patch_result['config_version'])

        self.logger.debug("Started 500 api calls with management disabled")
        for _ in range(0, 500):
            call_apis()
        self.logger.debug("Finished 500 api calls with management disabled")
        _ = number_of_records_matching(api_keys, 1000)

    @cluster(num_nodes=3)
    def test_audit_log_metrics(self):
        """
        Confirm that audit log metrics are present
        """
        def get_metrics_from_node(
            node: ClusterNode,
            patterns: list[str],
            endpoint: MetricsEndpoint = MetricsEndpoint.METRICS
        ) -> Optional[dict[str, MetricSamples]]:
            def get_metrics_from_node_sync(patterns: list[str]):
                samples = self.redpanda.metrics_samples(
                    patterns, [node], endpoint)
                success = samples is not None
                return success, samples

            try:
                return wait_until_result(
                    lambda: get_metrics_from_node_sync(patterns),
                    timeout_sec=2,
                    backoff_sec=.1)
            except TimeoutError as e:
                return None

        public_metrics = [
            "audit_last_event",
            "audit_errors_total",
        ]
        metrics = public_metrics + [
            "audit_buffer_usage_ratio",
            "audit_client_buffer_usage_ratio",
        ]

        self._wait_for_audit_log(timeout_sec=10)

        for node in self.redpanda.nodes:
            samples = get_metrics_from_node(node, metrics)
            assert samples, f"Missing expected metrics from node {node.name}"
            assert sorted(samples.keys()) == sorted(
                metrics), f"Metrics incomplete: {samples.keys()}"

        for node in self.redpanda.nodes:
            samples = get_metrics_from_node(node, metrics,
                                            MetricsEndpoint.PUBLIC_METRICS)
            assert samples, f"Missing expected public metrics from node {node.name}"
            assert sorted(samples.keys()) == sorted(
                public_metrics), f"Public metrics incomplete: {samples.keys()}"

        # Remove management setting
        patch_result = self.admin.patch_cluster_config(
            upsert={'audit_enabled_event_types': ['heartbeat']})
        wait_for_version_sync(self.admin, self.redpanda,
                              patch_result['config_version'])
