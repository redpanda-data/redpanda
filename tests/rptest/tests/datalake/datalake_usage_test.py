# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import os
import time
import random
import json
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.services.redpanda import RedpandaService
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings
from rptest.services.cluster import cluster
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.catalog_service_factory import supported_catalog_types, filesystem_catalog_type
from rptest.utils.functional import flat_map
from typing import Optional, Dict
from collections import namedtuple
from rptest.clients.rpk import RpkTool

TopicUsage = namedtuple("TopicUsage", [
    "begin_timestamp", "end_timestamp", "open", "revision",
    "kafka_bytes_processed", "missing_reason"
])


class IcebergUsageTest(RedpandaTest):
    """
    Test to check iceberg usage with multiple topics having iceberg enabled.
    """
    def __init__(self, test_ctx, *args, **kwargs):
        extra_rp_conf = dict(iceberg_enabled=True,
                             iceberg_catalog_commit_interval_ms=1000,
                             enable_usage=True,
                             usage_num_windows=30,
                             usage_window_width_interval_sec=1)
        self.test_ctx = test_ctx
        super(IcebergUsageTest,
              self).__init__(test_context=test_ctx,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf=extra_rp_conf,
                             *args,
                             **kwargs)

    def setUp(self):
        pass

    def usage_by_topic(self):
        """
        Get iceberg usage from the datalake /usage end point.
        """
        usage_by_topic = {}
        for node in self.redpanda.nodes:
            reported_usages = flat_map(
                lambda node: self.redpanda._admin.get_usage(node),
                self.redpanda.nodes)
            for window in reported_usages:
                dl_usage = window['datalake_usage']
                ts = window["begin_timestamp"]
                if "missing_reason" in dl_usage and dl_usage[
                        'missing_reason'] != "not_controller_leader":
                    continue
                if 'topics' not in dl_usage.keys():
                    continue
                for topic in dl_usage['topics']:
                    existing = usage_by_topic.get(topic['topic_name'], None)
                    if not existing or existing.begin_timestamp < int(
                            window['begin_timestamp']):
                        usage_by_topic[topic['topic_name']] = TopicUsage(
                            begin_timestamp=int(window['begin_timestamp']),
                            end_timestamp=int(window['end_timestamp']),
                            open=bool(window['open']),
                            revision=int(topic['topic_revision']),
                            kafka_bytes_processed=int(
                                topic['kafka_bytes_processed']),
                            missing_reason="none")

        self.logger.debug(
            f"Usage by topic: {json.dumps(usage_by_topic, indent=2)}")
        return usage_by_topic

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types(),
            query_engine=[QueryEngineType.SPARK],
            catalog_type=[filesystem_catalog_type()])
    def test_iceberg_usage(self, cloud_storage_type, query_engine,
                           catalog_type):
        """
        Test that verifies basic iceberg usage with multiple topics having iceberg enabled.
        """
        ib_topics = [f"iceberg_topic_{i}" for i in range(random.randint(1, 5))]

        message_size = random.randint(50, 100)
        total_messages = random.randint(500, 1000)
        total_bytes = total_messages * message_size

        def produce_data(iteration: int):
            for topic in ib_topics:
                dl.produce_to_topic(topic, message_size, total_messages)
                dl.wait_for_translation(topic, iteration * total_messages)

        self.logger.debug(
            f"Iceberg topics: {ib_topics}, total messages per round: {total_messages}, message size: {message_size}, total bytes: {total_bytes}"
        )

        def check_no_usage():
            """
            Check if all usages are empty or have no kafka bytes processed.
            """
            def do_check():
                usages = self.usage_by_topic()
                return len(usages) == 0

            wait_until(do_check,
                       timeout_sec=30,
                       backoff_sec=2,
                       err_msg=f"Timed out waiting for all usages to be empty")

        def check_all_usages_atleast(
                expected_topics: list[str],
                min_bytes: int,
                prev_usages: Optional[dict[str, TopicUsage]] = None):
            """
            Check if all usages have kafka bytes processed greater than or equal to min_bytes.
            """
            def do_check():
                usages = self.usage_by_topic()

                all_topics_present = all(topic in usages.keys()
                                         for topic in expected_topics)

                usages_increased = True
                if prev_usages is not None:
                    usages_increased = all([
                        (topic not in prev_usages.keys())
                        or usage.kafka_bytes_processed
                        > prev_usages[topic].kafka_bytes_processed
                        for topic, usage in usages.items()
                    ])

                return all_topics_present and usages_increased and all(
                    usage.kafka_bytes_processed >= min_bytes
                    for usage in usages.values())

            wait_until(
                do_check,
                timeout_sec=30,
                backoff_sec=2,
                err_msg=
                f"Timed out waiting for all usages to be at least {min_bytes} bytes"
            )

        def check_usage(usage: dict[str, TopicUsage]):
            def do_check():
                current_usages = self.usage_by_topic()
                if len(current_usages) != len(usage):
                    return False
                for topic, expected_usage in usage.items():
                    if topic not in current_usages:
                        return False
                    current_usage = current_usages[topic]
                    if current_usage.revision != expected_usage.revision or current_usage.kafka_bytes_processed != expected_usage.kafka_bytes_processed:
                        return False
                return True

            wait_until(do_check,
                       timeout_sec=30,
                       backoff_sec=2,
                       err_msg=f"Timed out waiting for usage to match {usage}")

        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              include_query_engines=[query_engine],
                              catalog_type=catalog_type) as dl:

            # No usage before producing messages
            check_no_usage()
            for topic in ib_topics:
                dl.create_iceberg_enabled_topic(topic,
                                                partitions=3,
                                                replicas=3,
                                                target_lag_ms=10000)
            # Produce some data and check usage.
            produce_data(iteration=1)
            check_all_usages_atleast(ib_topics, total_bytes)
            current_usages = self.usage_by_topic()
            # Produce more data and check usage again.
            produce_data(iteration=2)
            check_all_usages_atleast(ib_topics,
                                     total_bytes,
                                     prev_usages=current_usages)
            current_usages = self.usage_by_topic()
            # Restart the cluster and ensure usage is still tracked as expected.
            self.redpanda.restart_nodes(self.redpanda.nodes)
            check_usage(current_usages)

            usages = self.usage_by_topic()
            # Delete and recreate topics

            rpk = RpkTool(self.redpanda)
            for topic in ib_topics:
                rpk.delete_topic(topic)

            for topic in ib_topics:
                dl.create_iceberg_enabled_topic(topic,
                                                partitions=3,
                                                replicas=3,
                                                target_lag_ms=10000)
            produce_data(iteration=1)
            check_all_usages_atleast(ib_topics, total_bytes)
            current_usages = self.usage_by_topic()
            # Check the new revisions are strictly grreater than the previous ones.
            assert all(
                current_usages[topic].revision > usages[topic].revision
                for topic in ib_topics
            ), "Topic revisions did not increase after topic recreation"
