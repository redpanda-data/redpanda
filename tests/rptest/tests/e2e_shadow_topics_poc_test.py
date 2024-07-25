# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import random
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from requests.exceptions import HTTPError

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkException
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.action_injector import random_process_kills
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer, \
    KgoVerifierRandomConsumer, KgoVerifierSeqConsumer
from rptest.services.metrics_check import MetricCheck
from rptest.services.redpanda import SISettings, get_cloud_storage_type, make_redpanda_service, CHAOS_LOG_ALLOW_LIST, \
    MetricsEndpoint
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import Scale, wait_until_segments
from rptest.util import (
    produce_until_segments,
    wait_for_removal_of_n_segments,
    wait_for_local_storage_truncate,
)
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.si_utils import nodes_report_cloud_segments, BucketView, NTP, quiesce_uploads


class EndToEndShadowTopicsBase(EndToEndTest):
    segment_size = 1048576  # 1 Mb
    chunk_size = segment_size * 0.75
    s3_topic_name = "panda_topic_st"

    num_brokers = 3

    topics = (TopicSpec(
        name=s3_topic_name,
        partition_count=1,
        replication_factor=3,
    ), )

    def __init__(self, test_context, extra_rp_conf=None, environment=None):
        super(EndToEndShadowTopicsBase,
              self).__init__(test_context=test_context)

        if environment is None:
            environment = {'__REDPANDA_TOPIC_REC_DL_CHECK_MILLIS': 5000}
        self.test_context = test_context
        self.topic = self.s3_topic_name

        conf = dict(
            enable_cluster_metadata_upload_loop=False,
            cloud_storage_cluster_metadata_upload_interval_ms=1000,
            # Tests may configure spillover manually.
            cloud_storage_spillover_manifest_size=None,
            controller_snapshot_max_age_sec=1)

        if extra_rp_conf:
            for k, v in conf.items():
                extra_rp_conf[k] = v
        else:
            extra_rp_conf = conf

        extra_rp_conf['segment_fallocation_step'] = 0x1000

        self.si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=5,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            log_segment_size=self.segment_size,  # 1MB
            fast_uploads=True,
        )
        self.s3_bucket_name = self.si_settings.cloud_storage_bucket
        self.si_settings.load_context(self.logger, test_context)
        self.scale = Scale(test_context)

        self.redpanda = make_redpanda_service(context=self.test_context,
                                              num_brokers=self.num_brokers,
                                              si_settings=self.si_settings,
                                              extra_rp_conf=extra_rp_conf,
                                              environment=environment)
        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    def setUp(self):
        assert self.redpanda
        self.redpanda.start()
        for topic in self.topics:
            self.kafka_tools.create_topic(topic)


class EndToEndShadowTopicsTest(EndToEndShadowTopicsBase):
    def __init__(self, test_context, extra_rp_conf=None, env=None):
        super(EndToEndShadowTopicsTest, self).__init__(test_context,
                                                       extra_rp_conf, env)

    def await_num_produced(self, min_records, timeout_sec=30):
        wait_until(lambda: self.producer.num_acked > min_records,
                   timeout_sec=timeout_sec,
                   err_msg="Producer failed to produce messages for %ds." %\
                   timeout_sec)

    @cluster(num_nodes=5)
    def test_write(self):

        self.start_producer()

        for p in range(1000, 10000, 1000):
            self.await_num_produced(min_records=p)
            time.sleep(1.0)

        time.sleep(5.0)

        self.start_consumer()
        self.run_validation()

        full_local_size = 0
        stats = self.redpanda.data_stat(self.redpanda.nodes[0])
        for path, size in stats:
            self.logger.info(f"File stats {path} {size}")
            if "kafka/panda_topic_st" in str(path):
                full_local_size += size
        self.logger.info(f"Total size on node0 {full_local_size}")

        client = self.redpanda.cloud_storage_client
        objects = list(client.list_objects(self.s3_bucket_name))
        full_size = 0
        for o in objects:
            full_size += o.content_length
            self.logger.info(f"Cloud bucket has object {o}")
        self.logger.info(f"Total size in the cloud {full_size}")
        assert len(objects) > 0
