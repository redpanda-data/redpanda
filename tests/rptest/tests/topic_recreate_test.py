# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from enum import Enum
from time import sleep
from rptest.clients.default import DefaultClient
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.services.producer_swarm import ProducerSwarm
from rptest.services.redpanda import ResourceSettings

from ducktape.utils.util import wait_until
from ducktape.mark import matrix

from rptest.tests.redpanda_test import RedpandaTest


class Workload():
    ACKS_1 = 'ACKS_1'
    ACKS_ALL = 'ACKS_ALL'
    IDEMPOTENT = 'IDEMPOTENT'


class TopicRecreateTest(RedpandaTest):
    def __init__(self, test_context):
        super(TopicRecreateTest,
              self).__init__(test_context=test_context,
                             num_brokers=5,
                             resource_settings=ResourceSettings(num_cpus=1),
                             extra_rp_conf={
                                 "auto_create_topics_enabled": False,
                                 "max_compacted_log_segment_size":
                                 5 * (2 << 20)
                             })

    @cluster(num_nodes=6)
    @matrix(
        workload=[Workload.ACKS_1, Workload.ACKS_ALL, Workload.IDEMPOTENT],
        cleanup_policy=[TopicSpec.CLEANUP_COMPACT, TopicSpec.CLEANUP_DELETE])
    def test_topic_recreation_while_producing(self, workload, cleanup_policy):
        '''
        Test that we are able to recreate topic multiple times
        '''
        self._client = DefaultClient(self.redpanda)

        # scaling parameters
        partition_count = 30
        producer_count = 10

        spec = TopicSpec(partition_count=partition_count, replication_factor=3)
        spec.cleanup_policy = cleanup_policy

        self.client().create_topic(spec)

        producer_properties = {}
        if workload == Workload.ACKS_1:
            producer_properties['acks'] = 1
        elif workload == Workload.ACKS_ALL:
            producer_properties['acks'] = -1
        elif workload == Workload.IDEMPOTENT:
            producer_properties['acks'] = -1
            producer_properties['enable.idempotence'] = True
        else:
            assert False

        swarm = ProducerSwarm(self.test_context,
                              self.redpanda,
                              spec.name,
                              producer_count,
                              10000000000,
                              log_level="ERROR",
                              properties=producer_properties)
        swarm.start()

        rpk = RpkTool(self.redpanda)

        def topic_is_healthy():
            partitions = rpk.describe_topic(spec.name)
            offsets_present = [p.high_watermark > 0 for p in partitions]
            return len(offsets_present) == partition_count and all(
                offsets_present)

        for i in range(1, 20):
            rf = 3 if i % 2 == 0 else 1
            self.client().delete_topic(spec.name)
            spec.replication_factor = rf
            self.client().create_topic(spec)
            wait_until(topic_is_healthy, 30, 2)
            sleep(5)

        swarm.stop_all()
        swarm.wait()
