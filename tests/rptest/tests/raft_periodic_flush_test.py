# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import wait_until


class PeriodicFlushWithRelaxedConsistencyTest(EndToEndTest):
    @cluster(num_nodes=5)
    def test_changing_periodic_flush_threshold(self):

        self.start_redpanda(num_nodes=3)

        # create topic with single partition
        spec = TopicSpec(partition_count=1, replication_factor=3)
        self.client().create_topic(spec)

        self.topic = spec.name

        rpk = RpkTool(self.redpanda)
        # Disable background flushing
        rpk.alter_topic_config(self.topic, "flush.bytes", 10 * (1024 * 1024))
        rpk.alter_topic_config(self.topic, "flush.ms", 60 * 60 * 1000)

        self.start_producer(1, throughput=1000, acks=1)
        self.start_consumer()

        msg_count = 10000
        wait_until(
            lambda: len(self.producer.acked_values) >= msg_count,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"Producer didn't end producing {msg_count} messages")

        # wait for at least 15000 records to be consumed
        self.producer.stop()
        self.run_validation(min_records=msg_count,
                            producer_timeout_sec=300,
                            consumer_timeout_sec=300)

        admin = Admin(self.redpanda)
        p_state = admin.get_partition_state(namespace='kafka',
                                            topic=self.topic,
                                            partition=0)
        self.logger.info(f"initial partition state: {p_state}")
        assert all([
            r['committed_offset'] < r['dirty_offset']
            for r in p_state['replicas']
        ]), "With ACKS=1, committed offset should not be advanced immediately"

        # Enable timer based flush
        rpk.alter_topic_config(self.topic, "flush.ms", 100)

        def committed_offset_advanced():
            p_state = admin.get_partition_state(namespace='kafka',
                                                topic=self.topic,
                                                partition=0)
            return all([
                r['committed_offset'] == r['dirty_offset']
                for r in p_state['replicas']
            ])

        wait_until(
            committed_offset_advanced, 30, 1,
            "committed offset did not advance after the change of max flushed bytes"
        )
