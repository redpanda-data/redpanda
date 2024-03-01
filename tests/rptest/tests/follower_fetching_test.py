# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import string
import time
from ducktape.mark import matrix
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec

from rptest.services.cluster import cluster
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import SISettings
from rptest.services.admin import Admin
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.util import wait_for_local_storage_truncate, wait_until_result

from ducktape.utils.util import wait_until

from rptest.utils.mode_checks import skip_debug_mode


class FollowerFetchingTest(PreallocNodesTest):
    def __init__(self, test_context):
        self.log_segment_size = 1024 * 1024
        self.local_retention = 2 * self.log_segment_size
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=5,
            log_segment_size=self.log_segment_size,
            cloud_storage_enable_remote_read=True,
            cloud_storage_enable_remote_write=True,
        )
        self.s3_bucket_name = si_settings.cloud_storage_bucket

        super(FollowerFetchingTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            node_prealloc_count=1,
            extra_rp_conf={
                'enable_rack_awareness': True,
                # disable leader balancer to prevent leaders from moving and causing additional client retries
                'enable_leader_balancer': False
            },
            si_settings=si_settings)

    def setUp(self):
        # Delay startup, so that the test case can configure redpanda
        # based on test parameters before starting it.
        pass

    def produce(self, topic, bytes=10 * 1024 * 1024):
        msg_size = 512
        msg_cnt = int(bytes / msg_size)
        producer = KgoVerifierProducer(self.test_context, self.redpanda, topic,
                                       msg_size, msg_cnt,
                                       self.preallocated_nodes)
        producer.start()
        producer.wait()
        producer.free()

    def get_node_metric(self, node, topic, metric):
        return self.redpanda.metric_sum(namespace="kafka",
                                        nodes=[node],
                                        topic=topic,
                                        metric_name=metric)

    def get_fetch_bytes(self, node, topic):
        return self.get_node_metric(
            node, topic, "vectorized_cluster_partition_bytes_fetched_total")

    def get_follower_fetched_bytes(self, node, topic):
        return self.get_node_metric(
            node, topic,
            "vectorized_cluster_partition_bytes_fetched_from_follower_total")

    def create_consumer(self, topic, rack=None):

        properties = {}
        if rack:
            properties['client.rack'] = rack
        return KafkaCliConsumer(
            self.test_context,
            self.redpanda,
            topic=topic,
            group=
            f'test-gr-{"".join(random.choice(string.ascii_lowercase) for _ in range(8))}',
            from_beginning=True,
            consumer_properties=properties,
            formatter_properties={
                'print.value': 'false',
                'print.key': 'false',
                'print.partition': 'true',
                'print.offset': 'true',
            },
        )

    def _metrics_per_node(self, topic, metric):
        per_node = {}
        for n in self.redpanda.nodes:
            per_node[n] = self.get_node_metric(n, topic, metric=metric)
            self.logger.info(
                f"{metric}={per_node[n]} at {n.account.hostname}:{self.redpanda.node_id(n)}"
            )
        return per_node

    def _bytes_fetched_per_node(self, topic):
        return self._metrics_per_node(
            topic, "vectorized_cluster_partition_bytes_fetched_total")

    def _follower_bytes_fetched_per_node(self, topic):
        return self._metrics_per_node(
            topic,
            "vectorized_cluster_partition_bytes_fetched_from_follower_total")

    @cluster(num_nodes=5)
    @matrix(read_from_object_store=[True, False])
    def test_basic_follower_fetching(self, read_from_object_store):
        rack_layout_str = "ABC"
        rack_layout = [str(i) for i in rack_layout_str]

        for ix, node in enumerate(self.redpanda.nodes):
            extra_node_conf = {
                # We're introducing two racks, small and large.
                # The small rack has only one node and the
                # large one has four nodes.
                'rack': rack_layout[ix],
                # This parameter enables rack awareness
                'enable_rack_awareness': True,
            }
            self.redpanda.set_extra_node_conf(node, extra_node_conf)

        self.redpanda.start()
        topic = TopicSpec(partition_count=1, replication_factor=3)

        self.client().create_topic(topic)

        self.produce(topic.name)
        self.logger.info(f"Producing to {topic.name} finished")
        if read_from_object_store:
            RpkTool(self.redpanda).alter_topic_config(
                topic.name,
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
                self.local_retention,
            )
            wait_for_local_storage_truncate(self.redpanda,
                                            topic.name,
                                            target_bytes=self.local_retention)
        number_of_samples = 10
        for n in range(0, number_of_samples):
            node_idx = random.randint(0, 2)
            consumer_rack = rack_layout_str[node_idx]
            self.logger.info(
                f"Using consumer with {consumer_rack} in {n+1}/{number_of_samples} sample"
            )
            fetched_per_node_before = self._bytes_fetched_per_node(topic.name)
            f_fetched_before = self._follower_bytes_fetched_per_node(
                topic.name)
            consumer = self.create_consumer(topic.name, rack=consumer_rack)
            consumer.start()
            consumer.wait_for_messages(1000)
            consumer.stop()
            consumer.wait()
            consumer.clean()
            consumer.free()

            fetched_per_node_after = self._bytes_fetched_per_node(topic.name)
            f_fetched_after = self._follower_bytes_fetched_per_node(topic.name)
            preferred_replica = self.redpanda.nodes[node_idx]
            self.logger.info(
                f"preferred replica {preferred_replica.account.hostname}:{self.redpanda.node_id(preferred_replica)} in rack {consumer_rack}"
            )

            for n, new_fetched_bytes in fetched_per_node_after.items():
                current_bytes_fetched = new_fetched_bytes - fetched_per_node_before[
                    n]
                if n == preferred_replica:
                    assert current_bytes_fetched > 0
                else:
                    assert current_bytes_fetched == 0

            for n, new_fetched_bytes in f_fetched_after.items():
                follower_fetched = new_fetched_bytes - f_fetched_before[n]
                if n == preferred_replica:
                    # follower fetched bytes may be equal to 0 if preferred replica is a leader
                    assert follower_fetched >= 0
                else:
                    assert follower_fetched == 0

    @cluster(num_nodes=5)
    def test_with_leadership_transfers(self):
        """
        Test consuming from a single node while leadership is randomly transfered.
        """

        rack_layout = [str(i) for i in "ABC"]
        for ix, node in enumerate(self.redpanda.nodes):
            extra_node_conf = {
                'rack': rack_layout[ix],
                'enable_rack_awareness': True,
            }
            self.redpanda.set_extra_node_conf(node, extra_node_conf)
        self.redpanda.start()

        topic = TopicSpec(name="mytopic",
                          partition_count=1,
                          replication_factor=3)
        self.client().create_topic(topic)

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=512,
            # some large number to get produce load till the end of test
            msg_count=2**30,
            rate_limit_bps=2**20)
        producer.start()

        # consume from the same rack as node 0
        consumer = self.create_consumer(topic.name, rack=rack_layout[0])
        consumer.start()

        admin = Admin(self.redpanda)
        prev_consumed = 0
        for _ in range(10):
            # do a random leadership transfer
            admin.partition_transfer_leadership("kafka",
                                                topic.name,
                                                partition=0)

            # ensure continuous consumer progress
            consumer.wait_for_messages(prev_consumed + 1000)
            prev_consumed = consumer.message_cnt()
            self.logger.info(f"consumed {prev_consumed} msgs")

        consumer.stop()
        producer.stop()

        rpk = RpkTool(self.redpanda)

        def get_hwm():
            partitions = list(rpk.describe_topic(topic.name))
            if len(partitions) > 0:
                return True, partitions[0].high_watermark
            else:
                return False

        hwm = wait_until_result(get_hwm,
                                timeout_sec=30,
                                backoff_sec=1,
                                err_msg="couldn't get high watermark")

        # check that there were no consumer group resets caused by offset_out_of_range error
        self.logger.info(
            f"produced {hwm}, consumed {consumer.message_cnt()} msgs")
        assert consumer.message_cnt() <= hwm

    @cluster(num_nodes=5)
    def test_follower_fetching_with_maintenance_mode(self):
        rack_layout_str = "ABC"
        rack_layout = [str(i) for i in rack_layout_str]

        for ix, node in enumerate(self.redpanda.nodes):
            extra_node_conf = {
                # We're introducing two racks, small and large.
                # The small rack has only one node and the
                # large one has four nodes.
                'rack': rack_layout[ix],
                # This parameter enables rack awareness
                'enable_rack_awareness': True,
            }
            self.redpanda.set_extra_node_conf(node, extra_node_conf)

        self.redpanda.start()
        topic = TopicSpec(partition_count=1, replication_factor=3)

        self.client().create_topic(topic)

        self.produce(topic.name)
        self.logger.info(f"Producing to {topic.name} finished")

        number_of_samples = 10
        enable_maintenance_mode = True
        rpk = RpkTool(self.redpanda)
        for n in range(0, number_of_samples):
            node_idx = random.randint(0, 2)
            consumer_rack = rack_layout_str[node_idx]
            self.logger.info(
                f"Using consumer with {consumer_rack} in {n+1}/{number_of_samples} sample"
            )
            preferred_replica = self.redpanda.nodes[node_idx]
            self.logger.info(
                f"preferred replica {preferred_replica.account.hostname}:{self.redpanda.node_id(preferred_replica)} in rack {consumer_rack}"
            )
            if enable_maintenance_mode:
                rpk.cluster_maintenance_enable(
                    self.redpanda.node_id(preferred_replica), wait=True)

            fetched_per_node_before = self._bytes_fetched_per_node(topic.name)
            consumer = self.create_consumer(topic.name, rack=consumer_rack)
            consumer.start()
            consumer.wait_for_messages(1000)
            consumer.stop()
            consumer.wait()
            consumer.clean()
            consumer.free()

            fetched_per_node_after = self._bytes_fetched_per_node(topic.name)

            for n, new_fetched_bytes in fetched_per_node_after.items():
                current_bytes_fetched = new_fetched_bytes - fetched_per_node_before[
                    n]

                if enable_maintenance_mode:
                    if n == preferred_replica:
                        assert current_bytes_fetched == 0
                else:
                    if n == preferred_replica:
                        assert current_bytes_fetched > 0
                    else:
                        assert current_bytes_fetched == 0
            if enable_maintenance_mode:
                rpk.cluster_maintenance_disable(
                    self.redpanda.node_id(preferred_replica))

            enable_maintenance_mode = not enable_maintenance_mode


class IncrementalFollowerFetchingTest(PreallocNodesTest):
    def __init__(self, test_context):
        super(IncrementalFollowerFetchingTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             node_prealloc_count=1,
                             extra_rp_conf={
                                 'enable_rack_awareness': True,
                             })

    @skip_debug_mode
    @cluster(num_nodes=5)
    @matrix(follower_offline=[True, False])
    def test_incremental_fetch_from_follower(self, follower_offline):
        rack_layout_str = "ABC"
        rack_layout = [str(i) for i in rack_layout_str]

        for ix, node in enumerate(self.redpanda.nodes):
            extra_node_conf = {
                'rack': rack_layout[ix],
                'enable_rack_awareness': True,
            }
            self.redpanda.set_extra_node_conf(node, extra_node_conf)

        self.redpanda.start()
        topic = TopicSpec(partition_count=12, replication_factor=3)

        self.client().create_topic(topic)
        msg_size = 512

        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic,
                                       msg_size,
                                       1000000,
                                       self.preallocated_nodes,
                                       rate_limit_bps=256 * 1024)

        producer.start()
        consumer_group = "kafka-cli-group"
        rack = "A"

        # We are using kafka cli consumer to control metadata age and client rack id consumer properties
        cli_consumer = KafkaCliConsumer(self.test_context,
                                        self.redpanda,
                                        topic.name,
                                        group="kafka-cli-group",
                                        consumer_properties={
                                            "client.rack": rack,
                                            "metadata.max.age.ms": 10000
                                        })
        cli_consumer.start()
        cli_consumer.wait_for_messages(100)
        if follower_offline:
            idx = rack_layout_str.find(rack)
            self.redpanda.stop_node(self.redpanda.get_node(idx))

        # sleep long enough to cause metadata refresh on the consumer
        time.sleep(30)
        # stop the producer
        producer.stop()
        rpk = RpkTool(self.redpanda)

        def no_lag():
            gr = rpk.group_describe(consumer_group)
            if gr.state != "Stable":
                return False

            return all([p.lag == 0 for p in gr.partitions])

        # wait for consumer to clear the backlog
        wait_until(
            no_lag,
            60,
            backoff_sec=3,
            err_msg=
            "Consumer did not finish consuming topic. Lag exists on some of the partitions"
        )

        cli_consumer.stop()
