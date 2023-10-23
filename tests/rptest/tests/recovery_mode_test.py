# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import tempfile

from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.cluster import cluster
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.rpk_producer import RpkProducer
from rptest.util import wait_until_result


class RecoveryModeTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=4, **kwargs)

    def setUp(self):
        # start the nodes manually
        pass

    def _produce(self, topic):
        producer = RpkProducer(context=self.test_context,
                               redpanda=self.redpanda,
                               topic=topic,
                               msg_size=4096,
                               msg_count=1000)
        try:
            producer.run()
        finally:
            producer.free()

    @cluster(num_nodes=5)
    def test_recovery_mode(self):
        """Test that after restarting the cluster in recovery mode, produce/consume is forbidden,
        but metadata operations are still possible.
        """

        # start the 3-node cluster

        seed_nodes = self.redpanda.nodes[0:3]
        joiner_node = self.redpanda.nodes[3]
        self.redpanda.set_seed_servers(seed_nodes)

        self.redpanda.start(nodes=seed_nodes,
                            auto_assign_node_id=True,
                            omit_seeds_on_idx_one=False)

        # create a couple of topics and produce some data

        rpk = RpkTool(self.redpanda)
        rpk.create_topic("mytopic1", partitions=5, replicas=3)
        rpk.create_topic("mytopic2", partitions=5, replicas=3)

        self._produce("mytopic1")

        partitions = list(rpk.describe_topic("mytopic1", tolerant=False))
        assert len(partitions) == 5
        assert sum(p.high_watermark for p in partitions) == 1000

        # consume and create some consumer groups

        rpk.consume('mytopic1', n=500, group='mygroup1', quiet=True)
        assert rpk.group_list() == ['mygroup1']
        group1 = rpk.group_describe('mygroup1')
        assert group1.total_lag == 500
        rpk.consume('mytopic1', n=1000, group='mygroup2', quiet=True)

        # restart the cluster in recovery mode

        self.redpanda.restart_nodes(
            seed_nodes,
            auto_assign_node_id=True,
            omit_seeds_on_idx_one=False,
            override_cfg_params={"recovery_mode_enabled": True})
        self.redpanda.wait_for_membership(first_start=False)

        # check that describe, produce, consume return errors

        partitions = list(rpk.describe_topic("mytopic1", tolerant=True))
        assert len(partitions) == 5
        assert all(p.load_error is not None for p in partitions)

        try:
            rpk.produce('mytopic1', 'key', 'msg')
        except RpkException:
            pass
        else:
            assert False, "producing should fail"

        try:
            rpk.consume('mytopic1', n=1000, quiet=True, timeout=10)
            # rpk will retry indefinitely even in the presence of non-retryable errors,
            # so just wait for the timeout to occur.
        except RpkException:
            pass
        else:
            assert False, "consuming should fail"

        try:
            rpk.consume('mytopic1',
                        n=1000,
                        group='mygroup3',
                        quiet=True,
                        timeout=10)
        except RpkException:
            pass
        else:
            assert False, "group consuming should fail"

        # check consumer group ops

        assert rpk.group_list() == ['mygroup1', 'mygroup2']

        with tempfile.NamedTemporaryFile() as tf:
            # seek to the beginning of all partitions
            for i in range(5):
                tf.write(f"mytopic1 {i} 0\n".encode())
            tf.flush()
            rpk.group_seek_to_file('mygroup1', tf.name)

        group1 = rpk.group_describe('mygroup1', tolerant=True)
        assert sum(p.current_offset for p in group1.partitions) == 0

        group2 = rpk.group_describe('mygroup2', tolerant=True)
        assert sum(p.current_offset for p in group2.partitions) == 1000

        rpk.group_delete('mygroup2')
        assert rpk.group_list() == ['mygroup1']

        # check topic ops

        ## alter arbitrary topic config
        rpk.alter_topic_config('mytopic1', 'compression.type', 'snappy')
        wait_until(lambda: rpk.describe_topic_configs('mytopic1')[
            'compression.type'][0] == 'snappy',
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="failed to alter topic config")

        rpk.delete_topic('mytopic2')
        wait_until(lambda: list(rpk.list_topics()) == ['mytopic1'],
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="failed to delete topic")

        # check that a new node can join the cluster

        self.redpanda.start(nodes=[joiner_node], auto_assign_node_id=True)
        self.redpanda.wait_for_membership(first_start=True)

        # restart the cluster back in normal mode

        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    auto_assign_node_id=True)
        self.redpanda.wait_for_membership(first_start=False)

        # check that topic ops effects are still in place

        assert list(rpk.list_topics()) == ['mytopic1']
        for node in self.redpanda.storage().nodes:
            assert len(node.partitions('kafka', 'mytopic2')) == 0

        # check that altered topic config remains in place
        assert rpk.describe_topic_configs(
            'mytopic1')['compression.type'][0] == 'snappy'

        # check that produce and consume work

        self._produce("mytopic1")

        def partitions_ready():
            partitions = list(rpk.describe_topic("mytopic1", tolerant=False))
            return (len(partitions) == 5, partitions)

        partitions = wait_until_result(
            partitions_ready,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="failed to wait until partitions become ready")
        assert sum(p.high_watermark for p in partitions) == 2000

        assert rpk.group_list() == ['mygroup1']

        consumed = rpk.consume('mytopic1',
                               n=2000,
                               group='mygroup1',
                               quiet=True).rstrip().split('\n')
        # check that group seek was successful
        assert len(consumed) == 2000
