# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from rptest.services.cluster import cluster
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool, RpkException
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.rpk_producer import RpkProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.redpanda import ResourceSettings, RESTART_LOG_ALLOW_LIST

# An unreasonably large fetch request: we submit requests like this in the
# expectation that the server will properly clamp the amount of data it
# actually tries to marshal into a response.
# franz-go default maxBrokerReadBytes -- --fetch-max-bytes may not exceed this
BIG_FETCH = 104857600

# The maximum partition count that's stable on a three node redpanda cluster
# with replicas=3.
# Don't go above this partition count.  This limit reflects
# issues (at time of writing in redpanda 22.1) around disk contention on node startup,
# where ducktape's startup threshold is violated by the time it takes systems with
# more partitions to replay recent content on startup.
HARD_PARTITION_LIMIT = 10000


class ManyPartitionsTest(RedpandaTest):
    """
    Validates basic functionality in the presence of larger numbers
    of partitions than most other tests.
    """
    topics = ()

    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        super(ManyPartitionsTest, self).__init__(
            test_ctx,
            *args,
            num_brokers=6,
            extra_rp_conf={
                # It's not ideal that the user would have to specify these tuning
                # properties to run with many partitions, but it is the current situation.
                'storage_read_buffer_size': 32768,
                'storage_read_readahead_count': 2,

                # We will later try to validate that leadership was stable during under load,
                # so do not want the leader balancer to interfere
                'enable_leader_balancer': False,

                # Increasing raft timeouts to 3x their
                # defaults.  Avoids spurious leader elections
                # on weaker test nodes.
                'raft_heartbeat_interval_ms': 450,
                'raft_heartbeat_timeout_ms': 9000,
                'election_timeout_ms': 7500,
                'replicate_append_timeout_ms': 9000,
                'recovery_append_timeout_ms': 15000,
            },
            # Usually tests run with debug or trace logs, but when testing resource
            # limits we want to test in a more production-like configuration.
            log_level='info',
            **kwargs)
        self.rpk = RpkTool(self.redpanda)

    def _all_elections_done(self, topic_names: list[str], p_per_topic: int):
        any_incomplete = False
        for tn in topic_names:
            partitions = list(self.rpk.describe_topic(tn))
            if len(partitions) < p_per_topic:
                self.logger.info(f"describe omits partitions for topic {tn}")
                any_incomplete = True
                continue

            assert len(partitions) == p_per_topic
            for p in partitions:
                if p.leader == -1:
                    self.logger.info(
                        f"partition {tn}/{p.id} has no leader yet")
                    any_incomplete = True

        return not any_incomplete

    def _consume_all(self, topic_names: list[str], msg_count_per_topic: int,
                     timeout_per_topic: int):
        """
        Don't do anything with the messages, just consume them to demonstrate
        that doing so does not exhaust redpanda resources.
        """
        def consumer_saw_msgs(consumer):
            self.logger.info(
                f"Consumer message_count={consumer.message_count} / {msg_count_per_topic}"
            )
            # Tolerate greater-than, because if there were errors during production
            # there can have been retries.
            return consumer.message_count >= msg_count_per_topic

        for tn in topic_names:
            consumer = RpkConsumer(self._ctx,
                                   self.redpanda,
                                   tn,
                                   save_msgs=False,
                                   fetch_max_bytes=BIG_FETCH,
                                   num_msgs=msg_count_per_topic)
            consumer.start()
            wait_until(lambda: consumer_saw_msgs(consumer),
                       timeout_sec=timeout_per_topic,
                       backoff_sec=5)
            consumer.stop()
            consumer.free()

    def setUp(self):
        # defer redpanda startup to the test, it might want to tweak
        # ResourceSettings based on its parameters.
        pass

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(n_topics=1)  # All in one topic
    @parametrize(n_topics=10)  # Split up between several topics
    def test_many_partitions(self, n_partitions: int, n_topics: int):
        """
        Validate that redpanda works with partition counts close to its resource
        limits.

        This test should evolve over time as we improve efficiency and can reliably
        run with higher partition counts.  It should roughly track the values we
        use for topic_memory_per_partition and topic_fds_per_partition.

        * Check topic can be created.
        * Check leadership election succeeds for all partitions.
        * Write in enough data such that an unlimited size fetch
          would exhaust ram (check enforcement of kafka_max_bytes_per_fetch).
        * Consume all the data from the topic

        * Restart nodes several times (check that recovery works, and that the additional
          log segments created by rolling segments on restart do not cause us
          to exhaust resources.

        * Run a general produce+consume workload to check that the system remains in
          a functional state.
        """

        # This test requires dedicated system resources to run reliably.
        assert self.redpanda.dedicated_nodes

        # Scale tests are not run on debug builds
        assert not self.debug_mode

        replication_factor = 3
        node_count = len(self.redpanda.nodes)

        # If we run on nodes with more memory than our HARD_PARTITION_LIMIT, then
        # artificially throttle the nodes' memory to avoid the test being too easy.
        # We are validating that the system works up to the limit, and that it works
        # up to the limit within the default per-partition memory footprint.
        node_memory = self.redpanda.get_node_memory_mb()

        # HARD_PARTITION_LIMIT is for a 3 node cluster, adjust according to
        # the number of nodes in this cluster.
        partition_limit = HARD_PARTITION_LIMIT * (node_count / 3)

        mb_per_partition = 1

        # How much memory to reserve for internal partitions, such as
        # id_allocator.  This is intentionally higher than needed, to
        # avoid having to update this test each time a new internal topic
        # is added.
        internal_partition_slack = 10

        # Clamp memory if nodes have more memory than should be required
        # to exercise the partition limit.
        if node_memory > HARD_PARTITION_LIMIT / mb_per_partition:
            resource_settings = ResourceSettings(
                memory_mb=mb_per_partition *
                (HARD_PARTITION_LIMIT + internal_partition_slack))
            self.redpanda.set_resource_settings(resource_settings)
        elif node_memory < HARD_PARTITION_LIMIT / mb_per_partition:
            raise RuntimeError(f"Node memory is too small ({node_memory}MB)")

        # Partitions per topic
        n_partitions = partition_limit / n_topics

        self.logger.info(
            f"Running partition scale test with {n_partitions} partitions on {n_topics} topics"
        )

        self.redpanda.start()

        topic_names = [f"scale_{i:06d}" for i in range(0, n_topics)]
        for tn in topic_names:
            self.logger.info(
                f"Creating topic {tn} with {n_partitions} partitions")
            self.rpk.create_topic(tn,
                                  partitions=n_partitions,
                                  replicas=replication_factor)

        self.logger.info(f"Awaiting elections...")
        wait_until(lambda: self._all_elections_done(topic_names, n_partitions),
                   timeout_sec=60,
                   backoff_sec=5)
        self.logger.info(f"Initial elections done.")

        for node in self.redpanda.nodes:
            files = self.redpanda.lsof_node(node)
            file_count = sum(1 for _ in files)
            self.logger.info(
                f"Open files after initial selection on {node.name}: {file_count}"
            )

        # Assume fetches will be 10MB, the franz-go default
        fetch_mb_per_partition = 10 * 1024 * 1024

        # * Need enough data that if a consumer tried to fetch it all at once
        # in a single request, it would run out of memory.  OR the amount of
        # data that would fill a 10MB max_bytes per partition in a fetch, whichever
        # is lower (avoid writing excessive data for tests with fewer partitions).
        # * Then apply a factor of two to make sure we have enough data to drive writes
        # to disk during consumption, not just enough data to hold it all in the batch
        # cache.
        write_bytes_per_topic = min(
            int((self.redpanda.get_node_memory_mb() * 1024 * 1024) / n_topics),
            fetch_mb_per_partition * n_partitions) * 2

        if self.scale.release:
            # Release tests can be much longer running: 10x the amount of
            # data we fire through the system
            write_bytes_per_topic *= 10

        msg_size = 128 * 1024
        msg_count_per_topic = int((write_bytes_per_topic / msg_size))

        # Approx time to write or read all messages, for timeouts
        # Pessimistic bandwidth guess, based on smallest i3en instance type.
        expect_bandwidth = 100 * 1024 * 1024

        expect_transmit_time = int(write_bytes_per_topic / expect_bandwidth)

        for tn in topic_names:
            producer = RpkProducer(
                self._ctx,
                self.redpanda,
                tn,
                msg_size=msg_size,
                msg_count=msg_count_per_topic,
                # Need acks=all, because otherwise leadership changes during the production
                # can lead to truncation and dropping messages, and we later assert that we
                # see all the messages we sent.
                # This can be changed to acks=1 in future if we can maintain stable leadership
                # through the test and add an assertion to that effect.
                acks=-1,
                quiet=True,
                # Factor of two to allow for general timing noise
                produce_timeout=expect_transmit_time * 2)
            t1 = time.time()
            producer.start()
            producer.wait(timeout_sec=expect_transmit_time)
            duration = time.time() - t1
            self.logger.info(
                f"Wrote {write_bytes_per_topic} bytes to {tn} in {duration}s, bandwidth {(write_bytes_per_topic / duration)/(1024 * 1024)}MB/s"
            )
            producer.free()

        self._consume_all(topic_names, msg_count_per_topic,
                          expect_transmit_time)

        for node in self.redpanda.nodes:
            files = self.redpanda.lsof_node(node)
            file_count = sum(1 for _ in files)
            self.logger.info(
                f"Open files before restarts on {node.name}: {file_count}")

        # Over large partition counts, the startup time is linear with the
        # amount of data we played in, because no one partition gets far
        # enough to snapshot.
        expect_start_time = expect_transmit_time

        # We know that after many restarts, the file handle count grows in a bad way.  Just
        # do a few restarts, so that we are confident that the replay memory footprint from
        # input buffers is not going to cause bad_allocs
        restart_count = 3

        for i in range(1, restart_count + 1):
            self.logger.info(f"Cluster restart {i}/{restart_count}")
            self.redpanda.restart_nodes(self.redpanda.nodes,
                                        start_timeout=expect_start_time)

            self.logger.info(f"Awaiting post-restart elections...")
            wait_until(
                lambda: self._all_elections_done(topic_names, n_partitions),
                timeout_sec=60,
                backoff_sec=5)
            self.logger.info(f"Post-restart elections done.")

            for node in self.redpanda.nodes:
                files = self.redpanda.lsof_node(node)
                file_count = sum(1 for _ in files)
                self.logger.info(
                    f"Open files after {i} restarts on {node.name}: {file_count}"
                )

        # With increased overhead from all those segment rolls during restart,
        # check that consume still works.
        self._consume_all(topic_names, msg_count_per_topic,
                          expect_transmit_time)
