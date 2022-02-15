# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import os
import psutil

from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster_spec import ClusterSpec

from rptest.clients.rpk import RpkTool, RpkException
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.rpk_producer import RpkProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.redpanda import ResourceSettings

# This parameter is low today, to constrain the resources required
# for the test (not just memory, but how much IO we have to do to
# fill it).  Current test infrastructure does not cope well with many
# gigabytes of writes and thousands of partitions, so we test the per-MB
# partition limits with tiny node memory instead.
NODE_MEMORY_MB = 1024

# An unreasonably large fetch request: we submit requests like this in the
# expectation that the server will properly clamp the amount of data it
# actually tries to marshal into a response.
# franz-go default maxBrokerReadBytes -- --fetch-max-bytes may not exceed this
BIG_FETCH = 104857600

# This node size is selected to be big enough to have
# many thousands of partitions, but small enough to
# execute on a typical developer workstation with 32GB
# of memory.
resource_settings = ResourceSettings(
    num_cpus=2,
    memory_mb=NODE_MEMORY_MB,
    # Test nodes and developer workstations may have slow fsync.  For this test
    # we need things to be consistently fast, so disable fsync (this test
    # has nothing to do with verifying the correctness of the storage layer)
    bypass_fsync=True)


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
                'election_timeout_ms': 4500,
                'replicate_append_timeout_ms': 9000,
                'recovery_append_timeout_ms': 15000,
            },
            resource_settings=resource_settings,
            # Usually tests run with debug or trace logs, but when testing resource
            # limits we want to test in a more production-like configuration.
            #log_level='info',  # TODO: revert to info logging once we're confident in test
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

    @cluster(num_nodes=8)
    @parametrize(n_partitions=100,
                 n_topics=1)  # 100 partitions (baseline non-stressed test)
    @parametrize(n_partitions=int(NODE_MEMORY_MB / 10),
                 n_topics=10)  # 1 partition per MB spread across 10 topics
    @parametrize(n_partitions=NODE_MEMORY_MB,
                 n_topics=1)  # 1 partition per M in one topic
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

        * Restart nodes 10 times (check that recovery works, and that the additional
          log segments created by rolling segments on restart do not cause us
          to exhaust resources.

        * Run a general produce+consume workload to check that the system remains in
          a functional state.
        """

        replication_factor = 3

        # Do a phony allocation of all the non-redpanda nodes, so that we do not
        # fail the test in CI from having asked for nodes we didn't use.
        #
        # - We requested 8 nodes with @cluster but will only use 3.
        # - We're asking for extra nodes to get a bigger share of the test
        #   runner's resources, as it overcommits CPU and memory, but this test
        #   really requires the CPUs+memory that it's asking for.
        alloc_nodes = self._ctx.cluster.alloc(ClusterSpec.simple_linux(5))
        self._ctx.cluster.free(alloc_nodes)

        # Only run on release mode builds.  Debug builds are far too slow to handle
        # large partition counts.
        self.logger.info(f"Environment: {os.environ}")
        if os.environ.get('BUILD_TYPE', None) == 'debug':
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        # Only run if the test node is large enough to accomodate us.
        # - This currently assumes the test runner is on the same physical node
        #   that will run the redpanda containers.  Will need revising when tests
        #   are improved to run across multiple hosts.
        # - Relies on our container environment telling us the host's real memory size,
        #   rather than just what's assigned to this container (which is the case with
        #   podman and docker at time of writing)
        total_memory = psutil.virtual_memory().total
        self.logger.info(f"Memory: {total_memory}")
        if total_memory < resource_settings.memory_mb * 1024 * 1024 * 3:
            if os.environ.get('CI', None) == 'false':
                self.logger.warn(
                    f"Skipping resource intensive test, running on low-memory machine with {total_memory} bytes of RAM"
                )
            else:
                raise RuntimeError(
                    f"Too little memory {total_memory} on test machine")

        disk_usage = psutil.disk_usage('/var')
        self.logger.info(f"Disk: {disk_usage}")

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
            int((resource_settings.memory_mb * 1024 * 1024) / n_topics),
            fetch_mb_per_partition * n_partitions) * 2

        msg_size = 128 * 1024
        msg_count_per_topic = int((write_bytes_per_topic / msg_size))

        # Approx time to write or read all messages, for timeouts
        # This bandwidth guess is very low to enable running on heavily contended
        # test nodes in buildkite.
        expect_bandwidth = 10 * 1024 * 1024
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

        disk_usage = psutil.disk_usage('/var')
        self.logger.info(f"Disk before restarts: {disk_usage}")

        # Higher than default timeout for node starts, because we're replaying a bunch.
        expect_start_time = 30

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

        disk_usage = psutil.disk_usage('/var')
        self.logger.info(f"Disk after restarts: {disk_usage}")

        # With increased overhead from all those segment rolls during restart,
        # check that consume still works.
        self._consume_all(topic_names, msg_count_per_topic,
                          expect_transmit_time)
