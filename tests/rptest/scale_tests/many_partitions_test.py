# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import concurrent.futures

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool, RpkException
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.redpanda import ResourceSettings, RESTART_LOG_ALLOW_LIST
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableProducer, FranzGoVerifiableSeqConsumer, FranzGoVerifiableRandomConsumer

# An unreasonably large fetch request: we submit requests like this in the
# expectation that the server will properly clamp the amount of data it
# actually tries to marshal into a response.
# franz-go default maxBrokerReadBytes -- --fetch-max-bytes may not exceed this
BIG_FETCH = 104857600

# The maximum total number of partitions in a Redpanda cluster.  No matter
# how big the nodes or how many of them, the test does not exceed this.
# This limit represents bottlenecks in central controller/health
# functions.
HARD_PARTITION_LIMIT = 100000

# How many partitions we will create per shard: this value enables
# reaching HARD_PARTITION_LIMIT on 6x i3en.6xlarge nodes.
PARTITIONS_PER_SHARD = 2200

# Number of partitions to create when running in docker (i.e.
# when dedicated_nodes=false).  This is independent of the
# amount of RAM or CPU that the nodes claim to have, because
# we know they are liable to be oversubscribed.
DOCKER_PARTITION_LIMIT = 1000


class ManyPartitionsTest(PreallocNodesTest):
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
            node_prealloc_count=1,
            extra_rp_conf={
                # Disable leader balancer initially, to enable us to check for
                # stable leadership during initial elections and post-restart
                # elections.  We will switch it on later, to exercise it during
                # the traffic stress test.
                'enable_leader_balancer': False,
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
    def test_many_partitions(self):
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

        This test dynamically scales with the nodes provided.  It can run
        on docker environments, but that is only for developers iterating
        on the test itself: meaningful tests of scale must be done on
        dedicated nodes.
        """

        # Scale tests are not run on debug builds
        assert not self.debug_mode

        replication_factor = 3
        node_count = len(self.redpanda.nodes)

        # If we run on nodes with more memory than our HARD_PARTITION_LIMIT, then
        # artificially throttle the nodes' memory to avoid the test being too easy.
        # We are validating that the system works up to the limit, and that it works
        # up to the limit within the default per-partition memory footprint.
        node_memory = self.redpanda.get_node_memory_mb()
        node_cpus = self.redpanda.get_node_cpu_count()
        node_disk_free = self.redpanda.get_node_disk_free()

        self.logger.info(
            f"Nodes have {node_cpus} cores, {node_memory}MB memory, {node_disk_free / (1024 * 1024)}MB free disk"
        )

        # On large nodes, reserve half of shard 0 to minimize interference
        # between data and control plane, as control plane messages become
        # very large.

        shard0_reserve = None
        if node_cpus >= 8:
            shard0_reserve = PARTITIONS_PER_SHARD / 2

        # Reserve a few slots for internal partitions, do not be
        # super specific about how many because we may add some in
        # future for e.g. audit logging.
        internal_partition_slack = 10

        # Calculate how many partitions we will aim to create, based
        # on the size & count of nodes.  This enables running the
        # test on various instance sizes without explicitly adjusting.
        partition_limit = (node_count * node_cpus * PARTITIONS_PER_SHARD
                           ) / replication_factor - internal_partition_slack
        if shard0_reserve:
            partition_limit -= node_count * shard0_reserve

        partition_limit = min(HARD_PARTITION_LIMIT, partition_limit)

        if not self.redpanda.dedicated_nodes:
            partition_limit = min(DOCKER_PARTITION_LIMIT, partition_limit)

        self.logger.info(f"Selected partition limit {partition_limit}")

        # Emulate seastar's policy for default reserved memory
        reserved_memory = max(1536, int(0.07 * node_memory) + 1)
        effective_node_memory = node_memory - reserved_memory

        # Aim to use about half the disk space: set retention limits
        # to enforce that.  This enables traffic tests to run as long
        # as they like without risking filling the disk.
        partition_replicas_per_node = int(
            (partition_limit * replication_factor) / node_count)
        retention_bytes = int(
            (node_disk_free / 2) / partition_replicas_per_node)

        # Choose an appropriate segment size to enable retention
        # to kick in.
        # TODO: redpanda should figure this out automatically by
        #       rolling segments pre-emptively if low on disk space
        segment_size = int(retention_bytes / 2)

        self.logger.info(
            f"Selected retention.bytes={retention_bytes}, segment.bytes={segment_size}"
        )

        mb_per_partition = 1
        if not self.redpanda.dedicated_nodes:
            # In docker, assume we're on a laptop drive and not doing
            # real testing, so disable fsync to make test run faster.
            resource_settings_args = {'bypass_fsync': True}

            # In docker, make the test a bit more realistic by clamping
            # memory if nodes have more memory than should be required
            # to exercise the partition limit.
            if effective_node_memory > partition_replicas_per_node / mb_per_partition:
                clamp_memory = mb_per_partition * (
                    (partition_replicas_per_node + internal_partition_slack) +
                    reserved_memory)

                # Handy if hacking HARD_PARTITION_LIMIT to something low to run on a workstation
                clamp_memory = max(clamp_memory, 500)
                resource_settings_args['memory_mb'] = clamp_memory

            self.redpanda.set_resource_settings(
                ResourceSettings(**resource_settings_args))

        # Should not happen on the expected EC2 instance types where
        # the cores-RAM ratio is sufficient to meet our shards-per-core
        if effective_node_memory < partition_replicas_per_node / mb_per_partition:
            raise RuntimeError(
                f"Node memory is too small ({node_memory}MB - {reserved_memory}MB)"
            )

        # Run with one huge topic: this is the more stressful case for Redpanda, compared
        # with multiple modestly-sized topics, so it's what we test to find the system's limits.
        n_topics = 1

        # Partitions per topic
        n_partitions = int(partition_limit / n_topics)

        self.logger.info(
            f"Running partition scale test with {n_partitions} partitions on {n_topics} topics"
        )

        self.redpanda.start()

        self.logger.info("Entering topic creation")
        topic_names = [f"scale_{i:06d}" for i in range(0, n_topics)]
        for tn in topic_names:
            self.logger.info(
                f"Creating topic {tn} with {n_partitions} partitions")
            self.rpk.create_topic(tn,
                                  partitions=n_partitions,
                                  replicas=replication_factor,
                                  config={
                                      'segment.bytes': segment_size,
                                      'retention.bytes': retention_bytes
                                  })

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
        # Pessimistic bandwidth guess, accounting for the sub-disk bandwidth
        # that a single-threaded consumer may see

        expect_bandwidth = 50 * 1024 * 1024

        expect_transmit_time = int(write_bytes_per_topic / expect_bandwidth)
        expect_transmit_time = max(expect_transmit_time, 30)

        self.logger.info("Entering initial produce")
        for tn in topic_names:
            t1 = time.time()
            producer = FranzGoVerifiableProducer(
                self.test_context,
                self.redpanda,
                tn,
                msg_size,
                msg_count_per_topic,
                custom_node=self.preallocated_nodes)
            producer.start()
            producer.wait(timeout_sec=expect_transmit_time)
            self.free_preallocated_nodes()
            duration = time.time() - t1
            self.logger.info(
                f"Wrote {write_bytes_per_topic} bytes to {tn} in {duration}s, bandwidth {(write_bytes_per_topic / duration)/(1024 * 1024)}MB/s"
            )

        def get_fd_counts():
            counts = {}
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=node_count) as executor:
                futs = {}
                for node in self.redpanda.nodes:
                    futs[node.name] = executor.submit(
                        lambda: sum(1 for _ in self.redpanda.lsof_node(node)))

                for node_name, fut in futs.items():
                    file_count = fut.result()
                    counts[node_name] = file_count

            return counts

        for node_name, file_count in get_fd_counts().items():
            self.logger.info(
                f"Open files before restarts on {node_name}: {file_count}")

        # Over large partition counts, the startup time is linear with the
        # amount of data we played in, because no one partition gets far
        # enough to snapshot.
        expect_start_time = expect_transmit_time

        # Measure the impact of restarts on resource utilization on an idle system:
        # at time of writing we know that the used FD count will go up substantially
        # on each restart (https://github.com/redpanda-data/redpanda/issues/4057)
        restart_count = 2

        self.logger.info("Entering restart stress test")
        for i in range(1, restart_count + 1):
            self.logger.info(f"Cluster restart {i}/{restart_count}...")

            # Normal restarts are rolling restarts, but because replay takes substantial time,
            # on an idle system it is helpful to do a concurrent global restart rather than
            # waiting for each node one by one.
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=node_count) as executor:
                futs = []
                for node in self.redpanda.nodes:
                    futs.append(
                        executor.submit(self.redpanda.restart_nodes,
                                        nodes=[node],
                                        start_timeout=expect_start_time))

                for f in futs:
                    # Raise on error
                    f.result()
            self.logger.info(
                f"Restart {i}/{restart_count} complete.  Waiting for elections..."
            )

            wait_until(
                lambda: self._all_elections_done(topic_names, n_partitions),
                timeout_sec=60,
                backoff_sec=5)
            self.logger.info(f"Post-restart elections done.")

            for node_name, file_count in get_fd_counts().items():
                self.logger.info(
                    f"Open files after {i} restarts on {node_name}: {file_count}"
                )

        # With increased overhead from all those segment rolls during restart,
        # check that consume still works.
        self._consume_all(topic_names, msg_count_per_topic,
                          expect_transmit_time)

        # Now that we've tested basic ability to form consensus and survive some
        # restarts, move on to a more general stress test.
        self.logger.info("Entering traffic stress test")
        target_topic = topic_names[0]

        stress_msg_size = 32768

        stress_data_size = 1024 * 1024 * 1024 * 100

        stress_msg_count = int(stress_data_size / stress_msg_size)
        fast_producer = FranzGoVerifiableProducer(
            self.test_context,
            self.redpanda,
            target_topic,
            stress_msg_size,
            stress_msg_count,
            custom_node=self.preallocated_nodes)
        fast_producer.start()

        # Don't start consumers until the producer has written out its first
        # checkpoint with valid ranges.
        wait_until(lambda: fast_producer.produce_status.acked > 0,
                   timeout_sec=30,
                   backoff_sec=1.0)

        rand_consumer = FranzGoVerifiableRandomConsumer(
            self.test_context,
            self.redpanda,
            target_topic,
            0,
            100,
            10,
            nodes=self.preallocated_nodes)
        rand_consumer.start(clean=False)
        rand_consumer.shutdown()
        rand_consumer.wait()

        fast_producer.wait()
        self.logger.info(
            "Write+randread stress test complete, verifying sequentially")

        seq_consumer = FranzGoVerifiableSeqConsumer(self.test_context,
                                                    self.redpanda,
                                                    target_topic, 0,
                                                    self.preallocated_nodes)
        seq_consumer.start(clean=False)
        seq_consumer.shutdown()
        seq_consumer.wait()
        assert seq_consumer.consumer_status.invalid_reads == 0
        assert seq_consumer.consumer_status.valid_reads == stress_msg_count + msg_count_per_topic

        self.logger.info("Entering leader balancer stress test")

        # Enable the leader balancer and check that the system remains stable
        # under load.  We do not leave the leader balancer on for most of the test, because
        # it makes reads _much_ slower, because the consumer keeps stalling and waiting for
        # elections: at any moment in a 10k partition topic, it's highly likely at least
        # one partition is offline for a leadership migration.
        self.redpanda.set_cluster_config({'enable_leader_balancer': True},
                                         expect_restart=False)
        lb_stress_period = 120
        lb_stress_produce_bytes = expect_bandwidth * lb_stress_period
        lb_stress_message_count = int(lb_stress_produce_bytes /
                                      stress_msg_size)
        fast_producer = FranzGoVerifiableProducer(
            self.test_context,
            self.redpanda,
            target_topic,
            stress_msg_size,
            lb_stress_message_count,
            custom_node=self.preallocated_nodes)
        fast_producer.start()
        rand_consumer.start()
        time.sleep(lb_stress_period
                   )  # Let the system receive traffic for a set time period
        rand_consumer.shutdown()
        rand_consumer.wait()
        fast_producer.wait()
