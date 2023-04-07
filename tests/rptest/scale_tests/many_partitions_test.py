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
from collections import Counter

from ducktape.utils.util import wait_until, TimeoutError
import numpy

from rptest.services.cluster import cluster
from rptest.clients.rpk import RpkTool, RpkException
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.redpanda import ResourceSettings, RESTART_LOG_ALLOW_LIST, LoggingConfig
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer, KgoVerifierRandomConsumer
from rptest.services.kgo_repeater_service import KgoRepeaterService, repeater_traffic
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import OMBSampleConfigurations

# An unreasonably large fetch request: we submit requests like this in the
# expectation that the server will properly clamp the amount of data it
# actually tries to marshal into a response.
# franz-go default maxBrokerReadBytes -- --fetch-max-bytes may not exceed this
BIG_FETCH = 104857600

# The maximum total number of partitions in a Redpanda cluster.  No matter
# how big the nodes or how many of them, the test does not exceed this.
# This limit represents bottlenecks in central controller/health
# functions.
HARD_PARTITION_LIMIT = 50000

# How many partitions we will create per shard: this is the primary scaling
# factor that controls how many partitions a given cluster will get.
PARTITIONS_PER_SHARD = 1000

# Number of partitions to create when running in docker (i.e.
# when dedicated_nodes=false).  This is independent of the
# amount of RAM or CPU that the nodes claim to have, because
# we know they are liable to be oversubscribed.
# This is _not_ for running on oversubscribed CI environments: it's for
# runnig on a reasonably powerful developer machine while they work
# on the test.
DOCKER_PARTITION_LIMIT = 128


class ScaleParameters:
    def __init__(self, redpanda, replication_factor):
        self.redpanda = redpanda

        node_count = len(self.redpanda.nodes)

        # If we run on nodes with more memory than our HARD_PARTITION_LIMIT, then
        # artificially throttle the nodes' memory to avoid the test being too easy.
        # We are validating that the system works up to the limit, and that it works
        # up to the limit within the default per-partition memory footprint.
        node_memory = self.redpanda.get_node_memory_mb()
        self.node_cpus = self.redpanda.get_node_cpu_count()
        node_disk_free = self.redpanda.get_node_disk_free()

        self.logger.info(
            f"Nodes have {self.node_cpus} cores, {node_memory}MB memory, {node_disk_free / (1024 * 1024)}MB free disk"
        )

        # On large nodes, reserve half of shard 0 to minimize interference
        # between data and control plane, as control plane messages become
        # very large.
        shard0_reserve = None
        if self.node_cpus >= 8:
            shard0_reserve = PARTITIONS_PER_SHARD / 2

        # Reserve a few slots for internal partitions, do not be
        # super specific about how many because we may add some in
        # future for e.g. audit logging.
        internal_partition_slack = 10

        # Calculate how many partitions we will aim to create, based
        # on the size & count of nodes.  This enables running the
        # test on various instance sizes without explicitly adjusting.
        self.partition_limit = int(
            (node_count * self.node_cpus * PARTITIONS_PER_SHARD) /
            replication_factor - internal_partition_slack)
        if shard0_reserve:
            self.partition_limit -= node_count * shard0_reserve

        self.partition_limit = min(HARD_PARTITION_LIMIT, self.partition_limit)

        if not self.redpanda.dedicated_nodes:
            self.partition_limit = min(DOCKER_PARTITION_LIMIT,
                                       self.partition_limit)

        self.logger.info(f"Selected partition limit {self.partition_limit}")

        # Emulate seastar's policy for default reserved memory
        reserved_memory = max(1536, int(0.07 * node_memory) + 1)
        effective_node_memory = node_memory - reserved_memory

        # Aim to use about half the disk space: set retention limits
        # to enforce that.  This enables traffic tests to run as long
        # as they like without risking filling the disk.
        partition_replicas_per_node = int(
            (self.partition_limit * replication_factor) / node_count)
        self.retention_bytes = int(
            (node_disk_free / 2) / partition_replicas_per_node)

        # Choose an appropriate segment size to enable retention
        # rules to kick in promptly.
        # TODO: redpanda should figure this out automatically by
        #       rolling segments pre-emptively if low on disk space
        self.segment_size = int(self.retention_bytes / 4)

        # The expect_bandwidth is just for calculating sensible
        # timeouts when waiting for traffic: it is not a scientific
        # success condition for the tests.
        if self.redpanda.dedicated_nodes:
            # A 24 core i3en.6xlarge has about 1GB/s disk write
            # bandwidth.  Divide by 2 to give comfortable room for variation.
            # This is total bandwidth from a group of producers.
            self.expect_bandwidth = (node_count / replication_factor) * (
                self.node_cpus / 24.0) * 1E9 * 0.5

            # Single-producer tests are slower, bottlenecked on the
            # client side.
            self.expect_single_bandwidth = 200E6
        else:
            # Docker environment: curb your expectations.  Not only is storage
            # liable to be slow, we have many nodes sharing the same drive.
            self.expect_bandwidth = 5 * 1024 * 1024
            self.expect_single_bandwidth = 10E6

        self.logger.info(
            f"Selected retention.bytes={self.retention_bytes}, segment.bytes={self.segment_size}"
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
        else:
            # On dedicated nodes we will use an explicit reactor stall threshold
            # as a success condition.
            self.redpanda.set_resource_settings(
                ResourceSettings(reactor_stall_threshold=100))

        # Should not happen on the expected EC2 instance types where
        # the cores-RAM ratio is sufficient to meet our shards-per-core
        if effective_node_memory < partition_replicas_per_node / mb_per_partition:
            raise RuntimeError(
                f"Node memory is too small ({node_memory}MB - {reserved_memory}MB)"
            )

    @property
    def logger(self):
        return self.redpanda.logger


class ManyPartitionsTest(PreallocNodesTest):
    """
    Validates basic functionality in the presence of larger numbers
    of partitions than most other tests.
    """
    topics = ()

    # Redpanda is responsible for bounding its own startup time via
    # STORAGE_TARGET_REPLAY_BYTES.  The resulting walltime for startup
    # depends on the speed of the disk.  60 seconds is long enough
    # for an i3en.xlarge (and more than enough for faster instance types)
    EXPECT_START_TIME = 60

    LEADER_BALANCER_PERIOD_MS = 30000

    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        super(ManyPartitionsTest, self).__init__(
            test_ctx,
            *args,
            num_brokers=9,
            node_prealloc_count=3,
            extra_rp_conf={
                # Disable leader balancer initially, to enable us to check for
                # stable leadership during initial elections and post-restart
                # elections.  We will switch it on later, to exercise it during
                # the traffic stress test.
                #'enable_leader_balancer': False,

                # Avoid having to wait 5 minutes for leader balancer to activate
                "leader_balancer_idle_timeout": self.LEADER_BALANCER_PERIOD_MS,
                "leader_balancer_mute_timeout": self.LEADER_BALANCER_PERIOD_MS,

                # TODO: ensure that the systme works well _without_ these non-default
                # properties, or if they are necessary and we choose not to make them
                # the defaults, then that they are reflected propertly in cloud config profiles
                'reclaim_batch_cache_min_free': 256000000,
                'storage_read_buffer_size': 32768,
                'storage_read_readahead_count': 2,
                'disable_metrics': True,
                'disable_public_metrics': False,
                'append_chunk_size': 32768,
                'kafka_rpc_server_tcp_recv_buf': 131072,
                'kafka_rpc_server_tcp_send_buf': 131072,
                'kafka_rpc_server_stream_recv_buf': 32768,

                # Enable all the rate limiting things we would have in production, to ensure
                # their effect is accounted for, but with high enough limits that we do
                # not expect to hit them.
                'kafka_connection_rate_limit': 10000,
                'kafka_connections_max': 50000,

                # Enable segment size jitter as this is a stress test and does not
                # rely on exact segment counts.
                'log_segment_size_jitter_percent': 5,
            },
            # Configure logging the same way a user would when they have
            # very many partitions: set logs with per-partition messages
            # to warn instead of info.
            log_config=LoggingConfig('info',
                                     logger_levels={
                                         'storage': 'warn',
                                         'storage-gc': 'warn',
                                         'raft': 'warn',
                                         'offset_translator': 'warn'
                                     }),
            **kwargs)
        self.rpk = RpkTool(self.redpanda)

    def _all_elections_done(self, topic_names: list[str], p_per_topic: int):
        any_incomplete = False
        for tn in topic_names:
            try:
                partitions = list(self.rpk.describe_topic(tn, tolerant=True))
            except RpkException as e:
                # One retry.  This is a case where running rpk after a full
                # cluster restart can time out after 30 seconds, but succeed
                # promptly as soon as you retry.
                self.logger.error(f"Retrying describe_topic for {e}")
                partitions = list(self.rpk.describe_topic(tn, tolerant=True))

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

    def _node_leadership_evacuated(self, topic_names: list[str],
                                   p_per_topic: int, node_id: int):
        any_incomplete = False
        for tn in topic_names:
            partitions = list(self.rpk.describe_topic(tn, tolerant=True))
            if len(partitions) < p_per_topic:
                self.logger.info(f"describe omits partitions for topic {tn}")
                any_incomplete = True
                continue

            assert len(partitions) == p_per_topic
            for p in partitions:
                if p.leader == node_id:
                    self.logger.info(
                        f"partition {tn}/{p.id} still on node {node_id}")
                    any_incomplete = True

        return not any_incomplete

    def _node_leadership_balanced(self, topic_names: list[str],
                                  p_per_topic: int):
        node_leader_counts = Counter()
        any_incomplete = False
        for tn in topic_names:
            try:
                partitions = list(self.rpk.describe_topic(tn, tolerant=True))
            except RpkException as e:
                # We can get e.g. timeouts from rpk if it is trying to describe
                # a big topic on a heavily loaded cluster: treat these as retryable
                # and let our caller call us again.
                self.logger.warn(f"RPK error, assuming retryable: {e}")
                return False

            if len(partitions) < p_per_topic:
                self.logger.info(f"describe omits partitions for topic {tn}")
                any_incomplete = True
                continue

            assert len(partitions) == p_per_topic
            node_leader_counts.update(p.leader for p in partitions)

        for n, c in node_leader_counts.items():
            self.logger.info(f"node {n} leaderships: {c}")

        assert len(node_leader_counts) <= len(self.redpanda.nodes)
        if len(node_leader_counts) != len(self.redpanda.nodes):
            self.logger.info("Not all nodes have leaderships")
            return False

        if any_incomplete:
            return False

        data = list(node_leader_counts.values())
        stddev = numpy.std(data)
        error = stddev / (
            (len(topic_names) * p_per_topic) / len(self.redpanda.nodes))

        # FIXME: this isn't the same check the leader balancer itself does, but it
        # should suffice to check the leader balancer is progressing.
        threshold = 0.1
        if (p_per_topic * len(topic_names)) < 5000:
            # Low scale systems have bumpier stats
            threshold = 0.25

        balanced = error < threshold
        self.logger.info(
            f"leadership balanced={balanced} (stddev: {stddev}, error {error})"
        )
        return balanced

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

    def _get_fd_counts(self):
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(self.redpanda.nodes)) as executor:

            return list(
                executor.map(
                    lambda n: tuple(
                        [n, sum(1 for _ in self.redpanda.lsof_node(n))]),
                    self.redpanda.nodes))

    def _concurrent_restart(self):
        """
        Restart the whole cluster, all nodes in parallel.
        """

        # Normal restarts are rolling restarts, but because replay takes substantial time,
        # on an idle system it is helpful to do a concurrent global restart rather than
        # waiting for each node one by one.
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(self.redpanda.nodes)) as executor:
            futs = []
            for node in self.redpanda.nodes:
                futs.append(
                    executor.submit(self.redpanda.restart_nodes,
                                    nodes=[node],
                                    start_timeout=self.EXPECT_START_TIME))

            for f in futs:
                # Raise on error
                f.result()

    def _single_node_restart(self, scale: ScaleParameters, topic_names: list,
                             n_partitions: int):
        """
        Restart a single node to check stability through the movement of
        leadership to other nodes, plus the subsequent leader balancer
        activity to redistribute after it comes back up.
        """

        node = self.redpanda.nodes[-1]
        self.logger.info(f"Single node restart on node {node.name}")
        node_id = self.redpanda.idx(node)

        self.redpanda.stop_node(node)

        # Wait for leaderships to stabilize on the surviving nodes
        wait_until(
            lambda: self._node_leadership_evacuated(topic_names, n_partitions,
                                                    node_id), 30, 1)

        self.redpanda.start_node(node, timeout=self.EXPECT_START_TIME)

        # Heuristic: in testing we see leaderships transfer at about 10
        # per second.  2x margin for error.  Up to the leader balancer period
        # wait for it to activate.
        transfers_per_sec = 10
        expect_leader_transfer_time = 2 * (
            n_partitions / len(self.redpanda.nodes)) / transfers_per_sec + (
                self.LEADER_BALANCER_PERIOD_MS / 1000) * 2

        # Wait for leaderships to achieve balance.  This is bounded by:
        #  - Time for leader_balancer to issue+await all the transfers
        #  - Time for raft to achieve recovery, a prerequisite for
        #    leadership.
        t1 = time.time()
        wait_until(
            lambda: self._node_leadership_balanced(topic_names, n_partitions),
            expect_leader_transfer_time, 10)
        self.logger.info(
            f"Leaderships balanced in {time.time() - t1:.2f} seconds")

    def _restart_stress(self, scale: ScaleParameters, topic_names: list,
                        n_partitions: int, inter_restart_check: callable):
        """
        Restart the cluster several times, to check stability
        """

        # Measure the impact of restarts on resource utilization on an idle system:
        # at time of writing we know that the used FD count will go up substantially
        # on each restart (https://github.com/redpanda-data/redpanda/issues/4057)
        restart_count = 2

        for node_name, file_count in self._get_fd_counts():
            self.logger.info(
                f"Open files before restarts on {node_name}: {file_count}")

        self.logger.info("Entering restart stress test")
        for i in range(1, restart_count + 1):
            self.logger.info(f"Cluster restart {i}/{restart_count}...")
            self._concurrent_restart()

            self.logger.info(
                f"Restart {i}/{restart_count} complete.  Waiting for elections..."
            )

            wait_until(
                lambda: self._all_elections_done(topic_names, n_partitions),
                timeout_sec=60,
                backoff_sec=5)
            self.logger.info(f"Post-restart elections done.")

            inter_restart_check()

            for node_name, file_count in self._get_fd_counts():
                self.logger.info(
                    f"Open files after {i} restarts on {node_name}: {file_count}"
                )

    def _write_and_random_read(self, scale: ScaleParameters, topic_names):
        """
        This is a relatively low intensity test, that covers random
        and sequential reads & validates correctness of offsets in the
        partitions written to.

        It answers the question "is the cluster basically working properly"?  Before
        we move on to more stressful testing, and in the process ensures there
        is enough data in the system that we aren't in the "everything fits in
        memory" regime.

        Note: run this before other workloads, so that kgo-verifier's random
        readers are able to validate most of what they read (otherwise they
        will mostly be reading data written by a different workload, which
        drives traffic but is a less strict test because it can't validate
        the offsets of those messages)
        """
        # Now that we've tested basic ability to form consensus and survive some
        # restarts, move on to a more general stress test.
        self.logger.info("Entering traffic stress test")
        target_topic = topic_names[0]

        # Assume fetches will be 10MB, the franz-go default
        fetch_bytes_per_partition = 10 * 1024 * 1024

        # * Need enough data that if a consumer tried to fetch it all at once
        # in a single request, it would run out of memory.  OR the amount of
        # data that would fill a 10MB max_bytes per partition in a fetch, whichever
        # is lower (avoid writing excessive data for tests with fewer partitions).
        # * Then apply a factor of two to make sure we have enough data to drive writes
        # to disk during consumption, not just enough data to hold it all in the batch
        # cache.

        # Partitions per topic
        n_partitions = int(scale.partition_limit / len(topic_names))

        write_bytes_per_topic = min(
            int((self.redpanda.get_node_memory_mb() * 1024 * 1024) /
                len(topic_names)),
            fetch_bytes_per_partition * n_partitions) * 2

        if not self.redpanda.dedicated_nodes:
            # Docker developer mode: likely to be on a workstation with lots of RAM
            # and we don't want to wait to fill it all up.
            write_bytes_per_topic = int(1E9 / len(topic_names))

        msg_size = 128 * 1024
        msg_count_per_topic = int((write_bytes_per_topic / msg_size))

        # Approx time to write or read all messages, for timeouts
        # Pessimistic bandwidth guess, accounting for the sub-disk bandwidth
        # that a single-threaded consumer may see

        expect_transmit_time = int(write_bytes_per_topic /
                                   scale.expect_single_bandwidth)
        expect_transmit_time = max(expect_transmit_time, 30)

        for tn in topic_names:
            t1 = time.time()
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                tn,
                msg_size,
                msg_count_per_topic,
                custom_node=[self.preallocated_nodes[0]])
            producer.start()
            producer.wait(timeout_sec=expect_transmit_time)
            self.free_preallocated_nodes()
            duration = time.time() - t1
            self.logger.info(
                f"Wrote {write_bytes_per_topic} bytes to {tn} in {duration}s, bandwidth {(write_bytes_per_topic / duration)/(1024 * 1024)}MB/s"
            )

        stress_msg_size = 32768
        stress_data_size = 1024 * 1024 * 1024 * 100

        if not self.redpanda.dedicated_nodes:
            stress_data_size = 2E9

        stress_msg_count = int(stress_data_size / stress_msg_size)
        fast_producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            target_topic,
            stress_msg_size,
            stress_msg_count,
            custom_node=[self.preallocated_nodes[0]])
        fast_producer.start()

        # Don't start consumers until the producer has written out its first
        # checkpoint with valid ranges.
        wait_until(lambda: fast_producer.produce_status.acked > 0,
                   timeout_sec=30,
                   backoff_sec=1.0)

        rand_ios = 100
        rand_parallel = 100
        if not self.redpanda.dedicated_nodes:
            rand_parallel = 10
            rand_ios = 10

        rand_consumer = KgoVerifierRandomConsumer(
            self.test_context,
            self.redpanda,
            target_topic,
            msg_size=0,
            rand_read_msgs=rand_ios,
            parallel=rand_parallel,
            nodes=[self.preallocated_nodes[1]])
        rand_consumer.start(clean=False)
        rand_consumer.wait()

        fast_producer.stop()
        fast_producer.wait()
        self.logger.info(
            "Write+randread stress test complete, verifying sequentially")

        seq_consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            target_topic,
            0,
            nodes=[self.preallocated_nodes[2]])
        seq_consumer.start(clean=False)
        seq_consumer.wait()
        assert seq_consumer.consumer_status.validator.invalid_reads == 0
        assert seq_consumer.consumer_status.validator.valid_reads >= fast_producer.produce_status.acked + msg_count_per_topic

        self.free_preallocated_nodes()

    def _run_omb(self, scale: ScaleParameters):

        # 1.25GB/s is a rather gentle rate on 12*i3en.xlarge 12 core nodes
        # when using 1000 partitions per shard, use it as a baseline.
        # (OMB is not good at stress testing, the worker nodes start throwing
        # exceptions if a cluster isn't keeping up with the rate requested
        # by the workload, so we are not aiming to aggressively saturate the system)
        producer_bw = (
            (len(self.redpanda.nodes) * scale.node_cpus) / 144.0) * 1.25E9

        # It is necessary to scale the producer+consumer counts as well
        # as the total throughput: otherwise there are too few messages
        # per producer, and OMB reports this as a very high E2E latency.
        # Roughly node_cpus is of the right order of magnitude.
        producer_count = scale.node_cpus
        consumer_count = scale.node_cpus

        # Don't tweak this without also adjusting payload_file
        message_size = 4096

        producer_rate = producer_bw / message_size

        # For really high partition counts, the throughput can't keep up
        # with what the cluster did at more modest density, and this
        # causes OMB to start failing internally with 500s when it backs up,
        # or when it runs through, to fail because it didn't hit its latency
        # target.
        if PARTITIONS_PER_SHARD > 1000:
            producer_rate *= (1000.0 / PARTITIONS_PER_SHARD)

        # on 12x i3en.3xlarge
        # it is stable driving 1159.661 MB/s
        # across 16 producers per topic, 16 consumers per topic

        topic_count = 10 if self.redpanda.dedicated_nodes else 2
        workload = {
            "name":
            "ManyPartitionsWorkload",
            "topics":
            topic_count,
            "partitions_per_topic":
            scale.partition_limit / topic_count,
            "subscriptions_per_topic":
            1,
            "consumer_per_subscription":
            producer_count if self.redpanda.dedicated_nodes else 2,
            "producers_per_topic":
            consumer_count if self.redpanda.dedicated_nodes else 2,
            "producer_rate":
            producer_rate if self.redpanda.dedicated_nodes else 1000,
            "message_size":
            message_size,
            "payload_file":
            "payload/payload-4Kb.data",
            "consumer_backlog_size_GB":
            0,
            "test_duration_minutes":
            3,
            "warmup_duration_minutes":
            1,
        }

        bench_node = self.preallocated_nodes[0]
        worker_nodes = self.preallocated_nodes[1:]

        # TODO: remove these overrides once the cause of latency
        # spikes in OMB is found and mitigated. For now these
        # numbers are dervived from the outliers found in the
        # cloud benchmarking effort.
        # Tracking issue: https://github.com/redpanda-data/redpanda/issues/6334
        validator_overrides = {
            OMBSampleConfigurations.E2E_LATENCY_50PCT:
            [OMBSampleConfigurations.lte(51)],
            OMBSampleConfigurations.E2E_LATENCY_AVG:
            [OMBSampleConfigurations.lte(145)],
        }

        # TODO: use PROD_ENV_VALIDATOR?
        benchmark = OpenMessagingBenchmark(
            self._ctx,
            self.redpanda,
            "SIMPLE_DRIVER",
            (workload, OMBSampleConfigurations.UNIT_TEST_LATENCY_VALIDATOR
             | validator_overrides),
            node=bench_node,
            worker_nodes=worker_nodes)
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()

        self.free_preallocated_nodes()

    @cluster(num_nodes=12, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_many_partitions_compacted(self):
        self._test_many_partitions(compacted=True)

    @cluster(num_nodes=12, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_many_partitions(self):
        self._test_many_partitions(compacted=False)

    @cluster(num_nodes=12, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_omb(self):
        scale = ScaleParameters(self.redpanda, replication_factor=3)
        self.redpanda.start(parallel=True)

        # We have other OMB benchmark tests, but this one runs at the
        # peak partition count.
        self._run_omb(scale)

    def _test_many_partitions(self, compacted):
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

        scale = ScaleParameters(self.redpanda, replication_factor)

        # Run with one huge topic: it is more stressful for redpanda when clients
        # request the metadata for many partitions at once, and the simplest way
        # to get traffic generators to do that without the clients supporting
        # writing to arrays of topics is to put all the partitions into one topic.
        n_topics = 1

        # Partitions per topic
        n_partitions = int(scale.partition_limit / n_topics)

        self.logger.info(
            f"Running partition scale test with {n_partitions} partitions on {n_topics} topics"
        )

        self.redpanda.start(parallel=True)

        self.logger.info("Entering topic creation")
        topic_names = [f"scale_{i:06d}" for i in range(0, n_topics)]
        for tn in topic_names:
            self.logger.info(
                f"Creating topic {tn} with {n_partitions} partitions")
            config = {
                'segment.bytes': scale.segment_size,
                'retention.bytes': scale.retention_bytes
            }
            if compacted:
                config['cleanup.policy'] = 'compact'

            self.rpk.create_topic(tn,
                                  partitions=n_partitions,
                                  replicas=replication_factor,
                                  config=config)

        self.logger.info(f"Awaiting elections...")
        wait_until(lambda: self._all_elections_done(topic_names, n_partitions),
                   timeout_sec=60,
                   backoff_sec=5)
        self.logger.info(f"Initial elections done.")

        for node_name, file_count in self._get_fd_counts():
            self.logger.info(
                f"Open files after initial elections on {node_name}: {file_count}"
            )

        self.logger.info(
            "Entering initial traffic test, writes + random reads")
        self._write_and_random_read(scale, topic_names)

        # Start kgo-repeater
        # 768 workers on a 24 core node has been seen to work well.
        workers = 32 * scale.node_cpus

        if not self.redpanda.dedicated_nodes:
            workers = min(workers, 4)

        repeater_kwargs = {}
        if compacted:
            # Each parititon gets roughly 10 unique keys, after which
            # compaction should kick in.
            repeater_kwargs['key_count'] = int(scale.partition_limit * 10)
        else:
            # Not doing compaction, doesn't matter how big the keyspace
            # is, use whole 32 bit range to get best possible distribution
            # across partitions.
            repeater_kwargs['key_count'] = 2**32

        # Main test phase: with continuous background traffic, exercise restarts and
        # any other cluster changes that might trip up at scale.
        repeater_msg_size = 16384
        with repeater_traffic(context=self._ctx,
                              redpanda=self.redpanda,
                              nodes=self.preallocated_nodes,
                              topic=topic_names[0],
                              msg_size=repeater_msg_size,
                              workers=workers,
                              max_buffered_records=64,
                              cleanup=lambda: self.free_preallocated_nodes(),
                              **repeater_kwargs) as repeater:
            repeater_await_bytes = 1E9
            repeater_await_msgs = int(repeater_await_bytes / repeater_msg_size)

            def progress_check():
                # Explicit wait for consumer group, because we might have e.g.
                # just restarted the cluster, and don't want to include that
                # delay in our throughput-driven timeout expectations
                self.logger.info(f"Checking repeater group is ready...")
                repeater.await_group_ready()

                t = repeater_await_bytes / scale.expect_bandwidth
                self.logger.info(
                    f"Waiting for {repeater_await_msgs} messages in {t} seconds"
                )
                t1 = time.time()
                repeater.await_progress(repeater_await_msgs, t)
                t2 = time.time()

                # This is approximate, because await_progress isn't returning the very
                # instant the workers hit their collective target.
                self.logger.info(
                    f"Wait complete, approx bandwidth {(repeater_await_bytes / (t2-t1))/(1024*1024.0)}MB/s"
                )

            progress_check()

            self.logger.info(f"Entering single node restart phase")
            self._single_node_restart(scale, topic_names, n_partitions)
            progress_check()

            self.logger.info(f"Entering restart stress test phase")
            self._restart_stress(scale, topic_names, n_partitions,
                                 progress_check)

            self.logger.info(
                f"Post-restarts: checking repeater group is ready...")
            repeater.await_group_ready()

            # Done with restarts, now do a longer traffic soak
            self.logger.info(f"Entering traffic soak phase")
            soak_await_bytes = 100E9
            if not self.redpanda.dedicated_nodes:
                soak_await_bytes = 10E9

            soak_await_msgs = soak_await_bytes / repeater_msg_size
            t1 = time.time()
            initial_p, _ = repeater.total_messages()
            try:
                repeater.await_progress(
                    soak_await_msgs, soak_await_bytes / scale.expect_bandwidth)
            except TimeoutError:
                t2 = time.time()
                final_p, _ = repeater.total_messages()
                bytes_sent = (final_p - initial_p) * repeater_msg_size
                expect_mbps = scale.expect_bandwidth / (1024 * 1024.0)
                actual_mbps = (bytes_sent / (t2 - t1)) / (1024 * 1024.0)
                self.logger.error(
                    f"Expected throughput {expect_mbps:.2f}, got throughput {actual_mbps:.2f}MB/s"
                )
                raise
