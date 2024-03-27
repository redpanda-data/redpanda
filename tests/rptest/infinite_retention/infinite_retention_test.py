# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from dataclasses import dataclass
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize

import rptest.infinite_retention.helpers as helpers
from rptest.infinite_retention.checks import InfiniteRetentionChecks
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, \
    KgoVerifierRandomConsumer, KgoVerifierSeqConsumer, \
    KgoVerifierConsumerGroupConsumer
from rptest.services.redpanda import SISettings

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.utils.si_utils import quiesce_uploads

from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_monitor import RedpandaMonitor

from rptest.services.redpanda import FAILURE_INJECTION_LOG_ALLOW_LIST
from rptest.services.storage_failure_injection import BatchType

batchtype_archival_meta = "archival_metadata"
batchtype_raft_configuration = "raft_configuration"
batchtype_raft_data = "raft_data"


@dataclass(kw_only=True)
class InfiniteRetentionParameters:
    """
    Class to handle all test parameters and have expression based parameters
    recalculate on the spot.
    """
    # Initial parameters
    segment_upload_interval: int = 30
    manifest_upload_interval: int = 10
    grace_upload_wait_interval: int = 30
    producer_ack_timeout: int = 60
    node_start_timeout = 180
    node_stop_timeout = 180
    # small size to generate more segments
    segment_size: int = 1 * 1024 * 1024  # 1 or 4 Mb
    chunk_size: int = segment_size * 0.75
    partition_count: int = 1000
    s3_topic_name: str = "iretention-topic"
    # How much messages should we wait on start
    # before moving on
    msg_count_ack_on_start = 10000

    # Calculate other bitrates
    produce_byte_rate: int = 200 * 1024 * 1024
    # The producer should achieve throughput within this factor
    # of what we asked for: if this is violated then
    # it is something wrong with the client or test environment.
    producer_tolerance: int = 4
    # fill the BW as much as possible on consume
    parallel_consumers: int = 6
    # toggle just in case
    limit_consume_rate: bool = False
    consume_rate_mb: int = 200
    # consumers should receive at least half of the messages
    # aligned with bitrate
    consumer_tolerance: int = 4

    # last iteration includes consuming stage
    total_iterations: int = 5
    # Should be more than 1h in total
    target_runtime: int = 300
    # stress
    target_stress_start: int = 30
    target_stress_delay: int = 30
    # messages
    msg_size: int = 16384

    #####
    # Additional configs
    #####
    # Turn off message deletion
    retention_ms: int = -1
    # Generate more uploads of manifests to the cloud
    cloud_storage_spillover_manifest_max_segments: int = 10
    # Reduce locally cached files size to generate
    # more downloads
    local_retention_bytes: int = segment_size * 32

    # enable merging to generate more re-uploads
    cloud_storage_enable_segment_merging = True

    # segment merging params
    cloud_storage_segment_size_target = 3 * 1024 * 1024
    cloud_storage_segment_size_min = 2 * 1024 * 1024

    # clean out local data more frequently
    cloud_storage_housekeeping_interval_ms = 500

    # rps should not hit the limit set here
    cloud_storage_idle_threshold_rps = 1000000

    # manifest cache reduced
    cloud_storage_manifest_cache_size = 65536

    # evict it frequently
    cloud_storage_materialized_manifest_ttl_ms = 500
    #####

    # Get Bytes from MB
    @property
    def consume_byte_rate(self) -> int:
        return self.consume_rate_mb * 1024 * 1024

    # Get byte size of all messages to be produced
    # during given time
    @property
    def write_bytes(self) -> int:
        return self.produce_byte_rate * self.target_runtime

    # Get byte size of all messages to be consumed
    @property
    def read_bytes(self) -> int:
        return self.consume_byte_rate * self.parallel_consumers

    # Get target message count based on
    # bytes to be produced by msg size
    @property
    def msg_count(self) -> int:
        return self.write_bytes // self.msg_size

    # Message limit when next phase of rolling restarts
    # will not start.
    @property
    def msg_limit_rolling_restart(self) -> int:
        return self.msg_count - (
            (self.produce_byte_rate * self.target_stress_delay) //
            self.msg_size) * 6

    # Maximum producer duration for timing out
    @property
    def expected_producer_duration(self) -> int:
        return (self.write_bytes //
                self.produce_byte_rate) * self.producer_tolerance

    # Max consumer duration is multiplies by number of iterations
    # to accomodate total message count
    # In worst case it could be easily as long as all iterations
    # combined
    @property
    def max_consumer_duration(self) -> int:
        return self.target_runtime * self.consumer_tolerance * \
               self.total_iterations

    # If we limit consume rate, then set message count
    @property
    def consumer_message_count(self) -> int:
        return self.msg_count // (
            self.produce_byte_rate //
            self.consume_byte_rate) // self.consumer_tolerance


class InfiniteRetentionTest(PreallocNodesTest):
    DEFAULT_PARAMS = InfiniteRetentionParameters()
    FINJECT_PARAMS = InfiniteRetentionParameters(segment_upload_interval=45,
                                                 manifest_upload_interval=20,
                                                 grace_upload_wait_interval=60,
                                                 producer_ack_timeout=180)

    def __init__(self, test_context, *args, **kwargs):
        self.params = self.DEFAULT_PARAMS

        kwargs['si_settings'] = SISettings(
            test_context=test_context,
            log_segment_size=self.params.segment_size,
        )

        # Use interval uploads so that at end of test we may do an "everything
        # was uploaded" success condition.
        kwargs['extra_rp_conf'] = {
            # We do not intend to do interval-triggered uploads during produce,
            # but we set this property so that at the end of the test we may
            # do a simple "everything was uploaded" check after the interval.
            'cloud_storage_segment_max_upload_interval_sec':
            self.params.segment_upload_interval,
            # The test will assert that the number of manifest uploads does
            # not exceed what we would expect based on this interval.
            'cloud_storage_manifest_max_upload_interval_sec':
            self.params.manifest_upload_interval,
            # Default retention is infinite
            'delete_retention_ms': self.params.retention_ms,
            # enable merging to generate more re-uploads
            'cloud_storage_enable_segment_merging':
            self.params.cloud_storage_enable_segment_merging,

            # segment merging params
            'cloud_storage_segment_size_target':
            self.params.cloud_storage_segment_size_target,
            'cloud_storage_segment_size_min':
            self.params.cloud_storage_segment_size_min,

            # clean out local data more frequently
            'cloud_storage_housekeeping_interval_ms':
            self.params.cloud_storage_housekeeping_interval_ms,

            # rps should not hit the limit set here
            'cloud_storage_idle_threshold_rps':
            self.params.cloud_storage_idle_threshold_rps,

            # manifest cache reduced
            'cloud_storage_manifest_cache_size':
            self.params.cloud_storage_manifest_cache_size,

            # evict it frequently
            'cloud_storage_materialized_manifest_ttl_ms':
            self.params.cloud_storage_materialized_manifest_ttl_ms,

            # disable spillover: it will be explicitly set in the tests when needed
            'cloud_storage_spillover_manifest_size': None,
        }
        super().__init__(test_context, node_prealloc_count=1, *args, **kwargs)

        self.rpk = RpkTool(self.redpanda)

        self.topics = (TopicSpec(
            retention_ms=self.params.retention_ms,
            replication_factor=3,
            partition_count=self.params.partition_count), )

    def tearDown(self):
        # free nodes just in case
        self.free_nodes()
        self.free_preallocated_nodes()
        super().tearDown()

    def _log_parameters(self):
        """
        Function to output content of params class
        to provide visibility on the values calculated and used
        in the test
        """
        attrs = [k for k in dir(self.params) if not k.startswith('__')]
        attrs.sort()
        self.logger.info("Calculated parameters:\n{}".format(
            ("\n".join([f"{p} = {getattr(self.params, p)}" for p in attrs]))))

    def _create_producer(self):
        return KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic,
            msg_size=self.params.msg_size,
            msg_count=self.params.msg_count,
            batch_max_bytes=512 * 1024,
            rate_limit_bps=self.params.produce_byte_rate,
            custom_node=[self.preallocated_nodes[0]],
            debug_logs=True)

    def _create_rnd_consumer(self):
        return KgoVerifierRandomConsumer(self.test_context,
                                         self.redpanda,
                                         self.topic,
                                         self.params.msg_size,
                                         self.params.consumer_message_count,
                                         self.params.parallel_consumers,
                                         nodes=[self.preallocated_nodes[0]],
                                         debug_logs=True,
                                         trace_logs=True)

    def _create_seq_consumer(self):
        return KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic,
            msg_size=self.params.msg_size,
            max_msgs=None,  # consume everything
            max_throughput_mb=self.params.consume_rate_mb *
            self.params.parallel_consumers,
            nodes=[self.preallocated_nodes[0]],
            debug_logs=True,
            trace_logs=True)

    def _create_group_consumer(self):
        return KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            self.topic,
            msg_size=self.params.msg_size,
            max_msgs=self.params.msg_count,
            readers=self.params.parallel_consumers,
            max_throughput_mb=self.params.consume_rate_mb *
            self.params.parallel_consumers,
            nodes=[self.preallocated_nodes[0]],
            debug_logs=True,
            trace_logs=True)

    @cluster(num_nodes=4)
    @parametrize(restart_stress=True)
    @parametrize(restart_stress=False)
    def constant_throughput_prolonged_test(self, restart_stress):
        """
        Stress the tiered storage upload path on a multiple partitions
        for a prolonged time. This ensures that there is no skips happen
        or lost data occurs for additional amounts of time with retention
        equals to -1. Which is effectively turns it off

        :param restart_stress: if true, additionally restart nodes during
                               writes, and waive throughput success conditions:
                               in this mode we are checking for stability and
                               data durability.
        """
        # Overide duration
        self.params.target_runtime = 300
        producer = self._create_producer()

        self.logger.info(f"Producing {self.params.msg_count} msgs "
                         f"({self.params.write_bytes} bytes)")
        produce_start = time.time()
        producer.start()

        if restart_stress:
            while producer.produce_status.acked < self.params.msg_count:
                # delay stress start for 30 sec by default
                time.sleep(self.params.target_stress_start)
                # restart one with 5 min pause
                # real world: simulates cluster update
                for node in self.redpanda.nodes:
                    time.sleep(self.params.target_stress_delay)
                    self.redpanda.restart_nodes(
                        [node],
                        start_timeout=self.params.node_start_timeout,
                        stop_timeout=self.params.node_stop_timeout)

            # Wait for the cluster to recover after final restart,
            # so that subsequent post-stress success conditions
            # can count on their queries succeeding.
            self.redpanda.wait_until(self.redpanda.healthy,
                                     timeout_sec=60,
                                     backoff_sec=5)

        producer.wait(timeout_sec=self.params.expected_producer_duration)

        # calculate rate
        produce_duration = time.time() - produce_start

        # Init the checks class
        chk = InfiniteRetentionChecks(self.params)
        # Calculate actual byte rate
        actual_byte_rate = (self.params.write_bytes / produce_duration)
        mbps = int(actual_byte_rate / (1024 * 1024))
        self.logger.info(
            f"Produced {self.params.write_bytes} in {produce_duration}s, {mbps}MiB/s"
        )

        # Producer should be within a factor of two of the intended byte rate,
        # or something is wrong with the test (running on nodes that can't
        # keep up?) or with Redpanda (some instability interrupted produce?)
        chk.check_byte_rate_respected(actual_byte_rate)
        if not restart_stress:
            chk.check_byte_rate_respected_no_stress(actual_byte_rate)

        # Read the highest timestamp in local storage
        self.logger.info("Calculating stats for all partitions")
        stats = helpers._calculate_statistic(self.topics, self.rpk,
                                             self.redpanda)

        # Ensure that all messages made it to topic
        self.logger.info(f"calculated hwm: {stats['hwm']}, "
                         f"message count: {self.params.msg_count}")
        chk.check_expected_message_count(stats["hwm"])
        # Local timestamp for latest message
        self.logger.info(f"Max local ts = {stats['local_ts']}")

        # Measure how far behind the tiered storage uploads are:
        # success condition should be that they are within some
        # time range of the most recently produced data
        if not stats['uploaded_ts']:
            self.logger.warn(
                "No uploaded segments found! Check test parameters!")
        else:
            self.logger.info(f"Max uploaded ts = {stats['uploaded_ts']}")

            lag_seconds = (stats['local_ts'] - stats['uploaded_ts']) / 1000.0
            config_interval = (self.params.manifest_upload_interval +
                               (self.params.segment_size / actual_byte_rate))
            self.logger.info(f"Upload lag: {lag_seconds}s, "
                             f"Upload interval: {config_interval}")
            chk.check_lag_not_exceed_configured_interval(
                lag_seconds, config_interval)

        # Wait for all uploads to complete: this should take roughly
        # segment_max_upload_interval_sec plus manifest_max_upload_interval_sec
        quiesce_uploads(self.redpanda, [self.topic],
                        timeout_sec=self.params.manifest_upload_interval +
                        self.params.segment_upload_interval +
                        self.params.grace_upload_wait_interval)

        # Check manifest upload metrics:
        #  - we should not have uploaded the manifest more times
        #    then there were manifest upload intervals in the runtime.
        self.logger.info(
            f"Upload counts: {stats['manifest_uploads']} manifests, "
            f"{stats['segment_uploads']} segments")
        chk.check_non_zero_value(stats['manifest_uploads'],
                                 value_label="manifest_uploads")
        chk.check_non_zero_value(stats['segment_uploads'],
                                 value_label="segment_uploads")

        # +3 because:
        # - 1 empty upload at start
        # - 1 extra upload from runtime % upload interval
        # - 1 extra upload after the final interval_sec driven uploads
        expect_manifest_uploads = (
            (int(produce_duration) // self.params.manifest_upload_interval) +
            3)
        self.logger.info(f"Manifests uploads: {stats['manifest_uploads']}, "
                         f"Expected not less than: {expect_manifest_uploads}")
        chk.check_expected_manifest_uploads(stats["manifest_uploads"],
                                            produce_duration)

        # Do all required checks with Redpanda active
        chk.conduct_checks()

        # Run rp_storage_tool
        self.redpanda.stop_and_scrub_object_storage(run_timeout=1800)
        # Validate checks and generate AssertionError if needed
        if chk.assert_results():
            self.logger.info(chk.get_summary_as_text())

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    # @parametrize(rolling_restarts=True)
    @parametrize(rolling_restarts=False)
    def long_retention_with_spillover_test(self, rolling_restarts):
        """
        Main test with iterative flow and rolling restar as a
        on/off handle
        """
        self._iterative_retention_flow(rolling_restarts=rolling_restarts)

    # Based on
    # https://github.com/redpanda-data/redpanda/pull/10498/commits
    @cluster(num_nodes=6,
             log_allow_list=RESTART_LOG_ALLOW_LIST +
             FAILURE_INJECTION_LOG_ALLOW_LIST)
    @parametrize(batchtypes=[batchtype_archival_meta])
    @parametrize(
        batchtypes=[batchtype_raft_data, batchtype_raft_configuration])
    def long_retention_with_spillover_and_failure_injection_test(
            self, batchtypes):
        """
        Test with faulire injections that focuses on metadata and raft
        failures
        """
        batch_types_list = [getattr(BatchType, s) for s in batchtypes]
        self._iterative_retention_flow(failure_injection_enabled=True,
                                       batchtypes=batch_types_list)

    def _iterative_retention_flow(self,
                                  failure_injection_enabled=False,
                                  batchtypes=None,
                                  rolling_restarts=False):
        """
        Main infinite retention test function.
        input:
        - failure_injection_enabled: Flag to enable failure injection
            routines
        - batchtypes: failure injection batch types to use
        - rolling_restarts: Flag that enables rolling restarts

        Notes:
        Test is using iterative approach to add data to cloud storage
        in portions and do checks in between. This is done to catch
        issues sooner than later mainly because the test could be run
        for a really long time. Test goes as follows
        - If failure injections configured, prepare it
        - Start iteration
          - create producer and random consumer
          - for every odd iteration after 2nd configure spillover values
          - Start producer and wait for 'msg_count_ack_on_start'
          - Start random consumer
          - If rolling restarts configured, check if calculated msg
            count limit not reached and start rolling
          - Once all rolling restarts finish, use simple message
            message count check on producer to detect when it is finished

            Note: not using producer wait() here as redpanda service
            in combination with failure injections blocks flow completely
            And test needs only message count

          - Stop and free producer and then consumer
            Note: Strictly no cleaning to preserve logs
          - If this is the last iteration, start GroupConsumer
            to consume all messages from start to finish
          - Conduct metrics check (self explanatory names)
          - Do storage scrub
          - If it is not the last iteration, do redpanda restart
          - If it is the last, just forcibly stop (kill) redpanda
        - End iteration, loop according to iteration count
        - Calculate all checks added to checks class
        - Log iteration timings
        - Assert results of checks
        - If unexpected scrub exceptions catched, log them all
        """

        # Spillover configuration
        def _configure_spillovers(iteration):
            """
            Function uses iteration number to configure
            additional configuration for spillovers to happen more
            """
            # all ODD iterations after 2nd
            if iteration > 2 and iteration % 2 != 0:
                # Set config for spillovers
                _r = self.rpk.cluster_config_set(
                    "cloud_storage_spillover_manifest_max_segments",
                    str(self.params.
                        cloud_storage_spillover_manifest_max_segments))
                self.logger.info(_r)
            else:
                self.rpk.cluster_config_set(
                    "cloud_storage_spillover_manifest_max_segments", "null")
            # Log current value
            _cs_spillover_mmax_seg = self.rpk.cluster_config_get(
                'cloud_storage_spillover_manifest_max_segments')
            self.logger.info(
                "Current value for 'cloud_storage_spillover_manifest_max_segments': "
                f"{_cs_spillover_mmax_seg}")

        iteration_count = self.params.total_iterations
        iteration = 1
        # prepare for rolling restarts
        if rolling_restarts:
            self.logger.info("Message count limit for rolling restarts: "
                             f"{self.params.msg_limit_rolling_restart}")

        # Do additional configuration
        self.rpk.alter_topic_config(self.topic, 'retention.local.target.bytes',
                                    self.params.local_retention_bytes)

        if failure_injection_enabled:
            self.params = self.FINJECT_PARAMS

            finject_cfg = helpers._generate_failure_injection_config(
                self.topic, self.params.partition_count, batchtypes=batchtypes)
            self.redpanda.set_up_failure_injection(
                finject_cfg=finject_cfg,
                enabled=True,
                nodes=[self.redpanda.nodes[0]],
                tolerate_crashes=True)
            RedpandaMonitor(self.test_context, self.redpanda).start()

        # Log important parameters
        self._log_parameters()

        # Prepare to save timigs
        iteration_timings = {
            "high_watermark": {},
            "produce": {},
            "manifest_uploads": {},
            "segment_uploads": {},
            "sp_mn_uploads_total": {},
            "consumed_messages": {},
            "consume": {},
            "checks": {},
            "scrub": {},
            "restart": {}
        }

        # Announce scrub_exception
        scrub_exceptions = []
        # Do iterations of producing messages
        while iteration <= iteration_count:
            self.logger.info(f"Starting iteration {iteration},"
                             f"runtime {self.params.target_runtime}s")
            # Goal of the iteration is to have simultaneous produce and
            # consume. Since producing massages need less time, there
            # should be more consumers and they should be more 'greedy'

            # Producer
            producer = self._create_producer()

            # Random consumer used in parallel with the producer
            # this will cause redpanda to occasionally load some data
            # from cloud storage
            rand_consumer = self._create_rnd_consumer()

            # We are turning on spillovers for each odd iteration
            # skipping 2nd one to generate more data
            _configure_spillovers(iteration)

            # Producer stage also involved random consumer
            # It will use all available messages on loop
            # and act like a real-life load of consuming messages
            produce_start = time.time()
            producer.start()
            # wait for messages to appear in queue
            producer.wait_for_acks(
                self.params.msg_count_ack_on_start,
                timeout_sec=self.params.producer_ack_timeout,
                backoff_sec=10)
            producer.wait_for_offset_map()
            # start random_consumer
            # do not wait for it or control anything
            rand_consumer.start(clean=False)

            if rolling_restarts:
                # Do rolling restarts in between producing
                # while there is more than ~180s left till the end
                # of producing. In most critical situation, restarts
                # might take more than 90s and last messages
                # will be processed by 2 nodes only
                while producer.produce_status.acked < \
                    self.params.msg_limit_rolling_restart:
                    self.redpanda.rolling_restart_nodes(
                        self.redpanda.nodes,
                        start_timeout=self.params.node_start_timeout,
                        stop_timeout=self.params.node_stop_timeout)
                    self.logger.info("End rolling restart cycle")
                    # Have a delay to settle down
                    time.sleep(self.params.target_stress_delay * 2)

                # waiting for cluster to be healthy
                self.logger.info("Waiting for cluster to become healthy")
                wait_until(self.redpanda.healthy,
                           timeout_sec=180,
                           backoff_sec=1)

            # wait for all desired messages to be produced
            self.logger.info("Waiting for producer to finish")
            _s = time.time()
            while producer.produce_status.acked < self.params.msg_count:
                _c = time.time()
                if _c - _s > self.params.expected_producer_duration:
                    break
                else:
                    time.sleep(10)
            # track time and log end of producing
            produce_duration = time.time() - produce_start
            self.logger.info("Done producing messages")

            # and stop producer along with consumer
            producer.stop()
            rand_consumer.stop()
            # wait for node to finish
            rand_consumer.wait_node(rand_consumer.nodes[0])
            # Cleanup
            # producer.clean()
            producer.free()
            del producer
            # rand_consumer.clean()
            rand_consumer.free()
            del rand_consumer

            # Consume messages only on last stage
            if iteration == iteration_count:
                # Consumer stage
                self.logger.info("Starting consumer stage")
                # Create consumer with no limit
                consumer = self._create_group_consumer()
                # Once again all available messages on loop will be consumed
                consume_start = time.time()
                # It is important to start clean consumer
                consumer.start(clean=False)

                if rolling_restarts:
                    # Do rolling restart
                    while consumer.consumer_status.validator.total_reads < \
                        self.params.msg_limit_rolling_restart:
                        self.redpanda.rolling_restart_nodes(
                            self.redpanda.nodes,
                            start_timeout=self.params.node_start_timeout,
                            stop_timeout=self.params.node_stop_timeout)
                        # Have a small delay to settle down
                        time.sleep(self.params.target_stress_delay * 2)

                # wait for consumer to hit end of the queue
                consumer.wait(timeout_sec=self.params.max_consumer_duration)
                consume_duration = time.time() - consume_start
                self.logger.info("Done consuming messages")
                # Stop
                consumer.stop()

                iteration_timings["consume"][iteration] = consume_duration
                iteration_timings["consumed_messages"][iteration] = \
                    consumer.consumer_status.validator.total_reads
                # Clean
                # consumer.clean()
                del consumer

            # Dict for all checks
            self.logger.info("Starting metric collection stage")
            chk = InfiniteRetentionChecks(self.params)
            checks_start = time.time()
            # Calculate actual bitrate
            actual_byte_rate = (self.params.write_bytes / produce_duration)
            mbps = int(actual_byte_rate / (1024 * 1024))
            self.logger.info(f"Produced {self.params.write_bytes} "
                             f"in {produce_duration}s, {mbps}MiB/s")
            # Add it for checking
            chk.check_byte_rate_respected(
                actual_byte_rate, var_label=f"i{iteration}_actual_byte_rate")

            # Conduct message checks
            self.logger.info(
                f"Iteration {iteration}; calculating stats for all partitions")
            stats = helpers._calculate_statistic(self.topics, self.rpk,
                                                 self.redpanda)
            # Ensure that all messages made it to topic
            self.logger.info(f"calculated hwm: {stats['hwm']}, "
                             f"message count: {self.params.msg_count}")

            chk.check_iteration_message_count(stats["hwm"], iteration)

            self.logger.info(
                f"Upload counts: {stats['manifest_uploads']} manifests, "
                f"{stats['segment_uploads']} segments")
            chk.check_non_zero_value(
                stats['manifest_uploads'],
                value_label=f"i{iteration}_manifest_uploads")
            chk.check_non_zero_value(
                stats['segment_uploads'],
                value_label=f"i{iteration}_segment_uploads")

            checks_duration = time.time() - checks_start
            self.logger.info("Done collecting metrics")

            self.logger.info("Starting object storage scrubing")
            scrub_start = time.time()
            # Run 'rp_storage_tool' to check for anomalies
            try:
                self.redpanda.stop_and_scrub_object_storage(run_timeout=1800)
            except Exception as exc:
                self.logger.warn("Exception detected while running scrub")
                scrub_exceptions += [exc]
            scrub_duration = time.time() - scrub_start
            self.logger.info("Done object storage scrub")

            iteration_timings["produce"][iteration] = produce_duration
            iteration_timings["high_watermark"][iteration] = stats['hwm']
            iteration_timings["manifest_uploads"][iteration] = stats[
                'manifest_uploads']
            iteration_timings["segment_uploads"][iteration] = stats[
                'segment_uploads']
            iteration_timings["checks"][iteration] = checks_duration
            iteration_timings["scrub"][iteration] = scrub_duration
            iteration_timings["sp_mn_uploads_total"][iteration] = stats[
                "spillover_manifest_uploads_total"]

            # No need to restart on last iteration
            if iteration != iteration_count:
                # restart nodes
                self.logger.info("Starting node restart stage")
                restart_start = time.time()
                # random_node = random.choice(self.redpanda.nodes)
                # self.redpanda.remove_local_data(random_node)

                for node in self.redpanda.nodes:
                    self.redpanda.restart_nodes([node],
                                                start_timeout=180,
                                                stop_timeout=180)

                self.redpanda._admin.await_stable_leader("controller",
                                                         partition=0,
                                                         namespace='redpanda',
                                                         timeout_s=120,
                                                         backoff_s=10)

                wait_until(lambda: len(set(self.rpk.list_topics())) == 1,
                           timeout_sec=120,
                           backoff_sec=3)

                restart_duration = time.time() - restart_start
                self.logger.info("Done node restart")
                iteration_timings["restart"][iteration] = restart_duration
            else:
                # make sure that we stop all nodes
                self.redpanda.stop(forced=True)

            self.logger.info(f"Iteration {iteration} completed. Timings are:")
            for k, v in iteration_timings.items():
                if iteration in v:
                    _val = v[iteration]
                else:
                    _val = "n/a"
                self.logger.info(f"{k} = {_val}")
            # do next iteration
            iteration += 1

        chk.conduct_checks()
        # Format timigs as table
        timigs = helpers._get_timings_table(iteration_timings)
        self.logger.info(f"All iterations done. Timings:\n{timigs}")

        # Assert checks and print summary
        if chk.assert_results():
            self.logger.info(chk.get_summary_as_text())
        for exc in scrub_exceptions:
            self.logger.info(
                "Exception during running rp-storage-tool detected")
            self.logger.error(exc)
        if len(scrub_exceptions) > 0:
            raise Exception("Scrub exeptions happen")
