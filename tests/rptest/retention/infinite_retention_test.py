# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import time

from ducktape.utils.util import wait_until
from ducktape.mark import parametrize

from rptest.retention.checks import Checks
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, \
    KgoVerifierRandomConsumer, KgoVerifierSeqConsumer, \
    KgoVerifierConsumerGroupConsumer
from rptest.services.redpanda import SISettings, MetricsEndpoint
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.utils.si_utils import BucketView, quiesce_uploads


class InfiniteRetentionTest(PreallocNodesTest):
    # Initial parameters
    segment_upload_interval = 30
    manifest_upload_interval = 10
    segment_size = 4 * 1024 * 1024  # 64 Mb
    chunk_size = segment_size * 0.75
    partition_count = 1000
    s3_topic_name = "iretention-topic"

    # Calculate other
    # bitrates
    produce_byte_rate = 100 * 1024 * 1024
    # fill the BW as much as possible
    parallel_consumers = 10
    limit_consume_rate = False
    consume_rate_mb = 50
    consume_byte_rate = consume_rate_mb * 1024 * 1024

    # Huge message size to generate a lot of data
    msg_size = 16384

    # Run an hour
    total_iterations = 8
    target_runtime = 3600
    target_stress_start = 30
    target_stress_delay = 30
    write_bytes = produce_byte_rate * target_runtime
    read_bytes = consume_byte_rate * parallel_consumers
    msg_count = write_bytes // msg_size

    # The producer should achieve throughput within this factor
    # of what we asked for: if this is violated then
    # it is something wrong with the client or test environment.
    producer_tolerance = 2
    expected_producer_duration = (write_bytes //
                                  produce_byte_rate) * producer_tolerance

    # consumers should receive at least half of the messages
    # aligned with bitrate
    consumer_tolerance = 2
    max_consumer_duration = target_runtime * consumer_tolerance * total_iterations
    # If we limit consume rate, then set message count
    # consumer_message_count = msg_count // (
    #     produce_byte_rate // consume_byte_rate) // consumer_tolerance
    consumer_message_count = None

    # Additional configs
    retention_ms = -1
    cloud_storage_spillover_manifest_max_segments = 10

    topics = (TopicSpec(retention_ms=retention_ms,
                        replication_factor=3,
                        partition_count=partition_count), )

    def __init__(self, test_context, *args, **kwargs):
        self.si_settings = SISettings(
            test_context=test_context,
            log_segment_size=self.segment_size,
        )
        kwargs['si_settings'] = self.si_settings

        # Use interval uploads so that at end of test we may do an "everything
        # was uploaded" success condition.
        kwargs['extra_rp_conf'] = {
            # We do not intend to do interval-triggered uploads during produce,
            # but we set this property so that at the end of the test we may
            # do a simple "everything was uploaded" check after the interval.
            'cloud_storage_segment_max_upload_interval_sec':
            self.segment_upload_interval,
            # The test will assert that the number of manifest uploads does
            # not exceed what we would expect based on this interval.
            'cloud_storage_manifest_max_upload_interval_sec':
            self.manifest_upload_interval,
            # Default retention is infinite
            'delete_retention_ms':
            self.retention_ms,
            # This causes spillover to hapen more often
            'cloud_storage_spillover_manifest_max_segments':
            self.cloud_storage_spillover_manifest_max_segments,
        }
        super().__init__(test_context, node_prealloc_count=2, *args, **kwargs)

    def setUp(self):
        # ensure nodes are clean before the test
        super().setUp()

    def tearDown(self):
        # stop everything
        for node in self.redpanda.nodes:
            self.redpanda.stop_node(node)
        # free nodes
        self.free_nodes()
        self.free_preallocated_nodes()
        super().tearDown()

    @staticmethod
    def _get_timings_table(timings):
        _max_index = max([max(list(v.keys())) for v in timings.values()])

        # Log pretty table of timings
        # first row
        lines = []
        line = " " * 18
        for idx in range(1, _max_index + 1):
            line += f"{idx:^14}"
        lines.append(line)
        # process rows
        for k, v in timings.items():
            line = f"{k:<18}"
            for idx in range(1, _max_index + 1):
                # In case timing not saved, use 'n/a'
                _val = v[idx] if idx in v else "n/a"
                # print use special fmt for seconds
                if k in [
                        "high_watermark", "manifest_uploads",
                        "segment_uploads", "consumed_messages"
                ] or isinstance(_val, str):
                    _t = f"{_val}"
                else:
                    _t = f"{_val:.2f}s"
                line += f"{_t:^14}"
            lines.append(line)
        return "\n".join(lines)

    @staticmethod
    def _calculate_statistic(topics, rpk, redpanda):
        # TODO: Update for multiple topics used
        _stats = {
            # Sum of all watermarks from every partition
            "hwm": 0,
            # Maximum timestamp among all partitions in a topic
            "local_ts": 0,
            # Maximum timestamp among all partitions in a bucket
            "uploaded_ts": 0,
            # (offset_delta, segment_count, last_term)
            "partition_deltas": [],
        }

        # shortcut for max from list
        def _check_and_set_max(list, key):
            if not list:
                return
            else:
                _m = max(list)
                _stats[key] = _m if _stats[key] < _m else _stats[key]

        bucket = BucketView(redpanda)

        for topic in topics:
            _lts_list = []
            _uts_list = []
            for partition in rpk.describe_topic(topic.name):
                # Add currect high watermark for topic
                _stats["hwm"] += partition.high_watermark

                # get next max timestamp fot current partition
                _lts_list += [
                    int(
                        rpk.consume(topic=topic.name,
                                    n=1,
                                    partition=partition.id,
                                    offset=partition.high_watermark - 1,
                                    format="%d\\n").strip())
                ]

                # get manifest from bucket
                _m = bucket.manifest_for_ntp(topic.name, partition.id)

                # get latest timestamp from segments in current partiton
                if _m['segments']:
                    _uts_list += [
                        max(s['max_timestamp']
                            for s in _m['segments'].values())
                    ]
                    _top_segment = list(_m['segments'].values())[-1]
                    _stats['partition_deltas'].append([
                        _m['partition'], _top_segment['delta_offset_end'],
                        len(_m['segments']), _top_segment['segment_term']
                    ])

            # get latest timestamps
            _check_and_set_max(_lts_list, "local_ts")
            _check_and_set_max(_uts_list, "uploaded_ts")

        # Uploads
        # Check manifest upload metrics:
        #  - we should not have uploaded the manifest more times
        #    then there were manifest upload intervals in the runtime.
        _stats["manifest_uploads"] = redpanda.metric_sum(
            metric_name=
            "redpanda_cloud_storage_partition_manifest_uploads_total",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        _stats["segment_uploads"] = redpanda.metric_sum(
            metric_name="redpanda_cloud_storage_segment_uploads_total",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)

        return _stats

    def metadata_readable(self, rpk):
        return next(rpk.describe_topic(
            self.topic, tolerant=True)).high_watermark is not None

    def _create_producer(self):
        return KgoVerifierProducer(self.test_context,
                                   self.redpanda,
                                   self.topic,
                                   msg_size=self.msg_size,
                                   msg_count=self.msg_count,
                                   batch_max_bytes=512 * 1024,
                                   rate_limit_bps=self.produce_byte_rate,
                                   custom_node=[self.preallocated_nodes[0]],
                                   debug_logs=True)

    def _create_rnd_consumer(self):
        return KgoVerifierRandomConsumer(self.test_context,
                                         self.redpanda,
                                         self.topic,
                                         self.msg_size,
                                         self.consumer_message_count,
                                         self.parallel_consumers,
                                         nodes=[self.preallocated_nodes[1]],
                                         debug_logs=True,
                                         trace_logs=True)

    def _create_seq_consumer(self):
        return KgoVerifierSeqConsumer(self.test_context,
                                      self.redpanda,
                                      self.topic,
                                      msg_size=self.msg_size,
                                      max_msgs=self.consumer_message_count,
                                      max_throughput_mb=self.consume_rate_mb *
                                      self.parallel_consumers,
                                      nodes=[self.preallocated_nodes[1]],
                                      debug_logs=True,
                                      trace_logs=True)

    def _create_group_consumer(self):
        return KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            self.topic,
            msg_size=self.msg_size,
            max_msgs=self.consumer_message_count,
            readers=self.parallel_consumers,
            max_throughput_mb=self.consume_rate_mb * self.parallel_consumers,
            nodes=[self.preallocated_nodes[1]],
            debug_logs=True,
            trace_logs=True)

    @cluster(num_nodes=4)
    # @parametrize(restart_stress=True)
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

        rpk = RpkTool(self.redpanda)

        producer = self._create_producer()

        self.logger.info(
            f"Producing {self.msg_count} msgs ({self.write_bytes} bytes)")
        produce_start = time.time()
        producer.start()

        if restart_stress:
            while producer.produce_status.acked < self.msg_count:
                # wait for 5 min
                time.sleep(self.target_stress_start)
                # restart one with 5 min pause
                # real world: simulates cluster update
                for node in self.redpanda.nodes:
                    time.sleep(self.target_stress_delay)
                    self.redpanda.restart_nodes([node],
                                                start_timeout=180,
                                                stop_timeout=180)

            # Wait for the cluster to recover after final restart,
            # so that subsequent post-stress success conditions
            # can count on their queries succeeding.
            self.redpanda.wait_until(self.metadata_readable(rpk),
                                     timeout_sec=60,
                                     backoff_sec=5)

        producer.wait(timeout_sec=self.expected_producer_duration)

        # calculate rate
        produce_duration = time.time() - produce_start
        actual_byte_rate = (self.write_bytes / produce_duration)
        mbps = int(actual_byte_rate / (1024 * 1024))
        self.logger.info(
            f"Produced {self.write_bytes} in {produce_duration}s, {mbps}MiB/s")

        # Dict for all checks
        chk = Checks()

        # Producer should be within a factor of two of the intended byte rate,
        # or something is wrong with the test (running on nodes that can't
        # keep up?) or with Redpanda (some instability interrupted produce?)
        chk.add_arg("actual_byte_rate", actual_byte_rate)
        chk.add_arg("produce_byte_rate", self.produce_byte_rate)
        chk.add_arg("producer_tolerance", self.producer_tolerance)
        if not restart_stress:
            chk.add_check(
                "Producer should be within a factor of two of the intended byte rate, "
                "or something is wrong with the test (running on nodes that can't "
                "keep up?) or with Redpanda (some instability interrupted produce?)",
                "actual_byte_rate > produce_byte_rate / producer_tolerance",
            )
        # Check the workload is respecting rate limit
        chk.add_check(
            "Check the workload is respecting rate limit",
            "actual_byte_rate < produce_byte_rate * producer_tolerance",
        )

        # Read the highest timestamp in local storage
        self.logger.info("Calculating stats for all partitions")
        stats = self._calculate_statistic(self.topics, rpk, self.redpanda)

        # Ensure that all messages made it to topic
        self.logger.info(
            f"calculated hwm: {stats['hwm']}, message count: {self.msg_count}")
        chk.add_arg("sum_high_watermarks", stats['hwm'])
        chk.add_arg("msg_count", self.msg_count)
        chk.add_check(
            "Ensure that all messages made it to topic",
            "sum_high_watermarks >= msg_count",
        )
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
            config_interval = (self.manifest_upload_interval +
                               (self.segment_size / actual_byte_rate))
            self.logger.info(f"Upload lag: {lag_seconds}s, "
                             f"Upload interval: {config_interval}")
            chk.add_arg("lag_seconds", lag_seconds)
            chk.add_arg("config_interval", config_interval)
            chk.add_check(
                "Measure how far behind the tiered storage uploads are: "
                "success condition should be that they are within some "
                "time range of the most recently produced data",
                "lag_seconds < config_interval",
            )

        # Wait for all uploads to complete: this should take roughly
        # segment_max_upload_interval_sec plus manifest_max_upload_interval_sec
        quiesce_uploads(self.redpanda, [self.topic],
                        timeout_sec=self.manifest_upload_interval +
                        self.segment_upload_interval)

        # Check manifest upload metrics:
        #  - we should not have uploaded the manifest more times
        #    then there were manifest upload intervals in the runtime.
        self.logger.info(
            f"Upload counts: {stats['manifest_uploads']} manifests, "
            f"{stats['segment_uploads']} segments")
        chk.add_arg("manifest_uploads", stats['manifest_uploads'])
        chk.add_arg("segment_uploads", stats['segment_uploads'])
        chk.add_check(
            "Non-zero manifest uploads",
            "manifest_uploads > 0",
        )
        chk.add_check(
            "Non-zero segment uploads",
            "segment_uploads > 0",
        )

        # Check the kafka-raft offset delta: this is an indication of how
        # many extra raft records we wrote for archival_metadata_stm.
        #
        # At the maximum, this should be:
        # - 1 per segment (although could be as low as 1 for every four
        #   segments due to ntp_archiver::_concurrency)
        # - 1 per upload round (i.e. per 1-4 segments) for marking the manifest
        #   clean.
        # - 1 per raft term, for raft config batches written by new leaders
        #
        # This helps to assure us that ntp_archiver and archival_metadata_stm
        # are doing an efficient job of keeping the state machine up to date.

        # Turned off as per discussion with Andrew W.
        # for p in stats["partition_deltas"]:
        #     chk.add_arg(f"p{p[0]}_offset_delta", p[1])
        #     chk.add_arg(f"p{p[0]}_segment_count", p[2])
        #     chk.add_arg(f"p{p[0]}_last_term", p[3])
        #     chk.add_check(
        #         "Partition offset delta should be less than doubled "
        #         "segment count added by last term",
        #         f"p{p[0]}_offset_delta <= (2 * p{p[0]}_segment_count + p{p[0]}_last_term)",
        #         show=False
        #     )

        # +3 because:
        # - 1 empty upload at start
        # - 1 extra upload from runtime % upload interval
        # - 1 extra upload after the final interval_sec driven uploads
        expect_manifest_uploads = (
            (int(produce_duration) // self.manifest_upload_interval) + 3)
        self.logger.info(f"Manifests uploads: {stats['manifest_uploads']}, "
                         f"Expected not less than: {expect_manifest_uploads}")
        chk.add_arg("produce_duration", int(produce_duration))
        chk.add_arg("manifest_upload_interval", self.manifest_upload_interval)
        chk.add_check(
            "For spillover active tests manifest uploads should be "
            "significant",
            "manifest_uploads > produce_duration // manifest_upload_interval + 3",
        )
        # Do all required checks with Redpanda active
        chk.conduct_checks()
        # Run rp_storage_tool
        self.redpanda.stop_and_scrub_object_storage()
        # Validate checks and generate AssertionError if needed
        if chk.assert_results():
            self.logger.info(chk.get_summary_as_text())

    @cluster(num_nodes=6)
    def long_retention_with_spillover_test(self):
        rpk = RpkTool(self.redpanda)
        iteration_count = self.total_iterations
        iteration = 1

        self.logger.info(
            "Calculated parameters:\n"
            f"segment_upload_interval = {self.segment_upload_interval}\n"
            f"manifest_upload_interval = {self.manifest_upload_interval}\n"
            f"segment_size = {self.segment_size}\n"
            f"chunk_size = {self.chunk_size}\n"
            f"partition_count = {self.partition_count}\n"
            f"msg_size = {self.msg_size}\n"
            f"msg_count = {self.msg_count}\n"
            f"producer_rate = {self.produce_byte_rate/1024/1024}MB\n"
            f"producer_tolerance = {self.producer_tolerance}\n"
            f"consumer_rate = {self.consume_rate_mb}MB * {self.parallel_consumers}\n"
            f"consumer_tolerance = {self.consumer_tolerance}\n"
            f"parallel consumers = {self.parallel_consumers}\n"
            f"runtime = {self.target_runtime}s\n"
            f"mb_written = {self.write_bytes/1024/1024}MB\n"
            f"max_producer_duration = {self.expected_producer_duration}s\n"
            f"max_consumer_duration = {self.max_consumer_duration}s * {iteration_count}\n"
        )

        # Create consumer with no limit
        consumer = self._create_group_consumer()
        # SeqConsumer (broken)
        # consumer = self._create_seq_consumer()
        # # Start it in a clean node
        # consumer.start()

        # Producer
        producer = self._create_producer()

        # Prepare to save timigs
        iteration_timings = {
            "high_watermark": {},
            "produce": {},
            "manifest_uploads": {},
            "segment_uploads": {},
            "consumed_messages": {},
            "consume": {},
            "checks": {},
            "scrub": {},
            "restart": {}
        }

        # Announce scrub_exception
        scrub_exception = None
        # Do iterations of producing messages
        while iteration < iteration_count:
            self.logger.info(
                f"Starting iteration {iteration}, runtime {self.target_runtime}s"
            )
            # Consumer are to be created each time and released at the end

            # rand_consumer = self._create_rnd_consumer()
            # consumer = self._create_seq_consumer()
            # consumer = self._create_group_consumer()

            # Producer
            produce_start = time.time()
            producer.start()
            producer.wait_for_acks(1000, timeout_sec=60, backoff_sec=10)
            producer.wait_for_offset_map()
            # wait and stop
            producer.wait(timeout_sec=self.expected_producer_duration)
            produce_duration = time.time() - produce_start
            producer.stop()
            # Timing
            self.logger.info("Done producing messages")

            # Dict for all checks
            chk = Checks()
            checks_start = time.time()
            # Calculate actual bitrate
            actual_byte_rate = (self.write_bytes / produce_duration)
            mbps = int(actual_byte_rate / (1024 * 1024))
            self.logger.info(
                f"Produced {self.write_bytes} in {produce_duration}s, {mbps}MiB/s"
            )
            # Add it for checking
            chk.add_arg(f"i{iteration}_actual_byte_rate", actual_byte_rate)
            chk.add_arg("produce_byte_rate", self.produce_byte_rate)
            chk.add_arg("producer_tolerance", self.producer_tolerance)
            chk.add_check(
                "Check the workload is respecting rate limit",
                f"i{iteration}_actual_byte_rate < produce_byte_rate * producer_tolerance",
            )

            # Conduct message checks
            self.logger.info(
                f"Iteration {iteration}; calculating stats for all partitions")
            stats = self._calculate_statistic(self.topics, rpk, self.redpanda)
            # Ensure that all messages made it to topic
            self.logger.info(
                f"calculated hwm: {stats['hwm']}, message count: {self.msg_count}"
            )
            chk.add_arg(f"i{iteration}_sum_high_watermarks", stats['hwm'])
            chk.add_arg(f"i{iteration}_msg_count", self.msg_count * iteration)
            chk.add_check(
                "Ensure that all messages made it to topic",
                f"i{iteration}_sum_high_watermarks >= i{iteration}_msg_count",
            )
            self.logger.info(
                f"Upload counts: {stats['manifest_uploads']} manifests, "
                f"{stats['segment_uploads']} segments")
            chk.add_arg(f"i{iteration}_manifest_uploads",
                        stats['manifest_uploads'])
            chk.add_arg(f"i{iteration}_segment_uploads",
                        stats['segment_uploads'])
            chk.add_check(
                f"Iteration {iteration}; non-zero manifest uploads",
                f"i{iteration}_manifest_uploads > 0",
            )
            chk.add_check(
                f"Iteration {iteration}; non-zero segment uploads",
                f"i{iteration}_segment_uploads > 0",
            )
            checks_duration = time.time() - checks_start
            scrub_start = time.time()
            # Run 'rp_storage_tool' to check for anomalies
            try:
                self.redpanda.stop_and_scrub_object_storage(run_timeout=1800)
            except RuntimeError as exc:
                scrub_exception = exc
            scrub_duration = time.time() - scrub_start

            iteration_timings["produce"][iteration] = produce_duration
            iteration_timings["high_watermark"][iteration] = stats['hwm']
            iteration_timings["manifest_uploads"][iteration] = stats[
                'manifest_uploads']
            iteration_timings["segment_uploads"][iteration] = stats[
                'segment_uploads']
            iteration_timings["checks"][iteration] = checks_duration
            iteration_timings["scrub"][iteration] = scrub_duration

            # restart nodes
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

            wait_until(lambda: len(set(rpk.list_topics())) == 1,
                       timeout_sec=120,
                       backoff_sec=3)

            restart_duration = time.time() - restart_start
            iteration_timings["restart"][iteration] = restart_duration

            self.logger.info(f"Iteration {iteration} completed. Timings are:")
            for k, v in iteration_timings.items():
                if iteration in v:
                    _val = v[iteration]
                else:
                    _val = "n/a"
                self.logger.info(f"{k} = {_val}")
            if scrub_exception:
                break
            else:
                # do next iteration
                iteration += 1

        # Last iteration is consuming messages only
        # Consumer
        consume_start = time.time()
        consumer.start()
        # make sure that we get at least one status read befor polling
        time.sleep(5)
        # Consumer will work with all messages in a queue
        # So, it will take huge amount of time
        consumer.wait(timeout_sec=self.max_consumer_duration)
        consume_duration = time.time() - consume_start
        consumer.stop()
        self.logger.info("Done consuming messages")

        # Timings
        stats = self._calculate_statistic(self.topics, rpk, self.redpanda)
        iteration_timings["high_watermark"][iteration] = stats['hwm']
        iteration_timings["manifest_uploads"][iteration] = stats[
            'manifest_uploads']
        iteration_timings["segment_uploads"][iteration] = stats[
            'segment_uploads']
        iteration_timings["consume"][iteration] = consume_duration
        iteration_timings["consumed_messages"][iteration] = \
            consumer.consumer_status.validator.total_reads

        # Cleanup consumer
        # consumer.stop()
        consumer.free()

        chk.conduct_checks()
        # Format timigs as table
        timigs = self._get_timings_table(iteration_timings)
        # Assert checks and print summary
        if chk.assert_results():
            self.logger.info(chk.get_summary_as_text())
        if scrub_exception:
            self.logger.info("rp-storage-tool detected fatal anomaly "
                             f"in iteration {iteration}")
            raise scrub_exception
        self.logger.info(f"All iterations done. Timings:\n{timigs}")
