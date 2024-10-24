# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import dataclasses
import json
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, Future
from threading import Condition
from collections import defaultdict
from typing import List

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.action_injector import random_process_kills
from rptest.services.redpanda import RedpandaService
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer, KgoVerifierRandomConsumer, KgoVerifierSeqConsumer
from rptest.services.redpanda import SISettings, CloudStorageTypeAndUrlStyle, get_cloud_storage_type, get_cloud_storage_type_and_url_style, make_redpanda_service, CHAOS_LOG_ALLOW_LIST, MetricsEndpoint
from rptest.utils.mode_checks import skip_fips_mode
from rptest.utils.si_utils import nodes_report_cloud_segments, BucketView, NTP
from rptest.tests.tiered_storage_model import TestCase, TieredStorageEndToEndTest, get_tiered_storage_test_cases, TestRunStage, CONFIDENCE_THRESHOLD, get_test_case_from_name

MAX_RETRIES = 20


class TieredStorageTest(TieredStorageEndToEndTest, RedpandaTest):

    # Topic doesn't have tiered storage enabled by default
    topics = (
        TopicSpec(
            partition_count=1,  # TODO: maybe use more partitions
            replication_factor=3,
            retention_bytes=-1,
            retention_ms=-1,
        ), )

    def __init__(self, test_context, extra_rp_conf=None, environment=None):

        self.test_context = test_context

        # The settings will be changed in the 'setUp' method
        self.extra_rp_conf = extra_rp_conf or {}

        self.extra_rp_conf['cloud_storage_enable_segment_merging'] = False
        self.extra_rp_conf['compaction_ctrl_min_shares'] = 300
        self.extra_rp_conf['cloud_storage_disable_chunk_reads'] = True
        self.extra_rp_conf['cloud_storage_spillover_manifest_size'] = None

        super(TieredStorageTest, self).__init__(
            test_context=test_context,
            extra_rp_conf=self.extra_rp_conf,
            environment=environment,
            log_level="trace",
            si_settings=SISettings(
                test_context,
                cloud_storage_max_connections=10,
                log_segment_size=1024 * 1024,  # 1MB
                fast_uploads=True,
            ))

        self.s3_bucket_name = self.si_settings.cloud_storage_bucket

        # The producer has some defaults which could be changed later
        self.producer_config = dict(context=self.test_context,
                                    redpanda=self.redpanda,
                                    topic=self.topic,
                                    msg_size=1024,
                                    msg_count=20000,
                                    use_transactions=False,
                                    msgs_per_transaction=10,
                                    debug_logs=True,
                                    trace_logs=True)

        # Configurable defaults for the consumer
        self.consumer_config = dict(context=self.test_context,
                                    redpanda=self.redpanda,
                                    topic=self.topic,
                                    msg_size=None,
                                    debug_logs=True,
                                    trace_logs=True)
        self.timequery_map = {}

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        self.kafka_cat = KafkaCat(self.redpanda)
        self.thread_pool = ThreadPoolExecutor(max_workers=1024)
        self.public_metrics = defaultdict(int)
        self.private_metrics = defaultdict(int)
        self.cond_stop = Condition()
        self.stop_flag = False
        self._log_patterns = defaultdict(list)
        self.config_overrides = {}
        self.current_stage = TestRunStage.Startup
        self.bucket_view = None

    def setUp(self):
        super(TieredStorageTest, self).setUp()

    def _build_timequery_map(self, num_runs, fake_ts):
        """Build a map used by timequery check"""
        # TODO: add compaction/retention support
        # this code assumes that all offsets will be available
        # but this is not the case if retention or compaction (or both) is enabled
        if not fake_ts:
            self.logger.info(f"Timequery check is disabled")
            return
        fake_ts_step = self.producer_config.get('fake_timestamp_step_ms', 1000)
        num_messages = self.producer_config.get('msg_count', 10000)
        # use reservoir sampling to sample timestamps
        map_size = 100
        reservoir = [(fake_ts + i * fake_ts_step, i)
                     for i in range(0, map_size)]
        for i in range(map_size, num_runs * num_messages):
            x = random.randint(0, i)
            if x < map_size:
                reservoir[x] = (fake_ts + i * fake_ts_step, i)
        for ts, i in reservoir:
            self.timequery_map[ts] = i
        self.logger.info(
            f"Timequery map generated, size {len(self.timequery_map)}, number of produce runs: {num_runs}, fake_ts: {fake_ts}"
        )
        for ts, i in self.timequery_map.items():
            self.logger.debug(f"Timequery map: {ts} -> {i}")

    def _bg_loop(self, func, name, sleep_time=lambda: 1.0):
        """Run function in a loop in the background"""
        if self.stop_flag:
            self.logger.info(f"BG loop for {name} is stopped")
            return

        func()

        def recur(res: Future):
            if res.exception():
                # Keep retrying even in case of error
                self.logger.error(
                    f"Exception in the {name} background loop {res.exception()}"
                )

            self._bg_loop(func, name, sleep_time)

        def pause(res: Future):
            if res.exception():
                self.logger.error(
                    f"Exception in the {name} background loop {res.exception()}"
                )

            time.sleep(sleep_time())

        fut = self.thread_pool.submit(func)
        fut.add_done_callback(pause)
        fut.add_done_callback(recur)

    def _bg_validator_run(self, validator):
        """Run validator in background."""
        def invoke():
            try:
                self.logger.info(f"Running validator {validator.name()}")
                validator.run(self)
            except:
                self.logger.error("Failed to run validator", exc_info=True)

        self._bg_loop(invoke, validator.name(),
                      lambda: random.uniform(0.5, 1.5))

    def run_bg_validators(self, test_case):
        """Run all validators in background."""
        # Start all bg-loops which are needed to run validators
        self._bg_grep_logs()
        self._bg_metrics_pull(MetricsEndpoint.PUBLIC_METRICS)
        self._bg_metrics_pull(MetricsEndpoint.METRICS)
        self._bg_bucket_view_update()
        # Start validators running in the background
        for v in test_case.validators():
            self._bg_validator_run(v)

    def _bg_metrics_pull(self, endpoint):
        """Pull metrics from the redpanda node. This is needed to avoid pulling metrics
        from every validator individually. The validators are invoking functions of the
        test suite to find individual metrics."""
        try:
            if self.stop_flag:
                return
            res = defaultdict(int)
            for n in self.redpanda.nodes:
                if n in self.redpanda._started:
                    resp = self.redpanda.metrics(n, endpoint)
                    for family in resp:
                        for sample in family.samples:
                            res[sample.name] += sample.value
            # should be safe due to GIL
            if endpoint == MetricsEndpoint.PUBLIC_METRICS:
                self.public_metrics = res
                self.logger.info(f"Public metrics {res}")
            else:
                self.private_metrics = res
                self.logger.info(f"Private metrics {res}")
            time.sleep(2)
        except:
            self.logger.error("Failed to pull metrics", exc_info=True)
            time.sleep(2)
        finally:
            # Run 'loop' recursively
            if not self.stop_flag:
                self.thread_pool.submit(self._bg_metrics_pull, endpoint)

    def _bg_bucket_view_update(self):
        """Re-scan the bucket view in the background."""
        try:
            if self.stop_flag:
                return
            self.bucket_view = BucketView(self.redpanda,
                                          topics=self.topics,
                                          scan_segments=True)
            self.bucket_view._ensure_listing()
            self.logger.info(
                f"Bucket view updated, {self.bucket_view.segment_objects} segments scanned"
            )
            time.sleep(10)
        except:
            self.logger.error("Failed to scan bucket", exc_info=True)
            time.sleep(20)
        finally:
            if not self.stop_flag:
                self.thread_pool.submit(self._bg_bucket_view_update)

    def _bg_grep_logs(self):
        if self.stop_flag:
            return

        def tail_minus_f(node):
            self.logger.info(f"Running tail -f on {node.name}")
            try:
                # Redpanda produces fairly minor amount of logs, so it's OK to just
                # grep all of them.

                for line in node.account.ssh_capture(
                        f"tail -f {RedpandaService.STDOUT_STDERR_CAPTURE}"):
                    for pattern, validators in self._log_patterns.items():
                        if re.search(pattern, line):
                            for v in validators:
                                v.on_match(node.name, line)
                    if self.stop_flag:
                        return
            except:
                self.logger.error("Failed to grep logs", exc_info=True)

        for n in self.redpanda.nodes:
            node = n
            self.logger.info(f"Starting log fetch on a node {node.name}")
            self.thread_pool.submit(lambda: tail_minus_f(node))

    def tearDown(self):
        self.stop_flag = True
        self.thread_pool.shutdown()

    def apply_config_overrides(self):
        """Apply configuration overrides to the redpanda cluster."""
        self.logger.info(f"Applying config overrides {self.config_overrides}")
        needs_restart = False
        values = {}
        for k, v in self.config_overrides.items():
            values[k] = v['value']
            if v['needs_restart']:
                needs_restart = True
        if len(values) > 0:
            self.redpanda.set_cluster_config(values=values,
                                             expect_restart=needs_restart)
        self.config_overrides = {}

    # TieredStorageEndToEnd interface implementation

    def get_bucket_view(self):
        return self.bucket_view

    def get_ntp(self):
        return NTP("kafka", self.topic, 0)

    def set_producer_parameters(self, **kwargs):
        self.logger.info(f"Update producer parameters {kwargs}")
        self.producer_config.update(**kwargs)

    def set_consumer_parameters(self, **kwargs):
        self.logger.info(f"Update consumer parameters {kwargs}")
        self.consumer_config.update(**kwargs)

    def subscribe_to_logs(self, validator, pattern):
        """Subscribe to all log changes on all nodes. The validator will be invoked
        when the log line is matched."""
        self._log_patterns[pattern].append(validator)

    def alter_topic_config(self, config_name: str, config_value: str):
        self.logger.info(
            f"Alter topic config for {self.topic}, setting {config_name} to {config_value}"
        )
        self.rpk.alter_topic_config(self.topic, config_name, config_value)

    def get_redpanda_service(self):
        return self.redpanda

    def get_logger(self):
        return self.logger

    def set_redpanda_cluster_config(self,
                                    config_name,
                                    config_value,
                                    needs_restart: bool = False):
        """Override configuration of the redpanda service. The configuration will be changed
        later using the self.redpanda.set_cluster_config method. This method shouldn't be
        called for the same config name twice."""
        self.config_overrides[config_name] = dict(value=config_value,
                                                  needs_restart=needs_restart)

    def stop_redpanda_cluster(self):
        self.redpanda.stop()

    def get_public_metric(self, metric_name: str):
        value = self.public_metrics.get(metric_name)
        self.logger.info(f"{metric_name} = {value}")
        return value

    def get_private_metric(self, metric_name: str):
        value = self.private_metrics.get(metric_name)
        self.logger.info(f"{metric_name} = {value}")
        return value

    def _oneshot_produce(self, **kwargs):
        KgoVerifierProducer.oneshot(**self.producer_config)

    def _oneshot_consume(self, **kwargs):
        KgoVerifierSeqConsumer.oneshot(**self.consumer_config)

    def produce_until_validated(self, test_case):
        # If fake timestamps are enabled, we need to adjust the timestamp on every
        # iteration to make them continuous.
        fake_ts_enabled = self.producer_config.get(
            'fake_timestamp_ms') is not None
        fake_ts = self.producer_config.get('fake_timestamp_ms', 0)
        original_fake_ts = fake_ts
        fake_ts_step = self.producer_config.get('fake_timestamp_step_ms', 1000)
        num_messages = self.producer_config.get('msg_count', 10000)
        num_produce_runs = 0
        done = 0
        total = 0
        failed_validators = []
        for _ in range(0, MAX_RETRIES):
            lagging_validators = []
            self.logger.debug(f"Producer config: {self.producer_config}")
            self._oneshot_produce()
            num_produce_runs += 1
            done = 0
            total = 0
            for v in test_case.validators():
                if v.need_to_run(TestRunStage.Produce):
                    self.logger.debug(
                        f"Produce stage validator {v.name()} has result {v.get_result()} with {v.get_confidence()} confidence"
                    )
                    total += 1
                    success = v.get_result() and v.get_confidence(
                    ) > CONFIDENCE_THRESHOLD
                    if success:
                        done += 1
                    else:
                        lagging_validators.append(v.name())
            # Stop if all validators are confident enough
            if done == total:
                break
            else:
                self.logger.info(
                    f"Produce delayed by lagging validators: {lagging_validators}"
                )
                failed_validators = lagging_validators
                time.sleep(5)

            if fake_ts_enabled:
                fake_ts += fake_ts_step * num_messages
                self.producer_config['fake_timestamp_ms'] = fake_ts

            # Apply other inputs
            self.run_stage_inputs(TestRunStage.Produce, test_case)

        assert done == total, f"Failed to produce data after {num_produce_runs} runs, {failed_validators} validators failed"
        self._build_timequery_map(num_produce_runs, original_fake_ts)

    def clean_up_cache(self):
        for node in self.redpanda.nodes:
            path = self.redpanda.cache_dir + "/*"
            self.logger.info(f"removing all files in {path}")
            for line in node.account.ssh_capture(f"rm -rf  {path}"):
                self.logger.info(f"rm command returned {line}")

    def consume_until_validated(self, test_case):
        num_consume_runs = 0
        failed_validators = []
        for _ in range(0, MAX_RETRIES):
            self._oneshot_consume()
            self._timequery_consume()
            num_consume_runs += 1
            done = 0
            total = 0
            lagging_validators = []
            for v in test_case.validators():
                if v.need_to_run(TestRunStage.Consume):
                    self.logger.debug(
                        f"Consume stage validator {v.name()} has confidence {v.get_confidence()}"
                    )
                    total += 1
                    r = v.get_result()
                    c = v.get_confidence() > CONFIDENCE_THRESHOLD
                    if r and c:
                        done += 1
                    else:
                        lagging_validators.append(v.name())
            if done == total:
                break
            else:
                self.logger.info(
                    f"Consume delayed by lagging validators: {lagging_validators}"
                )
                failed_validators = lagging_validators
                time.sleep(5)

            # Apply inputs if needed
            self.run_stage_inputs(TestRunStage.Consume, test_case)

        assert done == total, f"Failed to consume data after {num_consume_runs} runs, {failed_validators} validators failed"
        return num_consume_runs

    def _timequery_consume(self):
        self.logger.info(
            f"Running timequery check, map size {len(self.timequery_map)}")
        if len(self.timequery_map) == 0:
            return
        for ts, expected_offset in self.timequery_map.items():
            self.logger.debug(
                f"Timequery check for timestamp: {ts}, expected offset: {expected_offset}"
            )
            actual_offset = self.kafka_cat.consume_one(
                self.topic, 0, first_timestamp=ts)['offset']
            matches = expected_offset == actual_offset
            if not matches:
                self.logger.error(
                    f"Timequery mismatch: expected {expected_offset} but got {actual_offset} for timestamp {ts}"
                )
            assert matches, "Timequery mismatch"

    def start_validators(self, test_case):
        for v in test_case.validators():
            self.logger.info(f"Starting validator {v.name()}")
            v.start(self)

    def transfer_topic_leadership(self):
        leader = self.redpanda._admin.get_partition_leader(namespace='kafka',
                                                           topic=self.topic,
                                                           partition=0)
        broker_ids = [x['node_id'] for x in self.redpanda._admin.get_brokers()]
        transfer_to = random.choice([n for n in broker_ids if n != leader])
        self.redpanda._admin.transfer_leadership_to(namespace="kafka",
                                                    topic=self.topic,
                                                    partition=0,
                                                    target_id=transfer_to,
                                                    leader_id=leader)
        self.redpanda._admin.await_stable_leader(
            self.topic,
            partition=0,
            namespace='kafka',
            timeout_s=30,
            backoff_s=2,
            check=lambda node_id: node_id == transfer_to)

    def run_stage_validators(self, stage, test_case):
        """Run all validators for the given stage."""
        failed_validators = []
        for ix in range(0, MAX_RETRIES):
            done = 0
            total = 0
            lagging_validators = []
            for v in test_case.validators():
                if v.need_to_run(stage):
                    self.logger.debug(
                        f"{stage} stage validator {v.name()} has result {v.get_result()} with {v.get_confidence()} confidence"
                    )
                    total += 1
                    r = v.get_result()
                    c = v.get_confidence() > CONFIDENCE_THRESHOLD
                    if r and c:
                        done += 1
                    else:
                        lagging_validators.append(v.name())
            # Stop if all validators are confident enough
            if done == total:
                break
            else:
                self.logger.info(
                    f"{stage} delayed by lagging validators: {lagging_validators}"
                )
                failed_validators = lagging_validators
                if ix % 2 == 0:
                    # Clean up cache on all nodes to force segment hydrations
                    time.sleep(60)  # Wait STM max eviction time
                    self.clean_up_cache()
                else:
                    # Force leadership transfer
                    self.transfer_topic_leadership()

        assert done == total, f"Failed to validate data after {MAX_RETRIES} runs, {failed_validators} validators failed"

    def start_inputs(self, test_case):
        """Start all inputs."""
        for inp in test_case.inputs():
            self.logger.info(f"Starting input {inp.name()}")
            inp.start(self)
        self.apply_config_overrides()

    def run_stage_inputs(self, stage, test_case):
        """Run all inputs for the given stage."""
        self.logger.info(f"Running input overrides for stage {stage}")
        for inp in test_case.inputs():
            if inp.need_to_run(stage):
                self.logger.info(f"Running input overrides {inp.name()}")
                inp.run(self)
        self.apply_config_overrides()

    def prepare_stage(self, stage, test_case):
        """Set the current stage and run all inputs for the given stage."""
        self.logger.info(f"Setting stage to {stage} and running inputs")
        self.current_stage = stage
        self.run_stage_inputs(stage, test_case)

    def shutdown_bg_tasks(self):
        self.logger.info(f"Shutting down background tasks")
        self.stop_flag = True
        self.thread_pool.shutdown()

    # fips on S3 is not compatible with path-style urls. TODO remove this once get_cloud_storage_type_and_url_style is fips aware
    @skip_fips_mode
    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type_and_url_style=get_cloud_storage_type_and_url_style(
        ),
        test_case=get_tiered_storage_test_cases(fast_run=True))
    def test_tiered_storage(self, cloud_storage_type_and_url_style: List[
        CloudStorageTypeAndUrlStyle], test_case: TestCase):
        """This is a main entry point of the test.
           The test runs if 5 phases:
            - Configuration phase
            - Producing the data
            - Intermediate step (change topic properties, wait for retention, etc)
            - Consuming the data (use timequery)
            - Shutting down
        """
        if not isinstance(test_case, TestCase):
            test_case_name = json.loads(test_case)["name"]
            test_case = get_test_case_from_name(test_case_name)
            assert test_case is not None, f"no test case found with name {test_case_name}"
        # Configuration phase
        self.start_inputs(test_case)
        self.start_validators(test_case)
        self.run_bg_validators(test_case)

        # Setup stage
        self.prepare_stage(TestRunStage.Startup, test_case)
        self.run_stage_validators(TestRunStage.Startup, test_case)

        # Producing the data
        self.prepare_stage(TestRunStage.Produce, test_case)
        self.produce_until_validated(test_case)

        # Intermediate step
        self.prepare_stage(TestRunStage.Intermediate, test_case)
        self.run_stage_validators(TestRunStage.Intermediate, test_case)

        # Consuming the data
        self.prepare_stage(TestRunStage.Consume, test_case)
        self.consume_until_validated(test_case)

        # Shutting down
        self.prepare_stage(TestRunStage.Shutdown, test_case)
        self.run_stage_validators(TestRunStage.Shutdown, test_case)

        self.shutdown_bg_tasks()

        # Check that all validators are done and raise error if
        # some of them failed.
        test_case.assert_validators(self)
