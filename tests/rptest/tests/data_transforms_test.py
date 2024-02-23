# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import typing
import time
import random
import json

from requests.exceptions import RequestException

from ducktape.mark import matrix
from rptest.clients.rpk import RpkException, RpkTool
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.services.transform_verifier_service import TransformVerifierProduceConfig, TransformVerifierProduceStatus, TransformVerifierService, TransformVerifierConsumeConfig, TransformVerifierConsumeStatus
from rptest.services.admin import Admin

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.tests.cluster_config_test import wait_for_version_sync
from rptest.utils.utf8 import CONTROL_CHARS_VALS, generate_string_with_control_character
from rptest.util import expect_exception


class WasmException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class BaseDataTransformsTest(RedpandaTest):
    """
    A base test class providing helpful data transform related functionality.
    """
    def __init__(self, *args, **kwargs):
        extra_rp_conf = {
            'data_transforms_enabled': True,
        } | kwargs.pop('extra_rp_conf', {})
        super(BaseDataTransformsTest,
              self).__init__(*args, extra_rp_conf=extra_rp_conf, **kwargs)
        self._rpk = RpkTool(self.redpanda)

    def _deploy_wasm(self,
                     name: str,
                     input_topic: TopicSpec,
                     output_topic: TopicSpec,
                     file="tinygo/identity.wasm"):
        """
        Deploy a wasm transform and wait for all processors to be running.
        """
        def do_deploy():
            self._rpk.deploy_wasm(name,
                                  input_topic.name,
                                  output_topic.name,
                                  file=file)
            return True

        wait_until(
            do_deploy,
            timeout_sec=30,
            backoff_sec=5,
            err_msg=f"unable to deploy wasm transform {name}",
            retry_on_exc=True,
        )

        def is_all_running():
            transforms = self._rpk.list_wasm()
            transform = next((x for x in transforms if x.name == name), None)
            if not transform:
                raise WasmException(f"missing transform {name} from report")
            for partition_id in range(0, input_topic.partition_count):
                processor = next(
                    (p
                     for p in transform.status if p.partition == partition_id),
                    None)
                if not processor:
                    raise WasmException(
                        f"missing processor {name}/{partition_id} from report")
                if processor.status != "running":
                    raise WasmException(
                        f"processor {name}/{partition_id} is not running: {processor.status}"
                    )
            return True

        wait_until(
            is_all_running,
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"wasm transform {name} did not reach steady state",
            retry_on_exc=True,
        )

    def _delete_wasm(self, name: str):
        """
        Delete a wasm transform and wait for all processors to stop.
        """
        def do_delete():
            self._rpk.delete_wasm(name)
            return True

        wait_until(
            do_delete,
            timeout_sec=30,
            backoff_sec=5,
            err_msg=f"unable to delete wasm transform {name}",
            retry_on_exc=True,
        )

        def transform_is_gone():
            transforms = self._rpk.list_wasm()
            transform = next((x for x in transforms if x.name == name), None)
            if transform:
                raise WasmException(f"transform {name} still in report")
            return True

        wait_until(
            transform_is_gone,
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"wasm transform {name} was not fully shutdown",
            retry_on_exc=True,
        )

    def _produce_input_topic(
            self,
            topic: TopicSpec,
            transactional: bool = False) -> TransformVerifierProduceStatus:
        status = TransformVerifierService.oneshot(
            context=self.test_context,
            redpanda=self.redpanda,
            config=TransformVerifierProduceConfig(
                bytes_per_second='256KB',
                max_batch_size='64KB',
                max_bytes='1MB',
                message_size='1KB',
                topic=topic.name,
                transactional=transactional,
            ))
        return typing.cast(TransformVerifierProduceStatus, status)

    def _consume_output_topic(
            self,
            topic: TopicSpec,
            status: TransformVerifierProduceStatus,
            timeout_sec=10) -> TransformVerifierConsumeStatus:
        result = TransformVerifierService.oneshot(
            context=self.test_context,
            redpanda=self.redpanda,
            config=TransformVerifierConsumeConfig(
                topic=topic.name,
                bytes_per_second='1MB',
                validate=status,
            ),
            timeout_sec=timeout_sec)
        return typing.cast(TransformVerifierConsumeStatus, result)

    def _deploy_invalid(self, file: str, expected_msg: str):
        def do_deploy():
            try:
                self._rpk.deploy_wasm("invalid",
                                      self.topics[0].name,
                                      self.topics[1].name,
                                      file=file)
                raise AssertionError(
                    "Unexpectedly was able to deploy transform")
            except RpkException as e:
                if expected_msg in e.stdout or expected_msg in e.stderr:
                    return True
                # Could be a flaky thing that needs to be retried due to network, etc.
                return False

        wait_until(
            do_deploy,
            timeout_sec=30,
            backoff_sec=5,
            err_msg=f"unable to deploy invalid wasm transform",
        )


class DataTransformsTest(BaseDataTransformsTest):
    """
    Tests related to WebAssembly powered data transforms
    """
    topics = [TopicSpec(partition_count=9), TopicSpec(partition_count=9)]

    @cluster(num_nodes=3)
    def test_lifecycle(self):
        """
        Test that a Wasm binary can be deployed, reach steady state and then be deleted.
        """
        self._deploy_wasm(name="identity-xform",
                          input_topic=self.topics[0],
                          output_topic=self.topics[1])
        self._delete_wasm(name="identity-xform")

    @cluster(num_nodes=4)
    @matrix(transactional=[False, True])
    def test_identity(self, transactional):
        """
        Test that a transform that only copies records from the input to the output topic works as intended.
        """
        input_topic = self.topics[0]
        output_topic = self.topics[1]
        self._deploy_wasm(name="identity-xform",
                          input_topic=input_topic,
                          output_topic=output_topic)
        producer_status = self._produce_input_topic(
            topic=self.topics[0], transactional=transactional)
        consumer_status = self._consume_output_topic(topic=self.topics[1],
                                                     status=producer_status)
        self.logger.info(f"{consumer_status}")
        assert consumer_status.invalid_records == 0, f"transform verification failed with invalid records: {consumer_status}"

    @cluster(num_nodes=3)
    def test_validation(self):
        """
        Test that invalid wasm binaries are validated at deploy time.
        """
        self._deploy_invalid(file="validation/garbage.wasm",
                             expected_msg="invalid WebAssembly module")
        self._deploy_invalid(file="validation/add_two.wasm",
                             expected_msg="missing required WASI functions")
        self._deploy_invalid(
            file="validation/wasi.wasm",
            expected_msg=
            "Does the broker support this version of the Data Transforms SDK?")


class DataTransformsChainingTest(BaseDataTransformsTest):
    """
    Tests related to WebAssembly powered data transforms that are chained together, it is possible to create a full DAG.
    """
    topics = [
        TopicSpec(partition_count=9),
        TopicSpec(partition_count=9),
        TopicSpec(partition_count=9),
        TopicSpec(partition_count=9),
    ]

    @cluster(num_nodes=4)
    def test_multiple_transforms_chained_together(self):
        """
        Test that multiple (3) transforms chained together can produce a record from the first to the last topic.
        """
        for n, (i, o) in enumerate(zip(self.topics, self.topics[1:])):
            self._deploy_wasm(f"identity-xform-{n}",
                              input_topic=i,
                              output_topic=o)
        producer_status = self._produce_input_topic(topic=self.topics[0])
        consumer_status = self._consume_output_topic(topic=self.topics[-1],
                                                     status=producer_status,
                                                     timeout_sec=60)
        self.logger.info(f"{consumer_status}")
        assert consumer_status.invalid_records == 0, f"transform verification failed with invalid records: {consumer_status}"


class Move(typing.NamedTuple):
    topic: str
    partition: int
    current_leader: int
    new_leader: int


class DataTransformsLeadershipChangingTest(BaseDataTransformsTest):
    """
    Tests related to WebAssembly powered data transforms leadership changing.
    On the input topic moves requires shutdown and spinning up processors, and
    for the output topic it requires produce retries.
    """
    def __init__(self, *args, **kwargs):
        super(DataTransformsLeadershipChangingTest, self).__init__(
            *args,
            extra_rp_conf={
                # Disable leader balancer, as this test is doing its own
                # leadership transfers and the balancer would interfere
                'enable_leader_balancer': False,
                # Lower the delay before we start processing.
                # In slow debug mode tests the default here is
                # three seconds, and we have to wait on leadership
                # transfer this long for commits to be flushed, so
                # we can more easily timeout with the long wait.
                'data_transforms_commit_interval_ms': 500,
            },
            **kwargs)
        self._admin = Admin(self.redpanda)

    topics = [
        TopicSpec(partition_count=20),
        TopicSpec(partition_count=20),
    ]

    def _start_producer(self):
        """
        Start a producer that should run for about 10 seconds.
        """
        producer = TransformVerifierService(
            context=self.test_context,
            redpanda=self.redpanda,
            config=TransformVerifierProduceConfig(
                bytes_per_second='256KB',
                max_batch_size='64KB',
                max_bytes='2.5MB',
                message_size='1KB',
                topic=self.topics[0].name,
                transactional=False,
            ))
        producer.start()
        return producer

    def _wait_for_producer(
            self, producer: TransformVerifierService
    ) -> TransformVerifierProduceStatus:
        producer.wait(timeout_sec=10)
        producer_status = producer.get_status()
        producer.stop()
        producer.free()
        return typing.cast(TransformVerifierProduceStatus, producer_status)

    def _select_random_move(self) -> Move:
        """
        Select a random ntp and a node to move it to.
        """
        topic_to_move = random.choice(self.topics)
        partition_id = random.choice(range(0, topic_to_move.partition_count))
        leader = self._admin.get_partition_leader(namespace="kafka",
                                                  topic=topic_to_move.name,
                                                  partition=partition_id)
        node_ids = [self.redpanda.node_id(n) for n in self.redpanda.nodes]
        new_leader_node_id: int = random.choice(
            [i for i in node_ids if i != leader])
        return Move(topic=topic_to_move.name,
                    partition=partition_id,
                    current_leader=leader,
                    new_leader=new_leader_node_id)

    def _perform_move(self, mv: Move):
        """
        Perform a leadership transfer, handling timeouts by retrying the request.
        """
        def do_move():
            try:
                self._admin.transfer_leadership_to(namespace="kafka",
                                                   topic=mv.topic,
                                                   partition=mv.partition,
                                                   target_id=mv.new_leader)
                return True
            except RequestException as e:
                # The broker returns a Gateway Timeout when the underlying request times out in the controller.
                # So that we should retry.
                if e.response is not None and e.response.status_code == 504:
                    return False
                # Other errors are unexpected and we should throw
                raise

        wait_until(
            do_move,
            timeout_sec=30,
            backoff_sec=5,
            err_msg=f"unable to perform move {mv}",
        )

    @cluster(num_nodes=4)
    def test_leadership_changing_randomly(self):
        """
        Test that transforms can still make progress without dropping records in the face of leadership transfers.
        """
        self._deploy_wasm("identity-xform",
                          input_topic=self.topics[0],
                          output_topic=self.topics[-1])

        producer = self._start_producer()
        # Move a leader every second the test runs
        for _ in range(8):
            time.sleep(1)
            mv = self._select_random_move()
            self.logger.debug(
                f"Moving {mv.topic}/{mv.partition} from {mv.current_leader} -> {mv.new_leader}"
            )
            self._perform_move(mv)

        producer_status = self._wait_for_producer(producer)
        consumer_status = self._consume_output_topic(topic=self.topics[-1],
                                                     status=producer_status,
                                                     timeout_sec=60)
        self.logger.info(f"{consumer_status}")
        assert consumer_status.invalid_records == 0, f"transform verification failed with invalid records: {consumer_status}"


class DataTransformsLoggingTest(BaseDataTransformsTest):
    """
    Tests for data transforms logging
    """

    topics = [TopicSpec(partition_count=9), TopicSpec(partition_count=9)]

    logs_topic = TopicSpec(name='_redpanda.transform_logs', partition_count=4)

    # TODO(oren): be nice to actually use the OTel protobufs here
    class LogRecord:
        class Attribute:
            def __init__(self, attr: dict):
                self.key = attr['key']
                assert len(attr['value']) == 1
                self.value_type = list(attr['value'].keys())[0]
                self.value = attr['value'][self.value_type]

            # don't really care about the value, just the correct key and value _type_
            def __eq__(self, other):
                return (self.key == other.key
                        and self.value_type == other.value_type
                        and type(self.value) == type(other.value))

        EXPECTED_ATTRS = [
            Attribute({
                'key': 'transform_name',
                'value': {
                    'stringValue': 'any'  # don't care for validation
                }
            }),
            Attribute({
                'key': 'node',
                'value': {
                    'intValue': 23  # don't care for validation
                }
            })
        ]

        BODY_FIELD = 'body'
        TS_FIELD = 'timeUnixNano'
        SEVERITY_FIELD = 'severityNumber'
        ATTRS_FIELD = 'attributes'

        def __init__(self, raw: str):
            self._record = json.loads(raw)
            self._record['value'] = json.loads(self._record['value'])

            # Redpanda bits
            self.offset = self._record['offset']
            self.key = self._record['key']

            # OTel bits
            self._rep = self._record['value']
            self.body = self._rep.get(self.BODY_FIELD, None)
            self.timestamp_ns = self._rep.get(self.TS_FIELD, None)
            self.severity = self._rep.get(self.SEVERITY_FIELD, None)
            self.attributes = [
                self.Attribute(attr)
                for attr in self._rep.get(self.ATTRS_FIELD, [])
            ]

        def __str__(self):
            return json.dumps(self._record, indent=1)

        def validate(
                self,
                offset: typing.Optional[int] = None
        ) -> typing.Optional[list[str]]:
            errors = []
            if offset is not None and offset != self.offset:
                errors.append(f"Expected offset {offset} got {self.offset}")
            if self.body is None:
                errors.append(f"Missing {self.BODY_FIELD}")
            if self.timestamp_ns is None:
                errors.append(f"Missing {self.TS_FIELD}")
            if self.severity is None:
                errors.append(f"Missing {self.SEVERITY_FIELD}")
            if self.ATTRS_FIELD not in self._rep:
                errors.append(f"Missing {self.ATTRS_FIELD}")

            if len(self.attributes) != 2:
                errors.append(
                    f"Expected 2 attributes, got {len(self.attributes)}")

            for attr in self.EXPECTED_ATTRS:
                if attr not in self.attributes:
                    errors.append(f"Missing attribute '{attr.key}'")

            if len(errors) > 0:
                return errors

            return None

    def setup_identity_xform(self):
        it, ot = self.topics
        self._deploy_wasm(name="identity-logging-xform",
                          input_topic=self.topics[0],
                          output_topic=self.topics[1],
                          file="tinygo/identity_logging.wasm")
        return self.topics

    def consume_one_log_record(self, offset=0, timeout=10) -> LogRecord:
        return self.LogRecord(
            self._rpk.consume(self.logs_topic.name,
                              n=1,
                              offset=offset,
                              timeout=timeout))

    @cluster(num_nodes=4)
    def test_logs_volume(self):
        input_topic, output_topic = self.setup_identity_xform()
        producer_status = self._produce_input_topic(topic=input_topic,
                                                    transactional=False)
        consumer_status = self._consume_output_topic(topic=output_topic,
                                                     status=producer_status)
        seqnos = consumer_status.latest_seqnos

        # NOTE(oren): all logs for a given transform should go to the same
        # partition assuming the partition count of the logs topic doesn't change.
        # Therefore the sum of the highest sequence numbers for each output partition
        # is a fine proxy for expected high water mark on the transform logs topic
        log_hwm = sum([seqnos[p] for p in seqnos])

        self.logger.debug(
            f"Expect to find log offsets up to the total # of records {log_hwm}"
        )
        test_offsets = [0, log_hwm // 2, log_hwm]
        for i in test_offsets:
            log = self.consume_one_log_record(offset=i)
            validation_errs = log.validate(offset=i)
            assert (
                validation_errs is None
            ), f"Validation failed, errors: {json.dumps(validation_errs, indent=1)}"

    @cluster(num_nodes=3)
    def test_logs_otel(self):
        """
        Verify that log events conform to a subset OTel logging spec as laid out in our RFC
        """
        input_topic, output_topic = self.setup_identity_xform()

        self._rpk.produce(input_topic.name, 'foo', 'bar')
        log = self.consume_one_log_record()
        validation_errors = log.validate()
        assert (
            validation_errors is None
        ), f"Log record validation failed, errors: {json.dumps(validation_errors, indent=1)}"

    @cluster(num_nodes=3)
    def test_logs_cc_escaping(self):
        input_topic, output_topic = self.setup_identity_xform()

        val = generate_string_with_control_character(12)
        buf = bytearray()
        buf.extend(map(ord, val))
        assert any([b in CONTROL_CHARS_VALS
                    for b in buf]), f"Expected control char(s) in {buf}"

        self._rpk.produce(input_topic.name, 'foo', val)

        log = self.consume_one_log_record()
        buf = bytearray()
        buf.extend(map(ord, log.body))
        assert all([b not in CONTROL_CHARS_VALS for b in buf
                    ]), f"Found control char(s) in log output: {buf}"

    @cluster(num_nodes=3)
    def test_log_topic_integrity(self):
        self.setup_identity_xform()

        self.logger.debug(
            f"{self.logs_topic.name}: delete topic should fail (empty response table)"
        )
        with expect_exception(RpkException,
                              lambda e: "expected one row; found 0" in str(e)):
            self._rpk.delete_topic(self.logs_topic.name)

        self.logger.debug(
            f"{self.logs_topic.name}: produce should fail (authZ error)")
        with expect_exception(
                RpkException,
                lambda e: "Topic authorization failed" in str(e)):
            self._rpk.produce(self.logs_topic.name, "hardy", "har har")

    @cluster(num_nodes=3)
    def test_tunable_configs(self):
        it, ot = self.setup_identity_xform()

        self.logger.debug(
            "See that we can consume a log message w/in 5s with the default interval"
        )
        self._rpk.produce(it.name, 'foo', 'bar')
        self.consume_one_log_record(offset=0, timeout=5)

        self.logger.debug(
            "Consume operations should time out if the flush interval is very long"
        )

        # 1h
        self.redpanda.set_cluster_config(
            {'data_transforms_logging_flush_interval_ms': 1000 * 60 * 60})

        self._rpk.produce(it.name, 'foo', 'bar')
        with expect_exception(RpkException, lambda e: "timed out" in str(e)):
            self.consume_one_log_record(offset=1, timeout=5)

        self.logger.debug(
            "Log messages should be truncated to the configured max line")

        max_line = 10
        self.redpanda.set_cluster_config({
            'data_transforms_logging_flush_interval_ms':
            500,
            'data_transforms_logging_line_max_bytes':
            max_line
        })

        self._rpk.produce(it.name, 'a' * max_line, 'b' * max_line)

        # ignore the record at offset{0} b/c it was already in the buffers
        # by the time we updated the max line length.
        log = self.consume_one_log_record(offset=2)

        assert len(
            log.body
        ) == max_line, f"Expected {max_line}B, got {len(log.body)}B ({log.body})"

    # TODO(oren): some tests based on metrics would probably be good
