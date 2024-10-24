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
import math
import random
import string
import json
from typing import Optional
import concurrent.futures

from requests.exceptions import RequestException

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from rptest.clients.rpk import RpkException, RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricSamples, MetricsEndpoint
from ducktape.utils.util import wait_until
from ducktape.errors import TimeoutError
from rptest.services.transform_verifier_service import TransformVerifierProduceConfig, TransformVerifierProduceStatus, TransformVerifierService, TransformVerifierConsumeConfig, TransformVerifierConsumeStatus
from rptest.services.admin import Admin, CommittedWasmOffset

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.tests.cluster_config_test import wait_for_version_sync
from rptest.util import expect_exception, wait_until_result


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
        self._admin = Admin(self.redpanda)

    def _deploy_wasm(self,
                     name: str,
                     input_topic: TopicSpec,
                     output_topic: TopicSpec | list[TopicSpec],
                     file="tinygo/identity.wasm",
                     compression_type: TopicSpec.CompressionTypes
                     | None = None,
                     wait_running: bool = True,
                     retry_on_exc: bool = True,
                     from_offset: str | None = None):
        """
        Deploy a wasm transform and wait for all processors to be running.
        """
        if not isinstance(output_topic, list):
            output_topic = [output_topic]

        def do_deploy():
            self._rpk.deploy_wasm(name,
                                  input_topic.name,
                                  [o.name for o in output_topic],
                                  file=file,
                                  compression_type=compression_type,
                                  from_offset=from_offset)
            return True

        wait_until(
            do_deploy,
            timeout_sec=30,
            backoff_sec=5,
            err_msg=f"unable to deploy wasm transform {name}",
            retry_on_exc=retry_on_exc,
        )

        if not wait_running:
            return

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

    def _list_committed_offsets(self) -> list[CommittedWasmOffset]:
        response = []

        def do_list():
            nonlocal response
            response = self._admin.transforms_list_committed_offsets(
                show_unknown=True)
            return True

        wait_until(
            do_list,
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"unable to list committed offsets",
            retry_on_exc=True,
        )
        return response

    def _gc_committed_offsets(self):
        def do_gc():
            self._admin.transforms_gc_committed_offsets()
            return True

        wait_until(
            do_gc,
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"unable to gc committed offsets",
            retry_on_exc=True,
        )

    def _modify_cluster_config(self, upsert):
        patch_result = self._admin.patch_cluster_config(upsert=upsert)
        wait_for_version_sync(self._admin, self.redpanda,
                              patch_result['config_version'])

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
                                      [self.topics[1].name],
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

    @cluster(num_nodes=4)
    @matrix(transactional=[False, True], wait_running=[False, True])
    def test_identity(self, transactional, wait_running):
        """
        Test that a transform that only copies records from the input to the output topic works as intended.
        """
        input_topic = self.topics[0]
        output_topic = self.topics[1]
        self._deploy_wasm(name="identity-xform",
                          input_topic=input_topic,
                          output_topic=output_topic,
                          wait_running=wait_running)
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
            "Check the broker support for the version of the Data Transforms SDK being used."
        )

    @cluster(num_nodes=3)
    def test_tracked_offsets_cleaned_up(self):
        """
        Test that a Wasm binary can be deployed, reach steady state and then be deleted.
        """
        # We need a fast commit time so that we don't have to wait long for commits before invoking GC
        self._modify_cluster_config({'data_transforms_commit_interval_ms': 1})
        self._deploy_wasm(name="identity-xform",
                          input_topic=self.topics[0],
                          output_topic=self.topics[1])

        def all_offsets_committed():
            committed = self._list_committed_offsets()
            self.logger.debug(
                f"committed offsets: {committed}, expecting length: {self.topics[0].partition_count}"
            )
            return len(committed) == self.topics[0].partition_count

        wait_until(
            all_offsets_committed,
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"all offsets did not commit",
            retry_on_exc=True,
        )
        self._delete_wasm(name="identity-xform")

        # Give **plenty** of time for any pending transform commits to flush.
        time.sleep(1)
        # Now cleanup old offsets, and they should all be gone.
        self._gc_committed_offsets()

        def all_offsets_removed():
            committed = self._list_committed_offsets()
            return len(committed) == 0

        wait_until(
            all_offsets_removed,
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"all offsets where not removed",
            retry_on_exc=True,
        )

    @cluster(num_nodes=3)
    @matrix(use_rpk=[False, True])
    def test_patch(self, use_rpk):
        input_topic = self.topics[0]
        output_topic = self.topics[1]
        self._deploy_wasm(name="identity-xform",
                          input_topic=input_topic,
                          output_topic=output_topic,
                          wait_running=True)

        def all_partitions_status(stat: str):
            report = self._rpk.list_wasm()
            return all(s.status == stat for s in report[0].status)

        def env_is(env: dict[str, str]):
            report = self._rpk.list_wasm()
            return report[0].environment == env

        if use_rpk:
            self._rpk.pause_wasm("identity-xform")
        else:
            rsp = self._admin.transforms_patch_meta("identity-xform",
                                                    pause=True)
            assert rsp.status_code == 200, f"/meta request failed, status: {rsp.status_code}"

        wait_until(lambda: all_partitions_status("inactive"),
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg=f"some partitions didn't become inactive",
                   retry_on_exc=True)

        with expect_exception(TimeoutError, lambda _: True):
            wait_until(lambda: all_partitions_status("running"),
                       timeout_sec=15,
                       backoff_sec=1,
                       retry_on_exc=True)

        if use_rpk:
            self._rpk.resume_wasm("identity-xform")
            wait_until(lambda: all_partitions_status("running"),
                       timeout_sec=30,
                       backoff_sec=1,
                       err_msg=f"some partitions didn't become active",
                       retry_on_exc=True)
        else:  # NOTE: patching ENV is not yet implemented in rpk
            env1 = {
                "FOO": "bar",
                "BAZ": "quux",
            }
            env2 = {"FOO": "bells"}

            rsp = self._admin.transforms_patch_meta("identity-xform",
                                                    pause=False,
                                                    env=env1)
            assert rsp.status_code == 200, f"/meta request failed, status: {rsp.status_code}"

            wait_until(
                lambda: all_partitions_status("running") and env_is(env1),
                timeout_sec=30,
                backoff_sec=1,
                err_msg=f"some partitions didn't come back",
                retry_on_exc=True)

            rsp = self._admin.transforms_patch_meta("identity-xform", env=env2)

            wait_until(
                lambda: all_partitions_status("running") and env_is(env2),
                timeout_sec=30,
                backoff_sec=1,
                err_msg="some partitions didn't take the env update",
                retry_on_exc=True)

            rsp = self._admin.transforms_patch_meta("identity-xform", env={})

            wait_until(lambda: all_partitions_status("running") and env_is({}),
                       timeout_sec=30,
                       backoff_sec=1,
                       err_msg="some partitions did not clear their envs",
                       retry_on_exc=True)

    @cluster(num_nodes=4)
    def test_consume_from_end(self):
        """
        Test that by default transforms read from the end of the topic if no records
        are produced between deploy time and transform start time.
        """
        input_topic = self.topics[0]
        output_topic = self.topics[1]
        producer_status = self._produce_input_topic(topic=self.topics[0])
        self._deploy_wasm(name="identity-xform",
                          input_topic=input_topic,
                          output_topic=output_topic,
                          wait_running=True)

        with expect_exception(TimeoutError, lambda _: True):
            consumer_status = self._consume_output_topic(
                topic=self.topics[1], status=producer_status)

    @cluster(num_nodes=4)
    @matrix(compression_type=[
        TopicSpec.CompressionTypes.GZIP,
        TopicSpec.CompressionTypes.LZ4,
        TopicSpec.CompressionTypes.NONE,
        TopicSpec.CompressionTypes.SNAPPY,
        TopicSpec.CompressionTypes.ZSTD,
        TopicSpec.CompressionTypes.PRODUCER,  # broker will reject this
    ])
    def test_compression(self, compression_type: TopicSpec.CompressionTypes):
        INVALID_MODES: list[TopicSpec.CompressionTypes] = [
            TopicSpec.CompressionTypes.PRODUCER,
        ]
        transform_name = "identity-xform"
        input_topic = self.topics[0]
        output_topic = self.topics[1]

        valid = compression_type not in INVALID_MODES

        def deploy():
            self._deploy_wasm(name=transform_name,
                              input_topic=input_topic,
                              output_topic=output_topic,
                              compression_type=compression_type,
                              wait_running=True,
                              retry_on_exc=valid)

        if not valid:
            with expect_exception(
                    RpkException,
                    lambda e: "invalid JSON request body" in str(e)):
                deploy()
            # just go ahead and deploy with no compression and let the test finish
            compression_type = TopicSpec.CompressionTypes.NONE
            deploy()
        else:
            deploy()

        def compression_set(compression_type: TopicSpec.CompressionTypes):
            report = self._rpk.list_wasm()
            return report[0].compression == compression_type

        wait_until(lambda: compression_set(compression_type),
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="compression did not update",
                   retry_on_exc=False)

        producer_status = self._produce_input_topic(topic=self.topics[0])
        consumer_status = self._consume_output_topic(topic=self.topics[1],
                                                     status=producer_status)
        self.logger.info(f"{consumer_status}")
        assert consumer_status.invalid_records == 0, f"transform verification failed with invalid records: {consumer_status}"

    @cluster(num_nodes=4)
    @matrix(offset=[
        "+0",
        "-1",
        f"@{int(time.time() * 1000)}",
        "+9223372036854775807",  # int64_max - should clamp to start at latest
    ])
    def test_consume_from_offset(self, offset):
        '''
        Verify that offset-delta based and timestamp based consumption works as expected.
        That is, records produced prior to deployment should still be accessible given an
        appropriate offset config.
        '''
        input_topic = self.topics[0]
        output_topic = self.topics[1]
        producer_status = self._produce_input_topic(topic=self.topics[0])
        self._deploy_wasm(name="identity-xform",
                          input_topic=input_topic,
                          output_topic=output_topic,
                          from_offset=offset,
                          wait_running=True)
        consumer_status = self._consume_output_topic(topic=self.topics[1],
                                                     status=producer_status)

        self.logger.info(f"{consumer_status}")
        assert consumer_status.invalid_records == 0, f"transform verification failed with invalid records: {consumer_status}"

    @cluster(num_nodes=4)
    @matrix(offset=[
        None,  # No offest -> read from the end of the topic
        "@33276193569000",  # June 3024
        "-0",  # '0 from end' should commit 'latest'
    ])
    def test_consume_off_end(self, offset):
        '''
        Verify that consuming off the end of the input topic works as expected.
        That is, confirm that records produced _prior_ to deployment do not reach the transform.
        '''
        input_topic = self.topics[0]
        output_topic = self.topics[1]
        producer_status = self._produce_input_topic(topic=self.topics[0])
        self._deploy_wasm(name="identity-xform",
                          input_topic=input_topic,
                          output_topic=output_topic,
                          from_offset=offset,
                          wait_running=True)

        with expect_exception(TimeoutError, lambda _: True):
            _ = self._consume_output_topic(topic=self.topics[1],
                                           status=producer_status)

    @cluster(num_nodes=3)
    @matrix(offset=[
        "@9223372036854775807",  # int64_max (out of range for millis)
        "+NaN",  # lexical cast error (literal NaN)
        "-9223372036854775808",  # lexical cast error (int64 overflow)
        f"@{time.time() * 1000}",  # lexical cast error (float value)
        "@-10",  # illegal negative value
        "--10",  # illegal negative value
        "+-10",  # illegal negative value
    ])
    def test_consume_junk_off(self, offset):
        '''
        Tests for junk data. Deployment should fail cleanly in the admin API or rpk.
        '''
        input_topic = self.topics[0]
        output_topic = self.topics[1]

        with expect_exception(
                RpkException,
                lambda e: print(e) or "bad offset" in str(e).lower(
                )):  # rpk returns 'bad offset', RP Admin returns 'Bad offset'.
            self._deploy_wasm(name="identity-xform",
                              input_topic=input_topic,
                              output_topic=output_topic,
                              from_offset=offset,
                              wait_running=False,
                              retry_on_exc=False)


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


class DataTransformsMultipleOutputTopicsTest(BaseDataTransformsTest):
    """
    Tests related to WebAssembly powered data transforms that write to multiple output topics.
    """
    topics = [TopicSpec(partition_count=3) for i in range(9)]

    @cluster(num_nodes=6)
    def test_multiple_output_topics(self):
        """
        Test that we write to all output topics (using a tee transform) with each output topic being valid.
        """
        self._deploy_wasm("tee-xform",
                          input_topic=self.topics[0],
                          output_topic=self.topics[1:],
                          file="tinygo/tee.wasm")
        producer_status = self._produce_input_topic(topic=self.topics[0])

        def validate_output(topic: TopicSpec,
                            producer_status: TransformVerifierProduceStatus):
            consumer_status = self._consume_output_topic(
                topic=topic, status=producer_status, timeout_sec=60)
            self.logger.info(f"{topic.name}={consumer_status}")
            assert consumer_status.invalid_records == 0, f"transform verification failed with invalid records for topic {topic.name}: {consumer_status}"

        # Limit concurrency to the number of available nodes that we have to schedule on.
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            tasks = [
                executor.submit(validate_output, topic, producer_status)
                for topic in self.topics[1:]
            ]
            for task in concurrent.futures.as_completed(tasks):
                task.result()  # Will throw if failed


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
        self.offset: int = self._record['offset']
        self.key: str = self._record['key']

        # OTel bits
        self._rep = self._record['value']
        body_object = self._rep.get(self.BODY_FIELD, None)
        if isinstance(body_object, dict):
            self.body: str = body_object.get("stringValue", None)
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
            offset: typing.Optional[int] = None) -> typing.Optional[list[str]]:
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
            errors.append(f"Expected 2 attributes, got {len(self.attributes)}")

        for attr in self.EXPECTED_ATTRS:
            if attr not in self.attributes:
                errors.append(f"Missing attribute '{attr.key}'")

        if len(errors) > 0:
            return errors

        return None


class BaseDataTransformsLoggingTest(BaseDataTransformsTest):
    """
    Tests for data transforms logging
    """

    logs_topic = TopicSpec(name='_redpanda.transform_logs', partition_count=4)

    def setup_identity_xform(self, it, ot, name="identity-logging_xform"):
        self._deploy_wasm(name=name,
                          input_topic=it,
                          output_topic=ot,
                          file="tinygo/identity_logging.wasm")
        return [it, ot]

    def consume_one_log_record(self, offset=0, timeout=None) -> LogRecord:
        def consume(timeout):
            record = LogRecord(
                self._rpk.consume(self.logs_topic.name,
                                  n=1,
                                  offset=offset,
                                  timeout=timeout))
            # RPK can reset and return the latest offset if our offset is
            # out of range, so we need to make sure we keep trying until
            # we get a record.
            return record.offset == offset, record

        if timeout != None:
            return consume(timeout)[1]

        return wait_until_result(
            lambda: consume(timeout=10),
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"never recieved log record at offset {offset}")


class DataTransformsLoggingTest(BaseDataTransformsLoggingTest):

    topics = [
        TopicSpec(partition_count=9),
        TopicSpec(partition_count=9),
    ]

    @cluster(num_nodes=4)
    def test_logs_volume(self):
        input_topic, output_topic = self.setup_identity_xform(
            self.topics[0], self.topics[1])
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
        # Offsets are zero based so make sure the last record is at log_hwm - 1
        test_offsets = [0, log_hwm // 2, log_hwm - 1]
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
        input_topic, _ = self.setup_identity_xform(self.topics[0],
                                                   self.topics[1])

        self._rpk.produce(input_topic.name, 'foo', 'bar')
        log = self.consume_one_log_record()
        validation_errors = log.validate()
        assert (
            validation_errors is None
        ), f"Log record validation failed, errors: {json.dumps(validation_errors, indent=1)}"

    @cluster(num_nodes=3)
    def test_log_topic_integrity(self):
        self.setup_identity_xform(self.topics[0], self.topics[1])

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
        it, _ = self.setup_identity_xform(self.topics[0], self.topics[1])

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

        # It's possible for log lines to be emitted multiple times, so we need to
        # handle any duplicate foo:bar messages from above until we get an a*:b*
        # message.
        # Do this by skipping any foo:bar messages until we get to our first a*:b*
        # message. We can start at offset 2 because we know we'll get at least 2
        # of those (barring failures) messages, then add an upper cap of how many
        # we attempt to read.
        for o in range(2, 20):
            log = self.consume_one_log_record(offset=o)

            if "foo:bar" in log.body:
                continue

            assert len(
                log.body
            ) == max_line, f"Expected {max_line}B, got {len(log.body)}B ({log.body})"
            break


class DataTransformsLoggingMetricsTest(BaseDataTransformsLoggingTest):

    topics = [
        TopicSpec(partition_count=9),
        TopicSpec(partition_count=9),
        TopicSpec(partition_count=9),
        TopicSpec(partition_count=9),
        TopicSpec(partition_count=9),
        TopicSpec(partition_count=9),
        TopicSpec(partition_count=9),
        TopicSpec(partition_count=9),
    ]

    LOGGER_METRICS = ["events_total", "events_dropped_total"]
    LOGGER_LABELS = ["function_name", "shard"]
    MANAGER_METRICS = [
        "log_manager_buffer_usage_ratio", "log_manager_write_errors_total"
    ]
    MANAGER_LABELS = ["shard"]

    def get_metrics_from_node(
        self,
        node: ClusterNode,
        patterns: list[str],
        endpoint=MetricsEndpoint.METRICS
    ) -> Optional[dict[str, MetricSamples]]:
        def get_metrics_from_node_sync(patterns: list[str]):
            samples = self.redpanda.metrics_samples(
                patterns,
                [node],
                endpoint,
            )
            success = samples is not None
            return success, samples

        try:
            return wait_until_result(
                lambda: get_metrics_from_node_sync(patterns),
                timeout_sec=2,
                backoff_sec=.1)
        except TimeoutError as e:
            return None

    def unpack_samples(self, metric_samples):
        return {
            k: [{
                'value': s.value,
                'labels': s.labels
            } for s in metric_samples[k].samples]
            for k in metric_samples.keys()
        }

    def random_ascii_string(self, min_len, max_len) -> str:
        return ''.join(
            random.choices(string.ascii_letters + string.digits,
                           k=random.randint(min_len, max_len)))

    @cluster(num_nodes=3)
    def test_logger_metrics_present(self):
        it, _ = self.setup_identity_xform(self.topics[0], self.topics[1])
        self._rpk.produce(it.name, 'foo', 'bar')
        self.consume_one_log_record()

        def has_logger_metrics(node, endpoint):
            metrics_samples = self.get_metrics_from_node(node,
                                                         self.LOGGER_METRICS,
                                                         endpoint=endpoint)
            assert metrics_samples is not None, "get_metrics timed out"
            return sorted(metrics_samples.keys()) == sorted(
                self.LOGGER_METRICS)

        assert any(
            [
                has_logger_metrics(n, MetricsEndpoint.METRICS)
                for n in self.redpanda.nodes
            ]
        ), "Expected some node to export logger metrics on the internal endpoint"

        assert any(
            [
                has_logger_metrics(n, MetricsEndpoint.PUBLIC_METRICS)
                for n in self.redpanda.nodes
            ]
        ), "Expected some node to export logger metrics on the public endpoint"

    @cluster(num_nodes=3)
    def test_logger_metrics_values(self):
        n_xform = len(self.topics) // 2

        its = []
        for i in range(0, n_xform):
            its.append(
                self.setup_identity_xform(
                    self.topics[2 * i],
                    self.topics[2 * i + 1],
                    name=f"logger{i+1}",
                )[0])
        N_PER_TP = 100
        for i in range(0, N_PER_TP):
            for it in its:
                self._rpk.produce(
                    it.name,
                    self.random_ascii_string(10, 100),
                    self.random_ascii_string(10, 100),
                )

        def get_total_events_per_xform() -> tuple[dict[str, int], int]:
            totals = {}
            dropped = 0
            for node in self.redpanda.nodes:
                samples = self.unpack_samples(
                    self.get_metrics_from_node(
                        node,
                        self.LOGGER_METRICS,
                        endpoint=MetricsEndpoint.METRICS))
                for m in samples['events_total']:
                    xfm_name = m['labels']['function_name']
                    totals[xfm_name] = totals.get(xfm_name, 0) + m['value']
                dropped += sum(
                    [m['value'] for m in samples["events_dropped_total"]])
            self.logger.debug(json.dumps(totals, indent=1))
            return (totals, dropped)

        def get_total_events() -> tuple[int, int]:
            tot = 0
            totals, dropped = get_total_events_per_xform()
            for k in totals:
                tot += totals[k]
            self.logger.debug(f"Cluster total log events: {tot}")
            return (tot, dropped)

        n_events_expected = N_PER_TP * len(its)
        wait_until(lambda: get_total_events()[0] >= n_events_expected,
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg=f"never got all the events")

        totals, dropped = get_total_events_per_xform()
        assert len(totals) == len(its)
        for name in totals:
            assert totals[
                name] >= N_PER_TP, f"Expected {N_PER_TP} log events from transform {name}, got{totals[name]}"
        assert dropped == 0, f"Expected no dropped events, got {dropped}"

    @cluster(num_nodes=3)
    def test_manager_metrics_present(self):
        it, _ = self.setup_identity_xform(self.topics[0], self.topics[1])

        def has_manager_metrics(node, endpoint):
            metrics_samples = self.get_metrics_from_node(node,
                                                         self.MANAGER_METRICS,
                                                         endpoint=endpoint)
            assert metrics_samples is not None, "Get_metrics timed out"
            return sorted(metrics_samples.keys()) == sorted(
                self.MANAGER_METRICS)

        assert all(
            [
                has_manager_metrics(n, MetricsEndpoint.METRICS)
                for n in self.redpanda.nodes
            ]
        ), "Expected all nodes to export manager metrics on the internal endpoint"

        assert not any([
            has_manager_metrics(n, MetricsEndpoint.PUBLIC_METRICS)
            for n in self.redpanda.nodes
        ]), "Expected no node to export manager metrics on the public endpoint"

    @cluster(num_nodes=3)
    def test_manager_metrics_values(self):
        self.logger.debug("Set flush interval to 1h so buffers fill up")
        self.redpanda.set_cluster_config(
            {
                'data_transforms_logging_flush_interval_ms': 60 * 60 * 1000,
                'data_transforms_logging_buffer_capacity_bytes': 100 * 1024
            },
            expect_restart=True)

        cfg = self._admin.get_cluster_config()

        buf_capacity_B = cfg.get(
            'data_transforms_logging_buffer_capacity_bytes', 0)
        assert buf_capacity_B > 0, "Expected non-zero log buffer capacity"

        max_log_line_B = cfg.get('data_transforms_logging_line_max_bytes', 0)
        assert max_log_line_B > 0, "Expected non-zero max log line"

        self.logger.warn(
            f"Buffer capacity: {buf_capacity_B}B, Max log line: {max_log_line_B}B"
        )

        input_topic = TopicSpec()
        self._rpk.create_topic(input_topic.name, input_topic.partition_count,
                               input_topic.replication_factor)

        it, ot = self.setup_identity_xform(
            input_topic,
            self.topics[1],
            name=f"logger-xform",
        )

        def get_buffer_usage() -> list[float]:
            all_nodes_usage: list[float] = []
            for node in self.redpanda.nodes:
                samples = self.unpack_samples(
                    self.get_metrics_from_node(
                        node,
                        ["log_manager_buffer_usage"],
                        endpoint=MetricsEndpoint.METRICS,
                    ))

                for s in samples['log_manager_buffer_usage']:
                    all_nodes_usage.append(s['value'])
            self.logger.debug(f"MAX: {max(all_nodes_usage)}")
            return all_nodes_usage

        N_PER_TP = int(math.ceil(buf_capacity_B / max_log_line_B))
        k = self.random_ascii_string(128, 128)

        self.logger.debug("Produce enough data to fill the buffers twice over")
        for _ in range(0, N_PER_TP * 2):
            self._rpk.produce(
                it.name,
                k,
                self.random_ascii_string(max_log_line_B, max_log_line_B),
            )

        self.logger.debug(
            "This should trigger a flush, preventing us from getting close to 100% usage"
        )

        with expect_exception(TimeoutError, lambda _: True):
            wait_until(lambda: any(bu > 1.0 for bu in get_buffer_usage()),
                       timeout_sec=10,
                       backoff_sec=1,
                       err_msg=f"buffers never filled up!")

        self.logger.debug(
            f"Final buffer usage: {json.dumps(get_buffer_usage(), indent=1)}")
