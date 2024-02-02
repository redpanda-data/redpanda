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

from requests.exceptions import RequestException

from ducktape.mark import matrix
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.services.transform_verifier_service import TransformVerifierProduceConfig, TransformVerifierProduceStatus, TransformVerifierService, TransformVerifierConsumeConfig, TransformVerifierConsumeStatus
from rptest.services.admin import Admin, CommittedWasmOffset

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.tests.cluster_config_test import wait_for_version_sync


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

    def _deploy_wasm(self, name: str, input_topic: TopicSpec,
                     output_topic: TopicSpec):
        """
        Deploy a wasm transform and wait for all processors to be running.
        """
        def do_deploy():
            self._rpk.deploy_wasm(name, input_topic.name, output_topic.name)
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
        def do_deploy():
            self._rpk.delete_wasm(name)
            return True

        wait_until(
            do_deploy,
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


class DataTransformsTest(BaseDataTransformsTest):
    """
    Tests related to WebAssembly powered data transforms
    """
    topics = [TopicSpec(partition_count=9), TopicSpec(partition_count=9)]

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
    def test_tracked_offsets_cleaned_up(self):
        """
        Test that a Wasm binary can be deployed, reach steady state and then be deleted.
        """
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
        # TODO(rockwood): Cleanup offsets after deploys
        # def all_offsets_removed():
        #     committed = self._list_committed_offsets()
        #     return len(committed) == 0
        # wait_until(
        #     all_offsets_removed,
        #     timeout_sec=30,
        #     backoff_sec=1,
        #     err_msg=f"all offsets where not removed",
        #     retry_on_exc=True,
        # )


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
