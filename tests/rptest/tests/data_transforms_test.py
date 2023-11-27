# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import typing

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.services.transform_verifier_service import TransformVerifierProduceConfig, TransformVerifierProduceStatus, TransformVerifierService, TransformVerifierConsumeConfig, TransformVerifierConsumeStatus

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec


class WasmException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class DataTransformsTest(RedpandaTest):
    """
    Tests related to WebAssembly powered data transforms
    """
    topics = [TopicSpec(partition_count=9), TopicSpec(partition_count=9)]

    def __init__(self, test_context):
        super(DataTransformsTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf={
                                 'data_transforms_enabled': True,
                                 'data_transforms_commit_interval_ms': 1
                             })
        self._ctx = test_context
        self._rpk = RpkTool(self.redpanda)

    def _deploy_wasm(self, name: str):
        """
        Deploy a wasm transform and wait for all processors to be running.
        """
        [input_topic, output_topic] = self.topics

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

    @cluster(num_nodes=3)
    def test_lifecycle(self):
        """
        Test that a Wasm binary can be deployed, reach steady state and then be deleted.
        """
        self._deploy_wasm("identity-xform")
        self._delete_wasm("identity-xform")

    def _produce_input_topic(self) -> TransformVerifierProduceStatus:
        input_topic = self.topics[0]

        status = TransformVerifierService.oneshot(
            context=self.test_context,
            redpanda=self.redpanda,
            config=TransformVerifierProduceConfig(
                bytes_per_second='256KB',
                max_batch_size='64KB',
                max_bytes='1MB',
                message_size='1KB',
                topic=input_topic.name,
            ))
        return typing.cast(TransformVerifierProduceStatus, status)

    def _consume_output_topic(
        self, status: TransformVerifierProduceStatus
    ) -> TransformVerifierConsumeStatus:
        output_topic = self.topics[1]

        result = TransformVerifierService.oneshot(
            context=self.test_context,
            redpanda=self.redpanda,
            config=TransformVerifierConsumeConfig(
                topic=output_topic.name,
                bytes_per_second='1MB',
                validate=status,
            ))
        return typing.cast(TransformVerifierConsumeStatus, result)

    @cluster(num_nodes=4)
    def test_identity(self):
        """
        Test that a transform that only copies records from the input to the output topic works as intended.
        """
        self._deploy_wasm("identity-xform")
        producer_status = self._produce_input_topic()
        consumer_status = self._consume_output_topic(producer_status)
        self.logger.info(f"{consumer_status}")
        assert consumer_status.invalid_records == 0, "transform verification failed with invalid records: {consumer_status}"
