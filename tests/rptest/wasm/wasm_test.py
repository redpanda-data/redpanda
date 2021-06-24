# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import uuid
import random
import string
import json

from kafka import TopicPartition

from rptest.wasm.topic import get_source_topic
from rptest.wasm.native_kafka_consumer import NativeKafkaConsumer
from rptest.wasm.native_kafka_producer import NativeKafkaProducer

from rptest.wasm.topic import construct_materialized_topic
from rptest.wasm.wasm_build_tool import WasmBuildTool

from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool

from ducktape.utils.util import wait_until

from functools import reduce


def flat_map(fn, ll):
    return reduce(lambda acc, x: acc + fn(x), ll, [])


def random_string(N):
    return ''.join(
        random.choice(string.ascii_uppercase + string.digits)
        for _ in range(N))


class WasmScript:
    def __init__(self, inputs=[], outputs=[], script=None):
        self.name = random_string(10)
        self.description = random_string(20)
        self.inputs = inputs
        self.outputs = outputs
        self.script = script
        self.dir_name = str(uuid.uuid4())

    def get_artifact(self, build_dir):
        artifact = os.path.join(build_dir, self.dir_name, "dist",
                                f"{self.dir_name}.js")
        if not os.path.exists(artifact):
            raise Exception(f"Artifact {artifact} was not built")
        return artifact


class WasmTest(RedpandaTest):
    def __init__(self, test_context, extra_rp_conf=dict(), num_brokers=3):
        def enable_wasm_options():
            return dict(
                developer_mode=True,
                enable_coproc=True,
            )

        wasm_opts = enable_wasm_options()
        wasm_opts.update(extra_rp_conf)
        super(WasmTest, self).__init__(test_context,
                                       extra_rp_conf=wasm_opts,
                                       num_brokers=num_brokers)
        self._rpk_tool = RpkTool(self.redpanda)
        self._detected_deployed = False
        self._build_tool = WasmBuildTool(self._rpk_tool)
        self._scripts = []
        self._input_consumer = None
        self._output_consumer = None
        self._producers = None

    def _verify_list_output(self):
        output = json.loads(self._rpk_tool.wasm_list())
        ids = self.redpanda.node_id_list()
        all_found = set([])
        if 'status' in output:
            if output['status'] == 'uninitialized':
                return
        elif len(output) == 0:
            return
        for node_id, result in output.items():
            if int(node_id) != result['node_id']:
                raise Exception("Key should equal advertised node_id")
            if int(node_id) not in ids:
                raise Exception("Unknown node_id returned from wasm list cmd")
            if result['status'] != "up":
                raise Exception("Wasm engine is down, should be up")
            if self._detected_deployed is True:
                copros = result['coprocessors']
                for name, options in copros.items():
                    found = [x for x in self._scripts if x.name == name]
                    assert len(found) < 2
                    if len(found) > 0:
                        e = found[0]
                        all_found.add(e.name)
                        if options['description'] != e.description:
                            raise Exception("Mismatch script description")
                        if set(options['input_topics']) != set(e.inputs):
                            raise Exception("Mistmatch input topics")
        if self._detected_deployed and all_found != set(
            [x.name for x in self._scripts]):
            raise Exception("Expected copro was missing")

    def _build_script(self, script):
        # Build the script itself
        self._build_tool.build_test_artifacts(script)

        # Deploy coprocessor
        self._rpk_tool.wasm_deploy(
            script.get_artifact(self._build_tool.work_dir), script.name,
            script.description)

    def restart_wasm_engine(self, node):
        self.logger.info(
            f"Begin manually triggered restart of wasm engine on node {node}")
        node.account.kill_process("bin/node", clean_shutdown=False)
        self.redpanda.start_wasm_engine(node)

    def restart_redpanda(self, node):
        self.logger.info(
            f"Begin manually triggered restart of redpanda on node {node}")
        self.redpanda.restart_nodes(node)

    def start(self, topic_spec, scripts):
        self._scripts = scripts

        def to_output_topic_spec(output_topics):
            """
            Create a list of TopicPartitions for the set of output topics.
            Must parse the materialzied topic for the input topic to determine
            the number of partitions.
            """
            result = []
            for src, _, _ in topic_spec:
                materialized_topics = [
                    TopicSpec(name=construct_materialized_topic(
                        src.name, dest),
                              partition_count=src.partition_count,
                              replication_factor=src.replication_factor,
                              cleanup_policy=src.cleanup_policy)
                    for dest in output_topics
                ]
                result += materialized_topics
            return result

        def expand_topic_spec(etc):
            """
            Convers a TopicSpec iterable to a TopicPartitions list
            """
            return set(
                flat_map(
                    lambda spec: [
                        TopicPartition(spec.name, x)
                        for x in range(0, spec.partition_count)
                    ], etc))

        for script in scripts:
            self._build_script(script)

        input_tps = expand_topic_spec([x[0] for x in topic_spec])
        output_tps = expand_topic_spec(
            to_output_topic_spec(
                flat_map(lambda script: script.outputs, scripts)))

        # Calcualte expected records on all inputs / outputs
        total_inputs = reduce(lambda acc, x: acc + x[1], topic_spec, 0)

        def accrue(acc, output_topic):
            src = get_source_topic(output_topic)
            num_records = [x[1] for x in topic_spec if x[0].name == src]
            assert (len(num_records) == 1)
            return acc + num_records[0]

        total_outputs = reduce(accrue, set([x.topic for x in output_tps]), 0)

        self.logger.info(f"Input consumer assigned: {input_tps}")
        self.logger.info(f"Output consumer assigned: {output_tps}")

        self._producers = []

        for tp_spec, num_records, record_size in topic_spec:
            try:
                producer = NativeKafkaProducer(self.redpanda.brokers(),
                                               tp_spec.name, num_records, 100,
                                               record_size)
                producer.start()
                self._producers.append(producer)
            except Exception as e:
                self.logger.error(f"Failed to create NativeKafkaProducer: {e}")
                raise

        try:
            self._input_consumer = NativeKafkaConsumer(self.redpanda.brokers(),
                                                       list(input_tps),
                                                       total_inputs)
            self._output_consumer = NativeKafkaConsumer(
                self.redpanda.brokers(), list(output_tps), total_outputs)
        except Exception as e:
            self.logger.error(f"Failed to create NativeKafkaConsumer: {e}")
            raise

        self.logger.info(
            f"Waiting for {total_inputs} input records and {total_outputs}"
            " result records")
        self._input_consumer.start()
        self._output_consumer.start()

    def wait_on_results(self):
        def all_done():
            # Uncomment to periodically see the amt of data read
            self.logger.info("Input: %d" %
                             self._input_consumer.results.num_records())
            self.logger.info("Output: %d" %
                             self._output_consumer.results.num_records())
            batch_total = self._input_consumer.results.num_records()
            self._verify_list_output()
            if batch_total > 0:
                self._detected_deployed = True
                self.records_recieved(batch_total)

            return self._input_consumer.is_finished() \
                and self._output_consumer.is_finished()

        timeout, backoff = self.wasm_test_timeout()
        wait_until(all_done, timeout_sec=timeout, backoff_sec=backoff)
        try:
            [x.join() for x in self._producers]
            self._input_consumer.join()
            self._output_consumer.join()
        except Exception as e:
            self.logger.error("Exception occured in background thread: {e}")
            raise e

        input_consumed = self._input_consumer.results.num_records()
        output_consumed = self._output_consumer.results.num_records()
        self.logger.info(f"Consumed {input_consumed} input"
                         f" records and"
                         f" {output_consumed} result records")

        input_expected = self._input_consumer._num_records
        output_expected = self._output_consumer._num_records
        if input_consumed < input_expected:
            raise Exception(
                f"Consumed {input_consumed} expected {input_expected} input records"
            )
        if output_consumed < output_expected:
            raise Exception(
                f"Consumed {output_consumed} expected {output_expected} output records"
            )

        return (self._input_consumer.results, self._output_consumer.results)

    def records_recieved(self, output_recieved):
        """
        Called when a traunch of records has been returned from consumers
        """
        pass

    def wasm_test_timeout(self):
        """
        2-tuple representing timeout(0) and backoff interval(1)
        """
        return (90, 1)
