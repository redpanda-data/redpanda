# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import operator
from kafka import TopicPartition

from rptest.wasm.topics_result_set import CmpErr, cmp_err_to_str
from rptest.wasm.topic import get_source_topic, get_dest_topic, is_materialized_topic, construct_materialized_topic
from rptest.wasm.native_kafka_consumer import NativeKafkaConsumer
from rptest.services.verifiable_producer import VerifiableProducer

from rptest.wasm.wasm_build_tool import WasmBuildTool

from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool

from ducktape.utils.util import wait_until

from functools import reduce


def flat_map(fn, ll):
    return reduce(lambda acc, x: acc + fn(x), ll, [])


class WasmTest(RedpandaTest):
    # Ensure the topics collection is initialized correctly
    topics = ()

    def __init__(self, test_context, extra_rp_conf=dict(), record_size=1024):
        def enable_wasm_options():
            return dict(developer_mode=True,
                        enable_coproc=True,
                        enable_idempotence=True,
                        auto_create_topics_enabled=False)

        wasm_opts = enable_wasm_options()
        wasm_opts.update(extra_rp_conf)
        super(WasmTest, self).__init__(test_context, extra_rp_conf=wasm_opts)
        self._rpk_tool = RpkTool(self.redpanda)
        self._test_context = test_context
        self._build_tool = WasmBuildTool(self._rpk_tool)
        self._input_consumer = None
        self._output_consumer = None
        self._producers = None
        self._record_size = record_size

    def _build_script(self, script):
        # Build the script itself
        self.logger.info("Building script: %s" % script.name)
        self._build_tool.build_test_artifacts(script)

        # Deploy coprocessor
        self._rpk_tool.wasm_deploy(
            script.get_artifact(self._build_tool.work_dir), script.name,
            "ducktape")

    def restart_wasm_engine(self, node):
        self.logger.info(
            f"Begin manually triggered restart of wasm engine on node {node}")
        node.account.kill_process("bin/node", clean_shutdown=False)
        self.redpanda.start_wasm_engine(node)

    def restart_redpanda(self, node):
        self.logger.info(
            f"Begin manually triggered restart of redpanda on node {node}")
        self.redpanda.restart_nodes(node)

    @staticmethod
    def _expand_topic_spec(topics):
        """
        Converts a TopicSpec iterable to a TopicPartitions set
        """
        return set(
            flat_map(
                lambda spec: [
                    TopicPartition(spec.name, x)
                    for x in range(0, spec.partition_count)
                ], topics))

    @staticmethod
    def _to_output_topic_spec(itopics, output_topics):
        """
        Create a list of TopicSpecs for the set of output topics.
        """
        def to_ots(src):
            return [
                TopicSpec(name=construct_materialized_topic(src.name, dest),
                          partition_count=src.partition_count,
                          replication_factor=src.replication_factor,
                          cleanup_policy=src.cleanup_policy)
                for dest in output_topics
            ]

        return flat_map(to_ots, itopics)

    @staticmethod
    def _verify_materialized_outputs(outputs):
        return all([is_materialized_topic(t) for t in outputs.keys()])

    def _create_input_producers(self, topics, rate_lookup):
        producers = {}
        for tp_spec in topics:
            topic = tp_spec.name
            num_records = rate_lookup[topic]
            self.logger.info(
                f"Input producer assigned: {tp_spec} - {num_records}")

            producer = VerifiableProducer(self._test_context,
                                          num_nodes=1,
                                          redpanda=self.redpanda,
                                          topic=topic,
                                          acks=-1,
                                          max_messages=num_records,
                                          message_validator=(lambda x: True),
                                          enable_idempotence=True)
            producer.start()
            producers[topic] = producer
        return producers

    def start_wasm(self):
        """
        Build all wasm scripts, produce onto inputs while starting consumers
        to consume from expected outputs
        """
        topic_spec = self.wasm_inputs_throughput()
        output_topic_spec = self.wasm_outputs_throughput()
        if not self._verify_materialized_outputs(output_topic_spec):
            raise Exception(
                f'All outputs must be materialized topics: {output_topic_spec}'
            )
        scripts = self.wasm_test_plan()
        for script in scripts:
            self._build_script(script)

        self.logger.info(f"Input producer assigned: {self.topics}")
        self._producers = self._create_input_producers(self.topics, topic_spec)

        input_tps = self._expand_topic_spec(self.topics)
        output_tps = self._expand_topic_spec(
            self._to_output_topic_spec(
                self.topics, flat_map(lambda script: script.outputs, scripts)))
        self.logger.info(f"Input consumer assigned: {input_tps}")
        self.logger.info(f"Output consumer assigned: {output_tps}")
        self._input_consumer = NativeKafkaConsumer(self.redpanda.brokers(),
                                                   list(input_tps), topic_spec)
        self._output_consumer = NativeKafkaConsumer(self.redpanda.brokers(),
                                                    list(output_tps),
                                                    output_topic_spec)
        self._input_consumer.start()
        self._output_consumer.start()

        # Calcualte expected records on all inputs / outputs -- only for logging
        total_inputs = reduce(operator.add, [v for k, v in topic_spec.items()],
                              0)
        total_outputs = reduce(operator.add,
                               [v for k, v in output_topic_spec.items()], 0)
        self.logger.info(
            f"Waiting for {total_inputs} input records and {total_outputs}"
            " result records")

    def wait_on_results(self):
        def all_done():
            self.logger.info("Input: %d" %
                             self._input_consumer.results.num_records())
            self.logger.info("Output: %d" %
                             self._output_consumer.results.num_records())
            batch_total = self._input_consumer.results.num_records()
            if batch_total > 0:
                self.records_recieved(batch_total)

            return self._input_consumer.is_finished() \
                and self._output_consumer.is_finished()

        # Wait for all production to stop first
        for _, producer in self._producers.items():
            wait_until(lambda: producer.num_acked >= producer.max_messages,
                       timeout_sec=20,
                       backoff_sec=1)
            producer.stop()

        timeout, backoff = self.wasm_test_timeout()
        wait_until(all_done, timeout_sec=timeout, backoff_sec=backoff)
        try:
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

        input_expected = self._input_consumer.total_expected_records()
        output_expected = self._output_consumer.total_expected_records()
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
        return (300, 1)

    def wasm_inputs_throughput(self):
        """
        The total number of records to expect on each input topic, small defaults are set here
        """
        utopics = set(self.input_topics())
        return {topic: 1024 for topic in utopics}

    def wasm_outputs_throughput(self):
        """
        The total number of records to expect on each output topic
        """
        raise Exception('Unimplemented method')

    def wasm_test_output(self):
        """
        Full list of outputs to expect in their fully qualified form i.e. <input>._<output>_
        this is assembled from each WasmScripts input/output declaration
        """
        def all_outputs(outputs):
            return flat_map(
                lambda input_topic: [
                    construct_materialized_topic(input_topic, output_topic)
                    for output_topic in outputs
                ], self.wasm_test_input())

        return flat_map(lambda script: all_outputs(script.outputs),
                        self.wasm_test_plan())

    def wasm_test_input(self):
        """
        Default behavior is for all scripts to have all topics as input topics
        """
        assert (len(self.topics) >= 1)
        return [x.name for x in self.topics]

    def wasm_test_plan(self):
        """
        List of rptest.wasm.WasmScripts to deploy
        """
        raise Exception('Unimplemented method')

    def verify_results(self, verifier):
        """
        Entry point for all tests, asynchronously we perform the following:
        1. Scripts are built & deployed
        2. Consumers are set-up listening for expected records on output topics
        3. Producers set-up and begin producing onto input topics
        4. When finished, perform assertions in this method
        """
        self.start_wasm()
        input_results, output_results = self.wait_on_results()
        for script in self.wasm_test_plan():
            for dest in script.outputs:
                outputs = set([
                    construct_materialized_topic(src, dest)
                    for src in self.wasm_test_input()
                ])
                tresults = output_results.filter(lambda x: x.topic in outputs)
                err = verifier(input_results, tresults)
                if err != CmpErr.Success:
                    err_str = cmp_err_to_str(err)
                    raise Exception(
                        f"Set {dest} results weren't as expected: {type(self).__name__} reason: {err_str}"
                    )
