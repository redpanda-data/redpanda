# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import random
from functools import partial

from rptest.services.admin import Admin
from rptest.services.verifiable_consumer import VerifiableConsumer

from ducktape.mark.resource import cluster
from ducktape.mark import ignore
from ducktape.utils.util import wait_until

from rptest.tests.end_to_end import EndToEndTest
from rptest.clients.rpk import RpkTool

from rptest.wasm.wasm_build_tool import WasmTemplateRepository, WasmBuildTool
from rptest.wasm.topic import construct_materialized_topic
from rptest.wasm.wasm_script import WasmScript

from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.clients.types import TopicSpec


class WasmPartitionMovementTest(PartitionMovementMixin, EndToEndTest):
    """
    Tests to ensure that the materialized partition movement feature
    is working as expected. This feature has materialized topics move to
    where their respective sources are moved to.
    """
    def __init__(self, ctx, *args, **kvargs):
        super(WasmPartitionMovementTest, self).__init__(
            ctx,
            *args,
            extra_rp_conf={
                # Disable leader balancer, as this test is doing its own
                # partition movement and the balancer would interfere
                'enable_leader_balancer': False,
                'enable_coproc': True,
                'developer_mode': True,
                'auto_create_topics_enabled': False
            },
            **kvargs)
        self._ctx = ctx
        self._rpk_tool = None
        self._build_tool = None
        self.result_data = []
        self.mconsumer = None

    def start_redpanda_nodes(self, nodes):
        self.start_redpanda(num_nodes=nodes)
        self._rpk_tool = RpkTool(self.redpanda)
        self._build_tool = WasmBuildTool(self._rpk_tool)

    def restart_random_redpanda_node(self):
        node = random.sample(self.redpanda.nodes, 1)[0]
        self.logger.info(f"Randomly killing redpanda node: {node.name}")
        self.redpanda.restart_nodes(node)

    def _deploy_identity_copro(self, inputs, outputs):
        script = WasmScript(inputs=inputs,
                            outputs=outputs,
                            script=WasmTemplateRepository.IDENTITY_TRANSFORM)

        self._build_tool.build_test_artifacts(script)

        self._rpk_tool.wasm_deploy(
            script.get_artifact(self._build_tool.work_dir), script.name,
            "ducktape")

    def _verify_materialized_assignments(self, topic, partition, assignments):
        admin = Admin(self.redpanda)
        massignments = self._get_assignments(admin, topic, partition)
        self.logger.info(
            f"materialized assignments for {topic}-{partition}: {massignments}"
        )

        self._wait_post_move(topic, partition, assignments)

    def _grab_input(self, topic):
        metadata = self.client().describe_topics()
        selected = [x for x in metadata if x['topic'] == topic]
        assert len(selected) == 1
        partition = random.choice(selected[0]["partitions"])
        return selected[0]["topic"], partition["partition"]

    def _on_data_consumed(self, record, node):
        self.result_data.append(record["value"])

    def _start_mconsumer(self, materialized_topic):
        self.mconsumer = VerifiableConsumer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=materialized_topic,
            group_id="test_group",
            on_record_consumed=self._on_data_consumed)
        self.mconsumer.start()

    def _await_consumer(self, limit, timeout):
        wait_until(
            lambda: self.mconsumer.total_consumed() >= limit,
            timeout_sec=timeout,
            err_msg=
            "Timeout of after %ds while awaiting delivery of %d materialized records, recieved: %d"
            % (timeout, limit, self.mconsumer.total_consumed()))
        self.mconsumer.stop()

    def _prime_env(self):
        output_topic = "identity_output2"
        self.start_redpanda_nodes(3)
        spec = TopicSpec(name="topic2",
                         partition_count=3,
                         replication_factor=3)
        self.client().create_topic(spec)
        self._deploy_identity_copro([spec.name], [output_topic])
        self.topic = spec.name
        self.start_producer(num_nodes=1, throughput=10000)
        self.start_consumer(1)
        self.await_startup(min_records=500)
        materialized_topic = construct_materialized_topic(
            spec.name, output_topic)

        def topic_created():
            metadata = self.client().describe_topics()
            self.logger.info(f"metadata: {metadata}")
            return any([x['topic'] == materialized_topic for x in metadata])

        wait_until(topic_created, timeout_sec=30, backoff_sec=2)

        self._start_mconsumer(materialized_topic)
        t, p = self._grab_input(spec.name)
        return {
            'topic': t,
            'partition': p,
            'materialized_topic': materialized_topic
        }

    @cluster(num_nodes=6)
    def test_dynamic(self):
        """
        Move partitions with active consumer / producer
        """
        s = self._prime_env()
        for _ in range(5):
            _, partition, assignments = self._do_move_and_verify(
                s['topic'], s['partition'])
            self._verify_materialized_assignments(s['materialized_topic'],
                                                  partition, assignments)

        # Vaidate input
        self.run_validation(min_records=500,
                            enable_idempotence=False,
                            consumer_timeout_sec=90)

        # Wait for all output
        self._await_consumer(self.consumer.total_consumed(), 90)

        # Validate number of records & their content
        self.logger.info(
            f'A: {self.mconsumer.total_consumed()} B: {self.consumer.total_consumed()}'
        )
        assert self.mconsumer.total_consumed() == self.consumer.total_consumed(
        )
        # Since the identity copro was deployed, contents of logs should be identical
        assert set(self.records_consumed) == set(self.result_data)

    @cluster(num_nodes=6)
    def test_dynamic_with_failure(self):
        s = self._prime_env()
        _, partition, assignments = self._do_move_and_verify(
            s['topic'], s['partition'])

        # Crash a node before verifying, it has a fixed amount of seconds to restart
        n = random.sample(self.redpanda.nodes, 1)[0]
        self.redpanda.restart_nodes(n)

        self._verify_materialized_assignments(s['materialized_topic'],
                                              partition, assignments)

        self.run_validation(min_records=500,
                            enable_idempotence=False,
                            consumer_timeout_sec=90)

        # Wait for all acked, output, significant because due to failure the number of
        # actual produced records may be less than expected
        num_producer_acked = len(self.producer.acked)
        self._await_consumer(num_producer_acked, 90)

        # GTE due to the fact that upon error, copro may re-processed already processed
        # data depending on when offsets were checkpointed
        assert self.mconsumer.total_consumed() >= num_producer_acked
