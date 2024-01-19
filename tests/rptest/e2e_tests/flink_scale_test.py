# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.e2e_tests.workload_manager import WorkloadManager
from rptest.services.cluster import cluster
from rptest.services.flink import FlinkService
from rptest.services.redpanda import MetricsEndpoint, MetricSamples
from rptest.tests.redpanda_test import RedpandaTest


class FlinkScaleTests(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        # Init parent
        super(FlinkScaleTests, self).__init__(test_context, log_level="trace")

        self.flink = FlinkService(test_context)
        # Prepare client
        config = self.redpanda.security_config()
        user = config.get("sasl_plain_username")
        passwd = config.get("sasl_plain_password")
        protocol = config.get("security_protocol", "SASL_PLAINTEXT")
        self.kafkacli = KafkaCliTools(self.redpanda,
                                      user=user,
                                      passwd=passwd,
                                      protocol=protocol)
        self.rpk = RpkTool(self.redpanda)
        # Prepare Workloads
        self.workload_manager = WorkloadManager(self.logger)

        return

    def tearDown(self):
        # Clean all up
        for spec in self.topic_specs:
            self.kafkacli.delete_topic(spec.name)
        return super().tearDown()

    def _run_workloads(self, workloads, config):
        """
            Run workloads from the list with supplied config

            Return: list of failed jobs
        """
        # Run produce job
        ids = []
        for workload in workloads:
            # Add script as a job
            self.logger.info(f"Adding {workload['name']} to flink")
            ids = self.flink.run_flink_job(workload['path'], config)
            if ids is None:
                raise RuntimeError("Failed to run job on flink for "
                                   f"workload: {workload['name']}")

            self.logger.debug(f"Workload '{workload['name']}' "
                              f"generated {len(ids)} "
                              f"jobs: {', '.join(ids)}")

        return ids

    @cluster(num_nodes=4)
    def test_transactions_scale_single_node(self):
        """
            Test uses same workload with different modes to produce
            and consume/process given number of transactions
        """
        def assert_failed_jobs(job_list):
            # Collect failed jobs
            failed = []
            for id in job_list:
                job = self.flink.get_job_by_id(id)
                if job['state'] == self.flink.STATE_FAILED:
                    self.logger.warning(f"Job '{id}' has failed")
                    failed.append(job)

            desc = [f"{j['name']} ({j['jid']}): {j['state']}" for j in failed]
            desc = "\n".join(desc)
            assert len(failed) == 0, \
                "Flink reports fails for " \
                f"transaction workload:\n{desc}"

        def get_hwm_for_topic(topic):
            _hwm = 0
            for partition in self.rpk.describe_topic(topic):
                # Add currect high watermark for topic
                _hwm += partition.high_watermark
            return _hwm

        # Validate test according to
        # - How many transactions can be generated per one job
        # - How mane transactions for the node

        # Prepare topics
        topics_per_node = 4
        # 4 mil events total
        total_events = 4 * 1024 * 1024

        self.topic_specs = []
        for idx in range(topics_per_node):
            topic_name = f"flink_scale_{idx}"
            spec = TopicSpec(name=topic_name, partition_count=8)
            self.kafkacli.create_topic(spec)
            self.topic_specs.append(spec)

        # Start Flink
        self.flink.start()

        # Load python workload to target node
        # Hardcoded file
        # TODO: Add workload config management
        # Currently, each INSERT operator will generate 1 subjob
        # So this config will generate 256 / 64 jobs
        _workload_config = {
            "log_level": "DEBUG",
            "brokers": self.redpanda.brokers(),
            "producer_group": "flink_group",
            "consumer_group": "flink_group",
            "topic_name": "flink_scale_topic_placeholder",
            "mode": "produce",
            "count": total_events // topics_per_node
        }

        # Get workload
        workloads = self.workload_manager.get_workloads(
            ['flink', 'transactions', 'scale'])
        # Run produce part
        all_jobs = []
        for spec in self.topic_specs:
            _workload_config['topic_name'] = spec.name
            self.logger.debug("Submitting job with config: \n"
                              f"{json.dumps(_workload_config, indent=2)}")
            all_jobs += self._run_workloads(workloads, _workload_config)

        # Assert failed jobs
        assert_failed_jobs(all_jobs)

        # Wait to finish
        self.flink.wait(timeout_sec=1800)

        topic_watermarks = []
        for spec in self.topic_specs:
            hwm = get_hwm_for_topic(spec.name)
            topic_watermarks.append(hwm)

        # Print out in one place:
        for idx in range(len(topic_watermarks)):
            self.logger.info(f"Topic: {self.topic_specs[idx].name}, "
                             f"watermark: {topic_watermarks[idx]}")
        self.logger.info(
            f"Total messages/High watermark sum: {sum(topic_watermarks)}")

        # Simple validation according to RP topic watermarks
        sum_hwm = sum(topic_watermarks)
        assert sum_hwm >= total_events, \
            "High watermark is less than targer total events: " \
            f"{sum_hwm} hwm, {total_events} target total"

        # Calculate per-node vectorized_internal_rpc_latency_sum/commit_tx
        def current_rpc_latency_sum(method, nodes) -> list[MetricSamples]:
            # Get metrics from redpanda nodes
            metrics = self.redpanda.metrics_sample(  # type: ignore
                "vectorized_internal_rpc_latency_sum",
                nodes=nodes,
                metrics_endpoint=MetricsEndpoint.METRICS)

            # Check if metrics received
            if not isinstance(metrics, MetricSamples):
                raise RuntimeError("Failed to get metrics")

            # Filter whole series with supplied method
            samples = []
            for s in metrics.samples:
                if s[4]['method'] == method:
                    samples.append(s)
            return samples

        # Get metric for commit_tx method
        rpc_latency_sum = current_rpc_latency_sum(
            "commit_tx", self.redpanda.started_nodes())

        # Rearrange as dict
        # {
        #   <node_hostname>: {
        #     shard_num: value
        #     total: sum(shards)
        #   }
        # }
        metric_per_node = {}
        rpc_latency_sum_total = 0
        for node in self.redpanda.started_nodes():
            h = node.account.hostname
            for metric in rpc_latency_sum:
                if node == metric.node:  # type: ignore
                    if h not in metric_per_node:
                        metric_per_node[h] = {}
                    metric_per_node[h].update(
                        {metric.labels["shard"]: metric.value})  # type: ignore
            metric_per_node[h]['total'] = sum(metric_per_node[h].values())
            rpc_latency_sum_total += metric_per_node[h]['total']

        # Collected metric is a count of commit_tx RPCs on the shard.
        # Since there is a commit_tx for each transaction, i.e. checkpoint
        # happens each 1 ms (see workload code). It is set in line:
        #       env.get_checkpoint_config().set_min_pause_between_checkpoints(1)

        # So, this is a number of transactions happened
        self.logger.info("Per-node metrics for latency is:\n"
                         f"{json.dumps(metric_per_node, indent=2)}")
        self.logger.info(f"Total: {rpc_latency_sum_total}")

        # Number of total transactions should be not less than
        # transaction_count / total_events. I.e. >0.1 in this case.
        # I.e. flink should be able to send at least 1 transaction per 1 ms
        assert 0.1 < rpc_latency_sum_total / total_events, \
            "Flink failed to generate at least 1 transaction " \
            "per checkpoint each 1 ms"

        return
