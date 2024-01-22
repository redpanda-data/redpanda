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

        def get_total_events_for_topic(topic):
            total_events = 0
            for partition in self.rpk.describe_topic(topic):
                # Add currect high watermark for topic
                total_events += partition.high_watermark
            return total_events

        # Validate test according to
        # - How many transactions can be generated per one job
        # - How mane transactions for the node

        # Prepare topics
        total_workloads = 4
        unique_topics = False
        # 4 mil events total
        target_total_events = 4 * 1024 * 1024

        self.topic_specs = []
        if unique_topics:
            for idx in range(total_workloads):
                topic_name = f"flink_scale_{idx}"
                spec = TopicSpec(name=topic_name, partition_count=8)
                self.kafkacli.create_topic(spec)
                self.topic_specs.append(spec)
        else:
            topic_name = "flink_scale_single_node"
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
            "transaction_id_prefix": "flink_scale_tid_prefix_placeholder",
            "mode": "produce",
            "count": target_total_events // total_workloads
        }

        # Get workload
        workloads = self.workload_manager.get_workloads(
            ['flink', 'transactions', 'scale'])
        # Run produce part
        all_jobs = []
        for idx in range(total_workloads):
            if unique_topics:
                _workload_config['topic_name'] = self.topic_specs[idx].name
                _workload_config[
                    'transaction_id_prefix'] = f"flink_scale_tid_{idx}"
            else:
                _workload_config['topic_name'] = self.topic_specs[0].name
                _workload_config[
                    'transaction_id_prefix'] = f"flink_scale_tid_{idx}"
            self.logger.debug("Submitting job with config: \n"
                              f"{json.dumps(_workload_config, indent=2)}")
            all_jobs += self._run_workloads(workloads, _workload_config)

        # Assert failed jobs
        assert_failed_jobs(all_jobs)

        # Wait to finish
        self.flink.wait(timeout_sec=1800)

        all_topic_events = []
        for spec in self.topic_specs:
            hwm = get_total_events_for_topic(spec.name)
            all_topic_events.append(hwm)

        # Print out in one place:
        for idx in range(len(all_topic_events)):
            self.logger.info(f"Topic: {self.topic_specs[idx].name}, "
                             f"event count: {all_topic_events[idx]}")
        self.logger.info(
            f"Total messages/High watermark sum: {sum(all_topic_events)}")

        # Simple validation according to RP topic watermarks
        topics_total_events = sum(all_topic_events)
        assert topics_total_events >= target_total_events, \
            "High watermark is less than targer total events: " \
            f"{topics_total_events} hwm, {target_total_events} target total"

        # Calculate per-node vectorized_internal_rpc_latency_sum/commit_tx
        def get_commit_requests_counts(method, nodes) -> list[MetricSamples]:
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
        commit_requests = get_commit_requests_counts(
            "commit_tx", self.redpanda.started_nodes())

        # Rearrange as dict
        # {
        #   <node_hostname>: {
        #     shard_num: value
        #     total: sum(shards)
        #   }
        # }
        metric_per_node = {}
        total_commit_requests = 0
        for node in self.redpanda.started_nodes():
            h = node.account.hostname
            for metric in commit_requests:
                if node == metric.node:  # type: ignore
                    if h not in metric_per_node:
                        metric_per_node[h] = {}
                    metric_per_node[h].update(
                        {metric.labels["shard"]: metric.value})  # type: ignore
            metric_per_node[h]['total'] = sum(metric_per_node[h].values())
            total_commit_requests += metric_per_node[h]['total']

        # Collected metric is a count of commit_tx RPCs on the shard.
        # Since there is a commit_tx for each transaction, i.e. checkpoint
        # happens each 1 ms (see workload code). It is set in line:
        #       env.get_checkpoint_config().set_min_pause_between_checkpoints(1)

        # So, this is a number of transactions happened
        self.logger.info("Per-node metrics for latency is:\n"
                         f"{json.dumps(metric_per_node, indent=2)}")
        self.logger.info(f"Total: {total_commit_requests}")

        # Number of total transactions should be not less than
        # transaction_count / total_events. I.e. >0.1 in this case.
        # I.e. flink should be able to send at least 1 transaction per 1 ms
        assert 0.1 < total_commit_requests / target_total_events, \
            "Flink failed to generate at least 1 transaction " \
            "per checkpoint each 1 ms"

        return
