# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import time

from concurrent.futures import ThreadPoolExecutor

from ducktape.mark import parametrize

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.e2e_tests.workload_manager import WorkloadManager
from rptest.services.cluster import cluster
from rptest.services.flink import FlinkService
from rptest.services.redpanda import MetricsEndpoint, MetricSamples
from rptest.tests.redpanda_test import RedpandaTest

from rptest.utils.mode_checks import skip_debug_mode


class FlinkScaleTests(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        # Init parent
        super(FlinkScaleTests, self).__init__(test_context)
        self.test_context = test_context

        # Prepare client
        self.kafkacli = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        # Prepare Workloads
        self.workload_manager = WorkloadManager(self.logger)
        # Topics
        self.topic_specs = []
        return

    def tearDown(self):
        # Clean created topics
        # We should do this if we are in a cloudv2
        # names = [spec.name for spec in self.topic_specs]
        # with ThreadPoolExecutor(max_workers=15) as executor:
        #     executor.map(self.kafkacli.delete_topic, names)
        return super().tearDown()

    @staticmethod
    def _run_workloads(flink, workloads, config, logger):
        """
            Run workloads from the list with supplied config

            Return: list of failed jobs
        """
        # Run produce job
        ids = []
        for workload in workloads:
            # Add script as a job
            logger.info(f"Adding {workload['name']} to flink")
            ids = flink.run_flink_job(workload['path'], config)
            if ids is None:
                raise RuntimeError("Failed to run job on flink for "
                                   f"workload: {workload['name']}")

            logger.debug(f"Workload '{workload['name']}' "
                         f"generated {len(ids)} "
                         f"jobs: {', '.join(ids)}")

        return ids

    @staticmethod
    def _assert_failed_jobs(flink, logger, job_list):
        # Collect failed jobs
        failed = []
        for id in job_list:
            job = flink.get_job_by_id(id)
            if job['state'] == flink.STATE_FAILED:
                logger.warning(f"Job '{id}' has failed")
                failed.append(job)

        desc = [f"{j['name']} ({j['jid']}): {j['state']}" for j in failed]
        desc = "\n".join(desc)
        assert len(failed) == 0, \
            "Flink reports fails for " \
            f"transaction workload:\n{desc}"

    def _assert_actual_rate(self, rate_single_node, topics_total_events,
                            total_commit_requests):
        # Account for network losses
        target_transaction_rate = rate_single_node * 0.8
        self.logger.info(
            f"Calculated target transaction rate: {target_transaction_rate} per 1 ms"
        )
        # Actual rate is simple total msg divided by transactions
        actual_transaction_rate = topics_total_events / total_commit_requests
        self.logger.info(
            f"Actual transaction rate: {actual_transaction_rate} per 1 ms")
        assert target_transaction_rate < actual_transaction_rate, \
            "Flink failed to generate at least 1 transaction " \
            f"per half of the task managers ({rate_single_node}) " \
            "per checkpoint (1 ms): " \
            f"{target_transaction_rate} < {actual_transaction_rate}"

    def _get_total_events_for_topic(self, topic):
        total_events = 0
        for partition in self.rpk.describe_topic(topic):
            # Add currect high watermark for topic
            total_events += partition.high_watermark
        return total_events

    # Calculate per-node vectorized_internal_rpc_latency_sum/commit_tx
    def _get_commit_requests_counts(self, method,
                                    nodes) -> list[MetricSamples]:
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

    def get_total_commit_requests(self):
        # Get metric for commit_tx method
        commit_requests = self._get_commit_requests_counts(
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
        return total_commit_requests, metric_per_node

    def _create_topic_swarm(self, flinks, total_workloads,
                            topics_per_workload):
        # Prepare topic specs
        self.topic_specs = []
        for flink in flinks:
            hostname = flink.hostname
            for idx_w in range(total_workloads):
                for idx_t in range(topics_per_workload):
                    self.topic_specs.append(
                        TopicSpec(name=f"flink-{hostname}-{idx_w}-{idx_t}",
                                  partition_count=1))

        # Create topics
        self.logger.debug("Creating topics")
        _start = time.time()
        # Use ThreadPoolExecutor to create topics
        with ThreadPoolExecutor(max_workers=15) as executor:
            executor.map(self.kafkacli.create_topic, self.topic_specs)
        elapsed = time.time() - _start
        tps = len(self.topic_specs) / elapsed
        self.logger.debug("Done creating topics ({:>5,.2f}s, {:>5,.2f} "
                          "topics per sec)".format(elapsed, tps))

    @skip_debug_mode
    @cluster(num_nodes=4)
    @parametrize(unique_topics=True)
    @parametrize(unique_topics=False)
    def test_transactions_scale_single_node(self, unique_topics):
        """
            Test uses same workload with different modes to produce
            and consume/process given number of transactions
        """
        # Create service
        flink = FlinkService(self.test_context,
                             self.redpanda.get_node_cpu_count(),
                             self.redpanda.get_node_memory_mb())

        if not self.redpanda.dedicated_nodes:
            # Single workload
            total_workloads = 1
            # 1 mil events total for docker
            target_total_events = 1 * 1024 * 1024
            # Create topics
        else:
            # Prepare topics
            total_workloads = flink.num_taskmanagers
            # 4 mil events total for EC2 nodes
            target_total_events = 4 * 1024 * 1024
            # Create topics

        # Start Flink
        flink.start()

        if unique_topics:
            self._create_topic_swarm([flink], total_workloads, 1)
        else:
            hostname = flink.hostname
            spec = TopicSpec(name=f"flink-{hostname}-0-0", partition_count=8)
            self.kafkacli.create_topic(spec)
            self.topic_specs.append(spec)

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
                    'transaction_id_prefix'] = f"flink-scale-tid-{idx}"
            else:
                _workload_config['topic_name'] = self.topic_specs[0].name
                _workload_config[
                    'transaction_id_prefix'] = f"flink-scale-tid-{idx}"
            self.logger.debug("Submitting job with config: \n"
                              f"{json.dumps(_workload_config, indent=2)}")
            all_jobs += self._run_workloads(flink, workloads, _workload_config,
                                            self.logger)

        # Assert failed jobs
        self._assert_failed_jobs(flink, self.logger, all_jobs)

        # Wait to finish
        flink.wait(timeout_sec=1800)

        all_topic_events = []
        for spec in self.topic_specs:
            hwm = self._get_total_events_for_topic(spec.name)
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

        # Collected metric is a count of commit_tx RPCs on the shard.
        # Since there is a commit_tx for each transaction, i.e. checkpoint
        # happens each 1 ms (see workload code). It is set in line:
        #       env.get_checkpoint_config().set_min_pause_between_checkpoints(1)
        total_commit_requests, metric_per_node = \
            self.get_total_commit_requests()

        # So, this is a number of transactions happened
        self.logger.info("Per-node metrics for latency is:\n"
                         f"{json.dumps(metric_per_node, indent=2)}")
        self.logger.info(f"Total: {total_commit_requests}")

        # With default parallelization optimizations currently set
        # number of total transaction should be less than total events and
        # the ratio should be >= vcpu count. I.e. ideally,
        # each vcpu should generate at least 1 transaction per checkpoint.
        # we can reasonably expect at least half of the taskmanagers
        # generate 1 transaction per 1 ms interval.
        # Also, we account for 20% losses on the network

        # Calculate rate for single node and assert actual transfer rate
        rate_single_node = flink.num_taskmanagers // 2
        self._assert_actual_rate(rate_single_node, topics_total_events,
                                 total_commit_requests)

        return

    @skip_debug_mode
    @cluster(num_nodes=8)
    def test_transactions_scale_swarm(self):
        """
            Test uses same workload with different modes to produce
            and consume/process given number of transactions
        """
        # Get number of available nodes
        # This will give us num_nodes - redpanda nodes
        # Currently, 8-3 = 5
        total_nodes = len(self.cluster._available_nodes.os_to_nodes['linux'])

        # Create flink
        flinks = [
            FlinkService(self.test_context, self.redpanda.get_node_cpu_count(),
                         self.redpanda.get_node_memory_mb())
            for idx in range(total_nodes)
        ]

        # Start Flinks
        for flink in flinks:
            flink.start()

        # Goal of the test is to confirm that RP can withstand bombardment by
        # very small sized transactions. Docker nodes are less powerfull
        # on the networking side, so they have 2x size of transactions

        # EC2 have more power so the number of events is less,
        # but the timing is tighter since they are coming in from 4 nodes
        if not self.redpanda.dedicated_nodes:
            # Prepare topics for docker env
            # Total number of subtasks would be 1 x 25 x 5 = 125
            workloads_per_node = 1
            topics_per_workload = 25
            # 500k events total for single node, per topic
            # Total number of events will be
            # (500k / workloads) * topics_per_workload = (500k / 1) * 25 = 12,800,000 (per node)
            # Grand total is 5 x 12,800,000 = 64,000,000 events/messages
            target_total_events = 500 * 1024
        else:
            # Prepare topics for EC2
            # Total number of subtasks would be 4 x 25 x 5 = 500
            workloads_per_node = 4
            # for 2xlarge, 80 topics per workload is a maximum per flink taskmanager
            # value of 100 results in
            # "org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox$MailboxClosedException: Mailbox is in state CLOSED, but is required to be in state OPEN for put operations."
            # We set 25 as a reasonaly expected from default xlarge node
            topics_per_workload = 25
            # 1 mil events total for single node per topic per workload.
            # Total number of events will be
            # (1,048,576 / 4 workloads) * topics_per_workload = 6,553,600 events per node
            # Total number of events will be
            # Grand total is 5 x 6,553,600 = 32,768,000 events/messages

            # For manual run this value can be set to 4x (or more)
            # if tougher flink nodes is used (2xlarge, 6xlarge)
            target_total_events = 1 * 1024 * 1024

        # Create topics
        self._create_topic_swarm(flinks, workloads_per_node,
                                 topics_per_workload)

        # Load python workload to target node
        # TODO: Add workload config management
        _workload_config = {
            "log_level": "DEBUG",
            "brokers": self.redpanda.brokers(),
            "producer_group": "flink_group",
            "consumer_group": "flink_group",
            "number_of_topics": topics_per_workload,
            "topic_prefix": "flink_scale_topic_placeholder",
            "transaction_id_prefix": "flink_scale_tid_prefix_placeholder",
            "mode": "produce",
            "count": target_total_events // workloads_per_node
        }

        # Get workload
        workloads = self.workload_manager.get_workloads(
            ['flink', 'transactions', 'topic', 'swarm'])
        # Run produce part
        all_jobs = {}
        for idx in range(workloads_per_node):
            for flink in flinks:
                # hostname of flink service
                hostname = flink.hostname
                if hostname not in all_jobs:
                    all_jobs[hostname] = []
                # set prefixes
                _workload_config['topic_prefix'] = f"flink-{hostname}-{idx}"
                _workload_config['transaction_id_prefix'] = \
                    f"flink-{hostname}-tid-{idx}"
                # Submit job
                self.logger.debug("Submitting job with config: \n"
                                  f"{json.dumps(_workload_config, indent=2)}")
                all_jobs[hostname] += self._run_workloads(
                    flink, workloads, _workload_config, self.logger)

        # Assert failed jobs
        for flink in flinks:
            hostname = flink.hostname
            self._assert_failed_jobs(flink, self.logger, all_jobs[hostname])

        # Wait to finish
        for flink in flinks:
            flink.wait(timeout_sec=1800)

        all_topic_events = []
        for spec in self.topic_specs:
            hwm = self._get_total_events_for_topic(spec.name)
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

        # Get metric for commit_tx method
        commit_requests = self._get_commit_requests_counts(
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

        # With default parallelization optimizations currently set
        # transaction rate from single node should be at least 1 per checkpoint
        # mostly due to the fact that we starting workloads in order.
        # Ideally, ratio should be >= vcpu count. I.e. each vcpu should
        # generate at least 1 transaction per checkpoint. But since there is
        # a swarm of topics involved, they will fight for cpu time,
        # so 1 transaction per node is reasonable.

        # Multiple nodes involved (5), so expected ratio is
        # 1 transaction per each node per per 1 ms interval.
        # Also, we account for 20% losses on the network
        rate_per_node = 1
        self._assert_actual_rate(rate_per_node * total_nodes,
                                 topics_total_events, total_commit_requests)
        return
