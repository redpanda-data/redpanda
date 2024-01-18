# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.e2e_tests.workload_manager import WorkloadManager
from rptest.services.cluster import cluster
from rptest.services.flink import FlinkService
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
    def test_transactions_scale(self):
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

        # TODO: Validate/Gather info on
        # - Transactions can be generated per one flink job
        # - Transactions per node
        # - Transactions total >500K

        # Prepare topics
        self.topic_name = "flink_scale_1"
        self.topic_specs = [TopicSpec(name=self.topic_name, partition_count=8)]
        for spec in self.topic_specs:
            self.kafkacli.create_topic(spec)

        # Start Flink
        self.flink.start()

        # Load python workload to target node
        # TODO: Add workload config management
        _workload_config = {
            "log_level": "DEBUG",
            "brokers": self.redpanda.brokers(),
            "producer_group": "flink_group",
            "consumer_group": "flink_group",
            "topic_name": self.topic_name,
            "mode": "produce",
            "count": 65535 * 32
        }

        # Get workload
        workloads = self.workload_manager.get_workloads(
            ['flink', 'transactions', 'scale'])
        # Run produce part
        all = self._run_workloads(workloads, _workload_config)
        # Assert failed jobs
        assert_failed_jobs(all)
        # Wait to finish
        self.flink.wait(timeout_sec=1800)

        # Run consume part
        _workload_config['mode'] = "consume"
        all = self._run_workloads(workloads, _workload_config)
        # Assert failed jobs
        assert_failed_jobs(all)

        # Wait to finish
        self.flink.wait(timeout_sec=1800)

        # TODO: Validate according to metrics
        # vectorized_internal_rpc_latency_sum{method="commit_tx",service="tx_gateway",shard="1"} 20029
        # vectorized_internal_rpc_latency_sum{method="commit_tx",service="tx_gateway",shard="2"} 11483
        # vectorized_internal_rpc_latency_sum{method="commit_tx",service="tx_gateway",shard="3"} 21542

        return
