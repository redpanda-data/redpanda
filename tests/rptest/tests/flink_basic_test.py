# Copyright 2023 Redpanda Data, Inc.
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


class FlinkBasicTests(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        # Init parent
        super(FlinkBasicTests, self).__init__(test_context, log_level="trace")

        # Prepare FlinkService
        self.topic_name = "flink_workload_topic"
        self.topics = [TopicSpec(name=self.topic_name)]
        self.flink = FlinkService(test_context, self.redpanda, self.topic)
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
        self.kafkacli.delete_topic(self.topic)
        return super().tearDown()

    @cluster(num_nodes=4)
    def test_basic_workload(self):
        # Currently test is failed on data processing

        # Start Flink
        self.flink.start()

        # Load python workload to target node
        # Hardcoded file
        # TODO: Add workload config management
        tags = ['flink', 'produce', 'basic']
        workloads = self.workload_manager.get_workloads(tags)
        if len(workloads) < 1:
            raise RuntimeError("No workloads found "
                               f"with tags: {', '.join(tags)}")
        _workload_config = {
            "log_level": "DEBUG",
            "brokers": self.redpanda.brokers(),
            "producer_group": "flink_produce_group",
            "consumer_group": "flink_consume_group",
            "topic_name": self.topic_name,
            "msg_size": 4096,
            "count": 10
        }
        for workload in workloads:
            # Add script as a job
            self.logger.info(f"Adding {workload['name']} to flink")
            _ids = self.flink.run_flink_job(workload['path'], _workload_config)
            if _ids is None:
                raise RuntimeError("Failed to run job on flink for "
                                   f"workload: {workload['name']}")

            self.logger.debug(f"Workload '{workload['name']}' "
                              f"generated {len(_ids)} "
                              f"jobs: {', '.join(_ids)}")

        # Wait for jobs to finish
        self.flink.wait(timeout_sec=3600)

        # Collect failed jobs
        _failed = []
        for _id in _ids:
            _job = self.flink.get_job_by_id(_id)
            if _job['state'] == self.flink.STATE_FAILED:
                self.logger.warning(f"Job '{_id}' has failed")
                _failed.append(_job)

        # Stop flink
        self.flink.stop()

        # Assert failed jobs
        assert len(_failed) == 0, \
            f"Flink reports failed jobs for the workload {_workload}"

        return
