# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from ducktape.mark import ignore

from rptest.clients.types import TopicSpec
from rptest.services.flink import FlinkService
from rptest.tests.redpanda_test import RedpandaTest


class FlinkBasicTests(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        # Init parent
        super(FlinkBasicTests, self).__init__(test_context, log_level="trace")

        # Prepare FlinkService
        self.topics = [TopicSpec()]
        self.flink = FlinkService(test_context, self.redpanda, self.topic)

        return

    @ignore
    def test_basic_workload(self):
        # Start Flink
        self.flink.start()

        # Load python workload to target node
        # Hardcoded file
        # TODO: Implement simple workload
        _workload = "/home/ubuntu/tests/rptests/e2e_tests/workloads/" \
                    "flink_simple_workload.py"
        _workload_config = {
            "log_level": "DEBUG",
            "brokers": self.redpanda.brokers(),
            "producer_group": "flink_produce_group",
            "consumer_group": "flink_consume_group",
            "topic_name": "flink_workload_topic",
            "msg_size": 4096,
            "count": 10
        }

        # Add script as a job
        _ids = self.flink.run_flink_job(_workload, _workload_config)
        if _ids is None:
            raise RuntimeError(f"Failed to run job on flink: {_workload}")

        self.logger.debug(f"Workload '{_workload}' generated {len(_ids)} "
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
