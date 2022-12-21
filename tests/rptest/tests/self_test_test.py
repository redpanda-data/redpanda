# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from ducktape.utils.util import wait_until


class SelfTestTest(RedpandaTest):
    """Tests for the redpanda self test feature."""
    def __init__(self, ctx):
        super(SelfTestTest, self).__init__(test_context=ctx)
        self._admin = Admin(self.redpanda)

    def wait_for_self_test_completion(self, timeout):
        """
        Completion is defined as all brokers reporting an 'idle'
        status in the self_test_status() API
        """
        def all_idle():
            node_reports = self._admin.self_test_status()
            return not any([x['status'] == 'running' for x in node_reports])

        wait_until(all_idle, timeout_sec=timeout, backoff_sec=1)

    @cluster(num_nodes=3)
    def test_self_test(self):
        """Assert the self test starts/completes with success."""
        test_options = {
            'disk_test_execution_time': 1,
            'network_test_execution_time': 1
        }

        # Launch the self test with the options above
        assert self._admin.self_test_start(test_options).status_code == 200

        # Wait for completion
        self.wait_for_self_test_completion(5)

        # Verify returned results
        node_reports = self._admin.self_test_status()
        for node in node_reports:
            assert node['status'] == 'idle'
            assert node.get('results') is not None
            for report in node['results']:
                assert 'error' not in report
                assert 'warning' not in report
                assert 'duration' in report
                assert report['duration'] >= 1000, report['duration']

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_self_test_node_crash(self):
        """Assert the self test starts/completes with success."""
        test_options = {
            'disk_test_execution_time': 3,
            'network_test_execution_time': 3
        }

        # Launch the self test with the options above
        assert self._admin.self_test_start(test_options).status_code == 200

        # Allow for some work be done
        time.sleep(1)

        # Crash a node
        stopped_nid = self.redpanda.idx(self.redpanda.nodes[0])
        self.logger.info(f"Killing node {stopped_nid}")
        self.redpanda.stop_node(self.redpanda.get_node(stopped_nid))

        # Wait for completion
        self.wait_for_self_test_completion(7)

        # Verify returned results
        all_status = self._admin.self_test_status()
        good_node_reports = [
            x for x in all_status if x['node_id'] != stopped_nid
        ]
        for node in good_node_reports:
            assert node['status'] == 'idle'
            assert node.get('results') is not None
            for report in node['results']:
                assert 'error' not in report
                assert 'warning' not in report
                assert 'duration' in report
                assert report['duration'] >= 1000, report['duration']
        crashed_nodes_report = [
            x for x in all_status if x['node_id'] == stopped_nid
        ][0]
        assert crashed_nodes_report['status'] == 'unreachable'
        assert crashed_nodes_report.get('results') is None

    @cluster(num_nodes=3)
    def test_self_test_cancellable(self):
        """Assert the self test can cancel an action on command."""
        test_options = {
            'disk_test_execution_time': 5,
            'network_test_execution_time': 5
        }

        # Launch the self test with the options above
        start = time.time()
        assert self._admin.self_test_start(test_options).status_code == 200

        # Wait a second, then send a stop() request
        time.sleep(1)

        # Stop is synchronous and will return when all jobs have stopped
        assert self._admin.self_test_stop().status_code == 200

        # Assert that at least a second of total recorded test time has
        # passed between start & stop calls
        stop = time.time()
        total_time_sec = stop - start
        assert total_time_sec < (test_options['disk_test_execution_time'] +
                                 test_options['network_test_execution_time'])

        # Ensure system is in an idle state and contains expected report
        node_reports = self._admin.self_test_status()
        for node in node_reports:
            assert node['status'] == 'idle'
            assert node.get('results') is not None
            for report in node['results']:
                assert 'error' not in report
                assert 'warning' in report
                assert 'duration' in report
                # If test was running it was cancelled otherwise it was
                # cancelled before it even had a chance to start, resulting in
                # a 0 value for duration
                assert report['duration'] >= 0
