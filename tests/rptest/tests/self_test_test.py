# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import time
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from ducktape.utils.util import wait_until
from rptest.utils.functional import flat_map
from rptest.util import wait_until_result
from math import comb


class SelfTestTest(RedpandaTest):
    """Tests for the redpanda self test feature."""
    def __init__(self, ctx):
        super(SelfTestTest, self).__init__(test_context=ctx)
        self._rpk = RpkTool(self.redpanda)

    def wait_for_self_test_completion(self):
        """
        Completion is defined as all brokers reporting an 'idle'
        status in the self_test_status() API
        """
        def all_idle():
            node_reports = self._rpk.self_test_status()
            return not any([x['status'] == 'running'
                            for x in node_reports]), node_reports

        return wait_until_result(all_idle, timeout_sec=30, backoff_sec=1)

    @cluster(num_nodes=3)
    def test_self_test(self):
        """Assert the self test starts/completes with success."""
        self._rpk.self_test_start(2000, 2000)

        # Wait for completion
        node_reports = self.wait_for_self_test_completion()

        # Verify returned results
        for node in node_reports:
            assert node['status'] == 'idle'
            assert node.get('results') is not None
            for report in node['results']:
                assert 'error' not in report
                assert 'warning' not in report
                assert report['duration'] > 0, report['duration']

        num_nodes = 3

        # Ensure the results appear as expected. Assertions aren't performed
        # on specific results, but rather what tests are oberved to have run
        reports = flat_map(lambda node: node['results'], node_reports)

        # Ensure 4 disk tests per node, read/write & latency/throughput
        disk_results = [r for r in reports if r['test_type'] == 'disk']
        expected_disk_results = num_nodes * 4
        assert len(
            disk_results
        ) == expected_disk_results, f"Expected {expected_disk_results} disk reports observed {len(disk_results)}"

        # Assert properties of the network results hold true
        network_results = [r for r in reports if r['test_type'] == 'network']

        # Ensure no other result sets exist
        assert len(disk_results) + len(network_results) == len(reports)

        # Ensure nCr network test reports, clusterwide
        assert len(network_results) == comb(
            num_nodes, 2
        ), f"Expected {comb(num_nodes,2)} reports observed {len(network_results)}"

        # Assert correct combinations of nodes were chosen. The network self
        # test should only be run on unique pairs of nodes
        def seperate_pairings(result):
            # Returns a 2-tuple of type (int, int[]) where the first parameter
            # is the node that issued the netcheck benchmark (client) and the
            # second parameter is the list of servers the test was run against
            network_results = [
                r['info'] for r in result['results']
                if r['test_type'] == 'network'
            ]
            matcher = re.compile(r".*:\s(\d+)")
            mmxs = [matcher.match(r) for r in network_results]
            assert all([r is not None for r in mmxs]), "Failed to match regex"
            return (int(result['node_id']), [int(r[1]) for r in mmxs])

        # Should be something like: {0: [1,2], 1: [2,3,4], ... etc}
        netcheck_pairings = [seperate_pairings(r) for r in node_reports]
        netcheck_pairings = {k: v for k, v in netcheck_pairings}
        self.logger.debug(f"netcheck_pairings: {netcheck_pairings}")
        for client, servers in netcheck_pairings.items():
            for server in servers:
                # Assert that for any (client, server) pair, there is not
                # a corresponding (server, client) pair.
                sxs = netcheck_pairings.get(server, [])
                assert client not in sxs

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_self_test_node_crash(self):
        """Assert the self test starts/completes with success."""
        self._rpk.self_test_start(3000, 3000)

        # Allow for some work be done
        time.sleep(1)

        # Crash a node
        stopped_nid = self.redpanda.idx(self.redpanda.nodes[0])
        self.logger.info(f"Killing node {stopped_nid}")
        self.redpanda.stop_node(self.redpanda.get_node(stopped_nid))

        # Wait for completion
        node_reports = self.wait_for_self_test_completion()

        # Verify returned results
        good_node_reports = [
            x for x in node_reports if x['node_id'] != stopped_nid
        ]
        for node in good_node_reports:
            assert node['status'] == 'idle'
            assert node.get('results') is not None
            for report in node['results']:
                # Errors related to crashed node are allowed, for example a network test on a good node that had attempted to connect to the crashed node
                if 'error' in report:
                    assert report[
                        'error'] == f'Failed to reach peer with node_id: {stopped_nid}'
                else:
                    assert 'warning' not in report
                    assert report['duration'] > 0, report['duration']
        crashed_nodes_report = [
            x for x in node_reports if x['node_id'] == stopped_nid
        ][0]
        assert crashed_nodes_report['status'] == 'unreachable'
        assert crashed_nodes_report.get('results') is None

    @cluster(num_nodes=3)
    def test_self_test_cancellable(self):
        """Assert the self test can cancel an action on command."""
        disk_test_time = 5000  # ms
        network_test_time = 5000  # ms

        # Launch the self test with the options above
        self._rpk.self_test_start(disk_test_time, network_test_time)
        start = time.time()

        # Wait a second, then send a stop() request
        time.sleep(1)

        # Stop is synchronous and will return when all jobs have stopped
        self._rpk.self_test_stop()

        # Assert that at least a second of total recorded test time has
        # passed between start & stop calls
        stop = time.time()
        total_time_sec = stop - start
        assert total_time_sec < (disk_test_time + network_test_time)

        # Ensure system is in an idle state and contains expected report
        node_reports = self._rpk.self_test_status()
        for node in node_reports:
            assert node['status'] == 'idle'
            assert node.get('results') is not None
            for report in node['results']:
                assert 'error' not in report, report['error']
                # Match for string 'cancel' in warning
                assert 'warning' in report
                assert report['warning'].find('cancel') != -1
                # If test was running it was cancelled otherwise it was
                # cancelled before it even had a chance to start, resulting in
                # a 0 value for duration
                assert report['duration'] >= 0
