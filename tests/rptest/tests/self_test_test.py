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
from collections import defaultdict
from rptest.services.cluster import cluster
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SISettings
from rptest.services.redpanda_installer import RedpandaVersionLine
from rptest.services.redpanda_installer import InstallOptions
from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from rptest.utils.functional import flat_map
from rptest.util import wait_until_result
from math import comb


class SelfTestTest(EndToEndTest):
    """Tests for the redpanda self test feature."""

    SELF_TEST_SHUTDOWN_LOG = [
        re.compile(
            ".*self test finished with error: rpc::errc::shutting_down.*")
    ]

    def __init__(self, ctx):
        super(SelfTestTest, self).__init__(test_context=ctx)

    def wait_for_self_test_completion(self):
        """
        Completion is defined as all brokers reporting an 'idle'
        status in the self_test_status() API
        """
        def all_idle():
            node_reports = self.rpk_client().self_test_status()
            return not any([x['status'] == 'running'
                            for x in node_reports]), node_reports

        return wait_until_result(all_idle, timeout_sec=90, backoff_sec=1)

    @cluster(num_nodes=3)
    def test_self_test(self):
        """Assert the self test starts/completes with success."""
        num_nodes = 3
        self.start_redpanda(
            num_nodes=num_nodes,
            si_settings=SISettings(test_context=self.test_context))
        self.rpk_client().self_test_start(2000, 2000, 5000, 100)

        # Wait for completion
        node_reports = self.wait_for_self_test_completion()

        for node in node_reports:
            assert node['status'] == 'idle'
            assert node.get('results') is not None
            for report in node['results']:
                assert 'error' not in report
                assert 'warning' not in report

        # Ensure the results appear as expected. Assertions aren't performed
        # on specific results, but rather what tests are oberved to have run
        reports = flat_map(lambda node: node['results'], node_reports)

        # Ensure 10 disk tests per node (see the RPK code for the full list)
        disk_results = [r for r in reports if r['test_type'] == 'disk']
        expected_disk_results = num_nodes * 10
        assert len(
            disk_results
        ) == expected_disk_results, f"Expected {expected_disk_results} disk reports observed {len(disk_results)}"

        # Assert properties of the network results hold true
        network_results = [r for r in reports if r['test_type'] == 'network']

        cloud_results = [r for r in reports if r['test_type'] == 'cloud']

        read_tests = ['List', 'Head', 'Get']
        write_tests = ['Put', 'Delete', 'Plural Delete']

        num_expected_cloud_storage_read_tests = num_nodes * len(read_tests)
        num_expected_cloud_storage_write_tests = num_nodes * len(write_tests)
        assert len(
            cloud_results
        ) == num_expected_cloud_storage_write_tests + num_expected_cloud_storage_read_tests

        # Ensure no other result sets exist
        assert len(disk_results) + len(network_results) + len(
            cloud_results) == len(reports)

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

    @cluster(num_nodes=3,
             log_allow_list=RESTART_LOG_ALLOW_LIST + SELF_TEST_SHUTDOWN_LOG)
    def test_self_test_node_crash(self):
        """Assert the self test starts/completes with success."""
        num_nodes = 3
        self.start_redpanda(num_nodes=num_nodes)

        self.rpk_client().self_test_start(3000, 3000)

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
                if report['test_type'] == 'network':
                    if 'error' in report:
                        assert report[
                            'error'] == f'Failed to reach peer with node_id: {stopped_nid}'
                    else:
                        assert 'warning' not in report
        crashed_nodes_report = [
            x for x in node_reports if x['node_id'] == stopped_nid
        ][0]
        assert crashed_nodes_report['status'] == 'unreachable'
        assert crashed_nodes_report.get('results') is None

    @cluster(num_nodes=3)
    def test_self_test_cancellable(self):
        """Assert the self test can cancel an action on command."""
        num_nodes = 3
        self.start_redpanda(num_nodes=num_nodes)
        disk_test_time = 5000  # ms
        network_test_time = 5000  # ms

        # Launch the self test with the options above
        self.rpk_client().self_test_start(disk_test_time, network_test_time)
        start = time.time()

        # Wait a second, then send a stop() request
        time.sleep(1)

        # Stop is synchronous and will return when all jobs have stopped
        self.rpk_client().self_test_stop()

        # Assert that at least a second of total recorded test time has
        # passed between start & stop calls
        stop = time.time()
        total_time_sec = stop - start
        assert total_time_sec < (disk_test_time + network_test_time)

        # Ensure system is in an idle state and contains expected report
        node_reports = self.rpk_client().self_test_status()
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

    @cluster(num_nodes=3)
    def test_self_test_unknown_test_type(self):
        """Assert the self test still runs when an invalid test type is passed.
           This helps ensure the self test will still work in a cluster
           with mixed versions of Redpanda."""
        num_nodes = 3

        self.start_redpanda(
            num_nodes=num_nodes,
            si_settings=SISettings(test_context=self.test_context))

        # Attempt to run with an unknown test type "pandatest"
        # and possibly unknown "cloud" test.
        # The rest of the tests should proceed as normal.
        request_json = {
            'tests': [{
                'type': 'pandatest'
            }, {
                'type': 'disk'
            }, {
                'type': 'network'
            }, {
                'type': 'cloud'
            }]
        }

        # Manually invoke self test admin endpoint.
        self.redpanda._admin._request('POST',
                                      'debug/self_test/start',
                                      json=request_json)

        # Populate list of unknown reports.
        unknown_report_types = ['pandatest']
        redpanda_versions = [
            self.redpanda.get_version_int_tuple(node)
            for node in self.redpanda.nodes
        ]

        # All nodes should have the same version of
        # Redpanda running.
        assert len(set(redpanda_versions)) == 1

        # Cloudcheck was introduced in 24.2.1.
        # Expect that it will be unknown to nodes running
        # earlier versions of redpanda.
        if redpanda_versions[0] < (24, 2, 1):
            unknown_report_types.append('cloud')

        # Wait for self test completion.
        node_reports = self.wait_for_self_test_completion()

        # Assert reports are passing, with the exception of unknown tests.
        reports = flat_map(lambda node: node['results'], node_reports)
        assert len(reports) > 0
        for report in reports:
            if report['test_type'] in unknown_report_types:
                assert 'error' in report
            else:
                assert 'error' not in report
                assert 'warning' not in report

    @cluster(num_nodes=3)
    def test_self_test_mixed_node_controller_lower_version(self):
        """Assert the self test still runs when the controller node
        is of a lower version than the rest of the nodes in the cluster.
        The upgraded follower nodes should be able to parse the "unknown"
        checks (currently just the cloudcheck), and then run and return
        their results to the controller node."""
        num_nodes = 3

        install_opts = InstallOptions(version=RedpandaVersionLine((24, 1)),
                                      num_to_upgrade=2)
        self.start_redpanda(
            num_nodes=num_nodes,
            si_settings=SISettings(test_context=self.test_context),
            install_opts=install_opts,
            license_required=True)

        # Attempt to run with a possibly unknown "cloud" test.
        # The controller, which is of a lower version than the other nodes in the cluster,
        # doesn't recognize "cloud" as a test, but the other nodes should.
        request_json = {
            'tests': [{
                'type': 'cloud',
                'backoff_ms': 100,
                'timeout_ms': 5000
            }]
        }

        redpanda_versions = {
            i: self.redpanda.get_version_int_tuple(node)
            for (i, node) in enumerate(self.redpanda.nodes)
        }

        controller_node_index = min(redpanda_versions,
                                    key=redpanda_versions.get)
        controller_node_id = controller_node_index + 1
        # Make sure that the lowest version node is the controller.
        self.redpanda._admin.partition_transfer_leadership(
            'redpanda', 'controller', 0, controller_node_id)
        wait_until(lambda: self.redpanda._admin.get_partition_leader(
            namespace="redpanda", topic="controller", partition=0) ==
                   controller_node_id,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Leadership did not stabilize")

        # Manually invoke self test admin endpoint, using the lowest version node as the target.
        self.redpanda._admin._request(
            'POST',
            'debug/self_test/start',
            json=request_json,
            node=self.redpanda.nodes[controller_node_index])

        # Wait for self test completion.
        node_reports = self.wait_for_self_test_completion()

        unknown_checks_map = defaultdict(set)
        for node, version in redpanda_versions.items():
            node_id = node + 1
            # Cloudcheck was introduced in 24.2.1.
            # Expect that it will be unknown to nodes running
            # earlier versions of redpanda.
            if version < (24, 2, 1):
                unknown_checks_map[node_id].add('cloud')

        # Assert reports are passing, with the exception of unknown tests.
        assert len(node_reports) > 0
        for report in node_reports:
            node = report['node_id']
            results = report['results']
            # Results shouldn't be empty, even for unknown checks.
            assert len(results) > 0
            for result in results:
                if result['test_type'] in unknown_checks_map[node]:
                    assert 'error' in result
                else:
                    assert 'error' not in result
                    assert 'warning' not in result
