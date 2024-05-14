# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until


def stop_stress(admin: Admin, node: ClusterNode):
    """
    Sends a request to the admin endpoint to stop stress fibers, returning True
    if successful and False if there was an error.
    """
    try:
        admin.stress_fiber_stop(node)
    except:
        return False
    return True


class CpuStressInjectionTest(RedpandaTest):
    def __init__(self, test_context):
        super(CpuStressInjectionTest, self).__init__(test_context,
                                                     num_brokers=1)

    def has_started_stress(self):
        return self.redpanda.search_log_any("Started stress fiber")

    def has_reactor_stalls(self):
        return self.redpanda.search_log_any("Reactor stalled for")

    @cluster(num_nodes=1)
    def test_stress_fibers_ms(self):
        """
        Test that time-based stress fibers can reliably spit out reactor
        stalls.
        """
        admin = Admin(self.redpanda)
        node = self.redpanda.nodes[0]
        try:
            admin.stress_fiber_start(node,
                                     10,
                                     min_ms_per_scheduling_point=30,
                                     max_ms_per_scheduling_point=300)
        except:
            # Ignore errors, since the HTTP endpoint may be stalled behind a
            # fiber.
            pass

        try:
            wait_until(self.has_reactor_stalls, timeout_sec=10, backoff_sec=1)
            assert self.has_started_stress()
        finally:
            wait_until(lambda: stop_stress(admin, node),
                       timeout_sec=30,
                       backoff_sec=1)

    @cluster(num_nodes=1)
    def test_stress_fibers_spins(self):
        """
        Basic test for count-based stress fibers.
        """
        admin = Admin(self.redpanda)
        node = self.redpanda.nodes[0]
        try:
            admin.stress_fiber_start(node,
                                     10,
                                     min_spins_per_scheduling_point=1000,
                                     max_spins_per_scheduling_point=100000)
        except:
            # Ignore errors, since the HTTP endpoint may be stalled behind a
            # fiber.
            pass

        try:
            # A test that relies on counting some number of cycles is subject
            # to being flaky in different environments. Instead of checking for
            # reactor stalls, just look that we started stress fibers.
            wait_until(self.has_started_stress, timeout_sec=10, backoff_sec=1)
        finally:
            wait_until(lambda: stop_stress(admin, node),
                       timeout_sec=30,
                       backoff_sec=1)

    @cluster(num_nodes=1)
    def test_misconfigured_stress_fibers(self):
        """
        Test that misconfiguring does not result in any started stress fibers.
        """
        admin = Admin(self.redpanda)
        node = self.redpanda.nodes[0]
        try:
            admin.stress_fiber_start(node,
                                     10,
                                     min_ms_per_scheduling_point=1000,
                                     max_ms_per_scheduling_point=10)
            assert False, "Expected failure: require ms min < max"
        except:
            pass

        try:
            admin.stress_fiber_start(node,
                                     10,
                                     min_spins_per_scheduling_point=1000,
                                     max_spins_per_scheduling_point=10)
            assert False, "Expected failure: require spins min < max"
        except:
            pass

        try:
            admin.stress_fiber_start(node,
                                     10,
                                     min_ms_per_scheduling_point=None,
                                     max_ms_per_scheduling_point=1000)
            assert False, "Expected failure: require both ms min/max be set"
        except:
            pass

        try:
            admin.stress_fiber_start(node,
                                     10,
                                     min_spins_per_scheduling_point=None,
                                     max_spins_per_scheduling_point=1000)
            assert False, "Expected failure: require both spins min/max be set"
        except:
            pass

        try:
            admin.stress_fiber_start(node,
                                     10,
                                     min_ms_per_scheduling_point=0,
                                     max_ms_per_scheduling_point=100,
                                     min_spins_per_scheduling_point=1000,
                                     max_spins_per_scheduling_point=1000)
            assert False, "Expected failure: require either spins or ms set"
        except:
            pass
        assert not self.has_started_stress()
