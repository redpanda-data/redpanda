# Copyright 2023 Redpanda Data, Inc.
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
from rptest.services.redpanda import RedpandaService, FAILURE_INJECTION_LOG_ALLOW_LIST
from rptest.services.redpanda_monitor import RedpandaMonitor
from rptest.services.storage_failure_injection import FailureInjectionConfig, NTPFailureInjectionConfig, FailureConfig, NTP, Operation, BatchType
from rptest.clients.rpk import RpkTool, RpkException
from rptest.util import expect_exception
from ducktape.utils.util import wait_until


class StorageFailureInjectionTest(RedpandaTest):
    ntp: NTP = NTP(topic="ftest", partition=0)

    def __init__(self, test_context):
        super(StorageFailureInjectionTest, self).__init__(test_context,
                                                          num_brokers=1)

    def setUp(self):
        pass

    def generate_failure_injection_config(self) -> FailureInjectionConfig:
        fcfg = FailureConfig(operation=Operation.write,
                             batch_type=BatchType.raft_data,
                             failure_probability=100)
        ntp_fcfg = NTPFailureInjectionConfig(ntp=self.ntp,
                                             failure_configs=[fcfg])

        return FailureInjectionConfig(seed=0, ntp_failure_configs=[ntp_fcfg])

    @cluster(num_nodes=2, log_allow_list=FAILURE_INJECTION_LOG_ALLOW_LIST)
    def test_storage_failure_injection(self):
        """
        Simple test that enables storage failure injection for a redpanda node.
        It can be used as a template for more complex failure injection tests.
        Roughly it performs the following:
        1. Set up a failure injection configuration
            * generate config
            * write config to file
            * prepare node level finject config
        2. Start the RedpandaMonitor service to bring the node back after the crash
        3. Enable failure injection via the admin API and watch the node crash and come back
        """
        lonely_node = self.redpanda.nodes[0]

        fail_cfg = self.generate_failure_injection_config()
        self.redpanda.set_up_failure_injection(finject_cfg=fail_cfg,
                                               enabled=False,
                                               nodes=[lonely_node],
                                               tolerate_crashes=True)

        self.redpanda.start()
        RedpandaMonitor(self.test_context, self.redpanda).start()

        rpk = RpkTool(self.redpanda)
        rpk.create_topic(self.ntp.topic, partitions=1, replicas=1)

        rpk.produce(self.ntp.topic, key="don't", msg="crash")

        Admin(self.redpanda).set_storage_failure_injection(lonely_node,
                                                           value=True)

        # Perform a few writes and expect Redpanda to crash.
        # An error should be injected when the first write is dispatched
        # to disk. That should happen as part of handling the first request.
        # Subsequent produce requests are a convenient way of detecting the crash.
        exception = None
        for _ in range(3):
            try:
                rpk.produce(self.ntp.topic,
                            key="poisoned",
                            msg="chalice",
                            timeout=1)
            except RpkException as e:
                exception = e

            if exception:
                break

            time.sleep(1)

        if exception is None:
            assert False, "Expected produce to trigger an assertion within Redpanda"

        wait_until(
            lambda: next(rpk.describe_topic(self.ntp.topic)).high_watermark ==
            1,
            # After the assertion caused by the failure injection,
            # the kernel needs to release resources associated with the process.
            # For some reason, this takes suprisingly long on CDT nodes,
            # hence the generous timeout.
            timeout_sec=60 * 4,
            backoff_sec=10,
            err_msg="Node did not come back after crash or HWM is wrong",
            retry_on_exc=True)

        rpk.produce(self.ntp.topic, key="don't", msg="crash")
        wait_until(lambda: next(rpk.describe_topic(self.ntp.topic)).
                   high_watermark == 2,
                   timeout_sec=3,
                   backoff_sec=1,
                   err_msg="HWM did not advance after crash")

        # Assert that the node was restarted by RedpandaMonitor
        assert self.redpanda.count_log_node(lonely_node, "Welcome") == 2
