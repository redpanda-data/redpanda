# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json

from ducktape.utils.util import wait_until

from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.tests.crash_loop_checks_test import HOSTNAME_ERRORS
from rptest.tests.metrics_reporter_test import MetricsReporterServer
from rptest.tests.redpanda_test import RedpandaTest


class CrashReporterServer(MetricsReporterServer):
    def crash_reports(self):
        return [
            json.loads(r["body"])
            for r in self.requests()
            if r["path"] == "/metrics/crash_reports"
        ]


class CrashReporterTest(RedpandaTest):
    def __init__(self, test_ctx):
        self.telemetry = CrashReporterServer(test_ctx)
        super(CrashReporterTest, self).__init__(
            test_context=test_ctx, num_brokers=3, extra_rp_conf=self.telemetry.rp_conf()
        )

    def setUp(self):
        # Start HTTP server before redpanda to avoid connection errors
        self.telemetry.start()
        self.redpanda.start()

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST + HOSTNAME_ERRORS)
    def test_redpanda_crash_reporting(self):
        crashing_broker = self.redpanda.nodes[0]
        self.logger.info(f"Triggering a crash on {crashing_broker.name}")
        self.redpanda.stop_node(crashing_broker)
        invalid_conf = dict(kafka_api=dict(address="unreachable_host.com", port=9092))
        self.redpanda.start_node(
            crashing_broker, override_cfg_params=invalid_conf, expect_fail=True
        )

        self.logger.info(f"Restarting {crashing_broker.name} after the crash")
        self.redpanda.start_node(crashing_broker)

        def _request_received():
            if self.telemetry.crash_reports():
                r = self.telemetry.crash_reports()[-1]
                self.logger.info(f"Latest request: {r}")
                return True
            else:
                self.logger.info("No requests yet")
                return False

        # Crash report uploads can fail in CI due to aborted connections, so allow extra time for retries
        timeout_s = 40
        self.logger.info(f"Waiting {timeout_s}s for crash report to be posted")
        wait_until(_request_received, timeout_s, backoff_sec=1)
        self.telemetry.stop()

        self.logger.info("Verifying the content of the crash report requests")
        crash_reports = self.telemetry.crash_reports()

        assert len(crash_reports) == 1, f"Unexpected: {len(crash_reports)=}"

        report = crash_reports[0]
        assert len(report["cluster_uuid"]) > 0, f"Unexpected: {report['cluster_uuid']=}"

        crashes = report["items"]
        assert len(crashes) == 1, f"Unexpected: {len(crashes)=}"

        crash = crashes[0]
        assert int(crash["timestamp"]) > 1740422170009, (
            f"Unexpected: {crash['timestamp']=}"
        )
        assert crash["node_id"] == 1, f"Unexpected: {crash['node_id']=}"
        assert len(crash["stacktrace"]) > 0, f"Unexpected: {crash['stacktrace']=}"
        assert crash["reason"] == "startup_exception", f"Unexpected: {crash['reason']=}"
        assert crash["description"] == "", f"Unexpected: {crash['description']}"
        assert len(crash["app_version"]) > 0, f"Unexpected: {crash['app_version']}"
        assert len(crash["arch"]) > 0, f"Unexpected: {crash['arch']}"
