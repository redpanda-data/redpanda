# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import signal

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.offline_log_viewer import OfflineLogViewer
from rptest.services.cluster import cluster
from rptest.utils.mode_checks import skip_fips_mode
from rptest.services.redpanda import LoggingConfig, RedpandaService, ResourceSettings
from rptest.tests.redpanda_test import RedpandaTest

CRASH_LOOP_LOG = [
    "Crash loop detected. Too many consecutive crashes.*",
    ".*Failure during startup: crash_tracker::crash_loop_limit_reached \(Crash loop detected, aborting startup.\).*",
    ".*crash reason to crash file.*",
]

SIGNAL_CRASH_LOG = [
    "Aborting on",
    "Aborting:",
    "Segmentation fault on",
    "Segmentation fault:",
    "Illegal instruction on",
    "Illegal instruction:",
    # ASan stack trace lines (e.g. "#4 0x... in seastar::promise_base::assert_task_shard")
    # and summary lines (e.g. "SUMMARY: AddressSanitizer: SEGV ... in ...::assert_task_shard")
    # In debug mode, the ASan output may include function names with 'assert' in them depending
    # on what is on the stack when the signal is received by the process. This 'assert' text
    # would trigger the bad log lines check. To avoid that, we exclude ASan output lines here.
    r"#\d+ 0x[0-9a-f]+ in ",
    "SUMMARY: AddressSanitizer:",
]

ASSERT_CRASH_LOG = ["assert - "]

# main - application.cc:348 - Failure during startup: std::__1::system_error (error C-Ares:4, unreachable_host.com: Not found)
# main - application.cc:363 - Failure during startup: std::__1::system_error (error C-Ares:11, unreachable_host.com: Connection refused)
HOSTNAME_ERRORS = [
    ".*Failure during startup: std::__1::system_error \(error C-Ares:4, unreachable_host.com: Not found\)",
    ".*Failure during startup: std::__1::system_error \(error C-Ares:11, unreachable_host.com: Connection refused\)",
]

CLOUD_STORAGE_CLIENT_CONFIG_ERRORS = [
    ".*[Ss]elf.configuration.*",
    ".*Cloud storage client self-configuration failed.*",
    ".*InvalidAccessKeyId.*",
    ".*Couldn't reach S3.*",
]


class CrashLoopChecksTest(RedpandaTest):
    "Checks crash loop detection works as expected."

    CRASH_LOOP_LIMIT = 3

    CRASH_LOOP_TRACKER_FILE = f"{RedpandaService.DATA_DIR}/startup_log"
    CRASH_REPORTS_DIR = f"{RedpandaService.DATA_DIR}/crash_reports"

    def __init__(self, test_context):
        super(CrashLoopChecksTest, self).__init__(
            test_context=test_context,
            num_brokers=1,
            extra_node_conf={
                "crash_loop_limit": CrashLoopChecksTest.CRASH_LOOP_LIMIT,
                "developer_mode": False,
            },
            log_config=LoggingConfig(
                "info", logger_levels={"main": "debug", "crash_tracker": "trace"}
            ),
            # Disable core dumps as they take a long time (>1min). Core dumps are uninteresting for
            # this test, since this test intentionally triggers crashes.
            resource_settings=ResourceSettings(core_dump_limit="0"),
        )
        self.broker = self.redpanda.nodes[0]

    def remove_crash_loop_tracker_file(self, broker):
        broker.account.ssh(f"rm -f {CrashLoopChecksTest.CRASH_LOOP_TRACKER_FILE}")

    def count_crash_files(self, broker):
        return int(
            broker.account.ssh_output(
                f'find "{CrashLoopChecksTest.CRASH_REPORTS_DIR}" -type f | wc -l',
                combine_stderr=False,
            ).strip()
        )

    def get_broker_to_crash_loop_state(self, broker):
        for _ in range(CrashLoopChecksTest.CRASH_LOOP_LIMIT):
            self.redpanda.signal_redpanda(node=broker)
            self.redpanda.start_node(broker)
        self.redpanda.signal_redpanda(node=broker)
        self.redpanda.start_node(node=broker, expect_fail=True)

    def expect_crash_count(self, expected):
        crash_files = self.count_crash_files(self.broker)
        assert crash_files == expected, (
            f"Unexpected number of crashes: {crash_files} != {expected}"
        )

    def wait_for_redpanda_stop(self, broker, timeout=10):
        """
        Wait for the redpanda process to terminate (e.g. after sending a crash signal)
        """
        wait_until(
            lambda: self.redpanda.redpanda_pid(broker) is None,
            timeout_sec=timeout,
            backoff_sec=0.2,
            err_msg=f"Redpanda processes did not terminate on {broker.name} in {timeout} sec",
        )

    def read_first_crash_report(self):
        viewer = OfflineLogViewer(self.redpanda)
        crash_reports = viewer.read_crash_reports(self.broker)
        self.logger.debug(f"Crash reports: {crash_reports}")
        assert len(crash_reports) > 0, "No crash reports found"
        report = next(iter(crash_reports.values()))
        self.logger.debug(f"First report: {report}")

        # Run some standard checks across all crash reports we read
        assert len(report["app_version"]) > 0, (
            f"Unexpected empty app_version for report: {report}"
        )
        assert len(report["arch"]) > 0, f"Unexpected empty arch for report: {report}"

        return report

    @cluster(num_nodes=1, log_allow_list=CRASH_LOOP_LOG)
    def test_crash_loop_checks_with_tracker_file(self):
        broker = self.redpanda.nodes[0]
        self.get_broker_to_crash_loop_state(broker)
        # Remove the crash loop log and restart, should start up.
        self.remove_crash_loop_tracker_file(broker)
        self.redpanda.start_node(broker)

    @cluster(num_nodes=1, log_allow_list=CRASH_LOOP_LOG)
    def test_crash_loop_checks_with_node_config(self):
        broker = self.redpanda.nodes[0]
        self.get_broker_to_crash_loop_state(broker)
        # Update node configuration file to reset checksum
        update = dict(kafka_api=dict(address="127.0.0.1", port=9099))
        self.redpanda.start_node(broker, override_cfg_params=update)

    @cluster(num_nodes=1, log_allow_list=CRASH_LOOP_LOG + HOSTNAME_ERRORS)
    def test_crash_loop_with_misconfiguration(self):
        broker = self.redpanda.nodes[0]
        self.redpanda.signal_redpanda(broker)

        invalid_conf = dict(kafka_api=dict(address="unreachable_host.com", port=9092))
        for _ in range(CrashLoopChecksTest.CRASH_LOOP_LIMIT + 1):
            self.redpanda.start_node(
                broker, override_cfg_params=invalid_conf, expect_fail=True
            )
        # None of the attempts so far should be considered a crash loop.
        assert not self.redpanda.search_log_node(broker, "Too many consecutive crashes")

        # Start again, crash loop should be detected.
        self.redpanda.start_node(
            broker, override_cfg_params=invalid_conf, expect_fail=True
        )
        assert self.redpanda.search_log_node(broker, "Too many consecutive crashes")

        # Fix the config and start, crash loop should be reset.
        self.redpanda.start_node(node=broker)

    @cluster(num_nodes=1, log_allow_list=CRASH_LOOP_LOG)
    def test_crash_loop_tracker_reset_via_recovery_mode(self):
        broker = self.redpanda.nodes[0]
        self.get_broker_to_crash_loop_state(broker)
        cfg = {"recovery_mode_enabled": True}
        self.redpanda.start_node(broker, override_cfg_params=cfg)

        # reset crash tracking explicitly
        admin = self.redpanda._admin
        admin.reset_crash_tracking(node=broker)
        assert self.redpanda.search_log_node(broker, "Deleted crash loop tracker file")
        # stop + restart without recovery mode.
        self.redpanda.stop_node(broker)
        self.redpanda.start_node(broker)

    @cluster(num_nodes=1, log_allow_list=CRASH_LOOP_LOG)
    def test_crash_loop_sleep(self):
        broker = self.redpanda.nodes[0]

        self.redpanda.add_extra_node_conf(broker, {"crash_loop_sleep_sec": 3})
        self.redpanda.restart_nodes(broker)

        for _ in range(CrashLoopChecksTest.CRASH_LOOP_LIMIT):
            self.redpanda.signal_redpanda(node=broker)
            self.redpanda.start_node(broker)
        self.redpanda.signal_redpanda(node=broker)

        # Expect the redpanda process to sleep for crash_loop_sleep_sec
        self.redpanda.start_node(node=broker, expect_fail=True)
        assert self.redpanda.search_log_node(broker, "Too many consecutive crashes")
        assert self.redpanda.search_log_node(
            broker, "Sleeping for 3 seconds before terminating..."
        )

    @cluster(num_nodes=1, log_allow_list=CRASH_LOOP_LOG + HOSTNAME_ERRORS)
    def test_crash_report_with_startup_exception(self):
        broker = self.redpanda.nodes[0]

        # A SIGKILL'd broker will leave behind an empty crash report
        self.redpanda.signal_redpanda(broker)
        self.expect_crash_count(1)

        # A clean broker start+stop will not leave behind a crash report
        self.redpanda.start_node(broker)
        self.redpanda.stop_node(broker)
        self.expect_crash_count(1)

        # Exceptions during startup should generate crash reports
        invalid_conf = dict(kafka_api=dict(address="unreachable_host.com", port=9092))
        for _ in range(CrashLoopChecksTest.CRASH_LOOP_LIMIT + 1):
            self.redpanda.start_node(
                broker, override_cfg_params=invalid_conf, expect_fail=True
            )
        self.expect_crash_count(1 + CrashLoopChecksTest.CRASH_LOOP_LIMIT + 1)

        # No new crash report should be generated for when redpanda stops with the crash loop limit reached
        self.redpanda.start_node(
            broker, override_cfg_params=invalid_conf, expect_fail=True
        )
        assert self.redpanda.search_log_node(broker, "Too many consecutive crashes")
        assert self.redpanda.search_log_node(
            broker,
            "Crash #4 at 20.* UTC - Redpanda version: .*. Failure during startup: std::__1::system_error (error C-Ares:4, unreachable_host.com: Not found) Backtrace: .*",
        )
        self.expect_crash_count(1 + CrashLoopChecksTest.CRASH_LOOP_LIMIT + 1)

    @cluster(num_nodes=1, log_allow_list=CRASH_LOOP_LOG + HOSTNAME_ERRORS)
    def test_crash_report_parser(self):
        broker = self.redpanda.nodes[0]
        self.redpanda.signal_redpanda(broker)

        invalid_conf = dict(kafka_api=dict(address="unreachable_host.com", port=9092))
        self.redpanda.start_node(
            broker, override_cfg_params=invalid_conf, expect_fail=True
        )

        report = self.read_first_crash_report()
        assert (
            "Failure during startup: std::__1::system_error (error C-Ares:4, unreachable_host.com: Not found)"
            == report["crash_message"]
        ), f"Unexpected crash message: {report['crash_message']}"
        assert len(report["stacktrace"]) > 0, (
            f"Unexpected empty stacktrace for report: {report}"
        )

    @cluster(num_nodes=1, log_allow_list=CRASH_LOOP_LOG + SIGNAL_CRASH_LOG)
    @matrix(signo=[signal.SIGSEGV, signal.SIGABRT, signal.SIGILL], signal_shard=[0, 1])
    def test_crash_report_with_signal(self, signo, signal_shard):
        if signal_shard == 0:
            signal_thread = RedpandaService.SHARD_0_THREAD_NAME
        else:
            signal_thread = RedpandaService.SHARD_1_THREAD_NAME

        self.redpanda.set_tolerate_crashes(True)
        broker = self.redpanda.nodes[0]

        # Send a crash signal to redpanda CRASH_LOOP_LIMIT times
        for _ in range(CrashLoopChecksTest.CRASH_LOOP_LIMIT):
            self.redpanda.signal_redpanda(broker, signal=signo, thread=signal_thread)
            self.wait_for_redpanda_stop(broker)
            self.redpanda.start_node(broker)

        # Expect to see a crash report for each crash + a new one for the last
        # start_node
        self.expect_crash_count(CrashLoopChecksTest.CRASH_LOOP_LIMIT + 1)

        # Sanity check the crash loop limit message has not been printed yet
        assert not self.redpanda.search_log_node(
            broker, "Too many consecutive crashes"
        ), "The crash loop limit message should not have been printed yet"

        # Send a crash signal + start again, now reaching the crash loop limit.
        self.redpanda.signal_redpanda(broker, signal=signo, thread=signal_thread)
        self.wait_for_redpanda_stop(broker)
        self.redpanda.start_node(broker, expect_fail=True)

        # Assert the crash loop limit message is printed with information about
        # the crashes
        assert self.redpanda.search_log_node(broker, "Too many consecutive crashes"), (
            "The crash loop limit should have been reached"
        )

        def signo_prefix():
            if signo == signal.SIGSEGV:
                return "Segmentation fault"
            elif signo == signal.SIGABRT:
                return "Aborting"
            elif signo == signal.SIGILL:
                return "Illegal instruction"
            else:
                assert False, "Test failure: not yet implemented"

        for i in range(1, CrashLoopChecksTest.CRASH_LOOP_LIMIT + 2):
            assert self.redpanda.search_log_node(
                broker,
                f"Crash #{i} at 20.* - Redpanda version: .*. {signo_prefix()} on shard {signal_shard}. Backtrace: ",
            ), (
                f"The #{i} crash description in the crash loop limit log message is missing or malformed"
            )

        report = self.read_first_crash_report()
        assert len(report["stacktrace"]) > 0, (
            f"Unexpected empty stacktrace for report: {report}"
        )

    @cluster(
        num_nodes=1, log_allow_list=CRASH_LOOP_LOG + SIGNAL_CRASH_LOG + ASSERT_CRASH_LOG
    )
    @matrix(signal_shard=[0, 1])
    def test_vassert_message(self, signal_shard: int):
        if not self.debug_mode:
            self.logger.info("Skipping test, endpoints only exist in debug mode")
            return
        admin = self.redpanda._admin
        msg = f"Message from shard {signal_shard}"
        if signal_shard == 0:
            signal_thread = RedpandaService.SHARD_0_THREAD_NAME
        else:
            signal_thread = RedpandaService.SHARD_1_THREAD_NAME

        self.redpanda.set_tolerate_crashes(True)
        admin.put_ctracker_va_message(shard=signal_shard, msg=msg, node=self.broker)

        # Use SIGILL as it's the signal raised on assert and we want
        # to ensure that our assert message inserted above is what
        # was written to the crash report and not anything from the
        # signal handler
        self.redpanda.signal_redpanda(
            self.broker, signal=signal.SIGILL, thread=signal_thread
        )

        report = self.read_first_crash_report()
        assert 5 == report["type"], f"Unexpected crash type: {report['type']}"
        assert f"{msg} on shard {signal_shard}." == report["crash_message"], (
            f"Unexpected crash message: {report['crash_message']}"
        )

    @skip_fips_mode
    @cluster(num_nodes=1, log_allow_list=CLOUD_STORAGE_CLIENT_CONFIG_ERRORS)
    def test_cloud_storage_client_misconfiguration(self):
        """
        Test that cloud storage self-configuration failures are recorded
        in the crash tracker.
        """
        broker = self.redpanda.nodes[0]

        self.redpanda.stop_node(broker)
        self.redpanda.clean_node(broker)

        self.redpanda.add_extra_rp_conf(
            {
                "cloud_storage_enabled": True,
                "cloud_storage_access_key": "FAKEACCESSKEYID",
                "cloud_storage_secret_key": "fakesecretaccesskey0123456789ABCDEFGHIJK",
                "cloud_storage_region": "us-east-1",
                "cloud_storage_bucket": "test-bucket",
                "cloud_storage_api_endpoint": "s3.us-east-1.amazonaws.com",
            }
        )
        self.redpanda.write_bootstrap_cluster_config()

        self.redpanda.start_node(broker, first_start=True, skip_readiness_check=True)
        self.wait_for_redpanda_stop(broker, timeout=60)

        self.expect_crash_count(1)
        report = self.read_first_crash_report()
        assert (
            "Cloud storage client self-configuration failed" in report["crash_message"]
        )
