# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import ducktape.errors
import requests.exceptions
import urllib.parse
import json

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.utils.utf8 import CONTROL_CHARS_MAP
from rptest.util import expect_exception


class LogLevelTest(RedpandaTest):
    initial_log_level = "trace"
    MAX_LOG_EXPIRY_S = 5
    extra_node_conf = {'verbose_logging_timeout_sec_max': MAX_LOG_EXPIRY_S}

    def __init__(self, *args, **kwargs):
        # Set an explicit log level rather than relying on the externally
        # configurable redpanda log level, so that the test knows where
        # it will start.
        super().__init__(*args,
                         log_level=self.initial_log_level,
                         extra_node_conf=self.extra_node_conf,
                         **kwargs)

    @cluster(num_nodes=3)
    def test_get_loggers(self):
        admin = Admin(self.redpanda)
        node = self.redpanda.nodes[0]
        loggers = admin.get_loggers(node)
        # Check for some basic loggers.
        expected_loggers = ["storage", "httpd", "kafka", "io"]
        assert all([l in loggers for l in expected_loggers
                    ]), "Expected at least {expected_loggers}: {loggers}"

        # Any logger we get we should be able to set.
        for logger in loggers:
            # Skip to avoid bad log lines.
            if logger == "assert":
                continue

            with self.redpanda.monitor_log(node) as mon:
                admin.set_log_level(logger, "info")
                mon.wait_until(f"Set log level for {{{logger}}}: .* -> info",
                               timeout_sec=5,
                               backoff_sec=1,
                               err_msg=f"Never saw message for {{{logger}}}")

    @cluster(num_nodes=3)
    def test_log_level_control(self):
        admin = Admin(self.redpanda)
        node = self.redpanda.nodes[0]

        # set to warn level. message seen at trace
        with self.redpanda.monitor_log(node) as mon:
            admin.set_log_level("admin_api_server", "warn")
            mon.wait_until(
                f"Set log level for {{admin_api_server}}: {self.initial_log_level} -> warn",
                timeout_sec=5,
                backoff_sec=1,
                err_msg="Never saw message")

        # set to debug. log level at warn, so shouldn't see it
        try:
            with self.redpanda.monitor_log(node) as mon:
                admin.set_log_level("admin_api_server", "debug", force=True)
                mon.wait_until(
                    "Set log level for {admin_api_server}: warn -> debug",
                    timeout_sec=10,
                    backoff_sec=1,
                    err_msg="Never saw message")
            assert False, "Should not have seen message"
        except ducktape.errors.TimeoutError:
            pass

        # should now see it again
        with self.redpanda.monitor_log(node) as mon:
            admin.set_log_level("admin_api_server", "info")
            mon.wait_until(
                "Set log level for {admin_api_server}: debug -> info",
                timeout_sec=5,
                backoff_sec=1,
                err_msg="Never saw message")

        with self.redpanda.monitor_log(node) as mon:
            admin.set_log_level("admin_api_server", "debug", expires=5)
            mon.wait_until(
                f"Expiring log level for {{admin_api_server}} to {self.initial_log_level}",
                timeout_sec=10,
                backoff_sec=1,
                err_msg="Never saw message")

    @cluster(num_nodes=1)
    @parametrize(loggers=("admin_api_server", "raft"))
    @parametrize(loggers=("raft", "admin_api_server"))
    def test_log_level_multiple_expiry(self, loggers=tuple[str, str]):
        """
        Check that more than one logger can be in a modified level and be expired correctly
        see https://redpandadata.atlassian.net/browse/CORE-96
        """
        admin = Admin(self.redpanda)
        node = self.redpanda.nodes[0]

        first_logger, second_logger = loggers
        # set two loggers to trace, expect that both of them expires in a timely fashion
        with self.redpanda.monitor_log(node) as mon:
            admin.set_log_level(first_logger, "trace", expires=10)
            time.sleep(1)
            admin.set_log_level(second_logger, "trace", expires=10)
            mon.wait_until(f"Expiring log level for {{{first_logger}}}",
                           timeout_sec=15,
                           backoff_sec=1,
                           err_msg=f"Never saw Expiring for {first_logger}")
            mon.wait_until(f"Expiring log level for {{{second_logger}}}",
                           timeout_sec=15,
                           backoff_sec=1,
                           err_msg=f"Never saw Expiring for {second_logger}")

    @cluster(num_nodes=1)
    def test_log_level_persist_a_never_expire_request(self):
        """
        check that this sequence of actions
        set log-level admin_api_server trace 10
        set log-level admin_api_server error 0

        never resets the logger to the info level
        """
        admin = Admin(self.redpanda)
        node = self.redpanda.nodes[0]

        with self.redpanda.monitor_log(node) as mon:
            admin.set_log_level("admin_api_server", "trace", expires=10)
            time.sleep(1)
            admin.set_log_level("admin_api_server", "error", expires=0)

            try:
                mon.wait_until("Expiring log level for {admin_api_server}",
                               timeout_sec=15,
                               backoff_sec=1)
                assert False, "Should not have seen message"
            except ducktape.errors.TimeoutError:
                pass

        level = admin.get_log_level("admin_api_server")[0]["level"]
        assert level == "error", f"expected level=error, got {level=}"

    @cluster(num_nodes=3)
    def test_max_expiry(self):
        admin = Admin(self.redpanda)
        node = self.redpanda.nodes[0]

        def set_log_and_check(level,
                              expiration,
                              actual,
                              force=False,
                              orig=self.initial_log_level):
            with self.redpanda.monitor_log(node) as mon:
                # note that this sends the request to all nodes in the cluster, synchronously
                rsps = admin.set_log_level("admin_api_server",
                                           level,
                                           expires=expiration,
                                           force=force)
                assert all(
                    r['expiration'] == actual for r in rsps
                ), f"Expected clamped expirations: {json.dumps([{'expiration': r['expiration']} for r in rsps])}"
                # check the log line
                mon.wait_until(
                    f"Set log level for {{admin_api_server}}: {orig} -> {level} (expiring {actual}s)",
                    timeout_sec=5,
                    backoff_sec=1,
                    err_msg="Never saw message")
                # confirm that the admin API reflects the expected settings
                rsps = admin.get_log_level("admin_api_server")
                assert all(
                    r['expiration'] <= actual and r['level'] == level
                    for r in rsps
                ), f"Expected {level} level expiring w/in {actual}s, got {json.dumps(rsps)}"

                # wait for a single node to expire
                mon.wait_until(
                    f"Expiring log level for {{admin_api_server}} to {self.initial_log_level}",
                    timeout_sec=actual + 1,
                    backoff_sec=1,
                    err_msg="Never saw message")

                rsps = admin.get_log_level("admin_api_server")
                # No guarantee that all servers have expired at this point
                assert any(
                    r['expiration'] == 0
                    and r['level'] == self.initial_log_level for r in rsps
                ), f"Expected expiration to {self.initial_log_level}, got {json.dumps(rsps)}"

        # set verbose level with expiration that exceeds the configured limit
        # and confirm that we clamp
        for lvl in ['trace', 'debug']:
            set_log_and_check(lvl,
                              expiration=self.MAX_LOG_EXPIRY_S * 10,
                              actual=self.MAX_LOG_EXPIRY_S)

        # set non-verbose level with expiration that exceeds configured max
        # and confirm that we DON'T clamp
        for lvl in ['error', 'warn', 'info']:
            set_log_and_check(lvl,
                              expiration=self.MAX_LOG_EXPIRY_S * 2,
                              actual=self.MAX_LOG_EXPIRY_S * 2)

        # try forcing an expiry that is longer than the configured max
        set_log_and_check('debug',
                          expiration=self.MAX_LOG_EXPIRY_S * 3,
                          actual=self.MAX_LOG_EXPIRY_S * 3,
                          force=True)

        # try non-expiring setting, confirm that it's clamped
        set_log_and_check('debug', expiration=0, actual=self.MAX_LOG_EXPIRY_S)

        # try forcing non-expiring level
        with self.redpanda.monitor_log(node) as mon:
            expiration = 0
            actual = 0
            lvl = 'debug'
            force = True
            with self.redpanda.monitor_log(node) as mon:
                rsps = admin.set_log_level("admin_api_server",
                                           lvl,
                                           expires=expiration,
                                           force=force)
                assert all(
                    r['expiration'] == actual for r in rsps
                ), f"Expected clamped expirations: {json.dumps([{'expiration': r['expiration']} for r in rsps])}"
                mon.wait_until(
                    f"Set log level for {{admin_api_server}}: trace -> {lvl} (expiring NEVER)",
                    timeout_sec=5,
                    backoff_sec=1,
                    err_msg="Never saw message")
                with expect_exception(ducktape.errors.TimeoutError,
                                      lambda _: True):
                    mon.wait_until(
                        f"Expiring log level for {{admin_api_server}} to {self.initial_log_level}",
                        timeout_sec=self.MAX_LOG_EXPIRY_S + 1,
                        backoff_sec=1,
                        err_msg="Never saw message")

        # finally, set with any expiration to confirm that we expire to the default
        # and NOT some previously set "permanent" level (debug in this case)
        set_log_and_check('info', expiration=2, actual=2, orig='debug')

    @cluster(num_nodes=3)
    def test_invalid_logger_name(self):
        admin = Admin(self.redpanda)
        logger = 'test\nlog'

        def check_log_for_invalid_parameter(val: str):
            pattern = f'Parameter contained invalid control characters: {val}'
            wait_until(lambda: self.redpanda.search_log_any(pattern),
                       timeout_sec=5)

        try:
            admin.set_log_level(urllib.parse.quote(logger), "debug")
            assert False, "Call should fail"
        except requests.exceptions.HTTPError:
            check_log_for_invalid_parameter(
                logger.translate(CONTROL_CHARS_MAP))
