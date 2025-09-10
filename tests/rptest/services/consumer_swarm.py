# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Optional

from ducktape.tests.test import TestContext

from rptest.services.client_swarm_base import ClientSwarmBase


class ConsumerSwarm(ClientSwarmBase):
    def __init__(
        self,
        context: TestContext,
        redpanda,
        topic: str,
        group: str,
        consumers: int,
        records_per_consumer: int,
        log_level="DEBUG",
        properties={},
        unique_topics: Optional[bool] = False,
        static_prefix: Optional[bool] = False,
        unique_groups=False,
        topics_per_client: Optional[int] = None,
    ):
        super().__init__(context, redpanda, topic, log_level, properties)

        self._group = group
        self._consumers = consumers
        self._records_per_consumer = records_per_consumer
        self._unique_topics = unique_topics
        self._unique_groups = unique_groups
        self._static_prefix = static_prefix
        self._topics_per_client = topics_per_client

    def _additional_args(self):
        cmd = ""
        cmd += " consumers"
        cmd += f" --group {self._group}"
        cmd += f" --count {self._consumers}"
        cmd += f" --messages {self._records_per_consumer}"

        if self._unique_topics:
            cmd += " --unique-topics"

        if self._unique_groups:
            cmd += " --unique-groups"

        if self._topics_per_client:
            cmd += f" --topics-per-client {self._topics_per_client}"

        return cmd

    def await_first(self, timeout_sec, err_msg=None):
        class Checker:
            def __init__(self, swarm: ConsumerSwarm):
                self.swarm = swarm
                self.checks_made = 0
                self.check_passed = False

            def __call__(self):
                try:
                    ms = self.swarm.get_metrics_summary()
                    self.checks_made += 1
                    self.swarm.logger.debug(
                        f"ConsumerSwarm summary (checks_made: {self.checks_made}: {ms}"
                    )
                    self.check_passed = ms.total_success > 0
                    return self.check_passed
                except RuntimeError:
                    # a common thing is that the swarm joins and then consumers immediately the
                    # requested number of messages between one poll and another, after which it
                    # stops, we don't treat this as a failure since though we do return False from
                    # this method to indicate the behavior
                    if self.checks_made > 0 and not self.swarm.is_alive():
                        self.swarm.logger.info(
                            "ConsumerSwarm await_first stopping wait after swarm stopped"
                        )
                        return True
                    raise

        checker = Checker(self)

        self._redpanda.wait_until(
            checker, timeout_sec=timeout_sec, backoff_sec=1, err_msg=err_msg
        )

        return checker.check_passed
