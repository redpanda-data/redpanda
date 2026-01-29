# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
from math import ceil
from typing import Any, Optional

from ducktape.tests.test import TestContext

from rptest.services.client_swarm_base import ClientSwarmBase
from rptest.services.redpanda import AnyRedpandaService
from rptest.services.utils import LocalPayloadDirectory, assert_int, assert_int_or_none


class ProducerSwarm(ClientSwarmBase):
    CUSTOM_PAYLOAD_DIR = os.path.join(
        ClientSwarmBase.PERSISTENT_ROOT, "custom_payloads"
    )

    def __init__(
        self,
        context: TestContext,
        redpanda: AnyRedpandaService,
        topic: str,
        producers: int,
        records_per_producer: int,
        log_level: str = "DEBUG",
        properties: dict[str, Any] = {},
        timeout_ms: int = 1000,
        compression_type: Optional[str] = None,
        compressible_payload: Optional[bool] = None,
        min_record_size: Optional[int] = None,
        max_record_size: Optional[int] = None,
        keys: Optional[int] = None,
        unique_topics: Optional[bool] = False,
        messages_per_second_per_producer: Optional[int] = None,
        message_period: Optional[str] = None,
        topics_per_client: Optional[int] = None,
        local_payload_dir: Optional[LocalPayloadDirectory] = None,
    ):
        super().__init__(context, redpanda, topic, log_level, properties)

        assert not (messages_per_second_per_producer and message_period), (
            "only one of these properties can be set"
        )

        assert not (
            (compressible_payload or min_record_size or max_record_size)
            and local_payload_dir
        ), (
            "if a local payload directory is specified then all other payload options are ignored"
        )

        if local_payload_dir is not None:
            assert local_payload_dir.has_payloads(), (
                "local_payload_dir must have at least one payload"
            )

        self._producers = assert_int(producers)
        self._records_per_producer = assert_int(records_per_producer)
        self._messages_per_second_per_producer = assert_int_or_none(
            messages_per_second_per_producer
        )
        self._timeout_ms = assert_int(timeout_ms)
        self._compression_type = compression_type
        self._compressible_payload = compressible_payload
        self._min_record_size = assert_int_or_none(min_record_size)
        self._max_record_size = assert_int_or_none(max_record_size)
        self._keys = assert_int_or_none(keys)
        self._unique_topics = unique_topics
        self._message_period = message_period
        self._topics_per_client = topics_per_client
        self._local_payload_dir = local_payload_dir

    def _additional_args(self) -> str:
        cmd = ""
        cmd += " producers "
        cmd += f" --count {self._producers}"
        cmd += f" --messages {self._records_per_producer}"
        cmd += f" --timeout-ms {self._timeout_ms}"

        if self._compressible_payload:
            cmd += " --compressible-payload"

        if self._compression_type is not None:
            cmd += f" --compression-type={self._compression_type}"

        if self._min_record_size is not None:
            cmd += f" --min-record-size={self._min_record_size}"

        if self._max_record_size is not None:
            cmd += f" --max-record-size={self._max_record_size}"

        if self._keys is not None:
            cmd += f" --keys={self._keys}"
        else:
            # by default use a very large key space so all partitions are written to
            cmd += " --keys=18446744073709551557"

        if self._unique_topics:
            cmd += " --unique-topics"

        if self._messages_per_second_per_producer is not None:
            cmd += f" --messages-per-second {self._messages_per_second_per_producer}"

        if self._message_period is not None:
            cmd += f" --message-period {self._message_period}"

        if self._topics_per_client:
            cmd += f" --topics-per-client {self._topics_per_client}"

        if self._local_payload_dir:
            cmd += f" --payload-directory {ProducerSwarm.CUSTOM_PAYLOAD_DIR}"

        return cmd

    def _pre_run_tasks(self):
        if self._local_payload_dir and self._node:
            self.logger.info("Copying custom payloads to producer swarm node.")
            self._local_payload_dir.copy_to_node(
                self._node, ProducerSwarm.CUSTOM_PAYLOAD_DIR
            )

    def wait_for_all_started(self):
        """Wait until the requested number of producers have started. Note that if the expected
        swarm runtime (messages / rate) is short, this may fail as all producers and start and
        finish before we are able to see this via the metrics endpoint and an exception is thrown."""

        # Calculate the theoretical start time based on the spawn rate and then
        # use 3x that + 30 seconds as the timeout. In addition to the sources of random
        # variation, producer swarm is subject to additional variance:
        #
        #   - The CLIENT_SPAWN_WAIT_MS is a sleep time in between spawns, not the
        # actual inter-spawn time, since the spawn itself takes non-zero time and also the
        # sleep may take longer that specified. So the actual inter-spawn times are longer
        # than the configured value and under heavy load may be substantially longer.
        #
        #   - A client is not considered started util is successfully sends a message to
        # the cluster. Since we have often just created the topic, this could be delayed
        # due to leadership transfers which occur in a burst some time between 0 and 5
        # minutes after the topic is created.
        timeout_s = 30 + 3 * ceil(self._producers * self.CLIENT_SPAWN_WAIT_MS / 1000)

        def started_count():
            started = self.get_metrics_summary().clients_started
            self.logger.debug(f"{started} producers started so far")
            return started

        self.logger.info(
            f"Waiting up to {timeout_s}s for all {self._producers} to start"
        )

        self._redpanda.wait_until(
            lambda: started_count() == self._producers,
            timeout_sec=timeout_s,
            backoff_sec=1,
            err_msg=lambda: f"{self} did not start after {timeout_s}: "
            f"{started_count()} started, expected {self._producers}",
        )
