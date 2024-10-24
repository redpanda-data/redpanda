# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from dataclasses import dataclass
import time

from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint
from rptest.tests.redpanda_test import RedpandaTest


@dataclass
class HeartbeatMetrics:
    lw_beats: int
    full_beats: int


class LwHeartbeatsTest(RedpandaTest):
    def get_heartbeat_metrics(self):
        lw_beats = sum([
            sample.value for sample in self.redpanda.metrics_sample(
                "lightweight_heartbeat_requests",
                metrics_endpoint=MetricsEndpoint.METRICS).samples
        ])

        full_beats = sum([
            sample.value for sample in self.redpanda.metrics_sample(
                "full_heartbeat_requests",
                metrics_endpoint=MetricsEndpoint.METRICS).samples
        ])

        return HeartbeatMetrics(lw_beats=lw_beats, full_beats=full_beats)

    @cluster(num_nodes=3)
    def test_use_of_lw_heartbeats(self):
        self.client().create_topic(TopicSpec(partition_count=9))
        self.previous_sample = self.get_heartbeat_metrics()

        def only_lw_beats_exchanged():
            m = self.get_heartbeat_metrics()
            lw_cnt = m.lw_beats - self.previous_sample.lw_beats
            f_cnt = m.full_beats - self.previous_sample.full_beats
            self.previous_sample = m
            self.logger.info(
                f"heartbeats since last sample: [lw: {lw_cnt}, full: {f_cnt}]")
            return lw_cnt > 0 and f_cnt == 0

        time.sleep(2)
        wait_until(
            only_lw_beats_exchanged, 30, 2,
            "Timeout waiting for cluster to reach state in which lw heartbeats are exchanged"
        )
