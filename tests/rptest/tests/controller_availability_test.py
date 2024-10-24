# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from math import ceil
import random
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from ducktape.tests.test import Test
from ducktape.mark import matrix

from rptest.services.redpanda import make_redpanda_service
from rptest.utils.mode_checks import cleanup_on_early_exit


class ControllerAvailabilityTest(Test):
    def __init__(self, test_ctx, *args, **kwargs):
        self.ctx = test_ctx
        self.redpanda = None
        self.admin = None
        super().__init__(test_ctx, *args, **kwargs)

    def start_redpanda(self, cluster_size):
        self.redpanda = make_redpanda_service(self.ctx,
                                              num_brokers=cluster_size)
        self.redpanda.start()
        self.admin = Admin(self.redpanda)

    def _tolerated_failures(self, cluster_size):
        return int(ceil(cluster_size / 2.0) - 1)

    def _controller_stable(self):

        started_ids = set(
            [self.redpanda.node_id(n) for n in self.redpanda.started_nodes()])
        self.logger.info(f"started redpanda nodes: {started_ids}")

        controller = self.redpanda.controller()

        if controller is None:
            self.logger.warn("No controller elected")
            return False
        self.logger.info(
            f"controller exists in the cluster: {controller.account.hostname} node_id: {self.redpanda.node_id(controller)}"
        )

        if self.redpanda.node_id(controller) not in started_ids:
            self.logger.info(
                f"Reported controller node {controller.account.hostname} is obsolete as it was stopped"
            )
            return False

        statuses = []
        for n in self.redpanda.started_nodes():
            controller_status = self.admin.get_controller_status(n)
            self.logger.info(
                f"Status: {controller_status} from {n.account.hostname}")
            statuses.append(controller_status)

        return all([cs == statuses[0] for cs in statuses[1:]])

    def _check_metrics(self, cluster_size):
        sent_vote_metrics = self.redpanda.metrics_sample(
            "sent_vote_requests", self.redpanda.started_nodes())

        for m in sent_vote_metrics.samples:
            self.logger.debug("Vote requests metric sample: {m}")
            assert (
                m.value <= 2 * cluster_size
            ), f"two rounds of leader election must be enough to elect a leader, current node vote request count: {m.value}"

    @cluster(num_nodes=5)
    @matrix(cluster_size=[3, 4, 5], stop=["single", "minority"])
    def test_controller_availability_with_nodes_down(self, cluster_size, stop):
        # start cluster
        self.start_redpanda(cluster_size)
        to_kill = self._tolerated_failures(
            cluster_size) if stop == "minority" else 1

        # stop first two nodes with the highest priorities
        nodes = sorted(self.redpanda.nodes.copy(),
                       key=lambda n: self.redpanda.node_id(n))

        for n in nodes[0:to_kill]:
            self.logger.info(
                f"stopping node: {n.account.hostname} with id: {self.redpanda.node_id(n)}"
            )
            self.redpanda.stop_node(n, forced=True)

        wait_until(lambda: self._controller_stable(), 10, 0.5,
                   "Controller is not available")
        self._check_metrics(cluster_size)

        cleanup_on_early_exit(self)
