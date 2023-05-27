# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import requests
import json
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.services.admin import Admin
from rptest.services.redpanda import make_redpanda_service
from rptest.tests.end_to_end import EndToEndTest


class ClusterViewTest(EndToEndTest):
    @cluster(num_nodes=3)
    def test_view_changes_on_add(self):
        self.redpanda = make_redpanda_service(self.test_context, 3)
        # start single node cluster
        self.redpanda.start(nodes=[self.redpanda.nodes[0]])

        admin = Admin(self.redpanda)

        seed = None

        def rp1_started():
            nonlocal seed
            try:
                #{"version": 0, "brokers": [{"node_id": 1, "num_cores": 3, "membership_status": "active", "is_alive": true}]}
                seed = admin.get_cluster_view(self.redpanda.nodes[0])
                self.redpanda.logger.info(
                    f"view from {self.redpanda.nodes[0]}: {json.dumps(seed)}")
                return len(seed["brokers"]) == 1
            except requests.exceptions.RequestException as e:
                self.redpanda.logger.debug(f"admin API isn't available ({e})")
                return False

        wait_until(
            rp1_started,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Cant get cluster view from {self.redpanda.nodes[0]}")

        self.redpanda.start_node(self.redpanda.nodes[1])
        self.redpanda.start_node(self.redpanda.nodes[2])

        def rest_started():
            try:
                last = None
                ids = None
                for i in range(0, 3):
                    view = admin.get_cluster_view(self.redpanda.nodes[i])
                    self.redpanda.logger.info(
                        f"view from {self.redpanda.nodes[i]}: {json.dumps(view)}"
                    )
                    if view["version"] <= seed["version"]:
                        return False
                    if len(view["brokers"]) != 3:
                        return False
                    if last == None:
                        last = view
                        ids = set(
                            map(lambda broker: broker["node_id"],
                                view["brokers"]))
                    if last["version"] != view["version"]:
                        return False
                    if not ids.issubset(
                            map(lambda broker: broker["node_id"],
                                view["brokers"])):
                        return False
                return True
            except requests.exceptions.RequestException as e:
                self.redpanda.logger.debug(f"admin API isn't available ({e})")
                return False

        wait_until(rest_started,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="Cant get cluster view from {self.redpanda.nodes}")
