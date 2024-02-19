# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import requests
from enum import IntEnum

import numpy as np

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.cluster.cluster import ClusterNode

from rptest.util import wait_until_result


class AdminUUIDOperationsTest(RedpandaTest):
    def __init__(self, ctx):
        super().__init__(test_context=ctx, num_brokers=3)

    def setUp(self):
        self.redpanda.start(auto_assign_node_id=True,
                            omit_seeds_on_idx_one=False)
        self._create_initial_topics()

    @cluster(num_nodes=3)
    def test_getting_node_id_to_uuid_map(self):
        admin = Admin(self.redpanda)
        uuids = admin.get_broker_uuids()
        assert len(uuids) == 3, "UUID map should contain 3 brokers"
        all_ids = set()
        for n in uuids:
            assert 'node_id' in n
            assert 'uuid' in n
            all_ids.add(n['node_id'])

        brokers = admin.get_brokers()
        for b in brokers:
            assert b['node_id'] in all_ids

    @cluster(num_nodes=3)
    def test_overriding_node_id(self):
        admin = Admin(self.redpanda)
        to_stop = self.redpanda.nodes[0]
        initial_to_stop_id = self.redpanda.node_id(to_stop)
        # Stop node and clear its data directory
        self.redpanda.stop_node(to_stop)
        self.redpanda.clean_node(to_stop,
                                 preserve_current_install=True,
                                 preserve_logs=False)

        self.redpanda.start_node(to_stop,
                                 auto_assign_node_id=True,
                                 omit_seeds_on_idx_one=False)

        def _uuids_updated():
            uuids = admin.get_broker_uuids()
            if len(uuids) != 4:
                return False, None

            return True, uuids

        # wait for the node to join with new ID
        uuids = wait_until_result(
            _uuids_updated,
            timeout_sec=30,
            err_msg="Node was unable to join the cluster")

        uuids = admin.get_broker_uuids()
        old_uuid = None

        for n in uuids:
            id = n['node_id']
            if id == initial_to_stop_id:
                old_uuid = n['uuid']

        # get current node id and UUID
        current = admin.get_broker_uuid(to_stop)

        admin.override_node_id(to_stop,
                               current_uuid=current['node_uuid'],
                               new_node_id=initial_to_stop_id,
                               new_node_uuid=old_uuid)

        self.redpanda.restart_nodes(to_stop,
                                    auto_assign_node_id=True,
                                    omit_seeds_on_idx_one=False)

        after_restart = admin.get_broker_uuid(to_stop)

        assert after_restart['node_id'] == initial_to_stop_id
        assert after_restart['node_uuid'] == old_uuid
