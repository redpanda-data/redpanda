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


class AdminUUIDOperationsTest(RedpandaTest):
    def __init__(self, ctx):
        super().__init__(test_context=ctx, num_brokers=3)

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
