# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from requests.exceptions import HTTPError
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.services.rpk_consumer import RpkConsumer

from datetime import datetime
from functools import reduce

from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.utils.functional import flat_map


class UsageTest(RedpandaTest):
    """
    Tests validating if Redpanda nodes are configured with racks and regions
    """
    def setUp(self):
        # defer starting redpanda to test body
        pass

    def __init__(self, test_context, **kwargs):
        self.racks = []
        self.regions = []
        super().__init__(test_context, **kwargs)

    def rack_for(self, n):
        return self.racks[self.redpanda.idx(n) - 1]

    def region_for(self, n):
        return self.regions[self.redpanda.idx(n) - 1]

    def node_by_id(self, node_id):
        for n in self.redpanda.nodes:
            if self.redpanda.node_id(n) == node_id:
                return n

    @cluster(num_nodes=3)
    def test_rack_aware_cluster(self):
        self.racks = ['r-1', 'r-2', 'r-3']

        for n in self.redpanda.nodes:
            self.redpanda.start_node(
                n, override_cfg_params={"rack": self.rack_for(n)})

        admin = Admin(self.redpanda)
        brokers = admin.get_brokers()

        for b in brokers:
            id = b['node_id']
            rack = b['rack']
            assert self.rack_for(self.node_by_id(id)) == rack

    @cluster(num_nodes=3)
    def test_multi_region_cluster(self):
        self.regions = ['region-a', 'region-b', 'region-c']
        self.racks = ['r-1', 'r-2', 'r-3']

        for n in self.redpanda.nodes:
            self.redpanda.start_node(n,
                                     override_cfg_params={
                                         "rack": self.rack_for(n),
                                         "region": self.region_for(n)
                                     })

        admin = Admin(self.redpanda)
        brokers = admin.get_brokers()
        for b in brokers:
            id = b['node_id']
            rack = b['rack']
            region = b['region']
            assert self.rack_for(self.node_by_id(id)) == rack
            assert self.region_for(self.node_by_id(id)) == region

    @cluster(
        num_nodes=3,
        log_allow_list=['.*Failure during startup: std::invalid_argument.*'])
    def test_incorrect_configuration(self):

        self.racks = ['r-1', 'r-2', 'r-3']

        # do not allow to start when region is set without a rack set
        self.redpanda.start_node(self.redpanda.nodes[0],
                                 override_cfg_params={"region": 'common'},
                                 expect_fail=True)

        for n in self.redpanda.nodes:
            self.redpanda.start_node(n,
                                     override_cfg_params={
                                         "rack": self.rack_for(n),
                                         "region": 'common'
                                     })

        admin = Admin(self.redpanda)
        brokers = admin.get_brokers()
        for b in brokers:
            id = b['node_id']
            rack = b['rack']
            region = b['region']
            assert self.rack_for(self.node_by_id(id)) == rack
            assert region == 'common'
