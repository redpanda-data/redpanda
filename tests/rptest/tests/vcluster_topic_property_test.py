# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from random import randbytes

from rptest.utils.xid_utils import random_xid_string


def get_vcluster_id(topic_properties: dict):
    if TopicSpec.PROPERTY_VIRTUAL_CLUSTER_ID not in topic_properties:
        return None
    return topic_properties[TopicSpec.PROPERTY_VIRTUAL_CLUSTER_ID][0]


class VirtualClusterTopicPropertyTest(RedpandaTest):
    def __init__(self, test_context):
        super(VirtualClusterTopicPropertyTest,
              self).__init__(test_context=test_context, num_brokers=3)

    @cluster(num_nodes=3)
    def test_basic_virtual_cluster_property(self):
        rpk = RpkTool(self.redpanda)
        # try creating vcluster enabled topic with extensions disabled
        topic = TopicSpec()
        vcluster_id = random_xid_string()
        rpk.create_topic(
            topic=topic.name,
            partitions=1,
            replicas=3,
            config={TopicSpec.PROPERTY_VIRTUAL_CLUSTER_ID: vcluster_id})

        configs = rpk.describe_topic_configs(topic.name)
        assert TopicSpec.PROPERTY_VIRTUAL_CLUSTER_ID not in configs, \
            "When MPX extensions are disabled there should be no virtual cluster property reported"
        try:
            configs = rpk.alter_topic_config(
                topic.name, TopicSpec.PROPERTY_VIRTUAL_CLUSTER_ID, vcluster_id)
            assert False, "Altering topic virtual cluster property should not be supported"
        except RpkException as e:
            assert "INVALID_CONFIG" in e.msg
        rpk.delete_topic(topic.name)
        # enable mpx extensions
        self.redpanda.set_cluster_config({"enable_mpx_extensions": True})
        # create topic with virtual cluster id assigned
        rpk.create_topic(
            topic=topic.name,
            partitions=1,
            replicas=3,
            config={TopicSpec.PROPERTY_VIRTUAL_CLUSTER_ID: vcluster_id})

        configs = rpk.describe_topic_configs(topic.name)

        assert get_vcluster_id(configs) == vcluster_id, \
            f"current virtual cluster id {get_vcluster_id(configs)} and expected cluster id {vcluster_id} doesn't match"

        try:
            rpk.alter_topic_config(topic.name,
                                   TopicSpec.PROPERTY_VIRTUAL_CLUSTER_ID,
                                   random_xid_string())
            assert False, "Altering topic virtual cluster property should not be supported"
        except RpkException as e:
            assert "INVALID_CONFIG" in e.msg
