# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from subprocess import CalledProcessError

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.util import expect_exception


class KafkaCliClientCompatTest(RedpandaTest):
    @cluster(num_nodes=3)
    def test_create_topic(self):
        for client_factory in KafkaCliTools.instances():
            client = client_factory(self.redpanda)
            topics = [TopicSpec() for _ in range(3)]
            for topic in topics:
                client.create_topic(topic)
            for topic in topics:
                spec = client.describe_topic(topic.name)
                assert spec == topic

    @cluster(num_nodes=3)
    def test_describe_broker_configs(self):
        # this uses the latest kafka client. older clients still need some work.
        # it seems as though at the protocol layer things work fine, but the
        # interface to cli clients are different. so some work generalizing the
        # client interface is needed.
        client_factory = KafkaCliTools.instances()[0]
        client = client_factory(self.redpanda)
        res = client.describe_broker_config()
        assert res.count("All configs for broker") == len(self.redpanda.nodes)

    @cluster(num_nodes=1)
    def test_create_role_acl(self):
        """
        Verify that we can bind an ACL to a "RedpandaRole" principal and that this
        binding appears, unaltered, in the ACLs list.
        """
        ROLE_NAME = 'my_role'
        ROLE_PFX = 'RedpandaRole'
        for client_factory in KafkaCliTools.instances():
            client = client_factory(self.redpanda)
            client.create_cluster_acls(ROLE_NAME, "describe", ptype=ROLE_PFX)
            res = client.list_acls()
            assert f"principal={ROLE_PFX}:{ROLE_NAME}" in res, f"Failed to list role ACL: {res}"

    @cluster(num_nodes=1)
    def test_create_bad_acl(self):
        """
        Verify that Redpanda rejects (and kafka cli tools correctly handle)
        ACL bindings with a bogus principal type
        """
        ROLE_NAME = 'my_role'
        ROLE_PFX = 'InvalidPrefix'
        for client_factory in KafkaCliTools.instances():
            client = client_factory(self.redpanda)
            with expect_exception(CalledProcessError,
                                  lambda e: "exit status 1" in str(e)):
                client.create_cluster_acls(ROLE_NAME,
                                           "describe",
                                           ptype=ROLE_PFX)
            res = client.list_acls()
            assert f"principal={ROLE_PFX}:{ROLE_NAME}" not in res, f"Unexpectedly found bogus ACL: {res}"
