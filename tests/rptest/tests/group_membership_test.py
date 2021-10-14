# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import requests

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kcl import KCL


class ListGroupsReplicationFactorTest(RedpandaTest):
    """
    We encountered an issue where listing groups would return a
    coordinator-loading error when the underlying group membership topic had a
    replication factor of 3 (we had not noticed this until we noticed that
    replication factor were defaulted to 1). it isn't clear if this is specific
    to `kcl` but that is the client that we encountered the issue with.
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context):
        extra_rp_conf = dict(default_topic_replications=3, )

        super(ListGroupsReplicationFactorTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_list_groups(self):
        kcl = KCL(self.redpanda)
        kcl.produce(self.topic, "msg\n")
        kcl.consume(self.topic, n=1, group="g0")
        kcl.list_groups()
        out = kcl.list_groups()
        assert "COORDINATOR_LOAD_IN_PROGRESS" not in out

    def _transfer_with_retry(self, namespace, topic, partition, target_id):
        """
        Leadership transfers may return 503 if done in a tight loop, as
        the current leader might still be writing their configuration after
        winning their election.

        503 is safe status to retry.
        """
        admin = Admin(redpanda=self.redpanda)
        timeout = time.time() + 10

        while time.time() < timeout:
            try:
                admin.transfer_leadership_to(namespace=namespace,
                                             topic=topic,
                                             partition=partition,
                                             target_id=target_id)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 503:
                    time.sleep(1)
                else:
                    raise
            else:
                break

    @cluster(num_nodes=3)
    def test_list_groups_has_no_duplicates(self):
        """
        Reproducer for:
        https://github.com/vectorizedio/redpanda/issues/2528
        """
        kcl = KCL(self.redpanda)
        kcl.produce(self.topic, "msg\n")
        # create 4 groups
        kcl.consume(self.topic, n=1, group="g0")
        kcl.consume(self.topic, n=1, group="g1")
        kcl.consume(self.topic, n=1, group="g2")
        kcl.consume(self.topic, n=1, group="g3")
        # transfer kafka_internal/group/0 leadership across the nodes to trigger
        # group state recovery on each node
        for n in self.redpanda.nodes:
            self._transfer_with_retry(namespace="kafka_internal",
                                      topic="group",
                                      partition=0,
                                      target_id=self.redpanda.idx(n))

        # assert that there are no duplicates in
        def _list_groups():
            out = kcl.list_groups()
            groups = []
            for l in out.splitlines():
                # skip header line
                if l.startswith("BROKER"):
                    continue
                parts = l.split()
                groups.append({'node_id': parts[0], 'group': parts[1]})
            return groups

        def _check_no_duplicates():
            groups = _list_groups()
            self.redpanda.logger.debug(f"groups: {groups}")
            return len(groups) == 4

        wait_until(_check_no_duplicates,
                   10,
                   backoff_sec=2,
                   err_msg="found persistent duplicates in groups listing")
