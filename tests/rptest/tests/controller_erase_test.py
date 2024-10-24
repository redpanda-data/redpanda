# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, RedpandaService
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.types import TopicSpec
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from rptest.util import wait_until_result

ERASE_ERROR_MSG = "Inconsistency detected between KVStore last_applied"
ERASE_LOG_ALLOW_LIST = RESTART_LOG_ALLOW_LIST + [ERASE_ERROR_MSG]


class ControllerEraseTest(RedpandaTest):
    @cluster(num_nodes=3, log_allow_list=ERASE_LOG_ALLOW_LIST)
    @parametrize(partial=True)
    @parametrize(partial=False)
    def test_erase_controller_log(self, partial):
        """
        This test covers the case where an administrator intentionally
        removes the controller log data from a node, but does not wipe
        the entire data directory for the node.

        In issue https://github.com/redpanda-data/redpanda/issues/4950
        this fails because the node's kvstore points to a higher last_applied
        offset than exists in the local log, blocking startup: this is a regression
        test for that behaviour.

        In general Redpanda doesn't handle on-disk corruption (such as selectively
        deleting segments) gracefully, but in the particular case of wiping
        the whole controller log, or a contiguous suffix region of the log,
        we can gracefully let raft fill in the gaps.
        """

        admin = Admin(self.redpanda)
        # disable controller snapshot in partial removal test to make sure that
        # the deleted segment is not snapshot before restarting the node
        if partial:
            self.redpanda.set_cluster_config(
                {"controller_snapshot_max_age_sec": 3600})

        # Do a bunch of metadata operations to put something in the controller log
        transfers_leadership_count = 4
        for i in range(0, transfers_leadership_count):
            for j in range(0, 4):
                spec = TopicSpec(partition_count=1, replication_factor=3)
                self.client().create_topic(spec)

            # Move a leader to roll a segment
            leader_node = self.redpanda.controller()
            next_leader = ((self.redpanda.idx(leader_node) + 1) % 3) + 1
            admin.partition_transfer_leadership('redpanda', 'controller', 0,
                                                next_leader)

        # Stop the node we will intentionally damage
        victim_node = self.redpanda.nodes[1]

        def controller_elected():
            ctrl = self.redpanda.controller()
            return (ctrl is not None, ctrl)

        bystander_node = wait_until_result(controller_elected,
                                           timeout_sec=15,
                                           backoff_sec=1)

        def wait_all_segments():
            storage = self.redpanda.node_storage(victim_node)
            segments = storage.ns['redpanda'].topics['controller'].partitions[
                "0_0"].segments.keys()
            # We expect that segments count for controller should be transfers_leadership_count + 1.
            # Because each transfer creates one segment + initial leadership after restart creates first segment
            return len(segments) == transfers_leadership_count + 1

        wait_until(
            wait_all_segments,
            timeout_sec=40,
            backoff_sec=1,
            err_msg=
            f"Victim node({victim_node}) does not contain expected segments count({transfers_leadership_count + 1}) for controller log"
        )

        bystander_node_dirty_offset = admin.get_controller_status(
            bystander_node)["dirty_offset"]

        def wait_victim_node_apply_segments():
            status = admin.get_controller_status(victim_node)
            last_applied = status["last_applied_offset"]
            dirty_offset = status["dirty_offset"]
            return dirty_offset == last_applied and last_applied >= bystander_node_dirty_offset

        wait_until(
            wait_victim_node_apply_segments,
            timeout_sec=40,
            backoff_sec=1,
            err_msg=
            f"Victim node did not apply {bystander_node_dirty_offset} offset")

        self.redpanda.stop_node(victim_node)

        # Erase controller log on the victim node
        controller_path = f"{RedpandaService.PERSISTENT_ROOT}/data/redpanda/controller/0_0"
        if partial:
            # Partial deletion: remove the latest controller log segment
            storage = self.redpanda.node_storage(victim_node)
            segments = storage.ns['redpanda'].topics['controller'].partitions[
                "0_0"].segments.keys()
            assert len(segments) == transfers_leadership_count + 1
            segments = sorted(list(segments))
            victim_path = f"{controller_path}/{segments[-1]}.log"
        else:
            # Full deletion: remove all log segments.
            victim_path = f"{controller_path}/*"

        victim_node.account.remove(victim_path)

        # Node should come up cleanly
        self.redpanda.start_node(victim_node)

        # Node should have logged a complaint during startup
        assert self.redpanda.search_log_node(victim_node,
                                             ERASE_ERROR_MSG) is True

        # Control: undamaged node should not have logged the complaint
        assert self.redpanda.search_log_node(bystander_node,
                                             ERASE_ERROR_MSG) is False
