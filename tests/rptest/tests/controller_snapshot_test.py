# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer
from rptest.clients.rpk import RpkTool
from rptest.util import wait_until_result

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

import random


class ControllerSnapshotPolicyTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         num_brokers=3,
                         extra_rp_conf={'controller_snapshot_max_age_sec': 5},
                         **kwargs)

    def setUp(self):
        # start the nodes manually
        pass

    @cluster(num_nodes=3)
    def test_snapshotting_policy(self):
        """
        Test that Redpanda creates a controller snapshot some time after controller commands appear.
        """
        self.redpanda.start()
        admin = Admin(self.redpanda)

        for n in self.redpanda.nodes:
            controller_status = admin.get_controller_status(n)
            assert controller_status['start_offset'] == 0

        admin.put_feature("controller_snapshots", {"state": "active"})

        # first snapshot will be triggered by the feature_update command
        # check that snapshot is created both on the leader and on followers
        node_idx2snapshot_info = {}
        for n in self.redpanda.nodes:
            idx = self.redpanda.idx(n)
            snap_info = self.redpanda.wait_for_controller_snapshot(n)
            node_idx2snapshot_info[idx] = snap_info

        # second snapshot will be triggered by the topic creation
        RpkTool(self.redpanda).create_topic('test')
        for n in self.redpanda.nodes:
            mtime, start_offset = node_idx2snapshot_info[self.redpanda.idx(n)]
            self.redpanda.wait_for_controller_snapshot(
                n, prev_mtime=mtime, prev_start_offset=start_offset)


class ControllerState:
    def __init__(self, redpanda, node):
        admin = Admin(redpanda, default_node=node)
        rpk = RpkTool(redpanda)

        self.features_response = admin.get_features(node=node)
        self.features_map = dict(
            (f['name'], f) for f in self.features_response['features'])
        self.config_response = admin.get_cluster_config(node=node)
        self.users = set(admin.list_users(node=node))
        self.acls = rpk.acl_list().strip().split('\n')

        self.topics = set(rpk.list_topics())
        self.topic_configs = dict()
        self.topic_partitions = dict()
        for t in self.topics:
            self.topic_configs[t] = rpk.describe_topic_configs(topic=t)

            partition_fields = ['id', 'replicas']
            partitions = list(
                dict((k, getattr(part, k)) for k in partition_fields)
                for part in rpk.describe_topic(topic=t, tolerant=True))
            self.topic_partitions[t] = partitions

        broker_fields = [
            'node_id', 'num_cores', 'rack', 'membership_status',
            'internal_rpc_status', 'internal_rpc_port'
        ]
        self.brokers = dict()
        for broker in admin.get_brokers(node=node):
            self.brokers[broker['node_id']] = dict(
                (k, broker.get(k)) for k in broker_fields)

    def _check_features(self, other):
        for k, v in self.features_response.items():
            if k == 'features':
                continue
            new_v = other.features_response.get(k)
            assert new_v == v, \
                f"features response mismatch (key: {k}): got {new_v}, expected {v}"

        for f, v in self.features_map.items():
            new_v = other.features_map.get(f)
            assert new_v == v, \
                f"features map mismatch (feature: {f}): got {new_v}, expected {v}"

    def _check_config(self, other):
        def to_set(resp):
            return set((k, (v if not isinstance(v, list) else tuple(v)))
                       for k, v in resp.items())

        symdiff = to_set(self.config_response) ^ to_set(other.config_response)
        assert len(
            symdiff) == 0, f"config responses differ, symdiff: {symdiff}"

    def _check_topics(self, other):
        symdiff = self.topics ^ other.topics
        assert len(symdiff) == 0, f"topics differ, symdiff: {symdiff}"

        for t in self.topics:
            configs = self.topic_configs[t]
            other_configs = other.topic_configs[t]
            assert configs == other_configs, \
                f"topic configs differ for topic {t}: {configs} vs {other_configs}"
            partitions = self.topic_partitions[t]
            other_partitions = other.topic_partitions[t]
            assert partitions == other_partitions, \
                f"topic partitions differ for topic {t}: {partitions} vs {other_partitions}"

    def _check_brokers(self, other):
        symdiff = set(self.brokers.keys()) ^ set(other.brokers.keys())
        assert len(symdiff) == 0, f"brokers differ, symdiff: {symdiff}"
        for id, broker in self.brokers.items():
            assert broker == other.brokers[id], \
                f"brokers differ for node id {id}: {broker} vs {other.brokers[id]}"

    def _check_security(self, other):
        assert self.users == other.users
        assert self.acls == other.acls

    def check(self, other):
        self._check_features(other)
        self._check_config(other)
        self._check_topics(other)
        self._check_brokers(other)
        self._check_security(other)


class ControllerSnapshotTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         num_brokers=4,
                         extra_rp_conf={
                             'controller_snapshot_max_age_sec': 5,
                         },
                         **kwargs)

    def setUp(self):
        # start the nodes manually
        pass

    @cluster(num_nodes=4)
    @matrix(auto_assign_node_ids=[False, True])
    def test_bootstrap(self, auto_assign_node_ids):
        """
        Test that Redpanda nodes can assemble into a cluster when controller snapshots are enabled. In particular, test that:
        1. Nodes can join
        2. Nodes can restart and the cluster remains healthy
        3. Node ids and cluster uuid remain stable
        """

        seed_nodes = self.redpanda.nodes[0:3]
        joiner_node = self.redpanda.nodes[3]
        self.redpanda.set_seed_servers(seed_nodes)

        self.redpanda.start(nodes=seed_nodes,
                            auto_assign_node_id=auto_assign_node_ids,
                            omit_seeds_on_idx_one=False)
        admin = Admin(self.redpanda, default_node=seed_nodes[0])
        admin.put_feature("controller_snapshots", {"state": "active"})

        for n in seed_nodes:
            self.redpanda.wait_for_controller_snapshot(n)

        cluster_uuid = admin.get_cluster_uuid(node=seed_nodes[0])
        assert cluster_uuid is not None

        node_ids_per_idx = {}

        def check_and_save_node_ids(started):
            for n in started:
                uuid = admin.get_cluster_uuid(node=n)
                assert cluster_uuid == uuid, f"unexpected cluster uuid {uuid}"

                brokers = admin.get_brokers(node=n)
                assert len(brokers) == len(started)

                node_id = self.redpanda.node_id(n, force_refresh=True)
                idx = self.redpanda.idx(n)
                if idx in node_ids_per_idx:
                    expected_node_id = node_ids_per_idx[idx]
                    assert expected_node_id == node_id,\
                        f"Expected {expected_node_id} but got {node_id}"
                else:
                    node_ids_per_idx[idx] = node_id

        check_and_save_node_ids(seed_nodes)

        self.logger.info(f"seed nodes formed cluster, uuid: {cluster_uuid}")

        self.redpanda.restart_nodes(seed_nodes,
                                    auto_assign_node_id=auto_assign_node_ids,
                                    omit_seeds_on_idx_one=False)
        self.redpanda.wait_for_membership(first_start=False)

        check_and_save_node_ids(seed_nodes)

        self.logger.info(f"seed nodes restarted successfully")

        self.redpanda.start(nodes=[joiner_node],
                            auto_assign_node_id=auto_assign_node_ids,
                            omit_seeds_on_idx_one=False)
        self.redpanda.wait_for_membership(first_start=True)

        check_and_save_node_ids(self.redpanda.nodes)

        self.logger.info("cluster fully bootstrapped, restarting...")

        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    auto_assign_node_id=auto_assign_node_ids,
                                    omit_seeds_on_idx_one=False)
        self.redpanda.wait_for_membership(first_start=False)

        check_and_save_node_ids(self.redpanda.nodes)

        self.logger.info("cluster restarted successfully")

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_join_restart_catch_up(self):
        """
        Test that a node correctly restores the controller state after it joins, restarts
        or has to catch up by installing a new controller snapshot after being offline for
        a bit. Make the test interesting by executing a few admin operations between
        restarts.
        """

        seed_nodes = self.redpanda.nodes[0:3]
        joiner = self.redpanda.nodes[3]
        self.redpanda.set_seed_servers(seed_nodes)

        # Start first three nodes
        self.redpanda.start(seed_nodes)

        admin = Admin(self.redpanda, default_node=seed_nodes[0])
        rpk = RpkTool(self.redpanda)

        # change controller state

        # could be any non-default value for any property
        self.redpanda.set_cluster_config(
            {'controller_snapshot_max_age_sec': 10})
        # turn off partition balancing to minimize the number of internally-generated
        # controller commands
        self.redpanda.set_cluster_config(
            {'partition_autobalancing_mode': 'off'})

        rpk.create_topic('test_topic')

        admin.create_user(username='test',
                          password='p4ssw0rd',
                          algorithm='SCRAM-SHA-256')
        rpk.acl_create_allow_cluster(username='test', op='describe')

        ops_fuzzer = AdminOperationsFuzzer(self.redpanda, min_replication=3)
        ops_fuzzer.create_initial_entities()

        def wait_for_everything_snapshotted(nodes):
            controller_max_offset = max(
                admin.get_controller_status(n)['commited_index']
                for n in nodes)
            self.logger.info(
                f"controller max offset is {controller_max_offset}")

            for n in nodes:
                self.redpanda.wait_for_controller_snapshot(
                    node=n, prev_start_offset=(controller_max_offset - 1))

            return controller_max_offset

        admin.put_feature("controller_snapshots", {"state": "active"})
        wait_for_everything_snapshotted(seed_nodes)

        # check initial state
        initial_state = ControllerState(self.redpanda, seed_nodes[0])
        assert initial_state.features_map['controller_snapshots'][
            'state'] == 'active'
        assert initial_state.config_response[
            'controller_snapshot_max_age_sec'] == 10
        assert 'test_topic' in initial_state.topics
        assert set(['admin', 'test']).issubset(initial_state.users)
        assert len(initial_state.acls) >= 2

        # make a node join and check its state

        # explicit clean step is needed because we are starting the node manually and not
        # using redpanda.start()
        self.redpanda.clean_node(joiner)
        self.redpanda.start_node(joiner)
        wait_until(lambda: self.redpanda.registered(joiner),
                   timeout_sec=30,
                   backoff_sec=1)

        def check(node, expected):
            node_id = self.redpanda.node_id(node)
            self.logger.info(f"checking node {node.name} (id: {node_id})...")
            admin.transfer_leadership_to(namespace='redpanda',
                                         topic='controller',
                                         partition=0,
                                         target_id=node_id)
            admin.await_stable_leader(namespace='redpanda',
                                      topic='controller',
                                      check=lambda id: id == node_id)

            state = ControllerState(self.redpanda, node)
            expected.check(state)

        expected_state = ControllerState(self.redpanda, seed_nodes[0])
        check(joiner, expected_state)

        # restart and check

        self.redpanda.restart_nodes(self.redpanda.nodes)
        self.redpanda.wait_for_membership(first_start=False)

        for n in self.redpanda.nodes:
            check(n, expected_state)

        # do a few batches of admin operations, forcing the joiner node to catch up to the
        # recent controller state each time.

        for iter in range(5):
            self.redpanda.stop_node(joiner)

            executed = 0
            to_execute = random.randint(1, 5)
            while executed < to_execute:
                if ops_fuzzer.execute_one():
                    executed += 1

            controller_max_offset = wait_for_everything_snapshotted(seed_nodes)

            # start joiner and wait until it catches up
            self.redpanda.start_node(joiner)
            wait_until(lambda: self.redpanda.registered(joiner),
                       timeout_sec=30,
                       backoff_sec=1)
            wait_until(lambda: admin.get_controller_status(joiner)[
                'last_applied_offset'] >= controller_max_offset,
                       timeout_sec=30,
                       backoff_sec=1)

            expected_state = ControllerState(self.redpanda, seed_nodes[0])
            check(joiner, expected_state)

            self.logger.info(
                f"iteration {iter} done (executed {to_execute} ops).")
