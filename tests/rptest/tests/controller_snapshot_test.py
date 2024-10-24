# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, RedpandaInstaller
from rptest.services.redpanda_installer import ver_triple, RedpandaVersionTriple, RedpandaVersionLine
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer
from rptest.clients.rpk import RpkTool
from rptest.util import wait_until_result

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

import random
import time


class ControllerSnapshotPolicyTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)

    def setUp(self):
        # start the nodes manually
        pass

    @cluster(num_nodes=3)
    def test_snapshotting_policy(self):
        """
        Test that Redpanda creates a controller snapshot some time after controller commands appear.
        """
        admin = Admin(self.redpanda)
        self.redpanda.set_extra_rp_conf({'controller_snapshot_max_age_sec': 5})
        self.redpanda.start()
        self.redpanda.set_feature_active('controller_snapshots',
                                         False,
                                         timeout_sec=10)

        for n in self.redpanda.nodes:
            controller_status = admin.get_controller_status(n)
            assert controller_status['start_offset'] == 0

        self.redpanda.set_feature_active('controller_snapshots',
                                         True,
                                         timeout_sec=10)

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
        self.config_response = admin.get_cluster_config(node=node,
                                                        include_defaults=False)
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
        brokers_resp = admin.get_brokers(node=node)
        for broker in brokers_resp:
            self.brokers[broker['node_id']] = dict(
                (k, broker.get(k)) for k in broker_fields)

        # New brokers may not report the version yet, so gather the cluster version
        # based on multiple brokers' reported version
        versions = list(
            set(broker['version'] for broker in brokers_resp
                if 'version' in broker))
        assert len(versions) == 1, f"Unexpected versions: {versions}"
        self.version_line = versions[0]

        self.version = ver_triple(self.version_line)

    def _check_features(self, other, allow_upgrade_changes=False):
        for k, v in self.features_response.items():
            if k == 'features':
                continue
            new_v = other.features_response.get(k)
            if allow_upgrade_changes and k in ('cluster_version',
                                               'node_latest_version',
                                               'node_earliest_version'):
                assert new_v >= v, f"unexpected value, ({k=}, {new_v=}, {v=})"
            else:
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

    def _check_topics(self, other, allow_upgrade_changes=False):
        symdiff = self.topics ^ other.topics
        assert len(symdiff) == 0, f"topics differ, symdiff: {symdiff}"

        def to_set(configs):
            if allow_upgrade_changes:
                return set((k, v) for k, v in configs.items()
                           if v[1] != 'DEFAULT_CONFIG')
            else:
                return set(configs.items())

        for t in self.topics:
            symdiff = to_set(self.topic_configs[t]) ^ to_set(
                other.topic_configs[t])

            # Newer versions include a bugfix to change the source type of cleanup.policy from
            # DYNAMIC_TOPIC_CONFIG to DEFAULT_CONFIG if the cleanup.policy wasn't specified as
            # a config value when the topic is created.
            # All versions newer that are at least as new as 24.2.1 will have the fix, and so
            # they don't need this explicit ignore.
            MIN_VERSION_WITH_ALL_LATER_VERSIONS_FIXED = RedpandaVersionTriple(
                (24, 2, 1))
            if self.version < MIN_VERSION_WITH_ALL_LATER_VERSIONS_FIXED:
                symdiff -= set({('cleanup.policy', ('delete',
                                                    'DYNAMIC_TOPIC_CONFIG'))})

            assert len(symdiff) == 0, f"configs differ, symdiff: {symdiff}"

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

    def check(self, other, allow_upgrade_changes=False):
        self._check_features(other, allow_upgrade_changes)
        self._check_config(other)
        self._check_topics(other, allow_upgrade_changes)
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

    def _wait_for_everything_snapshotted(self, nodes):
        controller_max_offset = max(
            Admin(self.redpanda).get_controller_status(n)['committed_index']
            for n in nodes)
        self.logger.info(f"controller max offset is {controller_max_offset}")

        for n in nodes:
            self.redpanda.wait_for_controller_snapshot(
                node=n, prev_start_offset=(controller_max_offset - 1))

        return controller_max_offset

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

        # Start without snapshots, to build up some data
        admin.put_feature("controller_snapshots", {"state": "disabled"})

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

        admin.put_feature("controller_snapshots", {"state": "active"})
        self._wait_for_everything_snapshotted(seed_nodes)

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

            # wait for the joiner to step down in case it was the controller
            joiner_id = self.redpanda.node_id(joiner)
            admin.await_stable_leader(namespace='redpanda',
                                      topic='controller',
                                      check=lambda id: id != joiner_id,
                                      timeout_s=30)

            executed = 0
            to_execute = random.randint(1, 5)
            while executed < to_execute:
                if ops_fuzzer.execute_one():
                    executed += 1

            controller_max_offset = self._wait_for_everything_snapshotted(
                seed_nodes)

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

    @cluster(num_nodes=4)
    def test_upgrade_compat(self):
        """
        Test that redpanda can start with a controller snapshot created by the previous version.
        """

        # turn off partition balancing to minimize the number of internally-generated
        # controller commands
        self.redpanda.add_extra_rp_conf(
            {'partition_autobalancing_mode': 'off'})

        installer = self.redpanda._installer
        initial_ver = installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)
        self.logger.info(
            f"will test compatibility with redpanda v. {initial_ver}")
        installer.install(self.redpanda.nodes, initial_ver)
        self.redpanda.start()

        admin = Admin(self.redpanda)
        rpk = RpkTool(self.redpanda)

        # issue some controller commands

        # could be any non-default value for any property
        self.redpanda.set_cluster_config(
            {'controller_snapshot_max_age_sec': 7})

        rpk.create_topic('test_topic')

        admin.create_user(username='test',
                          password='p4ssw0rd',
                          algorithm='SCRAM-SHA-256')
        rpk.acl_create_allow_cluster(username='test', op='describe')

        self._wait_for_everything_snapshotted(self.redpanda.nodes)

        expected_state = ControllerState(self.redpanda, self.redpanda.nodes[0])

        installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)

        self.redpanda.restart_nodes(self.redpanda.nodes)
        self.redpanda.wait_for_membership(first_start=False)

        state = ControllerState(self.redpanda, self.redpanda.nodes[1])
        expected_state.check(state, allow_upgrade_changes=True)
