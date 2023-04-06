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
from rptest.clients.rpk import RpkTool
from rptest.util import wait_until_result

from ducktape.mark import matrix
from ducktape.utils.util import wait_until


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

        for n in self.redpanda.nodes:
            assert self.redpanda.controller_start_offset(n) == 0

        admin = Admin(self.redpanda)
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
    def test_join_restart(self):
        seed_nodes = self.redpanda.nodes[0:3]
        joiner = self.redpanda.nodes[3]
        self.redpanda.set_seed_servers(seed_nodes)

        # Start first three nodes
        self.redpanda.start(seed_nodes)

        admin = Admin(self.redpanda, default_node=seed_nodes[0])
        rpk = RpkTool(self.redpanda)

        # change controller state

        admin.put_feature("controller_snapshots", {"state": "active"})

        # could be any non-default value for any property
        self.redpanda.set_cluster_config(
            {'controller_snapshot_max_age_sec': 10})

        rpk.create_topic('test_topic')

        admin.create_user(username='test',
                          password='p4ssw0rd',
                          algorithm='SCRAM-SHA-256')
        rpk.acl_create_allow_cluster(username='test', op='describe')

        # wait for controller snapshots
        for n in seed_nodes:
            self.redpanda.wait_for_controller_snapshot(n)

        # record expected values

        def get_features_map(features_response):
            return dict((f['name'], f) for f in features_response['features'])

        initial_features = admin.get_features()
        initial_features_map = get_features_map(initial_features)
        assert initial_features_map['controller_snapshots'][
            'state'] == 'active'

        def check_features_response(features):
            for k, v in initial_features.items():
                if k == 'features':
                    continue
                new_v = features.get(k)
                assert new_v == v, \
                    f"features response mismatch (key: {k}): got {new_v}, expected {v}"

            features_map = get_features_map(features)
            for f, v in initial_features_map.items():
                new_v = features_map.get(f)
                assert new_v == v, \
                    f"features map mismatch (feature: {f}): got {new_v}, expected {v}"

        initial_config_resp = admin.get_cluster_config()
        assert initial_config_resp['controller_snapshot_max_age_sec'] == 10

        def check_config_response(config_resp):
            def to_set(resp):
                return set((k, (v if not isinstance(v, list) else tuple(v)))
                           for k, v in resp.items())

            symdiff = to_set(initial_config_resp) ^ to_set(config_resp)
            assert len(
                symdiff) == 0, f"config responses differ, symdiff: {symdiff}"

        initial_topics = set(rpk.list_topics())
        assert initial_topics == set(['test_topic'])

        initial_users = set(admin.list_users())
        assert initial_users == set(['admin', 'test'])
        initial_acls = rpk.acl_list()
        assert len(initial_acls.strip().split('\n')) == 2

        def check_security(node):
            users = set(admin.list_users(node=node))
            assert users == initial_users

            acls = rpk.acl_list()
            assert acls == initial_acls

        def check(node):
            node_id = self.redpanda.node_id(node)
            admin.transfer_leadership_to(namespace='redpanda',
                                         topic='controller',
                                         partition=0,
                                         target_id=node_id)
            admin.await_stable_leader(namespace='redpanda',
                                      topic='controller',
                                      check=lambda id: id == node_id)

            check_features_response(admin.get_features(node=node))
            check_config_response(admin.get_cluster_config(node=node))
            assert set(rpk.list_topics()) == set(['test_topic'])
            check_security(node)

        # make a node join and check it

        self.redpanda.start_node(joiner)
        wait_until(lambda: self.redpanda.registered(joiner),
                   timeout_sec=30,
                   backoff_sec=1)

        check(joiner)

        # restart and check

        self.redpanda.restart_nodes(self.redpanda.nodes)
        self.redpanda.wait_for_membership(first_start=False)

        for n in self.redpanda.nodes:
            check(n)
