# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random

import requests
from rptest.clients.kafka_cat import KafkaCat
from time import sleep
from rptest.clients.default import DefaultClient

from rptest.utils.mode_checks import skip_debug_mode
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize
from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST, RESTART_LOG_ALLOW_LIST, RedpandaService
from rptest.utils.node_operations import NodeDecommissionWaiter


class NodesDecommissioningTest(EndToEndTest):
    """
    Basic nodes decommissioning test.
    """
    def __init__(self, test_context):

        super(NodesDecommissioningTest,
              self).__init__(test_context=test_context)

    @property
    def admin(self):
        # retry on timeout and service unavailable
        return Admin(self.redpanda, retry_codes=[503, 504])

    def _create_topics(self, replication_factors=[1, 3]):
        topics = []
        for i in range(10):
            spec = TopicSpec(
                partition_count=random.randint(1, 10),
                replication_factor=random.choice(replication_factors))
            topics.append(spec)

        for spec in topics:
            self.client().create_topic(spec)

        self.topic = random.choice(topics).name

    def _partitions_moving(self, node=None):
        reconfigurations = self.admin.list_reconfigurations(node=node)
        return len(reconfigurations) > 0

    def _partitions_not_moving(self):
        reconfigurations = self.admin.list_reconfigurations()
        return len(reconfigurations) == 0

    def _partition_to_move(self, predicate):
        rpk = RpkTool(self.redpanda)

        for tp in rpk.list_topics():
            desc = rpk.describe_topic(tp)
            for p in desc:
                if predicate(p):
                    return (tp, p.id, p.replicas)

    def _not_decommissioned_node(self, *args):
        decom_node_ids = args
        return [
            n for n in self.redpanda.started_nodes()
            if self.redpanda.node_id(n) not in decom_node_ids
        ][0]

    def _node_removed(self, removed_id, node_to_query):
        brokers = self.admin.get_brokers(node=node_to_query)
        for b in brokers:
            if b['node_id'] == removed_id:
                return False
        return True

    def _find_replacement(self, current_replicas, to_remove):
        new_replicas = []
        unique_node_ids = set()
        for r in current_replicas:
            if r['node_id'] != to_remove:
                unique_node_ids.add(r['node_id'])
                new_replicas.append(r)

        brokers = self.admin.get_brokers()

        to_add = None
        while len(unique_node_ids) < len(current_replicas):
            id = random.choice(brokers)['node_id']
            if id == to_remove:
                continue
            to_add = id
            unique_node_ids.add(to_add)

        new_replicas.append({"node_id": to_add, "core": 0})
        return new_replicas

    def _wait_until_status(self,
                           node_id,
                           status,
                           timeout_sec=15,
                           api_node=None):
        def requested_status():
            brokers = self.admin.get_brokers(node=api_node)
            for broker in brokers:
                if broker['node_id'] == node_id:
                    return broker['membership_status'] == status
            return False

        wait_until(requested_status, timeout_sec=timeout_sec, backoff_sec=1)

    def _set_recovery_rate(self, rate):
        # use admin API to leverage the retry policy when controller returns 503
        patch_result = self.admin.patch_cluster_config(
            upsert={"raft_learner_recovery_rate": rate})
        self.logger.debug(
            f"setting recovery rate to {rate} result: {patch_result}")

    # after node was removed the state should be consistent on all other not removed nodes
    def _check_state_consistent(self, decommissioned_id):

        not_decommissioned = [
            n for n in self.redpanda.started_nodes()
            if self.redpanda.node_id(n) != decommissioned_id
        ]

        def _state_consistent():

            for n in not_decommissioned:
                cfg_status = self.admin.get_cluster_config_status(n)
                brokers = self.admin.get_brokers(n)
                config_ids = [s['node_id'] for s in cfg_status]
                brokers_ids = [b['node_id'] for b in brokers]
                if sorted(brokers_ids) != sorted(config_ids):
                    return False
                if decommissioned_id in brokers_ids:
                    return False

            return True

        wait_until(_state_consistent, 10, 1)

    def _wait_for_node_removed(self, decommissioned_id):

        waiter = NodeDecommissionWaiter(self.redpanda,
                                        decommissioned_id,
                                        self.logger,
                                        progress_timeout=60)
        waiter.wait_for_removal()

        self._check_state_consistent(decommissioned_id)

    def _decommission(self, node_id, node=None):
        def decommissioned():
            try:

                results = []
                for n in self.redpanda.nodes:
                    if self.redpanda.node_id(n) == node_id:
                        continue

                    brokers = self.admin.get_brokers(node=n)
                    for b in brokers:
                        if b['node_id'] == node_id:
                            results.append(
                                b['membership_status'] == 'draining')

                if all(results):
                    return True

                self.admin.decommission_broker(node_id, node=node)
            except requests.exceptions.RetryError:
                return False
            except requests.exceptions.ConnectionError:
                return False
            except requests.exceptions.HTTPError:
                return False

        wait_until(decommissioned, 30, 1)

    def _recommission(self, node_id, node=None):
        def recommissioned():
            try:
                results = []
                for n in self.redpanda.started_nodes():
                    brokers = self.admin.get_brokers(node=n)
                    for b in brokers:
                        if b['node_id'] == node_id:
                            results.append(b['membership_status'] == 'active')

                if all(results):
                    return True
                self.admin.recommission_broker(node_id, node=node)
            except requests.exceptions.RetryError:
                return False
            except requests.exceptions.ConnectionError:
                return False
            except requests.exceptions.HTTPError:
                return False

        wait_until(recommissioned, 30, 1)

    @cluster(
        num_nodes=6,
        # A decom can look like a restart in terms of logs from peers dropping
        # connections with it
        log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(delete_topic=True)
    @parametrize(delete_topic=False)
    def test_decommissioning_working_node(self, delete_topic):
        self.start_redpanda(num_nodes=4)
        self._create_topics()

        self.start_producer(1)
        self.start_consumer(1)

        to_decommission = random.choice(self.redpanda.nodes)
        to_decommission_id = self.redpanda.idx(to_decommission)
        self.logger.info(f"decommissioning node: {to_decommission_id}", )
        self._decommission(to_decommission_id)
        if delete_topic:
            self.client().delete_topic(self.topic)
        self._wait_for_node_removed(to_decommission_id)

        # Stop the decommissioned node, because redpanda internally does not
        # fence it, it is the responsibility of external orchestrator to
        # stop the node they intend to remove.
        # This can be removed when we change redpanda to prevent decommissioned nodes
        # from responding to client Kafka requests.
        self.redpanda.stop_node(to_decommission)

        if not delete_topic:
            self.run_validation(enable_idempotence=False,
                                consumer_timeout_sec=45)

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_decommissioning_crashed_node(self):

        self.start_redpanda(num_nodes=4)
        self._create_topics(replication_factors=[3])

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        to_decommission = self.redpanda.nodes[1]
        node_id = self.redpanda.idx(to_decommission)
        self.redpanda.stop_node(node=to_decommission)
        self.logger.info(f"decommissioning node: {node_id}", )
        self._decommission(node_id)

        self._wait_for_node_removed(node_id)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

    @cluster(
        num_nodes=6,
        # A decom can look like a restart in terms of logs from peers dropping
        # connections with it
        log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommissioning_cancel_ongoing_movements(self):
        self.start_redpanda(num_nodes=4)
        self._create_topics()

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        brokers = self.admin.get_brokers()
        to_decommission = random.choice(brokers)['node_id']

        # throttle recovery
        self._set_recovery_rate(1)

        # schedule partition move to the node that is being decommissioned

        tp_to_move, p_to_move, replicas = self._partition_to_move(
            lambda p: to_decommission not in p.replicas)

        details = self.admin.get_partitions(topic=tp_to_move,
                                            partition=p_to_move)

        new_replicas = details['replicas']
        new_replicas = new_replicas[1:]
        new_replicas.append({"node_id": to_decommission, "core": 0})

        self.logger.info(
            f"moving partition {tp_to_move}/{p_to_move} - {details['replicas']} -> {new_replicas}"
        )

        self.admin.set_partition_replicas(topic=tp_to_move,
                                          partition=p_to_move,
                                          replicas=new_replicas)
        # moving partition should be present in moving list
        wait_until(lambda: self._partitions_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

        self.logger.info(f"decommissioning node: {to_decommission}", )
        self._decommission(to_decommission)

        # adjust recovery throttle to make sure moves will finish
        self._set_recovery_rate(2 << 30)

        self._wait_for_node_removed(to_decommission)
        # stop decommissioned node
        self.redpanda.stop_node(self.redpanda.get_node(to_decommission))

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=90)

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_recommissioning_node(self):
        self.start_redpanda(num_nodes=4)
        self._create_topics()

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        brokers = self.admin.get_brokers()
        to_decommission = random.choice(brokers)['node_id']

        # throttle recovery
        self._set_recovery_rate(1)

        self.logger.info(f"decommissioning node: {to_decommission}", )
        self._decommission(to_decommission)

        self._wait_until_status(to_decommission, 'draining')

        wait_until(lambda: self._partitions_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

        # recommission broker
        self._recommission(to_decommission)
        self._wait_until_status(to_decommission, 'active')

        wait_until(lambda: self._partitions_not_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_recommissioning_node_finishes(self):
        self.start_redpanda(num_nodes=4)
        self._create_topics()

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        brokers = self.admin.get_brokers()
        to_decommission = random.choice(brokers)['node_id']

        # throttle recovery
        self._set_recovery_rate(1)

        self.logger.info(f"decommissioning node: {to_decommission}", )
        self._decommission(to_decommission)

        self._wait_until_status(to_decommission, 'draining')

        wait_until(lambda: self._partitions_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

        # recommission broker
        self._recommission(to_decommission)
        self._wait_until_status(to_decommission, 'active')

        wait_until(lambda: self._partitions_not_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

        self._decommission(to_decommission)

        self._wait_until_status(to_decommission, 'draining')
        self._set_recovery_rate(1024 * 1024 * 1024)
        self._wait_for_node_removed(to_decommission)

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_recommissioning_do_not_stop_all_moves_node(self):
        self.start_redpanda(num_nodes=4)
        self._create_topics()

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        brokers = self.admin.get_brokers()
        to_decommission = random.choice(brokers)['node_id']

        # throttle recovery
        self._set_recovery_rate(1)

        # schedule partition move from the node being decommissioned before actually calling decommission

        to_move_tp, to_move_p, _ = self._partition_to_move(
            lambda p: to_decommission in p.replicas)
        details = self.admin.get_partitions(topic=to_move_tp,
                                            partition=to_move_p)

        new_replicas = self._find_replacement(details['replicas'],
                                              to_decommission)
        self.logger.info(
            f"moving partition {to_move_tp}/{to_move_p} - {details['replicas']} -> {new_replicas}"
        )

        self.admin.set_partition_replicas(topic=to_move_tp,
                                          partition=to_move_p,
                                          replicas=new_replicas)
        # moving partition should be present in moving list
        wait_until(lambda: self._partitions_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

        self.logger.info(f"decommissioning node: {to_decommission}", )
        self._decommission(to_decommission)

        self._wait_until_status(to_decommission, 'draining')

        wait_until(lambda: self._partitions_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

        # recommission broker
        self._recommission(to_decommission)
        self._wait_until_status(to_decommission, 'active')

        def one_left_moving():
            reconfigurations = self.admin.list_reconfigurations()
            return len(reconfigurations) == 1

        wait_until(one_left_moving, timeout_sec=15, backoff_sec=1)

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_recommissioning_one_of_decommissioned_nodes(self):
        self.start_redpanda(num_nodes=5)
        self._create_topics()

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        brokers = self.admin.get_brokers()
        to_decommission_1 = random.choice(brokers)['node_id']
        to_decommission_2 = to_decommission_1

        while to_decommission_1 == to_decommission_2:
            to_decommission_2 = random.choice(brokers)['node_id']

        # Send all our subsequent admin API requests to a node which is
        # not going to be decommed.
        survivor_node = self._not_decommissioned_node(to_decommission_1,
                                                      to_decommission_2)

        # throttle recovery
        self._set_recovery_rate(1)

        self.logger.info(f"decommissioning node: {to_decommission_1}", )
        self._decommission(to_decommission_1, node=survivor_node)
        self.logger.info(f"decommissioning node: {to_decommission_2}", )
        self._decommission(to_decommission_2, node=survivor_node)

        self._wait_until_status(to_decommission_1,
                                'draining',
                                api_node=survivor_node)
        self._wait_until_status(to_decommission_2,
                                'draining',
                                api_node=survivor_node)

        wait_until(lambda: self._partitions_moving(node=survivor_node),
                   timeout_sec=15,
                   backoff_sec=1)

        # recommission broker that was decommissioned first
        self._recommission(to_decommission_1, node=survivor_node)
        self._wait_until_status(to_decommission_1,
                                'active',
                                api_node=survivor_node)

        self._set_recovery_rate(2 << 30)
        self._wait_for_node_removed(to_decommission_2)

    def producer_throughput(self):
        return 1000 if self.debug_mode else 100000

    def records_to_wait(self):
        return 8 * self.producer_throughput()

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(shutdown_decommissioned=True)
    @parametrize(shutdown_decommissioned=False)
    def test_decommissioning_rebalancing_node(self, shutdown_decommissioned):
        # start redpanda with disabled rebalancing
        self.redpanda = RedpandaService(
            self.test_context,
            4,
            extra_rp_conf={"partition_autobalancing_mode": "node_add"})
        # start 3 nodes
        self.redpanda.start(nodes=self.redpanda.nodes[0:3])
        self._client = DefaultClient(self.redpanda)
        self._rpk_client = RpkTool(self.redpanda)

        topic = TopicSpec(partition_count=64, replication_factor=3)

        self.client().create_topic(topic)
        self.topic = topic.name

        self.start_producer(1, throughput=self.producer_throughput())
        self.start_consumer(1)
        self.await_startup(min_records=self.records_to_wait(), timeout_sec=120)
        # throttle recovery
        self.redpanda.clean_node(self.redpanda.nodes[-1],
                                 preserve_current_install=True)
        self.redpanda.start_node(self.redpanda.nodes[-1])
        self._set_recovery_rate(10)
        # wait for rebalancing to start

        to_decommission = self.redpanda.nodes[-1]
        to_decommission_id = self.redpanda.node_id(to_decommission)
        first_node = self.redpanda.nodes[0]
        wait_until(lambda: self._partitions_moving(node=first_node),
                   timeout_sec=15,
                   backoff_sec=1)

        # request decommission of newly added broker
        self._decommission(to_decommission_id, node=first_node)

        # stop the node that is being decommissioned
        if shutdown_decommissioned:
            self.redpanda.stop_node(to_decommission)

        # shut the broker down
        self._set_recovery_rate(2 << 30)
        self._wait_for_node_removed(to_decommission_id)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=240)

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(delete_topic=True)
    @parametrize(delete_topic=False)
    def test_decommissioning_finishes_after_manual_cancellation(
            self, delete_topic):

        self.start_redpanda(num_nodes=4)
        self._create_topics(replication_factors=[3])

        self.start_producer(1, throughput=self.producer_throughput())
        self.start_consumer(1)
        self.await_startup(min_records=self.records_to_wait(), timeout_sec=180)

        to_decommission = random.choice(self.redpanda.nodes)
        node_id = self.redpanda.node_id(to_decommission)

        survivor_node = self._not_decommissioned_node(node_id)
        self.logger.info(f"decommissioning node: {node_id}", )
        self._decommission(node_id)

        self._set_recovery_rate(100)
        # wait for some partitions to start moving
        wait_until(lambda: self._partitions_moving(node=survivor_node),
                   timeout_sec=15,
                   backoff_sec=1)
        # cancel all reconfigurations
        self.admin.cancel_all_reconfigurations()

        if delete_topic:
            self.client().delete_topic(self.topic)

        self._set_recovery_rate(2 << 30)
        self._wait_for_node_removed(node_id)

        if not delete_topic:
            self.run_validation(enable_idempotence=False,
                                consumer_timeout_sec=240)

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(node_is_alive=True)
    @parametrize(node_is_alive=False)
    def test_flipping_decommission_recommission(self, node_is_alive):

        self.start_redpanda(num_nodes=4)
        self._create_topics(replication_factors=[3])

        self.start_producer(1, throughput=self.producer_throughput())
        self.start_consumer(1)
        self.await_startup(min_records=self.records_to_wait(), timeout_sec=180)

        to_decommission = random.choice(self.redpanda.nodes)
        node_id = self.redpanda.node_id(to_decommission)

        survivor_node = self._not_decommissioned_node(node_id)

        if not node_is_alive:
            self.redpanda.stop_node(to_decommission)

        self._set_recovery_rate(1)
        for i in range(1, 10):
            # set recovery rate to small value to prevent node
            # from finishing decommission operation
            self.logger.info(f"decommissioning node: {node_id}")
            self._decommission(node_id)
            wait_until(lambda: self._partitions_moving(node=survivor_node) or
                       self._node_removed(node_id, survivor_node),
                       timeout_sec=60,
                       backoff_sec=1)
            if self._node_removed(node_id, survivor_node):
                break
            self.logger.info(f"recommissioning node: {node_id}", )
            self._recommission(node_id)

        self._set_recovery_rate(100 * 1024 * 1024)
        if not self._node_removed(node_id, survivor_node):
            # finally decommission node
            self.logger.info(f"decommissioning node: {node_id}", )
            self._decommission(node_id)

            self._wait_for_node_removed(node_id)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=240)

    def _replicas_per_node(self):
        kafkacat = KafkaCat(self.redpanda)
        node_replicas = {}
        md = kafkacat.metadata()
        self.redpanda.logger.debug(f"metadata: {md}")
        for topic in md['topics']:
            for p in topic['partitions']:
                for r in p['replicas']:
                    id = r['id']
                    if id not in node_replicas:
                        node_replicas[id] = 0
                    node_replicas[id] += 1

        return node_replicas

    @skip_debug_mode
    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_multiple_decommissions(self):
        self._extra_node_conf = {"empty_seed_starts_cluster": False}
        self.start_redpanda(num_nodes=5, new_bootstrap=True)
        self._create_topics()

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        def node_by_id(node_id):
            for n in self.redpanda.nodes:
                if self.redpanda.node_id(n) == node_id:
                    return n
            return None

        for i in range(0, 2):
            for b in self.redpanda.nodes:
                id = self.redpanda.node_id(b, force_refresh=True)
                self.logger.info(f"decommissioning node: {id}, iteration: {i}")

                decom_node = node_by_id(id)
                self._decommission(id)
                self._wait_for_node_removed(id)

                def has_partitions():
                    id = self.redpanda.node_id(node=decom_node,
                                               force_refresh=True)
                    per_node = self._replicas_per_node()
                    self.logger.info(f'replicas per node: {per_node}')
                    return id in per_node and per_node[id] > 0

                self.redpanda.stop_node(node=decom_node)
                self.redpanda.clean_node(node=decom_node, preserve_logs=True)
                self.redpanda.start_node(node=decom_node,
                                         auto_assign_node_id=True,
                                         omit_seeds_on_idx_one=False)

                wait_until(has_partitions, 180, 2)

        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    auto_assign_node_id=True,
                                    omit_seeds_on_idx_one=False)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=180)

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(new_bootstrap=True)
    @parametrize(new_bootstrap=False)
    def test_node_is_not_allowed_to_join_after_restart(self, new_bootstrap):
        self.start_redpanda(num_nodes=4, new_bootstrap=new_bootstrap)
        self._create_topics()

        to_decommission = self.redpanda.nodes[-1]
        to_decommission_id = self.redpanda.node_id(to_decommission)
        self.logger.info(f"decommissioning node: {to_decommission_id}")
        self._decommission(to_decommission_id)
        self._wait_for_node_removed(to_decommission_id)

        # restart decommissioned node without cleaning up the data directory,
        # the node should not be allowed to join the cluster
        # back as it was decommissioned
        self.redpanda.stop_node(to_decommission)
        self.redpanda.start_node(to_decommission,
                                 auto_assign_node_id=new_bootstrap,
                                 omit_seeds_on_idx_one=not new_bootstrap,
                                 skip_readiness_check=True)

        # wait until decommissioned node attempted to join the cluster back
        def tried_to_join():
            # allow fail as `grep` return 1 when no entries matches
            # the ssh access shouldn't be a problem as only few lines are transferred
            logs = to_decommission.account.ssh_output(
                f'tail -n 200 {RedpandaService.STDOUT_STDERR_CAPTURE} | grep members_manager | grep WARN',
                allow_fail=True).decode()

            # check if there are at least 3 failed join attempts
            return sum([
                1 for l in logs.splitlines() if "Error joining cluster" in l
            ]) >= 3

        wait_until(tried_to_join, 20, 1)

        assert len(self.admin.get_brokers(node=self.redpanda.nodes[0])) == 3
        self.redpanda.stop_node(to_decommission)
        # clean node and restart it, it should join the cluster
        self.redpanda.clean_node(to_decommission, preserve_logs=True)
        self.redpanda.start_node(to_decommission,
                                 omit_seeds_on_idx_one=not new_bootstrap,
                                 auto_assign_node_id=new_bootstrap)

        assert len(self.admin.get_brokers(node=self.redpanda.nodes[0])) == 4
