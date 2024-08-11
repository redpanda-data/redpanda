# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import concurrent
import random
import time

import requests
from rptest.clients.kafka_cat import KafkaCat
from time import sleep
from rptest.clients.default import DefaultClient
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.tests.prealloc_nodes import PreallocNodesTest

from rptest.utils.mode_checks import skip_debug_mode
from rptest.util import wait_for_recovery_throttle_rate
from rptest.clients.rpk import RpkTool
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize
from ducktape.mark import matrix
from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST, RESTART_LOG_ALLOW_LIST, RedpandaService, SISettings
from rptest.utils.node_operations import NodeDecommissionWaiter


class NodesDecommissioningTest(PreallocNodesTest):
    """
    Basic nodes decommissioning test.
    """
    def __init__(self, test_context):
        self._topic = None

        super(NodesDecommissioningTest,
              self).__init__(test_context=test_context,
                             num_brokers=5,
                             node_prealloc_count=1)

    def setup(self):
        # defer starting redpanda to test body
        pass

    @property
    def admin(self):
        # retry on timeout and service unavailable
        return Admin(self.redpanda, retry_codes=[503, 504])

    def _create_topics(self, replication_factors=[1, 3]):
        """
        :return: total number of partitions in all topics
        """
        total_partitions = 0
        topics = []
        for i in range(10):
            partitions = random.randint(1, 10)
            spec = TopicSpec(
                partition_count=partitions,
                replication_factor=random.choice(replication_factors))
            topics.append(spec)
            total_partitions += partitions

        for spec in topics:
            self.client().create_topic(spec)

        self._topic = random.choice(topics).name

        return total_partitions

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

    def _set_recovery_rate(self, new_rate: int):
        # use admin API to leverage the retry policy when controller returns 503
        patch_result = self.admin.patch_cluster_config(
            upsert={"raft_learner_recovery_rate": new_rate})
        self.logger.debug(
            f"setting recovery rate to {new_rate} result: {patch_result}")
        wait_for_recovery_throttle_rate(redpanda=self.redpanda,
                                        new_rate=new_rate)

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
                self.logger.info(
                    f"broker_ids: {brokers_ids}, ids from configuration status: {config_ids}"
                )
                if sorted(brokers_ids) != sorted(config_ids):
                    return False
                if decommissioned_id in brokers_ids:
                    return False

            return True

        wait_until(
            _state_consistent,
            10,
            1,
            err_msg=
            "Timeout waiting for nodes reported from configuration and cluster state to be consistent"
        )

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

    @property
    def msg_size(self):
        return 64

    @property
    def msg_count(self):
        return int(20 * self.producer_throughput / self.msg_size)

    @property
    def producer_throughput(self):
        return 1024 if self.debug_mode else 1024 * 1024

    def start_producer(self):
        self.logger.info(
            f"starting kgo-verifier producer with {self.msg_count} messages of size {self.msg_size} and throughput: {self.producer_throughput} bps"
        )
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self._topic,
            self.msg_size,
            self.msg_count,
            custom_node=self.preallocated_nodes,
            rate_limit_bps=self.producer_throughput)

        self.producer.start(clean=False)

        wait_until(lambda: self.producer.produce_status.acked > 10,
                   timeout_sec=120,
                   backoff_sec=1)

    def start_consumer(self):
        self.consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            self._topic,
            self.msg_size,
            readers=1,
            nodes=self.preallocated_nodes)

        self.consumer.start(clean=False)

    def verify(self):
        self.logger.info(
            f"verifying workload: topic: {self._topic}, with [rate_limit: {self.producer_throughput}, message size: {self.msg_size}, message count: {self.msg_count}]"
        )
        self.producer.wait()

        # Await the consumer that is reading only the subset of data that
        # was written before it started.
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.invalid_reads == 0, f"Invalid reads in topic: {self._topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"
        del self.consumer

        # Start a new consumer to read all data written
        self.start_consumer()
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.invalid_reads == 0, f"Invalid reads in topic: {self._topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"

    def start_redpanda(self, new_bootstrap=True):
        if new_bootstrap:
            self.redpanda.set_seed_servers(self.redpanda.nodes)

        self.redpanda.start(auto_assign_node_id=new_bootstrap,
                            omit_seeds_on_idx_one=not new_bootstrap)

    @cluster(
        num_nodes=6,
        # A decom can look like a restart in terms of logs from peers dropping
        # connections with it
        log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(delete_topic=[True, False], tick_interval=[5000, 3600000])
    def test_decommissioning_working_node(self, delete_topic, tick_interval):
        self.start_redpanda()
        self.redpanda.set_cluster_config({
            'partition_autobalancing_tick_interval_ms':
            tick_interval,
            'partition_autobalancing_concurrent_moves':
            2
        })
        self._create_topics()

        self.start_producer()
        self.start_consumer()

        to_decommission = random.choice(self.redpanda.nodes)
        to_decommission_id = self.redpanda.node_id(to_decommission)
        self.logger.info(f"decommissioning node: {to_decommission_id}", )
        self._decommission(to_decommission_id)
        if delete_topic:
            self.client().delete_topic(self._topic)
        self._wait_for_node_removed(to_decommission_id)

        # Stop the decommissioned node, because redpanda internally does not
        # fence it, it is the responsibility of external orchestrator to
        # stop the node they intend to remove.
        # This can be removed when we change redpanda to prevent decommissioned nodes
        # from responding to client Kafka requests.
        self.redpanda.stop_node(to_decommission)

        if not delete_topic:
            self.verify()

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_decommissioning_crashed_node(self):

        self.start_redpanda()
        self._create_topics(replication_factors=[3])

        self.start_producer()
        self.start_consumer()

        to_decommission = self.redpanda.nodes[1]
        node_id = self.redpanda.node_id(to_decommission)
        self.redpanda.stop_node(node=to_decommission)
        self.logger.info(f"decommissioning node: {node_id}", )
        self._decommission(node_id)

        self._wait_for_node_removed(node_id)

        self.verify()

    @cluster(
        num_nodes=6,
        # A decom can look like a restart in terms of logs from peers dropping
        # connections with it
        log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommissioning_cancel_ongoing_movements(self):
        self.start_redpanda()
        self._create_topics()

        self.start_producer()
        self.start_consumer()

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
        self.redpanda.stop_node(self.redpanda.get_node_by_id(to_decommission))

        self.verify()

    @skip_debug_mode
    @cluster(num_nodes=6)
    def test_learner_gap_metrics(self):
        self.start_redpanda()
        # set small segment size to calculate the gap correctly
        self.redpanda.set_cluster_config({"log_segment_size": 1024 * 1024})
        self._topic = TopicSpec(name="gap-test-topic", partition_count=10)
        self.client().create_topic(self._topic)

        self.start_producer()
        self.start_consumer()
        # set recovery rate to small value to stop moves
        self._set_recovery_rate(1)

        def calculate_total_learners_gap() -> int | None:
            gap = self.redpanda.metrics_sample("learners_gap_bytes")
            if gap is None:
                return None
            return sum(g.value for g in gap.samples)

        assert calculate_total_learners_gap(
        ) == 0, "when there are no pending partition movements the reported gap should be equal to 0"

        to_decommission = random.choice(self.redpanda.nodes)
        to_decommission_id = self.redpanda.node_id(to_decommission)

        self.logger.info(f"decommissioning node: {to_decommission_id}", )
        self._decommission(to_decommission_id)
        self.producer.wait()

        def learner_gap_reported(decommissioned_node_id: int):
            total_gap = calculate_total_learners_gap()
            p_size = self.redpanda.metrics_sample("partition_size")
            if not total_gap or not p_size:
                return False
            total_size = sum(
                ps.value for ps in p_size.samples
                if self.redpanda.node_id(ps.node) == decommissioned_node_id)

            self.logger.info(
                f"decommissioned node total size: {total_size}, total_gap: {total_gap}"
            )
            assert total_gap < total_size, "gap can not be larger than the size of partitions"
            # assume that the total gap is equal to the total size of
            # decommissioned node with the tolerance of 5%
            return (total_size - total_gap) < total_size * 0.05

        wait_until(lambda: learner_gap_reported(to_decommission_id),
                   timeout_sec=60,
                   backoff_sec=1)
        self._set_recovery_rate(100 * 1024 * 1024)
        # wait for decommissioned node to be removed
        self._wait_for_node_removed(to_decommission_id)

        # Stop the decommissioned node, because redpanda internally does not
        # fence it, it is the responsibility of external orchestrator to
        # stop the node they intend to remove.
        # This can be removed when we change redpanda to prevent decommissioned nodes
        # from responding to client Kafka requests.
        self.redpanda.stop_node(to_decommission)

        self.verify()

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_recommissioning_node(self):
        self.start_redpanda()
        self._create_topics()

        self.start_producer()
        self.start_consumer()

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

        self.verify()

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_recommissioning_node_finishes(self):
        self.start_redpanda()
        self._create_topics()

        self.start_producer()
        self.start_consumer()

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

        self.verify()

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_recommissioning_do_not_stop_all_moves_node(self):
        self.start_redpanda()
        self._create_topics()

        self.start_producer()
        self.start_consumer()

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
        self.verify()

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_recommissioning_one_of_decommissioned_nodes(self):
        self.start_redpanda()
        self._create_topics()

        self.start_producer()
        self.start_consumer()

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
        self.verify()

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(shutdown_decommissioned=True)
    @parametrize(shutdown_decommissioned=False)
    def test_decommissioning_rebalancing_node(self, shutdown_decommissioned):

        # start 4 nodes
        self.redpanda.start(nodes=self.redpanda.nodes[0:4])
        self._client = DefaultClient(self.redpanda)
        self._rpk_client = RpkTool(self.redpanda)

        topic = TopicSpec(partition_count=64, replication_factor=3)

        self.client().create_topic(topic)
        self._topic = topic.name

        self.start_producer()
        self.start_consumer()

        # wait for more data to be produced, so that the cluster re-balancing will not finish immediately
        wait_until(lambda: self.producer.produce_status.acked >
                   (self.msg_count / 2),
                   timeout_sec=240,
                   backoff_sec=1)
        # set recovery rate to small value but allow the controller to recover
        self._set_recovery_rate(10 * 1024)
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

        self.verify()

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(delete_topic=True)
    @parametrize(delete_topic=False)
    def test_decommissioning_finishes_after_manual_cancellation(
            self, delete_topic):

        self.start_redpanda()
        self._create_topics(replication_factors=[3])

        self.start_producer()
        self.start_consumer()

        to_decommission = random.choice(self.redpanda.nodes)
        node_id = self.redpanda.node_id(to_decommission)

        # throttle recovery
        self._set_recovery_rate(100)

        survivor_node = self._not_decommissioned_node(node_id)
        self.logger.info(f"decommissioning node: {node_id}", )
        self._decommission(node_id)

        # wait for some partitions to start moving
        wait_until(lambda: self._partitions_moving(node=survivor_node),
                   timeout_sec=15,
                   backoff_sec=1)
        # cancel all reconfigurations
        self.admin.cancel_all_reconfigurations()

        if delete_topic:
            self.client().delete_topic(self._topic)

        self._set_recovery_rate(2 << 30)
        self._wait_for_node_removed(node_id)

        if not delete_topic:
            self.verify()

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(node_is_alive=True)
    @parametrize(node_is_alive=False)
    def test_flipping_decommission_recommission(self, node_is_alive):

        self.start_redpanda()
        self._create_topics(replication_factors=[3])

        self.start_producer()
        self.start_consumer()

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

        self.verify()

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
    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_multiple_decommissions(self):
        self._extra_node_conf = {"empty_seed_starts_cluster": False}
        self.start_redpanda()
        total_partitions = self._create_topics()

        self.start_producer()
        self.start_consumer()

        def node_by_id(node_id):
            for n in self.redpanda.nodes:
                if self.redpanda.node_id(n) == node_id:
                    return n
            return None

        iteration_count = 2

        self.redpanda.set_expected_controller_records(
            10 * iteration_count * len(self.redpanda.nodes) * total_partitions)

        for i in range(0, iteration_count):
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

        self.verify()

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(new_bootstrap=True)
    @parametrize(new_bootstrap=False)
    def test_node_is_not_allowed_to_join_after_restart(self, new_bootstrap):
        self.start_redpanda(new_bootstrap=new_bootstrap)
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

        assert len(self.admin.get_brokers(node=self.redpanda.nodes[0])) == 4
        self.redpanda.stop_node(to_decommission)
        # clean node and restart it, it should join the cluster
        self.redpanda.clean_node(to_decommission, preserve_logs=True)
        self.redpanda.start_node(to_decommission,
                                 omit_seeds_on_idx_one=not new_bootstrap,
                                 auto_assign_node_id=new_bootstrap)

        assert len(self.admin.get_brokers(node=self.redpanda.nodes[0])) == 5

    @cluster(
        num_nodes=6,
        # A decom can look like a restart in terms of logs from peers dropping
        # connections with it
        log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommissioning_and_upgrade(self):
        self.installer = self.redpanda._installer
        # upgrade from previous to current version
        versions = [
            self.installer.highest_from_prior_feature_version(
                RedpandaInstaller.HEAD), RedpandaInstaller.HEAD
        ]
        to_decommission = None
        to_decommission_id = None
        for v in self.upgrade_through_versions(versions_in=versions,
                                               auto_assign_node_id=True):
            if v == versions[0]:
                self._create_topics()

                self.start_producer()
                self.start_consumer()
                # decommission node
                to_decommission = self.redpanda.nodes[-1]
                to_decommission_id = self.redpanda.node_id(to_decommission)
                self.logger.info(f"decommissioning node: {to_decommission_id}")
                self._decommission(to_decommission_id)

                self._wait_for_node_removed(to_decommission_id)
                self.redpanda.stop_node(to_decommission)
                self.redpanda.clean_node(to_decommission,
                                         preserve_logs=True,
                                         preserve_current_install=True)
                self.redpanda.start_node(to_decommission,
                                         auto_assign_node_id=True)
                # refresh node ids for rolling restarter
                self.redpanda.node_id(to_decommission, force_refresh=True)

            # check that the nodes reported from configuration status and
            # brokers endpoint is consistent

            self._check_state_consistent(to_decommission_id)

    @cluster(num_nodes=6)
    @matrix(auto_assign_node_id=[True, False])
    def test_recycle_all_nodes(self, auto_assign_node_id):

        # start redpanda on initial pool of nodes
        self.redpanda.start(nodes=self.redpanda.nodes,
                            auto_assign_node_id=auto_assign_node_id,
                            omit_seeds_on_idx_one=(not auto_assign_node_id))
        # configure controller not to take snapshot
        self.redpanda.set_cluster_config(
            {"controller_snapshot_max_age_sec": 20000})

        spec = TopicSpec(name=f"migration-test-workload",
                         partition_count=32,
                         replication_factor=3)

        self.client().create_topic(spec)
        self._topic = spec.name

        self.start_producer()
        self.start_consumer()

        # wait for some messages before executing actions
        self.producer.wait_for_acks(0.2 * self.msg_count,
                                    timeout_sec=60,
                                    backoff_sec=2)

        current_replicas = {
            self.redpanda.node_id(n)
            for n in self.redpanda.nodes
        }
        left_to_decommission = [
            self.redpanda.node_id(n) for n in self.redpanda.nodes
        ]

        next_id = 10
        admin = Admin(self.redpanda)

        def cluster_view_is_consistent():
            versions = set()
            for n in self.redpanda.started_nodes():
                node_id = self.redpanda.node_id(n)
                cv = admin.get_cluster_view(node=n)
                cv_nodes = {b['node_id'] for b in cv['brokers']}
                versions.add(cv['version'])
                self.logger.info(
                    f"cluster view from node {node_id} - {cv_nodes}, version: {cv['version']}"
                )
                if cv_nodes != current_replicas:
                    self.logger.warn(
                        f"inconsistent cluster view from {node_id}, expected: {current_replicas}, current: {cv_nodes}"
                    )
                    return False

                controller = admin.get_partition(ns="redpanda",
                                                 topic="controller",
                                                 id=0,
                                                 node=n)
                controller_replicas = {
                    r['node_id']
                    for r in controller['replicas']
                }
                self.logger.info(
                    f"controller partition replicas from node {node_id} - {controller_replicas}"
                )
                if controller_replicas != current_replicas:
                    self.logger.warn(
                        f"inconsistent controller replicas {node_id}, expected: {current_replicas}, current: {cv_nodes}"
                    )
                    return False

            return len(versions) == 1

        while len(left_to_decommission) > 0:
            decommissioned_id = left_to_decommission.pop()
            n = self.redpanda.get_node_by_id(decommissioned_id)
            assert n is not None
            self.logger.info(
                f"decommissioning node with id: {decommissioned_id} - {n.account.hostname}"
            )
            # force taking controller snapshot after first node is decommissioned
            if len(left_to_decommission) == (len(self.redpanda.nodes) - 1):
                self.redpanda.set_cluster_config(
                    {"controller_snapshot_max_age_sec": 10})
                self.redpanda.wait_for_controller_snapshot(
                    self.redpanda.controller(),
                    prev_start_offset=0,
                    timeout_sec=60)

            admin.decommission_broker(decommissioned_id)
            if len(left_to_decommission) == 5:
                self.redpanda.set_cluster_config(
                    {"controller_snapshot_max_age_sec": 20000})

            current_replicas.remove(decommissioned_id)
            # wait until node is decommissioned
            waiter = NodeDecommissionWaiter(
                self.redpanda,
                decommissioned_id,
                self.logger,
                60,
                decommissioned_node_ids=[decommissioned_id])
            waiter.wait_for_removal()

            # clean the node data
            self.redpanda.clean_node(n,
                                     preserve_logs=True,
                                     preserve_current_install=True)

            # start with new id
            self.logger.info(f"adding {n.account.hostname} with id {next_id}")

            self.redpanda.start_node(
                n,
                auto_assign_node_id=auto_assign_node_id,
                omit_seeds_on_idx_one=(not auto_assign_node_id),
                node_id_override=None if auto_assign_node_id else next_id)
            current_replicas.add(self.redpanda.node_id(n, force_refresh=True))
            next_id += 1

            wait_until(cluster_view_is_consistent, 60, 1,
                       "error waiting for consistent view of the cluster")

        self.verify()


class NodeDecommissionFailureReportingTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            num_brokers=4,
            extra_rp_conf={"partition_autobalancing_mode": "continuous"},
            **kwargs)

    def setUp(self):
        pass

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_allocation_failure_reporting(self):
        """Checks allocation failures for replicas on decommission nodes is reported correctly."""

        # Start all but one node
        all_but_one = self.redpanda.nodes[:-1]
        self.redpanda.set_seed_servers(all_but_one)
        self.redpanda.start(nodes=all_but_one,
                            auto_assign_node_id=True,
                            omit_seeds_on_idx_one=False)

        assert len(self.redpanda.started_nodes()) == 3, len(
            self.redpanda.started_nodes())

        spec = TopicSpec(partition_count=10, replication_factor=3)
        self.client().create_topic(spec)

        admin = self.redpanda._admin

        to_decommission = self.redpanda.nodes[-2]
        to_decommission_id = self.redpanda.node_id(to_decommission)

        partitions_json = admin.get_partitions(topic=spec.name,
                                               node=to_decommission)
        partitions = set([
            f"{p['ns']}/{p['topic']}/{p['partition_id']}"
            for p in partitions_json
        ])

        assert len(partitions) == 10, partitions

        def decommission(node_id, node=None):
            def decommissioned():
                try:
                    results = []
                    for n in all_but_one:
                        if self.redpanda.node_id(n) == node_id:
                            continue
                        brokers = admin.get_brokers(node=n)
                        for b in brokers:
                            if b['node_id'] == node_id:
                                results.append(
                                    b['membership_status'] == 'draining')
                    if all(results):
                        return True
                    admin.decommission_broker(node_id, node=node)
                except requests.exceptions.RetryError:
                    return False
                except requests.exceptions.ConnectionError:
                    return False
                except requests.exceptions.HTTPError:
                    return False

            wait_until(decommissioned, 30, 1)

        self.logger.info(f"decommissioning node: {to_decommission_id}")
        decommission(to_decommission_id)

        def wait_for_allocation_failures(failed_partitions):
            def wait(node):
                status = admin.get_decommission_status(to_decommission_id,
                                                       node=node)
                if "allocation_failures" not in status.keys():
                    return False
                alloc_failures = set(status["allocation_failures"])
                self.logger.debug(
                    f"alloc failures: {alloc_failures}, node: {node}")
                return failed_partitions == alloc_failures

            def check_all_nodes():
                return all(wait(n) for n in self.redpanda.started_nodes())

            self.logger.debug(
                f"Waiting for allocation failures: {failed_partitions}")
            wait_until(
                check_all_nodes,
                timeout_sec=60,
                backoff_sec=1,
                err_msg=
                f"Timed out waiting for all nodes to report allocation failures"
            )

        wait_for_allocation_failures(failed_partitions=partitions)

        # Start the last node, unblocks decommission
        self.redpanda.start(nodes=[self.redpanda.nodes[-1]],
                            auto_assign_node_id=True,
                            omit_seeds_on_idx_one=False)
        waiter = NodeDecommissionWaiter(self.redpanda,
                                        to_decommission_id,
                                        self.logger,
                                        progress_timeout=60)
        waiter.wait_for_removal()


class NodeDecommissionSpaceManagementTest(RedpandaTest):
    segment_upload_interval_sec = 5
    manifest_upload_interval_sec = 3
    retention_local_trim_interval_ms = 5_000

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context, num_brokers=4, *args, **kwargs)

    def setUp(self):
        # defer redpanda startup to the test
        pass

    def _kafka_usage(self):
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(self.redpanda.nodes)) as executor:
            return list(
                executor.map(
                    lambda n: self.redpanda.data_dir_usage("kafka", n),
                    self.redpanda.nodes))

    @skip_debug_mode
    @cluster(num_nodes=5)
    @matrix(single_partition=[True, False])
    def test_decommission(self, single_partition):
        log_segment_size = 1024 * 1024

        segments_per_partition = 50
        segments_per_partition_trimmed = 20
        if single_partition:
            # decommission will get stuck with the following params
            # if we don't take reclaimable partition sizes into account:
            partition_count = 4
            replication_factor = 1
        else:
            partition_count = 16
            replication_factor = 3

        target_size = log_segment_size * (
            segments_per_partition * partition_count * replication_factor //
            len(self.redpanda.nodes))

        # write a lot more data into the system than the target size to make
        # sure that we are definitely exercising target size enforcement.
        data_size = 2 * target_size * len(
            self.redpanda.nodes) // replication_factor

        msg_size = 16384
        msg_count = data_size // msg_size
        topic_name = "test_topic"

        # configure and start redpanda
        extra_rp_conf = {
            'cloud_storage_segment_max_upload_interval_sec':
            self.segment_upload_interval_sec,
            'cloud_storage_manifest_max_upload_interval_sec':
            self.manifest_upload_interval_sec,
            'retention_local_trim_interval':
            self.retention_local_trim_interval_ms,
            'retention_local_trim_overage_coeff':
            1.0,
            'retention_local_target_capacity_bytes':
            target_size,
            'retention_local_strict':
            False,
            'disk_reservation_percent':
            0,
            'retention_local_target_capacity_percent':
            100,
            'retention_local_target_bytes_default':
            segments_per_partition_trimmed * log_segment_size,
            # small fallocation step because the balancer adds it to the partition size
            'segment_fallocation_step':
            4096,
        }

        si_settings = SISettings(test_context=self.test_context,
                                 log_segment_size=log_segment_size,
                                 retention_local_strict=False)
        self.redpanda.set_extra_rp_conf(extra_rp_conf)
        self.redpanda.set_si_settings(si_settings)
        self.redpanda.start()

        # Sanity check test parameters against the nodes we are running on
        disk_space_required = data_size
        assert self.redpanda.get_node_disk_free(
        ) >= disk_space_required, f"Need at least {disk_space_required} bytes space"

        # create the target topic
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic_name,
                         partitions=partition_count,
                         replicas=replication_factor)

        # setup and start the background producer
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic_name,
                                       msg_size=msg_size,
                                       msg_count=msg_count,
                                       batch_max_bytes=msg_size * 8)
        producer.start()
        produce_start_time = time.time()

        # helper: bytes to MBs / human readable
        def hmb(bs):
            convert = lambda b: round(b / (1024 * 1024), 1)
            if isinstance(bs, int):
                return convert(bs)
            return [convert(b) for b in bs]

        producer.wait(timeout_sec=300)
        produce_duration = time.time() - produce_start_time
        self.logger.info(
            f"Produced {hmb([data_size])} in {produce_duration} seconds")

        totals = self._kafka_usage()
        self.logger.info(f"totals: {hmb(totals)}")
        to_decommission = self.redpanda.nodes[3]
        to_decommission_id = self.redpanda.node_id(to_decommission)
        Admin(self.redpanda).decommission_broker(to_decommission_id)
        waiter = NodeDecommissionWaiter(self.redpanda,
                                        to_decommission_id,
                                        self.logger,
                                        progress_timeout=60)
        waiter.wait_for_removal()
        self.redpanda.stop_node(to_decommission)
