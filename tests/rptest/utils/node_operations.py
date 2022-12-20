# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
from enum import Enum
import random
import threading
import time
import requests

from ducktape.utils.util import wait_until
from rptest.clients.kafka_cat import KafkaCat
from rptest.services.admin import Admin
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.redpanda import RedpandaService


class OperationType(Enum):
    DECOMMISSION = "decommission"
    ADD = "add"
    DE_RECOMMISSION = "decommission/recommission"


class NodeOperation:
    def __init__(self, type: OperationType, node: int, wait_for_finish: bool):
        self.type = type
        self.node = node
        self.wait_for_finish = wait_for_finish


def generate_random_workload(available_nodes):
    op_types = [
        OperationType.ADD,
        OperationType.DE_RECOMMISSION,
        OperationType.DECOMMISSION,
    ]

    # current state
    active_nodes = list(available_nodes)
    decommissioned_nodes = []

    def remove(idx):
        active_nodes.remove(idx)
        decommissioned_nodes.append(idx)

    def add(idx):
        active_nodes.append(idx)
        decommissioned_nodes.remove(idx)

    while True:
        if len(active_nodes) <= 3:
            # 3 nodes in the cluster, we need to add one
            idx = random.choice(decommissioned_nodes)
            add(idx)
            yield NodeOperation(OperationType.ADD, idx,
                                random.choice([True, False]))
        elif len(decommissioned_nodes) == 0:
            idx = random.choice(active_nodes)
            remove(idx)
            yield NodeOperation(OperationType.DECOMMISSION, idx,
                                random.choice([True, False]))
        else:
            op = random.choice(op_types)
            if op == OperationType.DECOMMISSION or op == OperationType.DE_RECOMMISSION:
                idx = random.choice(active_nodes)
                if op == OperationType.DECOMMISSION:
                    remove(idx)
                yield NodeOperation(op, idx, random.choice([True, False]))
            elif op == OperationType.ADD:
                idx = random.choice(decommissioned_nodes)
                add(idx)
                yield NodeOperation(op, idx, random.choice([True, False]))


class NodeOpsExecutor():
    def __init__(self, redpanda: RedpandaService, logger,
                 lock: threading.Lock):
        self.redpanda = redpanda
        self.logger = logger
        self.timeout = 360
        self.lock = lock

    def node_id(self, idx):
        return self.redpanda.node_id(self.redpanda.get_node(idx),
                                     force_refresh=True)

    def decommission(self, idx: int):
        node_id = self.node_id(idx)
        self.logger.info(
            f"executor - decommissioning node {node_id} (idx: {idx})")
        admin = Admin(self.redpanda)

        def decommissioned():
            try:
                # if broker is already draining, it is success
                brokers = admin.get_brokers()
                for b in brokers:
                    if b['node_id'] == node_id and b[
                            'membership_status'] == 'draining':
                        return True

                r = admin.decommission_broker(id=node_id)
                return r.status_code == 200
            except requests.exceptions.RetryError:
                return False
            except requests.exceptions.ConnectionError:
                return False
            except requests.exceptions.HTTPError:
                return False

        wait_until(decommissioned, timeout_sec=self.timeout, backoff_sec=1)
        wait_until(lambda: self.has_status(node_id, "draining"),
                   timeout_sec=self.timeout,
                   backoff_sec=1)

    def get_statuses(self, node_to_query=None):
        admin = Admin(self.redpanda)
        brokers = admin.get_brokers(node=node_to_query)

        return [{
            "id": b['node_id'],
            "status": b['membership_status']
        } for b in brokers]

    def has_status(self, node_id, status):

        try:
            brokers = self.get_statuses()

            self.logger.info(f"broker statuses: {brokers}")
            for b in brokers:
                if b['id'] == node_id and b['status'] == status:
                    return True

            return False
        except Exception as e:
            self.logger.info(f"error querying broker statuses - {e}")
            return False

    def wait_for_removed(self, node_id: int):
        self.logger.info(
            f"executor - waiting for node {node_id} to be removed")

        def is_node_removed(node_to_query, node_id):
            try:
                brokers = self.get_statuses(node_to_query)
                self.logger.info(
                    f"broker statuses from {self.redpanda.node_id(node_to_query)}: {brokers}"
                )
                ids = map(lambda broker: broker['id'], brokers)
                return not node_id in ids
            except Exception as e:
                self.logger.info(f"error querying broker statuses - {e}")
                return False

        def node_removed():
            node_removed_cnt = 0
            for n in self.redpanda.started_nodes():
                if is_node_removed(n, node_id):
                    node_removed_cnt += 1

            node_count = len(self.redpanda.started_nodes())
            majority = int(node_count / 2) + 1
            self.logger.debug(
                f"node {node_id} removed on {node_removed_cnt} nodes, majority: {majority}"
            )
            return node_removed_cnt >= majority

        wait_until(node_removed, timeout_sec=self.timeout, backoff_sec=1)

    def stop_node(self, idx):
        node = self.redpanda.get_node(idx)
        # remove from started nodes before actually stopping redpanda process
        # to prevent failures from being injected after this point
        with self.lock:
            self.redpanda.remove_from_started_nodes(node)
            self.redpanda.stop_node(node)
        self.redpanda.clean_node(node,
                                 preserve_logs=True,
                                 preserve_current_install=True)
        self.redpanda.set_seed_servers(self.redpanda.started_nodes())

    def recommission(self, idx: int):
        node_id = self.node_id(idx)
        self.logger.info(f"executor - recommissioning {node_id} (idx: {idx})")

        admin = Admin(self.redpanda)

        def recommissioned():
            try:
                statuses = []
                for n in self.redpanda.started_nodes():
                    brokers = admin.get_brokers(node=n)

                    for b in brokers:
                        if b['node_id'] == node_id:
                            statuses.append(b['membership_status'] == 'active')
                if all(statuses):
                    return True

                r = admin.recommission_broker(id=node_id)
            except requests.exceptions.RetryError:
                return False
            except requests.exceptions.ConnectionError:
                return False
            except requests.exceptions.HTTPError:
                return False

            return False

        wait_until(recommissioned, timeout_sec=self.timeout, backoff_sec=1)
        wait_until(lambda: self.has_status(node_id, "active"),
                   timeout_sec=self.timeout,
                   backoff_sec=1)

    def add(self, idx: int):
        self.logger.info(f"executor - adding node (idx: {idx})")

        node = self.redpanda.get_node(idx)

        self.redpanda.start_node(node,
                                 timeout=self.timeout,
                                 auto_assign_node_id=True,
                                 omit_seeds_on_idx_one=False)

        self.logger.info(
            f"added node: {idx} with new node id: {self.node_id(idx)}")

    def _replicas_per_node(self):
        kafkacat = KafkaCat(self.redpanda)
        node_replicas = defaultdict(int)
        md = kafkacat.metadata()
        for topic in md['topics']:
            for p in topic['partitions']:
                for r in p['replicas']:
                    id = r['id']
                    node_replicas[id] += 1

        return node_replicas

    def wait_for_rebalanced(self, idx: int):

        node_id = self.node_id(idx)
        self.logger.info(
            f"executor - waiting for  node {node_id} (idx: {idx}) to be rebalanced"
        )

        def has_new_replicas():
            per_node = self._replicas_per_node()
            self.logger.info(
                f"waiting for node {node_id} replicas, current replicas per node: {per_node}"
            )
            return node_id in per_node and per_node[node_id] > 5

        wait_until(has_new_replicas, timeout_sec=self.timeout, backoff_sec=1)

    def execute_operation(self, operation: NodeOperation):
        start = time.perf_counter()
        self.logger.info(
            f"executor - node {operation.node} operation: {operation.type}, wait for finish: {operation.wait_for_finish}"
        )
        if operation.type == OperationType.ADD:
            self.add(operation.node)
            if operation.wait_for_finish:
                self.wait_for_rebalanced(operation.node)
        elif operation.type == OperationType.DE_RECOMMISSION:
            self.decommission(operation.node)

            self.recommission(operation.node)
        elif operation.type == OperationType.DECOMMISSION:
            # query node_id before removing a node
            node_id = self.node_id(operation.node)
            self.decommission(operation.node)
            if operation.wait_for_finish:
                self.wait_for_removed(node_id)
                self.stop_node(operation.node)
            else:
                self.stop_node(operation.node)
                self.wait_for_removed(node_id)
        self.logger.info(
            f"executor - node {operation.node} operation: {operation.type}, wait for finish: {operation.wait_for_finish}, took {time.perf_counter() - start} seconds"
        )


class FailureInjectorBackgroundThread():
    def __init__(self, redpanda: RedpandaService, logger,
                 lock: threading.Lock):
        self.stop_ev = threading.Event()
        self.redpanda = redpanda
        self.thread = None
        self.logger = logger
        self.max_suspend_duration_seconds = 10
        self.min_inter_failure_time = 30
        self.max_inter_failure_time = 60
        self.node_start_stop_mutexes = {}
        self.lock = lock

    def start(self):
        self.logger.info(
            f"Starting failure injector thread with: (max suspend duration {self.max_suspend_duration_seconds},"
            f"min inter failure time: {self.min_inter_failure_time}, max inter failure time: {self.max_inter_failure_time})"
        )
        self.thread = threading.Thread(target=lambda: self._worker(), args=())
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.logger.info(f"Stopping failure injector thread")
        self.stop_ev.set()
        self.thread.join()

    def _worker(self):
        with FailureInjector(self.redpanda) as f_injector:
            while not self.stop_ev.is_set():

                f_type = random.choice(FailureSpec.FAILURE_TYPES)
                # failure injector uses only started nodes, this way
                # we guarantee that failures will not interfere with
                # nodes start checks

                # use provided lock to prevent interfering with nodes being stopped/started
                with self.lock:
                    node = random.choice(self.redpanda.started_nodes())

                    length = 0

                    if f_type == FailureSpec.FAILURE_SUSPEND:
                        length = random.randint(
                            1, self.max_suspend_duration_seconds)

                    f_injector.inject_failure(
                        FailureSpec(node=node, type=f_type, length=length))

                delay = random.randint(self.min_inter_failure_time,
                                       self.max_inter_failure_time)
                self.logger.info(
                    f"waiting {delay} seconds before next failure")
                time.sleep(delay)
