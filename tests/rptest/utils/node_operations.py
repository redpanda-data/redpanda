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
from rptest.services.redpanda_installer import VERSION_RE, int_tuple


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


class NodeDecommissionWaiter():
    def __init__(self, redpanda, node_id, logger, progress_timeout=30):
        self.redpanda = redpanda
        self.node_id = node_id
        self.logger = logger
        self.admin = Admin(self.redpanda)
        self.last_update = None
        self.last_replicas_left = None
        self.last_partitions_bytes_left = None
        self.progress_timeout = progress_timeout

    def _nodes_with_decommission_progress_api(self):
        def has_decommission_progress_api(node):
            v = int_tuple(
                VERSION_RE.findall(self.redpanda.get_version(node))[0])
            # decommission progress api is available since v22.3.12
            return v[0] >= 23 or (v[0] == 22 and v[1] == 3 and v[2] >= 12)

        return [
            n for n in self.redpanda.started_nodes()
            if has_decommission_progress_api(n)
        ]

    def _dump_partition_move_available_bandwidth(self):
        def get_metric(self, node):
            try:
                metrics = list(self.redpanda.metrics(node))
                family = filter(
                    lambda fam: fam.name ==
                    "vectorized_raft_recovery_partition_movement_available_bandwidth",
                    metrics)
                shard_to_bandwidth = [{
                    m.labels['shard']: m.value
                } for m in next(family).samples]
                return shard_to_bandwidth
            except:
                self.logger.debug(f"error querying metrics for {node}",
                                  exc_info=True)
                return None

        for n in self.redpanda.started_nodes():
            self.logger.debug(
                f"partition move available bandwidth: node_id: {self.redpanda.node_id(n)} ==> {get_metric(self, n)}"
            )

    def _not_decommissioned_node(self):
        return random.choice([
            n for n in self._nodes_with_decommission_progress_api()
            if self.redpanda.node_id(n) != self.node_id
        ])

    def _made_progress(self):
        return (time.time() - self.last_update) < self.progress_timeout

    def _node_removed(self):
        brokers = []
        node_to_query = self._not_decommissioned_node()
        try:
            brokers = self.admin.get_brokers(node=node_to_query)
        except:
            # Failure injection is not coordinated, some nodes may
            # not be reachable, ignore and retry.
            self.logger.debug(f"Unable to query {node_to_query}",
                              exc_info=True)
            return False
        for b in brokers:
            if b['node_id'] == self.node_id:
                return False
        return True

    def _collect_partitions_bytes_left(self, status):
        if 'partitions' not in status:
            return None

        total_left = 0

        for p in status['partitions']:
            total_left += p['bytes_left_to_move']

        return total_left

    def wait_for_removal(self):
        self.last_update = time.time()
        # wait for removal only if progress was reported
        while self._made_progress():
            try:
                decommission_status = self.admin.get_decommission_status(
                    self.node_id, self._not_decommissioned_node())
            except requests.exceptions.HTTPError as e:
                self.logger.info(
                    f"unable to get decommission status, HTTP error",
                    exc_info=True)
                time.sleep(1)
                continue
            except Exception as e:
                self.logger.warn(f"unable to get decommission status",
                                 exc_info=True)
                time.sleep(1)
                continue

            self.logger.debug(
                f"Node {self.node_id} decommission status: {decommission_status}"
            )

            replicas_left = decommission_status['replicas_left']
            partitions_bytes_left = self._collect_partitions_bytes_left(
                decommission_status)
            self.logger.info(
                f"total replicas left to move: (current: {replicas_left}, previous: {self.last_replicas_left}), "
                f"partition bytes left to move: (current: {partitions_bytes_left}, previous: {self.last_partitions_bytes_left})"
            )
            # check if node bytes left changed
            if self.last_replicas_left is None or replicas_left < self.last_replicas_left:
                self.last_update = time.time()
            if partitions_bytes_left is not None and partitions_bytes_left > 0:
                # check if currently moving partitions bytes left changed
                if self.last_partitions_bytes_left is None or partitions_bytes_left < self.last_partitions_bytes_left:
                    self.last_update = time.time()

            self.last_replicas_left = replicas_left
            self.last_partitions_bytes_left = partitions_bytes_left

            if decommission_status["finished"] == True:
                break
            self._dump_partition_move_available_bandwidth()
            time.sleep(1)

        assert self._made_progress(
        ), f"Node {self.node_id} decommissioning stopped making progress"

        wait_until(
            self._node_removed, timeout_sec=60, backoff_sec=1
        ), f"Node {self.node_id} still exists in the cluster but decommission operation status reported it is finished"


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
        # For quick decommissions with very little to no data movement, `draining` and removal
        # can be quick succession, so we check for either.
        wait_until(lambda: self.has_status(node_id, "draining") or self.
                   node_removed(node_id),
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

    def is_node_removed(self, node_to_query, node_id):
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

    def node_removed(self, node_id):
        node_removed_cnt = 0
        for n in self.redpanda.started_nodes():
            if self.is_node_removed(n, node_id):
                node_removed_cnt += 1

        node_count = len(self.redpanda.started_nodes())
        majority = int(node_count / 2) + 1
        self.logger.debug(
            f"node {node_id} removed on {node_removed_cnt} nodes, majority: {majority}"
        )
        return node_removed_cnt >= majority

    # just confirm if node removal was propagated to the the majority of nodes
    def wait_for_removed(self, node_id: int):
        self.logger.info(
            f"executor - waiting for node {node_id} to be removed")

        # wait for node to be removed of decommissioning to stop making progress
        waiter = NodeDecommissionWaiter(self.redpanda,
                                        node_id=node_id,
                                        logger=self.logger,
                                        progress_timeout=60)

        waiter.wait_for_removal()
        wait_until(lambda: self.node_removed(node_id),
                   timeout_sec=self.timeout,
                   backoff_sec=1)

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
            # If node is empty decommission will finish immediately and we won't
            # be able to recommission it
            self.wait_for_rebalanced(operation.node)
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
        self.error = None

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
        assert self.error is None, f"failure injector error, most likely node failed to stop: {self.error}"

    def _worker(self):
        with FailureInjector(self.redpanda) as f_injector:
            while not self.stop_ev.is_set():

                f_type = random.choice(FailureSpec.FAILURE_TYPES)
                # failure injector uses only started nodes, this way
                # we guarantee that failures will not interfere with
                # nodes start checks

                # use provided lock to prevent interfering with nodes being stopped/started
                with self.lock:
                    try:
                        node = random.choice(self.redpanda.started_nodes())

                        length = 0

                        if f_type == FailureSpec.FAILURE_SUSPEND:
                            length = random.randint(
                                1, self.max_suspend_duration_seconds)

                        f_injector.inject_failure(
                            FailureSpec(node=node, type=f_type, length=length))
                    except Exception as e:
                        self.logger.warn(f"error injecting failure - {e}")
                        self.error = e

                delay = random.randint(self.min_inter_failure_time,
                                       self.max_inter_failure_time)
                self.logger.info(
                    f"waiting {delay} seconds before next failure")
                time.sleep(delay)
