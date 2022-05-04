import dataclasses
import random
import time
from threading import Thread, Event
from typing import Optional, Union

from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.failure_injector import FailureSpec, FailureInjector
from rptest.services.redpanda import RedpandaService


@dataclasses.dataclass
class ActionConfig:
    # lead time the action injector waits for the cluster
    # to become healthy before starting disruptive actions
    cluster_start_lead_time_sec: float
    min_time_between_actions_sec: float
    max_time_between_actions_sec: float

    # if set, the action will not be applied after this count of nodes has been affected
    # subsequent calls to action will not do anything for the lifetime of the action injector thread.
    max_affected_nodes: Optional[int] = None

    def time_between_actions(self) -> float:
        """
        The action injector thread sleeps for this time interval between calls
        to DisruptiveAction
        """
        return random.uniform(self.min_time_between_actions_sec,
                              self.max_time_between_actions_sec)


class DisruptiveAction:
    """
    Defines an action taken on a node or on the cluster as a whole which causes a disruption.

    The action could be a process failure, leadership transfer, topic modification etc.

    The action can be reversible, it also stores the set of affected nodes and the last node
    the action was applied on.
    """
    def __init__(self, redpanda: RedpandaService, config: ActionConfig,
                 admin: Admin):
        self.admin = admin
        self.config = config
        self.redpanda = redpanda
        self.affected_nodes = set()
        self.is_reversible = False
        self.last_affected_node = None

    def max_affected_nodes_reached(self) -> bool:
        """
        Checks if the number of affected nodes so far equals the maximum number of nodes
        this action is allowed to affect. If so all future calls to action will be no-op.
        """
        raise NotImplementedError

    def target_node_or_broker(self) -> Optional[Union[ClusterNode, str]]:
        """
        Randomly selects the next node or broker to apply the action on. A set of affected
        nodes is maintained so that we do not apply the action on nodes which were
        already targeted in previous invocations.
        """
        available = set(self.redpanda.nodes) - self.affected_nodes
        if available:
            selected = random.choice(list(available))
            names = {n.account.hostname for n in available}
            self.redpanda.logger.info(
                f'selected {selected.account.hostname} of {names} for operation'
            )
            return selected
        return None

    def do_action(self) -> ClusterNode:
        """
        Applies the disruptive action, returns node or entity the action was applied on
        """
        raise NotImplementedError

    def action(self) -> Optional[ClusterNode]:
        if not self.max_affected_nodes_reached():
            return self.do_action()
        return None

    def do_reverse_action(self) -> ClusterNode:
        """
        Reverses the last applied action if applicable.
        """
        raise NotImplementedError

    def reverse(self) -> Optional[ClusterNode]:
        if self.is_reversible and self.last_affected_node is not None:
            return self.do_reverse_action()
        return None


class NodeDecommission(DisruptiveAction):
    DRAIN_TIMEOUT_SEC = 60
    REMOVAL_TIMEOUT_SEC = 60
    START_TIMEOUT_SEC = 60

    DRAIN_BACKOFF_SEC = 2
    REMOVAL_BACKOFF_SEC = 2
    START_BACKOFF_SEC = 2

    def __init__(
        self,
        redpanda: RedpandaService,
        config: ActionConfig,
        admin: Admin,
    ):
        super().__init__(redpanda, config, admin)
        self.is_reversible = True
        self.next_id = max(
            (self.redpanda.idx(n) for n in self.redpanda.nodes)) + 1

    def target_node_or_broker(self) -> ClusterNode:
        """
        Overrides selection of random cluster nodes, here we select a broker
        which is active to be able to decommission it. If we simply choose a
        random node, it may have been added to the cluster in the previous
        action reversal and may not yet be active.
        """
        brokers = self.admin.get_cluster_view(node=None)['brokers']
        active_brokers = [
            b['node_id'] for b in brokers if b['membership_status'] == 'active'
        ]
        selected = random.choice(active_brokers)
        self.redpanda.logger.debug(
            f'selected node idx {selected} from set {active_brokers}')
        return selected

    def max_affected_nodes_reached(self):
        return len(self.affected_nodes) >= self.config.max_affected_nodes

    def do_action(self) -> ClusterNode:
        idx = self.target_node_or_broker()
        node = self.redpanda.get_node(idx)

        self.redpanda.logger.info(
            f'decommissioning node {node.account.hostname} idx {idx}')
        # Make sure we fail if the admin api responds with error when decommissioning
        r = self.admin.decommission_broker(idx)
        r.raise_for_status()

        def is_draining():
            brokers = self.admin.get_cluster_view(node=None).get('brokers', {})
            for b in brokers:
                if b['node_id'] == idx and b['membership_status'] == 'draining':
                    return True
            return False

        wait_until(
            is_draining,
            timeout_sec=self.DRAIN_TIMEOUT_SEC,
            backoff_sec=self.DRAIN_BACKOFF_SEC,
            err_msg=f'Failed to decommission {node.account.hostname}',
        )

        def is_removed():
            brokers = self.admin.get_cluster_view(node=None).get('brokers', {})
            return idx not in {b['node_id'] for b in brokers}

        wait_until(
            is_removed,
            timeout_sec=self.REMOVAL_TIMEOUT_SEC,
            backoff_sec=self.REMOVAL_BACKOFF_SEC,
            err_msg=f'Failed to remove {node.account.hostname}',
        )

        self.redpanda.logger.info(
            f'removed {node.account.hostname} with broker id {idx}')
        self.last_affected_node = node
        return node

    def do_reverse_action(self) -> ClusterNode:
        revived = self.revive_node(self.last_affected_node)
        self.last_affected_node = None
        return revived

    def revive_node(self, node: ClusterNode) -> ClusterNode:
        new_id, self.next_id = self.next_id, self.next_id + 1
        self.redpanda.logger.info(
            f'reviving {node.account.hostname} with new broker id {new_id}')

        self.redpanda.stop_node(node)
        self.redpanda.logger.info(
            f'stopped {node.account.hostname} before reviving')
        self.redpanda.clean_node(node, preserve_logs=True)

        new_seed_servers = [{
            'address': n.account.hostname,
            'port': 33145,
        } for n in self.redpanda.nodes if n != node]

        self.redpanda.start_node(node,
                                 override_cfg_params={
                                     'node_id': new_id,
                                     'seed_servers': new_seed_servers,
                                 },
                                 timeout=300)

        kc = KafkaCat(self.redpanda)

        def has_replicas():
            md = kc.metadata()
            self.redpanda.logger.debug(
                f'adding {node.account.hostname}, metadata: {md}')
            for topic in md['topics']:
                for partition in topic['partitions']:
                    for replica in partition['replicas']:
                        if replica['id'] == new_id:
                            return True
            return False

        wait_until(
            has_replicas,
            timeout_sec=self.START_TIMEOUT_SEC,
            backoff_sec=self.START_BACKOFF_SEC,
            err_msg=
            f'Failed to find replicas on {node.account.hostname} after start')
        self.redpanda.logger.info(
            f'revived {node.account.hostname} with new id {new_id}')
        return node


class LeadershipTransfer(DisruptiveAction):
    def __init__(
        self,
        redpanda: RedpandaService,
        config: ActionConfig,
        admin: Admin,
        topic: TopicSpec,
    ):
        super().__init__(redpanda, config, admin)
        self.topic = topic
        self.partition_id = 0
        self.is_reversible = False

    def max_affected_nodes_reached(self):
        return False

    def target_node_or_broker(self) -> ClusterNode:
        """
        Returns a random node index which is not the current partition leader.
        """
        partition_info = self.admin.get_partitions(
            namespace='kafka',
            topic=self.topic.name,
            partition=self.partition_id,
        )

        current_leader = partition_info['leader_id']
        candidates = [
            n['node_id'] for n in partition_info['replicas']
            if n['node_id'] != current_leader
        ]
        self.redpanda.logger.debug(
            f'current partition leader id {current_leader}, picking next leader from ids {candidates}'
        )
        return random.choice(candidates)

    def do_action(self) -> ClusterNode:
        node_idx = self.target_node_or_broker()
        node = self.redpanda.get_node(node_idx)
        self.redpanda.logger.info(
            f'transferring leadership to node {node.account.hostname}, id {node_idx}'
        )
        self.admin.partition_transfer_leadership(
            'kafka',
            self.topic.name,
            self.partition_id,
            node_idx,
        )

        def partition_transferred():
            leader = self.admin.get_partition_leader(
                namespace='kafka',
                topic=self.topic.name,
                partition=self.partition_id,
            )
            self.redpanda.logger.debug(
                f'current leader is {leader}, waiting for {node_idx}')
            return leader == node_idx

        wait_until(
            partition_transferred,
            timeout_sec=30,
            backoff_sec=5,
            err_msg=
            f'Leadership transfer failed for {self.topic.name}/{self.partition_id}',
        )
        return node

    def do_reverse_action(self) -> ClusterNode:
        raise NotImplementedError('Leadership transfer is not reversible')


class ProcessKill(DisruptiveAction):
    PROCESS_START_WAIT_SEC = 20
    PROCESS_START_WAIT_BACKOFF = 2

    def __init__(self, redpanda: RedpandaService, config: ActionConfig,
                 admin: Admin):
        super(ProcessKill, self).__init__(redpanda, config, admin)
        self.failure_injector = FailureInjector(self.redpanda)
        self.is_reversible = True

    def max_affected_nodes_reached(self):
        return len(self.affected_nodes) >= self.config.max_affected_nodes

    def do_action(self):
        node = self.target_node_or_broker()
        if node:
            self.redpanda.logger.info(
                f'executing action on {node.account.hostname}')
            self.failure_injector.inject_failure(
                FailureSpec(FailureSpec.FAILURE_KILL, node))
            self.affected_nodes.add(node)
            self.last_affected_node = node

            # Update started_nodes so storage validations are run
            # on the correct set of nodes later.
            self.redpanda.remove_from_started_nodes(node)
            return node
        else:
            self.redpanda.logger.warn(f'no usable node')
            return None

    def do_reverse_action(self):
        self._start_rp(node=self.last_affected_node)
        self.affected_nodes.remove(self.last_affected_node)
        self.redpanda.add_to_started_nodes(self.last_affected_node)

        last_affected_node, self.last_affected_node = self.last_affected_node, None
        return last_affected_node

    def _start_rp(self, node):
        self.failure_injector._start(node)
        wait_until(
            lambda: self.redpanda.redpanda_pid(node),
            timeout_sec=self.PROCESS_START_WAIT_SEC,
            backoff_sec=self.PROCESS_START_WAIT_BACKOFF,
            err_msg=
            f'Failed to start redpanda process on {node.account.hostname}')


class ActionInjectorThread(Thread):
    def __init__(
        self,
        config: ActionConfig,
        redpanda: RedpandaService,
        disruptive_action: DisruptiveAction,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.disruptive_action = disruptive_action
        self.redpanda = redpanda
        self.config = config
        self._stop_requested = Event()
        self.action_triggered = False
        self.reverse_action_triggered = False

    def run(self):
        admin = Admin(self.redpanda)

        def all_nodes_started():
            statuses = [
                admin.ready(node).get("status") for node in self.redpanda.nodes
            ]
            return all(status == 'ready' for status in statuses)

        wait_until(all_nodes_started,
                   timeout_sec=self.config.cluster_start_lead_time_sec,
                   backoff_sec=2,
                   err_msg=f'Cluster not ready to begin actions')

        self.redpanda.logger.info('cluster is ready, starting action loop')

        while not self._stop_requested.is_set():
            if self.disruptive_action.action():
                self.action_triggered = True
            time.sleep(self.config.time_between_actions())
            if self.disruptive_action.reverse():
                self.reverse_action_triggered = True

    def stop(self):
        self._stop_requested.set()


class ActionCtx:
    def __init__(self, config: ActionConfig, redpanda: RedpandaService,
                 disruptive_action: DisruptiveAction):
        self.redpanda = redpanda
        self.config = config
        if config.max_affected_nodes is None:
            config.max_affected_nodes = len(redpanda.nodes) // 2
        self.disruptive_action = disruptive_action
        self.thread = ActionInjectorThread(config, redpanda, disruptive_action)

    def __enter__(self):
        self.redpanda.logger.info(f'entering random failure ctx')
        self.thread.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.redpanda.logger.info(f'leaving random failure ctx')
        self.thread.stop()
        self.thread.join()

    def assert_actions_triggered(self):
        """
        Helper to allow tests to assert that the forward and reverse
        actions were triggered by the thread
        """
        assert self.thread.action_triggered
        if self.disruptive_action.is_reversible:
            assert self.thread.reverse_action_triggered


def create_context_with_defaults(redpanda: RedpandaService,
                                 op_type,
                                 config: ActionConfig = None,
                                 *args,
                                 **kwargs) -> ActionCtx:
    admin = Admin(redpanda)
    config = config or ActionConfig(
        cluster_start_lead_time_sec=20,
        min_time_between_actions_sec=10,
        max_time_between_actions_sec=30,
    )
    return ActionCtx(config, redpanda,
                     op_type(redpanda, config, admin, *args, **kwargs))


def random_process_kills(redpanda: RedpandaService,
                         config: ActionConfig = None) -> ActionCtx:
    return create_context_with_defaults(redpanda, ProcessKill, config=config)


def random_decommissions(redpanda: RedpandaService,
                         config: ActionConfig = None) -> ActionCtx:
    return create_context_with_defaults(redpanda,
                                        NodeDecommission,
                                        config=config)


def random_leadership_transfers(redpanda: RedpandaService,
                                topic: TopicSpec,
                                config: ActionConfig = None) -> ActionCtx:
    admin = Admin(redpanda)
    return ActionCtx(config, redpanda,
                     LeadershipTransfer(
                         redpanda,
                         config,
                         admin,
                         topic,
                     ))
