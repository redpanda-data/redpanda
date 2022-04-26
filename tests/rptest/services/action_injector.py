import dataclasses
import random
import time
from threading import Thread, Event
from typing import Optional, List

from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.failure_injector import FailureSpec, FailureInjector
from rptest.services.redpanda import RedpandaService


@dataclasses.dataclass
class ActionConfig:
    cluster_start_lead_time_sec: float
    min_time_between_actions_sec: float
    max_time_between_actions_sec: float
    reverse_action_on_next_cycle: bool = False
    max_affected_nodes: Optional[int] = None

    def random_time_in_range(self) -> float:
        return random.uniform(self.min_time_between_actions_sec,
                              self.max_time_between_actions_sec)


class RandomNodeOp:
    def __init__(self, redpanda: RedpandaService, config: ActionConfig,
                 admin: Admin):
        self.admin = admin
        self.config = config
        self.redpanda = redpanda
        self.nodes_affected = set()
        self.is_reversible = False
        self.last_action_on_node = None

    def limit_reached(self) -> bool:
        raise NotImplementedError

    def target_node(self) -> ClusterNode:
        available = set(self.redpanda.nodes) - self.nodes_affected
        if available:
            selected = random.choice(list(available))
            names = {n.account.hostname for n in available}
            self.redpanda.logger.info(
                f'selected {selected.account.hostname} of {names} for operation'
            )
            return selected

    def action(self) -> ClusterNode:
        raise NotImplementedError

    def reverse(self, node: ClusterNode) -> ClusterNode:
        raise NotImplementedError

    def __call__(self, reverse=False, *args, **kwargs) -> ClusterNode:
        if reverse:
            if self.is_reversible and self.last_action_on_node is not None:
                return self.reverse(node=self.last_action_on_node)
            else:
                self.redpanda.logger.warn(
                    f'Ignoring reverse call with nothing to reverse')
        elif not self.limit_reached():
            return self.action()


class RandomNodeDecommission(RandomNodeOp):
    def limit_reached(self):
        return len(self.nodes_affected) >= self.config.max_affected_nodes

    def action(self) -> ClusterNode:
        brokers = self.admin.get_brokers()
        node_to_decommission = random.choice(brokers)

        broker_id = node_to_decommission['node_id']
        self.redpanda.logger.warn(f'going to decom broker id {broker_id}')
        self.admin.decommission_broker(id=node_to_decommission['node_id'])

        def done():
            return broker_id not in {
                b['node_id']
                for b in self.admin.get_brokers()
            }

        wait_until(done,
                   timeout_sec=120,
                   backoff_sec=2,
                   err_msg=f'Failed to decommission broker id {broker_id}')
        self.nodes_affected.add(broker_id)
        self.last_action_on_node = broker_id
        return self.last_action_on_node

    def reverse(self, node) -> ClusterNode:
        self.admin.recommission_broker(self.last_action_on_node)
        self.nodes_affected.remove(self.last_action_on_node)
        last_action_on_node = self.last_action_on_node
        self.last_action_on_node = None
        return last_action_on_node


class RandomLeadershipTransfer(RandomNodeOp):
    def __init__(
        self,
        redpanda: RedpandaService,
        config: ActionConfig,
        admin: Admin,
        topics: List[TopicSpec],
    ):
        super().__init__(redpanda, config, admin)
        self.topics = topics
        self.is_reversible = False

    def limit_reached(self):
        return False

    def action(self):
        for topic in self.topics:
            for partition in range(topic.partition_count):
                old_leader = self.admin.get_partition_leader(
                    namespace='kafka', topic=topic, partition=partition)
                self.admin.transfer_leadership_to(namespace='kafka',
                                                  topic=topic,
                                                  partition=partition,
                                                  target=None)

                def leader_is_changed():
                    new_leader = self.admin.get_partition_leader(
                        namespace='kafka', topic=topic, partition=partition)
                    return new_leader != -1 and new_leader != old_leader

                wait_until(leader_is_changed,
                           timeout_sec=30,
                           backoff_sec=2,
                           err_msg='Leadership transfer failed')

    def reverse(self, node: ClusterNode):
        raise NotImplementedError('Leadership transfer not reversible')


class RandomNodeProcessFailure(RandomNodeOp):
    def __init__(self, redpanda: RedpandaService, config: ActionConfig,
                 admin: Admin):
        super(RandomNodeProcessFailure, self).__init__(redpanda, config, admin)
        self.failure_injector = FailureInjector(self.redpanda)
        self.is_reversible = True

    def limit_reached(self):
        return len(self.nodes_affected) >= self.config.max_affected_nodes

    def action(self):
        node = self.target_node()
        if node:
            self.redpanda.logger.info(
                f'executing action on {node.account.hostname}')
            self.failure_injector.inject_failure(
                FailureSpec(FailureSpec.FAILURE_KILL, node))
            self.nodes_affected.add(node)
            self.last_action_on_node = node

            # Update started_nodes so validations are run on the correct
            # set of nodes later.
            try:
                self.redpanda.started_nodes().remove(node)
            except ValueError:
                self.redpanda.logger.warn(
                    f'failed to remove {node.account.hostname} from rp node list'
                )
            finally:
                return node
        else:
            self.redpanda.logger.warn(f'no usable node')

    def reverse(self, node: ClusterNode):
        self.failure_injector._start(self.last_action_on_node)
        self.nodes_affected.remove(self.last_action_on_node)
        self.redpanda.started_nodes().append(self.last_action_on_node)
        last_action_on_node = self.last_action_on_node
        self.last_action_on_node = None
        return last_action_on_node


@dataclasses.dataclass
class ActionLogEntry:
    node: ClusterNode
    is_reverse_action: bool

    def __repr__(self) -> str:
        return f'Node: {self.node.account.hostname}, reverse? {self.is_reverse_action}'


class ActionInjectorThread(Thread):
    def __init__(
        self,
        config: ActionConfig,
        redpanda: RedpandaService,
        random_op: RandomNodeOp,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.random_op = random_op
        self.redpanda = redpanda
        self.config = config
        self._stop_requested = Event()
        self.action_log = []

    def run(self):
        wait_until(lambda: self.redpanda.healthy(),
                   timeout_sec=self.config.cluster_start_lead_time_sec,
                   backoff_sec=2,
                   err_msg=f'Cluster not ready to begin actions')

        while not self._stop_requested.is_set():
            if self.config.reverse_action_on_next_cycle:
                result = self.random_op(reverse=True)
                if result:
                    self.action_log.append(
                        ActionLogEntry(result, is_reverse_action=True))
            result = self.random_op()
            if result:
                self.action_log.append(
                    ActionLogEntry(result, is_reverse_action=False))
            time.sleep(self.config.random_time_in_range())

    def stop(self):
        self._stop_requested.set()


class ActionCtx:
    def __init__(self, config: ActionConfig, redpanda: RedpandaService,
                 random_op: RandomNodeOp):
        self.redpanda = redpanda
        self.config = config
        if config.max_affected_nodes is None:
            config.max_affected_nodes = len(redpanda.nodes) // 2
        self.thread = ActionInjectorThread(config, redpanda, random_op)

    def __enter__(self):
        self.redpanda.logger.info(f'entering random failure ctx')
        self.thread.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.redpanda.logger.info(f'leaving random failure ctx')
        self.thread.stop()
        self.thread.join()

    def action_log(self):
        return self.thread.action_log


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
        reverse_action_on_next_cycle=True,
    )
    return ActionCtx(config, redpanda,
                     op_type(redpanda, config, admin, *args, **kwargs))


def random_process_kills(redpanda: RedpandaService,
                         config: ActionConfig = None) -> ActionCtx:
    return create_context_with_defaults(redpanda,
                                        RandomNodeProcessFailure,
                                        config=config)


def random_decommissions(redpanda: RedpandaService,
                         config: ActionConfig = None) -> ActionCtx:
    return create_context_with_defaults(redpanda,
                                        RandomNodeDecommission,
                                        config=config)


def random_leadership_transfers(redpanda: RedpandaService,
                                topics,
                                config: ActionConfig = None) -> ActionCtx:
    return create_context_with_defaults(redpanda,
                                        RandomLeadershipTransfer,
                                        topics,
                                        config=config)
