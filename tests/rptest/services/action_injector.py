import dataclasses
import random
import time
from threading import Thread, Event
from typing import Optional

from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until

from rptest.services.admin import Admin
from rptest.services.failure_injector import FailureSpec, make_failure_injector
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

    is_reversible = False

    def __init__(self, redpanda: RedpandaService, config: ActionConfig,
                 admin: Admin):
        self.admin = admin
        self.config = config
        self.redpanda = redpanda
        self.nodes_for_action = set(self.redpanda.nodes)
        self.affected_nodes = set()
        self.recovered_nodes = set()
        self.last_affected_node = None
        # Node cycle signals that all nodes in cluster have been processed once.
        # Since the nodes are selected randomly, it is possible that a node will
        # be selected last in one cycle and first in the next cycle. This repeated
        # action will cause a test failure in some cases. We avoid this by increasing
        # the sleep duration for the iteration when node cycle is complete.
        self.node_cycle_complete = False

    def recover_node(self, node: ClusterNode):
        """
        A node on which the disruptive action had been performed is recovered/restored. This
        happens for reversible operations such as killing redpanda, where recovery means starting
        redpanda back up again.
        """
        self.recovered_nodes.add(node)
        self.affected_nodes.remove(node)

    def max_affected_nodes_reached(self) -> bool:
        """
        Checks if the number of affected nodes so far equals the maximum number of nodes
        this action is allowed to affect. If so all future calls to action will be no-op.
        """
        raise NotImplementedError

    def target_node(self) -> Optional[ClusterNode]:
        """
        Selects the next node to process randomly from available nodes. If the set of available
        nodes is empty, then we have processed all the nodes in current iteration, and we start
        on the set of nodes which have already been processed once.
        """
        if not self.nodes_for_action:
            self.nodes_for_action, self.recovered_nodes = self.recovered_nodes, self.nodes_for_action
            self.node_cycle_complete = False
        node = random.choice([n for n in self.nodes_for_action])
        self.nodes_for_action.remove(node)
        return node

    def do_action(self) -> ClusterNode | None:
        """
        Applies the disruptive action, returns node or entity the action was applied on
        """
        raise NotImplementedError

    def action(self) -> Optional[ClusterNode]:
        if not self.max_affected_nodes_reached():
            node = self.do_action()
            if node and not self.nodes_for_action:
                # We have processed all nodes in the current cycle.
                self.node_cycle_complete = True
            return node
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


class ProcessKill(DisruptiveAction):
    PROCESS_START_WAIT_SEC = 20
    PROCESS_START_WAIT_BACKOFF = 2

    is_reversible = True

    def __init__(self, redpanda: RedpandaService, config: ActionConfig,
                 admin: Admin):
        super(ProcessKill, self).__init__(redpanda, config, admin)
        self.failure_injector = make_failure_injector(self.redpanda)

    def max_affected_nodes_reached(self):
        assert self.config.max_affected_nodes is not None
        return len(self.affected_nodes) >= self.config.max_affected_nodes

    def do_action(self):
        node = self.target_node()
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
        assert self.last_affected_node
        self._start_rp(node=self.last_affected_node)
        self.recover_node(self.last_affected_node)
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
        self.daemon = True
        self.disruptive_action = disruptive_action
        self.redpanda = redpanda
        self.config = config
        self._stop_requested = Event()
        self.action_triggered = False
        self.reverse_action_triggered = False
        self._ex = None

    def run(self):
        try:
            self._run()
        except Exception as e:
            self._ex = e

    def _run(self):
        def all_nodes_started(nodes):
            statuses = [self.redpanda.is_node_ready(node) for node in nodes]
            return all(status is True for status in statuses)

        wait_until(lambda: all_nodes_started(self.redpanda.nodes),
                   timeout_sec=self.config.cluster_start_lead_time_sec,
                   backoff_sec=2,
                   err_msg='Cluster not ready to begin actions')

        self.redpanda.logger.info('cluster is ready, starting action loop')

        while not self._stop_requested.is_set():
            if self.disruptive_action.action():
                self.action_triggered = True
            sleep_interval = self.config.time_between_actions()

            # If we have processed all nodes once, increase the sleep interval
            # just for this iteration. This avoids the case where the last node
            # in the previous cycle is restarted again in the next cycle.
            if self.disruptive_action.node_cycle_complete:
                sleep_interval *= 3
            time.sleep(sleep_interval)
            if self.disruptive_action.reverse():
                self.reverse_action_triggered = True

        # We reversed all actions, but reversing a node kill does not wait for
        # the node to become available, it just starts the process.  In order
        # for subsequent code to count on the cluster being able to respond
        # to API requests, we must additionally wait for readiness.
        self.redpanda.logger.info(
            f"Stopping/pausing failure injector, waiting for nodes {[n.name for n in self.redpanda.started_nodes()]} to be ready"
        )
        wait_until(lambda: all_nodes_started(self.redpanda.started_nodes()),
                   timeout_sec=self.config.cluster_start_lead_time_sec,
                   backoff_sec=0.5,
                   err_msg='Cluster did not recover after failures')

    def join(self, *args, **kwargs):
        super().join(*args, **kwargs)
        if self._ex is not None:
            raise self._ex

    def stop(self):
        self._stop_requested.set()


class ActionCtx:
    def __init__(self, config: ActionConfig, redpanda: RedpandaService,
                 disruptive_action: DisruptiveAction):
        self.redpanda = redpanda
        self.config = config
        if config.max_affected_nodes is None:
            config.max_affected_nodes = len(redpanda.nodes) // 2
        self.thread = ActionInjectorThread(config, redpanda, disruptive_action)

    def __enter__(self):
        self.redpanda.logger.info('entering random failure ctx')
        self.thread.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.redpanda.logger.info('leaving random failure ctx')
        self.thread.stop()
        self.thread.join()

    def assert_actions_triggered(self):
        """
        Helper to allow tests to assert that the forward and reverse
        actions were triggered by the thread
        """
        assert (self.thread.action_triggered
                and self.thread.reverse_action_triggered)


def create_context_with_defaults(redpanda: RedpandaService,
                                 op_type,
                                 config: ActionConfig | None = None,
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
                         config: ActionConfig | None = None) -> ActionCtx:
    return create_context_with_defaults(redpanda, ProcessKill, config=config)
