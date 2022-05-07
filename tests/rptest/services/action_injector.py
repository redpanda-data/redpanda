import dataclasses
import random
import time
from threading import Thread, Event
from typing import Optional

from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until

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

    def target_node(self) -> Optional[ClusterNode]:
        """
        Randomly selects the next node to apply the action on. A set of affected
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
        assert (self.thread.action_triggered
                and self.thread.reverse_action_triggered)


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
