import dataclasses
import random
import time
from threading import Thread
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
    max_affected_nodes: Optional[int] = None

    def random_time_in_range(self) -> float:
        return random.uniform(self.min_time_between_actions_sec, self.max_time_between_actions_sec)


class RandomNodeOp:
    def __init__(self, redpanda: RedpandaService, config: ActionConfig, admin: Admin):
        self.admin = admin
        self.config = config
        self.redpanda = redpanda
        self.nodes_affected = set()

    def limit_reached(self) -> bool:
        raise NotImplementedError

    def target_node(self) -> ClusterNode:
        available = set(self.redpanda.nodes) - self.nodes_affected
        return random.choice(list(available))

    def action(self):
        raise NotImplementedError

    def __call__(self, *args, **kwargs):
        if not self.limit_reached():
            self.action()


class RandomNodeDecommission(RandomNodeOp):
    def limit_reached(self):
        return len(self.nodes_affected) >= self.config.max_affected_nodes

    def action(self):
        node = self.target_node()
        broker_id = next((i for i, n in enumerate(self.redpanda.nodes) if n == node))
        self.redpanda.stop_node(node)
        self.admin.decommission_broker(id=broker_id)
        wait_until(
            lambda: broker_id not in {b['node_id'] for b in self.admin.get_brokers()},
            timeout_sec=120,
            backoff_sec=2,
            err_msg=f'Failed to decommission broker id {broker_id}'
        )
        self.nodes_affected.add(node)


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

    def limit_reached(self):
        return False

    def action(self):
        for topic in self.topics:
            for partition in range(topic.partition_count):
                old_leader = self.admin.get_partition_leader(namespace='kafka', topic=topic, partition=partition)
                self.admin.transfer_leadership_to(namespace='kafka',
                                                  topic=topic,
                                                  partition=partition,
                                                  target=None)

                def leader_is_changed():
                    new_leader = self.admin.get_partition_leader(namespace='kafka', topic=topic, partition=partition)
                    return new_leader != -1 and new_leader != old_leader

                wait_until(leader_is_changed,
                           timeout_sec=30,
                           backoff_sec=2,
                           err_msg='Leadership transfer failed')


class RandomNodeProcessFailure(RandomNodeOp):
    def __init__(self, redpanda: RedpandaService, config: ActionConfig, admin: Admin):
        super(RandomNodeProcessFailure, self).__init__(redpanda, config, admin)
        self.failure_injector = FailureInjector(self.redpanda)

    def limit_reached(self):
        return len(self.nodes_affected) >= self.config.max_affected_nodes

    def action(self):
        node = self.target_node()
        self.failure_injector.inject_failure(FailureSpec(FailureSpec.FAILURE_KILL, node))
        self.nodes_affected.add(node)


class ActionInjectorThread(Thread):
    def __init__(
            self,
            config: ActionConfig,
            redpanda: RedpandaService,
            random_op: RandomNodeOp,
            *args,
            **kwargs,
    ):
        self.random_op = random_op
        self.redpanda = redpanda
        self.config = config
        self._stop_requested = False
        super().__init__(*args, **kwargs)

    def run(self):
        wait_until(
            lambda: self.redpanda.healthy(),
            timeout_sec=self.config.cluster_start_lead_time_sec,
            backoff_sec=2,
            err_msg=f'Cluster not ready to begin actions'
        )

        while not self._stop_requested:
            time.sleep(self.config.random_time_in_range())
            self.random_op()

    def stop(self):
        self._stop_requested = True


class ActionCtx:
    def __init__(self, config: ActionConfig, redpanda: RedpandaService, random_op: RandomNodeOp):
        self.redpanda = redpanda
        self.config = config
        if config.max_affected_nodes is None:
            config.max_affected_nodes = len(redpanda.nodes) // 2
        self.thread = ActionInjectorThread(config, redpanda, random_op)

    def __enter__(self):
        self.thread.start()

    def __exit__(self, *args, **kwargs):
        self.thread.stop()


def create_context_with_defaults(redpanda: RedpandaService, op_type, *args, **kwargs):
    admin = Admin(redpanda)
    config = ActionConfig(
        cluster_start_lead_time_sec=120,
        min_time_between_actions_sec=10,
        max_time_between_actions_sec=30,
    )
    return ActionCtx(config, redpanda, op_type(redpanda, config, admin, *args, **kwargs))


def random_process_kills(redpanda: RedpandaService):
    return create_context_with_defaults(redpanda, RandomNodeProcessFailure)


def random_decommissions(redpanda: RedpandaService):
    return create_context_with_defaults(redpanda, RandomNodeDecommission)


def random_leadership_transfers(redpanda: RedpandaService, topics):
    return create_context_with_defaults(redpanda, RandomLeadershipTransfer, topics)
