# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import requests
import json
from typing import Optional, Callable
from contextlib import contextmanager

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster import ClusterNode
from ducktape.tests.test import TestContext

from rptest.services.redpanda import RedpandaService
from rptest.clients.rpk import RpkTool


class KgoRepeaterService(Service):
    EXE = "kgo-repeater"
    LOG_PATH = "/tmp/kgo-repeater.log"

    logs = {"repeater_log": {"path": LOG_PATH, "collect_default": True}}

    def __init__(self,
                 context: TestContext,
                 redpanda: RedpandaService,
                 nodes: Optional[list[ClusterNode]],
                 topic: str,
                 msg_size: Optional[int],
                 workers: int,
                 key_count: int,
                 group_name: str = "repeat01"):
        # num_nodes=0 because we're asking it to not allocate any for us
        super().__init__(context, num_nodes=0 if nodes else 1)

        if nodes is not None:
            assert len(nodes) > 0
            self.nodes = nodes

        self.redpanda = redpanda
        self.topic = topic
        self.msg_size = msg_size
        self.workers = workers
        self.group_name = group_name

        # Note: using a port that happens to already be in test environment
        # firewall rules from other use cases.  If changing this, update
        # terraform for test clusters.
        self.remote_port = 8080

        self.key_count = key_count

        self._stopped = False

    def clean_node(self, node):
        self.redpanda.logger.debug(f"{self.__class__.__name__}.clean_node")
        node.account.kill_process(self.EXE, clean_shutdown=False)
        if node.account.exists(self.LOG_PATH):
            node.account.remove(self.LOG_PATH)

    def start_node(self, node, clean=None):
        mb_per_worker = 1
        initial_data_mb = mb_per_worker * self.workers

        cmd = (
            "/opt/kgo-verifier/kgo-repeater "
            f"-topic {self.topic} -brokers {self.redpanda.brokers()} "
            f"-workers {self.workers} -initial-data-mb {initial_data_mb} "
            f"-group {self.group_name} -remote -remote-port {self.remote_port} "
        )

        if self.msg_size is not None:
            cmd += f" -payload-size={self.msg_size}"

        if self.key_count is not None:
            cmd += f" -keys={self.key_count}"

        cmd = f"nohup {cmd} >> {self.LOG_PATH} 2>&1 &"

        self.logger.info(f"start_node[{node.name}]: {cmd}")
        node.account.ssh(cmd)

    def stop(self, *args, **kwargs):
        # On first call to stop, log status from the workers.
        # (stop() may be called more than once during teardown)
        if not self._stopped:
            for node in self.nodes:
                # This is only advisory, make errors non-fatal
                try:
                    r = requests.get(self._remote_url(node, "status"))
                    self.logger.debug(
                        f"kgo-repeater status on node {node.name}:")
                    self.logger.debug(json.dumps(r.json(), indent=2))
                except:
                    self.logger.exception(
                        f"Error getting pre-stop status on {node.name}")

        self._stopped = True

        super().stop(*args, **kwargs)

    def stop_node(self, node):
        node.account.kill_process(self.EXE,
                                  allow_fail=False,
                                  clean_shutdown=True)

    def _remote_url(self, node, path):
        return f"http://{node.account.hostname}:{self.remote_port}/{path}"

    def remote_to_all(self, path):
        for node in self.nodes:
            self.logger.debug(f"Invoking remote path {path} on {node.name}")
            r = requests.get(self._remote_url(node, path))
            r.raise_for_status()

    def prepare_and_activate(self):
        """
        Wait for all the processes to come up and the consumer group
        to stabilize, then activate all their producers.
        """
        self.await_group_ready()

        self.logger.debug("Activating producer loop")
        self.remote_to_all("activate")

    def await_group_ready(self):

        expect_members = self.workers * len(self.nodes)

        def group_ready():
            rpk = RpkTool(self.redpanda)
            group = rpk.group_describe(self.group_name, summary=True)
            if group is None:
                self.logger.debug(
                    f"group_ready: {self.group_name} got None from describe")
                return False
            elif group.state != "Stable":
                self.logger.debug(
                    f"group_ready: waiting for stable, current state {group.state}"
                )
                return False
            elif group.members < expect_members / 2:
                # FIXME: this should really require that all consumers are present, but
                # in practice I see some a small minority of consumers drop out of the
                # group sometimes when the cluster undergoes an all-node concurrent restart,
                # and I don't want to stop the test for that.
                self.logger.debug(
                    f"group_ready: waiting for node count ({group.members} != {expect_members})"
                )
                return False
            else:
                return True

        self.logger.debug(f"Waiting for group {self.group_name} to be ready")
        t1 = time.time()
        wait_until(group_ready, timeout_sec=120, backoff_sec=10)
        self.logger.debug(
            f"Group {self.group_name} became ready in {time.time() - t1}s")

    def total_messages(self):
        """
        :return: 2-tuple of produced, consumed
        """
        produced = 0
        consumed = 0
        for node in self.nodes:
            r = requests.get(self._remote_url(node, "status"))
            r.raise_for_status()
            node_status = r.json()
            for worker_status in node_status:
                produced += worker_status['produced']
                consumed += worker_status['consumed']

        return produced, consumed

    def await_progress(self, msg_count, timeout_sec):
        """
        Call this in places you want to assert some progress
        is really being made: say how many messages should be
        produced+consumed & how long you expect it to take.
        """
        initial_p, initial_c = self.total_messages()

        # Give it at least some chance to progress before we start checking
        time.sleep(0.5)

        def check():
            p, c = self.total_messages()
            pct = min(
                float(p - initial_p) / (msg_count),
                float(c - initial_c) / (msg_count)) * 100
            self.logger.debug(
                f"await_progress: {pct:.1f}% p={p} c={c}, initial_p={initial_p}, initial_c={initial_c}, await count {msg_count})"
            )
            return p >= initial_p + msg_count and c >= initial_c + msg_count

        # Minimum window for checking for progress: otherwise when the bandwidth
        # expection is high, we end up failing if we happen to check at a moment
        # the system isn't at peak throughput (e.g. when it's just warming up)
        timeout_sec = max(timeout_sec, 60)

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=1)


@contextmanager
def repeater_traffic(context, redpanda, *args, cleanup: Callable, **kwargs):
    svc = KgoRepeaterService(context, redpanda, *args, **kwargs)
    svc.start()
    svc.prepare_and_activate()

    try:
        yield svc
    except:
        # Helpful to log the exception so that it appears before
        # all the logs from our teardown and developer can jump
        # straight to the point the error occurred.
        redpanda.logger.exception("Exception during repeater_traffic region")
        raise
    finally:
        svc.stop()
        cleanup()
