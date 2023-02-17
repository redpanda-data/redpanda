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
from collections import defaultdict

from ducktape.services.service import Service
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
                 *,
                 nodes: Optional[list[ClusterNode]] = None,
                 num_nodes: Optional[int] = None,
                 topic: str,
                 msg_size: Optional[int],
                 workers: int,
                 key_count: Optional[int] = None,
                 group_name: str = "repeat01",
                 max_buffered_records: Optional[int] = None,
                 mb_per_worker: Optional[int] = None,
                 use_transactions: bool = False,
                 transaction_abort_rate: Optional[float] = None,
                 rate_limit_bps: Optional[int] = None,
                 msgs_per_transaction: Optional[int] = None):
        """
        :param rate_limit_bps: Total rate for all nodes: each node will get an equal share.
        """
        if num_nodes is None and nodes is None:
            # Default: run a single node
            num_nodes = 1

        super().__init__(context, num_nodes=0 if nodes else num_nodes)

        if nodes is not None:
            assert len(nodes) > 0
            self.nodes = nodes

        self.redpanda = redpanda
        self.topic = topic
        self.msg_size = msg_size
        self.workers = workers
        self.group_name = group_name

        self.rate_limit_bps_per_node = rate_limit_bps // len(
            self.nodes) if rate_limit_bps else None

        # Note: using a port that happens to already be in test environment
        # firewall rules from other use cases.  If changing this, update
        # terraform for test clusters.
        self.remote_port = 8080

        self.key_count = key_count
        self.max_buffered_records = max_buffered_records

        if mb_per_worker is None:
            mb_per_worker = 4

        self.mb_per_worker = mb_per_worker

        self.use_transactions = use_transactions
        self.transaction_abort_rate = transaction_abort_rate
        self.msgs_per_transaction = msgs_per_transaction

        self._stopped = False

    def clean_node(self, node):
        self.redpanda.logger.debug(f"{self.__class__.__name__}.clean_node")
        node.account.kill_process(self.EXE, clean_shutdown=False)
        if node.account.exists(self.LOG_PATH):
            node.account.remove(self.LOG_PATH)

    def start_node(self, node, clean=None):
        initial_data_mb = self.mb_per_worker * self.workers

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

        if self.max_buffered_records is not None:
            cmd += f" -max-buffered-records={self.max_buffered_records}"

        if self.rate_limit_bps_per_node is not None:
            cmd += f" -rate-limit-bps={self.rate_limit_bps_per_node}"

        if self.use_transactions:
            cmd += f" -use-transactions"

            if self.transaction_abort_rate is not None:
                cmd += f" -transaction-abort-rate={self.transaction_abort_rate}"

            if self.msgs_per_transaction is not None:
                cmd += f" -msgs-per-transaction={self.msgs_per_transaction}"

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
            try:
                group = rpk.group_describe(self.group_name, summary=True)
            except Exception as e:
                self.logger.debug(
                    f"group_ready: {self.group_name} got exception from describe: {e}"
                )
                return False

            if group is None:
                self.logger.debug(
                    f"group_ready: {self.group_name} got None from describe")
                return False
            elif group.state != "Stable":
                self.logger.debug(
                    f"group_ready: waiting for stable, current state {group.state}"
                )
                return False
            elif group.members < expect_members:
                self.logger.debug(
                    f"group_ready: waiting for node count ({group.members} != {expect_members})"
                )
                return False
            else:
                return True

        self.logger.debug(f"Waiting for group {self.group_name} to be ready")
        t1 = time.time()
        try:
            self.redpanda.wait_until(group_ready,
                                     timeout_sec=120,
                                     backoff_sec=10)
        except:
            # On failure, dump stacks on all workers in case there is an apparent client bug to investigate
            for node in self.nodes:
                try:
                    r = requests.get(self._remote_url(node, "print_stack"))
                    r.raise_for_status()
                except Exception as e:
                    # Just log exceptions: we want to proceed with rest of teardown
                    self.logger.warn(
                        f"Failed to print stack on {node.name}: {e}")

            # On failure, inspect the group to identify which workers
            # specifically were absent.  This information helps to
            # go inspect the remote kgo-repeater logs to see if the
            # client logged any errors.

            rpk = RpkTool(self.redpanda)
            group = rpk.group_describe(self.group_name, summary=False)

            # Identify which of the clients is dropping some consumers
            workers_by_host = defaultdict(set)
            for p in group.partitions:
                # kgo-repeater worker names are like this:
                # {hostname}_{pid}_w_{index}, where index is a zero-based counter
                host, pid, _, n = p.client_id.split("_")
                workers_by_host[host].add(int(n))

            for host, live_workers in workers_by_host.items():
                expect_workers = set(range(0, self.workers))
                missing_workers = expect_workers - live_workers
                self.logger.error(f"  {host}: missing {missing_workers}")

            raise

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

        self.redpanda.wait_until(check, timeout_sec=timeout_sec, backoff_sec=1)


@contextmanager
def repeater_traffic(context,
                     redpanda,
                     *args,
                     cleanup: Optional[Callable] = None,
                     **kwargs):
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
        if cleanup:
            cleanup()
