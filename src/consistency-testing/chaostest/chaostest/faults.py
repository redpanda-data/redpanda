# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from gobekli.logging import m
import logging
import sys
import time
import traceback

chaos_event_log = logging.getLogger("chaos-event")


class StrobeRecoverableFault:
    def __init__(self, node_selector, scope):
        self.node_selector = node_selector
        self.node = None
        self.workload = None
        self.title = f"strobe time ({scope})"
        self.scope = scope

    def inject(self, cluster, workload):
        try:
            self.workload = workload
            self.node = self.node_selector(cluster)
            if self.node == None:
                chaos_event_log.info(m("can't select a node").with_time())
                raise Exception("can't select a node")

            chaos_event_log.info(
                m(f"starting strobing on {self.node.node_id} ({self.scope})").
                with_time())
            self.workload.availability_logger.log_fault(
                f"starting strobing on {self.node.node_id} ({self.scope})")
            self.node.strobe_inject()
            chaos_event_log.info(
                m(f"strobbing on {self.node.node_id}").with_time())
        except:
            e, v = sys.exc_info()[:2]
            chaos_event_log.info(
                m("can't inject strobe",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=traceback.format_exc()).with_time())
            raise

    def recover(self):
        chaos_event_log.info(
            m(f"stopping strobbing on {self.node.node_id}").with_time())
        self.node.strobe_recover()
        chaos_event_log.info(
            m(f"stopped strobbing on {self.node.node_id}").with_time())
        self.workload.availability_logger.log_recovery(
            f"stopped strobbing on {self.node.node_id}")


class TerminateNodeRecoverableFault:
    def __init__(self, node_selector, scope):
        self.node_selector = node_selector
        self.node = None
        self.workload = None
        self.title = f"terminate service ({scope})"
        self.scope = scope

    def inject(self, cluster, workload):
        self.workload = workload
        self.node = self.node_selector(cluster)
        if self.node == None:
            chaos_event_log.info(m("can't select a node").with_time())
            raise Exception("can't select a node")

        try:
            chaos_event_log.info(
                m(f"terminating a service on {self.node.node_id} ({self.scope})"
                  ).with_time())
            self.workload.availability_logger.log_fault(
                f"terminating a service on {self.node.node_id} ({self.scope})")
            self.node.kill()
            chaos_event_log.info(
                m(f"a service on {self.node.node_id} terminated").with_time())
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            chaos_event_log.info(
                m("error on terminating a service",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=stacktrace).with_time())
            raise

        is_running = True

        for _ in range(0, 3):
            time.sleep(1)
            try:
                is_running = self.node.is_service_running()
            except:
                e, v = sys.exc_info()[:2]
                stacktrace = traceback.format_exc()
                chaos_event_log.info(
                    m("error on checking status of a service",
                      error_type=str(e),
                      error_value=str(v),
                      stacktrace=stacktrace).with_time())
                raise

        if is_running:
            chaos_event_log.info(
                m(f"can't terminate a service on {self.node.node_id}").
                with_time())
            raise Exception(
                f"can't terminate a service on {self.node.node_id}")

    def recover(self):
        chaos_event_log.info(
            m(f"restarting a service on {self.node.node_id}").with_time())
        self.node.start_service()
        chaos_event_log.info(
            m(f"a service on {self.node.node_id} restarted").with_time())

        attempts = 4
        while True:
            attempts -= 1
            time.sleep(5)
            if not self.node.is_service_running():
                chaos_event_log.info(
                    m(f"a service on {self.node.node_id} isn't running").
                    with_time())
                if attempts < 0:
                    raise Exception(
                        f"can't start a service on {self.node.node_id}")
                else:
                    continue
            break

        self.workload.availability_logger.log_recovery(
            f"a service on {self.node.node_id} restarted")


class SuspendServiceRecoverableFault:
    def __init__(self, node_selector, scope):
        self.node = None
        self.node_selector = node_selector
        self.workload = None
        self.title = f"pause service ({scope})"
        self.scope = scope

    def inject(self, cluster, workload):
        self.workload = workload
        self.node = self.node_selector(cluster)
        if self.node == None:
            chaos_event_log.info(m("can't select a node").with_time())
            raise Exception("can't select a node")

        chaos_event_log.info(
            m(f"suspending a service on {self.node.node_id} ({self.scope})").
            with_time())
        self.workload.availability_logger.log_fault(
            f"suspending a service on {self.node.node_id} ({self.scope})")
        self.node.pause_service()
        chaos_event_log.info(
            m(f"a service on {self.node.node_id} suspended").with_time())

    def recover(self):
        chaos_event_log.info(
            m(f"resuming a service on {self.node.node_id}").with_time())
        self.node.continue_service()
        self.workload.availability_logger.log_recovery(
            f"a service on {self.node.node_id} resumed")
        chaos_event_log.info(
            m(f"a service on {self.node.node_id} resumed").with_time())


class MakeIOSlowerRecoverableFault:
    def __init__(self, node_selector, scope):
        self.node_selector = node_selector
        self.node = None
        self.workload = None
        self.title = f"introduce 10ms disk delay ({scope})"
        self.scope = scope

    def inject(self, cluster, workload):
        self.workload = workload
        self.node = self.node_selector(cluster)
        if self.node == None:
            chaos_event_log.info(m("can't select a node").with_time())
            raise Exception("can't select a node")

        chaos_event_log.info(
            m(f"injecting 10ms disk delay on {self.node.node_id} ({self.scope})"
              ).with_time())
        self.workload.availability_logger.log_fault(
            f"injecting 10ms disk delay on {self.node.node_id} ({self.scope})")
        self.node.io_delay("all", 10)
        chaos_event_log.info(
            m(f"10ms disk delay on {self.node.node_id} injected").with_time())

    def recover(self):
        chaos_event_log.info(
            m(f"removing disk delay on {self.node.node_id}").with_time())
        self.node.io_recover()
        self.workload.availability_logger.log_recovery(
            f"disk delay on {self.node.node_id} removed")
        chaos_event_log.info(
            m(f"disk delay on {self.node.node_id} removed").with_time())


class MakeFsyncSlowerRecoverableFault:
    def __init__(self, node_selector, scope):
        self.node_selector = node_selector
        self.node = None
        self.workload = None
        self.title = f"introduce 10ms disk delay ({scope})"
        self.scope = scope

    def inject(self, cluster, workload):
        self.workload = workload
        self.node = self.node_selector(cluster)
        if self.node == None:
            chaos_event_log.info(m("can't select a node").with_time())
            raise Exception("can't select a node")

        chaos_event_log.info(
            m(f"injecting 10ms disk delay on {self.node.node_id} ({self.scope})"
              ).with_time())
        self.workload.availability_logger.log_fault(
            f"injecting 10ms disk delay on {self.node.node_id} ({self.scope})")
        self.node.io_delay("flush", 10)
        self.node.io_delay("fsync", 10)
        chaos_event_log.info(
            m(f"10ms disk delay on {self.node.node_id} injected").with_time())

    def recover(self):
        chaos_event_log.info(
            m(f"removing disk delay on {self.node.node_id}").with_time())
        self.node.io_recover()
        self.workload.availability_logger.log_recovery(
            f"disk delay on {self.node.node_id} removed")
        chaos_event_log.info(
            m(f"disk delay on {self.node.node_id} removed").with_time())


class RuinIORecoverableFault:
    def __init__(self, node_selector, scope):
        self.node = None
        self.workload = None
        self.node_selector = node_selector
        self.title = f"fail every disk operation ({scope})"
        self.scope = scope

    def inject(self, cluster, workload):
        self.workload = workload
        self.node = self.node_selector(cluster)
        if self.node == None:
            chaos_event_log.info(m("can't select a node").with_time())
            raise Exception("can't select a node")

        chaos_event_log.info(
            m(f"injecting disk error for any op on {self.node.node_id} ({self.scope})"
              ).with_time())
        self.workload.availability_logger.log_fault(
            f"injecting disk error for any op on {self.node.node_id} ({self.scope})"
        )
        self.node.io_ruin("all")
        chaos_event_log.info(
            m(f"disk error for any op on {self.node.node_id} injected").
            with_time())

    def recover(self):
        chaos_event_log.info(
            m(f"removing disk fault injection & restarting a service on {self.node.node_id}"
              ).with_time())
        self.node.io_recover()
        self.node.kill()
        self.node.start_service()

        attempts = 4
        while True:
            attempts -= 1
            time.sleep(5)
            if not self.node.is_service_running():
                chaos_event_log.info(
                    m(f"a service on {self.node.node_id} isn't running").
                    with_time())
                if attempts < 0:
                    raise Exception(
                        f"can't start a service on {self.node.node_id}")
                else:
                    continue
            break

        self.workload.availability_logger.log_recovery(
            f"disk fault injection removed & a service on {self.node.node_id} restarted"
        )
        chaos_event_log.info(
            m(f"disk fault injection removed & a service on {self.node.node_id} restarted"
              ).with_time())


class BaselineRecoverableFault:
    def __init__(self):
        self.workload = None
        self.title = f"baseline"
        self.name = f"baseline"

    def inject(self, cluster, workload):
        self.workload = workload
        self.workload.availability_logger.log_fault("nothing")
        time.sleep(3)

    def recover(self):
        time.sleep(3)
        self.workload.availability_logger.log_recovery("nothing")


class IsolateNodeRecoverableFault:
    def __init__(self, node_selector, scope):
        self.node = None
        self.workload = None
        self.ips = []
        self.peers = []
        self.node_selector = node_selector
        self.title = f"isolate node from all peers ({scope})"
        self.scope = scope

    def inject(self, cluster, workload):
        self.workload = workload
        self.node = self.node_selector(cluster)
        if self.node == None:
            chaos_event_log.info(m("can't select a node").with_time())
            raise Exception("can't select a node")

        for node_id in cluster.nodes.keys():
            if node_id != self.node.node_id:
                self.ips.append(cluster.nodes[node_id].ip)
                self.peers.append(node_id)

        peers = ", ".join(self.peers)
        chaos_event_log.info(
            m(f"isolating node {self.node.node_id} ({self.scope}) from {peers}"
              ).with_time())
        self.workload.availability_logger.log_fault(
            f"isolating node {self.node.node_id} ({self.scope}) from {peers}")
        self.node.isolate(self.ips)
        chaos_event_log.info(
            m(f"node {self.node.node_id} isolated from {peers}").with_time())

    def recover(self):
        peers = ", ".join(self.peers)
        chaos_event_log.info(
            m(f"rejoining node {self.node.node_id} to {peers}").with_time())
        self.node.rejoin(self.ips)
        self.workload.availability_logger.log_recovery(
            f"node {self.node.node_id} rejoined to {peers}")
        chaos_event_log.info(
            m(f"node {self.node.node_id} rejoined to {peers}").with_time())
