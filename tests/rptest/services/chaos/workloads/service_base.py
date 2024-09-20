# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import requests
import sys
import os
from enum import Enum
from abc import ABC, abstractmethod
import dataclasses

from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.local_filesystem_utils import mkdir_p
from ducktape.errors import TimeoutError

from rptest.util import wait_until

from ..types import NoProgressError


class NodeState(str, Enum):
    INITIALIZED = 'initialized'
    STARTED = 'started'
    STOPPED = 'stopped'


@dataclasses.dataclass
class WorkloadInfo:
    succeeded_ops: int = 0
    failed_ops: int = 0
    timedout_ops: int = 0
    is_active: bool = False


class WorkloadServiceBase(ABC, Service):
    @property
    @abstractmethod
    def java_module_name(self):
        pass

    @abstractmethod
    def extra_config(self, node):
        pass

    @abstractmethod
    def validate_consistency(self):
        pass

    @abstractmethod
    def collect_stats(self):
        pass

    PERSISTENT_ROOT = "/var/lib/chaos_workload"
    SYSTEM_LOG_PATH = os.path.join(PERSISTENT_ROOT, "system.log")
    WORKLOAD_LOG_PATH = os.path.join(PERSISTENT_ROOT, "workload.log")

    def __init__(self, ctx, brokers_str, num_nodes):
        super().__init__(ctx, num_nodes=num_nodes)

        self._remote_port = 8080
        self._node_states = dict()
        self._brokers_str = brokers_str

        self.logs = {
            "system": {
                "path": self.SYSTEM_LOG_PATH,
                "collect_default": True,
            },
            "workload": {
                "path": self.WORKLOAD_LOG_PATH,
                "collect_default": True,
            }
        }

    def _remote_url(self, node, path):
        return f"http://{node.account.hostname}:{self._remote_port}/{path}"

    def _request(self, verb, node, path, timeout_sec=10, **kwargs):
        kwargs["timeout"] = timeout_sec
        url = self._remote_url(node, path)
        self.logger.debug(f"dispatching {verb} {url}")
        existing_exc = sys.exc_info()[1]
        try:
            r = requests.request(verb, url, **kwargs)
        except requests.RequestException as e:
            # Request exceptions (e.g. timeouts) come with a long context chain and
            # one of those exceptions has the __suppress_context__ attribute set to True.
            # This is not nice because the original context corresponding to existing_exc
            # (e.g. a test error) will not not be printed. To avoid that, throw out
            # all requests exceptions from the context chain.
            e.__context__ = existing_exc
            raise
        if r.status_code != 200:
            raise Exception(f"unexpected status code: {r.status_code}")
        return r

    def _is_alive(self, node):
        pids = node.account.java_pids(f"{self.java_module_name}\.App")
        return len(pids) > 0

    def _is_ready(self, node):
        try:
            r = requests.get(self._remote_url(node, "ping"), timeout=1)
        except Exception as e:
            # Broad exception handling for any lower level connection errors etc
            # that might not be properly classed as `requests` exception.
            self.logger.debug(
                f"Status endpoint {self.who_am_i()} not ready: {e}")
            return False
        else:
            return r.status_code == 200

    ### Service overrides

    def start_node(self, node, timeout_sec=10):
        self.logger.info(
            f"{self.who_am_i()}: starting worker on node {node.name}")
        assert not self._is_alive(node)

        node.account.mkdirs(self.PERSISTENT_ROOT)

        cmd = f"java -cp /opt/verifiers/verifiers.jar io.vectorized.chaos.{self.java_module_name}.App"
        wrapped_cmd = f"nohup {cmd} > {self.SYSTEM_LOG_PATH} 2>&1 & echo $!"

        pid_str = node.account.ssh_output(wrapped_cmd, timeout_sec=10)
        self.logger.debug(
            f"spawned {self.who_am_i()} node={node.name} pid={pid_str} port={self._remote_port}"
        )

        # Wait for the status endpoint to respond.
        wait_until(
            lambda: self._is_ready(node),
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=
            f"{self.who_am_i()}: worker failed to become ready within {timeout_sec} sec",
            retry_on_exc=False)

        # load the workload config
        workload_config = {
            "hostname": node.name,
            "results_dir": self.PERSISTENT_ROOT,
            "brokers": self._brokers_str,
        }
        workload_config |= self.extra_config(node)
        r = self._request("post",
                          node,
                          "init",
                          json=workload_config,
                          timeout_sec=timeout_sec)

        self._node_states[node.name] = NodeState.INITIALIZED

    def _kill(self, node, clean_shutdown):
        node.account.kill_java_processes(f"{self.java_module_name}\.App",
                                         clean_shutdown=clean_shutdown,
                                         allow_fail=True)

    def stop_node(self, node, timeout_sec=10):
        self.logger.info(
            f"{self.who_am_i()}: stopping worker on node {node.name}")

        try:
            self.stop_workload(nodes=[node])
        except Exception as e:
            self.logger.warn(
                f"{self.who_am_i()}: failed to stop workload on {node.name}")

        self.logger.debug(f"terminating worker on {node.name}")
        self._kill(node, clean_shutdown=True)

        try:
            wait_until(lambda: not self._is_alive(node),
                       timeout_sec=timeout_sec,
                       backoff_sec=1,
                       retry_on_exc=False)
        except TimeoutError:
            self.logger.warn(f"{self.who_am_i()}: process on {node.name} "
                             f"failed to stop within {timeout_sec} sec")
            self._kill(node, clean_shutdown=False)

    def clean_node(self, node):
        self.logger.info(
            f"{self.who_am_i()}: cleaning worker node {node.name}")
        self._kill(node, clean_shutdown=False)
        node.account.remove(self.PERSISTENT_ROOT, allow_fail=True)

    ### workload management

    def start_workload(self, nodes=None):
        if nodes is None:
            nodes = self.nodes

        for node in nodes:
            assert self._node_states.get(node.name) == NodeState.INITIALIZED
            self.logger.info(f"starting workload on {node.name}")
            self._request("post", node, "start")
            self._node_states[node.name] = NodeState.STARTED

    def stop_workload(self, nodes=None):
        if nodes is None:
            nodes = self.nodes

        for node in nodes:
            if self._node_states.get(node.name) == NodeState.STARTED:
                self._request("post", node, "stop")
                self._node_states[node.name] = NodeState.STOPPED

    def info(self, node, timeout_sec=10):
        r = self._request("get", node, "info", timeout_sec=timeout_sec)
        return WorkloadInfo(**{
            f.name: r.json()[f.name]
            for f in dataclasses.fields(WorkloadInfo)
        })

    def wait_progress(self, timeout_sec=10):
        started = {node.name: self.info(node) for node in self.nodes}
        progressed = set()

        def made_progress():
            for node in self.nodes:
                if node in progressed:
                    continue
                self.logger.debug(
                    f"checking if node {node.name} made progress")
                info = self.info(node)
                if info.succeeded_ops > started[node.name].succeeded_ops:
                    progressed.add(node.name)
            return len(progressed) == len(self.nodes)

        try:
            wait_until(made_progress, timeout_sec=timeout_sec, backoff_sec=1)
        except TimeoutError:
            raise NoProgressError(
                f"workload failed to progress within {timeout_sec} sec")

    def emit_event(self, node, name):
        self._request("post", node, "event/" + name)

    ### post-workload checks

    def _results_dir(self):
        return os.path.join(
            TestContext.results_dir(self.context, self.context.test_index),
            self.service_id)

    def _node_results_dir(self, node):
        return os.path.join(self._results_dir(), node.account.hostname)

    def copy_workload_logs(self):
        for node in self.nodes:
            assert self._node_states.get(node.name) == NodeState.STOPPED
            dest = self._node_results_dir(node)
            if not os.path.isdir(dest):
                mkdir_p(dest)
            node.account.copy_from(self.WORKLOAD_LOG_PATH, dest)

        # disable automatic copying
        del self.logs["workload"]

    def stop_and_validate(self):
        self.stop_workload()
        self.copy_workload_logs()

        try:
            stats = self.collect_stats()
        except:
            self.logger.warn(
                f"{self.who_am_i()}: failed to collect workload stats",
                exc_info=True)
        else:
            self.logger.info(f"{self.who_am_i()} workload stats: {stats}")

        self.validate_consistency()
