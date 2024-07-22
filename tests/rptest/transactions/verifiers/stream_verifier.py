# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.services.service import Service
import requests
import sys
from rptest.util import wait_until

from rptest.remote_scripts.stream_verifier_txn import COMMAND_PRODUCE, \
    COMMAND_CONSUME, COMMAND_ATOMIC

title = "StreamVerifier"
filename = "stream_verifier_txn.py"
available_actions = [COMMAND_PRODUCE, COMMAND_CONSUME, COMMAND_ATOMIC]


class CrashedException(Exception):
    pass


OUTPUT_LOG = f"/opt/remote/var/{title}.log"


class StreamVerifier(Service):
    logs = {f"{title}": {"path": OUTPUT_LOG, "collect_default": True}}

    def __init__(self, context, redpanda):
        super(StreamVerifier, self).__init__(context, num_nodes=1)
        self._port = 8090
        self._redpanda = redpanda
        self._is_done = False
        self._node = None
        self._partitions = None

    def is_alive(self, node):
        """Check that webserver is running on target node

        Args:
            node (ClusterNode): node that is running the StreamVerifier

        Returns:
            bool: True is webserver is running
        """
        result = node.account.ssh_output(
            f"bash /opt/remote/control/alive.sh {title}")
        result = result.decode("utf-8")
        pid_alive = "YES" in result

        if not pid_alive:
            return False

        # Use requests to check if webserver returns any status
        return len(self.get_service_config()) > 0

    def is_action_ready(self, action):
        if action not in available_actions:
            raise RuntimeError(
                f"'{action}' is unknown. "
                f"Available actions are {', '.join(available_actions)}")
        try:
            r = self.get_produce_status()
            if r['status'][action] == 'READY':
                # Check that no active command is running
                return True
            else:
                self.logger.debug(f"Requested action of '{action}' is not "
                                  "ready. Current statuses are "
                                  f"'{r['status']}' ")
                return False
            return True
        except requests.exceptions.ConnectionError:
            return False

    def get_action_status(self, action) -> dict:
        if action == COMMAND_PRODUCE:
            return self.get_produce_status()
        elif action == COMMAND_CONSUME:
            return self.get_verify_status()
        elif action == COMMAND_ATOMIC:
            return self.get_atomic_status()
        else:
            # Should never happen
            raise RuntimeError(f"Invalid action '{action}'")

    def get_processed_count(self, action):
        s = self.get_action_status(action)
        # A bit ugly, but works
        count = s.get('processed_messages', 0)
        self.logger.debug(f"[{action}] ...{count} messages")
        return count

    def ensure_progress(self, action, delta, timeout_sec):
        # Validations
        if not self.is_alive(self._node):
            raise CrashedException(
                f"{title} process has crashed. Check its logs for details")
        elif action not in available_actions:
            raise RuntimeError(
                f"'{action}' is unknown. "
                f"Available actions are {', '.join(available_actions)}")
        elif self.is_action_ready(action):
            self.logger.info(f"Action '{action}' already finished")
            return

        initial_message_count = self.get_processed_count(action)
        self.logger.info(
            f"[{action}] initial messages count is {initial_message_count}")
        self.logger.info(f"[{action}] Waiting for "
                         f"{initial_message_count + delta} message count")
        wait_until(lambda: self.get_processed_count(action) >
                   initial_message_count + delta,
                   timeout_sec=timeout_sec,
                   backoff_sec=1,
                   err_msg=f"{title} service ({self._node.account.hostname}) "
                   f"failed progressing in {action} within {timeout_sec} sec",
                   retry_on_exc=False)

    # Service overrides
    def start_node(self, node, timeout_sec=30):

        # Build cmd line
        cmd = f'bash /opt/remote/control/start.sh {title} ' \
            f'\"python3 /opt/remote/{filename} ' \
            f'--brokers {self._redpanda.brokers()} webservice ' \
            f'--port {self._port}\"'
        # run webserver
        node.account.ssh(cmd)
        self._pid = node.account.ssh_output(f"cat /opt/remote/var/{title}.pid")
        self._pid = self._pid.decode('utf-8').strip()
        self._node = node

        wait_until(lambda: self.is_alive(node),
                   timeout_sec=timeout_sec,
                   backoff_sec=1,
                   err_msg=f"{title} service {node.account.hostname} "
                   f"failed to start within {timeout_sec} sec",
                   retry_on_exc=False)

        wait_until(lambda: self.is_action_ready("produce"),
                   timeout_sec=timeout_sec,
                   backoff_sec=1,
                   err_msg=f"{title} service {node.account.hostname} failed "
                   f"to become ready within {timeout_sec} sec",
                   retry_on_exc=False)

    def stop_node(self, node):
        if self.is_alive(node):
            node.account.ssh(f"bash /opt/remote/control/stop.sh {title}")

    def clean_node(self, node):
        # make sure that there is no such process
        _pid = node.account.ssh_output(f"sudo ps -aux | grep {filename} | "
                                       f"grep -v grep | tr -s ' ' | "
                                       f"cut -d' ' -f2")
        node.account.ssh_output(f"kill -9 {_pid.decode().strip()}")

    def wait_node(self, node, timeout_sec=sys.maxsize):
        wait_until(lambda: not (self.is_alive(node)),
                   timeout_sec=timeout_sec,
                   backoff_sec=1,
                   err_msg=f"{title} service {node.account.hostname} failed "
                   f"to stop within {timeout_sec} sec",
                   retry_on_exc=False)
        return True

    # RPCs
    @staticmethod
    def _raise_on_non_success(request_result):
        if request_result.status_code != 200:
            raise Exception(
                f"unexpected status code: {request_result.status_code} "
                f"content: {request_result.content}")

    def _get_ip(self):
        return self._node.account.hostname

    def _get_url(self, suffix=""):
        return f"http://{self._get_ip()}:{self._port}/{suffix}"

    def _get(self, url: str, raise_on_fails=False) -> dict:
        self._redpanda.logger.debug(f"Dispatching GET {url}")
        try:
            # Use 120 sec timeout for all requests
            r = requests.get(url, timeout=120)
            if raise_on_fails:
                self._raise_on_non_success(r)
            return r.json()
        except (requests.ConnectionError, requests.ConnectTimeout):
            self.logger.debug(
                f"{self.service_id} failed to connect to {title} using {url}")
            return {}

    def _post(self, url: str, payload: dict) -> dict:
        self._redpanda.logger.debug(f"Dispatching POST {url} with '{payload}'")
        try:
            r = requests.post(url, json=payload)
            self._raise_on_non_success(r)
            return r.json()
        except (requests.ConnectionError, requests.ConnectTimeout) as e:
            self.logger.warning(
                f"{self.service_id} failed to connect to {title} using {url}: "
                f"{e}")
            return {}

    def _delete(self, url: str) -> dict:
        self._redpanda.logger.debug(f"Dispatching DELETE {url}")
        try:
            r = requests.delete(url)
            self._raise_on_non_success(r)
            return r.json()
        except (requests.ConnectionError, requests.ConnectTimeout) as e:
            self.logger.warning(
                f"{self.service_id} failed to connect to {title} using {url}: "
                f"{e}")
            return {}

    def get_service_config(self):
        return self._get(self._get_url())

    def update_service_config(self, config):
        return self._post(self._get_url(), config)

    def get_produce_status(self):
        return self._get(self._get_url("produce"), raise_on_fails=False)

    def get_verify_status(self):
        return self._get(self._get_url("verify"), raise_on_fails=False)

    def get_atomic_status(self):
        return self._get(self._get_url("atomic"), raise_on_fails=False)

    def remote_start_produce(self,
                             target_topic_name,
                             message_count,
                             messages_per_sec=0):
        payload = {
            "topic_group_id": "stream_verifier_group",
            "target_topic_name": target_topic_name,
            # Consider increasing when debugging
            # "consume_timeout_s": 30,
            "msg_rate_limit": messages_per_sec,
            "total_messages": message_count
        }

        return self._post(self._get_url("produce"), payload)

    def remote_start_verify(self, source_topic_name):
        payload = {"source_topic_name": source_topic_name}
        return self._post(self._get_url("verify"), payload)

    def remote_start_atomic(self,
                            source_topic_name,
                            target_topic_name,
                            messages_per_sec=0):
        payload = {
            "topic_group_id": "stream_verifier_group",
            "target_topic_name": target_topic_name,
            "source_topic_name": source_topic_name,
            # Consider increasing when debugging
            # "consume_timeout_s": 30,
            "msg_rate_limit": messages_per_sec
        }
        return self._post(self._get_url("atomic"), payload)

    def remote_stop_produce(self):
        return self._delete(self._get_url("produce"))

    def remote_stop_verify(self):
        return self._delete(self._get_url("verify"))

    def remote_stop_atomic(self):
        return self._delete(self._get_url("atomic"))

    def remote_wait_action(self, action, timeout_sec=600):
        def is_action_active(action):
            r = self.get_action_status(action)
            status = r['status']
            self.logger.debug(
                f"[{action}] {status[action]}; {r['processed_messages']}")
            return True if status[action] == 'ACTIVE' else False

        self.logger.info(f"Waiting for '{action}' action to finish")
        wait_until(lambda: not is_action_active(action),
                   timeout_sec=timeout_sec,
                   backoff_sec=5,
                   err_msg=f"{title} service {self._node.account.hostname} "
                   f"failed to finish {action} action in {timeout_sec} sec",
                   retry_on_exc=False)

    def wait_for_processed_count(self, action, message_count, timeout_sec=600):
        # Validations
        if not self.is_alive(self._node):
            raise CrashedException(
                f"{title} process has crashed. Check its logs for details")
        elif action not in available_actions:
            raise RuntimeError(
                f"'{action}' is unknown. "
                f"Available actions are {', '.join(available_actions)}")
        self.logger.info(
            f"[{action}] Waiting for {message_count} messages to be produced ")
        wait_until(lambda: self.get_processed_count(action) >= message_count,
                   timeout_sec=timeout_sec,
                   backoff_sec=1,
                   err_msg=f"{title} service ({self._node.account.hostname}) "
                   f"failed to reach {message_count} messages for {action}"
                   f" within {timeout_sec} sec",
                   retry_on_exc=False)
