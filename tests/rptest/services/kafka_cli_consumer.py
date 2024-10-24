# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import threading
import time
from ducktape.services.background_thread import BackgroundThreadService

from rptest.clients.kafka_cli_tools import KafkaCliTools
from ducktape.utils.util import wait_until


class KafkaCliConsumer(BackgroundThreadService):
    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 group=None,
                 offset=None,
                 partitions=None,
                 isolation_level=None,
                 from_beginning=False,
                 consumer_properties={},
                 formatter_properties={},
                 instance_name=None):
        super(KafkaCliConsumer, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._group = group
        self._offset = offset
        self._partitions = partitions
        self._isolation_level = isolation_level
        self._from_beginning = from_beginning
        self._consumer_properties = consumer_properties
        self._formatter_properties = formatter_properties
        self._stopping = threading.Event()
        self._instance_name = "cli-consumer" if instance_name is None else instance_name
        self._done = None
        self._progress_reporter = None
        self._last_consumed = time.time()
        self._lock = threading.Lock()
        assert self._partitions is not None or self._group is not None, "either partitions or group have to be set"

        self._cli = KafkaCliTools(self._redpanda)
        self._message_cnt = 0

    def script(self):
        return self._cli._script("kafka-console-consumer.sh")

    def _worker(self, _, node):
        self._done = False
        self._stopping.clear()
        self._progress_reporter = threading.Thread(
            target=lambda: self._report_progress(), daemon=True)
        self._progress_reporter.start()
        try:

            cmd = [self.script()]
            cmd += ["--topic", self._topic]
            if self._group is not None:
                cmd += ["--group", str(self._group)]
            if self._offset is not None:
                cmd += ['--offset', str(self._offset)]
            if self._partitions is not None:
                cmd += ['--partition', ','.join(self._partitions)]
            if self._isolation_level is not None:
                cmd += ["--isolation-level", str(self._isolation_level)]
            if self._from_beginning:
                cmd += ["--from-beginning"]
            for k, v in self._consumer_properties.items():
                cmd += ['--consumer-property', f"{k}={v}"]
            for k, v in self._formatter_properties.items():
                cmd += ['--property', f"{k}={v}"]

            cmd += ["--bootstrap-server", self._redpanda.brokers()]

            for l in node.account.ssh_capture(' '.join(cmd)):
                self._redpanda.logger.debug(l)
                # last line does not correspond to a consumed message and looks like
                # "Processed a total of N messages"
                if not l.startswith("Processed a total of "):
                    with self._lock:
                        self._message_cnt += 1
                        self._last_consumed = time.time()
        except:
            if self._stopping.is_set():
                # Expect a non-zero exit code when killing during teardown
                pass
            else:
                raise
        finally:
            self._done = True

    def message_cnt(self):
        with self._lock:
            return self._message_cnt

    def wait_for_messages(self, messages, timeout=30):
        wait_until(lambda: self.message_cnt() >= messages,
                   timeout,
                   backoff_sec=2)

    def wait_for_started(self, timeout=10):
        def all_started():
            return all([
                len(node.account.java_pids("ConsoleConsumer")) == 1
                for node in self.nodes
            ])

        wait_until(all_started, timeout, backoff_sec=1)

    def stop_node(self, node):
        self._stopping.set()
        node.account.kill_process("java", clean_shutdown=True)
        if self._progress_reporter.is_alive():
            self._progress_reporter.join()

        try:
            wait_until(lambda: self._done is None or self._done == True,
                       timeout_sec=10)
        except:
            self.logger.warn(
                f"{self._instance_name} running on {node.name} failed to stop gracefully"
            )
            node.account.kill_process("java", clean_shutdown=False)
            wait_until(
                lambda: self._done is None or self._done == True,
                timeout_sec=5,
                err_msg=
                f"{self._instance_name} running on {node.name} failed to stop after SIGKILL"
            )

    def clean_node(self, node):
        pass

    def _report_progress(self):
        while (not self._stopping.is_set()):
            with self._lock:
                self.logger.info(
                    f"Consumed {self._message_cnt} messages, time since last consumed: {time.time() - self._last_consumed} seconds"
                )
            if self._stopping.is_set():
                break

            time.sleep(5)
