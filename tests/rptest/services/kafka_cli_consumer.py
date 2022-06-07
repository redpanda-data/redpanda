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
                 save_msgs=True,
                 kafka_version: str = None):
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
        self._save_msgs = save_msgs
        self._stopping = threading.Event()
        # assert self._partitions is not None or self._group is not None, "either partitions or group have to be set"

        self._cli = KafkaCliTools(self._redpanda, version=kafka_version)
        self._messages = []

    def script(self):
        return self._cli._script("kafka-console-consumer.sh")

    def _worker(self, _, node):
        self._stopping.clear()
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

            for line in node.account.ssh_capture(' '.join(cmd)):
                line.strip()
                self.logger.debug(f"consumed: '{line}'")

                if self._save_msgs:
                    self._messages.append(line)

                if self._stopping.is_set():
                    break

        except:
            if self._stopping.is_set():
                # Expect a non-zero exit code when killing during teardown
                pass
            else:
                raise
        finally:
            self.done = True

    def wait_for_messages(self, messages, timeout=30):
        wait_until(lambda: len(self._messages) >= messages,
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

    def allocate_nodes(self):
        if self.use_custom_node:
            return
        else:
            return super(KafkaCliConsumer, self).allocate_nodes()

    def free(self):
        if self.use_custom_node:
            return
        else:
            return super(KafkaCliConsumer, self).free_all()
