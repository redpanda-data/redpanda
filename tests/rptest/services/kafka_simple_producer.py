# Copyright 2021 Redpanda Data, Inc.
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
from typing import Optional


class KafkaSimpleProducer(BackgroundThreadService):
    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 runtime: int,
                 compression: Optional[str] = None):
        super(KafkaSimpleProducer, self).__init__(context, num_nodes=1)

        self._redpanda = redpanda
        self._topic = topic
        self._runtime = runtime
        self._compression = compression
        self._stopping = threading.Event()
        self.record_count = 0
        self.done = False

    def _worker(self, _, node):
        self._stopping.clear()
        try:
            find_class_path = '$(mvn -q exec:exec -Dexec.executable=echo -Dexec.args="%classpath"):target/simple-producer-1.0-SNAPSHOT.jar'
            cmd = f'cd /opt/simple-producer && java -cp {find_class_path} com.redpanda.simpleproducer.App'
            cmd += f' {self._redpanda.brokers()} {self._topic} {self._runtime}'

            if self._compression is not None:
                cmd += f' {self._compression}'

            for line in node.account.ssh_capture(cmd):
                line.strip()
                self.logger.debug(line)

                # Example output: Sent record 112962
                m = re.match(r'Sent record ([0-9]+)$', line)
                if m is not None:
                    self.record_count += 1

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

    def stop_node(self, node):
        self._stopping.set()
        node.account.kill_process("java", clean_shutdown=False)
