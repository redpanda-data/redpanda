# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import sys
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.cluster.remoteaccount import RemoteCommandError
from threading import Event


class KafProducer(BackgroundThreadService):
    def __init__(self, context, redpanda, topic, num_records=sys.maxsize):
        super(KafProducer, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._num_records = num_records
        self._stopping = Event()
        self._output_line_count = 0

    def _worker(self, _idx, node):
        cmd = f"echo $$ ; for (( i=0; i < {self._num_records}; i++ )) ; do export KEY=key-$(printf %08d $i) ; export VALUE={self.value_gen()} ; echo $VALUE | kaf produce -b {self._redpanda.brokers()} --key $KEY {self._topic} ; done"

        self._stopping.clear()
        self._pid = None
        try:
            out_iter = node.account.ssh_capture(cmd, timeout_sec=10)
            for line in out_iter:
                if self._pid is None:
                    # Take first line as pid
                    self._pid = line.strip()
                    self._redpanda.logger.debug(
                        f"Spawned remote shell {self._pid}")
                    continue
                else:
                    self._output_line_count += 1
                    self.logger.debug(line.rstrip())
        except RemoteCommandError:
            if self._stopping.is_set():
                pass
            else:
                raise

    @property
    def output_line_count(self):
        return self._output_line_count

    def stop_node(self, node):
        self._stopping.set()
        try:
            if self._pid is not None:
                node.account.signal(self._pid, 9, allow_fail=True)
            node.account.kill_process("kaf", clean_shutdown=False)
        except RemoteCommandError as e:
            if b"No such process" in e.msg:
                pass
            else:
                raise

    def clean_node(self, nodes):
        pass

    def value_gen(self):
        return "record-$(printf %08d $i)"
