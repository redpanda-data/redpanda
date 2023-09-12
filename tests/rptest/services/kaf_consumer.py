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


class KafConsumer(BackgroundThreadService):
    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 num_records=None,
                 offset_for_read="newest"):
        super(KafConsumer, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._num_records = num_records
        self._stopping = threading.Event()
        self.done = False
        self.offset = dict()
        self._offset_for_read = offset_for_read
        self._pid = None

    def _worker(self, _, node):
        self._stopping.clear()
        try:
            partition = None
            cmd = "echo $$ ; kaf consume -b %s %s --offset %s %s" % (
                self._redpanda.brokers(), "--follow" if self._num_records
                is None else f"--limit-messages {self._num_records}",
                self._offset_for_read, self._topic)
            for line in node.account.ssh_capture(cmd):
                if self._pid is None:
                    self._pid = line.strip()

                if self._stopping.is_set():
                    break

                m = re.match("Partition:\s+(?P<partition>\d+)", line)
                if m:
                    assert partition is None
                    partition = int(m.group("partition"))
                    continue

                m = re.match("Offset:\s+(?P<offset>\d+)", line)
                if m:
                    assert partition is not None
                    offset = int(m.group("offset"))
                    self.offset[partition] = offset
                    partition = None
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
        if self._pid is not None:
            self.logger.debug("Killing pid %s" % {self._pid})
            node.account.signal(self._pid, 9, allow_fail=True)
        else:
            node.account.kill_process("kaf", clean_shutdown=False)
