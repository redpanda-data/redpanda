# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import time
import threading
from ducktape.services.background_thread import BackgroundThreadService


class RpkConsumer(BackgroundThreadService):
    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 partitions=[],
                 offset='oldest',
                 ignore_errors=True,
                 retries=3,
                 group='',
                 commit=False):
        super(RpkConsumer, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._partitions = partitions
        self._offset = offset
        self._ignore_errors = ignore_errors
        self._retries = retries
        self._group = group
        self._commit = commit
        self._stopping = threading.Event()
        self.done = False
        self.messages = []
        self.error = None
        self.offset = dict()

    def _worker(self, idx, node):
        retry_sec = 5
        err = None

        self._stopping.clear()
        attempt = 0
        while attempt <= self._retries and not self._stopping.is_set():
            try:
                self._consume(node)
            except Exception as e:
                if self._stopping.is_set():
                    break

                err = e
                self._redpanda.logger.error(
                    f"Consumer failed with error: '{e}'. Retrying in {retry_sec} seconds."
                )
                attempt += 1
                self._stopping.wait(retry_sec)
                time.sleep(retry_sec)
            else:
                err = None

        self.done = True
        if err is not None:
            self.error = err

    def stop_node(self, node):
        self._redpanda.logger.info(
            f"Stopping RpkConsumer on ({node.account.hostname})")
        self._stopping.set()
        node.account.kill_process('rpk', clean_shutdown=False)

    def _consume(self, node):
        cmd = '%s topic consume --offset %s --pretty-print=false --brokers %s %s' % (
            self._redpanda.find_binary('rpk'),
            self._offset,
            self._redpanda.brokers(),
            self._topic,
        )

        if self._group:
            cmd += ' -g %s' % self._group

        if self._partitions:
            cmd += ' -p %s' % ','.join([str(p) for p in self._partitions])

        if self._commit:
            cmd += ' --commit'

        for line in node.account.ssh_capture(cmd):
            if self._stopping.is_set():
                break

            l = line.rstrip()
            if not l:
                continue

            msg = json.loads(l)
            self.messages.append(msg)

    def clean_node(self, nodes):
        pass
