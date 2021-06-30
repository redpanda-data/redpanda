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
        self.done = False
        self.messages = []
        self.error = None
        self.offset = dict()

    def _worker(self, idx, node):
        retries = 1
        retry_sec = 5
        err = None
        while retries < self._retries:
            try:
                self._consume(node)
            except Exception as e:
                err = e
                self._redpanda.logger.error(
                    f"Consumer failed with error: '{e}'. Retrying in {retry_sec} seconds."
                )
                retries += 1
                time.sleep(retry_sec)

        self.done = True
        if retries == self._retries and err is not None:
            self.error = err

    def stop_node(self, node):
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
            l = line.rstrip()
            if not l:
                continue

            msg = json.loads(l)
            self.messages.append(msg)

    def clean_node(self, nodes):
        pass
