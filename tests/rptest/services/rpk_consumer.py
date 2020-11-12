# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
from ducktape.services.background_thread import BackgroundThreadService


class RpkConsumer(BackgroundThreadService):
    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 partitions=[],
                 offset='oldest',
                 group='',
                 commit=False):
        super(RpkConsumer, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._partitions = partitions
        self._offset = offset
        self._group = group
        self._commit = commit
        self.done = False
        self.messages = []
        self.error = None
        self.offset = dict()

    def _worker(self, idx, node):
        try:
            self.messages = self._consume(node)
        except Exception as e:
            self.error = e
            raise
        finally:
            self.done = True

    def stop_node(self, node):
        node.account.kill_process('rpk', clean_shutdown=False)

    def _consume(self, node):
        cmd = '%s api consume --offset %s --pretty-print=false --brokers %s %s' % (
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
            self.logger.debug(l)
            if not l:
                continue
            msg = json.loads(l)
            self.messages.append(msg)
