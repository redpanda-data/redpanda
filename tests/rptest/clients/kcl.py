# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import namedtuple
import re
import subprocess
import time

KclPartitionOffset = namedtuple(
    'KclPartitionOffset',
    ['broker', 'topic', 'partition', 'start_offset', 'end_offset', 'error'])

KclPartitionEpochEndOffset = namedtuple('KclPartitionEpochEndOffset', [
    'broker', 'topic', 'partition', 'leader_epoch', 'epoch_end_offset', 'error'
])


class KCL:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def list_topics(self):
        return self._cmd(['topic', 'list'])

    def list_groups(self):
        return self._cmd(["group", "list"])

    def produce(self, topic, msg):
        return self._cmd(["produce", topic], input=msg)

    def offset_for_leader_epoch(self,
                                topics,
                                leader_epoch,
                                current_leader_epoch=None):
        cmd = ['misc', 'offset-for-leader-epoch']
        if isinstance(topics, list):
            cmd += topics
        else:
            cmd += [topics]
        cmd += ['-e', str(leader_epoch)]
        if current_leader_epoch:
            cmd += ['-c', str(current_leader_epoch)]
        lines = self._cmd(cmd).splitlines()
        ret = []
        for l in lines:
            m = re.match(
                r" *(?P<broker>\d+) +(?P<topic>.+?) +(?P<partition>\d+) +(?P<epoch>-?\d*?) +(?P<end_offset>-?\d*?) +(?P<error>.*) *",
                l)
            if m:
                ret.append(
                    KclPartitionEpochEndOffset(
                        m['broker'], m['topic'], int(m['partition']),
                        int(m['epoch']) if m['epoch'] is not None else -1,
                        int(m['end_offset'])
                        if m['end_offset'] is not None else -1, m['error']))
        return ret

    def list_offsets(self, topics):
        cmd = ['misc', 'list-offsets']
        if isinstance(topics, list):
            cmd += topics
        else:
            cmd += [topics]

        lines = self._cmd(cmd).splitlines()
        ret = []
        for l in lines:
            m = re.match(
                r" *(?P<broker>\d+) +(?P<topic>.+?) +(?P<partition>\d+) +(?P<start>-?\d*?) +(?P<end>-?\d*?) +(?P<error>.*) *",
                l)
            if m:
                ret.append(
                    KclPartitionOffset(m['broker'], m['topic'],
                                       int(m['partition']),
                                       int(m['start']) if m['start'] else -1,
                                       int(m['end']) if m['end'] else -1,
                                       m['error']))
        return ret

    def consume(self,
                topic,
                n=None,
                group=None,
                regex=False,
                fetch_max_bytes=None):
        cmd = ["consume"]
        if group is not None:
            cmd += ["-g", group]
        if n is not None:
            cmd.append(f"-n{n}")
        if regex:
            cmd.append("-r")
        if fetch_max_bytes is not None:
            cmd += ["--fetch-max-bytes", str(fetch_max_bytes)]
        cmd.append(topic)
        return self._cmd(cmd)

    def alter_broker_config(self, values, incremental, broker=None):
        """
        :param broker: node id.  Not supported in redpanda but used for testing error handling.
        :param values: dict of property name to new value
        :param incremental: if true, use incremental kafka APIs
        :return:
        """
        cmd = ["admin", "configs", "alter", "-tb"]
        if incremental:
            cmd.append("-i")
        for k, v in values.items():
            cmd.extend(["-k", f"s:{k}={v}" if incremental else f"{k}={v}"])

        if broker:
            # cmd needs to be string, so handle things like broker=1
            cmd.append(str(broker))

        return self._cmd(cmd, attempts=1)

    def delete_broker_config(self, keys, incremental):
        """
        :param keys: list of key names to clear
        :param incremental: if true, use incremental kafka APIs
        :return:
        """
        cmd = ["admin", "configs", "alter", "-tb"]
        if incremental:
            cmd.append("-i")
        for k in keys:
            cmd.extend(["-k", f"d:{k}" if incremental else k])

        return self._cmd(cmd, attempts=1)

    def _cmd(self, cmd, input=None, attempts=5):
        """

        :param attempts: how many times to try before giving up (1 for no retries)
        :return: stdout string
        """
        brokers = self._redpanda.brokers()
        cmd = ["kcl", "-X", f"seed_brokers={brokers}", "--no-config-file"
               ] + cmd
        assert attempts > 0
        for retry in reversed(range(attempts)):
            try:
                res = subprocess.check_output(cmd,
                                              text=True,
                                              input=input,
                                              stderr=subprocess.STDOUT)
                self._redpanda.logger.debug(res)
                return res
            except subprocess.CalledProcessError as e:
                if retry == 0:
                    raise
                self._redpanda.logger.debug(
                    "kcl retrying after exit code {}: {}".format(
                        e.returncode, e.output))
                time.sleep(1)
        # it looks impossible to reach this case, but pyright static analyzer
        # can't see that and deduces Optional[str] as return type.
        raise RuntimeError(f"Command failed after retries: {cmd}")
