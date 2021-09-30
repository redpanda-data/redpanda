# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import tempfile
import time
import re

DEFAULT_TIMEOUT = 30


class RpkException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return f"RpkException<{self.msg}>"


class RpkPartition:
    def __init__(self, id, leader, replicas, hw):
        self.id = id
        self.leader = leader
        self.replicas = replicas
        self.high_watermark = hw

    def __str__(self):
        return "id: {}, leader: {}, replicas: {}, hw: {}".format(
            self.id, self.leader, self.replicas, self.high_watermark)


class RpkClusterInfoNode:
    def __init__(self, id, address):
        self.id = id
        self.address = address


class RpkTool:
    """
    Wrapper around rpk.
    """
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def create_topic(self, topic, partitions=None):
        cmd = ["create", topic]
        if partitions is not None:
            cmd += ["--partitions", str(partitions)]
        return self._run_topic(cmd)

    def delete_topic(self, topic):
        cmd = ["delete", topic]
        return self._run_topic(cmd)

    def list_topics(self):
        cmd = ["list"]

        output = self._run_topic(cmd)
        if "No topics found." in output:
            return []

        def topic_line(line):
            parts = line.split()
            assert len(parts) == 3
            return parts[0]

        lines = output.splitlines()
        for i, line in enumerate(lines):
            if line.split() == ["NAME", "PARTITIONS", "REPLICAS"]:
                return map(topic_line, lines[i + 1:])

        assert False, "Unexpected output format"

    def produce(self,
                topic,
                key,
                msg,
                headers=[],
                partition=None,
                timeout=None):
        cmd = [
            'produce', '--brokers',
            self._redpanda.brokers(), '--key', key, topic
        ]
        if headers:
            cmd += ['-H ' + h for h in headers]
        if partition is not None:
            cmd += ['-p', str(partition)]
        out = self._run_topic(cmd, stdin=msg, timeout=timeout)

        offset = re.search("at offset (\d+)", out).group(1)
        return int(offset)

    def describe_topic(self, topic):
        cmd = ['describe', topic]
        output = self._run_topic(cmd)
        if "not found" in output:
            return None
        lines = output.splitlines()

        def partition_line(line):
            m = re.match(
                r" *(?P<id>\d+) +(?P<leader>\d+) +\[(?P<replicas>.+?)\] +(?P<hw>\d+) *",
                line)
            if m == None:
                return None
            replicas = map(lambda r: int(r), m.group('replicas').split())
            return RpkPartition(id=int(m.group('id')),
                                leader=int(m.group('leader')),
                                replicas=replicas,
                                hw=int(m.group('hw')))

        return filter(lambda p: p != None, map(partition_line, lines))

    def alter_topic_config(self, topic, set_key, set_value):
        cmd = ['alter-config', topic, "--set", f"{set_key}={set_value}"]
        self._run_topic(cmd)

    def wasm_deploy(self, script, name, description):
        cmd = [
            self._rpk_binary(), 'wasm', 'deploy', script, '--brokers',
            self._redpanda.brokers(), '--name', name, '--description',
            description
        ]
        return self._execute(cmd)

    def wasm_remove(self, name):
        cmd = ['wasm', 'remove', name, '--brokers', self._redpanda.brokers()]
        return self._execute(cmd)

    def wasm_gen(self, directory):
        cmd = [self._rpk_binary(), 'wasm', 'generate', directory]
        return self._execute(cmd)

    def _run_topic(self, cmd, stdin=None, timeout=None):
        cmd = [
            self._rpk_binary(), "topic", "--brokers",
            self._redpanda.brokers()
        ] + cmd
        return self._execute(cmd, stdin=stdin, timeout=timeout)

    def cluster_info(self, timeout=None):
        # Matches against `rpk cluster info`'s output & parses the brokers'
        # ID & address. Example:

        # BROKERS
        # =======
        # ID    HOST  PORT
        # 1*    n1    9092
        # 2     n2    9092
        # 3     n3    9092

        def _parse_out(line):
            m = re.match(
                r"^(?P<id>\d+)[\s*]+(?P<host>[^\s]+)\s+(?P<port>\d+)$",
                line.strip())
            if m is None:
                return None

            address = f"{m.group('host')}:{m.group('port')}"

            return RpkClusterInfoNode(id=int(m.group('id')), address=address)

        cmd = [
            self._rpk_binary(), 'cluster', 'info', '--brokers',
            self._redpanda.brokers(1)
        ]
        output = self._execute(cmd, stdin=None, timeout=timeout)
        parsed = map(_parse_out, output.splitlines())
        return [p for p in parsed if p is not None]

    def _execute(self, cmd, stdin=None, timeout=None):
        if timeout is None:
            timeout = DEFAULT_TIMEOUT

        self._redpanda.logger.debug("Executing command: %s", cmd)
        f = None
        if stdin:
            if isinstance(stdin, str):
                # Convert the string msg to bytes
                stdin = stdin.encode()

            f = tempfile.TemporaryFile()
            f.write(stdin)
            f.seek(0)

        # rpk logs everything on STDERR by default
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=f, text=True)
        start_time = time.time()

        ret = None
        while time.time() < start_time + timeout:
            ret = p.poll()
            if ret != None:
                break
            time.sleep(0.5)

        if ret is None:
            p.terminate()
            raise RpkException(f"command {' '.join(cmd)} timed out")

        output = p.stdout.read()
        self._redpanda.logger.debug(output)

        if p.returncode:
            raise RpkException('command %s returned %d, output: %s' %
                               (' '.join(cmd), p.returncode, output))

        return output

    def _rpk_binary(self):
        return self._redpanda.find_binary("rpk")
