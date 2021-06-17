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


class RpkPartition:
    def __init__(self, id, leader, replicas, hw):
        self.id = id
        self.leader = leader
        self.replicas = replicas
        self.high_watermark = hw

    def __str__(self):
        return "id: {}, leader: {}, replicas: {}, hw: {}".format(
            self.id, self.leader, self.replicas, self.high_watermark)


class RpkTool:
    """
    Wrapper around rpk.
    """
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def create_topic(self, topic, partitions=1):
        cmd = ["topic", "create", topic]
        cmd += ["--partitions", str(partitions)]
        return self._run_api(cmd)

    def list_topics(self):
        cmd = ["topic", "list"]

        output = self._run_api(cmd)
        if "No topics found." in output:
            return []

        def topic_line(line):
            parts = line.split()
            assert len(parts) == 3
            return parts[0]

        lines = output.splitlines()
        for i, line in enumerate(lines):
            if line.split() == ["Name", "Partitions", "Replicas"]:
                return map(topic_line, lines[i + 1:])

        assert False, "Unexpected output format"

    def produce(self, topic, key, msg, headers=[], partition=None):
        cmd = [
            'produce', '--brokers',
            self._redpanda.brokers(), '--key', key, topic
        ]
        if headers:
            cmd += ['-H ' + h for h in headers]
        if partition:
            cmd += ['-p', str(partition)]
        return self._run_api(cmd, stdin=msg)

    def describe_topic(self, topic):
        cmd = ['topic', 'describe', topic]
        output = self._run_api(cmd)
        if "not found" in output:
            return None
        lines = output.splitlines()

        def partition_line(line):
            m = re.match(
                r" *(?P<id>\d+) +(?P<leader>\d+) +\[(?P<replicas>.+?)\] + \[.+\] +(?P<hw>\d+) *",
                line)
            if m == None:
                return None
            replicas = map(lambda r: int(r), m.group('replicas').split())
            return RpkPartition(id=int(m.group('id')),
                                leader=int(m.group('leader')),
                                replicas=replicas,
                                hw=int(m.group('hw')))

        return filter(lambda p: p != None, map(partition_line, lines))

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

    def _run_api(self, cmd, stdin=None, timeout=30):
        cmd = [
            self._rpk_binary(), "api", "--brokers",
            self._redpanda.brokers(1)
        ] + cmd
        return self._execute(cmd, stdin=stdin, timeout=timeout)

    def _execute(self, cmd, stdin=None, timeout=30):
        self._redpanda.logger.debug("Executing command: %s", cmd)
        try:
            output = None
            f = subprocess.PIPE

            if stdin:
                f = tempfile.TemporaryFile()
                f.write(stdin)
                f.seek(0)

            # rpk logs everything on STDERR by default
            p = subprocess.Popen(cmd,
                                 stderr=subprocess.PIPE,
                                 stdin=f,
                                 text=True)
            start_time = time.time()

            ret = None
            while time.time() < start_time + timeout:
                ret = p.poll()
                if ret != None:
                    break
                time.sleep(0.5)

            if ret is None:
                p.terminate()

            output = p.stderr.read()

            if p.returncode:
                raise Exception('command %s returned %d, output: %s' %
                                (' '.join(cmd), p.returncode, output))

            self._redpanda.logger.debug(output)
            return output
        except subprocess.CalledProcessError as e:
            self._redpanda.logger.debug("Error (%d) executing command: %s",
                                        e.returncode, e.output)

    def _rpk_binary(self):
        return self._redpanda.find_binary("rpk")
