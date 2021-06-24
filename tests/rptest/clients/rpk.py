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

    def create_topic(self, topic, partitions=1):
        cmd = ["create", topic]
        cmd += ["--partitions", str(partitions)]
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
        return self._run_topic(cmd, stdin=msg)

    def describe_topic(self, topic):
        cmd = ['describe', topic]
        output = self._run_topic(cmd)
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

    def wasm_list(self):
        cmd = [
            self._rpk_binary(), 'wasm', 'list', '--raw', '--brokers',
            self._redpanda.brokers()
        ]
        return self._execute(cmd)

    def _run_topic(self, cmd, stdin=None, timeout=30):
        cmd = [
            self._rpk_binary(), "topic", "--brokers",
            self._redpanda.brokers()
        ] + cmd
        return self._execute(cmd, stdin=stdin, timeout=timeout)

    def cluster_info(self, timeout=30):
        # Matches against `rpk cluster info`'s output & parses the brokers'
        # ID & address. Example:
        #
        #  Redpanda Cluster Info
        #
        #  1 (192.168.52.1:9092)  (No partitions)
        #
        #  2 (192.168.52.2:9092)  (No partitions)
        #
        #  3 (192.168.52.3:9092)  (No partitions)
        #
        def _parse_out(line):
            m = re.match(r" *(?P<id>\d) \((?P<addr>.+\:[0-9]+)\) *", line)
            if m is None:
                return None

            return RpkClusterInfoNode(id=int(m.group('id')),
                                      address=m.group('addr'))

        cmd = [
            self._rpk_binary(), 'cluster', 'info', '--brokers',
            self._redpanda.brokers(1)
        ]
        output = self._execute(cmd, stdin=None, timeout=timeout)
        parsed = map(_parse_out, output.splitlines())
        return [p for p in parsed if p is not None]

    def _execute(self, cmd, stdin=None, timeout=30):
        self._redpanda.logger.debug("Executing command: %s", cmd)
        try:
            output = None
            f = None
            if stdin:
                f = subprocess.PIPE
                if isinstance(stdin, str):
                    # Convert the string msg to bytes
                    stdin = stdin.encode()

                f = tempfile.TemporaryFile()
                f.write(stdin)
                f.seek(0)

            p = subprocess.Popen(cmd,
                                 stdout=subprocess.PIPE,
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

            output = p.stdout.read()

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
