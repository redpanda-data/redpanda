# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import socket
import re

DEFAULT_TIMEOUT = 30

DEFAULT_PRODUCE_TIMEOUT = 5


class RpkException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return f"RpkException<{self.msg}>"


class RpkPartition:
    def __init__(self, id, leader, replicas, hw, start_offset):
        self.id = id
        self.leader = leader
        self.replicas = replicas
        self.high_watermark = hw
        self.start_offset = start_offset

    def __str__(self):
        return "id: {}, leader: {}, replicas: {}, hw: {}, start_offset: {}".format(
            self.id, self.leader, self.replicas, self.high_watermark,
            self.start_offset)


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

    def create_topic(self, topic, partitions=1, replicas=None, config=None):
        cmd = ["create", topic]
        cmd += ["--partitions", str(partitions)]
        if replicas is not None:
            cmd += ["--replicas", str(replicas)]
        if config is not None:
            cfg = [f"{k}:{v}" for k, v in config.items()]
            for it in cfg:
                cmd += ["--topic-config", it]
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

        if timeout is None:
            # For produce, we use a lower timeout than the general
            # default, because tests generally call this when
            # they expect a system to be ready.
            timeout = DEFAULT_PRODUCE_TIMEOUT

        cmd = [
            'produce', '--key', key, '-z', 'none', '--delivery-timeout',
            f'{timeout}s', '-f', '%v', topic
        ]
        if headers:
            cmd += ['-H ' + h for h in headers]
        if partition is not None:
            cmd += ['-p', str(partition)]

        # Run remote process with a slightly higher timeout than the
        # rpk delivery timeout, so that we get a clean-ish rpk timeout
        # message rather than sigkilling the remote process.
        out = self._run_topic(cmd, stdin=msg, timeout=timeout + 0.5)

        offset = re.search("at offset (\d+)", out).group(1)
        return int(offset)

    def describe_topic(self, topic):
        cmd = ['describe', topic, '-p']
        output = self._run_topic(cmd)
        if "not found" in output:
            return None
        lines = output.splitlines()

        def partition_line(line):
            m = re.match(
                r" *(?P<id>\d+) +(?P<leader>\d+) +\[(?P<replicas>.+?)\] +(?P<logstart>\d+?) +(?P<hw>\d+) *",
                line)
            if m == None:
                return None
            replicas = list(map(lambda r: int(r), m.group('replicas').split()))
            return RpkPartition(id=int(m.group('id')),
                                leader=int(m.group('leader')),
                                replicas=replicas,
                                hw=int(m.group('hw')),
                                start_offset=int(m.group("logstart")))

        return filter(lambda p: p != None, map(partition_line, lines))

    def describe_topic_configs(self, topic):
        cmd = ['describe', topic, '-c']
        output = self._run_topic(cmd)
        if "not found" in output:
            return None
        lines = output.splitlines()
        res = {}
        for line in lines:
            try:
                key, value, source = line.split()
                if key == "KEY":
                    continue
                res[key] = value, source
            except:
                pass
        return res

    def alter_topic_config(self, topic, set_key, set_value):
        cmd = ['alter-config', topic, "--set", f"{set_key}={set_value}"]
        self._run_topic(cmd)

    def delete_topic_config(self, topic, key):
        cmd = ['alter-config', topic, "--delete", key]
        self._run_topic(cmd)

    def consume(self,
                topic,
                n=None,
                group=None,
                regex=False,
                offset=None,
                fetch_max_bytes=None):
        cmd = ["consume", topic]
        if group is not None:
            cmd += ["-g", group]
        if n is not None:
            cmd.append(f"-n{n}")
        if regex:
            cmd.append("-r")
        if fetch_max_bytes is not None:
            cmd += ["--fetch-max-bytes", str(fetch_max_bytes)]
        if offset is not None:
            cmd += ["-o", f"{n}"]
        return self._run_topic(cmd)

    def group_seek_to(self, group, to):
        cmd = ["seek", group, "--to", to]
        self._run_group(cmd)

    def group_seek_to_group(self, group, to_group):
        cmd = ["seek", group, "--to-group", to_group]
        self._run_group(cmd)

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

    def _run_group(self, cmd, stdin=None, timeout=None):
        cmd = [
            self._rpk_binary(), "group", "--brokers",
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

    def admin_config_print(self, node):
        return self._execute([
            self._rpk_binary(), "redpanda", "admin", "config", "print",
            "--host", f"{node.account.hostname}:9644"
        ])

    def _execute(self, cmd, stdin=None, timeout=None):
        if timeout is None:
            timeout = DEFAULT_TIMEOUT

        self._redpanda.logger.debug("Executing command: %s", cmd)

        p = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stdin=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             text=True)
        try:
            output, error = p.communicate(input=stdin, timeout=timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            raise RpkException(f"command {' '.join(cmd)} timed out")

        self._redpanda.logger.debug(output)

        if p.returncode:
            raise RpkException(
                'command %s returned %d, output: %s, error: %s' %
                (' '.join(cmd), p.returncode, output, error))

        return output

    def _rpk_binary(self):
        return self._redpanda.find_binary("rpk")
