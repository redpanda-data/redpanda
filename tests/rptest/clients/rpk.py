# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import re
import typing
from ducktape.cluster.cluster import ClusterNode

DEFAULT_TIMEOUT = 30

DEFAULT_PRODUCE_TIMEOUT = 5


class ClusterAuthorizationError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class RpkException(Exception):
    def __init__(self, msg, stderr=""):
        self.msg = msg
        self.stderr = stderr

    def __str__(self):
        if self.stderr:
            err = f" error: {self.stderr}"
        else:
            err = ""
        return f"RpkException<{self.msg}{err}>"


class RpkPartition:
    def __init__(self, id, leader, leader_epoch, replicas, hw, start_offset):
        self.id = id
        self.leader = leader
        self.leader_epoch = leader_epoch
        self.replicas = replicas
        self.high_watermark = hw
        self.start_offset = start_offset

    def __str__(self):
        return "id: {}, leader: {}, leader_epoch: {} replicas: {}, hw: {}, start_offset: {}".format(
            self.id, self.leader, self.leader_epoch, self.replicas,
            self.high_watermark, self.start_offset)

    def __eq__(self, other):
        if other is None:
            return False
        return self.id == other.id and self.leader == other.leader \
            and self.leader_epoch == other.leader_epoch \
            and self.replicas == other.replicas \
            and self.high_watermark == other.high_watermark \
            and self.start_offset == other.start_offset


class RpkGroupPartition(typing.NamedTuple):
    topic: str
    partition: int
    current_offset: int
    log_end_offset: int
    lag: int
    member_id: str
    client_id: str
    host: str


class RpkGroup(typing.NamedTuple):
    name: str
    coordinator: int
    state: str
    balancer: str
    members: int
    partitions: list[RpkGroupPartition]


class RpkClusterInfoNode:
    def __init__(self, id, address):
        self.id = id
        self.address = address


class RpkMaintenanceStatus(typing.NamedTuple):
    node_id: int
    draining: bool
    finished: bool
    errors: bool
    partitions: int
    eligible: int
    transferring: int
    failed: int


class RpkTool:
    """
    Wrapper around rpk.
    """
    def __init__(self,
                 redpanda,
                 username: str = None,
                 password: str = None,
                 sasl_mechanism: str = None):
        self._redpanda = redpanda
        self._username = username
        self._password = password
        self._sasl_mechanism = sasl_mechanism

    def create_topic(self, topic, partitions=1, replicas=None, config=None):
        cmd = ["create", topic]
        cmd += ["--partitions", str(partitions)]
        if replicas is not None:
            cmd += ["--replicas", str(replicas)]
        if config is not None:
            cfg = [f"{k}:{v}" for k, v in config.items()]
            for it in cfg:
                cmd += ["--topic-config", it]
        output = self._run_topic(cmd)
        self._check_stdout_success(output)
        return output

    def _check_stdout_success(self, output):
        """
        Helper for topic operations where rpk does not surface errors
        in return codes
        (https://github.com/redpanda-data/redpanda/issues/3397)
        """

        lines = output.strip().split("\n")
        status_line = lines[1]
        if not status_line.endswith("OK"):
            raise RpkException(f"Bad status: '{status_line}'")

    def sasl_allow_principal(self, principal, operations, resource,
                             resource_name, username, password, mechanism):
        if resource == "topic":
            resource = "--topic"
        elif resource == "transactional-id":
            resource = "--transactional-id"
        else:
            raise Exception(f"unknown resource: {resource}")

        cmd = [
            "acl", "create", "--allow-principal", principal, "--operation",
            ",".join(operations), resource, resource_name, "--brokers",
            self._redpanda.brokers(), "--user", username, "--password",
            password, "--sasl-mechanism", mechanism
        ]
        return self._run(cmd)

    def sasl_create_user(self, new_username, new_password, mechanism):
        cmd = ["acl", "user", "create", new_username, "-p", new_password]
        cmd += ["--api-urls", self._redpanda.admin_endpoints()]
        cmd += ["--sasl-mechanism", mechanism]
        return self._run(cmd)

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

        m = re.search(r"at offset (\d+)", out)
        assert m, f"Reported offset not found in: {out}"
        return int(m.group(1))

    def describe_topic(self, topic):
        cmd = ['describe', topic, '-p']
        output = self._run_topic(cmd)
        if "not found" in output:
            raise Exception(f"Topic not found: {topic}")
        lines = output.splitlines()

        def partition_line(line):
            m = re.match(
                r" *(?P<id>\d+) +(?P<leader>\d+) +(?P<epoch>\d+) +\[(?P<replicas>.+?)\] +(?P<logstart>\d+?) +(?P<hw>\d+) *",
                line)
            if m == None:
                return None
            replicas = list(map(lambda r: int(r), m.group('replicas').split()))
            return RpkPartition(id=int(m.group('id')),
                                leader=int(m.group('leader')),
                                leader_epoch=int(m.group('epoch')),
                                replicas=replicas,
                                hw=int(m.group('hw')),
                                start_offset=int(m.group("logstart")))

        return filter(None, map(partition_line, lines))

    def describe_topic_configs(self, topic):
        cmd = ['describe', topic, '-c']
        output = self._run_topic(cmd)
        assert "not found" not in output, \
                f"Cannot describe configs for unknown topic {topic}"
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

    def add_topic_partitions(self, topic, additional):
        cmd = ['add-partitions', topic, '--num', str(additional)]
        output = self._run_topic(cmd)
        self._check_stdout_success(output)
        return output

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

    def group_describe(self, group):
        def parse_field(field_name, string):
            pattern = re.compile(f" *{field_name} +(?P<value>.+)")
            m = pattern.match(string)
            assert m is not None, f"Field string '{string}' does not match the pattern"
            return m['value']

        partition_pattern = re.compile(
            "(?P<topic>.+) +(?P<partition>\d+) +(?P<offset>\d+) +(?P<log_end>\d+) +(?P<lag>\d+) *(?P<member_id>.*)? *(?P<client_id>.*)? *(?P<host>.*)?"
        )

        def parse_partition(string):
            m = partition_pattern.match(string)
            assert m is not None, f"Partition string '{string}' does not match the pattern"
            return RpkGroupPartition(topic=m['topic'],
                                     partition=int(m['partition']),
                                     current_offset=int(m['offset']),
                                     log_end_offset=int(m['log_end']),
                                     lag=int(m['lag']),
                                     member_id=m['member_id'],
                                     client_id=m['client_id'],
                                     host=m['host'])

        cmd = ["describe", group]
        out = self._run_group(cmd)
        lines = out.splitlines()

        group_name = parse_field("GROUP", lines[0])
        coordinator = parse_field("COORDINATOR", lines[1])
        state = parse_field("STATE", lines[2])
        balancer = parse_field("BALANCER", lines[3])
        members = parse_field("MEMBERS", lines[4])
        partitions = []
        for l in lines[5:]:
            #skip header line
            if l.startswith("TOPIC") or len(l) < 2:
                continue
            p = parse_partition(l)
            partitions.append(p)

        return RpkGroup(name=group_name,
                        coordinator=int(coordinator),
                        state=state,
                        balancer=balancer,
                        members=int(members),
                        partitions=partitions)

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
        cmd = [
            self._rpk_binary(), 'wasm', 'generate', '--skip-version', directory
        ]
        return self._execute(cmd)

    def _run_topic(self, cmd, stdin=None, timeout=None):
        cmd = [self._rpk_binary(), "topic"] + self._kafka_conn_settings() + cmd
        return self._execute(cmd, stdin=stdin, timeout=timeout)

    def _run_group(self, cmd, stdin=None, timeout=None):
        cmd = [self._rpk_binary(), "group"] + self._kafka_conn_settings() + cmd
        return self._execute(cmd, stdin=stdin, timeout=timeout)

    def _run(self, cmd, stdin=None, timeout=None):
        cmd = [self._rpk_binary()] + cmd
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
            self._redpanda.brokers()
        ]
        output = self._execute(cmd, stdin=None, timeout=timeout)
        parsed = map(_parse_out, output.splitlines())
        return [p for p in parsed if p is not None]

    def _admin_host(self, node=None):
        if node is None:
            return ",".join(
                [f"{n.account.hostname}:9644" for n in self._redpanda.nodes])
        else:
            return f"{node.account.hostname}:9644"

    def admin_config_print(self, node):
        return self._execute([
            self._rpk_binary(), "redpanda", "admin", "config", "print",
            "--host",
            self._admin_host(node)
        ])

    def cluster_config_export(self, file, all):
        cmd = [
            self._rpk_binary(), '--api-urls',
            self._admin_host(), "cluster", "config", "export", "--filename",
            file
        ]
        if all:
            cmd.append("--all")
        return self._execute(cmd)

    def cluster_config_import(self, file, all):
        cmd = [
            self._rpk_binary(), "--api-urls",
            self._admin_host(), "cluster", "config", "import", "--filename",
            file
        ]
        if all:
            cmd.append("--all")
        return self._execute(cmd)

    def cluster_config_status(self):
        cmd = [
            self._rpk_binary(), "--api-urls",
            self._admin_host(), "cluster", "config", "status"
        ]
        return self._execute(cmd)

    def cluster_config_get(self, key):
        cmd = [
            self._rpk_binary(), "--api-urls",
            self._admin_host(), "cluster", "config", "get", key
        ]
        return self._execute(cmd).strip()

    def cluster_config_set(self, key, value):
        cmd = [
            self._rpk_binary(), "--api-urls",
            self._admin_host(), "cluster", "config", "set", key, value
        ]
        return self._execute(cmd)

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
            self._redpanda.logger.error(error)
            raise RpkException(
                'command %s returned %d, output: %s' %
                (' '.join(cmd), p.returncode, output), error)

        return output

    def _rpk_binary(self):
        return self._redpanda.find_binary("rpk")

    def cluster_maintenance_enable(self, node, wait=False):
        node_id = self._redpanda.idx(node) if isinstance(node,
                                                         ClusterNode) else node
        cmd = [
            self._rpk_binary(), "--api-urls",
            self._admin_host(), "cluster", "maintenance", "enable",
            str(node_id)
        ]
        if wait:
            cmd.append("--wait")
        return self._execute(cmd)

    def cluster_maintenance_disable(self, node):
        node_id = self._redpanda.idx(node) if isinstance(node,
                                                         ClusterNode) else node
        cmd = [
            self._rpk_binary(), "--api-urls",
            self._admin_host(), "cluster", "maintenance", "disable",
            str(node_id)
        ]
        return self._execute(cmd)

    def cluster_maintenance_status(self):
        """
        Run `rpk cluster maintenance status` and return the parsed results.
        """
        def parse(line):
            # jerry@garcia:~/src/redpanda$ rpk cluster maintenance status
            # NODE-ID  DRAINING  FINISHED  ERRORS  PARTITIONS  ELIGIBLE  TRANSFERRING  FAILED
            # 1        false     false     false   0           0         0             0
            line = line.split()
            assert len(
                line
            ) == 8, f"`rpk cluster maintenance status` has changed: {line}"
            line = [x.strip() for x in line]
            if line[0] == "NODE-ID":
                return None
            return RpkMaintenanceStatus(node_id=int(line[0]),
                                        draining=line[1] == "true",
                                        finished=line[2] == "true",
                                        errors=line[3] == "true",
                                        partitions=int(line[4]),
                                        eligible=int(line[5]),
                                        transferring=int(line[6]),
                                        failed=int(line[7]))

        cmd = [
            self._rpk_binary(),
            "--api-urls",
            self._admin_host(),
            "cluster",
            "maintenance",
            "status",
        ]

        output = self._execute(cmd)
        return list(filter(None, map(parse, output.splitlines())))

    def _kafka_conn_settings(self):
        flags = [
            "--brokers",
            self._redpanda.brokers(),
        ]
        if self._username:
            flags += [
                "--user",
                self._username,
                "--password",
                self._password,
                "--sasl-mechanism",
                self._sasl_mechanism,
            ]
        return flags
