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
import time
from typing import Optional
from ducktape.cluster.cluster import ClusterNode
from rptest.util import wait_until_result
from rptest.services import tls
from ducktape.errors import TimeoutError

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
    def __init__(self, id, leader, leader_epoch, replicas, lso, hw,
                 start_offset):
        self.id = id
        self.leader = leader
        self.leader_epoch = leader_epoch
        self.replicas = replicas
        self.last_stable_offset = lso
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
    current_offset: Optional[int]
    log_end_offset: Optional[int]
    lag: Optional[int]
    member_id: str
    instance_id: str
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
                 sasl_mechanism: str = None,
                 tls_cert: Optional[tls.Certificate] = None):
        self._redpanda = redpanda
        self._username = username
        self._password = password
        self._sasl_mechanism = sasl_mechanism
        self._tls_cert = tls_cert

    def create_topic(self, topic, partitions=1, replicas=None, config=None):
        def create_topic():
            try:
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
                return (True, output)
            except RpkException as e:
                if "Kafka replied that the controller broker is -1" in str(e):
                    return False
                raise e

        try:
            wait_until_result(create_topic,
                              10,
                              0.1,
                              err_msg="Can't create a topic within 10s")
        except TimeoutError:
            raise RpkException("rpk couldn't create topic within 10s timeout")

    def add_partitions(self, topic, partitions):
        cmd = ["add-partitions", topic, "-n", str(partitions)]
        return self._run_topic(cmd)

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

    def describe_topic(self, topic: str, tolerant: bool = False):
        """
        By default this will omit any partitions which do not have full
        metadata in the response: this means that if we are unlucky and a
        partition returns NOT_LEADER due to a leadership transfer while
        we query offsets, it will be missing.  To be more forgiving, pass
        tolerant=true

        :param topic: topic name
        :param tolerant: if true, RpkPartition results may be included with some
                         fields set to None, as long as the leader field is present.
        :return:
        """
        cmd = ['describe', topic, '-p']
        output = self._run_topic(cmd)
        if "not found" in output:
            raise Exception(f"Topic not found: {topic}")
        lines = output.splitlines()[1:]

        def partition_line(line):
            m = re.match(
                r" *(?P<id>\d+) +(?P<leader>\d+) +(?P<epoch>\d+) +\[(?P<replicas>.+?)\] +(?P<logstart>\d+?) +(?P<lso>\d+?)? +(?P<hw>\d+) *",
                line)
            if m is None and tolerant:
                m = re.match(r" *(?P<id>\d+) +(?P<leader>\d+) .*", line)
                if m is None:
                    self._redpanda.logger.info(f"No match on '{line}'")
                    return None

                return RpkPartition(id=int(m.group('id')),
                                    leader=int(m.group('leader')),
                                    leader_epoch=None,
                                    replicas=None,
                                    lso=None,
                                    hw=None,
                                    start_offset=None)

            elif m is None:
                return None
            elif m:
                replicas = list(
                    map(lambda r: int(r),
                        m.group('replicas').split()))
                return RpkPartition(
                    id=int(m.group('id')),
                    leader=int(m.group('leader')),
                    leader_epoch=int(m.group('epoch')),
                    replicas=replicas,
                    # lso is not always returned by describe.
                    lso=int(m.group('lso')) if m.group('lso') else 0,
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
        out = self._run_topic(cmd)
        if 'INVALID' in out:
            raise RpkException(
                f"Invalid topic config {topic} {set_key}={set_value}")

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
                partition=None,
                fetch_max_bytes=None,
                quiet=False):
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
        if partition is not None:
            cmd += ["-p", f"{partition}"]
        if quiet:
            cmd += ["-f", "_\\n"]

        return self._run_topic(cmd)

    def group_seek_to(self, group, to):
        cmd = ["seek", group, "--to", to]
        self._run_group(cmd)

    def group_describe(self, group, summary=False):
        def parse_field(field_name, string):
            pattern = re.compile(f" *{field_name} +(?P<value>.+)")
            m = pattern.match(string)
            assert m is not None, f"Field string '{string}' does not match the pattern"
            return m['value']

        static_member_pattern = re.compile("^([^\s]+\s+){8}[^\s]+$")

        partition_pattern_static_member = re.compile(
            "(?P<topic>[^\s]+) +(?P<partition>\d+) +(?P<offset>\d+|-) +(?P<log_end>\d+|-) +(?P<lag>-?\d+|-) *(?P<member_id>[^\s]*)? *(?P<instance_id>[^\s]*) *(?P<client_id>[^\s]*)? *(?P<host>[^\s]*)?"
        )
        partition_pattern_dynamic_member = re.compile(
            "(?P<topic>[^\s]+) +(?P<partition>\d+) +(?P<offset>\d+|-) +(?P<log_end>\d+|-) +(?P<lag>-?\d+|-) *(?P<member_id>[^\s]*)? *(?P<client_id>[^\s]*)? *(?P<host>[^\s]*)?"
        )

        def check_lines(lines):
            for line in lines:
                # UNKNOWN_TOPIC_OR_PARTITION: This server doesn't contain this partition or topic.
                # We should wait until server will get information about it.
                if line.find('UNKNOWN_TOPIC_OR_PARTITION') != -1:
                    return False

                # Leadership movements are underway
                if 'NOT_LEADER_FOR_PARTITION' in line:
                    return False

                # Cluster not ready yet
                if 'unknown broker' in line:
                    return False

                if "missing from list offsets" in line:
                    return False

            return True

        def parse_partition(string):

            pattern = partition_pattern_dynamic_member
            if static_member_pattern.match(string):
                pattern = partition_pattern_static_member
            m = pattern.match(string)

            # Check to see if info for the partition was queried during a change in leadership.
            # if it was we'd expect to see a partition string ending in;
            # NOT_LEADER_FOR_PARTITION: This server is not the leader for that topic-partition.
            if m is None and string.find('NOT_LEADER_FOR_PARTITION') != -1:
                return None

            # Account for negative numbers and '-' value
            all_digits = lambda x: x.lstrip('-').isdigit()

            offset = int(m['offset']) if all_digits(m['offset']) else None
            log_end = int(m['log_end']) if all_digits(m['log_end']) else None
            lag = int(m['lag']) if all_digits(m['lag']) else None

            return RpkGroupPartition(topic=m['topic'],
                                     partition=int(m['partition']),
                                     current_offset=offset,
                                     log_end_offset=log_end,
                                     lag=lag,
                                     member_id=m['member_id'],
                                     instance_id=m.groupdict().get(
                                         'instance_id', None),
                                     client_id=m['client_id'],
                                     host=m['host'])

        def try_describe_group(group):
            if summary:
                cmd = ["describe", "-s", group]
            else:
                cmd = ["describe", group]

            try:
                out = self._run_group(cmd)
            except RpkException as e:
                if "COORDINATOR_NOT_AVAILABLE" in e.msg:
                    # Transient, return None to retry
                    return None
                elif "NOT_COORDINATOR" in e.msg:
                    # Transient, retry
                    return None
                elif "broker replied that group" in e.msg:
                    # Transient, return None to retry
                    # e.g. broker replied that group repeat01 has broker coordinator 8, but did not reply with that broker in the broker list
                    return None
                elif "connection refused" in e.msg:
                    # Metadata directed us to a broker that is uncontactable, perhaps
                    # it was just stopped.  Retry should succeed once metadata updates.
                    return None
                else:
                    raise

            lines = out.splitlines()

            if not check_lines(lines):
                return None

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
                # p is None if the leader of the partition has changed.
                if p is None:
                    return None

                partitions.append(p)

            return RpkGroup(name=group_name,
                            coordinator=int(coordinator),
                            state=state,
                            balancer=balancer,
                            members=int(members),
                            partitions=partitions)

        attempts = 10
        rpk_group = None
        # try to wait for leadership to stabilize.
        while rpk_group is None and attempts != 0:
            rpk_group = try_describe_group(group)
            attempts = attempts - 1
            # give time for redpanda to end its election
            if rpk_group is None:
                time.sleep(0.5)

        assert rpk_group is not None, "Unable to determine group within set number of attempts"

        return rpk_group

    def group_seek_to_group(self, group, to_group):
        cmd = ["seek", group, "--to-group", to_group]
        self._run_group(cmd)

    def group_seek_to_file(self, group, file):
        cmd = ["seek", group, "--to-file", file]
        self._run_group(cmd)

    def group_delete(self, group):
        cmd = ["delete", group]
        self._run_group(cmd)

    def group_list(self):
        cmd = ['list']
        out = self._run_group(cmd)

        return [l.split()[1] for l in out.splitlines()[1:]]

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

    def cluster_config_set(self, key: str, value):
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

        self._redpanda.logger.debug(f'\n{output}')

        if p.returncode:
            self._redpanda.logger.error(error)
            raise RpkException(
                'command %s returned %d, output: %s' %
                (' '.join(cmd), p.returncode, output), error)

        return output

    def _rpk_binary(self):
        # NOTE: since this runs on separate nodes from the service, the binary
        # path used by each node may differ from that returned by
        # redpanda.find_binary(), e.g. if using a RedpandaInstaller.
        rp_install_path_root = self._redpanda._context.globals.get(
            "rp_install_path_root", None)
        return f"{rp_install_path_root}/bin/rpk"

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
            if line.startswith("Request error") or not line.strip():
                # RPK may print messages about request errors, which it internally
                # retries.  Drop these lines.
                return None

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
        if self._tls_cert:
            flags += [
                "--tls-key",
                self._tls_cert.key,
                "--tls-cert",
                self._tls_cert.crt,
                "--tls-truststore",
                self._tls_cert.ca.crt,
            ]
        return flags

    def acl_list(self):
        """
        Run `rpk acl list` and return the results.

        If this client is not authorized to list ACLs then
        ClusterAuthorizationError will be raised.

        TODO: parse output into acl structures. currently ducktape tests lack
        any structured representation of acls. however at the time of writing we
        are interested only in an authz success/fail signal.
        """
        cmd = [
            self._rpk_binary(),
            "acl",
            "list",
        ] + self._kafka_conn_settings()

        output = self._execute(cmd)

        if "CLUSTER_AUTHORIZATION_FAILED" in output:
            raise ClusterAuthorizationError("acl list")

        return output

    def acl_create_allow_cluster(self, username, op):
        """
        Add allow+describe+cluster ACL
        """
        cmd = [
            self._rpk_binary(),
            "acl",
            "create",
            "--allow-principal",
            f"User:{username}",
            "--operation",
            op,
            "--cluster",
        ] + self._kafka_conn_settings()
        output = self._execute(cmd)
        return output

    def cluster_metadata_id(self):
        """
        Calls 'cluster metadata' and returns the cluster ID, if set,
        else None.  Don't return the rest of the output (brokers list)
        because there are already other ways to get at that.
        """
        cmd = [
            self._rpk_binary(), '--brokers',
            self._redpanda.brokers(), 'cluster', 'metadata'
        ]
        output = self._execute(cmd)
        lines = output.strip().split("\n")

        # Output is like:
        #
        # CLUSTER
        # =======
        # foobar

        # Output only has a "CLUSTER" section if cluster id is set
        if lines[0] != "CLUSTER":
            return None
        else:
            return lines[2]

    def license_set(self, path, license=""):
        cmd = [
            self._rpk_binary(), "--api-urls",
            self._admin_host(), "cluster", "license", "set"
        ]

        if license:
            cmd += [license]
        if path:
            cmd += ["--path", path]

        return self._execute(cmd)

    def license_info(self):

        cmd = [
            self._rpk_binary(), "--api-urls",
            self._admin_host(), "cluster", "license", "info", "--format",
            "json"
        ]

        return self._execute(cmd)
