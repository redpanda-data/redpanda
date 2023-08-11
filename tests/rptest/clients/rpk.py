# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import subprocess
import re
import typing
import time
import itertools
import os
from collections import namedtuple
from typing import Iterator, Optional
from ducktape.cluster.cluster import ClusterNode
from rptest.clients.types import TopicSpec
from rptest.util import wait_until_result
from rptest.services import tls
from ducktape.errors import TimeoutError
from dataclasses import dataclass

DEFAULT_TIMEOUT = 30

DEFAULT_PRODUCE_TIMEOUT = 5


class ClusterAuthorizationError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class RpkException(Exception):
    def __init__(self, msg, stderr="", returncode=None, stdout=""):
        self.msg = msg
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        # Useful for when its desired to still propogate parsed stdout
        # to caller when when rpk exits 1
        self.parsed_output = None

    def __str__(self):
        if self.stderr:
            err = f" error: {self.stderr}"
        else:
            err = ""
        if self.returncode:
            retcode = f" returncode: {self.returncode}"
        else:
            retcode = ""
        if self.stdout:
            stdout = f" stdout: {self.stdout}"
        else:
            stdout = ""
        return f"RpkException<{self.msg}{err}{stdout}{retcode}>"


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
    total_lag: int
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


class RpkOffsetDeleteResponsePartition(typing.NamedTuple):
    topic: str
    partition: int
    status: str
    error_msg: str


class RpkTrimOffsetResponse(typing.NamedTuple):
    topic: str
    partition: int
    new_start_offset: int
    error_msg: str


@dataclass
class RpkColumnHeader:
    name: str
    padding: int = 0

    def width(self):
        return len(self.name) + self.padding


@dataclass
class RpkTable:
    columns: list[RpkColumnHeader]
    rows: list[list[str]]


def parse_rpk_table(out):
    lines = out.splitlines()

    h_idx = 0
    for line in lines:
        m = re.match("^\(.+\)$", line)
        if not m:
            break
        h_idx += 1

    header = lines[h_idx]

    seen_names = set()
    columns = []
    while len(header) > 0:
        m = re.match("^([^ ]+)( *)", header)
        if not m:
            raise RpkException(f"can't parse header: '{lines[0]}'")
        columns.append(RpkColumnHeader(m.group(1), len(m.group(2))))
        header = header[columns[-1].width():]
        if columns[-1].name in seen_names:
            raise RpkException(
                f"rpk table have duplicated column: '{columns[-1].name}'")
        seen_names.add(columns[-1].name)

    rows = []
    for line in lines[h_idx + 1:]:
        row = []
        position = 0
        for column in columns:
            value = None
            if column == columns[-1]:
                if position < len(line):
                    value = line[position:]
                else:
                    value = ""
            else:
                if position + column.width() < len(line):
                    if line[position + column.width() - 1] != ' ':
                        raise RpkException(
                            f"can't parse '{line}': value at {column.name} must end with padding"
                        )
                    value = line[position:position + column.width()]
                elif position >= len(line):
                    value = ""
                else:
                    value = line[position:]
                value = value.rstrip()
                position += column.width()
            row.append(value)
        rows.append(row)

    return RpkTable(columns, rows)


class RpkTool:
    """
    Wrapper around rpk.
    """
    def __init__(self,
                 redpanda,
                 username: str = None,
                 password: str = None,
                 sasl_mechanism: str = None,
                 tls_cert: Optional[tls.Certificate] = None,
                 tls_enabled: Optional[bool] = None):
        self._redpanda = redpanda
        self._username = username
        self._password = password
        self._sasl_mechanism = sasl_mechanism
        self._tls_cert = tls_cert
        self._tls_enabled = tls_enabled

        # if testing redpanda cloud, override with default superuser
        if hasattr(redpanda, 'GLOBAL_CLOUD_CLUSTER_CONFIG'):
            self._username, self._password, self._sasl_mechanism = redpanda._superuser
            self._tls_enabled = True

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
                self._redpanda.logger.debug(str(e))
                if "Kafka replied that the controller broker is -1" in str(e):
                    return False
                elif "SASL authentication failed" in str(e):
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

    def _sasl_create_user_cmd(self, new_username, new_password, mechanism):
        cmd = ["acl", "user", "create", new_username]
        cmd += ["--api-urls", self._redpanda.admin_endpoints()]
        cmd += ["--mechanism", mechanism]

        if new_password != "":
            cmd += ["-p", new_password]

        return cmd

    def sasl_create_user(self,
                         new_username,
                         new_password="",
                         mechanism="SCRAM-SHA-256"):
        cmd = self._sasl_create_user_cmd(new_username, new_password, mechanism)

        return self._run(cmd)

    def sasl_create_user_basic(self,
                               new_username,
                               auth_user="",
                               auth_password="",
                               new_password="",
                               mechanism="SCRAM-SHA-256"):
        cmd = self._sasl_create_user_cmd(new_username, new_password, mechanism)
        cmd += ["--user", auth_user, "--password", auth_password]

        return self._run(cmd)

    def sasl_update_user(self, user, new_password):
        cmd = [
            "acl", "user", "update", user, "--new-password", new_password,
            "-X", "admin.hosts=" + self._redpanda.admin_endpoints()
        ]
        return self._run(cmd)

    def delete_topic(self, topic):
        cmd = ["delete", topic]
        output = self._run_topic(cmd)
        table = parse_rpk_table(output)
        expected_columns = ["TOPIC", "STATUS"]
        expected = ",".join(expected_columns)
        found = ",".join(map(lambda x: x.name, table.columns))
        if expected != found:
            raise RpkException(f"expected: {expected}; found: {found}")

        if len(table.rows) != 1:
            raise RpkException(f"expected one row; found {len(table.rows)}")

        if table.rows[0][1] != "OK":
            raise RpkException(f"status isn't ok: {table.rows[0][1]}")

        if table.rows[0][0] != topic:
            raise RpkException(
                f"output topic {table.rows[0][0]} doesn't match input topic {topic}"
            )

        return True

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
                timeout=None,
                compression_type=TopicSpec.COMPRESSION_NONE):

        if timeout is None:
            # For produce, we use a lower timeout than the general
            # default, because tests generally call this when
            # they expect a system to be ready.
            timeout = DEFAULT_PRODUCE_TIMEOUT

        cmd = [
            'produce', '--key', key, '-z', f'{compression_type}',
            '--delivery-timeout', f'{timeout}s', '-f', '%v', topic
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

    def describe_topic(self,
                       topic: str,
                       tolerant: bool = False) -> Iterator[RpkPartition]:
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
        def int_or_none(value):
            m = re.match("^-?\d+$", value)
            if m:
                return int(value)
            return None

        cmd = ['describe', topic, '-p']
        output = self._run_topic(cmd)
        table = parse_rpk_table(output)

        expected_columns = set([
            "PARTITION", "LEADER", "EPOCH", "REPLICAS", "LOG-START-OFFSET",
            "HIGH-WATERMARK", "LAST-STABLE-OFFSET"
        ])
        received_columns = set()

        for column in table.columns:
            if column.name not in expected_columns:
                self._redpanda.logger.error(
                    f"Unexpected column: {column.name}")
                raise RpkException(f"Unexpected column: {column.name}")
            received_columns.add(column.name)

        missing_columns = expected_columns - received_columns
        # sometimes LSO is present, sometimes it isn't
        # same is true for EPOCH:
        # https://github.com/redpanda-data/redpanda/issues/8381#issuecomment-1403051606
        missing_columns = missing_columns - {"LAST-STABLE-OFFSET", "EPOCH"}

        if len(missing_columns) != 0:
            missing_columns = ",".join(missing_columns)
            self._redpanda.logger.error(f"Missing columns: {missing_columns}")
            raise RpkException(f"Missing columns: {missing_columns}")

        for row in table.rows:
            obj = dict()
            obj["LAST-STABLE-OFFSET"] = "-"
            obj["EPOCH"] = "-1"
            for i in range(0, len(table.columns)):
                obj[table.columns[i].name] = row[i]

            obj["PARTITION"] = int(obj["PARTITION"])
            obj["LEADER"] = int(obj["LEADER"])
            obj["EPOCH"] = int(obj["EPOCH"])
            m = re.match("^\[(.+)\]$", obj["REPLICAS"])
            if m:
                obj["REPLICAS"] = list(map(int, m.group(1).split(" ")))
            else:
                obj["REPLICAS"] = None
            obj["LOG-START-OFFSET"] = int_or_none(obj["LOG-START-OFFSET"])
            obj["HIGH-WATERMARK"] = int_or_none(obj["HIGH-WATERMARK"])
            obj["LAST-STABLE-OFFSET"] = int_or_none(obj["LAST-STABLE-OFFSET"])

            initialized = obj["LEADER"] >= 0 and obj["EPOCH"] >= 0 and obj[
                "REPLICAS"] != None and obj["LOG-START-OFFSET"] != None and obj[
                    "LOG-START-OFFSET"] >= 0 and obj[
                        "HIGH-WATERMARK"] != None and obj["HIGH-WATERMARK"] >= 0

            partition = RpkPartition(id=obj["PARTITION"],
                                     leader=obj["LEADER"],
                                     leader_epoch=obj["EPOCH"],
                                     replicas=obj["REPLICAS"],
                                     lso=None,
                                     hw=obj["HIGH-WATERMARK"],
                                     start_offset=obj["LOG-START-OFFSET"])

            if initialized or tolerant:
                yield partition

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
        lines = out.splitlines()
        lines = list(map(lambda x: x.strip(), lines))
        if len(lines) != 2:
            raise RpkException(
                f"Unexpected output, expected two lines, got {len(lines)} on setting {topic} {set_key}={set_value}"
            )
        if not re.match("^TOPIC\\s+STATUS$", lines[0]):
            raise RpkException(
                f"Unexpected output, expected 'TOPIC\\s+STATUS' got '{lines[0]}' on setting {topic} {set_key}={set_value}"
            )
        if not re.match(f"^{topic}\\s+OK$", lines[1]):
            raise RpkException(
                f"Unexpected output, expected '{topic}\\s+OK' got '{lines[1]}' on setting {topic} {set_key}={set_value}"
            )

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
                quiet=False,
                format=None,
                timeout=None):
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
            cmd += ["-o", f"{offset}"]
        if partition is not None:
            cmd += ["-p", f"{partition}"]
        if quiet:
            cmd += ["-f", "_\\n"]
        elif format is not None:
            cmd += ["-f", format]

        return self._run_topic(cmd, timeout=timeout)

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
                if "COORDINATOR_NOT_AVAILABLE" in e.msg + e.stderr:
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
            total_lag = parse_field("TOTAL-LAG", lines[5])
            partitions = []
            for l in lines[6:]:
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
                            partitions=partitions,
                            total_lag=int(total_lag))

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

    def cluster_recovery_start(self, wait: bool = True, polling_interval='5s'):
        cmd = [
            self._rpk_binary(), '--api-urls',
            self._admin_host(), 'cluster', 'storage', 'recovery', 'start'
        ]
        if wait:
            cmd.append('--wait')
            if polling_interval:
                cmd.append('--polling-interval')
                cmd.append(polling_interval)
        return self._execute(cmd)

    def self_test_start(self,
                        disk_duration_ms=None,
                        network_duration_ms=None,
                        only_disk=False,
                        only_network=False,
                        node_ids=None):
        cmd = [
            self._rpk_binary(), '--api-urls',
            self._admin_host(), 'cluster', 'self-test', 'start', '--no-confirm'
        ]
        if disk_duration_ms is not None:
            cmd += ['--disk-duration-ms', str(disk_duration_ms)]
        if network_duration_ms is not None:
            cmd += ['--network-duration-ms', str(network_duration_ms)]
        if only_disk is True:
            cmd += ['--only-disk-test']
        if only_network is True:
            cmd += ['--only-network-test']
        if node_ids is not None:
            ids = ",".join([str(x) for x in node_ids])
            cmd += ['--participants-node-ids', ids]
        return self._execute(cmd)

    def self_test_stop(self):
        cmd = [
            self._rpk_binary(), '--api-urls',
            self._admin_host(), 'cluster', 'self-test', 'stop'
        ]
        return self._execute(cmd)

    def self_test_status(self, output_format='json'):
        cmd = [
            self._rpk_binary(), '--api-urls',
            self._admin_host(), 'cluster', 'self-test', 'status', '--format',
            output_format
        ]
        output = self._execute(cmd)
        return json.loads(output) if output_format == 'json' else output

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
            return ",".join([
                f"{n.account.hostname}:9644"
                for n in self._redpanda.started_nodes()
            ])
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

    def _execute(self, cmd, stdin=None, timeout=None, log_cmd=True, env=None):
        if timeout is None:
            timeout = DEFAULT_TIMEOUT

        # Unconditionally enable verbose logging
        cmd += ['-v']

        if log_cmd:
            self._redpanda.logger.debug("Executing command: %s", cmd)

        if env is not None:
            env.update(os.environ.copy())

        p = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stdin=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             env=env,
                             text=True)
        try:
            output, stderror = p.communicate(input=stdin, timeout=timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            output, stderr = p.communicate()
            raise RpkException(
                f"command {' '.join(cmd)} timed out, output: {output} \n error: {stderr}",
                stderr, None, output)

        self._redpanda.logger.debug(f'\n{output}')

        if p.returncode:
            self._redpanda.logger.error(stderror)
            raise RpkException(
                'command %s returned %d, output: %s' %
                (' '.join(cmd) if log_cmd else '[redacted]', p.returncode,
                 output), stderror, p.returncode, output)
        else:
            # Send the verbose output of rpk to debug logger
            self._redpanda.logger.debug(f"\n{stderror}")

        return output

    def _rpk_binary(self):
        # NOTE: since this runs on separate nodes from the service, the binary
        # path used by each node may differ from that returned by
        # redpanda.find_binary(), e.g. if using a RedpandaInstaller.
        rp_install_path_root = self._redpanda._context.globals.get(
            "rp_install_path_root", None)
        return f"{rp_install_path_root}/bin/rpk"

    def cluster_maintenance_enable(self, node, wait=False):
        node_id = self._redpanda.node_id(node) if isinstance(
            node, ClusterNode) else node
        cmd = [
            self._rpk_binary(), "--api-urls",
            self._admin_host(), "cluster", "maintenance", "enable",
            str(node_id)
        ]
        if wait:
            cmd.append("--wait")
        return self._execute(cmd)

    def cluster_maintenance_disable(self, node):
        node_id = self._redpanda.node_id(node) if isinstance(
            node, ClusterNode) else node
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
            "-X",
            "brokers=" + self._redpanda.brokers(),
        ]
        if self._username:
            flags += [
                "-X",
                "user=" + self._username,
                "-X",
                "pass=" + self._password,
                "-X",
                "sasl.mechanism=" + self._sasl_mechanism,
            ]
        if self._tls_cert:
            flags += [
                "-X",
                "tls.key=" + self._tls_cert.key,
                "-X",
                "tls.cert=" + self._tls_cert.crt,
                "-X",
                "tls.ca=" + self._tls_cert.ca.crt,
            ]
        if self._tls_enabled:
            flags += [
                "-X",
                "tls.enabled=" + str(self._tls_enabled),
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
        table = parse_rpk_table(output)
        expected_columns = [
            "PRINCIPAL", "HOST", "RESOURCE-TYPE", "RESOURCE-NAME",
            "RESOURCE-PATTERN-TYPE", "OPERATION", "PERMISSION", "ERROR"
        ]

        expected = ",".join(expected_columns)
        found = ",".join(map(lambda x: x.name, table.columns))
        if expected != found:
            raise RpkException(f"expected: {expected}; found: {found}")

        if len(table.rows) != 1:
            raise RpkException(f"expected one row; found {len(table.rows)}")

        if table.rows[0][-1] != "":
            raise RpkException(
                f"acl_create_allow_cluster failed with {table.rows[0][-1]}")

        return output

    def cluster_metadata_id(self):
        """
        Calls 'cluster metadata' and returns the cluster ID, if set,
        else None.  Don't return the rest of the output (brokers list)
        because there are already other ways to get at that.
        """
        cmd = [
            self._rpk_binary(), '-X', "brokers=" + self._redpanda.brokers(),
            'cluster', 'metadata'
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
            self._rpk_binary(), "-X", "admin.hosts=" + self._admin_host(),
            "cluster", "license", "set"
        ]

        if license:
            cmd += [license]
        if path:
            cmd += ["--path", path]

        return self._execute(cmd, log_cmd=False)

    def license_info(self):

        cmd = [
            self._rpk_binary(), "--api-urls",
            self._admin_host(), "cluster", "license", "info", "--format",
            "json"
        ]

        return self._execute(cmd)

    def offset_delete(self, group, topic_partitions):
        # rpk group offset-delete expects the topic-partition input data as a file

        def parse_offset_delete_output(output):
            regex = re.compile(
                r"\s*(?P<topic>\S*)\s*(?P<partition>\d*)\s*(?P<status>\w+):?(?P<error>.*)"
            )
            matched = [regex.match(x) for x in output]
            failed_matches = any([x for x in matched if x is None])
            if failed_matches:
                raise RuntimeError("Failed to parse offset-delete output")
            return [
                RpkOffsetDeleteResponsePartition(x['topic'],
                                                 int(x['partition']),
                                                 x['status'], x['error'])
                for x in matched
            ]

        def try_offset_delete(retries=5):
            while retries > 0:
                try:
                    output = self._execute(cmd)
                    return parse_offset_delete_output(output.splitlines())
                except RpkException as e:
                    if e.returncode != 1:
                        raise e
                    if not 'NOT_COORDINATOR' in str(e):
                        e.parsed_output = parse_offset_delete_output(
                            e.stdout.splitlines())
                        raise e
                    retries -= 1
            raise RpkException("Max number of retries exceeded")

        # First convert partitions from integers to strings
        as_strings = {
            k: ",".join([str(x) for x in v])
            for k, v in topic_partitions.items()
        }

        # Group each kv pair to string item like '<topic>:p1,p2,p3'
        request_args = [f"{x}:{y}" for x, y in as_strings.items()]

        # Append each arg with the -t (topic) flag
        # interleaves a list of -t strings with each argument producing
        # [-t, arg1, -t arg2, ... , -t argn]
        request_args_w_flags = list(
            itertools.chain(
                *zip(["-t"
                      for _ in range(0, len(request_args))], request_args)))

        # Execute the rpk group offset-delete command
        cmd = [
            self._rpk_binary(),
            "--brokers",
            self._redpanda.brokers(),
            "group",
            "offset-delete",
            group,
        ] + request_args_w_flags

        # Retry if NOT_COORDINATOR is observed when command exits 1
        return try_offset_delete(retries=5)

    def generate_grafana(self, dashboard):

        cmd = [
            self._rpk_binary(), "generate", "grafana-dashboard", "--dashboard",
            dashboard
        ]

        return self._execute(cmd)

    def describe_log_dirs(self):
        cmd = [
            self._rpk_binary(), "--brokers",
            self._redpanda.brokers(), "cluster", "logdirs", "describe"
        ]
        output = self._execute(cmd)
        lines = output.split("\n")

        DescribeLogDirItem = namedtuple(
            'DescribeLogDirItem',
            ['broker', 'dir', 'topic', 'partition', 'size'])

        result = []
        for l in lines[1:]:
            l = l.strip()
            if not l:
                continue

            tokens = l.split()
            # The line format depends on --aggregate-into: we are just using
            # the default non-aggregated output
            try:
                broker, dir, topic, partition, size = tokens[0:5]
            except:
                self._redpanda.logger.warn(f"Unexpected line format: '{l}'")
                raise

            result.append(
                DescribeLogDirItem(broker, dir, topic, int(partition),
                                   int(size)))

        return result

    def trim_prefix(self, topic, offset, partitions=[]):
        def parse(line):
            if line.startswith("Request error") or not line.strip():
                # RPK may print messages about request errors, which it internally
                # retries.  Drop these lines.
                return None

            # $ rpk topic trim-prefix foo -p 0 -o 100
            # TOPIC             PARTITION  NEW-START-OFFSET  ERROR
            # foo               0          100

            line = [x.strip() for x in line.split()]
            if line[0] == "TOPIC":
                return None
            return RpkTrimOffsetResponse(
                topic=line[0],
                partition=int(line[1]),
                new_start_offset=int(line[2]) if line[2] != "-" else -1,
                error_msg=line[3] if len(line) > 3 else "")

        cmd = [
            self._rpk_binary(),
            "topic",
            "trim-prefix",
            topic,
            "--offset",
            str(offset),
            "--no-confirm",
            "-X",
            "brokers=" + self._redpanda.brokers(),
        ]

        if len(partitions) > 0:
            cmd += ["--partitions", ",".join([str(x) for x in partitions])]

        output = self._execute(cmd)
        return list(filter(None, map(parse, output.splitlines())))

    def plugin_list(self):
        cmd = [self._rpk_binary(), "plugin", "list", "--local"]

        return self._execute(cmd)

    def cloud_byoc_aws_apply(self, redpanda_id, token, extra_flags=[]):
        envs = {
            "RPK_CLOUD_SKIP_VERSION_CHECK": "true",
            "RPK_CLOUD_TOKEN": token
        }

        cmd = [
            self._rpk_binary(), "cloud", "byoc", "aws", "apply",
            "--redpanda-id", redpanda_id
        ]

        if len(extra_flags) > 0:
            cmd += extra_flags

        out = self._execute(cmd, env=envs)
        return json.loads(out)
