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
from typing import Any, Iterator, Optional
from ducktape.cluster.cluster import ClusterNode
from rptest.clients.types import TopicSpec
from rptest.services.redpanda_types import SSL_SECURITY, KafkaClientSecurity, check_username_password
from rptest.util import wait_until_result
from rptest.services import tls
from rptest.clients.types import TopicSpec
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
        self.parsed_output: list[RpkOffsetDeleteResponsePartition] | None = None

    def __str__(self):
        if self.stderr:
            err = f"; stderr: {self.stderr}"
        else:
            err = ""
        if self.returncode:
            retcode = f"; returncode: {self.returncode}"
        else:
            retcode = ""
        if self.stdout:
            stdout = f"; stdout: {self.stdout}"
        else:
            stdout = ""
        return f"RpkException<{self.msg}{err}{stdout}{retcode}>"


class RpkPartition:
    def __init__(self,
                 id,
                 leader,
                 leader_epoch,
                 replicas,
                 lso,
                 hw,
                 start_offset,
                 load_error=None):
        self.id = id
        self.leader = leader
        self.leader_epoch = leader_epoch
        self.replicas = replicas
        self.last_stable_offset = lso
        self.high_watermark = hw
        self.start_offset = start_offset
        self.load_error = load_error

    def __str__(self):
        ret = "id: {}, leader: {}, leader_epoch: {} replicas: {}, hw: {}, start_offset: {}".format(
            self.id, self.leader, self.leader_epoch, self.replicas,
            self.high_watermark, self.start_offset)
        if self.load_error:
            ret += f", load_error: `{self.load_error}'"
        return ret

    def __eq__(self, other):
        if other is None:
            return False
        return self.id == other.id and self.leader == other.leader \
            and self.leader_epoch == other.leader_epoch \
            and self.replicas == other.replicas \
            and self.high_watermark == other.high_watermark \
            and self.start_offset == other.start_offset \
            and self.load_error == other.load_error


class RpkGroupPartition(typing.NamedTuple):
    topic: str
    partition: int
    current_offset: Optional[int]
    log_start_offset: Optional[int]
    log_end_offset: Optional[int]
    lag: Optional[int]
    member_id: str
    instance_id: str | None
    client_id: str
    host: str
    error: str | None


class RpkGroup(typing.NamedTuple):
    name: str
    coordinator: int
    state: str
    balancer: str
    members: int
    total_lag: int
    partitions: list[RpkGroupPartition]


class RpkListGroup(typing.NamedTuple):
    broker: str
    group: str
    state: str

    @staticmethod
    def from_line(line: str):
        parts = line.split()
        return RpkListGroup(broker=parts[0], group=parts[1], state=parts[2])


class RpkWasmListProcessorResponse(typing.NamedTuple):
    node_id: int
    partition: int
    # running | inactive | errored | unknown
    status: str
    lag: int


class RpkWasmListResponse(typing.NamedTuple):
    name: str
    input_topic: str
    output_topics: list[str]
    status: list[RpkWasmListProcessorResponse]
    environment: dict[str, str]
    compression: TopicSpec.CompressionTypes


class RpkClusterInfoNode:
    def __init__(self, id, address):
        self.id = id
        self.address = address


class RpkMaintenanceStatus(typing.NamedTuple):
    node_id: int
    enabled: bool
    finished: bool | None
    errors: bool | None
    partitions: int | None
    eligible: int | None
    transferring: int | None
    failed: int | None


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
    return parse_rpk_table_lines(lines)


def parse_rpk_table_lines(lines):
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


AccessControlList = namedtuple(
    'AccessControlList',
    [
        'principal',
        'host',
        'resource',
        'resource_name',
        'resource_pattern_type',
        'operation',
        'permission',
        'error',
    ],
)


class AclList:
    def __init__(self, table: RpkTable):
        self._acls = {
            u[u.find(':') + 1:]:
            [AccessControlList(*r) for r in table.rows if r[0] == u]
            for u in set([r[0] for r in table.rows])
        }

    @classmethod
    def parse_raw(cls, raw: str):
        table = parse_rpk_table(raw)
        return AclList(table)

    def has_permission(self,
                       principal: str,
                       operation: str,
                       resource: str,
                       resource_name: str,
                       permission: str = 'ALLOW'):
        return any([
            l.operation == operation.upper()
            and l.resource == resource.upper()
            and l.resource_name == resource_name
            and l.permission == permission.upper()
            for l in self._acls.get(principal, [])
        ])


class RpkTool:
    """
    Wrapper around rpk.
    """
    def __init__(self,
                 redpanda,
                 username: str | None = None,
                 password: str | None = None,
                 sasl_mechanism: str | None = None,
                 tls_cert: Optional[tls.Certificate] = None,
                 tls_enabled: Optional[bool] = None):
        self._redpanda = redpanda

        check_username_password(username, password)

        sasl_set = any(
            [v is not None for v in (username, password, sasl_mechanism)])

        if tls_cert:
            assert tls_enabled is not False, 'using tls_cert implies tls_enabled'
            tls_enabled = True

        default_security: KafkaClientSecurity = redpanda.kafka_client_security(
        )

        if not sasl_set and tls_cert:
            # By convention, if none of the SASL properties are set and tls_cert
            # is set, we treat this as using mTLS authentication & mapping and so
            # do not merge in in the default SASL credentials.
            self._security = SSL_SECURITY
        else:
            # integrate any provided credentials with the default redpanda ones
            self._security = default_security.override(username, password,
                                                       sasl_mechanism,
                                                       tls_enabled)

        self._tls_cert = tls_cert

    def create_topic(self,
                     topic: str,
                     partitions: int = 1,
                     replicas: int | None = None,
                     config: dict[str, Any] | None = None):
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

    def _sasl_set_principal_access(self,
                                   principal,
                                   operations,
                                   resource,
                                   resource_name,
                                   username: Optional[str] = None,
                                   password: Optional[str] = None,
                                   mechanism: Optional[str] = None,
                                   deny=False,
                                   ptype: str = "principal"):

        username = username if username is not None else self._username
        password = password if password is not None else self._password
        mechanism = mechanism if mechanism is not None else self._sasl_mechanism
        RESOURCES = set([
            'topic',
            'transactional-id',
            'group',
        ])
        if resource in RESOURCES:
            resource = "--" + resource
        else:
            raise Exception(f"unknown resource: {resource}")

        perm = f'--allow-{ptype}' if not deny else f'--deny-{ptype}'

        cmd = [
            "acl", "create", perm, principal, "--operation",
            ",".join(operations), resource, resource_name, "--brokers",
            self._redpanda.brokers(), "--user", username, "--password",
            password, "--sasl-mechanism", mechanism
        ] + self._tls_settings()
        return self._run(cmd)

    def sasl_allow_principal(self, *args, **kwargs):
        return self._sasl_set_principal_access(*args, **kwargs, deny=False)

    def sasl_deny_principal(self, *args, **kwargs):
        return self._sasl_set_principal_access(*args, **kwargs, deny=True)

    def sasl_allow_role(self, *args, **kwargs):
        return self._sasl_set_principal_access(*args,
                                               **kwargs,
                                               deny=False,
                                               ptype="role")

    def sasl_deny_role(self, *args, **kwargs):
        return self._sasl_set_principal_access(*args,
                                               **kwargs,
                                               deny=True,
                                               ptype="role")

    def allow_principal(self, principal, operations, resource, resource_name):
        if resource == "topic":
            resource = "--topic"
        elif resource == "transactional-id":
            resource = "--transactional-id"
        else:
            raise Exception(f"unknown resource: {resource}")

        cmd = [
            "security", "acl", "create", "--allow-principal", principal,
            "--operation", ",".join(operations), resource, resource_name,
            "--brokers",
            self._redpanda.brokers()
        ]
        return self._run(cmd)

    def delete_principal(self, principal, operations, resource, resource_name):
        if resource == "topic":
            resource = "--topic"
        elif resource == "transactional-id":
            resource = "--transactional-id"
        else:
            raise Exception(f"unknown resource: {resource}")

        cmd = [
            "security", "acl", "delete", "--allow-principal", principal,
            "--operation", ",".join(operations), resource, resource_name,
            "--no-confirm"
        ] + self._kafka_conn_settings()
        return self._run(cmd)

    def _sasl_create_user_cmd(self, new_username, new_password, mechanism):
        cmd = ["security", "user", "create", new_username]
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

    def sasl_list_users(self):
        cmd = ["security", "user", "list"]
        cmd += ["--api-urls", self._redpanda.admin_endpoints()]

        return self._run(cmd)

    def sasl_delete_user(self, username):
        cmd = ["acl", "user", "delete", username]
        cmd += ["--api-urls", self._redpanda.admin_endpoints()]

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

    def sasl_create_user_basic_mix(self,
                                   new_username,
                                   auth_user="",
                                   auth_password="",
                                   new_password="",
                                   mechanism="SCRAM-SHA-256"):
        cmd = [
            "acl", "user", "create", new_username, "--password", new_password,
            "--mechanism", mechanism, "-X",
            "admin.hosts=" + self._redpanda.admin_endpoints()
        ]
        cmd += ["-X", "user=" + auth_user, "-X", "pass=" + auth_password]

        return self._run(cmd)

    def sasl_update_user(self, user, new_password, new_mechanism):
        cmd = [
            "acl", "user", "update", user, "--new-password", new_password,
            "--mechanism", new_mechanism, "-X",
            "admin.hosts=" + self._redpanda.admin_endpoints()
        ]
        return self._run(cmd)

    def delete_topic(self, topic: str):
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

    def list_topics(self, detailed=False):
        cmd = ["list"]

        output = self._run_topic(cmd)
        if "No topics found." in output:
            return []

        def topic_line(line):
            parts = line.split()
            assert len(parts) == 3
            return parts[0] if not detailed else parts

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
                compression_type=TopicSpec.COMPRESSION_NONE,
                schema_id=None,
                schema_key_id=None,
                proto_msg=None,
                proto_key_msg=None,
                tombstone=False):

        if timeout is None:
            # For produce, we use a lower timeout than the general
            # default, because tests generally call this when
            # they expect a system to be ready.
            timeout = DEFAULT_PRODUCE_TIMEOUT

        cmd = [
            'produce', '--key', key, '-z', f'{compression_type}',
            '--delivery-timeout', f'{timeout}s', topic
        ]

        # An empty msg needs to be a newline for stdin purposes.
        if msg == '':
            msg = '\n'

        if msg not in ['\n']:
            cmd += ['-f', '%v']

        use_schema_registry = False
        if headers:
            cmd += ['-H ' + h for h in headers]
        if partition is not None:
            cmd += ['-p', str(partition)]
        if schema_id is not None:
            cmd += ["--schema-id", f'{schema_id}']
            use_schema_registry = True
        if schema_key_id is not None:
            cmd += ["--schema-key-id", f'{schema_key_id}']
            use_schema_registry = True
        if proto_msg is not None:
            cmd += ["--schema-type", proto_msg]
            use_schema_registry = True
        if proto_key_msg is not None:
            cmd += ["--schema-key-type", proto_key_msg]
            use_schema_registry = True
        if tombstone:
            cmd += ["--tombstone"]

        # Run remote process with a slightly higher timeout than the
        # rpk delivery timeout, so that we get a clean-ish rpk timeout
        # message rather than sigkilling the remote process.
        out = self._run_topic(cmd,
                              stdin=msg,
                              timeout=timeout + 0.5,
                              use_schema_registry=use_schema_registry)

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
            "HIGH-WATERMARK", "LAST-STABLE-OFFSET", "LOAD-ERROR"
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
        # LOAD-ERROR is not present if there was no error.
        missing_columns = missing_columns - {
            "LAST-STABLE-OFFSET", "EPOCH", "LOAD-ERROR"
        }

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
                                     start_offset=obj["LOG-START-OFFSET"],
                                     load_error=obj.get("LOAD-ERROR"))

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
                timeout=None,
                use_schema_registry=None):
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
        if use_schema_registry is not None:
            cmd += ["--use-schema-registry=" + use_schema_registry]
        elif format is not None:
            cmd += ["-f", format]

        return self._run_topic(cmd,
                               timeout=timeout,
                               use_schema_registry=use_schema_registry
                               is not None)

    def group_seek_to(self, group, to):
        cmd = ["seek", group, "--to", to]
        self._run_group(cmd)

    def group_describe(self, group, summary=False, tolerant=False):
        def parse_field(field_name, string):
            pattern = re.compile(f" *{field_name} +(?P<value>.+)")
            m = pattern.match(string)
            assert m is not None, f"Field string '{string}' does not match the pattern"
            return m['value']

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

            group_name = parse_field("GROUP", lines[0])
            coordinator = parse_field("COORDINATOR", lines[1])
            state = parse_field("STATE", lines[2])
            balancer = parse_field("BALANCER", lines[3])
            members = parse_field("MEMBERS", lines[4])
            total_lag = parse_field("TOTAL-LAG", lines[5])

            # lines[6] can be empty or the ERROR field, skip it either way.
            partition_lines = [l for l in lines[7:] if len(l) > 0]
            partitions = []
            if len(partition_lines) > 0:
                table = parse_rpk_table_lines(partition_lines)

                received_columns = set(c.name for c in table.columns)
                required_columns = set([
                    "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-START-OFFSET",
                    "LOG-END-OFFSET", "LAG", "MEMBER-ID", "CLIENT-ID", "HOST"
                ])
                optional_columns = set(["INSTANCE-ID", "ERROR"])

                missing_columns = required_columns - received_columns
                if len(missing_columns) > 0:
                    raise RpkException(f"Missing columns: {missing_columns}")

                unexpected_columns = received_columns - required_columns - optional_columns
                if len(unexpected_columns) > 0:
                    raise RpkException(
                        f"Unexpected columns: {unexpected_columns}")

                for row in table.rows:
                    obj = dict((table.columns[i].name, row[i])
                               for i in range(len(table.columns)))

                    # Check to see if info for the partition was queried during a change in leadership.
                    error = obj.get("ERROR")
                    if not tolerant and error:
                        error_strs = [
                            # UNKNOWN_TOPIC_OR_PARTITION: This server doesn't contain this partition
                            # or topic. We should wait until server will get information about it.
                            "UNKNOWN_TOPIC_OR_PARTITION",
                            # Leadership movements are underway
                            "NOT_LEADER_FOR_PARTITION",
                            # Cluster not ready yet
                            "unknown broker",
                            # ListOffsets request (needed to calculate lag) errored or was incomplete
                            "missing from list offsets",
                        ]
                        if any(e in error for e in error_strs):
                            return None

                    def maybe_parse_int(field):
                        # Account for negative numbers and '-' value
                        if field.lstrip('-').isdigit():
                            return int(field)
                        return None

                    partition = RpkGroupPartition(
                        topic=obj["TOPIC"],
                        partition=int(obj["PARTITION"]),
                        current_offset=maybe_parse_int(obj["CURRENT-OFFSET"]),
                        log_start_offset=maybe_parse_int(
                            obj["LOG-START-OFFSET"]),
                        log_end_offset=maybe_parse_int(obj["LOG-END-OFFSET"]),
                        lag=maybe_parse_int(obj["LAG"]),
                        member_id=obj["MEMBER-ID"],
                        instance_id=obj.get("INSTANCE-ID"),
                        client_id=obj["CLIENT-ID"],
                        host=obj["HOST"],
                        error=error,
                    )

                    partitions.append(partition)

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

    def group_list(self, states: list[str] = []) -> list[RpkListGroup]:
        cmd = ['list']
        if states:
            cmd += ['--states', ','.join(states)]
        out = self._run_group(cmd)

        return [RpkListGroup.from_line(l) for l in out.splitlines()[1:]]

    def group_list_names(self) -> list[str]:
        return [res.group for res in self.group_list()]

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
                        cloud_timeout_ms=None,
                        cloud_backoff_ms=None,
                        only_disk=False,
                        only_network=False,
                        only_cloud=False,
                        node_ids=None):
        cmd = [
            self._rpk_binary(), '--api-urls',
            self._admin_host(), 'cluster', 'self-test', 'start', '--no-confirm'
        ]
        if disk_duration_ms is not None:
            cmd += ['--disk-duration-ms', str(disk_duration_ms)]
        if network_duration_ms is not None:
            cmd += ['--network-duration-ms', str(network_duration_ms)]
        if cloud_timeout_ms is not None:
            cmd += ['--cloud-timeout-ms', str(cloud_timeout_ms)]
        if cloud_backoff_ms is not None:
            cmd += ['--cloud-backoff-ms', str(cloud_backoff_ms)]
        if only_disk is True:
            cmd += ['--only-disk-test']
        if only_network is True:
            cmd += ['--only-network-test']
        if only_cloud is True:
            cmd += ['--only-cloud-test']
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

    def _run_topic(self,
                   cmd,
                   stdin=None,
                   timeout=None,
                   use_schema_registry=False):
        cmd = [self._rpk_binary(), "topic"] + self._kafka_conn_settings() + cmd
        if use_schema_registry:
            cmd = cmd + self._schema_registry_conn_settings()
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
            self._admin_host(), "cluster", "config", "set", key,
            str(value)
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
            raise RpkException(f"command `{' '.join(cmd)}' timed out",
                               stdout=output,
                               stderr=stderr)

        self._redpanda.logger.debug(f'\n{output}')

        if p.returncode:
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

    def cluster_maintenance_disable(self, node, timeout=None):
        node_id = self._redpanda.node_id(node) if isinstance(
            node, ClusterNode) else node
        cmd = [
            self._rpk_binary(), "--api-urls",
            self._admin_host(), "cluster", "maintenance", "disable",
            str(node_id)
        ]
        return self._execute(cmd, timeout=timeout)

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
            # NODE-ID  ENABLED  FINISHED  ERRORS  PARTITIONS  ELIGIBLE  TRANSFERRING  FAILED
            # 1        false     false     false   0           0         0             0
            line = line.split()

            assert len(
                line
            ) == 8, f"`rpk cluster maintenance status` has changed: {line}"
            line = [x.strip() for x in line]
            if line[0] == "NODE-ID":
                return None

            def bool_or_none(value: str):
                return None if value == "-" else value == "true"

            def int_or_none(value: str):
                return None if value == "-" else int(value)

            return RpkMaintenanceStatus(node_id=int(line[0]),
                                        enabled=line[1] == "true",
                                        finished=bool_or_none(line[2]),
                                        errors=bool_or_none(line[3]),
                                        partitions=int_or_none(line[4]),
                                        eligible=int_or_none(line[5]),
                                        transferring=int_or_none(line[6]),
                                        failed=int_or_none(line[7]))

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

    def _tls_settings(self):
        flags = []
        if self._tls_cert is not None:
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

    def _kafka_conn_settings(self, node: Optional[ClusterNode] = None):
        brokers = self._redpanda.broker_address(node) if node else \
                  self._redpanda.brokers()
        flags = ["-X", "brokers=" + brokers]
        if self._username:
            # u, p and mechanism must always be all set or all unset
            assert self._password and self._sasl_mechanism
            flags += [
                "-X",
                "user=" + self._username,
                "-X",
                "pass=" + self._password,
                "-X",
                "sasl.mechanism=" + self._sasl_mechanism,
            ]
        flags += self._tls_settings()
        return flags

    def acl_list(self, flags: list[str] = [], request_timeout_overhead=None):
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
        ] + flags + self._kafka_conn_settings()

        # How long rpk will wait for a response from the broker, default is 5s
        if request_timeout_overhead is not None:
            cmd += [
                "-X", "globals.request_timeout_overhead=" +
                f'{str(request_timeout_overhead)}s'
            ]

        output = self._execute(cmd)

        if "CLUSTER_AUTHORIZATION_FAILED" in output:
            raise ClusterAuthorizationError("acl list")

        return output

    def acl_create_allow_cluster(self,
                                 username: str,
                                 op: str,
                                 principal_type: str = "User"):
        """
        Add allow+describe+cluster ACL
        """
        cmd = [
            self._rpk_binary(),
            "acl",
            "create",
            "--allow-principal",
            f"{principal_type}:{username}",
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

            def make(x: re.Match[str] | None):
                if not x:
                    raise RuntimeError("Failed to parse offset-delete output")
                return RpkOffsetDeleteResponsePartition(
                    x['topic'], int(x['partition']), x['status'], x['error'])

            return [make(regex.match(x)) for x in output]

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
        ] + self._kafka_conn_settings() + request_args_w_flags

        # Retry if NOT_COORDINATOR is observed when command exits 1
        return try_offset_delete(retries=5)

    def generate_grafana(self, dashboard, datasource="", metrics_endpoint=""):
        cmd = [
            self._rpk_binary(), "generate", "grafana-dashboard", "--dashboard",
            dashboard, "-X", "admin.hosts=" + self._redpanda.admin_endpoints()
        ]

        if datasource != "":
            cmd += ["--datasource", datasource]

        if metrics_endpoint != "":
            cmd += ["--metrics-endpoint", metrics_endpoint]

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
        ] + self._kafka_conn_settings()

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

    def _schema_registry_host(self, node=None):
        if node is None:
            return ",".join([
                f"{n.account.hostname}:8081"
                for n in self._redpanda.started_nodes()
            ])
        else:
            return f"{node.account.hostname}:8081"

    def _schema_registry_conn_settings(self):
        flags = [
            "-X",
            "registry.hosts=" + self._schema_registry_host(),
        ]
        if self._username:
            assert self._password and self._sasl_mechanism
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
                "registry.tls.key=" + self._tls_cert.key,
                "-X",
                "registry.tls.cert=" + self._tls_cert.crt,
                "-X",
                "registry.tls.ca=" + self._tls_cert.ca.crt,
            ]
        if self._tls_enabled:
            flags += [
                "-X",
                "registry.tls.enabled=" + str(self._tls_enabled),
            ]
        return flags

    def _run_registry(self,
                      cmd,
                      stdin=None,
                      timeout=None,
                      output_format="json"):
        cmd = [self._rpk_binary(), "registry", "--format", output_format
               ] + self._schema_registry_conn_settings() + cmd
        out = self._execute(cmd, stdin=stdin, timeout=timeout)
        return json.loads(out) if output_format == "json" else out

    def get_compatibility_level(self, subject="", include_global=True):
        cmd = ["compatibility-level", "get", subject]
        if include_global:
            cmd += ["--global"]

        return self._run_registry(cmd)

    def set_compatibility_level(self, subject, level, include_global=False):
        cmd = ["compatibility-level", "set", subject, "--level", level]
        if include_global:
            cmd += ["--global"]

        return self._run_registry(cmd)

    def check_compatibility(self, subject, version, schema_path):
        cmd = [
            "schema", "check-compatibility", subject, "--schema-version",
            version, "--schema", schema_path
        ]

        return self._run_registry(cmd)

    def create_schema(self, subject, schema_path, references=None):
        cmd = ["schema", "create", subject, "--schema", schema_path]

        if references is not None:
            cmd += ["--references", references]

        return self._run_registry(cmd)

    def get_schema(self,
                   subject=None,
                   id=None,
                   version=None,
                   schema_path=None):
        cmd = ["schema", "get"]

        if subject is not None:
            cmd += [subject]

        if id is not None:
            cmd += ["--id", id]

        if version is not None:
            cmd += ["--schema-version", version]

        if schema_path is not None:
            cmd += ["--schema", schema_path]

        return self._run_registry(cmd)

    def list_schemas(self, subjects=[], deleted=False):
        cmd = ["schema", "list"]

        if len(subjects) > 0:
            cmd += subjects

        if deleted:
            cmd += ["--deleted"]

        return self._run_registry(cmd)

    def delete_schema(self, subject, version, permanent=False):
        cmd = ["schema", "delete", subject, "--schema-version", version]

        if permanent:
            cmd += ["--permanent"]

        return self._run_registry(cmd)

    def schema_references(self, subject, version, deleted=False):
        cmd = ["schema", "references", subject, "--schema-version", version]

        if deleted:
            cmd += ["--deleted"]

        return self._run_registry(cmd)

    def list_subjects(self, deleted=False):
        cmd = ["subject", "list"]

        if deleted:
            cmd += ["--deleted"]

        return self._run_registry(cmd)

    def delete_subjects(self, subjects=[], permanent=False):
        cmd = ["subject", "delete"] + subjects

        if permanent:
            cmd += ["--permanent"]

        return self._run_registry(cmd)

    def get_mode(self, subjects=[], includeGlobal=True, format="json"):
        cmd = ["mode", "get"]

        if includeGlobal:
            cmd += ["--global"]

        if len(subjects) > 0:
            cmd += subjects

        return self._run_registry(cmd, output_format=format)

    def set_mode(self, mode, subjects=[], format="json"):
        cmd = ["mode", "set", "--mode", mode]

        if len(subjects) > 0:
            cmd += subjects

        return self._run_registry(cmd, output_format=format)

    def reset_mode(self, subjects=[], format="json"):
        cmd = ["mode", "reset"]

        if len(subjects) > 0:
            cmd += subjects

        return self._run_registry(cmd, output_format=format)

    def _run_wasm(self, rest):
        cmd = [self._rpk_binary(), "transform"]
        cmd += ["-X", "admin.hosts=" + self._redpanda.admin_endpoints()]
        cmd += rest
        return self._execute(cmd)

    def deploy_wasm(self,
                    name,
                    input_topic,
                    output_topics,
                    file="tinygo/identity.wasm",
                    compression_type: TopicSpec.CompressionTypes
                    | None = None,
                    from_offset: str | None = None):
        cmd = [
            "deploy", "--name", name, "--input-topic", input_topic, "--file",
            f"/opt/transforms/{file}"
        ]
        if compression_type is not None:
            cmd += ["--compression", compression_type]
        if from_offset is not None:
            cmd += ["--from-offset", from_offset]
        assert len(output_topics) > 0, "missing output topics"
        for topic in output_topics:
            cmd += ["--output-topic", topic]
        self._run_wasm(cmd)

    def delete_wasm(self, name):
        self._run_wasm(["delete", name, "--no-confirm"])

    def list_wasm(self):
        out = self._run_wasm([
            "list",
            "--format",
            "json",
            "--detailed",
        ])
        loaded = json.loads(out)

        def status_from_json(loaded):
            return RpkWasmListProcessorResponse(
                node_id=loaded["node_id"],
                partition=loaded["partition"],
                status=loaded["status"],
                lag=loaded["lag"],
            )

        def transform_from_json(loaded):
            return RpkWasmListResponse(
                name=loaded["name"],
                input_topic=loaded["input_topic"],
                output_topics=loaded["output_topics"],
                status=[status_from_json(s) for s in loaded["status"]],
                environment=loaded["environment"],
                compression=TopicSpec.CompressionTypes(loaded["compression"]),
            )

        return [transform_from_json(o) for o in loaded]

    def pause_wasm(self, name):
        self._run_wasm(["pause", name])

    def resume_wasm(self, name):
        self._run_wasm(["resume", name])

    def describe_txn_producers(self, topics, partitions, all=False):
        cmd = [
            "describe-producers", "--topics", ",".join(topics), "--partitions",
            ",".join([str(x) for x in partitions])
        ]

        if all:
            cmd += ["--all"]

        return self._run_txn(cmd)

    def describe_txn(self, txn_id, print_partitions=False) -> dict | str:
        cmd = ["describe", txn_id]

        if print_partitions:
            cmd += ["--print-partitions"]

        return self._run_txn(cmd)

    def list_txn(self):
        return self._run_txn(["list"])

    def _run_txn(self,
                 cmd,
                 stdin=None,
                 timeout=None,
                 output_format="json") -> dict | str:
        cmd = [
            self._rpk_binary(),
            "cluster",
            "txn",
            "--format",
            output_format,
        ] + self._kafka_conn_settings() + cmd

        out = self._execute(cmd, stdin=stdin, timeout=timeout)

        return json.loads(out) if output_format == "json" else out

    def force_partition_recovery(self, from_nodes, to_node=None):
        cmd = [
            self._rpk_binary(),
            "cluster",
            "partitions",
            "unsafe-recover",
            "--no-confirm",
            "--from-nodes",
            ",".join([str(x) for x in from_nodes]),
        ]

        if to_node is not None:
            cmd += ["-X", "admin.hosts=" + f"{to_node.account.hostname}:9644"]
        else:
            cmd += ["-X", "admin.hosts=" + self._admin_host()]
        return self._execute(cmd)

    @property
    def _username(self):
        return self._security.username

    @property
    def _password(self):
        return self._security.password

    @property
    def _sasl_mechanism(self):
        return self._security.mechanism

    @property
    def _tls_enabled(self):
        return self._security.tls_enabled

    def create_role(self, role_name):
        return self._run_role(["create", role_name])

    def list_roles(self):
        return self._run_role(["list"])

    def delete_role(self, role_name):
        cmd = ["delete", role_name, "--no-confirm"
               ] + self._kafka_conn_settings()
        return self._run_role(cmd)

    def assign_role(self, role_name, principals):
        cmd = ["assign", role_name, "--principal", ",".join(principals)]
        return self._run_role(cmd)

    def unassign_role(self, role_name, principals):
        cmd = ["unassign", role_name, "--principal", ",".join(principals)]
        return self._run_role(cmd)

    def describe_role(self, role_name):
        return self._run_role(["describe", role_name] +
                              self._kafka_conn_settings())

    def _run_role(self, cmd, output_format="json"):
        cmd = [
            self._rpk_binary(), "security", "role", "--format", output_format,
            "-X", "admin.hosts=" + self._redpanda.admin_endpoints()
        ] + cmd

        out = self._execute(cmd)

        return json.loads(out) if output_format == "json" else out

    def describe_cluster_quotas(self,
                                any=[],
                                default=[],
                                name=[],
                                strict=False,
                                output_format="json"):
        cmd = ["describe"]

        if strict:
            cmd += ["--strict"]
        if len(any) > 0:
            cmd += ["--any", ",".join(any)]
        if len(default) > 0:
            cmd += ["--default", ",".join(default)]
        if len(name) > 0:
            cmd += ["--name", ",".join(name)]

        return self._run_cluster_quotas(cmd, output_format=output_format)

    def alter_cluster_quotas(self,
                             add=[],
                             delete=[],
                             default=[],
                             name=[],
                             dry=False,
                             output_format="json",
                             node: Optional[ClusterNode] = None):
        cmd = ["alter"]

        if dry:
            cmd += ["--dry"]
        if len(add) > 0:
            cmd += ["--add", ",".join(add)]
        if len(delete) > 0:
            cmd += ["--delete", ",".join(delete)]
        if len(default) > 0:
            cmd += ["--default", ",".join(default)]
        if len(name) > 0:
            cmd += ["--name", ",".join(name)]

        return self._run_cluster_quotas(cmd,
                                        output_format=output_format,
                                        node=node)

    def import_cluster_quota(self, source, output_format="json"):
        cmd = ["import", "--no-confirm", "--from", source]
        return self._run_cluster_quotas(cmd, output_format=output_format)

    def _run_cluster_quotas(self,
                            cmd,
                            output_format="json",
                            node: Optional[ClusterNode] = None):
        cmd = [
            self._rpk_binary(),
            "cluster",
            "quotas",
            "--format",
            output_format,
        ] + self._kafka_conn_settings(node) + cmd

        out = self._execute(cmd)

        return json.loads(out) if output_format == "json" else out

    def run_mock_plugin(self, cmd):
        cmd = [self._rpk_binary(), "pluginmock"] + cmd
        out = self._execute(cmd)
        return json.loads(out)
