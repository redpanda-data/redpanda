# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import itertools
import json
import random
import re
import string
import subprocess
import time
from functools import cache
from typing import Any, NamedTuple

from ducktape.utils.util import wait_until


# Kafka error codes → names, used to translate kcl v0.18's numeric `error` field
# back into the names tests grep for in RuntimeError messages (e.g. "INVALID_CONFIG").
# kcl v0.16 printed names directly; v0.18 only prints codes. Unknown codes fall back
# to "ERROR_<code>" so the numeric value still surfaces.
_KAFKA_ERROR_NAMES = {
    0: "NONE",
    40: "INVALID_CONFIG",
    89: "THROTTLING_QUOTA_EXCEEDED",
}


class KclPartitionOffset(NamedTuple):
    broker: str
    topic: str
    partition: int
    start_offset: int
    end_offset: int
    error: str


class KclPartitionEpochEndOffset(NamedTuple):
    broker: str
    topic: str
    partition: int
    leader_epoch: int
    epoch_end_offset: int
    error: str


class KclCreateTopicsRequestTopic(NamedTuple):
    topic: str
    num_partitions: int
    replication_factor: int


class KclCreatePartitionsRequestTopic(NamedTuple):
    name: str
    num_partitions: int
    assignment: int


class KclListPartitionReassignmentsResponse(NamedTuple):
    topic: str
    partition: int
    replicas: list[int]
    adding_replicas: list[int]
    removing_replicas: list[int]


class KCL:
    def __init__(
        self,
        redpanda: Any,
        username: str | None = None,
        password: str | None = None,
        sasl_mechanism: str | None = None,
    ) -> None:
        self._redpanda = redpanda
        self._username = username
        self._password = password
        self._sasl_mechanism = sasl_mechanism
        if self._username is None:
            assert self._password is None and self._sasl_mechanism is None, (
                "Incomplete KCL sasl credentials"
            )
        else:
            assert self._password is not None and self._sasl_mechanism is not None, (
                "Incomplete KCL sasl credentials"
            )

    def sasl_enabled(self) -> bool:
        return self._username is not None

    def list_topics(self) -> str:
        return self._cmd(["topic", "list"])

    def list_groups(self) -> str:
        return self._cmd(["group", "list"])

    def produce(self, topic: str, msg: str) -> str:
        return self._cmd(["produce", topic], input=msg)

    def offset_for_leader_epoch(
        self,
        topics: str | list[str],
        leader_epoch: int,
        current_leader_epoch: int | None = None,
    ) -> list[KclPartitionEpochEndOffset]:
        cmd = ["misc", "offset-for-leader-epoch"]
        if isinstance(topics, list):
            cmd += topics
        else:
            cmd += [topics]
        cmd += ["-e", str(leader_epoch)]
        if current_leader_epoch:
            cmd += ["-c", str(current_leader_epoch)]
        lines = self._cmd(cmd).splitlines()
        ret: list[KclPartitionEpochEndOffset] = []
        for l in lines:
            m = re.match(
                r" *(?P<broker>\d+) +(?P<topic>.+?) +(?P<partition>\d+) +(?P<epoch>-?\d*?) +(?P<end_offset>-?\d*?) +(?P<error>.*) *",
                l,
            )
            if m:
                ret.append(
                    KclPartitionEpochEndOffset(
                        m["broker"],
                        m["topic"],
                        int(m["partition"]),
                        int(m["epoch"]) if m["epoch"] is not None else -1,
                        int(m["end_offset"]) if m["end_offset"] is not None else -1,
                        m["error"],
                    )
                )
        return ret

    def list_offsets(self, topics: str | list[str]) -> list[KclPartitionOffset]:
        cmd = ["misc", "list-offsets"]
        if isinstance(topics, list):
            cmd += topics
        else:
            cmd += [topics]

        lines = self._cmd(cmd).splitlines()
        ret: list[KclPartitionOffset] = []
        for l in lines:
            # kcl v0.18 list-offsets output: BROKER TOPIC PARTITION START STABLE END ERROR.
            # The STABLE column was added; we don't surface it in KclPartitionOffset.
            m = re.match(
                r" *(?P<broker>\d+) +(?P<topic>.+?) +(?P<partition>\d+) +(?P<start>-?\d*?) +(?P<stable>-?\d*?) +(?P<end>-?\d*?) +(?P<error>.*) *",
                l,
            )
            if m:
                ret.append(
                    KclPartitionOffset(
                        m["broker"],
                        m["topic"],
                        int(m["partition"]),
                        int(m["start"]) if m["start"] else -1,
                        int(m["end"]) if m["end"] else -1,
                        m["error"],
                    )
                )
        return ret

    def consume(
        self,
        topic: str,
        n: int | None = None,
        group: str | None = None,
        regex: bool = False,
        fetch_max_bytes: int | None = None,
        rack: str | None = None,
    ) -> str:
        cmd = ["consume"]
        if group is not None:
            cmd += ["-g", group]
        if n is not None:
            cmd.append(f"-n{n}")
        if regex:
            cmd.append("-r")
        if fetch_max_bytes is not None:
            cmd += ["--fetch-max-bytes", str(fetch_max_bytes)]
        if rack is not None:
            cmd += ["--rack", rack]
        cmd.append(topic)
        return self._cmd(cmd)

    def _alter_config(
        self,
        values: dict[str, Any],
        incremental: bool,
        entity_type: str,
        entity: Any,
        node: Any | None = None,
    ) -> str:
        """
        :param broker: node id.
        :param values: dict of property name to new value
        :param incremental: if true, use incremental kafka APIs
        :param entity_type: one of 'broker', 'topic'
        :param entity: string-izable entity, or None to omit
        """
        cmd = ["--format=json", "admin", "configs", "alter"]

        if entity_type == "broker":
            cmd.append("-tb")
        elif entity_type == "topic":
            cmd.append("-tt")
        else:
            raise NotImplementedError(entity_type)

        if incremental:
            cmd.append("-i")
            # `-s key=value` is the new clean flag and implicitly sends
            # IncrementalAlterConfigs (kafka API key 44).
            for k, v in values.items():
                cmd.extend(["-s", f"{k}={v}"])
        else:
            # By default, non-incremental AlterConfig will prompt on stdin (and hang)
            cmd.append("--yes")
            # `-k key=value` is the deprecated --kv syntax preserved in v0.18.
            # It still sends the legacy AlterConfigs (kafka API key 33) on the
            # wire -- which is the path tests like ClusterConfigTest.test_alter_configs
            # depend on to verify redpanda's rejection. v0.18's new --set/--delete
            # flags auto-enable incremental mode and so cannot reach the legacy API.
            for k, v in values.items():
                cmd.extend(["-k", f"{k}={v}"])

        if entity:
            # cmd needs to be string, so handle things like broker=1
            cmd.append(str(entity))

        # kcl v0.18's text output no longer carries the kafka error name (it
        # prints the numeric code and, for broker alters, an empty message on
        # success). Parse the JSON response so we can synthesize an "OK" string
        # on success and a RuntimeError whose message contains the kafka error
        # name on failure -- the contract callers have relied on since v0.16.
        raw = self._cmd(cmd, attempts=1, node=node)
        result = json.loads(raw)["results"][0]
        error_code = result["error"]
        error_message = result["error_message"]
        if error_code == 0:
            return f"OK: {error_message}" if error_message else "OK"
        error_name = _KAFKA_ERROR_NAMES.get(error_code, f"ERROR_{error_code}")
        raise RuntimeError(f"{error_name}: {error_message}")

    def alter_broker_config(
        self, values: dict[str, Any], incremental: bool, broker: Any | None = None
    ) -> str:
        return self._alter_config(values, incremental, "broker", broker)

    def alter_topic_config(
        self,
        values: dict[str, Any],
        incremental: bool,
        topic: str,
        node: Any | None = None,
    ) -> str:
        return self._alter_config(values, incremental, "topic", topic, node=node)

    def delete_broker_config(self, keys: list[str], incremental: bool) -> str:
        """
        :param keys: list of key names to clear
        :param incremental: if true, use incremental kafka APIs
        :return:
        """
        cmd = ["admin", "configs", "alter", "-tb"]
        if incremental:
            cmd.append("-i")
        for k in keys:
            cmd.extend(["--delete", k])

        return self._cmd(cmd, attempts=1)

    def describe_topic(
        self,
        topic: str,
        with_docs: bool = False,
        with_types: bool = False,
        node: Any | None = None,
    ) -> str:
        """
        :param topic: the name of the topic to describe
        :param with_docs: if true, include documention strings in the response
        :param with_types: if true, include config type information in the reponse
        :return: stdout string
        """
        cmd = ["admin", "configs", "describe", topic, "--type", "topic"]
        if with_docs:
            cmd.append("--with-docs")
        if with_types:
            cmd.append("--with-types")

        # kcl v0.18 puts the property table on stdout and (when --with-docs is
        # set) the doc-strings section on stderr. Ask _cmd to merge stderr
        # into stdout in that case so callers see the same single-blob output
        # v0.16 produced.
        stderr = subprocess.STDOUT if with_docs else subprocess.PIPE
        output = self._cmd(cmd, attempts=1, node=node, stderr=stderr)
        # kcl v0.18 prefixes the response with a "KEY [TYPE] VALUE SOURCE"
        # header row; drop the first line so callers parsing the table see
        # only data rows. Slice the original string to preserve trailing
        # newlines that callers rely on when re-splitting (a `\n`.join here
        # would collapse a trailing empty line).
        if output.lstrip().startswith("KEY"):
            newline_idx = output.find("\n")
            if newline_idx != -1:
                output = output[newline_idx + 1 :]
        return output

    def offset_delete(
        self, group: str, topic_partitions: dict[str, list[int]]
    ) -> dict[str, Any]:
        """
        kcl group offset-delete <group> -t <topic>:partition_1,partition_2,... -t ...
        """

        # First convert partitions from integers to strings
        as_strings = {
            k: ",".join([str(x) for x in v]) for k, v in topic_partitions.items()
        }

        # Group each kv pair to string item like '<topic>:p1,p2,p3'
        request_args = [f"{x}:{y}" for x, y in as_strings.items()]

        # Append each arg with the -t (topic) flag
        # interleaves a list of -t strings with each argument producing
        # [-t, arg1, -t arg2, ... , -t argn]
        request_args_w_flags = list(
            itertools.chain(
                *zip(["-t" for _ in range(0, len(request_args))], request_args)
            )
        )

        # kcl v0.18 exits non-zero on per-item failures but still emits valid
        # JSON on stdout; catch the CalledProcessError and parse e.output so
        # callers can inspect per-item errors.
        cmd = ["--format=json", "group", "offset-delete", group] + request_args_w_flags
        try:
            raw = self._cmd(cmd, attempts=1)
        except subprocess.CalledProcessError as e:
            raw = e.output
        return json.loads(raw)

    def sasl_options(self) -> list[str]:
        if self.sasl_enabled():
            return [
                "-X",
                f"sasl_user={self._username}",
                "-X",
                f"sasl_pass={self._password}",
                "-X",
                f"sasl_method={self._sasl_mechanism}",
            ]

        return []

    def alter_partition_reassignments(
        self,
        topics: dict[str, dict[int, list[int]]],
        user_cred: dict[str, str] | None = None,
        timeout_s: int = 10,
    ) -> list[str]:
        """
        :param topics: the key is a topic and the value is a dict that maps partition IDs
                       to new replica assignments
        :return: list of KclAlterPartitionReassignmentsResponse
        """
        cmd = ["admin", "partas", "alter"]

        for topic in topics:
            assert len(topics[topic]) > 0
            reassignment_str = f"{topic}:"
            partitions: list[str] = []
            for pid in topics[topic]:
                if len(topics[topic][pid]) == 0:
                    raise NotImplementedError("Canceling a reassignment is unsupported")

                part_str = f"{pid}->{topics[topic][pid]}"
                # Remove empty and [] characters
                part_str = part_str.replace("[", "").replace("]", "")
                part_str = part_str.replace(" ", "")
                partitions.append(part_str)
            join_partitions = ";".join(partitions)
            reassignment_str += join_partitions
            cmd.append(reassignment_str)

        @cache
        def make_partition_err_re(err_str: str) -> re.Pattern[str]:
            return re.compile(
                rf"^(?P<topic>[a-z\-]+?) +(?P<partition>[0-9]+?) +{re.escape(err_str)}.*$"
            )

        def has_partition_err(line: str, err: str) -> bool:
            re_pattern = make_partition_err_re(err)
            return re_pattern.match(line) is not None

        lines: list[str] = []

        def do_alter_partitions() -> bool:
            nonlocal lines
            lines = self._cmd(cmd).splitlines()
            # kcl v0.18 prefixes the response with a "TOPIC PARTITION STATUS
            # DETAIL" header row; drop it so callers see only data rows.
            if lines and lines[0].lstrip().startswith("TOPIC"):
                lines = lines[1:]

            # Check for errors here instead of outside the KCL wrapper
            # because test writers can use method params to account for their expectations
            for l in lines:
                l = l.strip()
                self._redpanda.logger.debug(l)

                # No broker available means the partition did not find any eligible allocation nodes.
                # See the map from cluster errors to kafka errors in kafka::map_topic_error_code()
                if has_partition_err(l, "BROKER_NOT_AVAILABLE"):
                    raise RuntimeError("No eligible allocation nodes")

                # Invalid replication factor means the number of replicas for one (or more) partitions
                # in a request does not match the replication factor for the topic.
                if has_partition_err(l, "INVALID_REPLICATION_FACTOR"):
                    raise RuntimeError("Number of replicas != topic replication factor")

                # RP may report that the topic does not exist, this can
                # happen when the recieving broker has out-of-date metadata. So
                # retry the request.
                if has_partition_err(l, "UNKNOWN_TOPIC_OR_PARTITION"):
                    return False

                # A concurrent reassignment may be triggered by partition_balancer
                if has_partition_err(l, "REASSIGNMENT_IN_PROGRESS"):
                    return False

            return True

        wait_until(
            do_alter_partitions,
            timeout_sec=timeout_s,
            backoff_sec=1,
            err_msg="Failed to alter partitions",
        )

        assert lines is not None
        return lines

    def list_partition_reassignments(
        self, topics: dict[str, list[int]] | None = None
    ) -> list[KclListPartitionReassignmentsResponse]:
        """
        :param topics: dict where topic name is the key and the value is the list
                       of partition IDs
        :return: list of KclListPartitionReassignmentsResponse
        """
        cmd = ["admin", "partas", "list"]

        lines: list[str] | None = None
        if topics is None:
            lines = self._cmd(cmd).splitlines()
        else:
            for topic in topics:
                topic_str = f"{topic}:{topics[topic]}"
                # Remove empty and [] characters
                topic_str = topic_str.replace("[", "").replace("]", "")
                topic_str = topic_str.replace(" ", "")
                cmd.append(topic_str)

            lines = self._cmd(cmd, attempts=1).splitlines()
        self._redpanda.logger.debug(lines)

        def replicas_as_int(replicas: list[str]) -> list[int]:
            return [int(node_id) for node_id in replicas]

        res_re = re.compile(
            r"^(?P<topic>[a-z\-]+?) +(?P<partition>[0-9]+?) +\[(?P<replicas>[0-9 ]+?)\] +\[(?P<adding>[0-9 ]*?)\] +\[(?P<removing>[0-9 ]*?)\]$"
        )
        ret: list[KclListPartitionReassignmentsResponse] = []
        for l in lines:
            l = l.strip()
            self._redpanda.logger.debug(l)
            m = res_re.match(l)
            if m is not None:
                replicas = replicas_as_int(list(m["replicas"].replace(" ", "")))
                adding_replicas = replicas_as_int(list(m["adding"].replace(" ", "")))
                removing_replicas = replicas_as_int(
                    list(m["removing"].replace(" ", ""))
                )
                ret.append(
                    KclListPartitionReassignmentsResponse(
                        m["topic"],
                        int(m["partition"]),
                        replicas,
                        adding_replicas,
                        removing_replicas,
                    )
                )

        return ret

    def _cmd(
        self,
        cmd: list[str],
        input: str | None = None,
        attempts: int = 5,
        node: Any | None = None,
        as_version: str | None = None,
        stderr: int = subprocess.PIPE,
    ) -> str:
        """

        :param attempts: how many times to try before giving up (1 for no retries)
        :param as_version: if set, pass ``--as-version <tag>`` as a global kcl
                              flag (e.g. ``"2.7.0"``). This caps the franz-go
                              client's per-API max-versions to that Kafka
                              release's table. Used by ``RawKCL`` to target
                              specific request versions without invoking kcl's
                              raw-req version-pin code path, which anchors
                              MinVersions to ``kversion.Stable()`` and would
                              mandate Metadata v13 from the broker.
        :param stderr: passed through to ``subprocess.check_output``. The
                              default (``subprocess.PIPE``) captures stderr
                              separately so ``CalledProcessError.stderr``
                              carries kcl's error text (v0.18 puts server-side
                              messages like "CLUSTER_AUTHORIZATION_FAILED" on
                              stderr; tests reading ``e.stderr`` pick those
                              up). Pass ``subprocess.STDOUT`` for the rare
                              command whose successful output is split across
                              streams (``admin configs describe --with-docs``
                              puts the property table on stdout and the doc
                              strings on stderr) -- the merged blob is then
                              returned as the function's result.
        :return: stdout (or stdout+stderr merged, depending on ``stderr``).
                 One v0.18 command (``group offset-delete``) exits non-zero
                 on per-item failures while still emitting a valid JSON body
                 on stdout; that call site wraps this helper in
                 ``try/except CalledProcessError`` and reads the response
                 from ``e.output``.
        """
        brokers = node.name if node is not None else self._redpanda.brokers()
        cmd = (
            ["kcl", "-X", f"seed_brokers={brokers}", "--no-config-file"]
            + (["--as-version", as_version] if as_version is not None else [])
            + self.sasl_options()
            + cmd
        )
        assert attempts > 0
        self._redpanda.logger.debug(f"Executing {cmd}")
        for retry in reversed(range(attempts)):
            try:
                res = subprocess.check_output(
                    cmd,
                    text=True,
                    input=input,
                    stderr=stderr,
                )
                self._redpanda.logger.debug(res)
                return res
            except subprocess.CalledProcessError as e:
                if retry == 0:
                    raise
                self._redpanda.logger.debug(
                    "kcl retrying after exit code {}: {}".format(e.returncode, e.output)
                )
                time.sleep(1)
        # it looks impossible to reach this case, but pyright static analyzer
        # can't see that and deduces Optional[str] as return type.
        raise RuntimeError(f"Command failed after retries: {cmd}")


class RawKCL(KCL):
    """
    Extentions to KCL class intented to be used with the 'misc raw-req' API

    Callers should expect raw kafka responses json encoded with franz-go key naming scheme
    """

    def _controller_id(self) -> int:
        return self._redpanda.node_id(self._redpanda.controller())

    def create_topics(
        self,
        version: int,
        topics: list[dict[str, Any]] = [],
        validate_only: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Create some topics based on the provided dicts
        Valid fields, which will be propagated into the request, are:
          - 'name' - default 12 random ascii letters
          - 'partition_count' - default -1
          - 'replication_factor' - default -1
        """
        tps: list[KclCreateTopicsRequestTopic] = []
        for tp in topics:
            tps.append(
                KclCreateTopicsRequestTopic(
                    tp.get("name", "".join(random.choices(string.ascii_letters, k=12))),
                    tp.get("partition_count", -1),
                    tp.get("replication_factor", -1),
                )
            )
        try:
            return json.loads(
                self.raw_create_topics(version, tps, validate_only=validate_only)
            )["Topics"]
        except Exception:
            return []

    @staticmethod
    def _unwrap_raw_response(raw_json: str) -> Any:
        """kcl v0.18 wraps `misc raw-req` output in
        ``{"_command": ..., "_version": ..., "response": ...}``; return just
        the response payload so callers don't need to know about the envelope.
        """
        parsed: dict[str, Any] = json.loads(raw_json)
        if "_command" in parsed and "response" in parsed:
            return parsed["response"]
        return parsed

    # Lookup table: (api_key, request_version) -> the smallest Kafka release tag
    # whose per-API max version table has `max[api_key] == request_version`. Used
    # by raw_* methods to target a specific request version via kcl's
    # `--as-version` global flag (which calls `kgo.MaxVersions` with the release
    # tag's table). Capping a single client invocation at exactly the requested
    # version is functionally equivalent to pinning, because the broker supports
    # at-or-above that version in every test scenario.
    #
    # `--as-version` is used instead of `-v` / JSON `"Version"` because those
    # routes through kcl's raw-req pin code path (misc.go:303-316), which anchors
    # MinVersions to `kversion.Stable()` and mandates Metadata v13 from the
    # broker -- breaking requests against pre-v13 redpandas in upgrade tests.
    _AS_VERSION_BY_API: dict[tuple[int, int], str] = {
        # CreateTopics (19)
        (19, 5): "2.4.0",
        (19, 6): "2.7.0",
        (19, 7): "2.8.0",
        # DeleteTopics (20)
        (20, 4): "2.4.0",
        (20, 5): "2.7.0",
        # CreatePartitions (37)
        (37, 2): "2.5.0",
        (37, 3): "2.7.0",
        # AlterConfigs (33)
        (33, 0): "0.11.0",
        (33, 1): "2.0.0",
        # FindCoordinator (10)
        (10, 3): "2.4.0",
        # JoinGroup (11)
        (11, 5): "2.3.0",
    }

    @classmethod
    def _as_version_for(cls, api_key: int, request_version: int) -> str:
        try:
            return cls._AS_VERSION_BY_API[(api_key, request_version)]
        except KeyError:
            raise ValueError(
                f"no --as-version mapping for api_key={api_key} "
                f"version={request_version}; add an entry to "
                f"RawKCL._AS_VERSION_BY_API"
            )

    def raw_create_topics(
        self,
        version: int,
        topics: list[KclCreateTopicsRequestTopic],
        validate_only: bool = False,
    ) -> str:
        assert version >= 0 and version <= 7, (
            "version out of supported redpanda range for this API"
        )
        create_topics_request = {
            "ValidateOnly": validate_only,
            "TimeoutMillis": 60000,
            "Topics": [
                {
                    "Topic": t.topic,
                    "NumPartitions": t.num_partitions,
                    "ReplicationFactor": t.replication_factor,
                }
                for t in topics
            ],
        }
        res = self._cmd(
            ["misc", "raw-req", "-b", str(self._controller_id()), "-k", "19"],
            input=json.dumps(create_topics_request),
            as_version=self._as_version_for(19, version),
        )
        return json.dumps(self._unwrap_raw_response(res))

    def raw_delete_topics(self, version: int, topics: list[str]) -> str:
        assert version >= 0 and version <= 5, (
            "version out of supported redpanda range for this API"
        )
        delete_topics_request = {
            "TimeoutMillis": 15000,
            "TopicNames": topics,
        }
        res = self._cmd(
            ["misc", "raw-req", "-b", str(self._controller_id()), "-k", "20"],
            input=json.dumps(delete_topics_request),
            as_version=self._as_version_for(20, version),
        )
        return json.dumps(self._unwrap_raw_response(res))

    def raw_create_partitions(
        self, version: int, topics: list[KclCreatePartitionsRequestTopic]
    ) -> str:
        assert version >= 0 and version <= 3, (
            "version out of supported redpanda range for this API"
        )
        create_partitions_request: dict[str, Any] = {
            "ValidateOnly": False,
            "TimeoutMillis": 15000,
            "Topics": [{"Topic": t.name, "Count": t.num_partitions} for t in topics],
        }
        res = self._cmd(
            ["misc", "raw-req", "-b", str(self._controller_id()), "-k", "37"],
            input=json.dumps(create_partitions_request),
            as_version=self._as_version_for(37, version),
        )
        return json.dumps(self._unwrap_raw_response(res))

    def raw_alter_topic_config(
        self, version: int, topic: str, configs: dict[str, Any]
    ) -> str:
        assert version >= 0 and version <= 1, (
            "version out of supported redpanda range for this API"
        )
        alter_configs_request: dict[str, Any] = {
            "TimeoutMillis": 15000,
            "Resources": [
                {"ResourceType": "TOPIC", "ResourceName": topic, "Configs": []}
            ],
            "ValidateOnly": False,
        }

        alter_configs_request["Resources"][0]["Configs"] = [
            {"Name": k, "Value": str(v)} for k, v in configs.items()
        ]

        self._redpanda.logger.info(f"DBG: {json.dumps(alter_configs_request)}")
        res = self._cmd(
            ["misc", "raw-req", "-b", str(self._controller_id()), "-k", "33"],
            input=json.dumps(alter_configs_request),
            as_version=self._as_version_for(33, version),
        )
        return json.dumps(self._unwrap_raw_response(res))

    def raw_alter_quotas(
        self, body: dict[str, Any], node: Any | None = None
    ) -> dict[str, Any]:
        res = self._cmd(
            ["misc", "raw-req", "-b", str(self._controller_id()), "-k", "49"],
            input=json.dumps(body),
            node=node,
        )
        return self._unwrap_raw_response(res)

    def raw_describe_quotas(self, body: dict[str, Any]) -> dict[str, Any]:
        res = self._cmd(
            ["misc", "raw-req", "-b", str(self._controller_id()), "-k", "48"],
            input=json.dumps(body),
        )
        return self._unwrap_raw_response(res)

    def raw_find_coordinator(
        self, body: dict[str, Any], version: int | None = None
    ) -> dict[str, Any]:
        res = self._cmd(
            ["misc", "raw-req", "-k", "10"],
            input=json.dumps(body),
            as_version=self._as_version_for(10, version)
            if version is not None
            else None,
        )
        return self._unwrap_raw_response(res)

    def raw_join_group(
        self, body: dict[str, Any], version: int | None = None
    ) -> dict[str, Any]:
        res = self.raw_find_coordinator(
            {"CoordinatorKey": body["Group"], "CoordinatorType": 0},
            version=3,
        )
        res = self._cmd(
            ["misc", "raw-req", "-b", str(res["NodeID"]), "-k", "11"],
            input=json.dumps(body),
            as_version=self._as_version_for(11, version)
            if version is not None
            else None,
        )
        return self._unwrap_raw_response(res)
