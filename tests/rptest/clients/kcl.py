# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import namedtuple
import json
import re
import subprocess
import time
import itertools
from typing import Optional
from ducktape.utils.util import wait_until

KclPartitionOffset = namedtuple(
    'KclPartitionOffset',
    ['broker', 'topic', 'partition', 'start_offset', 'end_offset', 'error'])

KclPartitionEpochEndOffset = namedtuple('KclPartitionEpochEndOffset', [
    'broker', 'topic', 'partition', 'leader_epoch', 'epoch_end_offset', 'error'
])

KclCreateTopicsRequestTopic = namedtuple(
    'KclCreateTopicsRequestTopic',
    ['topic', 'num_partitions', 'replication_factor'])

KclCreatePartitionsRequestTopic = namedtuple('KclCreatePartitionsRequestTopic',
                                             ['topic', 'count', 'assignment'])

KclListPartitionReassignmentsResponse = namedtuple(
    'KclListPartitionReassignmentsResponse',
    ['topic', 'partition', 'replicas', 'adding_replicas', 'removing_replicas'])


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

    def _alter_config(self, values, incremental, entity_type, entity):
        """
        :param broker: node id.
        :param values: dict of property name to new value
        :param incremental: if true, use incremental kafka APIs
        :param entity_type: one of 'broker', 'topic'
        :param entity: string-izable entity, or None to omit
        """
        cmd = ["admin", "configs", "alter"]

        if entity_type == "broker":
            cmd.append("-tb")
        elif entity_type == "topic":
            cmd.append("-tt")
        else:
            raise NotImplementedError(entity_type)

        if incremental:
            cmd.append("-i")
        else:
            # By default, non-incremental AlterConfig will prompt on stdin (and hang)
            cmd.append("--no-confirm")
        for k, v in values.items():
            cmd.extend(["-k", f"s:{k}={v}" if incremental else f"{k}={v}"])

        if entity:
            # cmd needs to be string, so handle things like broker=1
            cmd.append(str(entity))

        r = self._cmd(cmd, attempts=1)
        if 'OK' not in r:
            raise RuntimeError(r)
        else:
            return r

    def alter_broker_config(self, values, incremental, broker=None):
        return self._alter_config(values, incremental, "broker", broker)

    def alter_topic_config(self, values, incremental, topic):
        return self._alter_config(values, incremental, "topic", topic)

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

    def describe_topic(self,
                       topic: str,
                       with_docs: bool = False,
                       with_types: bool = False):
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

        return self._cmd(cmd, attempts=1)

    def offset_delete(self, group: str, topic_partitions: dict):
        """
        kcl group offset-delete <group> -t <topic>:partition_1,partition_2,... -t ...
        """

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

        cmd = ['group', 'offset-delete', "-j", group] + request_args_w_flags
        return json.loads(self._cmd(cmd, attempts=5))

    def get_user_credentials_cmd(self,
                                 user_cred: Optional[dict[str, str]] = None):
        if user_cred is not None:
            assert "user" in user_cred
            assert "passwd" in user_cred
            assert "method" in user_cred
            return [
                "-X", f'sasl_user={user_cred["user"]}', "-X",
                f'sasl_pass={user_cred["passwd"]}', "-X",
                f'sasl_method={user_cred["method"]}'
            ]

        return []

    def alter_partition_reassignments(self,
                                      topics: dict[str, dict[int, list[int]]],
                                      user_cred: Optional[dict[str,
                                                               str]] = None,
                                      timeout_s: int = 10):
        """
        :param topics: the key is a topic and the value is a dict that maps partition IDs
                       to new replica assignments
        :return: list of KclAlterPartitionReassignmentsResponse
        """
        cmd = self.get_user_credentials_cmd(user_cred) + [
            "admin", "partas", "alter"
        ]

        for topic in topics:
            assert len(topics[topic]) > 0
            reassignment_str = f"{topic}:"
            partitions = []
            for pid in topics[topic]:
                if len(topics[topic][pid]) == 0:
                    raise NotImplementedError(
                        'Canceling a reassignment is unsupported')

                part_str = f"{pid}->{topics[topic][pid]}"
                # Remove empty and [] characters
                part_str = part_str.replace('[', '').replace(']', '')
                part_str = part_str.replace(' ', '')
                partitions.append(part_str)
            join_partitions = ";".join(partitions)
            reassignment_str += join_partitions
            cmd.append(reassignment_str)

        no_broker_re = re.compile(
            r"^(?P<topic>[a-z\-]+?) +(?P<partition>[0-9]+?) +BROKER_NOT_AVAILABLE.*$"
        )
        bad_rep_factor_re = re.compile(
            r"^(?P<topic>[a-z\-]+?) +(?P<partition>[0-9]+?) +INVALID_REPLICATION_FACTOR.*$"
        )
        unknown_tp_re = re.compile(
            r"^(?P<topic>[a-z\-]+?) +(?P<partition>[0-9]+?) +UNKNOWN_TOPIC_OR_PARTITION:.*$"
        )

        lines = None

        def do_alter_partitions():
            nonlocal lines
            lines = self._cmd(cmd).splitlines()

            # Check for errors here instead of outside the KCL wrapper
            # because test writers can use method params to account for their expectations
            for l in lines:
                l = l.strip()
                self._redpanda.logger.debug(l)

                m = no_broker_re.match(l)
                # No broker available means the partition did not find any eligible allocation nodes.
                # See the map from cluster errors to kafka errors in kafka::map_topic_error_code()
                if m is not None:
                    raise RuntimeError('No eligible allocation nodes')

                # Invalid replication factor means the number of replicas for one (or more) partitions
                # in a request does not match the replication factor for the topic.
                m = bad_rep_factor_re.match(l)
                if m is not None:
                    raise RuntimeError(
                        'Number of replicas != topic replication factor')

                # RP may report that the topic does not exist, this can
                # happen when the recieving broker has out-of-date metadata. So
                # retry the request.
                m = unknown_tp_re.match(l)
                if m is not None:
                    return False

            return True

        wait_until(do_alter_partitions,
                   timeout_sec=timeout_s,
                   backoff_sec=1,
                   err_msg="Failed to alter partitions")

        return lines

    def list_partition_reassignments(self,
                                     topics: Optional[dict[str,
                                                           list[int]]] = None,
                                     user_cred: Optional[dict[str,
                                                              str]] = None):
        """
        :param topics: dict where topic name is the key and the value is the list
                       of partition IDs
        :return: list of KclListPartitionReassignmentsResponse
        """
        cmd = self.get_user_credentials_cmd(user_cred) + [
            "admin", "partas", "list"
        ]

        lines = None
        if topics is None:
            lines = self._cmd(cmd).splitlines()
        else:
            for topic in topics:
                topic_str = f"{topic}:{topics[topic]}"
                # Remove empty and [] characters
                topic_str = topic_str.replace('[', '').replace(']', '')
                topic_str = topic_str.replace(' ', '')
                cmd.append(topic_str)

            lines = self._cmd(cmd, attempts=1).splitlines()
        self._redpanda.logger.debug(lines)

        def replicas_as_int(replicas: list[str]):
            return [int(node_id) for node_id in replicas]

        res_re = re.compile(
            r"^(?P<topic>[a-z\-]+?) +(?P<partition>[0-9]+?) +\[(?P<replicas>[0-9 ]+?)\] +\[(?P<adding>[0-9 ]*?)\] +\[(?P<removing>[0-9 ]*?)\]$"
        )
        ret = []
        for l in lines:
            l = l.strip()
            self._redpanda.logger.debug(l)
            m = res_re.match(l)
            if m is not None:
                replicas = replicas_as_int(list(m["replicas"].replace(' ',
                                                                      '')))
                adding_replicas = replicas_as_int(
                    list(m["adding"].replace(' ', '')))
                removing_replicas = replicas_as_int(
                    list(m["removing"].replace(' ', '')))
                ret.append(
                    KclListPartitionReassignmentsResponse(
                        m['topic'], int(m['partition']), replicas,
                        adding_replicas, removing_replicas))

        return ret

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


class RawKCL(KCL):
    """
    Extentions to KCL class intented to be used with the 'misc raw-req' API

    Callers should expect raw kafka responses json encoded with franz-go key naming scheme
    """
    def raw_create_topics(self, version, topics):
        assert version >= 0 and version <= 6, "version out of supported redpanda range for this API"
        create_topics_request = {
            'Version':
            version,
            'ValidateOnly':
            False,
            'TimeoutMillis':
            60000,
            'Topics': [{
                'Topic': t.topic,
                'NumPartitions': t.num_partitions,
                'ReplicationFactor': t.replication_factor
            } for t in topics]
        }
        return self._cmd(['misc', 'raw-req', '-k', '19'],
                         input=json.dumps(create_topics_request))

    def raw_delete_topics(self, version, topics):
        assert version >= 0 and version <= 5, "version out of supported redpanda range for this API"
        delete_topics_request = {
            'Version': version,
            'TimeoutMillis': 15000,
            'TopicNames': topics
        }
        return self._cmd(['misc', 'raw-req', '-k', '20'],
                         input=json.dumps(delete_topics_request))

    def raw_create_partitions(self, version, topics):
        assert version >= 0 and version <= 3, "version out of supported redpanda range for this API"
        create_partitions_request = {
            'Version': version,
            'ValidateOnly': False,
            'TimeoutMillis': 15000,
            'Topics': [{
                'Topic': t.topic,
                'Count': t.count
            } for t in topics]
        }
        return self._cmd(['misc', 'raw-req', '-k', '37'],
                         input=json.dumps(create_partitions_request))

    def raw_alter_topic_config(self, version, topic, configs):
        assert version >= 0 and version <= 1, "version out of supported redpanda range for this API"
        alter_configs_request = {
            'Version':
            version,
            'ValidateOnly':
            False,
            'TimeoutMillis':
            15000,
            'Resources': [{
                'ResourceType': 2,
                'ResourceName': topic,
                'Configs': []
            }],
            'ValidateOnly':
            False
        }

        for k, v in configs.items():

            alter_configs_request['Resources'][0]['Configs'].append({
                "Name":
                k,
                "Value":
                str(v)
            })
        self._redpanda.logger.info(f"DBG: {json.dumps(alter_configs_request)}")
        return self._cmd(['misc', 'raw-req', '-k', '33'],
                         input=json.dumps(alter_configs_request))
