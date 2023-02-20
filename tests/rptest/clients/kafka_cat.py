# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import time
import json
from typing import Optional

from rptest.util import wait_until_result


class KafkaCat:
    """
    Wrapper around the kcat utility.

    This tools is useful because it offers a JSON output format.
    """
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def metadata(self):
        return self._cmd(["-L"])

    def consume_one(self,
                    topic,
                    partition,
                    offset=None,
                    *,
                    first_timestamp: Optional[int] = None):
        if offset is not None:
            # <value>  (absolute offset)
            query = offset
        else:
            assert first_timestamp is not None
            # s@<value> (timestamp in ms to start at)
            query = f"s@{first_timestamp}"

        return self._cmd([
            "-C", "-e", "-t", f"{topic}", "-p", f"{partition}", "-o",
            f"{query}", "-c1"
        ])

    def produce_one(self, topic, msg, tx_id=None):
        cmd = ['-P', '-t', topic]
        if tx_id:
            cmd += ['-X', f'transactional.id={tx_id}']
        return self._cmd_raw(cmd, input=f"{msg}\n")

    def _cmd(self, cmd, input=None):
        res = self._cmd_raw(cmd + ['-J'], input=input)
        res = json.loads(res)
        self._redpanda.logger.debug(json.dumps(res, indent=2))
        return res

    def _cmd_raw(self, cmd, input=None):
        for retry in reversed(range(10)):
            if getattr(self._redpanda, "sasl_enabled", lambda: False)():
                cfg = self._redpanda.security_config()
                cmd += [
                    "-X", f"security.protocol={cfg['security_protocol']}", "-X"
                    f"sasl.mechanism={cfg['sasl_mechanism']}", "-X",
                    f"sasl.username={cfg['sasl_plain_username']}", "-X",
                    f"sasl.password={cfg['sasl_plain_password']}"
                ]
                if cfg['sasl_mechanism'] == "GSSAPI":
                    cmd += [
                        "-X", "sasl.kerberos.service.name=redpanda",
                        '-Xsasl.kerberos.kinit.cmd=kinit client -t /var/lib/redpanda/client.keytab'
                    ]
            try:
                res = subprocess.check_output(
                    ["kcat", "-b", self._redpanda.brokers()] + cmd,
                    text=True,
                    input=input)
                self._redpanda.logger.debug(res)
                return res
            except subprocess.CalledProcessError as e:
                if retry == 0:
                    raise
                self._redpanda.logger.debug(
                    "kcat retrying after exit code {}: {}".format(
                        e.returncode, e.output))
                time.sleep(2)

    def get_partition_leader(self, topic, partition, timeout_sec=None):
        """
        :param topic: string, topic name
        :param partition: integer
        :param timeout_sec: wait for leader (if falsey return immediately)
        :return: 2-tuple of (leader id or None, list of replica broker IDs)
        """
        if not timeout_sec:
            return self._get_partition_leader(topic, partition)

        def get_leader():
            leader = self._get_partition_leader(topic, partition)
            return leader[0] is not None, leader

        return wait_until_result(get_leader,
                                 timeout_sec=timeout_sec,
                                 backoff_sec=2)

    def _get_partition_leader(self, topic, partition):
        topic_meta = None
        all_metadata = self.metadata()
        for t in all_metadata['topics']:
            if t['topic'] == topic:
                topic_meta = t
                break

        # Raise AssertionError if user queried a topic that does not exist
        assert topic_meta is not None

        # Raise IndexError if user queried a partition that does not exist
        partition = topic_meta['partitions'][partition]

        leader_id = partition['leader']
        replicas = [p['id'] for p in partition['replicas']]
        if leader_id == -1:
            return None, replicas
        else:
            return leader_id, replicas

    def list_offsets(self, topic, partition):
        def cmd(ts):
            return ["-Q", "-t", f"{topic}:{partition}:{ts}"]

        def offset(res):
            # partition is a string in the output
            return res[topic][f"{partition}"]["offset"]

        oldest = offset(self._cmd(cmd(-2)))
        newest = offset(self._cmd(cmd(-1)))

        return (oldest, newest)

    def query_offset(self, topic, partition, ts):
        res = self._cmd(["-Q", "-t", f"{topic}:{partition}:{ts}"])

        return res[topic][f"{partition}"]["offset"]
