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
from typing import Any, cast

from rptest.services.redpanda_types import RedpandaServiceForClients
from rptest.util import wait_until_result

# pyright: strict


class KafkaCat:
    """
    Wrapper around the kcat utility.

    This tools is useful because it offers a JSON output format.
    """
    def __init__(self, redpanda: RedpandaServiceForClients):
        self._redpanda = redpanda

    def metadata(self):
        return self._cmd(["-L"])

    def consume_one(self,
                    topic: str,
                    partition: int,
                    offset: int | None = None,
                    *,
                    first_timestamp: int | None = None):
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

    def produce_one(self, topic: str, msg: str, tx_id: int | None = None):
        cmd = ['-P', '-t', topic]
        if tx_id:
            cmd += ['-X', f'transactional.id={tx_id}']
        return self._cmd_raw(cmd, input=f"{msg}\n")

    def _cmd(self, cmd: list[str], input: str | None = None):
        res = self._cmd_raw(cmd + ['-J'], input=input)
        res = json.loads(res)
        self._redpanda.logger.debug(json.dumps(res, indent=2))
        return res

    def _cmd_raw(self, cmd: list[str], input: str | None = None):
        for retry in reversed(range(10)):
            cfg = self._redpanda.kafka_client_security()
            if cfg.sasl_enabled:
                if cfg.mechanism == 'GSSAPI':
                    cmd += [
                        "-X", "sasl.kerberos.service.name=redpanda",
                        '-Xsasl.kerberos.kinit.cmd=kinit client -t /var/lib/redpanda/client.keytab'
                    ]
                else:
                    creds = cfg.simple_credentials()
                    assert creds
                    cmd += [
                        "-X", f"security.protocol={cfg.security_protocol}",
                        "-X"
                        f"sasl.mechanism={creds.mechanism}", "-X",
                        f"sasl.username={creds.username}", "-X",
                        f"sasl.password={creds.password}"
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
        assert False, "unreachable"  # help the type checker

    def get_partition_leader(self,
                             topic: str,
                             partition: int,
                             timeout_sec: int | None = None):
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

    def _get_partition_leader(self, topic: str, partition: int):
        topic_meta = None
        all_metadata = self.metadata()
        for t in all_metadata['topics']:
            if t['topic'] == topic:
                topic_meta = t
                break

        # Raise AssertionError if user queried a topic that does not exist
        assert topic_meta is not None

        # Raise IndexError if user queried a partition that does not exist
        partition_meta: dict[str, Any] = topic_meta['partitions'][partition]

        leader_id = cast(int, partition_meta['leader'])
        replicas = [cast(int, p['id']) for p in partition_meta['replicas']]
        if leader_id == -1:
            return None, replicas
        else:
            return leader_id, replicas

    def list_offsets(self, topic: str, partition: int):
        def cmd(ts: int):
            return ["-Q", "-t", f"{topic}:{partition}:{ts}"]

        def offset(res: dict[str, Any]):
            # partition is a string in the output
            return res[topic][f"{partition}"]["offset"]

        oldest = offset(self._cmd(cmd(-2)))
        newest = offset(self._cmd(cmd(-1)))

        return (oldest, newest)

    def query_offset(self, topic: str, partition: int, ts: int):
        res = self._cmd(["-Q", "-t", f"{topic}:{partition}:{ts}"])

        return res[topic][f"{partition}"]["offset"]
