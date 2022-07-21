# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions
from rptest.services.redpanda import RedpandaService
from ducktape.utils.util import wait_until
from typing import (Any, Optional)
from ducktape.tests.test import TestContext

from confluent_kafka import (Producer, KafkaException, Message)
from random import choice
from string import ascii_uppercase


def on_delivery(err: Optional[Any], msg: Message) -> None:
    if err is not None:
        raise KafkaException(err)


PAYLOAD_1KB = ''.join(choice(ascii_uppercase) for i in range(1024))


class TxAbortSnapshotTest(RedpandaTest):
    topics = [TopicSpec()]
    """
    Basic test that upgrading software works as expected.
    """
    def __init__(self, test_context: TestContext):
        extra_rp_conf = {
            "default_topic_replications": 3,
            "default_topic_partitions": 1,
            "enable_transactions": True,
            "log_segment_size": 1048576,
            "delete_retention_ms": 1,
            "abort_index_segment_size": 2
        }
        super(TxAbortSnapshotTest, self).__init__(test_context=test_context,
                                                  num_brokers=3,
                                                  extra_rp_conf=extra_rp_conf)

    def fill_idx(self, topic: str) -> None:
        p = Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '1',
        })
        p.init_transactions()
        for _ in range(0, 5):
            p.begin_transaction()
            p.produce(topic,
                      key="key1".encode('utf-8'),
                      value=PAYLOAD_1KB.encode('utf-8'),
                      callback=on_delivery)
            p.flush()
            p.abort_transaction()

    def fill_segment(self, topic: str) -> None:
        p = Producer({
            "bootstrap.servers": self.redpanda.brokers(),
            "enable.idempotence": True,
            "retries": 5
        })
        for _ in range(0, 4 * 1024):
            p.produce(topic,
                      key="key1".encode('utf-8'),
                      value=PAYLOAD_1KB.encode('utf-8'),
                      callback=on_delivery)
        p.flush()

    def find_indexes(self, topic: str) -> dict[str, str]:
        idxes = dict()
        for node in self.redpanda.nodes:
            idxes[node.account.hostname] = []
            cmd = f"find {RedpandaService.DATA_DIR}"
            out_iter = node.account.ssh_capture(cmd)
            for line in out_iter:
                m = re.match(
                    f"{RedpandaService.DATA_DIR}/kafka/{topic}/\\d+_\\d+/(abort.idx.\\d+.\\d+)",
                    line)
                if m:
                    idxes[node.account.hostname].append(m.group(1))
        return idxes

    def find_segments(self, topic: str) -> dict[str, list[str]]:
        segments = dict()
        for node in self.redpanda.nodes:
            segments[node.account.hostname] = []
            cmd = f"find {RedpandaService.DATA_DIR}"
            out_iter = node.account.ssh_capture(cmd)
            for line in out_iter:
                m = re.match(
                    f"{RedpandaService.DATA_DIR}/kafka/{topic}/\\d+_\\d+/(.+).log",
                    line)
                if m:
                    self.logger.info(f"{node.account.hostname} {line}")
                    segments[node.account.hostname].append(m.group(1))
        return segments

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_index_removal(self) -> None:
        self.fill_idx(self.topics[0].name)
        segments = self.find_segments(self.topics[0].name)
        for node in self.redpanda.nodes:
            assert len(segments[node.account.hostname]) == 1
        self.redpanda.restart_nodes(self.redpanda.nodes)
        idx = self.find_indexes(self.topics[0].name)
        for node in self.redpanda.nodes:
            assert len(idx[node.account.hostname]) > 0
        self.fill_segment(self.topics[0].name)

        def segments_gone() -> bool:
            current = self.find_segments(self.topics[0].name)
            for node in segments.keys():
                if node not in current:
                    continue
                if segments[node][0] in current[node]:
                    return False
            return True

        wait_until(segments_gone, timeout_sec=60, backoff_sec=1)

        self.redpanda.restart_nodes(self.redpanda.nodes)
        current_idx = self.find_indexes(self.topics[0].name)
        for node in self.redpanda.nodes:
            assert len(current_idx[node.account.hostname]) == 0
