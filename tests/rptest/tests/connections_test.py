# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.rpk_producer import RpkProducer
from rptest.services.rpk_consumer import RpkConsumer
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.utils.util import wait_until
import socket
import re
import os
import time


class StressConnections(BackgroundThreadService):
    def __init__(self, context, host_ip):
        super(StressConnections, self).__init__(context, num_nodes=1)
        self.host_ip = host_ip

    def _worker(self, idx, node):
        python = "python3"
        script_name = "connections_test_helper.py"

        dir = os.path.dirname(os.path.realpath(__file__))
        src_path = os.path.join(dir, script_name)
        dest_path = os.path.join("/tmp", script_name)

        node.account.copy_to(src_path, dest_path)
        ssh_output = node.account.ssh_capture(
            f"{python} {dest_path} {self.host_ip}")
        for line in ssh_output:
            self.logger.info(line)

    def stop_node(self, node):
        self._stopping.set()
        try:
            self.logger.debug("Killing connections spammer")
            node.account.kill_process("connections_test_helper",
                                      clean_shutdown=False)
        except RemoteCommandError as e:
            if b"No such process" not in e.msg:
                raise


class ConnectionsTest(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=1), )
    msg_count = 100000

    def __init__(self, test_context):
        super(ConnectionsTest, self).__init__(test_context=test_context,
                                              num_brokers=1)

        self.consumer = RpkConsumer(context=self.test_context,
                                    redpanda=self.redpanda,
                                    topic=self.topics[0].name,
                                    retries=1)

    def produce_data(self):
        producer = RpkProducer(context=self.test_context,
                               redpanda=self.redpanda,
                               topic=self.topics[0].name,
                               msg_size=4096,
                               msg_count=self.msg_count,
                               acks=-1)
        producer.start()
        producer.wait()

    @cluster(num_nodes=4)
    def test_active_connections(self):
        self.produce_data()
        self.consumer.start()

        connections_spammer = StressConnections(
            self.test_context,
            self.redpanda.brokers_list()[0])
        connections_spammer.start()

        self.old_msg_number = 0
        self.new_msg_count = 0

        def waiter():
            self.new_msg_count = len(self.consumer.messages)
            return self.new_msg_count == self.msg_count or self.new_msg_count > self.old_msg_number

        for i in range(1000):
            wait_until(waiter,
                       timeout_sec=1,
                       backoff_sec=0.2,
                       err_msg="Can not consume data")
            self.old_msg_number = self.new_msg_count
            if self.new_msg_count == self.msg_count:
                break
