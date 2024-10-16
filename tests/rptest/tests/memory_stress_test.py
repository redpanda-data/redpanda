# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from enum import Enum

from ducktape.mark import ignore, parametrize
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kaf_consumer import KafConsumer
from rptest.services.redpanda import ResourceSettings
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.rpk_producer import RpkProducer
from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode


class Consumers(Enum):
    RPK = 1
    KAF = 2
    VERIFIABLE = 3


class MemoryStressTest(RedpandaTest):
    """
    Try various ways to reach out-of-memory condition in a broker via Kafka API
    """
    def setUp(self):
        # Override parent setUp so that we don't start redpanda until each test,
        # enabling each test case to customize its ResourceSettings
        pass

    @ignore
    @cluster(num_nodes=5)
    @parametrize(memory_share_for_fetch=0.05)
    @parametrize(memory_share_for_fetch=0.5)
    @parametrize(memory_share_for_fetch=0.7)
    # the last one is the maximum at which the test has been checked to not fail in practice
    def test_fetch_with_many_partitions(self, memory_share_for_fetch: float):
        """
        Exhaust memory by consuming from too many partitions in a single Fetch
        API request.
        """
        # memory_mb does not work with debug redpanda build, therefore the test
        # only makes sense with release redpanda, hence @skip_debug_mode
        self.redpanda.set_resource_settings(
            ResourceSettings(memory_mb=512, num_cpus=1))
        self.redpanda.set_seed_servers(self.redpanda.nodes)
        self.redpanda.add_extra_rp_conf({
            "kafka_batch_max_bytes":
            10 * 1024 * 1024,
            "kafka_memory_share_for_fetch":
            memory_share_for_fetch
        })
        self.redpanda.start(omit_seeds_on_idx_one=False)

        # the maximum message size that does not make redpanda OOM with all
        # the other params as they are is 64 MiB
        msg_size = 1024 * 1024
        partition_count = 400
        self.topics = [
            TopicSpec(partition_count=partition_count,
                      max_message_bytes=msg_size * 2)
        ]
        self._create_initial_topics()

        msg_count = partition_count
        rpk_response_timeout = 10 + partition_count // 10 + msg_count * msg_size // (
            150 * 500_000)
        produce_timeout = msg_count * msg_size // 2184533
        self.logger.info(
            f"Starting producer. msg_size={msg_size}, msg_count={msg_count}, "
            f"partiton_count={partition_count}, rpk_response_timeout={rpk_response_timeout}, "
            f"produce_timeout={produce_timeout}")

        producer = RpkProducer(self.test_context,
                               self.redpanda,
                               self.topic,
                               msg_size=msg_size,
                               msg_count=msg_count,
                               printable=True,
                               produce_timeout=rpk_response_timeout,
                               max_message_bytes=msg_size * 2)
        producer.start()
        producer.wait(produce_timeout)
        producer.stop()
        producer.free()

        for consumer_type in Consumers:
            self.logger.info(f"Starting consumer {consumer_type.name}")

            consumers = []
            for k in range(2):
                if consumer_type == Consumers.RPK:
                    consumers.append(
                        RpkConsumer(self.test_context,
                                    self.redpanda,
                                    self.topic,
                                    fetch_max_bytes=1024 * 1024 * 3,
                                    num_msgs=msg_count))
                elif consumer_type == Consumers.KAF:
                    consumers.append(
                        KafConsumer(self.test_context,
                                    self.redpanda,
                                    self.topic,
                                    offset_for_read="oldest",
                                    num_records=msg_count))
                elif consumer_type == Consumers.VERIFIABLE:
                    if k == 0:  # more than one are not stable enough
                        consumers.append(
                            VerifiableConsumer(self.test_context,
                                               1,
                                               self.redpanda,
                                               self.topic,
                                               "verifiable-group",
                                               max_messages=msg_count))
                else:
                    assert False, "unsupported consumer type"
            for consumer in consumers:
                consumer.start()
            for consumer in consumers:
                consumer.wait()
            for consumer in consumers:
                consumer.stop()
            for consumer in consumers:
                consumer.free()

        # TBD: for faster detection of redpanda crash, have a check in all
        # redpanda nodes that the process is still alive and abort the test
        # if it is not

        # TBD: add librdkafa based consumer
