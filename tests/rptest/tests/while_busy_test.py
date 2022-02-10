# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# December 2021, engineers were tasked to identify failures with one of Redpanda's latest features, shadow indexing.
# Many new tests and workloads were created as a result of this effort. These tests, however, were setup and run manually.
# Within this file are those same tests but written and run as ducktape tests.

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster_spec import ClusterSpec

import os
import time
import requests
import random
from math import ceil

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableProducer, FranzGoVerifiableSeqConsumer, FranzGoVerifiableRandomConsumer
from rptest.services.franz_go_verifiable_services import ServiceStatus


class WhileBusyTest(RedpandaTest):
    # Run common procedures against the cluster with SI enabled
    # while the system is under load (busy).

    # Use class variables for constant values
    MSG_SIZE = 1000000
    SEGMENT_SIZE = 5 * 1000000
    RAND_READ_MSGS = 1000
    PARALLAL_CONS = 20
    RETENTION_MS = 60000

    topics = (TopicSpec(partition_count=100, replication_factor=3), )

    def __init__(self, ctx):
        si_settings = SISettings(log_segment_size=WhileBusyTest.SEGMENT_SIZE,
                                 cloud_storage_cache_size=5 *
                                 WhileBusyTest.SEGMENT_SIZE)

        super(WhileBusyTest, self).__init__(test_context=ctx,
                                            extra_rp_conf={
                                                'disable_metrics': True,
                                                'election_timeout_ms': 5000,
                                                'raft_heartbeat_interval_ms':
                                                500,
                                            },
                                            si_settings=si_settings)

        self._random_topics = []
        self._msg_count = 20000

        # The timeout for creating or deleting topics in a loop.
        # Locally, in a ducktape env, it takes approx 220s for the producer
        # to complete 20k messages. That is approx 90 messages per second.
        # Base the timeout on this rate.
        self._topic_create_del_timeout = ceil(self._msg_count / 90)

        self._frgo_node = ctx.cluster.alloc(ClusterSpec.simple_linux(1))
        self.logger.debug(f"Allocated verifier node {self._frgo_node[0].name}")

        self._producer = FranzGoVerifiableProducer(ctx, self.redpanda,
                                                   self.topic,
                                                   WhileBusyTest.MSG_SIZE,
                                                   self._msg_count,
                                                   self._frgo_node)
        self._seq_consumer = FranzGoVerifiableSeqConsumer(
            ctx, self.redpanda, self.topic, WhileBusyTest.MSG_SIZE,
            self._frgo_node)
        # self._rand_consumer = FranzGoVerifiableRandomConsumer(
        #     ctx, self.redpanda, self.topic, WhileBusyTest.MSG_SIZE,
        #     WhileBusyTest.RAND_READ_MSGS, WhileBusyTest.PARALLAL_CONS,
        #     self._frgo_node)

    def free_nodes(self):
        # Free the normally allocated nodes (e.g. RedpandaService)
        super().free_nodes()

        assert len(self._frgo_node) == 1

        # The verifier opens huge numbers of connections, which can interfere
        # with subsequent tests' use of the node.  Clear them down first.
        wait_until(lambda: self.redpanda.sockets_clear(self._frgo_node[0]),
                   timeout_sec=120,
                   backoff_sec=10)

        # Free the hand-allocated node that we share between the various
        # verifier services
        self.logger.debug(f"Freeing verifier node {self._frgo_node[0].name}")
        self.test_context.cluster.free_single(self._frgo_node[0])

    # In the future producer will signal about json creation
    def _create_json_file(self, msg_count):
        small_producer = FranzGoVerifiableProducer(self.test_context,
                                                   self.redpanda, self.topic,
                                                   WhileBusyTest.MSG_SIZE,
                                                   msg_count, self._frgo_node)
        small_producer.start()
        small_producer.wait()

    def _setup_remote_io(self):
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic, 'redpanda.remote.write', 'true')
        rpk.alter_topic_config(self.topic, 'redpanda.remote.read', 'true')
        rpk.alter_topic_config(self.topic, 'retention.bytes',
                               str(WhileBusyTest.SEGMENT_SIZE))
        rpk.alter_topic_config(self.topic, 'retention.ms',
                               str(WhileBusyTest.RETENTION_MS))

        # Need create json file for consumer at first
        self._create_json_file(10000)

        # Verify that we really enabled shadow indexing correctly, such
        # that some objects were written
        objects = list(self.redpanda.get_objects_from_si())
        assert len(objects) > 0
        for o in objects:
            self.logger.info(f"S3 object: {o.Key}, {o.ContentLength}")

    @cluster(num_nodes=4)
    def test_create_or_delete_topics_while_busy(self):
        self.logger.info(f"Environment: {os.environ}")
        if os.environ.get('BUILD_TYPE', None) == 'debug':
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        self._setup_remote_io()

        self._producer.start(clean=False)
        self._seq_consumer.start(clean=False)

        # self._rand_consumer.start(clean=False)

        # Ensure the services are running before the test continues
        def services_are_running():
            self.logger.info(
                f"Prod Status: {self._producer.status}, SEQ Cons Status: {self._seq_consumer.status}"
            )
            return self._producer.status == ServiceStatus.RUNNING and self._seq_consumer.status == ServiceStatus.RUNNING

        wait_until(services_are_running,
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg="One of the services is not running")

        # Now that the system is busy, randomly create and delete topics
        # until the producer is finished.

        def create_or_delete_until_producer_fin():
            trigger = random.randint(1, 6)

            if trigger == 2:
                some_topic = TopicSpec()
                self.logger.info(
                    f"Create topic: {some_topic}, Prod Status: {self._producer.status}"
                )
                self.client().create_topic(some_topic)
                self._random_topics.append(some_topic)

            if trigger == 3:
                if len(self._random_topics) > 0:
                    some_topic = self._random_topics.pop()
                    self.logger.info(
                        f"Delete topic: {some_topic}, Prod Status: {self._producer.status}"
                    )
                    self.client().delete_topic(some_topic.name)

            if trigger == 4:
                random.shuffle(self._random_topics)

            return self._producer.status == ServiceStatus.FINISH

        wait_until(
            create_or_delete_until_producer_fin,
            timeout_sec=self._topic_create_del_timeout,
            backoff_sec=1,
            err_msg=
            f"The producer didn't finish. Prod Status: {self._producer.status}"
        )

        self._producer.wait()
        self._seq_consumer.shutdown()
        # self._rand_consumer.shutdown()
        self._seq_consumer.wait(timeout_sec=1200)
        # self._rand_consumer.wait(timeout_sec=1200)
