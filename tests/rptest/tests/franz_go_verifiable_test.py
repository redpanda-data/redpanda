# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster_spec import ClusterSpec

import time
import random
import uuid

from rptest.archival.s3_client import S3Client
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableProducer, FranzGoVerifiableSeqConsumer, FranzGoVerifiableRandomConsumer


class SiWithFranzGoTest(RedpandaTest):
    MSG_SIZE = 120000

    segment_size = 16 * 1048576
    s3_host_name = "minio-s3"
    s3_access_key = "panda-user"
    s3_secret_key = "panda-secret"
    s3_region = "panda-region"
    s3_topic_name = "panda-topic"

    topics = (TopicSpec(partition_count=100, replication_factor=3), )

    def __init__(self, ctx):
        self.s3_bucket_name = f"panda-bucket-{uuid.uuid1()}"
        super(SiWithFranzGoTest, self).__init__(
            test_context=ctx,
            num_brokers=3,
            extra_rp_conf={
                # Disable prometheus metrics, because we are doing lots
                # of restarts with lots of partitions, and current high
                # metric counts make that sometimes cause reactor stalls
                # during shutdown on debug builds.
                'disable_metrics': True,

                # We will run relatively large number of partitions
                # and want it to work with slow debug builds and
                # on noisy developer workstations: relax the raft
                # intervals
                'election_timeout_ms': 5000,
                'raft_heartbeat_interval_ms': 500,

                # Cloud storage config
                'log_segment_size': self.segment_size,
                'cloud_storage_enabled': True,
                'cloud_storage_access_key': self.s3_access_key,
                'cloud_storage_secret_key': self.s3_secret_key,
                'cloud_storage_region': self.s3_region,
                'cloud_storage_bucket': self.s3_bucket_name,
                'cloud_storage_disable_tls': True,
                'cloud_storage_api_endpoint': self.s3_host_name,
                'cloud_storage_api_endpoint_port': 9000,
                'cloud_storage_cache_size': self.segment_size * 10
            })

        self.redpanda_nodes = self.redpanda.nodes

        self.s3client = S3Client(
            region=self.s3_region,
            access_key=self.s3_access_key,
            secret_key=self.s3_secret_key,
            endpoint=f"http://{self.s3_host_name}:9000",
            logger=self.logger,
        )
        self.s3client.create_bucket(self.s3_bucket_name)

        self._node_for_franz_go = ctx.cluster.alloc(
            ClusterSpec.simple_linux(1))

        self._producer = FranzGoVerifiableProducer(ctx, self.redpanda,
                                                   self.topic,
                                                   SiWithFranzGoTest.MSG_SIZE,
                                                   1000000000,
                                                   self._node_for_franz_go)
        self._seq_consumer = FranzGoVerifiableSeqConsumer(
            ctx, self.redpanda, self.topic, SiWithFranzGoTest.MSG_SIZE,
            self._node_for_franz_go)
        self._rand_consumer = FranzGoVerifiableRandomConsumer(
            ctx, self.redpanda, self.topic, SiWithFranzGoTest.MSG_SIZE, 10000,
            20, self._node_for_franz_go)

    def tearDown(self):
        failed_deletions = self.s3client.empty_bucket(self.s3_bucket_name)
        assert len(failed_deletions) == 0
        self.s3client.delete_bucket(self.s3_bucket_name)

    # In the future producer will signal about json creation
    def _create_json_file(self):
        small_producer = FranzGoVerifiableProducer(self.test_context,
                                                   self.redpanda, self.topic,
                                                   SiWithFranzGoTest.MSG_SIZE,
                                                   10000,
                                                   self._node_for_franz_go)
        small_producer.start()
        small_producer.wait()

    @cluster(num_nodes=5)
    def test_with_all_type_of_loads_and_si(self):

        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic, 'redpanda.remote.write', 'true')
        rpk.alter_topic_config(self.topic, 'redpanda.remote.read', 'true')
        rpk.alter_topic_config(self.topic, 'retention.bytes',
                               str(self.segment_size * 2))

        # Need create json file for consumer at first
        self._create_json_file()

        # Verify that we really enabled shadow indexing correctly, such
        # that some objects were written
        objects = list(self.s3client.list_objects(self.s3_bucket_name))
        assert len(objects) > 0
        for o in objects:
            self.logger.info(f"S3 object: {o.Key}, {o.ContentLength}")

        self._producer.start()

        # Produce some data
        time.sleep(10)

        self._seq_consumer.start()
        self._rand_consumer.start()

        for i in range(30):
            node_for_restart = random.randrange(len(self.redpanda_nodes))
            self.redpanda.restart_nodes(self.redpanda_nodes[node_for_restart],
                                        start_timeout=200,
                                        stop_timeout=200)
            time.sleep(30)
