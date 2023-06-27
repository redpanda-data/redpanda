# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import uuid
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer
from rptest.services.workload_protocol import PWorkload
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.si_utils import BucketView, quiesce_uploads


class ProducerConsumerWorkload(PWorkload):
    """
    This workload will setup a KGoProducer/Consumer to verify operations across an upgrade
    """
    def __init__(self, ctx: PreallocNodesTest) -> None:
        self.ctx = ctx
        self.rpk = RpkTool(self.ctx.redpanda)
        self.last_uploaded_kafka_offset = 0

        is_debug = self.ctx.debug_mode

        # setup a produce workload that should span the whole test run, and then add a buffer to it
        produce_byte_rate = 256 * 1024  # a quarter of Mb per second
        target_runtime_secs = 20 * 60
        self.MSG_SIZE = 256
        self.PRODUCE_COUNT = (produce_byte_rate *
                              target_runtime_secs) // self.MSG_SIZE
        # the topic is requires less resources in debug mode for the sake of the test
        self.topic = TopicSpec(
            name=f"topic-{self.__class__.__name__}-{str(uuid.uuid4())}",
            partition_count=1 if is_debug else 3,
            replication_factor=1 if is_debug else 3,
            redpanda_remote_write=True,
            redpanda_remote_read=True,
            segment_bytes=1024 * 1024,
            retention_bytes=
            1024,  # this low value will indirectly speed up the upload of segments
        )

        self._producer = KgoVerifierProducer(
            self.ctx.test_context,
            self.ctx.redpanda,
            self.topic,
            msg_size=self.MSG_SIZE,
            msg_count=self.PRODUCE_COUNT,
            rate_limit_bps=produce_byte_rate,
            custom_node=self.ctx.preallocated_nodes)
        self._seq_consumer = KgoVerifierSeqConsumer(
            self.ctx.test_context,
            self.ctx.redpanda,
            self.topic,
            self.MSG_SIZE,
            nodes=self.ctx.preallocated_nodes)

    def begin(self):
        self.rpk.cluster_config_set("cloud_storage_enabled", "true")
        # ensure cloud storage is enabled
        assert self.rpk.cluster_config_get("cloud_storage_enabled") == "true"

        self.ctx.client().create_topic(self.topic)
        # double check the topic is configured correctly
        topic_cfg = self.ctx.client().describe_topic_configs(self.topic.name)
        assert topic_cfg["redpanda.remote.read"].value == "true" and topic_cfg[
            "redpanda.remote.write"].value == "true"

        self._producer.start(clean=False)
        self._producer.wait_for_offset_map()
        self._seq_consumer.start(clean=False)

    def end(self):
        self._producer.stop()
        self._producer.wait()
        self._seq_consumer.wait()
        wrote_at_least = self._producer.produce_status.acked
        assert self._seq_consumer.consumer_status.validator.valid_reads >= wrote_at_least

        self.ctx.client().delete_topic(self.topic.name)

    def get_earliest_applicable_release(self):
        return (22, 3)

    def on_cluster_upgraded(self, version: tuple[int, int, int]) -> int:
        """
        query remote storage and compute uploaded_kafka_offset, to check that progress is made
        """
        major_version = version[0:2]
        quiesce_uploads(self.ctx.redpanda, [self.topic.name], 120)

        # get the manifest for a topic and do some checking
        topic_description = self.rpk.describe_topic(self.topic.name)
        partition_zero = next(topic_description)

        bucket = BucketView(self.ctx.redpanda)
        manifest = bucket.manifest_for_ntp(self.topic.name, partition_zero.id)

        if major_version < (23, 2):
            assert manifest[
                'version'] <= 1, f"Manifest version {manifest['version']} is not <= 1"
        else:
            # 23.2 starts to use the new manifest. wait until it gets uploaded
            if manifest['version'] < 2:
                return PWorkload.NOT_DONE

        if not ("segments" in manifest and len(manifest['segments']) > 0):
            # no segments uploaded yet
            return PWorkload.NOT_DONE

        # retrieve highest committed kafka offset and check that progress is made
        top_segment = max(manifest['segments'].values(),
                          key=lambda seg: seg['base_offset'])
        uploaded_raft_offset = top_segment['committed_offset']
        uploaded_kafka_offset = uploaded_raft_offset - top_segment[
            'delta_offset_end']

        if uploaded_kafka_offset > self.last_uploaded_kafka_offset:
            self.last_uploaded_kafka_offset = uploaded_kafka_offset
            return PWorkload.DONE

        return PWorkload.NOT_DONE
