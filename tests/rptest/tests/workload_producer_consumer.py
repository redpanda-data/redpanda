# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Optional
import uuid
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services import redpanda
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer
from rptest.services.redpanda_installer import RedpandaVersionLine, RedpandaVersionTriple
from rptest.services.workload_protocol import PWorkload
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.si_utils import NTP, BucketView, quiesce_uploads


def all_done(*args) -> int:
    return PWorkload.DONE if all(res == PWorkload.DONE
                                 for res in args) else PWorkload.NOT_DONE


class ProducerConsumerWorkload(PWorkload):
    """
    This workload will setup a KGoProducer/Consumer to verify operations across an upgrade
    """
    def __init__(self, ctx: PreallocNodesTest) -> None:
        self.ctx = ctx
        self.rpk = RpkTool(self.ctx.redpanda)
        self.last_version_check: Optional[RedpandaVersionTriple] = None
        self.last_uploaded_kafka_offset = 0
        self.spillover_check_fn = self._spillover_manifest_check_generator()
        next(self.spillover_check_fn)  # go to first suspension point
        self.delete_record_check_fn = ProducerConsumerWorkload._once_per_version(
            lambda: self._delete_records_generator())
        next(self.delete_record_check_fn)  # go to first suspension point

        is_debug = self.ctx.debug_mode

        # setup a produce workload that should span the whole test run, and then add a buffer to it
        produce_byte_rate = 1024 * 1024  # 1Mb per second
        target_runtime_secs = 40 * 60
        self.MSG_SIZE = 1024
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
            #retention_bytes=
            #1024,  # this low value will indirectly speed up the upload of segments
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

    @staticmethod
    def _once_per_version(check_generator):
        # helper to run a test only once per redpanda_version.
        test_fn = check_generator()
        next(test_fn)

        redpanda_version = yield

        while True:
            #step 1: proxy phase, repeat until done
            while test_fn.send(redpanda_version) == PWorkload.NOT_DONE:
                yield PWorkload.NOT_DONE

            #step2: signal done, wait for next repdanda version
            new_redpanda_version = yield PWorkload.DONE
            while new_redpanda_version == redpanda_version:
                new_redpanda_version = yield PWorkload.DONE

            # reset the test function, restart loop
            redpanda_version = new_redpanda_version
            test_fn = check_generator()
            next(test_fn)

    def _delete_records(self, topic: str, partition: int,
                        truncate_offset: int):
        # lifted from delete_records_test, helper wrapper for kck.delete_records
        response = self.rpk.trim_prefix(topic,
                                        offset=truncate_offset,
                                        partitions=[partition])
        assert len(response) == 1
        assert response[0].error_msg == "", f"Err msg: {response[0].error_msg}"
        return response[0].new_start_offset

    def _spillover_manifest_check_generator(self):
        """
        Check in the form of a coroutine that waits for v23.1->v23.2 step to trigger spillover manifest production, by using aggressive settings temporarily
        """
        major_version = yield

        while False:
            # temp disable test
            yield PWorkload.DONE

        while major_version < (23, 1):
            # 23.1 does not have spillover manifests
            major_version = yield PWorkload.DONE

        # step 1: change params that are already available when cluster is 23.1
        self.ctx.logger.info(
            f"changing params of {self.topic.name} to trigger spillover manifests"
        )
        # 4kb for an increased segment count
        self.rpk.cluster_config_set("log_segment_size_min", str(4 * 1024))
        self.rpk.alter_topic_config(self.topic.name, 'segment.bytes', 4 * 1024)
        self.rpk.alter_topic_config(self.topic.name,
                                    'retention.local.target.bytes',
                                    5 * 1024 * 1024)

        while major_version < (23, 2):
            major_version = yield PWorkload.DONE

        # step 2: set new cluster configs to speed up the check
        new_cluster_configs = {
            "cloud_storage_idle_timeout_ms": "3000",
            "cloud_storage_housekeeping_interval_ms": "1000",
            "cloud_storage_spillover_manifest_max_segments": "10",
            #"cloud_storage_topic_purge_grace_period_ms": "5"
        }

        prev_cluster_configs = {}
        for k, v in new_cluster_configs.items():
            prev_cluster_configs[k] = self.rpk.cluster_config_get(k)
            self.rpk.cluster_config_set(k, v)

        bucket = BucketView(self.ctx.redpanda)
        spillover = None
        while spillover is None:
            # wait until spillover manifests are created
            yield PWorkload.NOT_DONE
            self.ctx.logger.info(
                f"waiting for spillover manifests of {self.topic.name} @{major_version=}"
            )
            spillover = bucket.get_spillover_manifests(
                NTP(ns='kafka', topic=self.topic.name, partition=0))

        self.ctx.logger.info(
            f"spillover manifests of {self.topic.name} @{major_version=} found"
        )
        # spillover manifests are created, restore topic cfg and disable this test
        for k, v in prev_cluster_configs.items():
            self.rpk.cluster_config_set(k, v)

        while True:
            yield PWorkload.DONE

    def _delete_records_generator(self):
        redpanda_version: RedpandaVersionTriple = yield
        # checks that delete-records moves forward the segment offset delta
        # for each version, tries to delete ~50 records
        if redpanda_version[0:2] < (23, 2):
            redpanda_version = yield PWorkload.DONE

        # step 1: compute truncation_offset to be approx start_offset+50
        while True:
            partition_zero = next(self.rpk.describe_topic(self.topic.name))
            truncation_offset = min(partition_zero.start_offset + 20,
                                    partition_zero.high_watermark - 5)
            if truncation_offset <= partition_zero.start_offset:
                # can happen, if not enough data is in yet
                yield PWorkload.NOT_DONE
                continue  # retry

            break

        new_low_watermark = self._delete_records(self.topic.name,
                                                 partition_zero.id,
                                                 truncation_offset)
        assert new_low_watermark == truncation_offset, f"Expected low watermark: {truncation_offset=} observed: {new_low_watermark=}"

        # check results of delete_records
        partition_zero = next(self.rpk.describe_topic(self.topic.name))
        assert partition_zero.id == 0, f"Partition id: {partition_zero.id}"
        assert partition_zero.start_offset == truncation_offset, f"Start offset: {partition_zero.start_offset}"
        # check that offset=truncation_offset can be read
        self.rpk.consume(self.topic.name,
                         partition=partition_zero.id,
                         quiet=True,
                         offset=f"{truncation_offset}:{truncation_offset+1}")

        yield PWorkload.DONE

    def on_cluster_upgraded(self, version: RedpandaVersionTriple) -> int:
        """
        query remote storage and compute uploaded_kafka_offset, to check that progress is made
        """
        if self.last_version_check is None or self.last_version_check != version:
            # perform quiesce only once per version
            quiesce_uploads(self.ctx.redpanda, [self.topic.name], 300)
            self.last_version_check = version

        major_version = version[0:2]
        # get the manifest for a topic and do some checking
        topic_description = self.rpk.describe_topic(self.topic.name)
        partition_zero = next(topic_description)

        bucket = BucketView(self.ctx.redpanda)
        remote_manifest = bucket.manifest_for_ntp(self.topic.name,
                                                  partition_zero.id)

        def _producers_is_making_progress():
            # checks that over time, redpanda uploads new data on cloud storage

            if not ("segments" in remote_manifest
                    and len(remote_manifest['segments']) > 0):
                # no segments uploaded yet
                return PWorkload.NOT_DONE

            # retrieve highest committed kafka offset and check that progress is made
            top_segment = max(remote_manifest['segments'].values(),
                              key=lambda seg: seg['base_offset'])
            uploaded_raft_offset = top_segment['committed_offset']
            uploaded_kafka_offset = uploaded_raft_offset - top_segment[
                'delta_offset_end']

            if uploaded_kafka_offset > self.last_uploaded_kafka_offset:
                self.last_uploaded_kafka_offset = uploaded_kafka_offset
                return PWorkload.DONE

            return PWorkload.NOT_DONE

        def _manifest_correct_version():
            # 23.2 starts to use the new manifest. wait until it gets uploaded
            if major_version < (23, 2):
                assert remote_manifest[
                    'version'] <= 1, f"Manifest version {remote_manifest['version']} is not <= 1"
                return PWorkload.DONE

            if remote_manifest['version'] < 2:
                return PWorkload.NOT_DONE

            return PWorkload.DONE

        return all_done(_producers_is_making_progress(),
                        _manifest_correct_version(),
                        self.spillover_check_fn.send(major_version),
                        self.delete_record_check_fn.send(version))
