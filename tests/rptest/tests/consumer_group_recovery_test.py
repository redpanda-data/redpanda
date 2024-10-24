# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import re
import time
from collections import defaultdict
from dataclasses import astuple, dataclass

from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.redpanda import SISettings
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.util import wait_until_result
from rptest.utils.si_utils import BucketView
from rptest.utils.si_utils import quiesce_uploads


@dataclass(frozen=True)
class TopicPartitionOffset:
    topic: str
    partition: int
    offset: int

    def __iter__(self):
        return iter(astuple(self))

    def prev(self):
        return TopicPartitionOffset(self.topic, self.partition,
                                    self.offset - 1)


class ConsumerOffsetsRecoveryTest(PreallocNodesTest):
    def __init__(self, test_ctx, *args):
        self._ctx = test_ctx
        self.initial_partition_count = 3
        super(ConsumerOffsetsRecoveryTest, self).__init__(
            test_ctx,
            num_brokers=3,
            *args,
            extra_rp_conf={
                "kafka_nodelete_topics": [],
                "group_topic_partitions": self.initial_partition_count,
                'controller_snapshot_max_age_sec': 1,
                "enable_cluster_metadata_upload_loop": True,
                "cloud_storage_cluster_metadata_upload_interval_ms": 1000,
            },
            node_prealloc_count=1,
            si_settings=SISettings(
                test_ctx,
                cloud_storage_segment_max_upload_interval_sec=1,
                fast_uploads=True,
            ))

    def describe_all_groups(self,
                            num_groups: int = 1
                            ) -> dict[str, set[TopicPartitionOffset]]:
        rpk = RpkTool(self.redpanda)
        all_groups = defaultdict(lambda: set())

        # The one consumer group in this test comes from KgoVerifierConsumerGroupConsumer
        # for example, kgo-verifier-1691097745-347-0
        kgo_group_re = re.compile(r'^kgo-verifier-[0-9]+-[0-9]+-0$')

        self.logger.debug(f"Issue ListGroups, expect {num_groups} groups")

        def do_list_groups():
            res = rpk.group_list_names()

            if res is None:
                return False

            if len(res) != num_groups:
                return False

            kgo_group_m = kgo_group_re.match(res[0])
            self.logger.debug(f"kgo group match {kgo_group_m}")
            return False if kgo_group_m is None else (True, res)

        group_list_res = wait_until_result(
            do_list_groups,
            timeout_sec=30,
            backoff_sec=0.5,
            err_msg="RPK failed to list consumer groups")

        for g in group_list_res:
            gd = rpk.group_describe(g)
            for p in gd.partitions:
                all_groups[gd.name].add(
                    TopicPartitionOffset(topic=p.topic,
                                         partition=p.partition,
                                         offset=p.current_offset))

        return all_groups

    def bucket_has_consumer_offsets(self):
        try:
            s3_snapshot = BucketView(self.redpanda, topics=self.topics)
            latest_manifest = s3_snapshot.latest_cluster_metadata_manifest
            if len(latest_manifest["offsets_snapshots_by_partition"]) == 0:
                return False
        except Exception as e:
            self.logger.warn(f"Error while populating snapshot: {str(e)}")
            return False
        return True

    @cluster(num_nodes=4)
    def test_consumer_offsets_partition_recovery(self):
        partition_count = 16
        topic = TopicSpec(partition_count=partition_count,
                          replication_factor=3)
        self.client().create_topic([topic])
        msg_size = 1024
        msg_cnt = 10000

        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic.name,
                                       msg_size,
                                       msg_cnt,
                                       custom_node=self.preallocated_nodes)

        producer.start(clean=False)

        wait_until(lambda: producer.produce_status.acked > 10,
                   timeout_sec=30,
                   backoff_sec=0.5)

        consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size,
            readers=3,
            nodes=self.preallocated_nodes)
        consumer.start(clean=False)

        producer.wait()
        consumer.wait()

        quiesce_uploads(self.redpanda, [topic.name], timeout_sec=60)
        wait_until(self.bucket_has_consumer_offsets,
                   timeout_sec=60,
                   backoff_sec=5)
        time.sleep(10)

        groups_pre_restore = self.describe_all_groups()
        self.logger.debug(f"Groups pre-restore {groups_pre_restore}")

        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)

        self.redpanda.si_settings.set_expected_damage(
            {"ntr_no_topic_manifest"})
        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    auto_assign_node_id=True,
                                    omit_seeds_on_idx_one=False)
        self.redpanda._admin.await_stable_leader("controller",
                                                 partition=0,
                                                 namespace='redpanda',
                                                 timeout_s=60,
                                                 backoff_s=2)

        self.redpanda._admin.initialize_cluster_recovery()

        def cluster_recovery_complete():
            return "inactive" in self.redpanda._admin.get_cluster_recovery_status(
            ).json()["state"]

        wait_until(cluster_recovery_complete, timeout_sec=60, backoff_sec=1)

        groups_post_restore = self.describe_all_groups()
        self.logger.debug(f"Groups post-restore {groups_post_restore}")
        assert groups_post_restore == groups_pre_restore, f"{groups_post_restore} vs {groups_pre_restore}"

        self.run_workload_after_restore(groups_pre_restore, partition_count,
                                        topic)

    def run_workload_after_restore(self, groups, partition_count, topic):
        rpk = RpkTool(self.redpanda)
        group = next((k for k in groups.keys() if k.startswith('kgo-')), None)
        assert group is not None, f'Missing kgo group in group description'

        tp_offsets = groups[group]
        for (t, part, _) in tp_offsets:
            # Produce one message per partition, so we can consume n=partition_count messages later
            # and verify the offset per partition
            produce_result = rpk.produce(t,
                                         key=f'{part}',
                                         msg=f'{part}',
                                         partition=part)
            self.logger.debug(f'{produce_result=}')

        # Use the same group as the original `kgo-` group, so we start reading from wherever __consumer_offsets
        # points to per partition for our group.
        for line in rpk.consume(topic.name,
                                n=partition_count,
                                group=group,
                                format='%t,%p,%o\n').strip().splitlines():
            tokens = line.split(',')
            # The offset we read for our group+partition should match the group description pre-restore
            assert TopicPartitionOffset(
                topic=tokens[0],
                partition=int(tokens[1]),
                offset=int(tokens[2])
            ) in tp_offsets, f'Topic, partition and offset {line} missing from committed offsets: {tp_offsets}'

        groups_desc = self.describe_all_groups()
        assert group in groups_desc
        for latest_tp_offset in groups_desc[group]:
            assert latest_tp_offset not in tp_offsets, (
                f'{latest_tp_offset} found in {tp_offsets} '
                f'after consuming more records for partition')
            # After new consume for the group, the offsets should have been incremented
            assert latest_tp_offset.prev(
            ) in tp_offsets, f'partition offset did not increment correctly after consume: {latest_tp_offset}'
