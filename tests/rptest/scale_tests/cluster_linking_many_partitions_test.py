from ducktape.mark import matrix

from rptest.tests.cluster_linking_test_base import (
    ALL_STORAGE_MODES,
    ClusterLinkingProgressVerifier,
    ShadowLinkPreAllocTestBase,
)
from rptest.clients.default import TopicSpec
from rptest.services.cluster import TestContext, cluster
from rptest.services.multi_cluster_services import SecondaryClusterArgs
from rptest.utils.scale_parameters import ScaleParameters

# A cloud-topic partition read is latency-bound: a per-object metastore lookup,
# a footer read, and an object-store GET. At the default
# fetch_max_read_concurrency=1 a fetch reads its partitions serially, so at
# topic_count=1, where the whole partition budget lands on one topic, reads miss
# the consumer's fetch deadline and it stops making progress entirely. A little
# concurrency pipelines the reads and hides the per-read round trip.
#
# Both clusters need it. Each serves a consumer over the full partition count:
# the target additionally mirrors every partition, and the source is marginal on
# its own (it finished 172 messages short of the workload in one run and stalled
# 771 short in the next).
#
# Raising it is not free, so only the modes that read through the object store
# get it. The max_bytes short-circuit in the fetch path races across concurrent
# reads, and up to fetch_max_read_concurrency reads count as the obligatory
# batch read, so a fetch can buffer more than it was asked for. local and
# tiered_v1 would pay that for nothing: their reads were never latency-bound,
# and both pass at the default.
FETCH_CONCURRENCY_CONF = {"fetch_max_read_concurrency": 4}
CLOUD_TOPIC_STORAGE_MODES = (
    TopicSpec.STORAGE_MODE_CLOUD,
    TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
)


class ClusterLinkingScaleTest(ShadowLinkPreAllocTestBase):
    def __init__(self, test_context: TestContext):
        storage_mode = (test_context.injected_args or {}).get("storage_mode")
        reads_via_object_store = storage_mode in CLOUD_TOPIC_STORAGE_MODES
        if reads_via_object_store:
            secondary_args = SecondaryClusterArgs(
                extra_rp_conf=dict(FETCH_CONCURRENCY_CONF)
            )
        else:
            secondary_args = SecondaryClusterArgs()
        super().__init__(
            test_context=test_context,
            secondary_cluster_args=secondary_args,
        )
        if reads_via_object_store:
            self.redpanda.add_extra_rp_conf(dict(FETCH_CONCURRENCY_CONF))
        self.scale = ScaleParameters(
            self.redpanda,
            replication_factor=3,
            mib_per_partition=ScaleParameters.DEFAULT_MIB_PER_PARTITION,
            topic_replicas_per_shard=2000,
            tiered_storage_enabled=False,
            partition_memory_reserve_percentage=ScaleParameters.DEFAULT_PARTITIONS_MEMORY_ALLOCATION_PERCENT,
        )

    @cluster(num_nodes=7)
    @matrix(topic_count=[1, 5, 10], storage_mode=ALL_STORAGE_MODES)
    def test_many_partitions(self, topic_count: int, storage_mode: str):
        topics = [
            TopicSpec(
                name=f"source-topic-{i}",
                partition_count=int(self.scale.partition_limit / topic_count),
                replication_factor=3,
            )
            for i in range(topic_count)
        ]

        self.create_link("many_partitions_link")

        for topic in topics:
            self.create_source_topic(topic, storage_mode)

        total_bytes = 5 * 1024 * 1024 * 1024  # 5GB
        msg_size = 4 * 1024
        msg_count = int(total_bytes / msg_size)
        verifier = ClusterLinkingProgressVerifier(
            self.test_context,
            self.source_cluster,
            self.target_cluster,
            topic=topics[0].name,
            preallocated_nodes=self.preallocated_nodes,
            logger=self.logger,
            msg_count=msg_count,
            msg_size=msg_size,
        )

        verifier.start()

        success, error_msg = verifier.wait_and_verify()
        assert success, f"Verification failed: {error_msg}"
