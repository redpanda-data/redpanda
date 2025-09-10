from ducktape.mark import matrix

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.redpanda_test import RedpandaTest


class UsageStatsReportingTest(RedpandaTest):
    @cluster(num_nodes=4)
    @matrix(replication_factor=[1, 3])
    def test_usage_stats_reporting(self, replication_factor):
        topic = TopicSpec(partition_count=1, replication_factor=replication_factor)
        self.client().create_topic(topic)
        stats = self.redpanda.usage_stats
        # validate stats are initially 0
        assert (
            stats.batches_read == 0
            and stats.batches_written == 0
            and stats.disk_bytes_read == 0
            and stats.disk_bytes_written == 0
            and stats.internal_rpc_bytes_recv == 0
            and stats.internal_rpc_bytes_sent == 0
            and stats.cloud_storage_gets == 0
            and stats.cloud_storage_puts == 0
        ), "Before restarting Redpanda, the usage stats should be 0"

        # restart nodes to trigger usage stats reporting
        self.redpanda.restart_nodes(self.redpanda.nodes)
        initial_stats = self.redpanda.usage_stats_dict

        # validate stats are non-zero, some data was stored and read during
        # cluster initialization
        stats = self.redpanda.usage_stats
        assert (
            stats.batches_read > 0
            and stats.batches_written > 0
            and stats.disk_bytes_read > 0
            and stats.disk_bytes_written > 0
        ), "After initial restart the usage stats should be greater than 0"
        if replication_factor > 1:
            assert (
                stats.internal_rpc_bytes_recv > 0 and stats.internal_rpc_bytes_sent > 0
            ), "After initial restart the internal rpc stats should be greater than 0"

        total_messages = 128
        msg_size = 512
        batch_max_bytes = 512 * 2
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size=msg_size,
            msg_count=total_messages,
            batch_max_bytes=batch_max_bytes,
        )
        total_bytes = total_messages * msg_size
        total_batches = total_bytes // batch_max_bytes

        # stop all nodes
        for n in self.redpanda.nodes:
            self.redpanda.stop_node(n)
        stats_after_produce = self.redpanda.usage_stats_dict

        self.logger.info(
            f"initial usage: {initial_stats}, final usage: {stats_after_produce}"
        )
        batches_read = (
            stats_after_produce["batches_read"] - initial_stats["batches_read"]
        )
        batches_written = (
            stats_after_produce["batches_written"] - initial_stats["batches_written"]
        )
        disk_bytes_read = (
            stats_after_produce["disk_bytes_read"] - initial_stats["disk_bytes_read"]
        )
        disk_bytes_written = (
            stats_after_produce["disk_bytes_written"]
            - initial_stats["disk_bytes_written"]
        )
        internal_rpc_bytes_recv = (
            stats_after_produce["internal_rpc_bytes_recv"]
            - initial_stats["internal_rpc_bytes_recv"]
        )
        internal_rpc_bytes_sent = (
            stats_after_produce["internal_rpc_bytes_sent"]
            - initial_stats["internal_rpc_bytes_sent"]
        )

        self.logger.info(
            f"batches_read: {batches_read}, batches_written: {batches_written}, bytes_read: {disk_bytes_read}, bytes_written: {disk_bytes_written}, internal_rpc_bytes_recv: {internal_rpc_bytes_recv}, internal_rpc_bytes_sent: {internal_rpc_bytes_sent}"
        )

        assert batches_written >= total_batches * replication_factor, (
            f"Expected batches_written to be greater than or equal to {total_batches * replication_factor}, but got {batches_written}"
        )

        assert disk_bytes_written >= total_bytes * replication_factor, (
            f"Expected bytes_written to be greater than or equal to {total_bytes * replication_factor}, but got {disk_bytes_written}"
        )
