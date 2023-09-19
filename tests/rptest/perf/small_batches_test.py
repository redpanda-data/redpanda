from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import OMBSampleConfigurations
from rptest.services.cluster import cluster


class SmallBatchesTest(RedpandaTest):
    """
    A many clients and partitions test where producers send small batches to Redpanda.
    """
    def __init__(self, ctx):
        self._ctx = ctx
        super(SmallBatchesTest,
              self).__init__(test_context=ctx,
                             extra_rp_conf={"aggregate_metrics": True})

    @cluster(num_nodes=6)
    def omb_test(self):
        workload = {
            "name": "SmallBatchesWorkload",
            "topics": 1,
            "partitions_per_topic": 20000,
            "subscriptions_per_topic": 1,
            "consumer_per_subscription": 200,
            "producers_per_topic": 200,
            "producer_rate": 150_000,
            "message_size": 1024,
            "payload_file": "payload/payload-1Kb.data",
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": 5,
            "warmup_duration_minutes": 1,
        }

        benchmark = OpenMessagingBenchmark(
            self._ctx, self.redpanda, "ACK_ALL_GROUP_LINGER_1MS",
            (workload, OMBSampleConfigurations.UNIT_TEST_LATENCY_VALIDATOR))

        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
