from ducktape.mark import matrix
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

    def setUp(self):
        pass

    @cluster(num_nodes=6)
    @matrix(write_caching=["on", "off"])
    def omb_test(self, write_caching):
        self.redpanda.add_extra_rp_conf({"write_caching": write_caching})
        self.redpanda.start()

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
        validator = {
            OMBSampleConfigurations.E2E_LATENCY_50PCT:
            [OMBSampleConfigurations.lte(30)],
            OMBSampleConfigurations.E2E_LATENCY_AVG:
            [OMBSampleConfigurations.lte(75)],
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS:
            [OMBSampleConfigurations.gte(145)],
            OMBSampleConfigurations.PUB_LATENCY_50PCT:
            [OMBSampleConfigurations.lte(40)],
        }

        benchmark = OpenMessagingBenchmark(self._ctx, self.redpanda,
                                           "ACK_ALL_GROUP_LINGER_1MS",
                                           (workload, validator))

        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
