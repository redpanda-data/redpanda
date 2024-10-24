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
            "partitions_per_topic": 100,
            "subscriptions_per_topic": 1,
            "consumer_per_subscription": 1,
            "producers_per_topic": 10,
            "producer_rate": 30_000,
            "message_size": 100,
            "payload_file": "payload/payload-100b.data",
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": 2,
            "warmup_duration_minutes": 2,
        }
        driver = {
            "name": "SmallBatchesDriver",
            "replication_factor": 3,
            "request_timeout": 300000,
            "producer_config": {
                "enable.idempotence": "true",
                "acks": "all",
                "linger.ms": 1,
                "max.in.flight.requests.per.connection": 5,
                "batch.size": 1,
            },
            "consumer_config": {
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                "max.partition.fetch.bytes": 131072
            },
        }
        validator = {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS:
            [OMBSampleConfigurations.gte(2)],
        }

        benchmark = OpenMessagingBenchmark(ctx=self._ctx,
                                           redpanda=self.redpanda,
                                           driver=driver,
                                           workload=(workload, validator),
                                           topology="ensemble")

        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
