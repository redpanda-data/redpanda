# taken from https://github.com/apache/kafka @ 9368743

import json
from ducktape.tests.test import Test
from kafkatest.services.trogdor.produce_bench_workload import ProduceBenchWorkloadService, ProduceBenchWorkloadSpec
from kafkatest.services.trogdor.task_spec import TaskSpec
from kafkatest.services.trogdor.trogdor import TrogdorService
from rp.ducktape.services.redpanda import RedpandaService


class ProduceBenchTest(Test):
    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ProduceBenchTest, self).__init__(test_context)
        self.redpanda = RedpandaService(test_context, num_nodes=3)
        self.workload_service = ProduceBenchWorkloadService(
            test_context, self.redpanda)
        self.trogdor = TrogdorService(
            context=self.test_context,
            client_services=[self.redpanda, self.workload_service])
        self.active_topics = {
            "produce_bench_topic[0-1]": {
                "numPartitions": 1,
                "replicationFactor": 3
            }
        }
        self.inactive_topics = {
            "produce_bench_topic[2-9]": {
                "numPartitions": 1,
                "replicationFactor": 3
            }
        }

    def setUp(self):
        self.trogdor.start()
        self.redpanda.start()

    def teardown(self):
        self.trogdor.stop()
        self.redpanda.stop()

    def test_produce_bench(self):
        spec = ProduceBenchWorkloadSpec(
            0,
            TaskSpec.MAX_DURATION_MS,
            self.workload_service.producer_node,
            self.workload_service.bootstrap_servers,
            target_messages_per_sec=1000,
            max_messages=100000,
            producer_conf={},
            admin_client_conf={},
            common_client_conf={},
            inactive_topics=self.inactive_topics,
            active_topics=self.active_topics)
        workload1 = self.trogdor.create_task("workload1", spec)
        workload1.wait_for_done(timeout_sec=360)
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" %
                         json.dumps(tasks, sort_keys=True, indent=2))

    def test_produce_bench_transactions(self):
        spec = ProduceBenchWorkloadSpec(
            0,
            TaskSpec.MAX_DURATION_MS,
            self.workload_service.producer_node,
            self.workload_service.bootstrap_servers,
            target_messages_per_sec=1000,
            max_messages=100000,
            producer_conf={},
            admin_client_conf={},
            common_client_conf={},
            inactive_topics=self.inactive_topics,
            active_topics=self.active_topics,
            transaction_generator={
                # 10 transactions with 10k messages
                "type": "uniform",
                "messagesPerTransaction": "10000"
            })
        workload1 = self.trogdor.create_task("workload1", spec)
        workload1.wait_for_done(timeout_sec=360)
        tasks = self.trogdor.tasks()
        self.logger.info("TASKS: %s\n" %
                         json.dumps(tasks, sort_keys=True, indent=2))
