import random
import time

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
import requests

from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest


class SimpleEndToEndTest(EndToEndTest):
    @cluster(num_nodes=6)
    def test_correctness_while_evicitng_log(self):
        '''
        Validate that all the records will be delivered to consumers when there 
        are multiple producers and log is evicted
        '''
        # use small segment size to enable log eviction
        self.start_redpanda(num_nodes=3,
                            extra_rp_conf={
                                "log_segment_size": 1048576,
                                "retention_bytes": 5242880,
                                "default_topic_replications": 3,
                            })

        spec = TopicSpec(name="topic", partition_count=1, replication_factor=1)
        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(2, throughput=10000)
        self.start_consumer(1)
        self.await_startup()

        self.run_validation(min_records=100000,
                            producer_timeout_sec=300,
                            consumer_timeout_sec=300)
