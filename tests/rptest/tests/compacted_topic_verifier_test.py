from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.errors import DucktapeError

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.compacted_verifier import CompactedTopicVerifier
from rptest.clients.rpk import RpkTool


class CompactedTopicVerifierTest(RedpandaTest):
    """
    Verify that segment indices are recovered on startup.
    """
    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_compaction_interval_ms=2000,
            compacted_log_segment_size=1048576,
        )

        super(CompactedTopicVerifierTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_verify_compacted_topics(self):
        partition_count = 1
        record_count = 10000
        key_cardinality = 200
        v = CompactedTopicVerifier(self.redpanda)
        rpk = RpkTool(self.redpanda)
        res = v.produce(record_count=record_count,
                        p=partition_count,
                        key_cardinality=key_cardinality)

        def records_readable():
            partitions = rpk.describe_topic(v.topic)
            for p in partitions:
                if p.high_watermark < record_count:
                    return False
            return True

        wait_until(lambda: records_readable(),
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg="Records are not readable")
        #TODO: added verification if segments were actually compacted
        res = v.verify()
        if res == None:
            raise DucktapeError("Compacted topic verification failed")
