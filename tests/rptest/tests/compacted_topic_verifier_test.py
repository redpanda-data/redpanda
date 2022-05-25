# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from ducktape.errors import DucktapeError

from rptest.tests.redpanda_test import RedpandaTest
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
            group_initial_rebalance_delay=300,
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
