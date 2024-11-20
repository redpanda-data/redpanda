# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings
from rptest.tests.datalake.utils import supported_storage_types
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster

LOG_ALLOW_LIST = [r'Error cluster::errc:16 processing partition state for ntp']


class CoordinatorRetentionTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(CoordinatorRetentionTest, self).__init__(
            test_ctx,
            num_brokers=3,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
                "datalake_coordinator_snapshot_max_delay_secs": 10,
            },
            *args,
            **kwargs)
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def wait_until_coordinator_snapshots(self):
        try:
            replica_last_snapshot_offsets = []
            for pid in range(0, 3):
                state = self.redpanda._admin.get_partition_state(
                    "kafka_internal", "datalake_coordinator", pid)
                for r in state["replicas"]:
                    if r["raft_state"]["is_leader"]:
                        replica_last_snapshot_offsets.append(
                            r["raft_state"]["last_snapshot_index"] > 0)
            # Only one of the partitions must snapshot, particularly the
            # one that is coordinating for the test topic.
            return len(replica_last_snapshot_offsets
                       ) == 3 and replica_last_snapshot_offsets.count(
                           True) == 1
        except:
            self.redpanda.logger.debug("Exception querying snapshot states",
                                       exc_info=True)
            return False

    def do_test_retention(self, dl: DatalakeServices):
        dl.create_iceberg_enabled_topic(self.topic_name,
                                        partitions=10,
                                        replicas=3)
        producer = KgoVerifierProducer(self.test_context, self.redpanda,
                                       self.topic_name, 1024, 10000000)
        producer.start()
        for pid in range(0, 3):
            self.redpanda._admin.await_stable_leader(
                namespace="kafka_internal",
                topic="datalake_coordinator",
                partition=pid,
                timeout_s=30,
                backoff_s=5)
        try:
            wait_until(
                self.wait_until_coordinator_snapshots,
                timeout_sec=30,
                backoff_sec=3,
                err_msg=
                "Timed out waiting for coordinator partitions to snapshot.")
        finally:
            producer.stop()
            producer.clean()
            producer.free()

    @cluster(num_nodes=5, log_allow_list=LOG_ALLOW_LIST)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_retention(self, cloud_storage_type):
        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=False,
                              include_query_engines=[]) as dl:
            self.do_test_retention(dl)
