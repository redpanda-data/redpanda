# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.spark_service import QueryEngineType
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import PandaproxyConfig, SchemaRegistryConfig, SISettings
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster
from rptest.tests.datalake.utils import supported_storage_types


class PartitionMovementTest(PartitionMovementMixin, RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(PartitionMovementTest,
              self).__init__(test_ctx,
                             num_brokers=1,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf={
                                 "iceberg_enabled": "true",
                                 "iceberg_catalog_commit_interval_ms": 5000
                             },
                             schema_registry_config=SchemaRegistryConfig(),
                             pandaproxy_config=PandaproxyConfig(),
                             *args,
                             **kwargs)
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def setUp(self):
        pass

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_cross_core_movements(self, cloud_storage_type):
        """Tests interaction between cross core partition movement and iceberg translation.
        Cross core partition movement involves shutting down the partition replica machinery on one
        core and restarting it on another core. This test ensures the translation can make progress
        during these cross core movements."""

        moves = 15
        admin = self.redpanda._admin
        topic = self.topic_name
        partition = 0
        stream = "cross_core_test"

        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=False,
                              include_query_engines=[QueryEngineType.TRINO
                                                     ]) as dl:
            dl.create_iceberg_enabled_topic(topic)
            # A long running counter that runs until stopped
            connect = dl.start_counter_stream(name=stream,
                                              topic=topic,
                                              count=0,
                                              interval="1ms")

            def total_records_ingested():
                metrics = connect.stream_metrics(name=stream)
                samples = metrics["output_sent"]
                for s in samples:
                    if s.name == "output_sent_total":
                        return s.value
                assert False, f"Unable to probe metrics for stream {stream}"

            def ensure_stream_progress(target: int):
                wait_until(
                    lambda: total_records_ingested() >= target,
                    timeout_sec=20,
                    backoff_sec=5,
                    err_msg=
                    f"Timed out waiting for stream producer to reach target: {target}"
                )

            for _ in range(moves):
                assignments = self._get_current_node_cores(
                    admin, topic, partition)
                for a in assignments:
                    a['core'] = (a['core'] +
                                 1) % self.redpanda.get_node_cpu_count()

                counter_before = total_records_ingested()

                self._set_partition_assignments(topic,
                                                partition,
                                                assignments,
                                                admin=admin)
                self._wait_post_move(topic, partition, assignments, 180)
                # Make sure the stream is not stuck
                ensure_stream_progress(counter_before + 500)

            connect.stop_stream(name=stream, wait_to_finish=False)

            total_row_count = total_records_ingested()
            dl.wait_for_translation_until_offset(topic, total_row_count - 1)

            verifier = DatalakeVerifier(self.redpanda, topic, dl.trino())
            verifier.start()
            verifier.wait()
