# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0


from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.admin import v2 as admin_v2
from rptest.clients.rpk import RpkTool
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    CloudStorageType,
    SISettings,
    SchemaRegistryConfig,
    get_cloud_storage_type,
)
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.rpcn_utils import counter_stream_config
from rptest.services.redpanda_connect import RedpandaConnectService


class DatalakeAdminEndpointTest(RedpandaTest):
    def __init__(self, test_ctx: TestContext, *args, **kwargs) -> None:
        super(DatalakeAdminEndpointTest, self).__init__(
            test_ctx,
            num_brokers=3,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
            },
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )
        self.test_ctx = test_ctx

    def setUp(self) -> None:
        # redpanda will be started by DatalakeServices
        pass

    @cluster(num_nodes=6)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(docker_use_arbitrary=True),
        catalog_type=[CatalogType.REST_JDBC],
    )
    def test_get_coordinator_state(
        self, cloud_storage_type: CloudStorageType, catalog_type: CatalogType
    ):
        """
        Creates topics, produces some data, and verifies that the coordinator
        state can be retrieved via the admin API.
        """
        topic1 = "tapioca"
        topic2 = "milktea"
        count = 100

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=catalog_type,
        ) as dl:
            dl.create_iceberg_enabled_topic(topic1, partitions=3)
            dl.create_iceberg_enabled_topic(topic2, partitions=2)

            # Produce data to both topics.
            dl.produce_to_topic(topic1, 1024, count)
            dl.produce_to_topic(topic2, 1024, count)
            dl.wait_for_translation(topic1, msg_count=count)
            dl.wait_for_translation(topic2, msg_count=count)

            # Call the get_coordinator_state admin endpoint. First with a
            # topics filter.
            admin: admin_v2.Admin = admin_v2.Admin(self.redpanda)
            request = admin_v2.datalake_pb.GetCoordinatorStateRequest(
                topics_filter=[topic1]
            )
            response: admin_v2.datalake_pb.GetCoordinatorStateResponse = (
                admin.datalake().get_coordinator_state(request)
            )
            assert len(response.state.topic_states) == 1, (
                f"Response should contain 2 topics: {response.state.topic_states}"
            )

            # Then get both topics (empty topics filter) and do some sanity
            # checks.
            request = admin_v2.datalake_pb.GetCoordinatorStateRequest()
            response: admin_v2.datalake_pb.GetCoordinatorStateResponse = (
                admin.datalake().get_coordinator_state(request)
            )

            assert len(response.state.topic_states) == 2, (
                f"Response should contain 2 topics: {response.state.topic_states}"
            )
            topic_states: dict[str, admin_v2.datalake_pb.TopicState] = (
                response.state.topic_states
            )

            # Do some simple sanity checks about the state.
            assert topic1 in topic_states, f"Topic {topic1} should be in response"
            assert topic2 in topic_states, f"Topic {topic2} should be in response"
            t1_state: admin_v2.datalake_pb.TopicState = topic_states[topic1]
            t2_state: admin_v2.datalake_pb.TopicState = topic_states[topic2]
            self.logger.debug(f"{topic1}: {t1_state}")
            self.logger.debug(f"{topic2}: {t2_state}")
            assert t1_state.revision != t2_state.revision
            assert t1_state.total_kafka_processed_bytes >= 0
            assert t2_state.total_kafka_processed_bytes >= 0
            assert (
                t1_state.lifecycle_state
                == admin_v2.datalake_pb.LifecycleState.LIFECYCLE_STATE_LIVE
            )
            assert (
                t2_state.lifecycle_state
                == admin_v2.datalake_pb.LifecycleState.LIFECYCLE_STATE_LIVE
            )
            assert t1_state.last_committed_snapshot_id is not None
            assert t1_state.last_committed_snapshot_id > 0
            assert t2_state.last_committed_snapshot_id is not None
            assert t2_state.last_committed_snapshot_id > 0

            assert len(t1_state.partition_states) == 3
            assert len(t2_state.partition_states) == 2

            # NOTE: we waited for our data to be committed above, so our
            # partitions should have no pending entries and we should have as
            # many offsets as there were messages.
            t1_committed = 0
            for _, p_state in t1_state.partition_states.items():
                assert p_state.pending_entries is not None
                assert len(p_state.pending_entries) == 0
                assert p_state.last_committed is not None
                assert p_state.last_committed != 0
                t1_committed += p_state.last_committed + 1
            assert t1_committed == 100

            t2_committed = 0
            for _, p_state in t2_state.partition_states.items():
                assert p_state.pending_entries is not None
                assert len(p_state.pending_entries) == 0
                assert p_state.last_committed is not None
                assert p_state.last_committed != 0
                t2_committed += p_state.last_committed + 1
            assert t2_committed == 100

    @cluster(num_nodes=7)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(docker_use_arbitrary=True),
        catalog_type=[CatalogType.REST_JDBC],
    )
    def test_get_coordinator_state_pending(
        self, cloud_storage_type: CloudStorageType, catalog_type: CatalogType
    ):
        """
        Test that the admin endpoint correctly sees pending entries.
        """
        topic = "tapioca"
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            catalog_type=CatalogType.REST_JDBC,
        ) as dl:
            self.rpcn: RedpandaConnectService = RedpandaConnectService(
                self.test_context, self.redpanda
            )
            self.rpcn.start()

            # Slow down the commit rate so we can get some backlog of pending
            # files to commit.
            rpk: RpkTool = RpkTool(self.redpanda)
            rpk.cluster_config_set("iceberg_catalog_commit_interval_ms", "3600000")

            # Create a stream to continually produce until the test is done.
            dl.create_iceberg_enabled_topic(topic, partitions=1)
            stream_name = "tapioca_stream"
            self.rpcn.start_stream(
                stream_name,
                counter_stream_config(self.redpanda, topic, "", cnt=0, interval_ms=10),
            )

            def coordinator_has_state():
                """
                Returns true iff there are pending entries for partition 0.
                """
                admin: admin_v2.Admin = admin_v2.Admin(self.redpanda)
                request = admin_v2.datalake_pb.GetCoordinatorStateRequest()
                try:
                    response: admin_v2.datalake_pb.GetCoordinatorStateResponse = (
                        admin.datalake().get_coordinator_state(request)
                    )
                except Exception:
                    return False
                if topic not in response.state.topic_states:
                    return False
                t_state = response.state.topic_states[topic]
                self.logger.debug(f"{topic}: {t_state}")
                if 0 not in t_state.partition_states:
                    return False

                p_state = t_state.partition_states[0]
                if len(p_state.pending_entries) == 0:
                    return False
                return True

            wait_until(coordinator_has_state, timeout_sec=20, backoff_sec=1)
            self.rpcn.stop_stream(stream_name, should_finish=None)
