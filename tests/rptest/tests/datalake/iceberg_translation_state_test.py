# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any

import requests
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from ducktape.tests.test import TestContext

from rptest.clients.admin.proto.redpanda.core.rest import iceberg_pb2
from rptest.clients.rpk import RpkTool
from rptest.context.cloud_storage import CloudStorageType
from rptest.services.cluster import cluster
from rptest.services.redpanda import PandaproxyConfig, SISettings, SecurityConfig
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest


class IcebergTranslationStateTest(RedpandaTest):
    def __init__(self, test_ctx: TestContext, *args: Any, **kwargs: Any):
        kwargs.setdefault("pandaproxy_config", PandaproxyConfig())
        super().__init__(
            test_ctx,
            num_brokers=3,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
            },
            *args,
            **kwargs,
        )
        self.test_ctx = test_ctx

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def _base_uri(self) -> str:
        return f"http://{self.redpanda.nodes[0].account.hostname}:8082"

    def _make_request(
        self,
        topics_filter: list[str] | None = None,
        auth: tuple[str, str] | None = None,
    ) -> requests.Response:
        req = iceberg_pb2.GetTranslationStateRequest()
        if topics_filter is not None:
            req.topics_filter.extend(topics_filter)

        return requests.post(
            f"{self._base_uri()}/v1/redpanda/datalake/translation_state",
            data=req.SerializeToString(),
            headers={
                "Content-Type": "application/proto",
                "Accept": "application/proto",
            },
            auth=auth if auth is not None else None,
        )

    def _get_translation_state(
        self,
        topics_filter: list[str] | None = None,
        auth: tuple[str, str] | None = None,
    ) -> iceberg_pb2.GetTranslationStateResponse:
        resp = self._make_request(topics_filter, auth=auth)
        assert resp.status_code == 200, (
            f"Expected 200, got {resp.status_code}: {resp.text}"
        )
        result = iceberg_pb2.GetTranslationStateResponse()
        result.ParseFromString(resp.content)
        return result

    def _try_get_translation_state(
        self,
        topics_filter: list[str] | None = None,
        auth: tuple[str, str] | None = None,
    ) -> iceberg_pb2.GetTranslationStateResponse | None:
        resp = self._make_request(topics_filter, auth=auth)
        if resp.status_code != 200:
            return None
        result = iceberg_pb2.GetTranslationStateResponse()
        result.ParseFromString(resp.content)
        return result

    def _get_raw_status_code(
        self,
        topics_filter: list[str] | None = None,
        auth: tuple[str, str] | None = None,
    ) -> int:
        return self._make_request(topics_filter, auth=auth).status_code

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_translation_state_smoke(self, cloud_storage_type: list[CloudStorageType]):
        topic_name = "iceberg-test-topic"
        non_iceberg_topic = "plain-topic"

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[],
        ) as dl:
            rpk = RpkTool(self.redpanda)

            dl.create_iceberg_enabled_topic(topic_name, partitions=3)
            rpk.create_topic(non_iceberg_topic)

            for i in range(10):
                rpk.produce(topic_name, f"key-{i}", f"value-{i}")

            # Wait for translation state to become available
            def translation_state_ready():
                result = self._try_get_translation_state(topics_filter=[topic_name])
                return result is not None and topic_name in result.topic_states

            wait_until(
                translation_state_ready,
                timeout_sec=30,
                backoff_sec=2,
                err_msg="Translation state not available for topic",
            )

            result = self._get_translation_state(topics_filter=[topic_name])
            assert topic_name in result.topic_states, (
                f"Topic {topic_name} not in response"
            )

            state = result.topic_states[topic_name]
            assert state.translation_status == iceberg_pb2.TRANSLATION_STATUS_ENABLED, (
                f"Unexpected status: {state.translation_status}"
            )
            assert list(state.namespace_name) == ["redpanda"], (
                f"Expected namespace ['redpanda'], got {list(state.namespace_name)}"
            )
            assert state.table_name == topic_name, (
                f"Expected table name '{topic_name}', got '{state.table_name}'"
            )
            assert state.dlq_table_name == f"{topic_name}~dlq", (
                f"Expected DLQ table name '{topic_name}~dlq', got '{state.dlq_table_name}'"
            )
            self.logger.info(f"Translation state for {topic_name}: {state}")

            # Requesting with an empty topics_filter must return 400
            assert self._get_raw_status_code(topics_filter=[]) == 400

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_translation_state_filtering(
        self, cloud_storage_type: list[CloudStorageType]
    ):
        topics = ["filter-topic-a", "filter-topic-b", "filter-topic-c"]

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[],
        ) as dl:
            rpk = RpkTool(self.redpanda)

            for t in topics:
                dl.create_iceberg_enabled_topic(t, partitions=1)
                rpk.produce(t, "key", "value")

            def all_topics_ready():
                result = self._try_get_translation_state(topics_filter=topics)
                return result is not None and all(
                    t in result.topic_states for t in topics
                )

            wait_until(
                all_topics_ready,
                timeout_sec=30,
                backoff_sec=2,
                err_msg="Error waiting for all topics to appear in translation state",
            )

            # All 3 topics must be present with correct names
            result = self._get_translation_state(topics_filter=topics)
            for t in topics:
                assert t in result.topic_states, f"Topic {t} missing from response"
                state = result.topic_states[t]
                assert list(state.namespace_name) == ["redpanda"], (
                    f"Topic {t}: expected namespace ['redpanda'], got {list(state.namespace_name)}"
                )
                assert state.table_name == t, (
                    f"Topic {t}: expected table name '{t}', got '{state.table_name}'"
                )
                assert state.dlq_table_name == f"{t}~dlq", (
                    f"Topic {t}: expected DLQ table name '{t}~dlq', got '{state.dlq_table_name}'"
                )

            # Filter for a subset of 2 topics
            subset = topics[:2]
            excluded = topics[2]
            result = self._get_translation_state(topics_filter=subset)
            for t in subset:
                assert t in result.topic_states, (
                    f"Topic {t} missing from filtered response"
                )
            assert excluded not in result.topic_states, (
                f"Topic {excluded} should not be in filtered response"
            )

            # Filter for a single topic
            result = self._get_translation_state(topics_filter=[topics[2]])
            assert topics[2] in result.topic_states, (
                f"Topic {topics[2]} missing from single-filter response"
            )
            assert len(result.topic_states) == 1, (
                f"Expected exactly 1 topic, got {len(result.topic_states)}"
            )

            # Filter for a non-existent topic returns empty
            result = self._get_translation_state(topics_filter=["nonexistent-topic"])
            assert "nonexistent-topic" not in result.topic_states, (
                "Non-existent topic should not appear"
            )

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_translation_state_after_disable(
        self, cloud_storage_type: list[CloudStorageType]
    ):
        topic_name = "disable-iceberg-topic"

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[],
        ) as dl:
            rpk = RpkTool(self.redpanda)

            dl.create_iceberg_enabled_topic(topic_name, partitions=1)
            rpk.produce(topic_name, "key", "value")

            # Wait for translation state to become available
            def translation_state_ready():
                result = self._try_get_translation_state(topics_filter=[topic_name])
                return (
                    result is not None
                    and topic_name in result.topic_states
                    and result.topic_states[topic_name].translation_status
                    == iceberg_pb2.TRANSLATION_STATUS_ENABLED
                )

            wait_until(
                translation_state_ready,
                timeout_sec=30,
                backoff_sec=2,
                err_msg="Translation state not available for topic",
            )

            # Disable iceberg on the topic
            rpk.alter_topic_config(topic_name, "redpanda.iceberg.mode", "disabled")

            # Status should change to disabled
            def translation_disabled():
                result = self._try_get_translation_state(topics_filter=[topic_name])
                return (
                    result is not None
                    and topic_name in result.topic_states
                    and result.topic_states[topic_name].translation_status
                    == iceberg_pb2.TRANSLATION_STATUS_DISABLED
                )

            wait_until(
                translation_disabled,
                timeout_sec=30,
                backoff_sec=2,
                err_msg="Translation status did not change to disabled",
            )

            result = self._get_translation_state(topics_filter=[topic_name])
            state = result.topic_states[topic_name]
            assert (
                state.translation_status == iceberg_pb2.TRANSLATION_STATUS_DISABLED
            ), f"Expected DISABLED, got {state.translation_status}"


class IcebergTranslationStateAuthTest(RedpandaTest):
    """Tests per-topic authorization filtering on the translation state
    endpoint. With SASL + HTTP Basic auth enabled, only topics the
    authenticated user has describe ACLs for should be returned."""

    username = "testuser"
    password = "testpassword12345"
    algorithm = "SCRAM-SHA-256"

    def __init__(self, test_ctx: TestContext, *args: Any, **kwargs: Any):
        security = SecurityConfig()
        security.enable_sasl = True
        security.endpoint_authn_method = "sasl"

        pandaproxy_config = PandaproxyConfig()
        pandaproxy_config.authn_method = "http_basic"

        super().__init__(
            test_ctx,
            *args,
            num_brokers=3,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
            },
            security=security,
            pandaproxy_config=pandaproxy_config,
            **kwargs,
        )
        self.test_ctx = test_ctx

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def _base_uri(self) -> str:
        return f"http://{self.redpanda.nodes[0].account.hostname}:8082"

    def _superuser_auth(self) -> tuple[str, str]:
        su, sp, _ = self.redpanda.SUPERUSER_CREDENTIALS
        return (su, sp)

    def _make_request(
        self,
        topics_filter: list[str] | None = None,
        auth: tuple[str, str] | None = None,
    ) -> requests.Response:
        req = iceberg_pb2.GetTranslationStateRequest()
        if topics_filter is not None:
            req.topics_filter.extend(topics_filter)

        return requests.post(
            f"{self._base_uri()}/v1/redpanda/datalake/translation_state",
            data=req.SerializeToString(),
            headers={
                "Content-Type": "application/proto",
                "Accept": "application/proto",
            },
            auth=auth if auth is not None else None,
        )

    def _get_translation_state(
        self,
        topics_filter: list[str] | None = None,
        auth: tuple[str, str] | None = None,
    ) -> iceberg_pb2.GetTranslationStateResponse:
        resp = self._make_request(topics_filter, auth=auth)
        assert resp.status_code == 200, (
            f"Expected 200, got {resp.status_code}: {resp.text}"
        )
        result = iceberg_pb2.GetTranslationStateResponse()
        result.ParseFromString(resp.content)
        return result

    def _try_get_translation_state(
        self,
        topics_filter: list[str] | None = None,
        auth: tuple[str, str] | None = None,
    ) -> iceberg_pb2.GetTranslationStateResponse | None:
        resp = self._make_request(topics_filter, auth=auth)
        if resp.status_code != 200:
            return None
        result = iceberg_pb2.GetTranslationStateResponse()
        result.ParseFromString(resp.content)
        return result

    def _create_user(self, rpk: RpkTool):
        rpk.sasl_create_user(self.username, self.password, self.algorithm)

    def _grant_describe(self, rpk: RpkTool, topic: str):
        su, sp, sa = self.redpanda.SUPERUSER_CREDENTIALS
        rpk.sasl_allow_principal(
            f"User:{self.username}",
            ["describe"],
            "topic",
            topic,
            su,
            sp,
            sa,
        )

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_translation_state_authz_filtering(
        self, cloud_storage_type: list[CloudStorageType]
    ):
        topics = ["authz-topic-a", "authz-topic-b", "authz-topic-c"]

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[],
        ) as dl:
            rpk = RpkTool(self.redpanda)
            self._create_user(rpk)

            for t in topics:
                dl.create_iceberg_enabled_topic(t, partitions=1)
                rpk.produce(t, "key", "value")

            su_auth = self._superuser_auth()

            # Wait until all topics are visible to the superuser.
            def all_topics_ready():
                result = self._try_get_translation_state(
                    topics_filter=topics, auth=su_auth
                )
                return result is not None and all(
                    t in result.topic_states for t in topics
                )

            wait_until(
                all_topics_ready,
                timeout_sec=30,
                backoff_sec=2,
                err_msg="Topics not ready in translation state",
            )

            user_auth = (self.username, self.password)

            # Without any ACLs the unprivileged user sees no topics.
            result = self._get_translation_state(topics_filter=topics, auth=user_auth)
            assert len(result.topic_states) == 0, (
                f"Expected empty response, got {list(result.topic_states.keys())}"
            )

            # Grant describe on the first topic only.
            self._grant_describe(rpk, topics[0])

            result = self._get_translation_state(topics_filter=topics, auth=user_auth)
            assert topics[0] in result.topic_states, (
                f"Topic {topics[0]} should be visible after ACL grant"
            )
            assert len(result.topic_states) == 1, (
                f"Expected 1 topic, got {list(result.topic_states.keys())}"
            )
            state = result.topic_states[topics[0]]
            assert list(state.namespace_name) == ["redpanda"], (
                f"Expected namespace ['redpanda'], got {list(state.namespace_name)}"
            )
            assert state.table_name == topics[0], (
                f"Expected table name '{topics[0]}', got '{state.table_name}'"
            )
            assert state.dlq_table_name == f"{topics[0]}~dlq", (
                f"Expected DLQ name '{topics[0]}~dlq', got '{state.dlq_table_name}'"
            )

            # Grant describe on the second topic.
            self._grant_describe(rpk, topics[1])

            result = self._get_translation_state(topics_filter=topics, auth=user_auth)
            assert topics[0] in result.topic_states
            assert topics[1] in result.topic_states
            assert topics[2] not in result.topic_states, (
                f"Topic {topics[2]} should not be visible"
            )

            # Superuser still sees all topics.
            result = self._get_translation_state(topics_filter=topics, auth=su_auth)
            for t in topics:
                assert t in result.topic_states, f"Superuser should see topic {t}"
