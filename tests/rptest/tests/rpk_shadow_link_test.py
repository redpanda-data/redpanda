# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import sys
import tempfile
from typing import Any

import yaml
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RPKACLInput, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import TestContext, cluster
from rptest.services.multi_cluster_services import SecondaryClusterArgs
from rptest.services.redpanda import SchemaRegistryConfig, SecurityConfig
from rptest.tests.cluster_linking_test_base import ShadowLinkTestBase


class RpkShadowLinkTestBase(ShadowLinkTestBase):
    """Shared config and helpers for rpk shadow-link tests. Holds no test
    methods so subclasses with different security topologies do not re-run
    each other's tests."""

    LINK_NAME = "rpk-test-link"
    CONSUMER_GROUP = "shadow-group"

    def __init__(
        self,
        test_context: TestContext,
        *args: Any,
        security: SecurityConfig | None = None,
        **kwargs: Any,
    ):
        # Only pass security through when set; RedpandaService distinguishes
        # an unset security config from an explicit None.
        secondary_kwargs: dict[str, Any] = {
            "schema_registry_config": SchemaRegistryConfig(),
        }
        if security is not None:
            secondary_kwargs["security"] = security
            kwargs["security"] = security
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            secondary_cluster_args=SecondaryClusterArgs(**secondary_kwargs),
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )

    def _topic_filters(self, rpk: RpkTool) -> list[dict[str, str]]:
        describe = rpk.shadow_describe(self.LINK_NAME)
        opts = describe.get("topic_metadata_sync_options") or {}
        return opts.get("auto_create_shadow_topic_filters") or []

    def _client_auth(self, rpk: RpkTool) -> dict[str, Any]:
        describe = rpk.shadow_describe(self.LINK_NAME)
        opts = describe.get("client_options") or {}
        return opts.get("authentication_configuration") or {}

    def _wait_link_active(self, rpk: RpkTool) -> None:
        def link_active() -> bool:
            status = rpk.shadow_status(self.LINK_NAME)
            self.logger.debug(f"rpk shadow status: {status}")
            return status["overview"]["state"] == "ACTIVE"

        wait_until(
            link_active,
            timeout_sec=60,
            backoff_sec=2,
            retry_on_exc=True,
            err_msg="shadow link did not reach ACTIVE state",
        )


class RpkShadowLinkTest(RpkShadowLinkTestBase):
    @cluster(num_nodes=6)  # 3 target + 3 source brokers
    def test_shadow_link_full_sync(self):
        """
        Simple smoke test of a Shadow Link creation but
        using 'rpk shadow' command space.
        """
        rpk = self.target_cluster_rpk

        # Seed.
        topic = self._seed_source_topic()
        expected_acl = self._seed_source_acl()
        source_offsets = self._seed_source_consumer_group(topic)
        subject = self._seed_source_schema()

        assert not self.topic_exists_in_target(topic.name), (
            f"{topic.name} unexpectedly present on the shadow cluster before linking"
        )
        assert len(rpk.acl_list(format="json")["matches"]) == 0

        # Create a link that mirrors topics, offsets, ACLs and SR.
        rpk.shadow_create(self._full_link_config())
        self._wait_link_active(rpk)
        self._assert_link_listed(rpk)

        # Every configured sync should land on the shadow cluster.
        wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=90,
            backoff_sec=2,
            retry_on_exc=True,
            err_msg="topic was not mirrored to the shadow cluster",
        )
        wait_until(
            lambda: self._acl_synced(rpk, expected_acl),
            timeout_sec=90,
            backoff_sec=2,
            retry_on_exc=True,
            err_msg="ACL was not synced to the shadow cluster",
        )
        wait_until(
            lambda: self._offsets_synced(rpk, source_offsets),
            timeout_sec=90,
            backoff_sec=2,
            retry_on_exc=True,
            err_msg="consumer offsets were not synced to the shadow cluster",
        )
        wait_until(
            lambda: self._schema_synced(subject),
            timeout_sec=90,
            backoff_sec=2,
            retry_on_exc=True,
            err_msg="schema registry subject was not synced to the shadow cluster",
        )

    def _full_link_config(self) -> dict[str, Any]:
        """
        Mirror all topics, all consumer-group offsets, all ACLs,
        and the Schema Registry using the schema_topic option.
        """
        match_all_names = {
            "pattern_type": "LITERAL",
            "filter_type": "INCLUDE",
            "name": "*",
        }
        match_any_access = {"operation": "ANY", "permission_type": "ANY"}
        return {
            "name": self.LINK_NAME,
            "client_options": {
                "bootstrap_servers": self.source_cluster_service.brokers_list(),
            },
            "topic_metadata_sync_options": {
                "auto_create_shadow_topic_filters": [dict(match_all_names)],
            },
            "consumer_offset_sync_options": {
                "group_filters": [dict(match_all_names)],
            },
            "security_sync_options": {
                "acl_filters": [
                    {
                        "resource_filter": {
                            "resource_type": "ANY",
                            "pattern_type": "ANY",
                        },
                        "access_filter": dict(match_any_access),
                    },
                    {
                        "resource_filter": {
                            "resource_type": "SR_ANY",
                            "pattern_type": "ANY",
                        },
                        "access_filter": dict(match_any_access),
                    },
                ],
            },
            "schema_registry_sync_options": {
                "shadow_schema_registry_topic": {},
            },
        }

    def _seed_source_topic(self) -> TopicSpec:
        topic = TopicSpec(
            name="shadowed-topic", partition_count=3, replication_factor=3
        )
        self.source_default_client().create_topic(topic)
        assert self.topic_exists_in_source(topic.name)
        return topic

    def _seed_source_acl(self) -> dict[str, str]:
        """Create a user and an ACL referencing it on the source cluster, and
        return the ACL fields expected to appear on the shadow cluster."""
        user = "shadow-user"
        acl_topic = "acl-topic"
        self.source_cluster_rpk.sasl_create_user(user, "shadow-pass-1234")
        self.source_cluster_rpk.acl_create(
            RPKACLInput(
                allow_principal=[user],
                topic=[acl_topic],
                operation=["read"],
                resource_pattern_type="literal",
            )
        )
        return {
            "principal": f"User:{user}",
            "resource_type": "TOPIC",
            "resource_name": acl_topic,
            "operation": "READ",
            "permission": "ALLOW",
        }

    def _seed_source_consumer_group(
        self, topic: TopicSpec
    ) -> dict[tuple[str, int], int | None]:
        """Produce to the topic, consume it with a group so offsets are
        committed, and return the per-partition committed offsets."""
        source_rpk = self.source_cluster_rpk
        msg_count = 10
        for i in range(msg_count):
            source_rpk.produce(topic.name, key=f"k{i}", msg=f"v{i}")
        source_rpk.consume(topic.name, n=msg_count, group=self.CONSUMER_GROUP)

        desc = source_rpk.group_describe(self.CONSUMER_GROUP)
        offsets = {(p.topic, p.partition): p.current_offset for p in desc.partitions}
        self.logger.debug(f"source group offsets: {offsets}")
        assert offsets, "source consumer group committed no offsets"
        return offsets

    def _seed_source_schema(self) -> str:
        """Register a schema on the source cluster and return its subject."""
        subject = "shadow-subject-value"
        schema = (
            '{"type":"record","name":"shadow_record",'
            '"fields":[{"name":"f1","type":"string"}]}'
        )
        self.source_cluster_rpk.create_schema_from_str(subject, schema)
        return subject

    def _assert_link_listed(self, rpk: RpkTool) -> None:
        links = rpk.shadow_list()
        self.logger.debug(f"rpk shadow list: {links}")
        assert any(entry["name"] == self.LINK_NAME for entry in links), links

        describe = rpk.shadow_describe(self.LINK_NAME)
        self.logger.debug(f"rpk shadow describe: {describe}")
        assert describe["name"] == self.LINK_NAME, describe
        assert describe.get("client_options") is not None, describe

    def _acl_synced(self, rpk: RpkTool, expected: dict[str, str]) -> bool:
        matches = rpk.acl_list(format="json")["matches"]
        self.logger.debug(f"target ACLs: {matches}")
        return any(all(acl.get(k) == v for k, v in expected.items()) for acl in matches)

    def _offsets_synced(
        self, rpk: RpkTool, source_offsets: dict[tuple[str, int], int | None]
    ) -> bool:
        if self.CONSUMER_GROUP not in [g.group for g in rpk.group_list()]:
            return False
        desc = rpk.group_describe(self.CONSUMER_GROUP, tolerant=True)
        target_offsets = {
            (p.topic, p.partition): p.current_offset for p in desc.partitions
        }
        self.logger.debug(f"target group offsets: {target_offsets}")
        return target_offsets == source_offsets

    def _schema_synced(self, subject: str) -> bool:
        subjects = self.target_cluster_rpk.list_subjects()
        self.logger.debug(f"target subjects: {subjects}")
        names = [s["subject"] if isinstance(s, dict) else s for s in subjects]
        return subject in names


# Stand-in for $EDITOR in 'rpk shadow update' editor-mode tests. rpk is not
# shell-parsing $EDITOR, so it must be a single executable; rpk invokes it as
# `<script> <seeded yaml file>` and submits the file once it exits. The script
# saves the seeded file aside for the test to assert on, then appends a topic
# filter, leaving every other field (including the '<redacted>' password
# placeholder) untouched. Formatted with python (the test venv interpreter,
# which has yaml installed) and seeded_copy (where to save the seeded file).
_FAKE_EDITOR_SCRIPT = """#!{python}
import shutil
import sys

import yaml

path = sys.argv[1]
shutil.copy(path, {seeded_copy!r})

with open(path) as f:
    cfg = yaml.safe_load(f)

cfg["topic_metadata_sync_options"]["auto_create_shadow_topic_filters"].append(
    {{"pattern_type": "PREFIX", "filter_type": "INCLUDE", "name": "topic-b-"}}
)

with open(path, "w") as f:
    yaml.safe_dump(cfg, f)
"""


class RpkShadowLinkAuthTest(RpkShadowLinkTestBase):
    """Password-preservation on 'rpk shadow update' editor mode. This needs a
    SASL-authenticated link: CreateShadowLink runs a source preflight check,
    so the link must authenticate against the source with real
    credentials."""

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        security = SecurityConfig()
        security.enable_sasl = True
        super().__init__(test_context, security=security, *args, **kwargs)

    def _scram_link_config(self) -> dict[str, Any]:
        # Both clusters bootstrap the same superuser; using it as the link
        # principal lets the source preflight authenticate successfully.
        su = self.redpanda.SUPERUSER_CREDENTIALS
        return {
            "name": self.LINK_NAME,
            "client_options": {
                "bootstrap_servers": self.source_cluster_service.brokers_list(),
                "authentication_configuration": {
                    "scram_configuration": {
                        "username": su.username,
                        "password": su.password,
                        "scram_mechanism": su.algorithm,
                    },
                },
            },
            "topic_metadata_sync_options": {
                "auto_create_shadow_topic_filters": [
                    {
                        "pattern_type": "LITERAL",
                        "filter_type": "INCLUDE",
                        "name": "topic-a",
                    },
                ],
            },
        }

    @cluster(num_nodes=6)
    def test_shadow_update_editor_preserves_password(self):
        """
        Editor-mode updates seed a set SCRAM password as the '<redacted>'
        placeholder and submit a full replacement. rpk must strip an untouched
        placeholder to an empty password so the server preserves the stored
        one; submitting the placeholder verbatim would silently change the
        password and break re-authentication to the source.
        """
        rpk = self.target_cluster_rpk
        su = self.redpanda.SUPERUSER_CREDENTIALS

        rpk.shadow_create(self._scram_link_config())
        self._wait_link_active(rpk)

        auth = self._client_auth(rpk)
        assert auth.get("password_set") is True, auth
        password_set_at = auth.get("password_set_at")
        assert password_set_at, auth

        with tempfile.TemporaryDirectory() as td:
            seeded_copy = os.path.join(td, "seeded.yaml")
            editor = os.path.join(td, "editor")
            with open(editor, "w") as f:
                f.write(
                    _FAKE_EDITOR_SCRIPT.format(
                        python=sys.executable, seeded_copy=seeded_copy
                    )
                )
            os.chmod(editor, 0o755)

            rpk.shadow_update_editor(self.LINK_NAME, editor)

            with open(seeded_copy) as f:
                seeded = yaml.safe_load(f)

        # check that rpk seeded the editor with the placeholder, never a real password.
        seeded_scram = seeded["client_options"]["authentication_configuration"][
            "scram_configuration"
        ]
        assert seeded_scram.get("password") == "<redacted>", seeded_scram

        # The update landed (the filter appended in the editor is present),
        # the placeholder was stripped rather than stored (set-at timestamp
        # unchanged), and the link still authenticates to the source with the
        # preserved password.
        self._wait_link_active(rpk)
        auth = self._client_auth(rpk)
        assert auth.get("password_set") is True, auth
        assert auth.get("username") == su.username, auth
        assert auth.get("password_set_at") == password_set_at, auth
        assert self._topic_filters(rpk) == [
            {"pattern_type": "LITERAL", "filter_type": "INCLUDE", "name": "topic-a"},
            {"pattern_type": "PREFIX", "filter_type": "INCLUDE", "name": "topic-b-"},
        ], self._topic_filters(rpk)
