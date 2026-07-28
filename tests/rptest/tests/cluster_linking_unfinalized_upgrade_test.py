# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json

import google.protobuf.duration_pb2 as duration_pb2
import google.protobuf.field_mask_pb2 as field_mask_pb2
from connectrpc.errors import ConnectError, ConnectErrorCode
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    features_pb2,
    security_pb2,
    shadow_link_pb2,
)
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.multi_cluster_services import SecondaryClusterArgs
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SchemaRegistryConfig
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.tests.cluster_linking_test_base import ShadowLinkTestBase
from rptest.tests.rbac_test_v2 import AdminV2RoleWrapper
from rptest.tests.schema_registry_test import SchemaRegistryRedpandaClient
from rptest.tests.unfinalized_upgrade_mixin import UnfinalizedUpgradeMixin

LINK_NAME = "unfinalized-link"

# Gate messages from shadow_link.cc (check_role_sync_supported /
# check_sr_api_sync_supported). Matched as substrings so exact wording drift in
# the tail of the sentence does not break the test.
ROLE_SYNC_GATE = "Role sync cannot be configured"
SR_API_GATE = "Schema Registry API sync mode cannot be configured"

# A source topic mirrored to the target to prove the link's data plane keeps
# working across the upgrade/downgrade transitions (feature-agnostic signal).
MIRROR_TOPIC = "mirror-check"
MIRROR_RECORDS_PRE = 10
MIRROR_RECORDS_POST = 10

# Roles seeded on the source. Only the "synced-" prefixed one is in the role
# sync filter; the excluded one must never appear on the target.
SYNCED_ROLE = "synced-app"
EXCLUDED_ROLE = "excluded-app"

# A single Schema Registry subject registered on the source, expected to appear
# on the target once SR API-mode sync is configured post-finalize.
SR_SUBJECT = "synced-value"


class ShadowLinkUnfinalizedUpgradeTest(ShadowLinkTestBase, UnfinalizedUpgradeMixin):
    """
    Full-lifecycle cluster-linking test across an *unfinalized* upgrade.

    The target cluster (the one that owns the shadow link, runs the sync tasks,
    and persists the `link_configuration`) is `self.redpanda`. It boots on the
    prior release, opts out of auto-finalization, and is rolled forward to HEAD
    while the active (downgrade-floor) version is held back -- the state a real
    deployment may sit in for weeks. The source cluster is a fixed HEAD cluster
    (features active) providing topics, roles, and schemas to mirror.

    The lifecycle exercised end to end:
      1. While unfinalized, the link's data plane works (topic mirroring). If
         the prior release predates the v26.2 sync features, they are gated:
         configuring role sync or Schema Registry API-mode sync is refused with
         FAILED_PRECONDITION. From a v26.2.1+ prior release the features are
         active before the upgrade begins, so there is no gate to observe and
         step 1 instead pins down that they stay active.
      2. The link's `link_configuration` -- written to the controller log by the
         HEAD binary -- survives a rollback to the prior release: the cluster
         comes back healthy and mirroring resumes, proving the record is
         forward/backward wire-compatible (the integration counterpart to the
         serde unit test link_configuration_legacy_reads_v1_role_sync).
      3. After the upgrade is finally finalized, the gates open and both features
         actually sync real data: roles matching the filter appear on the target,
         and the source's schema appears in the target's Schema Registry.
    """

    # The release that introduced the role-sync and SR API-mode sync features
    # (shadow_link_role_sync, shadow_link_sr_api_sync) and their gates.
    SYNC_FEATURES_MIN_RELEASE = (26, 2, 1)

    def __init__(self, test_context, *args, **kwargs):
        # Schema Registry on both clusters so SR API-mode sync can be exercised:
        # the source registers schemas, the target imports them over the link.
        super().__init__(
            test_context,
            num_brokers=3,
            secondary_cluster_args=SecondaryClusterArgs(
                schema_registry_config=SchemaRegistryConfig()
            ),
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )

    def setUp(self):
        # Install the old release on the target BEFORE the base setUp starts it.
        # MultiClusterServices asserts the primary has no started nodes and then
        # starts it, so pre-installing here makes the target boot the old binary;
        # the source (secondary) boots HEAD.
        self.installer = self.redpanda._installer
        self.old_release = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD
        )
        assert self.old_release >= self.MIN_OLD_RELEASE, (
            f"prior feature version {self.old_release} predates the "
            f"features_auto_finalization backport {self.MIN_OLD_RELEASE}; cannot "
            "opt out before upgrade"
        )
        # The sync features shipped in v26.2.1. When the old release predates
        # them, an unfinalized upgrade holds them unavailable and their
        # surfaces must refuse; from any newer old release they are active
        # before the upgrade begins and there is no gate to observe.
        self.sync_features_gated = self.old_release < self.SYNC_FEATURES_MIN_RELEASE
        self.logger.info(f"Target starts on old release {self.old_release}")
        self.installer.install(self.redpanda.nodes, self.old_release)

        super().setUp()

        self.admin = Admin(self.redpanda)
        self.old_logical = self.admin.get_features()["cluster_version"]
        self.logger.info(
            f"Old release {self.old_release} reports logical version {self.old_logical}"
        )

        # Role clients: source is where roles are authored, target is where the
        # migrator should reproduce the filtered subset.
        self._src_roles = AdminV2RoleWrapper(AdminV2(self.source_cluster_service))
        self._dst_roles = AdminV2RoleWrapper(AdminV2(self.target_cluster_service))

        # Schema Registry clients (port 8081) for the SR API-mode sync check.
        self._src_sr = SchemaRegistryRedpandaClient(self.source_cluster_service)
        self._dst_sr = SchemaRegistryRedpandaClient(self.target_cluster_service)

    # ---- small helpers -----------------------------------------------------

    def _role_members(self, wrapper, role):
        """Return the set of member names (users and groups) of `role`, or an
        empty set if the role does not exist yet (tolerates a not-yet-synced
        role on the target)."""
        try:
            members = wrapper.get_role(role).members
        except ConnectError as e:
            assert e.code == ConnectErrorCode.NOT_FOUND, (
                f"unexpected error reading role {role}: {e}"
            )
            return set()
        names = set()
        for m in members:
            which = m.WhichOneof("member")
            if which == "user":
                names.add(m.user.name)
            elif which == "group":
                names.add(m.group.name)
        return names

    def _target_topic_hwm(self, topic):
        """Sum of per-partition high watermarks for `topic` on the target, or
        None if the (mirror) topic is not present yet. describe_topic is a lazy
        generator, so the iteration itself must be guarded."""
        rpk = RpkTool(self.target_cluster_service)
        try:
            partitions = list(rpk.describe_topic(topic, tolerant=True))
        except Exception:
            return None
        if not partitions:
            return None
        return sum(p.high_watermark for p in partitions if p.high_watermark is not None)

    def _produce_source(self, topic, count):
        rpk = RpkTool(self.source_cluster_service)
        for i in range(count):
            rpk.produce(topic, key=f"k{i}", msg=f"v{i}", partition=0)

    def _wait_mirrored(self, topic, min_hwm, label):
        wait_until(
            lambda: (self._target_topic_hwm(topic) or 0) >= min_hwm,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=lambda: (
                f"topic {topic} did not mirror to target ({label}): "
                f"hwm={self._target_topic_hwm(topic)} < {min_hwm}"
            ),
        )

    def _register_source_schema(self):
        # Body shape matches SchemaRegistrySyncMixin._register: the schema dict
        # is JSON-encoded into the "schema" field (AVRO is the SR default).
        schema = {
            "type": "record",
            "name": "SyncedRecord",
            "fields": [{"name": "f", "type": "string"}],
        }
        body = json.dumps({"schema": json.dumps(schema)})
        resp = self._src_sr.post_subjects_subject_versions(
            subject=SR_SUBJECT, data=body
        )
        assert resp.status_code == 200, (
            f"registering {SR_SUBJECT} on source SR failed: "
            f"{resp.status_code} {resp.text}"
        )

    def _target_subjects(self):
        # get_subjects returns the raw HTTP response, not a parsed list.
        resp = self._dst_sr.get_subjects()
        if resp.status_code != 200:
            return []
        return resp.json()

    def _create_link_mirroring_topic(self, topic):
        """Create the shadow link mirroring only `topic`.

        The internal _schemas topic is not mirrored by an ordinary topic
        filter: the source topic syncer special-cases it (select_topic gates
        _schemas on is_topic_mode()), so it is shadowed only when topic-mode
        Schema Registry sync is configured -- never via the topic filter, "*"
        included. That is what keeps _verify_sr_api_sync_works non-vacuous:
        with topic-mode off, API-mode sync is the only path for a source
        schema to reach the target SR. The single literal filter just keeps
        this test's mirrored set to the one data topic it asserts on."""
        req = self.create_default_link_request(
            LINK_NAME,
            mirror_all_topics=False,
            mirror_all_groups=False,
            mirror_all_acls=False,
        )
        req.shadow_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters.append(
            shadow_link_pb2.NameFilter(
                pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                name=topic,
            )
        )
        return self.create_link_with_request(req=req)

    def _update_link_role_sync(self):
        """Fetch the link, add a role-sync config selecting the SYNCED_ROLE
        prefix, and apply it via the role_sync_options field mask."""
        link = self.get_link(LINK_NAME)
        link.configurations.role_sync_options.CopyFrom(
            shadow_link_pb2.RoleSyncOptions(
                interval=duration_pb2.Duration(seconds=1),
                paused=False,
                role_name_filters=[
                    shadow_link_pb2.NameFilter(
                        pattern_type=shadow_link_pb2.PATTERN_TYPE_PREFIX,
                        filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                        name="synced-",
                    )
                ],
            )
        )
        return self.update_link(
            shadow_link=link,
            update_mask=field_mask_pb2.FieldMask(
                paths=["configurations.role_sync_options"]
            ),
        )

    def _update_link_sr_api(self):
        """Fetch the link, add Schema Registry API-mode sync pointed at the
        source SR, and apply it via the schema_registry_sync_options mask."""
        link = self.get_link(LINK_NAME)
        api = shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryApi(
            source_url=self._src_sr.base_uri(),
            tail_interval=duration_pb2.Duration(seconds=2),
            full_sync_interval=duration_pb2.Duration(seconds=2),
        )
        link.configurations.schema_registry_sync_options.shadow_schema_registry_api.CopyFrom(
            api
        )
        return self.update_link(
            shadow_link=link,
            update_mask=field_mask_pb2.FieldMask(
                paths=["configurations.schema_registry_sync_options"]
            ),
        )

    def _update_link_sr_topic(self):
        """Fetch the link and enable topic-mode Schema Registry sync (shadow the
        source _schemas topic byte-for-byte). Unlike API-mode, topic-mode
        predates v26.2 and is not behind the shadow_link_sr_api_sync gate, so it
        is accepted even while the upgrade is unfinalized."""
        link = self.get_link(LINK_NAME)
        link.configurations.schema_registry_sync_options.shadow_schema_registry_topic.CopyFrom(
            shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryTopic()
        )
        return self.update_link(
            shadow_link=link,
            update_mask=field_mask_pb2.FieldMask(
                paths=["configurations.schema_registry_sync_options"]
            ),
        )

    def _assert_sr_sync_mode(self, expected, label):
        """Read the link back through the controller and assert its persisted
        Schema Registry sync mode is `expected` (a oneof field name, e.g.
        "shadow_schema_registry_topic")."""
        opts = self.get_link(LINK_NAME).configurations.schema_registry_sync_options
        got = opts.WhichOneof("schema_registry_shadowing_mode")
        assert got == expected, f"expected SR sync mode {expected} ({label}), got {got}"

    # ---- lifecycle phases --------------------------------------------------

    def _assert_gated(self, action, gate_msg, what):
        """Assert `action` -- an update engaging a v26.2-gated sync surface -- is
        refused with FAILED_PRECONDITION carrying the feature-specific gate
        message. This is what keeps a downgrade safe: no gated state reaches the
        controller log while the upgrade is unfinalized."""
        try:
            action()
        except ConnectError as e:
            assert e.code == ConnectErrorCode.FAILED_PRECONDITION, (
                f"{what} should be gated by a precondition, got {e}"
            )
            assert gate_msg in str(e), (
                f"{what} rejected, but not via the feature gate: {e}"
            )
        else:
            raise AssertionError(f"{what} should be gated while unfinalized")

    def _check_sync_gates_unfinalized(self):
        """Pin down the state of the v26.2 sync surfaces while unfinalized.

        When the old release predates the sync features, both surfaces must be
        refused on the target with FAILED_PRECONDITION and the feature-specific
        message -- this is what keeps a downgrade safe (no role/SR-API state
        the old binary cannot read can be written). When the old release
        already ships the features, they activated before the upgrade began
        and any sync state is legible to the old binary; there is no gate to
        observe, so instead assert the unfinalized upgrade did not regress them
        to unavailable."""
        if self.sync_features_gated:
            self._assert_gated(self._update_link_role_sync, ROLE_SYNC_GATE, "role sync")
            self._assert_gated(
                self._update_link_sr_api, SR_API_GATE, "SR API-mode sync"
            )
        else:
            for feature in ("shadow_link_role_sync", "shadow_link_sr_api_sync"):
                state = self.redpanda.get_feature_state(feature)
                assert state == "active", (
                    f"{feature} was active on the old release and must stay "
                    f"active while unfinalized, got {state}"
                )

    def _verify_role_sync_works(self):
        """Post-finalize: configuring role sync is accepted and the migrator
        actually reproduces the filtered roles on the target."""
        self._update_link_role_sync()

        def synced():
            return self._role_members(self._dst_roles, SYNCED_ROLE) == {
                "alice",
                "eng",
            }

        wait_until(
            synced,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="filtered role did not sync to the target after finalize",
        )
        assert EXCLUDED_ROLE not in self._dst_roles.list_role_names(), (
            "a role outside the sync filter was mirrored to the target"
        )

    def _verify_sr_api_sync_works(self):
        """Post-finalize: configuring SR API-mode sync is accepted and the
        source's subject appears in the target's Schema Registry.

        _schemas is never topic-mirrored here (it is shadowed only under
        topic-mode sync, not by the topic filter), so SR API-mode is the only
        path by which a schema can reach the target SR. Assert the
        subject is absent before configuring it, so its later appearance is
        attributable to the feature rather than to topic mirroring or pre-existing
        state -- the same rigor the role-sync check gets from the migrator being
        the sole path for roles."""
        assert SR_SUBJECT not in self._target_subjects(), (
            f"{SR_SUBJECT} is on the target SR before SR API-mode sync was "
            "configured; it reached the target by some other path (e.g. _schemas "
            "topic mirroring), which would make this verification vacuous"
        )
        self._update_link_sr_api()

        wait_until(
            lambda: SR_SUBJECT in self._target_subjects(),
            timeout_sec=90,
            backoff_sec=1,
            err_msg="schema did not sync to the target Schema Registry after finalize",
        )

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_full_lifecycle(self):
        # Seed the source data plane that the link mirrors/syncs from.
        self._src_roles.create_role(
            role=SYNCED_ROLE,
            members=[
                security_pb2.RoleMember(user=security_pb2.RoleUser(name="alice")),
                security_pb2.RoleMember(group=security_pb2.RoleGroup(name="eng")),
            ],
        )
        self._src_roles.create_role(
            role=EXCLUDED_ROLE,
            members=[security_pb2.RoleMember(user=security_pb2.RoleUser(name="bob"))],
        )
        self._register_source_schema()
        RpkTool(self.source_cluster_service).create_topic(MIRROR_TOPIC, partitions=1)

        # Opt out of auto-finalization on the old binary, before upgrading.
        self._disable_auto_finalization()

        # Roll the target forward to HEAD. Auto-finalization is off, so the
        # active version is held at the old version: READY_TO_FINALIZE.
        self._restart_at_new(self.redpanda.nodes)
        status = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE
        )
        assert self.admin.get_features()["cluster_version"] == self.old_logical
        assert not status.auto_finalization_enabled

        # The shadow_linking feature is v25.3, so the link machinery is fully
        # live even though the upgrade is unfinalized: create a topic-mirroring
        # link (mirroring only MIRROR_TOPIC -- see _create_link_mirroring_topic
        # for why _schemas must be excluded). This create writes a v1
        # link_configuration to the controller log.
        self._create_link_mirroring_topic(MIRROR_TOPIC)
        self._produce_source(MIRROR_TOPIC, MIRROR_RECORDS_PRE)
        self._wait_mirrored(MIRROR_TOPIC, MIRROR_RECORDS_PRE, "unfinalized")

        # ...and the v26.2 sync surfaces are refused or active depending on
        # whether the old release predates them.
        self._check_sync_gates_unfinalized()

        # Roll the target back to the old release WITHOUT finalizing. The v1
        # link_configuration written by HEAD must replay on the old binary: the
        # cluster returns healthy, the active version is unchanged, and mirroring
        # resumes -- proving the persisted record survived the version boundary.
        self._downgrade_all_to(self.old_release)
        assert self.admin.get_features()["cluster_version"] == self.old_logical
        self._produce_source(MIRROR_TOPIC, MIRROR_RECORDS_POST)
        self._wait_mirrored(
            MIRROR_TOPIC, MIRROR_RECORDS_PRE + MIRROR_RECORDS_POST, "downgraded"
        )

        # Roll forward again and this time finalize: the active version advances
        # past the gate's require_version and the v26.2 features activate.
        self._restart_at_new(self.redpanda.nodes)
        self._wait_for_status_state(features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE)
        self._finalize()
        self._wait_for_version_everywhere(self.new_logical)
        finalized = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_FINALIZED
        )
        assert finalized.active_version == self.new_logical

        # Both gates are open now: the features must actually sync real data.
        self._verify_role_sync_works()
        self._verify_sr_api_sync_works()

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_topic_mode_sr_sync_survives_downgrade(self):
        """Topic-mode Schema Registry sync -- shadowing the source _schemas topic
        byte-for-byte -- is the pre-v26.2 mechanism and is NOT behind the
        shadow_link_sr_api_sync gate. An operator can therefore enable it during
        the (potentially weeks-long) unfinalized window, so the *populated*
        schema_registry_sync_config it persists must survive a rollback to the
        prior release.

        test_full_lifecycle only ever persists an empty schema_registry_sync_cfg
        across the downgrade (its link mirrors a data topic, and API-mode is
        gated). This drives a populated one through a real downgrade/replay --
        the integration counterpart to the serde unit test
        schema_registry_sync_config_legacy_reads_v1_topic_mode, where a v0
        (prior-release) reader must recover field 0 (topic mode) from the v1
        record the upgraded binary wrote.

        Lifecycle:
          1. Unfinalized on HEAD: topic-mode is accepted and read back through
             the controller. When the prior release predates the v26.2 sync
             features, API-mode is additionally refused (gated) -- the gate
             distinguishes the two modes.
          2. Roll back to the prior release without finalizing: the cluster
             returns healthy (the populated v1 record replayed on the old binary)
             and the link's data plane keeps mirroring.
          3. Roll forward again: the topic-mode config is still intact, proving it
             made the v1 -> v0 -> v1 round trip uncorrupted.

        Note: topic-mode has no source-side preflight (sr_preflight_checker only
        probes API-mode), and no source schema is registered here, so enabling it
        persists the config without actually shadowing _schemas. Functional
        _schemas shadowing is covered by the dedicated SR sync suites; this test
        is about the persisted config surviving the version boundary.
        """
        RpkTool(self.source_cluster_service).create_topic(MIRROR_TOPIC, partitions=1)

        # Opt out of auto-finalization on the old binary, then roll the target
        # forward to HEAD with the active (downgrade-floor) version held back.
        self._disable_auto_finalization()
        self._restart_at_new(self.redpanda.nodes)
        self._wait_for_status_state(features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE)
        assert self.admin.get_features()["cluster_version"] == self.old_logical

        # shadow_linking is v25.3, so the link machinery is live while
        # unfinalized: create a topic-mirroring link and confirm the data plane.
        self._create_link_mirroring_topic(MIRROR_TOPIC)
        self._produce_source(MIRROR_TOPIC, MIRROR_RECORDS_PRE)
        self._wait_mirrored(MIRROR_TOPIC, MIRROR_RECORDS_PRE, "unfinalized")

        # When the old release predates the sync features, the gate
        # distinguishes the SR sync modes: API-mode is refused while topic-mode
        # (pre-v26.2, ungated) is accepted. From a v26.2.1+ old release there
        # is no gate to observe on API-mode; the persistence round trip below
        # is regime-independent.
        if self.sync_features_gated:
            self._assert_gated(
                self._update_link_sr_api, SR_API_GATE, "SR API-mode sync"
            )
        # Topic-mode is accepted and persisted as a populated
        # schema_registry_sync_config in the controller log.
        self._update_link_sr_topic()
        self._assert_sr_sync_mode("shadow_schema_registry_topic", "unfinalized")

        # Roll back to the prior release WITHOUT finalizing. The populated v1
        # link_configuration must replay on the old binary: healthy cluster,
        # unchanged active version, and the data plane still mirroring.
        self._downgrade_all_to(self.old_release)
        assert self.admin.get_features()["cluster_version"] == self.old_logical
        self._produce_source(MIRROR_TOPIC, MIRROR_RECORDS_POST)
        self._wait_mirrored(
            MIRROR_TOPIC, MIRROR_RECORDS_PRE + MIRROR_RECORDS_POST, "downgraded"
        )

        # Roll forward again: the topic-mode config survived the round trip.
        self._restart_at_new(self.redpanda.nodes)
        self._wait_for_status_state(features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE)
        self._assert_sr_sync_mode("shadow_schema_registry_topic", "re-upgraded")
