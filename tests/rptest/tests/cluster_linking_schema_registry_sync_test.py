# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# End-to-end coverage for the shadow-link Schema Registry "API mode" sync: a
# reference-heavy set of source schemas is replicated into the destination
# cluster's Schema Registry over the HTTP source reader, including schemas added
# while a sync is in flight and schemas added after a sync (picked up by the
# next full sync). The layered diamond DAG mirrors the reconciler
# `concurrent_stress` unit test at a larger scale, so the concurrent fetch/import
# path is exercised against a real registry. That suite runs plaintext, default
# context, no auth; source-SR authentication and TLS coverage lives in the
# SchemaRegistrySyncAuthMixin leaves below (HTTP Basic, https, mTLS).

import json
import socket
from typing import Any

import google.protobuf.duration_pb2
import google.protobuf.field_mask_pb2
from connectrpc.errors import ConnectError, ConnectErrorCode
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.v2 import shadow_link_pb2
from rptest.clients.admin.proto.redpanda.core.common.v1 import tls_pb2
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import TestContext, cluster
from rptest.services.confluent_schema_registry import ConfluentSchemaRegistryService
from rptest.services.multi_cluster_services import (
    SecondaryClusterArgs,
    SecondaryClusterSpec,
    ServiceType,
)
from rptest.services.redpanda import (
    MetricsEndpoint,
    RedpandaService,
    SchemaRegistryConfig,
    SecurityConfig,
)
from rptest.services.tls import Certificate, TLSCertManager
from rptest.tests.cluster_linking_test_base import (
    ClusterLinkingTLSProvider,
    ShadowLinkTestBase,
)
from rptest.tests.schema_registry_test import SchemaRegistryRedpandaClient
from rptest.util import expect_exception, firewall_blocked

NS = "com.acme"
LINK_NAME = "sr-sync"

Pair = tuple[str, int]


class SchemaRegistrySyncMixin:
    """Shared shadow-link Schema Registry "API mode" sync suite: source cluster
    SR -> destination cluster SR over the HTTP source reader, at scale and with
    concurrent / post-sync additions.

    Not a Test: mixed into a ShadowLinkTestBase leaf per source vendor (see
    SchemaRegistrySyncE2ETest / ConfluentSchemaRegistrySyncE2ETest below). Each
    leaf supplies the source registry via _make_source_client and declares its
    own node budget; ducktape collects the leaves, never this class."""

    # Members supplied by ShadowLinkTestBase once mixed into a leaf.
    logger: Any
    source_cluster_service: Any
    target_cluster_service: Any
    create_default_link_request: Any
    create_link_with_request: Any
    get_link: Any

    # A layered diamond DAG (top -> mid -> leaf), like the reconciler
    # concurrent_stress unit test but larger: adjacent referrers share referents,
    # so the reconciler must import referents-first while several fibers fetch
    # over the (serialized) HTTP source connection.
    LEAVES = 30
    MIDS = 15
    TOPS = 8
    # Added to the source right after the link is created, racing the first
    # in-flight sync.
    EXTRA_LEAVES = 10
    EXTRA_MIDS = 10

    # --- source Schema Registry hook ---------------------------------------
    # Each leaf points this at its vendor's Schema Registry; the destination
    # side stays Redpanda throughout.

    def _make_source_client(self) -> SchemaRegistryRedpandaClient:
        raise NotImplementedError

    def _source_sr_url(self) -> str:
        # Derive from the seeding client so the link source_url and the client
        # can never point at different registries.
        return self._make_source_client().base_uri()

    # --- schema builders ---------------------------------------------------

    def _record(self, name: str, fields: list[dict]) -> dict:
        return {"type": "record", "name": name, "namespace": NS, "fields": fields}

    def _leaf_schema(self, i: int, evolve: bool = False) -> dict:
        fields: list[dict] = [{"name": "v", "type": "string"}]
        if evolve:
            # Backward-compatible new version: an added field with a default.
            fields.append({"name": "extra", "type": "long", "default": 0})
        return self._record(f"Leaf{i}", fields)

    def _mid_schema(self, i: int, la: int, lb: int) -> dict:
        return self._record(
            f"Mid{i}",
            [
                {"name": "a", "type": f"{NS}.Leaf{la}"},
                {"name": "b", "type": f"{NS}.Leaf{lb}"},
            ],
        )

    def _top_schema(self, i: int, ma: int, mb: int) -> dict:
        return self._record(
            f"Top{i}",
            [
                {"name": "x", "type": f"{NS}.Mid{ma}"},
                {"name": "y", "type": f"{NS}.Mid{mb}"},
            ],
        )

    def _large_schema(self, i: int, default_bytes: int) -> dict:
        # Inflate the raw stored definition with a big field default. Defaults
        # are kept verbatim in the stored body the reconciler budgets by
        # (unlike doc, which canonicalization may drop), so the memory budget
        # sees a genuinely large body.
        return self._record(
            f"Big{i}",
            [{"name": "v", "type": "string", "default": "x" * default_bytes}],
        )

    # --- registration helpers ----------------------------------------------

    def _register(
        self,
        client: SchemaRegistryRedpandaClient,
        subject: str,
        schema: dict,
        references: list[dict] | None = None,
        metadata: dict | None = None,
        rule_set: dict | None = None,
    ) -> int:
        payload: dict[str, Any] = {"schema": json.dumps(schema)}
        if references:
            payload["references"] = references
        # Confluent-only Data Contract fields Redpanda does not model. A source
        # that carries them lets a test drive the unsupported-feature policy.
        if metadata is not None:
            payload["metadata"] = metadata
        if rule_set is not None:
            payload["ruleSet"] = rule_set
        resp = client.post_subjects_subject_versions(
            subject=subject, data=json.dumps(payload)
        )
        assert resp.status_code == 200, f"register {subject} failed: {resp.text}"
        return resp.json()["id"]

    def _ref(self, name: str, subject: str, version: int = 1) -> dict:
        return {"name": name, "subject": subject, "version": version}

    def _add_leaf(
        self,
        src: SchemaRegistryRedpandaClient,
        i: int,
        evolve: bool = False,
        version: int = 1,
    ) -> Pair:
        # `version` is the subject version this registration produces (1 for a
        # fresh subject, 2 for the evolved leaf-0); it is not the schema id that
        # _register returns.
        self._register(src, f"leaf-{i}-value", self._leaf_schema(i, evolve))
        return (f"leaf-{i}-value", version)

    def _add_mid(
        self, src: SchemaRegistryRedpandaClient, i: int, la: int, lb: int
    ) -> Pair:
        self._register(
            src,
            f"mid-{i}-value",
            self._mid_schema(i, la, lb),
            references=[
                self._ref(f"{NS}.Leaf{la}", f"leaf-{la}-value"),
                self._ref(f"{NS}.Leaf{lb}", f"leaf-{lb}-value"),
            ],
        )
        return (f"mid-{i}-value", 1)

    def _add_top(
        self, src: SchemaRegistryRedpandaClient, i: int, ma: int, mb: int
    ) -> Pair:
        self._register(
            src,
            f"top-{i}-value",
            self._top_schema(i, ma, mb),
            references=[
                self._ref(f"{NS}.Mid{ma}", f"mid-{ma}-value"),
                self._ref(f"{NS}.Mid{mb}", f"mid-{mb}-value"),
            ],
        )
        return (f"top-{i}-value", 1)

    def _seed_initial_dag(self, src: SchemaRegistryRedpandaClient) -> list[Pair]:
        # Register referents before referrers (the source SR requires it); the
        # reconciler rediscovers the order itself on the destination.
        pairs: list[Pair] = []
        for i in range(self.LEAVES):
            pairs.append(self._add_leaf(src, i))
        for i in range(self.MIDS):
            pairs.append(
                self._add_mid(src, i, (2 * i) % self.LEAVES, (2 * i + 1) % self.LEAVES)
            )
        for i in range(self.TOPS):
            pairs.append(
                self._add_top(src, i, (2 * i) % self.MIDS, (2 * i + 1) % self.MIDS)
            )
        return pairs

    # --- link creation -----------------------------------------------------

    def _create_sr_link(
        self,
        source_url: str | None = None,
        full_sync_interval_sec: int = 2,
        source_filter_subjects: list[str] | None = None,
        source_filter_contexts: list[str] | None = None,
        exact_context_map: dict[str, str] | None = None,
        feature_policy: shadow_link_pb2.UnsupportedSchemaFeaturePolicy.ValueType
        | None = None,
        max_source_requests_per_second: int | None = None,
    ) -> str:
        # Create a shadow link that syncs only the Schema Registry, in API mode,
        # pointing at the source cluster's SR endpoint (or an explicit URL, e.g.
        # a bad one to exercise the unavailable path). Short intervals so
        # subsequent full syncs land quickly. An optional exact-subject filter
        # scopes replication. Returns the source URL used.
        if source_url is None:
            source_url = self._source_sr_url()
        req = self.create_default_link_request(
            link_name=LINK_NAME,
            mirror_all_topics=False,
            mirror_all_groups=False,
            mirror_all_acls=False,
        )
        api = shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryApi(
            source_url=source_url,
            tail_interval=google.protobuf.duration_pb2.Duration(seconds=2),
            full_sync_interval=google.protobuf.duration_pb2.Duration(
                seconds=full_sync_interval_sec
            ),
        )
        if max_source_requests_per_second is not None:
            api.max_source_requests_per_second = max_source_requests_per_second
        if feature_policy is not None:
            api.unsupported_schema_feature_policy = feature_policy
        if source_filter_subjects is not None:
            api.source_filter.subjects.extend(source_filter_subjects)
        if source_filter_contexts is not None:
            api.source_filter.contexts.extend(source_filter_contexts)
        if exact_context_map is not None:
            for source, destination in exact_context_map.items():
                api.destination.exact.mappings.add(
                    source=source, destination=destination
                )
        self._maybe_apply_source_sr_credentials(api)
        req.shadow_link.configurations.schema_registry_sync_options.shadow_schema_registry_api.CopyFrom(
            api
        )
        self._maybe_apply_source_kafka_credentials(
            req.shadow_link.configurations.client_options
        )
        self.create_link_with_request(req=req)
        return source_url

    def _maybe_apply_source_sr_credentials(
        self,
        api: "shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryApi",
    ) -> None:
        # Hook for auth/TLS leaves to attach source-SR credentials to the link
        # request. The plaintext no-auth suite leaves this a no-op; the auth
        # leaves (see SchemaRegistrySyncAuthMixin) override it to set
        # `auth_options.basic` and/or `tls_settings`.
        pass

    def _maybe_apply_source_kafka_credentials(
        self, client_options: "shadow_link_pb2.ShadowLinkClientOptions"
    ) -> None:
        # Hook for auth/TLS leaves to add the Kafka-level source credentials the
        # link needs when the source cluster runs SASL/TLS. No-op for the
        # plaintext no-auth suite.
        pass

    # --- verification ------------------------------------------------------

    def _schema_view(
        self, client: SchemaRegistryRedpandaClient, subject: str, version: int
    ) -> dict | None:
        # The canonical stored form of one (subject, version): the schema ID,
        # the parsed schema body, and its references. Returns None when absent.
        resp = client.get_subjects_subject_versions_version(subject, version)
        if resp.status_code != 200:
            return None
        body = resp.json()
        return {
            "id": body["id"],
            "schema": json.loads(body["schema"]),
            "references": [
                {"name": r["name"], "subject": r["subject"], "version": r["version"]}
                for r in body.get("references", [])
            ],
        }

    def _wait_synced(
        self,
        src: SchemaRegistryRedpandaClient,
        dest: SchemaRegistryRedpandaClient,
        pairs: list[Pair],
        timeout_sec: int = 120,
    ):
        # Compares the destination against the source's stored form (schema ID,
        # body, and references) so the assertion does not depend on whether the
        # SR canonicalizes on registration. A preserved ID is the point of
        # IMPORT-mode replication.
        def synced() -> bool:
            missing = 0
            for subject, version in pairs:
                want = self._schema_view(src, subject, version)
                assert want is not None, f"source missing {subject} v{version}"
                if self._schema_view(dest, subject, version) != want:
                    missing += 1
            if missing:
                self.logger.debug(f"{missing}/{len(pairs)} not yet synced")
            return missing == 0

        wait_until(
            synced,
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg="schemas did not sync to destination Schema Registry",
        )

    def _sr_versions(
        self, client: SchemaRegistryRedpandaClient, subject: str, deleted: bool
    ) -> set[int]:
        resp = client.get_subjects_subject_versions(subject, deleted=deleted)
        return set(resp.json()) if resp.status_code == 200 else set()

    def _version_state(
        self, client: SchemaRegistryRedpandaClient, subject: str
    ) -> tuple[frozenset[int], frozenset[int]]:
        # (active, soft-deleted) versions of a subject. A soft-deleted version
        # is listed only with deleted=true; a fully soft-deleted subject has an
        # empty active set.
        active = self._sr_versions(client, subject, deleted=False)
        every = self._sr_versions(client, subject, deleted=True)
        return frozenset(active), frozenset(every - active)

    def _wait_delete_synced(
        self,
        src: SchemaRegistryRedpandaClient,
        dest: SchemaRegistryRedpandaClient,
        subjects: list[str],
        timeout_sec: int = 120,
    ):
        # The destination's per-subject (active, deleted) version partition must
        # converge to the source's.
        def synced() -> bool:
            for subject in subjects:
                if self._version_state(dest, subject) != self._version_state(
                    src, subject
                ):
                    return False
            return True

        wait_until(
            synced,
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg="soft-delete state did not converge to the source",
        )

    # --- status-counter checks ---------------------------------------------

    SR_SYNC_METRIC_COUNTERS = (
        "subject_versions_changed",
        "compatibility_configs_changed",
        "modes_changed",
        "unsupported_features_removed",
        "errors",
    )

    def _sr_sync_metric_totals(
        self, metrics_endpoint: MetricsEndpoint
    ) -> dict[str, float] | None:
        """Cluster-wide sums of the shadow-link Schema Registry counters for
        LINK_NAME from the given metrics endpoint, keyed by counter name, or
        None while any counter has no series (e.g. no leader yet)."""
        patterns = {
            counter: f"shadow_link_schema_registry_{counter}"
            for counter in self.SR_SYNC_METRIC_COUNTERS
        }
        samples = self.target_cluster_service.metrics_samples(
            sample_patterns=list(patterns.values()), metrics_endpoint=metrics_endpoint
        )
        totals: dict[str, float] = {}
        for counter, pattern in patterns.items():
            if pattern not in samples:
                return None
            values = [
                s.value
                for s in samples[pattern].samples
                if s.labels["shadow_link_name"] == LINK_NAME
            ]
            if not values:
                return None
            totals[counter] = sum(values)
        return totals

    def _versions_changed_metric(self, metrics_endpoint: MetricsEndpoint) -> float:
        """The cluster-wide subject_versions_changed counter, or -1 while the
        series is absent (distinguishable from a real 0 so wait_until
        predicates can wait for either state)."""
        totals = self._sr_sync_metric_totals(metrics_endpoint)
        return totals["subject_versions_changed"] if totals is not None else -1.0

    def _verify_prometheus_counters(self, expected_versions: int):
        # Both metrics endpoints must agree with the admin API's
        # totals_since_task_start. The counters read the task's live status at
        # scrape time, so once the admin totals have converged no extra wait is
        # needed.
        for endpoint in (MetricsEndpoint.METRICS, MetricsEndpoint.PUBLIC_METRICS):
            totals = self._sr_sync_metric_totals(endpoint)
            assert totals is not None, f"no SR sync series on {endpoint}"
            self.logger.info(f"[{endpoint}] SR sync counters: {totals}")
            assert totals["subject_versions_changed"] == expected_versions, totals
            assert totals["errors"] == 0, totals
            for name in self.EXPECTED_ZERO_COUNTERS:
                assert totals[name] == 0, totals

    # Counters expected to stay zero in the override-free DAG tests: those DAGs
    # register no mode or compatibility overrides and carry no unsupported
    # fields, so mode/config replication and unsupported-feature handling are
    # no-ops there. Tests that do exercise them
    # (test_schema_registry_api_sync_compatibility, the
    # test_schema_registry_api_sync_unsupported_* suite) assert their counters
    # advance.
    EXPECTED_ZERO_COUNTERS = (
        "compatibility_configs_changed",
        "modes_changed",
        "unsupported_features_removed",
    )

    def _admin_sr_status(self):
        link = self.get_link(LINK_NAME)
        return link.status.schema_registry_sync_status

    def _observe_current_full_sync(self, timeout_sec: int = 60):
        # While a sync runs, the status exposes current_sync; capture it during
        # the first (longest-running) full sync of the seeded DAG and confirm it
        # is a FULL sync. Tail sync is not implemented yet, so FULL is the only
        # type produced -- this pins the reported sync_type regardless.
        observed: dict[str, Any] = {}

        def in_progress() -> bool:
            sr = self._admin_sr_status()
            if sr.HasField("current_sync"):
                observed["sync_type"] = sr.current_sync.sync_type
                return True
            return False

        wait_until(
            in_progress,
            timeout_sec=timeout_sec,
            backoff_sec=0.2,
            err_msg="never observed a Schema Registry sync in progress",
        )
        assert (
            observed["sync_type"] == shadow_link_pb2.SCHEMA_REGISTRY_SYNC_TYPE_FULL
        ), observed

    def _verify_counters(self, all_pairs: list[Pair]):
        expected_versions = len(all_pairs)
        expected_subjects = len({s for s, _ in all_pairs})

        # The counters reflect the most recently completed full sync; give the
        # task a couple of full-sync cycles to re-scan after the last addition.
        def counters_ready() -> bool:
            sr = self._admin_sr_status()
            inv = sr.inventory
            totals = sr.totals_since_task_start
            return (
                inv.selected_source_subjects == expected_subjects
                and inv.selected_source_subject_versions == expected_versions
                and inv.destination_subjects == expected_subjects
                and inv.destination_subject_versions == expected_versions
                and totals.subject_versions_changed == expected_versions
                and totals.errors == 0
            )

        wait_until(
            counters_ready,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="SR sync status counters did not reach the expected values",
        )

        sr = self._admin_sr_status()
        self._log_counters("admin API", sr)

        # Timestamps: the last completed full sync is present with finish at or
        # after start; the cumulative summary carries the task start time and,
        # being open-ended, has no finish time.
        assert sr.HasField("last_full_sync"), sr
        lfs = sr.last_full_sync
        assert lfs.HasField("start_time") and lfs.HasField("finish_time"), lfs
        assert lfs.finish_time.ToNanoseconds() >= lfs.start_time.ToNanoseconds(), lfs

        # Counters this suite does not exercise must stay zero (flagged above).
        totals = sr.totals_since_task_start
        # The cumulative summary carries the task start time (stamped once on the
        # task's first run) and, being open-ended, has no finish time.
        assert totals.HasField("start_time"), totals
        assert totals.start_time.seconds > 0, totals
        assert not totals.HasField("finish_time"), totals
        for name in self.EXPECTED_ZERO_COUNTERS:
            assert getattr(totals, name) == 0, (
                f"unexpected {name}={getattr(totals, name)}"
            )

        # Cross-check the prometheus counters and the rpk `shadow status`
        # rendering against the admin API.
        self._verify_prometheus_counters(expected_versions)
        self._verify_rpk_status(expected_subjects, expected_versions)

    def _log_counters(self, source: str, sr):
        inv = sr.inventory
        totals = sr.totals_since_task_start
        self.logger.info(
            f"[{source}] inventory: "
            f"selected_source_subjects={inv.selected_source_subjects} "
            f"selected_source_subject_versions={inv.selected_source_subject_versions} "
            f"destination_subjects={inv.destination_subjects} "
            f"destination_subject_versions={inv.destination_subject_versions}"
        )
        self.logger.info(
            f"[{source}] totals_since_task_start: "
            f"subject_versions_changed={totals.subject_versions_changed} "
            f"compatibility_configs_changed={totals.compatibility_configs_changed} "
            f"modes_changed={totals.modes_changed} "
            f"unsupported_features_removed={totals.unsupported_features_removed} "
            f"errors={totals.errors}"
        )

    def _verify_rpk_status(self, expected_subjects: int, expected_versions: int):
        # `rpk shadow status --format json` renders the same admin-v2
        # SchemaRegistrySyncStatus; assert it agrees with the admin API.
        rpk = RpkTool(self.target_cluster_service)
        status = rpk.shadow_status(LINK_NAME)
        self.logger.info(f"rpk shadow status: {json.dumps(status)}")
        sr = status["schema_registry"]
        inv = sr["inventory"]
        totals = sr["totals_since_task_start"]
        assert inv["selected_source_subjects"] == expected_subjects, status
        assert inv["selected_source_subject_versions"] == expected_versions, status
        assert inv["destination_subjects"] == expected_subjects, status
        assert inv["destination_subject_versions"] == expected_versions, status
        assert totals["subject_versions_changed"] == expected_versions, status
        assert totals["errors"] == 0, status
        for name in self.EXPECTED_ZERO_COUNTERS:
            assert totals[name] == 0, status

    def _test_schema_registry_api_sync_e2e(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        # The destination `_schemas` topic is NOT warmed up here on purpose:
        # creating the SR-API shadow link (below) bootstraps it, so this
        # exercises that path.

        # 1. Seed the source SR with the large reference DAG.
        initial = self._seed_initial_dag(src)

        # 2. Create a shadow link that only syncs the Schema Registry.
        source_sr_url = self._create_sr_link()
        self.logger.info(f"source SR: {source_sr_url}; seeded {len(initial)} subjects")

        # While the first full sync of the seeded DAG runs, confirm the status
        # reports an in-progress FULL sync.
        self._observe_current_full_sync()

        # 3. SCENARIO A -- additions racing an in-flight sync. Immediately after
        #    creating the link (the first full sync is now running), register a
        #    second wave of leaves + mids that reference them. Whether they are
        #    enumerated by the current sync or the next one, they must all land.
        mid_sync: list[Pair] = []
        for i in range(self.LEAVES, self.LEAVES + self.EXTRA_LEAVES):
            mid_sync.append(self._add_leaf(src, i))
        for i in range(self.MIDS, self.MIDS + self.EXTRA_MIDS):
            j = i - self.MIDS
            la = self.LEAVES + (2 * j) % self.EXTRA_LEAVES
            lb = self.LEAVES + (2 * j + 1) % self.EXTRA_LEAVES
            mid_sync.append(self._add_mid(src, i, la, lb))

        # Wait for the initial DAG and the mid-sync additions to all replicate,
        # with bodies, references, and IDs matching the source.
        self._wait_synced(src, dest, initial + mid_sync)
        self.logger.info(
            f"initial + mid-sync waves synced ({len(initial) + len(mid_sync)} subjects)"
        )

        # 4. SCENARIO B -- additions after a completed sync, picked up by the
        #    next full sync. Add a new backward-compatible version of an existing
        #    subject, a brand-new standalone subject, and new tops referencing
        #    already-synced mids.
        after_sync: list[Pair] = []
        after_sync.append(self._add_leaf(src, 0, evolve=True, version=2))
        self._register(
            src, "late-value", self._record("Late", [{"name": "n", "type": "int"}])
        )
        after_sync.append(("late-value", 1))
        for i in range(self.TOPS, self.TOPS + 4):
            after_sync.append(
                self._add_top(src, i, (2 * i) % self.MIDS, (2 * i + 1) % self.MIDS)
            )

        # Re-verify leaf-0 v1 alongside its new v2: a backward-compatible new
        # version must not clobber the original. Kept out of all_pairs below so
        # the counter totals are not double-counted.
        self._wait_synced(src, dest, after_sync + [("leaf-0-value", 1)])
        self.logger.info("post-sync additions replicated to destination")

        # 5. Sanity-check the destination subject inventory covers every wave.
        all_pairs = initial + mid_sync + after_sync
        all_subjects = {s for s, _ in all_pairs}
        dest_subjects = set(dest.get_subjects().json())
        # Exact equality (not subset): the destination must hold the synced
        # subjects and nothing more -- catches spurious or duplicated imports.
        assert dest_subjects == all_subjects, (
            f"missing: {all_subjects - dest_subjects}; "
            f"unexpected: {dest_subjects - all_subjects}"
        )

        # 6. Surface and verify the Schema Registry sync status counters, both
        #    through the shadow-link admin API and the rpk `shadow status`
        #    rendering of it.
        self._verify_counters(all_pairs)

    def _test_schema_registry_api_sync_soft_delete(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        # seeded-value: mixed active/deleted before the first sync.
        # after-value: version-level delete after a sync.
        # whole-value: subject-level delete after a sync (source's
        #   active-only listing then 404s).
        seeded, after, whole = "seeded-value", "after-value", "whole-value"

        def _v1(name: str) -> dict:
            return self._record(name, [{"name": "v", "type": "string"}])

        def _v2(name: str) -> dict:
            # Backward-compatible evolution (added field with a default).
            return self._record(
                name,
                [
                    {"name": "v", "type": "string"},
                    {"name": "e", "type": "long", "default": 0},
                ],
            )

        self._register(src, seeded, _v1("Seeded"))
        self._register(src, seeded, _v2("Seeded"))
        assert src.delete_subject_version(seeded, "2").status_code == 200

        self._register(src, after, _v1("After"))
        self._register(src, after, _v2("After"))

        self._register(src, whole, _v1("Whole"))

        self._create_sr_link()

        subjects = [seeded, after, whole]
        self._wait_delete_synced(src, dest, subjects)

        assert src.delete_subject_version(after, "2").status_code == 200
        assert src.delete_subject(whole).status_code == 200
        self._wait_delete_synced(src, dest, subjects)

    def _test_schema_registry_api_sync_compatibility(self):
        # A subject-level compatibility override round-tripping through the
        # destination's write API -- a path the reconciler unit fakes cannot
        # cover -- then un-setting it so the destination reverts to the
        # inherited default. Mode replication is covered e2e by
        # test_schema_registry_api_sync_context_remap.
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        keep = "keep-value"
        schema = self._record("V", [{"name": "v", "type": "string"}])
        self._register(src, keep, schema)
        # A subject-level compatibility override on the source, to be mirrored.
        assert (
            src.set_config_subject(
                keep, data=json.dumps({"compatibility": "FULL"})
            ).status_code
            == 200
        )

        self._create_sr_link()
        self._wait_synced(src, dest, [(keep, 1)])

        # The compatibility override replicates to the destination and moves the
        # compatibility_configs_changed counter.
        def config_synced() -> bool:
            c = dest.get_config_subject(keep)
            return c.status_code == 200 and c.json().get("compatibilityLevel") == "FULL"

        wait_until(
            config_synced,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="compatibility config did not replicate to the destination",
        )
        wait_until(
            lambda: self._admin_sr_status().totals_since_task_start.compatibility_configs_changed
            >= 1,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="compatibility_configs_changed counter did not advance",
        )

        # Un-setting the source override propagates as a delete, reverting the
        # destination subject to the inherited (global) default rather than
        # leaving the stale FULL override behind.
        before = self._admin_sr_status().totals_since_task_start.compatibility_configs_changed
        assert src.delete_config_subject(keep).status_code == 200

        def config_delete_synced() -> bool:
            # With no subject-level override remaining, a non-fallback GET on the
            # destination reports it missing (40401); a fallback GET now yields
            # the global default rather than the removed FULL override.
            missing = dest.get_config_subject(keep).status_code == 404
            fallback = dest.get_config_subject(keep, fallback=True)
            reverted = (
                fallback.status_code == 200
                and fallback.json().get("compatibilityLevel") != "FULL"
            )
            return missing and reverted

        wait_until(
            config_delete_synced,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="config override deletion did not replicate to the destination",
        )
        wait_until(
            lambda: self._admin_sr_status().totals_since_task_start.compatibility_configs_changed
            > before,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="compatibility_configs_changed counter did not advance for the delete",
        )

    def _test_schema_registry_api_sync_hard_delete(self):
        # A source hard-delete (which requires a soft-delete first on the
        # destination) being propagated as a purge -- a path the reconciler unit
        # fakes cannot cover.
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        keep, gone = "keep-value", "gone-value"
        schema = self._record("V", [{"name": "v", "type": "string"}])
        self._register(src, keep, schema)
        self._register(src, gone, schema)

        self._create_sr_link()
        self._wait_synced(src, dest, [(keep, 1), (gone, 1)])

        # Hard-delete gone-value at the source (soft then permanent). The next
        # full sync must purge it from the destination, soft-deleting the still
        # active destination version before the permanent delete.
        assert src.delete_subject(gone).status_code == 200
        assert src.delete_subject(gone, permanent=True).status_code == 200

        def gone_purged() -> bool:
            active, deleted = self._version_state(dest, gone)
            return not active and not deleted

        wait_until(
            gone_purged,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="source hard-delete was not propagated to the destination",
        )

        # The in-source subject is untouched by the purge.
        assert self._schema_view(dest, keep, 1) is not None

    def _test_schema_registry_api_sync_context_delete(self):
        # A whole source context is deleted (Confluent-style: empty it, then
        # DELETE /contexts). The destination must converge: the context's
        # subjects purged and the context itself tombstoned so it no longer
        # lists, while a default-context subject is left untouched.
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        prod_orders = ":.prod:orders-value"
        keep = "keep-value"  # default context

        self._register(
            src, prod_orders, self._record("Orders", [{"name": "v", "type": "string"}])
        )
        self._register(
            src, keep, self._record("Keep", [{"name": "v", "type": "string"}])
        )

        self._create_sr_link()

        # Both subjects replicate and the .prod context materializes on the
        # destination.
        self._wait_synced(src, dest, [(prod_orders, 1), (keep, 1)])
        wait_until(
            lambda: ".prod" in set(dest.get_contexts().json()),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="the .prod context did not materialize on the destination",
        )

        # Delete the whole .prod context at the source: hard-delete its only
        # subject to empty it (soft then permanent, as the source requires), then
        # DELETE /contexts/.prod.
        assert src.delete_subject(prod_orders).status_code == 200
        assert src.delete_subject(prod_orders, permanent=True).status_code == 200
        assert src.delete_context(".prod").status_code in (200, 204)

        # The next full sync purges the .prod subject and tombstones the context,
        # so the destination's context listing drops back to the default context.
        wait_until(
            lambda: ".prod" not in set(dest.get_contexts().json()),
            timeout_sec=90,
            backoff_sec=1,
            err_msg="source context deletion did not propagate to the destination",
        )

        # The default-context subject is untouched; the .prod subject is gone.
        assert self._schema_view(dest, keep, 1) is not None
        assert self._schema_view(dest, prod_orders, 1) is None

    def _test_schema_registry_api_sync_context_remap(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        def qualified(ctx: str, name: str) -> str:
            return f":{ctx}:{name}"

        def by_name(refs: list[dict]) -> list[dict]:
            return sorted(refs, key=lambda r: r["name"])

        base_src = qualified(".prod", "base-value")
        leaf_src = qualified(".prod", "leaf-value")
        orders_src = qualified(".prod", "orders-value")
        base_dst = qualified(".dest", "base-value")
        leaf_dst = qualified(".dest", "leaf-value")
        orders_dst = qualified(".dest", "orders-value")

        base_schema = self._record("Base", [{"name": "b", "type": "string"}])
        leaf_schema = self._record("Leaf", [{"name": "l", "type": "string"}])
        orders_schema = self._record(
            "Orders",
            [
                {"name": "base", "type": f"{NS}.Base"},
                {"name": "leaf", "type": f"{NS}.Leaf"},
            ],
        )

        # Register referents first (the source SR validates references on
        # registration). orders references base with a context-qualified
        # reference (":.prod:base-value") and leaf with an unqualified one (the
        # bare "leaf-value", which the source resolves against orders' own .prod
        # context).
        self._register(src, base_src, base_schema)
        self._register(src, leaf_src, leaf_schema)
        self._register(
            src,
            orders_src,
            orders_schema,
            references=[
                self._ref(f"{NS}.Base", base_src),
                self._ref(f"{NS}.Leaf", "leaf-value"),
            ],
        )

        # Subject-level compatibility and mode overrides on the referrer, both to
        # be mirrored under the remapped context. Set the mode last: READONLY
        # blocks no further writes here, and the destination import bypasses it.
        assert (
            src.set_config_subject(
                orders_src, data=json.dumps({"compatibility": "FULL"})
            ).status_code
            == 200
        )
        assert (
            src.set_mode_subject(
                orders_src, data=json.dumps({"mode": "READONLY"})
            ).status_code
            == 200
        )

        # The source's canonical stored forms; the destination is compared
        # against these (not against the registered dicts) so the assertions do
        # not depend on SR canonicalization. Capturing them also guards the test
        # premise: the qualified reference is stored qualified and the
        # unqualified one is NOT canonicalized into a qualified subject.
        src_base = self._schema_view(src, base_src, 1)
        src_leaf = self._schema_view(src, leaf_src, 1)
        src_orders = self._schema_view(src, orders_src, 1)
        assert src_base is not None and src_leaf is not None
        assert src_orders is not None
        assert by_name(src_orders["references"]) == by_name(
            [
                {"name": f"{NS}.Base", "subject": base_src, "version": 1},
                {"name": f"{NS}.Leaf", "subject": "leaf-value", "version": 1},
            ]
        ), src_orders["references"]

        # Replicate only .prod, remapping it onto the destination .dest context.
        self._create_sr_link(
            source_filter_contexts=[".prod"],
            exact_context_map={".prod": ".dest"},
        )

        expected_orders_refs = by_name(
            [
                # The qualified reference's own context is remapped .prod -> .dest.
                {"name": f"{NS}.Base", "subject": base_dst, "version": 1},
                # The unqualified reference is untouched: on the destination it
                # resolves against orders' remapped .dest parent.
                {"name": f"{NS}.Leaf", "subject": "leaf-value", "version": 1},
            ]
        )

        def remapped() -> bool:
            b = self._schema_view(dest, base_dst, 1)
            leaf = self._schema_view(dest, leaf_dst, 1)
            o = self._schema_view(dest, orders_dst, 1)
            if b is None or leaf is None or o is None:
                return False
            # Referents (no references of their own) replicate verbatim.
            if b != src_base or leaf != src_leaf:
                return False
            # The referrer keeps its ID and body; only its reference contexts
            # remap.
            return (
                o["id"] == src_orders["id"]
                and o["schema"] == src_orders["schema"]
                and by_name(o["references"]) == expected_orders_refs
            )

        wait_until(
            remapped,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="schemas/references did not remap into the .dest context",
        )

        # Nothing is written under the original .prod context on the destination.
        for sub in (base_src, leaf_src, orders_src):
            assert self._schema_view(dest, sub, 1) is None, sub

        # The compatibility and mode overrides replicate under the remapped
        # context (read the explicit override, not the global fallback).
        def overrides_remapped() -> bool:
            c = dest.get_config_subject(orders_dst)
            m = dest.get_mode_subject(orders_dst, fallback=False)
            return (
                c.status_code == 200
                and c.json().get("compatibilityLevel") == "FULL"
                and m.status_code == 200
                and m.json().get("mode") == "READONLY"
            )

        wait_until(
            overrides_remapped,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="compatibility/mode overrides did not remap into .dest",
        )

        # Both overrides are counted, and the remap completed error-free.
        def counters_advanced() -> bool:
            totals = self._admin_sr_status().totals_since_task_start
            return (
                totals.compatibility_configs_changed >= 1
                and totals.modes_changed >= 1
                and totals.errors == 0
            )

        wait_until(
            counters_advanced,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="mode/compatibility counters did not advance under remap",
        )

    def _test_schema_registry_api_sync_out_of_scope_reference(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        # leaf-0 is a referent; mid-0 references it; ok-value is independent.
        self._add_leaf(src, 0)
        self._add_mid(src, 0, 0, 0)
        self._register(
            src, "ok-value", self._record("Ok", [{"name": "v", "type": "string"}])
        )

        # Select the referrer and the independent subject but NOT the referent,
        # so mid-0's reference is out of scope. Importing mid-0 must fail and be
        # counted, while ok-value still syncs and the full sync completes (the
        # link neither parks nor faults).
        self._create_sr_link(source_filter_subjects=["mid-0-value", "ok-value"])

        self._wait_synced(src, dest, [("ok-value", 1)])

        # last_full_sync.errors >= 1 proves the error was counted within a
        # completed full sync (best-effort), not that the task parked/faulted.
        def errored() -> bool:
            sr = self._admin_sr_status()
            return (
                sr.HasField("last_full_sync")
                and sr.last_full_sync.errors >= 1
                and sr.totals_since_task_start.errors >= 1
            )

        wait_until(
            errored,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="out-of-scope reference did not surface as a counted error",
        )

        # The counted error is also exported through the errors counter on
        # both prometheus endpoints (the value tests above only ever see it at
        # zero).
        for endpoint in (MetricsEndpoint.METRICS, MetricsEndpoint.PUBLIC_METRICS):
            totals = self._sr_sync_metric_totals(endpoint)
            assert totals is not None, f"no SR sync series on {endpoint}"
            assert totals["errors"] >= 1, totals

        # The referrer never imported; the in-scope independent subject did.
        dest_subjects = set(dest.get_subjects().json())
        assert "ok-value" in dest_subjects, dest_subjects
        assert "mid-0-value" not in dest_subjects, dest_subjects

    def _test_schema_registry_api_sync_recovers_after_source_unavailable(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        self._register(
            src,
            "orders-value",
            self._record("Orders", [{"name": "v", "type": "string"}]),
        )

        # Link creation preflight-probes the source for reachability, so the
        # link must be created against the live source; let its first full sync
        # land before the source is cut off.
        self._create_sr_link()
        self._wait_synced(src, dest, [("orders-value", 1)])

        wait_until(
            lambda: self._admin_sr_status().HasField("last_full_sync"),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="link did not complete its first full sync",
        )

        # Sever the destination cluster's egress to the source Schema Registry
        # (which listens on 8081): the running sync's list_contexts fails with
        # source_unavailable, so the task records last_error_message and leaves
        # _last_full_sync unadvanced -- it keeps retrying on the normal cadence,
        # so recovery needs no config change, just the source coming back.
        with firewall_blocked(self.target_cluster_service.nodes, 8081):
            wait_until(
                lambda: self._admin_sr_status().last_error_message != "",
                timeout_sec=90,
                backoff_sec=1,
                err_msg="link did not report an error while the source was unreachable",
            )
            # Frozen while unreachable (no full sync can complete), so a later
            # finish_time proves a fresh sync ran once connectivity returned.
            # last_error_message is sticky (never cleared on success), so it
            # cannot serve as the recovery signal.
            synced_before = (
                self._admin_sr_status().last_full_sync.finish_time.ToNanoseconds()
            )

        # Connectivity restored: the next full sync reaches the source on the
        # normal cadence and completes.
        wait_until(
            lambda: self._admin_sr_status().last_full_sync.finish_time.ToNanoseconds()
            > synced_before,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="link did not recover after the source became reachable again",
        )

    def _test_schema_registry_api_sync_memory_backpressure(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        # Shrink the reconcile memory budget to its 1 MiB floor and raise
        # parallelism so many bodies are fetched at once; their combined size
        # exceeds the budget, forcing the byte-budget semaphore to serialize.
        # Each body stays under the 200 KiB per-allocation cap the ducktape log
        # checker enforces, so no single import trips an oversized-allocation
        # failure while the aggregate still exercises backpressure.
        self.target_cluster_service.set_cluster_config(
            {
                "schema_registry_sync_memory_bytes": 1024 * 1024,
                "schema_registry_sync_parallelism": 14,
            }
        )

        # ~100 KiB per body x 16 subjects: 14 fetched concurrently is ~1.4 MiB,
        # above the 1 MiB budget.
        body = 100 * 1024
        pairs: list[Pair] = []
        for i in range(16):
            subject = f"big-{i}-value"
            self._register(src, subject, self._large_schema(i, body))
            pairs.append((subject, 1))

        # Effectively unthrottled: at the default 30 req/s the periodic full
        # syncs saturate the 2s interval on slow CI hardware, so teardown
        # always lands mid-sync and can hang node shutdown on an unabortable
        # destination write racing raft shutdown (issue #31108). A fast sync
        # restores the idle-at-teardown profile this test always ran with;
        # drop the override once #31108 is fixed. Rate-limiting behavior
        # itself is covered by test_schema_registry_api_sync_teardown_mid_sync.
        self._create_sr_link(max_source_requests_per_second=1000)
        self._wait_synced(src, dest, pairs)

    def _test_schema_registry_api_sync_teardown_mid_sync(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        # Enough subjects that a full sync at 2 requests/s takes far longer
        # than the 2s full-sync interval, so the task is effectively always
        # mid-sync from here on.
        pairs: list[Pair] = []
        for i in range(16):
            subject = f"subj-{i}-value"
            self._register(src, subject, self._leaf_schema(i))
            pairs.append((subject, 1))

        # The per-link cap doubles as coverage that the admin field reaches
        # the reader's rate limiter: at the default rate this sync finishes in
        # a couple of seconds, so _wait_synced only passes this slowly because
        # the field applied.
        self._create_sr_link(max_source_requests_per_second=2)
        self._wait_synced(src, dest, pairs)
        # The regression under test fires at teardown: stopping a node while
        # the throttled sync is mid-flight used to deadlock cluster_link
        # shutdown (the manager drained its work queue and gate before
        # aborting the tasks they were blocked on) until the 30s node-stop
        # timeout killed the node. Success here is simply a clean teardown.

    def _test_schema_registry_api_sync_survives_leadership_change(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)
        admin = Admin(self.target_cluster_service)

        # A small reference DAG so the first task instance does real import work.
        pairs: list[Pair] = []
        for i in range(5):
            pairs.append(self._add_leaf(src, i))
        for i in range(3):
            pairs.append(self._add_mid(src, i, (2 * i) % 5, (2 * i + 1) % 5))
        expected_versions = len(pairs)
        expected_subjects = len({s for s, _ in pairs})

        self._create_sr_link()
        self._wait_synced(src, dest, pairs)

        # Snapshot the first task instance once it has fully synced.
        def first_synced() -> bool:
            sr = self._admin_sr_status()
            return (
                sr.HasField("last_full_sync")
                and sr.inventory.destination_subject_versions == expected_versions
                and sr.totals_since_task_start.subject_versions_changed
                >= expected_versions
            )

        wait_until(
            first_synced,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="first task instance did not fully sync",
        )
        before_start = (
            self._admin_sr_status().totals_since_task_start.start_time.ToNanoseconds()
        )
        # The first instance exports its totals as prometheus counters from the
        # leader broker (summed cluster-wide; only the leader has the series).
        before_changed = (
            self._admin_sr_status().totals_since_task_start.subject_versions_changed
        )
        assert before_changed >= expected_versions
        assert (
            self._versions_changed_metric(MetricsEndpoint.PUBLIC_METRICS)
            == before_changed
        )

        # Move _schemas/0 leadership to another destination broker. The sync
        # task follows leadership, so a fresh instance takes over there.
        info = admin.get_partitions(namespace="kafka", topic="_schemas", partition=0)
        leader = info["leader_id"]
        replicas = [r["node_id"] for r in info["replicas"]]
        assert len(replicas) > 1, f"_schemas/0 has no failover target: {info}"
        target = next(n for n in replicas if n != leader)
        admin.partition_transfer_leadership(
            namespace="kafka", topic="_schemas", partition=0, target_id=target
        )
        admin.await_stable_leader(
            topic="_schemas",
            partition=0,
            namespace="kafka",
            timeout_s=60,
            check=lambda node_id: node_id == target,
        )

        # The new instance resets its cumulative counters (documented on
        # totals_since_task_start): a fresh, later start_time and
        # subject_versions_changed back at 0. It re-derives the destination
        # inventory from the replicated _schemas log and completes a full sync
        # that imports nothing (last_full_sync == 0) -- everything is already
        # present. This relies on the destination scan syncing the local store
        # first: without it, the freshly-elected leader could see a stale view
        # of its own store, spuriously re-import already-present versions, and
        # permanently inflate subject_versions_changed for this instance (it
        # only ever accumulates, so a transient race here would never recover).
        def re_derived() -> bool:
            sr = self._admin_sr_status()
            totals = sr.totals_since_task_start
            return (
                sr.HasField("last_full_sync")
                and totals.start_time.ToNanoseconds() > before_start
                and totals.subject_versions_changed == 0
                and totals.errors == 0
                and sr.last_full_sync.subject_versions_changed == 0
                and sr.last_full_sync.errors == 0
                and sr.inventory.selected_source_subjects == expected_subjects
                and sr.inventory.destination_subject_versions == expected_versions
            )

        wait_until(
            re_derived,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="new leader did not re-derive counters after the bounce",
        )

        # The cluster-wide metric drops to the new instance's 0: the old
        # leader unregistered its series on stop (a lingering stale series
        # would keep the sum at the pre-bounce value), and the new leader
        # exports the reset totals. -1 (no series anywhere) must not pass.
        wait_until(
            lambda: self._versions_changed_metric(MetricsEndpoint.PUBLIC_METRICS) == 0,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="prometheus counter did not follow leadership to the new broker",
        )

        # The new leader keeps syncing: a subject added now is imported by it,
        # advancing the new instance's cumulative change counter.
        base_changed = (
            self._admin_sr_status().totals_since_task_start.subject_versions_changed
        )
        self._register(
            src,
            "post-bounce-value",
            self._record("PostBounce", [{"name": "v", "type": "string"}]),
        )
        self._wait_synced(src, dest, [("post-bounce-value", 1)])
        wait_until(
            lambda: self._admin_sr_status().totals_since_task_start.subject_versions_changed
            > base_changed,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="new leader did not count the post-bounce import",
        )

        # Now bounce leadership back to the original broker (A -> B -> A). Its
        # task instance ran before, was stopped on losing leadership, and is now
        # reused -- not a fresh object. stop() clears the in-memory sync state,
        # so the returning leader must look just as fresh as B did: a later
        # start_time, zeroed cumulative counters, and an immediate full sync (a
        # stale _last_full_sync would otherwise defer it up to the full-sync
        # interval). Without the reset it would report its pre-B totals instead.
        expected_subjects += 1  # post-bounce-value
        expected_versions += 1
        before_return_start = (
            self._admin_sr_status().totals_since_task_start.start_time.ToNanoseconds()
        )
        admin.partition_transfer_leadership(
            namespace="kafka", topic="_schemas", partition=0, target_id=leader
        )
        admin.await_stable_leader(
            topic="_schemas",
            partition=0,
            namespace="kafka",
            timeout_s=60,
            check=lambda node_id: node_id == leader,
        )

        def returning_leader_fresh() -> bool:
            sr = self._admin_sr_status()
            totals = sr.totals_since_task_start
            return (
                sr.HasField("last_full_sync")
                and totals.start_time.ToNanoseconds() > before_return_start
                and totals.subject_versions_changed == 0
                and totals.errors == 0
                and sr.last_full_sync.subject_versions_changed == 0
                and sr.last_full_sync.errors == 0
                and sr.inventory.selected_source_subjects == expected_subjects
                and sr.inventory.destination_subject_versions == expected_versions
            )

        wait_until(
            returning_leader_fresh,
            timeout_sec=90,
            backoff_sec=1,
            err_msg="reused instance did not reset state on regaining leadership",
        )

        # Same series check for the return leg: B unregistered on stop and the
        # reused instance on A re-registered with its reset totals.
        wait_until(
            lambda: self._versions_changed_metric(MetricsEndpoint.PUBLIC_METRICS) == 0,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="prometheus counter did not follow leadership back to the original broker",
        )

    # --- unsupported-feature policy ----------------------------------------
    # Confluent-only: only a Confluent source accepts the unsupported fields
    # (schema metadata.tags, config compatibilityGroup) on registration, so a
    # Redpanda source cannot seed them. The destination models neither, so the
    # sync surfaces them and applies the configured policy.

    def _schema_tags(self, i: int) -> dict:
        # A metadata.tags block: Redpanda models only metadata.properties, so
        # this surfaces "/metadata/tags" as an unsupported schema feature.
        return {"tags": {f"{NS}.Leaf{i}": ["PII"]}}

    def _set_source_config(
        self, client: SchemaRegistryRedpandaClient, subject: str, body: dict
    ) -> None:
        resp = client.set_config_subject(subject, json.dumps(body))
        assert resp.status_code == 200, f"set config {subject} failed: {resp.text}"

    def _wait_totals(self, predicate: Any, err_msg: str, timeout_sec: int = 90):
        # Waits until the cumulative counters satisfy `predicate`, then returns
        # the status for logging and further assertions.
        def ready() -> bool:
            return predicate(self._admin_sr_status().totals_since_task_start)

        wait_until(ready, timeout_sec=timeout_sec, backoff_sec=1, err_msg=err_msg)
        return self._admin_sr_status()

    def _test_schema_registry_api_sync_unsupported_schema_remove(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)
        # A clean subject and one carrying an unsupported metadata.tags block.
        self._register(src, "clean-value", self._leaf_schema(1))
        self._register(
            src, "tagged-value", self._leaf_schema(2), metadata=self._schema_tags(2)
        )
        self._create_sr_link(
            feature_policy=shadow_link_pb2.UNSUPPORTED_SCHEMA_FEATURE_POLICY_REMOVE
        )
        # Both import (the tagged one as its supported projection); the removed
        # feature is counted and no error is raised.
        self._wait_synced(src, dest, [("clean-value", 1), ("tagged-value", 1)])
        sr = self._wait_totals(
            lambda t: t.unsupported_features_removed >= 1 and t.errors == 0,
            "REMOVE did not count an unsupported schema feature",
        )
        self._log_counters("admin API", sr)

    def _test_schema_registry_api_sync_unsupported_schema_fail(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)
        self._register(src, "clean-value", self._leaf_schema(1))
        self._register(
            src, "tagged-value", self._leaf_schema(2), metadata=self._schema_tags(2)
        )
        self._create_sr_link(
            feature_policy=shadow_link_pb2.UNSUPPORTED_SCHEMA_FEATURE_POLICY_FAIL
        )
        # The clean subject syncs; the tagged one is a per-item error, skipped.
        self._wait_synced(src, dest, [("clean-value", 1)])
        sr = self._wait_totals(
            lambda t: t.errors >= 1 and t.unsupported_features_removed == 0,
            "FAIL did not count the unsupported schema as an error",
        )
        self._log_counters("admin API", sr)
        assert self._schema_view(dest, "tagged-value", 1) is None, (
            "FAIL must not import a schema carrying unsupported features"
        )

    def _test_schema_registry_api_sync_unsupported_config_remove(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)
        self._register(src, "cfg-value", self._leaf_schema(1))
        # Subject config with an unsupported governance field
        # (compatibilityGroup) alongside the supported compatibilityLevel.
        self._set_source_config(
            src,
            "cfg-value",
            {"compatibility": "FULL", "compatibilityGroup": "app.major.version"},
        )
        # A governance-only subject config (no compatibility level set at all):
        # the sync must treat it as "no override" plus policy input rather than
        # fail the config read, so the whole run stays error-free.
        self._register(src, "gov-value", self._leaf_schema(2))
        self._set_source_config(
            src, "gov-value", {"compatibilityGroup": "app.major.version"}
        )
        self._create_sr_link(
            feature_policy=shadow_link_pb2.UNSUPPORTED_SCHEMA_FEATURE_POLICY_REMOVE
        )
        self._wait_synced(src, dest, [("cfg-value", 1), ("gov-value", 1)])
        # Each full sync re-reads both configs and re-counts their dropped
        # field (+2 per sync), even once the writes are no-ops; >= 3 therefore
        # proves a second sync counted, pinning the per-sync semantics.
        sr = self._wait_totals(
            lambda t: t.unsupported_features_removed >= 3 and t.errors == 0,
            "REMOVE did not keep counting unsupported config features",
        )
        self._log_counters("admin API", sr)
        # The supported compatibilityLevel is still synced to the destination.
        resp = dest.get_config_subject("cfg-value")
        assert (
            resp.status_code == 200 and resp.json()["compatibilityLevel"] == "FULL"
        ), f"REMOVE must still sync the compat level: {resp.status_code} {resp.text}"

    def _test_schema_registry_api_sync_unsupported_config_fail(self):
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)
        self._register(src, "cfg-value", self._leaf_schema(1))
        self._set_source_config(
            src,
            "cfg-value",
            {"compatibility": "FULL", "compatibilityGroup": "app.major.version"},
        )
        self._create_sr_link(
            feature_policy=shadow_link_pb2.UNSUPPORTED_SCHEMA_FEATURE_POLICY_FAIL
        )
        # The clean schema still imports; the config carrying the unsupported
        # field is a per-item error and its write is skipped.
        self._wait_synced(src, dest, [("cfg-value", 1)])
        sr = self._wait_totals(
            lambda t: t.errors >= 1 and t.unsupported_features_removed == 0,
            "FAIL did not count the unsupported config as an error",
        )
        self._log_counters("admin API", sr)
        # The subject config write is skipped: no FULL override lands.
        resp = dest.get_config_subject("cfg-value")
        assert (
            resp.status_code != 200 or resp.json().get("compatibilityLevel") != "FULL"
        ), f"FAIL must not sync the unsupported config: {resp.status_code} {resp.text}"


class SchemaRegistrySyncE2ETest(ShadowLinkTestBase, SchemaRegistrySyncMixin):
    """Redpanda source: a secondary Redpanda cluster provides the source Schema
    Registry (co-located on its brokers).

    Node budget: 3 (destination Redpanda) + 3 (source Redpanda) = 6."""

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        source_sr_config = SchemaRegistryConfig()
        source_sr_config.mode_mutability = True
        super().__init__(
            test_context,
            # Source (secondary) cluster runs a Schema Registry too.
            secondary_cluster_args=SecondaryClusterArgs(
                schema_registry_config=source_sr_config
            ),
            # Destination (primary) cluster Schema Registry.
            schema_registry_config=SchemaRegistryConfig(),
            # The sync task runs on whichever destination broker leads
            # _schemas/0, and totals_since_task_start (admin counters and the
            # prometheus series) resets whenever leadership moves. With the
            # destination fully synced a fresh instance imports nothing, so a
            # balancer-initiated move would strand every exact-value counter
            # wait/assert at 0. Leadership movement is exercised explicitly by
            # test_schema_registry_api_sync_survives_leadership_change.
            extra_rp_conf={"enable_leader_balancer": False},
            *args,
            **kwargs,
        )

    def _make_source_client(self) -> SchemaRegistryRedpandaClient:
        return SchemaRegistryRedpandaClient(self.source_cluster_service)

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_e2e(self):
        self._test_schema_registry_api_sync_e2e()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_soft_delete(self):
        self._test_schema_registry_api_sync_soft_delete()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_compatibility(self):
        self._test_schema_registry_api_sync_compatibility()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_hard_delete(self):
        self._test_schema_registry_api_sync_hard_delete()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_context_remap(self):
        self._test_schema_registry_api_sync_context_remap()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_context_delete(self):
        self._test_schema_registry_api_sync_context_delete()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_out_of_scope_reference(self):
        self._test_schema_registry_api_sync_out_of_scope_reference()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_recovers_after_source_unavailable(self):
        self._test_schema_registry_api_sync_recovers_after_source_unavailable()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_memory_backpressure(self):
        self._test_schema_registry_api_sync_memory_backpressure()

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_survives_leadership_change(self):
        self._test_schema_registry_api_sync_survives_leadership_change()

    # Redpanda-source only: the teardown behavior under test is
    # vendor-agnostic, so one leaf suffices.
    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_teardown_mid_sync(self):
        self._test_schema_registry_api_sync_teardown_mid_sync()


class ConfluentSchemaRegistrySyncE2ETest(ShadowLinkTestBase, SchemaRegistrySyncMixin):
    """Confluent Platform Schema Registry (backed by an Apache Kafka source
    cluster) as the shadow-link source; the destination stays Redpanda.
    See CORE-16776 / CORE-16357.

    Node budget: 3 (destination Redpanda) + 1 (Apache Kafka source) + 1
    (Confluent SR) = 5. The Confluent SR is a separate service that needs its
    own node (a Redpanda source's SR is co-located on its brokers), so the Kafka
    source is held to a single broker -- see the SecondaryClusterArgs below.

    Runs the full suite. Against a Confluent source, global compat/mode
    replication has a known cross-vendor gap -- see EXPECTED_ZERO_COUNTERS."""

    # Full-sync compat/mode replication reads the registry-wide global config
    # via Redpanda's ":.__GLOBAL:" pseudo-subject. A Confluent source 404s it
    # (it exposes the global config at GET /config, with no subject), so the
    # sync reads "no override" and deletes the destination's global config,
    # landing compatibility_configs_changed at 1 instead of 0. Known cross-vendor
    # gap in global compat replication, orthogonal to what this suite exercises,
    # so drop that one counter from the zero-check here. (modes_changed stays 0
    # on the pinned Confluent version.)
    EXPECTED_ZERO_COUNTERS = ("modes_changed", "unsupported_features_removed")

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(
            test_context,
            # Destination (primary) cluster Schema Registry; the source SR is
            # the separate Confluent service started in setUp.
            schema_registry_config=SchemaRegistryConfig(),
            # Single-broker Kafka source: the Confluent SR takes a node of its
            # own, so a one-broker source keeps the topology at 5 nodes (3 dest
            # + 1 kafka + 1 SR). A one-broker source is fine here -- the SR's
            # _schemas topic is RF=1.
            secondary_cluster_args=SecondaryClusterArgs(num_brokers=1),
            # Keep _schemas/0 leadership stable for the exact-value counter
            # asserts; same rationale as SchemaRegistrySyncE2ETest.
            extra_rp_conf={"enable_leader_balancer": False},
            *args,
            **kwargs,
        )
        self._confluent_sr: ConfluentSchemaRegistryService | None = None

    def get_source_cluster_spec(self) -> SecondaryClusterSpec:
        # Source cluster is Apache Kafka (KRaft); the Confluent SR is attached to
        # it in setUp. Mirrors the Kafka-source topology used by
        # cluster_linking_e2e_test.
        return SecondaryClusterSpec(
            ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
        )

    def setUp(self):
        # Starts the Apache Kafka source + Redpanda destination clusters.
        super().setUp()
        # Attach a Confluent Schema Registry to the Kafka source and start it.
        self._confluent_sr = ConfluentSchemaRegistryService(
            self.test_context, bootstrap_provider=self.source_cluster_service
        )
        self._confluent_sr.start()

    def _confluent(self) -> ConfluentSchemaRegistryService:
        assert self._confluent_sr is not None, "Confluent SR not started"
        return self._confluent_sr

    def _make_source_client(self) -> SchemaRegistryRedpandaClient:
        sr = self._confluent()
        # The base client hardcodes port 8081 and only needs
        # .nodes[*].account.hostname + .logger, so the ducktape service
        # duck-types cleanly; the Confluent SR must use the default port.
        assert sr.port == 8081, "Confluent SR must listen on 8081 for the SR client"
        return SchemaRegistryRedpandaClient(sr)  # type: ignore[arg-type]

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_e2e(self):
        self._test_schema_registry_api_sync_e2e()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_soft_delete(self):
        self._test_schema_registry_api_sync_soft_delete()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_out_of_scope_reference(self):
        self._test_schema_registry_api_sync_out_of_scope_reference()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_recovers_after_source_unavailable(self):
        self._test_schema_registry_api_sync_recovers_after_source_unavailable()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_memory_backpressure(self):
        self._test_schema_registry_api_sync_memory_backpressure()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_survives_leadership_change(self):
        self._test_schema_registry_api_sync_survives_leadership_change()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_compatibility(self):
        self._test_schema_registry_api_sync_compatibility()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_hard_delete(self):
        self._test_schema_registry_api_sync_hard_delete()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_context_remap(self):
        self._test_schema_registry_api_sync_context_remap()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_context_delete(self):
        self._test_schema_registry_api_sync_context_delete()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_unsupported_schema_remove(self):
        self._test_schema_registry_api_sync_unsupported_schema_remove()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_unsupported_schema_fail(self):
        self._test_schema_registry_api_sync_unsupported_schema_fail()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_unsupported_config_remove(self):
        self._test_schema_registry_api_sync_unsupported_config_remove()

    @cluster(num_nodes=5)
    def test_schema_registry_api_sync_unsupported_config_fail(self):
        self._test_schema_registry_api_sync_unsupported_config_fail()


class _CredentialedSRClient(SchemaRegistryRedpandaClient):
    """A SchemaRegistryRedpandaClient that bakes the source-SR credentials into
    every request, so the sync mixin's seeding/verification helpers reach an
    authenticated source over TLS without threading kwargs through each call.

    SchemaRegistryRedpandaClient.request() already forwards tls_enabled/auth/
    cert/verify to `requests`; this subclass only supplies them as per-request
    defaults (a caller can still override any of them per call)."""

    def __init__(
        self,
        redpanda: RedpandaService,
        *,
        tls: bool = False,
        ca: str | None = None,
        cert: tuple[str, str] | None = None,
        auth: tuple[str, str] | None = None,
    ):
        super().__init__(redpanda)
        self._defaults: dict[str, Any] = {}
        if tls:
            self._defaults["tls_enabled"] = True
        if ca is not None:
            self._defaults["verify"] = ca  # CA bundle path, for https verify
        if cert is not None:
            self._defaults["cert"] = cert  # (crt, key) paths, for mTLS
        if auth is not None:
            self._defaults["auth"] = auth  # (username, password), for HTTP Basic

    def request(self, verb: str, path: str, **kwargs: Any):  # type: ignore[override]
        for k, v in self._defaults.items():
            kwargs.setdefault(k, v)
        return super().request(verb, path, **kwargs)

    def base_uri(self) -> str:
        scheme = "https" if self._defaults.get("tls_enabled") else "http"
        return f"{scheme}://{self.redpanda.nodes[0].account.hostname}:8081"


class SchemaRegistrySyncAuthMixin(SchemaRegistrySyncMixin):
    """Source-SR authentication / TLS coverage: exercises the real HTTP source
    reader auth path -- HTTP Basic, https (server TLS), and mTLS -- against a
    Redpanda source Schema Registry. (No-auth http is the control, covered by
    SchemaRegistrySyncE2ETest.)

    A plain mixin (like SchemaRegistrySyncMixin) providing the source-security
    wiring and shared test bodies; each concrete leaf below inherits
    (ShadowLinkTestBase, this) and sets USE_TLS / REQUIRE_CLIENT_AUTH /
    USE_BASIC. A small DAG keeps each mode fast -- the reconciler ordering is
    covered at scale by SchemaRegistrySyncE2ETest; the point here is transport.

    Topology: 3 (destination Redpanda) + 1 (source Redpanda) = 4 nodes. A
    single-broker source is fine -- the SR `_schemas` topic is RF=1, as the
    Confluent leaf already relies on."""

    # Small reference DAG (top -> mid -> leaf); just enough to prove references
    # replicate over the authenticated connection. Overrides the E2E scale.
    LEAVES = 4
    MIDS = 2
    TOPS = 1

    # Selected by the concrete leaf to pick the source-SR security posture.
    USE_TLS = False
    REQUIRE_CLIENT_AUTH = False  # mTLS: source SR demands a client cert
    USE_BASIC = False  # HTTP Basic auth (delegated to source-cluster SASL)

    def _auth_secondary_args(self, test_context: TestContext) -> SecondaryClusterArgs:
        # Build the source (secondary) cluster args from the leaf's flags. Called
        # from the leaf __init__ before super().__init__, so the source cluster
        # is constructed with the right SR security posture. Also seeds the
        # per-test credential state consumed lazily on first use.
        self.test_context = test_context
        # TLSCertManager owns the source CA; created here (like the topic-syncing
        # TLS tests) because the source cluster's tls_provider must be wired
        # before the cluster is constructed.
        self.tls: TLSCertManager | None = None
        self._client_cert: Certificate | None = None
        self._link_auth: tuple[str, str] | None = None
        self._link_tls: tls_pb2.TLSSettings | None = None  # source SR connection
        self._kafka_tls: tls_pb2.TLSSettings | None = None  # source Kafka connection
        self._creds_loaded = False

        source_security = SecurityConfig()
        source_sr_config = SchemaRegistryConfig()
        source_sr_config.mode_mutability = True

        if self.USE_TLS:
            self.tls = TLSCertManager(self.logger)
            source_security.tls_provider = ClusterLinkingTLSProvider(self.tls)
            # Setting a tls_provider enables TLS on ALL source listeners, so the
            # link's Kafka connection to the source uses TLS too. Keep that Kafka
            # listener server-TLS-only; mTLS (require_client_auth) is scoped to
            # the SR listener alone, so the negative test exercises SR client-cert
            # enforcement rather than the Kafka handshake. SR server cert/
            # truststore are staged automatically (RedpandaService.write_tls_certs).
            source_security.require_client_auth = False
            source_sr_config.require_client_auth = self.REQUIRE_CLIENT_AUTH

        if self.USE_BASIC:
            source_security.enable_sasl = True
            source_security.endpoint_authn_method = "sasl"
            source_sr_config.authn_method = "http_basic"

        return SecondaryClusterArgs(
            num_brokers=1,
            schema_registry_config=source_sr_config,
            security=source_security,
        )

    def _ca_pem(self) -> str:
        assert self.tls is not None
        with open(self.tls.ca.crt, encoding="utf-8") as f:
            return f.read()

    def _ensure_source_credentials(self) -> None:
        # Materialize the source CA/client cert and Basic creds once the source
        # cluster is up. Lazy so leaves need no setUp override.
        if self._creds_loaded:
            return
        if self.tls is not None:
            ca = self._ca_pem()
            # Kafka connection: server TLS only (CA to validate the source).
            self._kafka_tls = tls_pb2.TLSSettings(
                enabled=True, tls_pem_settings=tls_pb2.TLSPEMSettings(ca=ca)
            )
            # SR connection: same CA, plus a client cert/key for mTLS.
            sr_pem = tls_pb2.TLSPEMSettings(ca=ca)
            if self.REQUIRE_CLIENT_AUTH:
                # A client cert signed by the source CA, presented by the reader
                # (and the seeding client) for mutual TLS to the SR listener.
                self._client_cert = self.tls.create_cert(
                    socket.gethostname(),
                    name="sr-source-reader-client",
                    common_name="sr-source-reader-client",
                )
                with open(self._client_cert.crt, encoding="utf-8") as f:
                    sr_pem.cert = f.read()
                with open(self._client_cert.key, encoding="utf-8") as f:
                    sr_pem.key = f.read()
            self._link_tls = tls_pb2.TLSSettings(enabled=True, tls_pem_settings=sr_pem)
        if self.USE_BASIC:
            # HTTP Basic against the source SR authenticates as a source-cluster
            # SASL principal; the bootstrapped superuser exists and can write.
            creds = self.source_cluster_service.SUPERUSER_CREDENTIALS
            self._link_auth = (creds.username, creds.password)
        self._creds_loaded = True

    # --- hooks consumed by SchemaRegistrySyncMixin --------------------------

    def _make_source_client(self) -> SchemaRegistryRedpandaClient:
        self._ensure_source_credentials()
        cert = None
        if self._client_cert is not None:
            cert = (self._client_cert.crt, self._client_cert.key)
        ca = self.tls.ca.crt if (self._link_tls is not None and self.tls) else None
        return _CredentialedSRClient(
            self.source_cluster_service,
            tls=self._link_tls is not None,
            ca=ca,
            cert=cert,
            auth=self._link_auth,
        )

    def _maybe_apply_source_sr_credentials(
        self,
        api: "shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryApi",
    ) -> None:
        if self._link_auth is not None:
            api.auth_options.basic.username = self._link_auth[0]
            api.auth_options.basic.password = self._link_auth[1]
        if self._link_tls is not None:
            api.tls_settings.CopyFrom(self._link_tls)

    def _maybe_apply_source_kafka_credentials(
        self, client_options: "shadow_link_pb2.ShadowLinkClientOptions"
    ) -> None:
        # Enabling TLS/SASL on the source also affects the link's Kafka connection
        # to the source (used by the create preflight); without matching Kafka
        # credentials the preflight fails with broker_not_available. Supply them
        # here so the preflight reaches the SR check. These are the Kafka-level
        # credentials, kept independent of the source-SR credentials
        # (_link_auth/_link_tls): the negative test sabotages the SR credentials
        # to prove the *SR* layer rejects the reader, so the Kafka connection must
        # stay valid.
        if self._kafka_tls is not None:
            client_options.tls_settings.CopyFrom(self._kafka_tls)
        if self.USE_BASIC:
            creds = self.source_cluster_service.SUPERUSER_CREDENTIALS
            client_options.authentication_configuration.scram_configuration.CopyFrom(
                shadow_link_pb2.ScramConfig(
                    username=creds.username,
                    password=creds.password,
                    scram_mechanism=shadow_link_pb2.SCRAM_MECHANISM_SCRAM_SHA_256,
                )
            )

    # --- shared test bodies -------------------------------------------------

    def _run_auth_sync_smoke(self) -> None:
        # Seed a small reference DAG on the authenticated source SR, create the
        # link with matching credentials, and confirm the DAG replicates to the
        # destination with no reported errors -- i.e. the real source-reader
        # auth/TLS path works end to end.
        src = self._make_source_client()
        dest = SchemaRegistryRedpandaClient(self.target_cluster_service)

        initial = self._seed_initial_dag(src)
        source_sr_url = self._create_sr_link()
        self.logger.info(f"source SR: {source_sr_url}; seeded {len(initial)} subjects")

        self._wait_synced(src, dest, initial)

        # Assert the most recently completed full sync was error-free. Using the
        # per-sync counter rather than the monotonic lifetime total tolerates a
        # transient error during startup (e.g. the reader racing a not-yet-ready
        # source SR) while still catching errors in the sync that did the work.
        def clean_full_sync() -> bool:
            sr = self._admin_sr_status()
            return sr.HasField("last_full_sync") and sr.last_full_sync.errors == 0

        wait_until(
            clean_full_sync,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="no error-free full sync completed after the DAG replicated",
        )
        self._log_counters("admin API", self._admin_sr_status())

    def _run_auth_sync_rejects_bad_credentials(self) -> None:
        # Negative control proving the source SR actually enforces credentials
        # (and that the matching positive test is not a false positive): write
        # one schema with valid credentials (which also confirms the source SR is
        # up and the good-cred client is accepted), then attempt to create the
        # link with the mode's credential sabotaged. The link-creation preflight
        # queries the source SR and must reject -- Basic: a wrong password -> 401;
        # mTLS: no client cert -> the server's require_client_auth fails the TLS
        # handshake -- so create_shadow_link raises FAILED_PRECONDITION and no
        # link is created.
        src = self._make_source_client()  # valid creds, baked at construction
        self._add_leaf(src, 0)  # asserts a 200 on register, i.e. good creds work

        if self.USE_BASIC:
            assert self._link_auth is not None
            self._link_auth = (self._link_auth[0], "definitely-not-the-password")
        elif self.REQUIRE_CLIENT_AUTH:
            # Keep only the CA: the reader can validate the server but has no
            # client cert to satisfy the source's require_client_auth.
            self._link_tls = tls_pb2.TLSSettings(
                enabled=True, tls_pem_settings=tls_pb2.TLSPEMSettings(ca=self._ca_pem())
            )
        else:
            raise AssertionError("no credential to sabotage for this mode")

        def rejected_at_sr(e: ConnectError) -> bool:
            # The create preflight probes both the source Kafka and the source
            # SR, and a Kafka-level failure ALSO surfaces as FAILED_PRECONDITION
            # (broker_not_available). Require that the rejection is the SR check,
            # not the Kafka one -- otherwise a regression in the link's Kafka
            # credentials would make this negative pass for the wrong reason.
            return (
                e.code == ConnectErrorCode.FAILED_PRECONDITION
                and "broker_not_available" not in str(e).lower()
                and "schema registry" in str(e).lower()
            )

        with expect_exception(ConnectError, rejected_at_sr):
            self._create_sr_link()


class SchemaRegistrySyncBasicAuthTest(ShadowLinkTestBase, SchemaRegistrySyncAuthMixin):
    """HTTP Basic auth over plaintext http to the source Schema Registry."""

    USE_BASIC = True

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(
            test_context,
            secondary_cluster_args=self._auth_secondary_args(test_context),
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )

    @cluster(num_nodes=4)
    def test_basic_auth_sync(self):
        self._run_auth_sync_smoke()

    @cluster(num_nodes=4)
    def test_basic_auth_rejects_bad_credentials(self):
        self._run_auth_sync_rejects_bad_credentials()


class SchemaRegistrySyncTlsTest(ShadowLinkTestBase, SchemaRegistrySyncAuthMixin):
    """https (server-side TLS, no client cert, no auth) to the source SR."""

    USE_TLS = True

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(
            test_context,
            secondary_cluster_args=self._auth_secondary_args(test_context),
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )

    @cluster(num_nodes=4)
    def test_tls_sync(self):
        self._run_auth_sync_smoke()


class SchemaRegistrySyncMtlsTest(ShadowLinkTestBase, SchemaRegistrySyncAuthMixin):
    """https with mutual TLS: the source SR requires a client certificate."""

    USE_TLS = True
    REQUIRE_CLIENT_AUTH = True

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(
            test_context,
            secondary_cluster_args=self._auth_secondary_args(test_context),
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )

    @cluster(num_nodes=4)
    def test_mtls_sync(self):
        self._run_auth_sync_smoke()

    @cluster(num_nodes=4)
    def test_mtls_rejects_missing_client_cert(self):
        self._run_auth_sync_rejects_bad_credentials()


class SchemaRegistrySyncTlsBasicAuthTest(
    ShadowLinkTestBase, SchemaRegistrySyncAuthMixin
):
    """https server TLS plus HTTP Basic auth -- the common Confluent Cloud
    posture (an API key/secret sent over TLS)."""

    USE_TLS = True
    USE_BASIC = True

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(
            test_context,
            secondary_cluster_args=self._auth_secondary_args(test_context),
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )

    @cluster(num_nodes=4)
    def test_tls_basic_auth_sync(self):
        self._run_auth_sync_smoke()
