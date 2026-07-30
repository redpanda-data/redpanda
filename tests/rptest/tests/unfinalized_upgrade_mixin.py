# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from connectrpc.errors import ConnectError, ConnectErrorCode
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkException, RpkTool
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.util import wait_until_result


class UnfinalizedUpgradeMixin:
    """Reusable driver for the manual-finalization (unfinalized upgrade) flow:
    roll a cluster's binaries forward with `features_auto_finalization=false`
    so the active (downgrade-floor) version is held back, observe/finalize via
    `rpk cluster upgrade` status/finalize (the operator surface over the admin
    v2 FeaturesService RPCs), and roll back before finalizing.

    These primitives were factored out of ManualFinalizationUpgradeTest so a
    second test can drive the same flow against a different cluster topology
    (e.g. the target cluster of a shadow link, with a fixed source alongside).

    The concrete test must provide, before any of these are called:
      - ``self.redpanda``   -- the RedpandaService being upgraded/downgraded,
      - ``self.installer``  -- its RedpandaInstaller (usually
                               ``self.redpanda._installer``),
      - ``self.admin``      -- a v1 Admin against that cluster,
      - ``self.old_logical``-- the logical version of the pre-upgrade binary
                               (set by the test's own "start at old" step).
    ``_restart_at_new`` sets ``self.new_logical``.

    rpk runs from the test runner's HEAD install (see RpkTool._rpk_binary), not
    from the cluster nodes, so the `cluster upgrade` subcommands are available
    regardless of which release the brokers currently run. They are still only
    invoked once every broker is on HEAD, because the old releases lack the
    server-side admin v2 FeaturesService.
    """

    # The pre-upgrade binary must be a released patch that already carries the
    # backported `features_auto_finalization` knob, so auto-finalization can be
    # disabled *before* upgrading to a HEAD build that ships the gating logic.
    # Backported to v26.1.9.
    MIN_OLD_RELEASE = (26, 1, 9)

    def _restart_at_new(self, nodes):
        """Upgrade `nodes` to the HEAD build and restart them in place."""
        self.installer.install(nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(nodes)
        self._wait_for_cluster_settled()

        self.new_logical = self._node_latest_logical_version(nodes[0])
        self.logger.info(f"HEAD build reports logical version {self.new_logical}")
        assert self.new_logical > self.old_logical, (
            f"expected HEAD logical version {self.new_logical} to exceed the old "
            f"version {self.old_logical}; the upgrade did not raise the version"
        )

    def _node_latest_logical_version(self, node):
        """Read a (possibly just-restarted) node's latest logical version,
        tolerating the brief window before it is serving the admin API."""

        def query():
            return self.admin.get_features(node=node).get("node_latest_version")

        return wait_until_result(
            query,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="node did not report node_latest_version after upgrade",
        )

    def _upgrade_all_to(self, version):
        """Install `version` on every node, restart in place, and return the
        binary's latest logical version."""
        self.installer.install(self.redpanda.nodes, version)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._wait_for_cluster_settled()
        return self._node_latest_logical_version(self.redpanda.nodes[0])

    def _wait_for_cluster_settled(self, timeout_sec=90):
        """Wait for the cluster to settle after a restart: every broker has
        rejoined and no under-replicated partitions remain. `start_node` only
        waits for per-node readiness (the v1 ready probe), not cluster
        convergence -- so without this a "did not advance" assertion could pass
        merely because the controller has not yet observed the upgrade."""
        self.redpanda.wait_for_membership(first_start=False)
        wait_until(
            self.redpanda.healthy,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg="cluster did not become healthy after restart",
        )

    def _downgrade_all_to(self, release):
        """Roll every node back to `release` (an older binary) without
        finalizing, then wait for the cluster to come back healthy. This is the
        rollback the unfinalized-upgrade feature is meant to preserve: because
        the active version was never advanced, the older binary can still run on
        the existing on-disk data."""
        self.installer.install(self.redpanda.nodes, release)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._wait_for_cluster_settled()

    def _wait_for_version_everywhere(self, target_version, timeout_sec=20):
        """Wait until every node reports cluster_version == target_version.
        Version propagation lags feature-flag writes because it rides periodic
        health messages, so this tolerates a short delay."""

        def check():
            return all(
                self.admin.get_features(node=node)["cluster_version"] == target_version
                for node in self.redpanda.nodes
            )

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=1)

    def _disable_auto_finalization(self):
        self.redpanda.set_cluster_config({"features_auto_finalization": False})

    def _call_with_leader_retry(self, call, timeout_sec=30):
        """Retry a controller-leader-routed admin call through the transient
        UNAVAILABLE window after restarts/leadership changes. Handles both
        transports the tests use: direct admin v2 connect calls (typed error
        code) and rpk invocations, where the connect code appears in rpk's
        stderr (e.g. "unable to retrieve upgrade status: unavailable: ...").
        Only UNAVAILABLE is retried; other errors (e.g. FAILED_PRECONDITION)
        propagate immediately."""
        deadline = time.time() + timeout_sec
        while True:
            try:
                return call()
            except ConnectError as e:
                if e.code != ConnectErrorCode.UNAVAILABLE or time.time() >= deadline:
                    raise
            except RpkException as e:
                if "unavailable" not in e.stderr.lower() or time.time() >= deadline:
                    raise
            time.sleep(1)

    def _finalize(self):
        """Finalize the upgrade via `rpk cluster upgrade finalize`."""
        rpk = RpkTool(self.redpanda)
        return self._call_with_leader_retry(rpk.cluster_upgrade_finalize)

    def _get_upgrade_status(self):
        """Return the parsed `rpk cluster upgrade status` JSON response."""
        rpk = RpkTool(self.redpanda)
        return self._call_with_leader_retry(rpk.cluster_upgrade_status)

    def _wait_for_status_state(self, state, timeout_sec=30):
        """Wait until the upgrade status reports `state` (an
        RpkUpgradeFinalizationState string); return the status that
        reported it."""

        def check():
            status = self._get_upgrade_status()
            return status["state"] == state, status

        return wait_until_result(
            check,
            timeout_sec=timeout_sec,
            backoff_sec=1,
        )
