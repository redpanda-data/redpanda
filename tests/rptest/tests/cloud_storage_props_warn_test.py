# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from datetime import datetime
from enum import Enum
import json
import time

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.utils import LogSearchLocal
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.utils.util import wait_until


class WarningType(Enum):
    CREATE = 'created topic'
    UPDATE = 'topic properties update'
    SNAPSHOT = 'topic snapshot'


class CloudStoragePropsWarnTest(RedpandaTest):
    SNAPSHOT_MAX_AGE_S = 2

    def __init__(self, test_context):
        super().__init__(test_context,
                         num_brokers=3,
                         extra_rp_conf={
                             'controller_snapshot_max_age_sec':
                             self.SNAPSHOT_MAX_AGE_S
                         },
                         si_settings=SISettings(
                             test_context=test_context,
                             cloud_storage_enable_remote_read=False,
                             cloud_storage_enable_remote_write=False,
                             fast_uploads=True))
        self.admin = Admin(self.redpanda)
        self.log_searcher = LogSearchLocal(test_context, [],
                                           self.redpanda.logger,
                                           self.redpanda.STDOUT_STDERR_CAPTURE)
        self._rpk = RpkTool(self.redpanda)

    def scrape_warning(self, nodes: list[ClusterNode]) -> list[str]:
        matches: list[str] = []
        WARNING_LOG = f"'Cloud storage not fully enabled'"
        for n in nodes:
            matches += self.log_searcher._capture_log(n, WARNING_LOG)
        # basically sort by timestamp rather than by origin node
        return sorted(matches)

    # NOTE(oren): request should be synchronous with the properties check routine,
    # so asserting an exact count is not timing dependent
    def wait_for_logs(self,
                      expected: int,
                      offset: int,
                      node: ClusterNode,
                      warn_types: list[WarningType] | None = None) -> int:
        """
        Waits for the specified number of cloud topic properties warnings to appear
        in broker logs for the specified node.
          expected: the number of NEW warnings expected
          offset: relative to the specified offset in the log accumulator
          node: originating on a certain node
          warn_types: and (optionally) of specific type(s) of warning
            (create, update, snapshot)
        Returns:
          the total number of accumulated matching logs use to calculate an offset
            for the next call
        """
        assert warn_types is None or len(warn_types) == expected, \
            f"{warn_types=} does not match {expected=}"

        def _check():
            logs = self.scrape_warning([node])
            diff_n = len(logs) - offset
            new_warnings = logs[-diff_n:] if diff_n > 0 else []
            self.logger.warn(
                f"Warnings from {node.name=} {offset=} ({expected=}) got: {json.dumps(new_warnings, indent=1)}"
            )
            assert diff_n <= expected, \
                f"Too many new warnings! {expected=} got {json.dumps(new_warnings, indent=1)}"
            if diff_n == expected:
                assert warn_types is None or all(
                    typ.value in msg
                    for (msg, typ) in zip(new_warnings, warn_types))
                return True, logs
            else:
                return False, None

        return len(
            wait_until_result(lambda: _check(),
                              timeout_sec=10,
                              backoff_sec=1,
                              err_msg="Didn't get the right number"))

    def get_controller_leader(self) -> ClusterNode:
        id = self.admin.await_stable_leader('controller',
                                            partition=0,
                                            namespace="redpanda",
                                            timeout_s=30)
        n = self.redpanda.get_node(id)
        self.logger.warning(f"Controller leader is {n.name}")
        return n

    def force_snapshots(self):
        t = datetime.now().timestamp()
        for n in self.redpanda.nodes:
            self.redpanda.wait_for_controller_snapshot(n, prev_mtime=int(t))

    @cluster(num_nodes=3)
    @matrix(
        enabled=[
            True,
            False,
        ],
        rd=[
            True,
            False,
        ],
    )
    def test_create_topic(self, enabled: bool, rd: bool):
        topic_props = {
            'redpanda.remote.read': str(enabled).lower(),
            'redpanda.remote.write': str(enabled).lower(),
            'redpanda.remote.delete': str(rd).lower(),
        }

        self._rpk.create_topic('foo', partitions=1, config=topic_props)

        leader = self.get_controller_leader()

        n_logs = self.wait_for_logs(
            0 if enabled else 1,
            offset=0,
            node=leader,
            warn_types=None if enabled else [WarningType.CREATE],
        )

        self.force_snapshots()

        self.logger.debug(
            "Confirm that the warning is written out again after a restart, this time from a snapshot."
        )
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes)
        leader = self.get_controller_leader()

        n_logs = self.wait_for_logs(
            0 if enabled else 1,
            offset=n_logs,
            node=leader,
            warn_types=None if enabled else [WarningType.SNAPSHOT],
        )

        if enabled:
            self.logger.debug(
                "Full cloud storage already enabled, so we're done. Disable it and check for warning logs"
            )
            self._rpk.alter_topic_config('foo', 'redpanda.remote.write',
                                         'false')
            self._rpk.alter_topic_config('foo', 'redpanda.remote.read',
                                         'false')
            n_logs = self.wait_for_logs(
                2,
                n_logs,
                node=leader,
                warn_types=[WarningType.UPDATE, WarningType.UPDATE],
            )

        self.logger.debug(
            "Enabling remote.write still warns because remote.read is still OFF"
        )

        self._rpk.alter_topic_config('foo', 'redpanda.remote.write', 'true')
        n_logs = self.wait_for_logs(1,
                                    n_logs,
                                    node=leader,
                                    warn_types=[WarningType.UPDATE])

        self._rpk.alter_topic_config('foo', 'redpanda.remote.read', 'true')

        self.logger.debug(
            "Topic is fully CS enabled  now, the final alter-config producing no new warnings"
        )
        n_logs = self.wait_for_logs(0, offset=n_logs, node=leader)

        self.force_snapshots()

        self.logger.debug(
            "Restart the cluster again. Create & patch commands should be snapshotted away"
        )

        self.redpanda.rolling_restart_nodes(self.redpanda.nodes)
        leader = self.get_controller_leader()

        n_logs = self.wait_for_logs(0, offset=n_logs, node=leader)

    # TODO(oren): tests for default properties
    # TODO(oren): tests for switching off ts on an existing topic (though I guess that's covered above)
    # TODO(oren): maybe more snapshot fiddling?
