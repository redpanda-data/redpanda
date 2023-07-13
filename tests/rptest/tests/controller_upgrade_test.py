# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient
from rptest.services.admin import Admin
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer, RedpandaAdminOperation
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, make_redpanda_service
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.tests.end_to_end import EndToEndTest
from rptest.utils.mode_checks import skip_debug_mode

# TODO: fix https://github.com/redpanda-data/redpanda/issues/5629
ALLOWED_LOGS = [
    # e.g. cluster - controller_backend.cc:466 - exception while executing partition operation: {type: update_finished, ntp: {kafka/test-topic-1944-1639161306808363/1}, offset: 413, new_assignment: { id: 1, group_id: 65, replicas: {{node_id: 3, shard: 2}, {node_id: 4, shard: 2}, {node_id: 1, shard: 0}} }, previous_assignment: {nullopt}} - std::__1::__fs::filesystem::filesystem_error (error system:39, filesystem error: remove failed: Directory not empty [/var/lib/redpanda/data/kafka/test-topic-1944-1639161306808363])
    re.compile("cluster - .*Directory not empty"),

    # < 22.2 versions may log bare std::exception error
    # (https://github.com/redpanda-data/redpanda/issues/5886)
    re.compile("(kafka|rpc) - .*std::exception"),

    #  <= 22.2 versions may log bare seastar::condition_variable_timed_out error
    re.compile(
        "(kafka|rpc) - Service handler threw an exception: seastar::condition_variable_timed_out"
    ),

    # < 22.2 versions may log a "cannot find consensus group" error message
    # (https://github.com/redpanda-data/redpanda/pull/5742)
    re.compile("r/heartbeat - .*cannot find consensus group"),
]


class ControllerUpgradeTest(EndToEndTest):
    @skip_debug_mode
    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST + ALLOWED_LOGS)
    def test_updating_cluster_when_executing_operations(self):
        '''
        Validates that cluster is operational when upgrading controller log
        '''

        self.redpanda = make_redpanda_service(self.test_context, 5)
        installer = self.redpanda._installer
        prev_version = installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)

        # for upgrades from v22.2.x to v22.3.x we disable setting topic
        # configuration properties as during the upgrade phase setting
        # topic properties is explicitly forbidden

        if prev_version[0] == 22 and prev_version[1] == 2:
            admin_operations = [
                o for o in RedpandaAdminOperation
                if o != RedpandaAdminOperation.UPDATE_TOPIC
            ]
        else:
            admin_operations = [o for o in RedpandaAdminOperation]

        # DeleteRecords is not an allowable admin operation until the cluster
        # has been fully upgraded
        admin_operations = [
            o for o in admin_operations
            if o != RedpandaAdminOperation.DELETE_RECORDS
        ]

        installer.install(self.redpanda.nodes, prev_version)

        self.redpanda.start()
        admin_fuzz = AdminOperationsFuzzer(self.redpanda,
                                           allowed_operations=admin_operations,
                                           min_replication=3)
        self._client = DefaultClient(self.redpanda)

        spec = TopicSpec(partition_count=6, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(1, throughput=5000)
        self.start_consumer(1)
        self.await_startup()
        admin_fuzz.start()

        def cluster_is_stable():
            admin = Admin(self.redpanda)
            brokers = admin.get_brokers()
            if len(brokers) < 3:
                return False

            for b in brokers:
                self.logger.debug(f"broker:  {b}")
                if not (b['is_alive'] and 'disk_space' in b):
                    return False

            return True

        # Get to a stable starting point before asserting anything about our
        # workload.
        wait_until(cluster_is_stable, 90, backoff_sec=2)

        admin_fuzz.wait(5, 240)

        # reset to use latest Redpanda version
        installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)

        for n in self.redpanda.nodes:
            # NOTE: we're not doing a rolling restart to avoid interfering with
            # the admin operations.
            self.redpanda.restart_nodes([n], stop_timeout=60, start_timeout=90)
            num_executed_before_restart = admin_fuzz.executed

            # wait for leader balancer to start evening out leadership
            wait_until(cluster_is_stable, 90, backoff_sec=2)
            admin_fuzz.wait(num_executed_before_restart + 2, 240)

        self.run_validation(min_records=100000,
                            producer_timeout_sec=300,
                            consumer_timeout_sec=180)
        num_executed_after_upgrade = admin_fuzz.executed
        admin_fuzz.wait(num_executed_after_upgrade + 5, 240)
        admin_fuzz.stop()
