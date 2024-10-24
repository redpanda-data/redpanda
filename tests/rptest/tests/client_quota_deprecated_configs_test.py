# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.util import wait_until_result
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService

NON_DEFAULT_QUOTA_CONFIGS = {"target_fetch_quota_byte_rate": 10240}


def _has_config_nag(redpanda: RedpandaService):
    return redpanda.search_log_any(
        "You have configured client quotas using cluster configs.*")


class ClientQuotaDeprecatedConfigs_ConfigUpdateTest(RedpandaTest):
    @cluster(num_nodes=3)
    def test_config_update(self):
        assert not _has_config_nag(self.redpanda), \
            f"We should not see the nag with the default configs"

        self.redpanda.set_cluster_config(NON_DEFAULT_QUOTA_CONFIGS)

        wait_until_result(
            lambda: _has_config_nag(self.redpanda),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timeout waiting for config nag to show up in the logs")


class ClientQuotaDeprecatedConfigs_StartupTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(extra_rp_conf=NON_DEFAULT_QUOTA_CONFIGS,
                         *args,
                         **kwargs)

    @cluster(num_nodes=3)
    def test_startup(self):
        wait_until_result(
            lambda: _has_config_nag(self.redpanda),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timeout waiting for config nag to show up in the logs")
