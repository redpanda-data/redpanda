# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from uuid import uuid4
import requests
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.services.admin import Admin, DebugBundleStartConfig, DebugBundleStartConfigParams
from rptest.services.cluster import cluster
from rptest.services.redpanda import LoggingConfig
from rptest.tests.redpanda_test import RedpandaTest

log_config = LoggingConfig('info',
                           logger_levels={
                               'admin_api_server': 'trace',
                               'debug-bundle-service': 'trace'
                           })


class DebugBundleTest(RedpandaTest):
    """
    Smoke test for debug bundle admin API
    """
    def __init__(self, context, num_brokers=1, **kwargs) -> None:
        super(DebugBundleTest, self).__init__(context,
                                              num_brokers=num_brokers,
                                              log_config=log_config,
                                              **kwargs)

    @cluster(num_nodes=1)
    @matrix(ignore_none=[True, False])
    def test_post_debug_bundle(self, ignore_none: bool):
        admin = Admin(self.redpanda)
        node = random.choice(self.redpanda.started_nodes())

        job_id = uuid4()
        res = admin.post_debug_bundle(DebugBundleStartConfig(
            job_id=job_id,
            config=DebugBundleStartConfigParams(cpu_profiler_wait_seconds=16,
                                                metrics_interval_seconds=16)),
                                      ignore_none=ignore_none,
                                      node=node)

        assert res.status_code == requests.codes.ok, res.json()

        # Start a second debug bundle with the same job_id, expect a conflict
        try:
            admin.post_debug_bundle(
                DebugBundleStartConfig(job_id=job_id,
                                       config=DebugBundleStartConfigParams(
                                           cpu_profiler_wait_seconds=16,
                                           metrics_interval_seconds=16)),
                ignore_none=ignore_none,
                node=node)
            assert False, f"Expected a conflict {res.content}"
        except requests.HTTPError as e:
            assert e.response.status_code == requests.codes.conflict, res.json(
            )

        # Get the debug bundle status, expect running
        res = admin.get_debug_bundle(node=node)
        assert res.status_code == requests.codes.ok, res.json()
        assert res.json()['status'] == 'running', res.json()
        assert res.json()['job_id'] == str(job_id), res.json()

        # Wait until the debug bundle has completed
        try:
            wait_until(lambda: admin.get_debug_bundle(node=node).json()[
                'status'] != 'running',
                       timeout_sec=60,
                       backoff_sec=1)
        except Exception as e:
            self.redpanda.logger.warning(
                f"response: {admin.get_debug_bundle(node=node).json()}")
            raise e

        # Get the debug bundle status, expect success
        res = admin.get_debug_bundle(node=node)
        assert res.status_code == requests.codes.ok, res.json()
        assert res.json()['status'] == 'success', res.json()
        assert res.json()['job_id'] == str(job_id), res.json()
        assert res.json()['filename'] == f"{job_id}.zip", res.json()

        # Delete the debug bundle after it has completed, expect a conflict
        try:
            admin.delete_debug_bundle(job_id=job_id, node=node)
            assert False, f"Expected a conflict {res.content}"
        except requests.HTTPError as e:
            assert e.response.status_code == requests.codes.conflict, res.json(
            )
