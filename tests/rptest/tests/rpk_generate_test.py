# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json

from rptest.services.cluster import cluster

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool


class RpkGenerateTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkGenerateTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=1)
    def test_generate_grafana(self):
        """
        Test that rpk generate grafana-dashboard will generate the required dashboard
        and that it's a proper JSON file.
        """

        # dashboard is the dictionary of the current dashboards and their title.
        dashboards = [
            "operations", "consumer-metrics", "consumer-offsets",
            "topic-metrics", "legacy"
        ]
        for name in dashboards:
            datasource = ""
            metrics_endpoint = ""
            if name == "legacy":
                datasource = "redpanda"
                n = self.redpanda.get_node(1)
                metrics_endpoint = self.redpanda.admin_endpoint(
                    n) + "/public_metrics"
            out = self._rpk.generate_grafana(name,
                                             datasource=datasource,
                                             metrics_endpoint=metrics_endpoint)
            try:
                # We just need to assert that it was able to retrieve and parse the dashboard as valid json
                json.loads(out)
            except json.JSONDecodeError as err:
                self.logger.error(
                    f"unable to parse generated' {name}' dashboard : {err}")
