# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from rptest.services.cluster import cluster
from rptest.tests.datalake.datalake_services import DatalakeServicesBase


class DatalakeE2ETests(DatalakeServicesBase):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeE2ETests, self).__init__(test_ctx,
                                               num_brokers=1,
                                               *args,
                                               **kwargs)
        self.topic_name = "test"

    @cluster(num_nodes=5)
    def test_e2e_basic(self):
        # Create a topic
        # Produce some events
        # Ensure they end up in datalake
        count = 100
        self.create_iceberg_enabled_topic(self.topic_name)
        self.produce_to_topic(self.topic_name, 1024, count)
        self.wait_for_translation(self.topic_name, msg_count=count)
