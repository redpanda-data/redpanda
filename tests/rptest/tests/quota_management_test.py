# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.kafka_cli_tools import KafkaCliTools, KafkaCliToolsError
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception


def expect_kafka_cli_error_msg(error_msg: str):
    return expect_exception(KafkaCliToolsError,
                            lambda e: error_msg in e.output)


class QuotaManagementTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.kafka_cli = KafkaCliTools(self.redpanda)

    @cluster(num_nodes=1)
    def test_alter(self):
        self.kafka_cli.alter_quota_config(
            "--entity-type clients --entity-name client_1",
            to_add={"consumer_byte_rate": 10240})

    @cluster(num_nodes=1)
    def test_describe(self):
        self.kafka_cli.describe_quota_config("--client-defaults")
