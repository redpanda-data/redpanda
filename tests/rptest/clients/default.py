# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools


class DefaultClient:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def create_topic(self, specs):
        if isinstance(specs, TopicSpec):
            specs = [specs]
        client = KafkaCliTools(self._redpanda)
        for spec in specs:
            client.create_topic(spec)
