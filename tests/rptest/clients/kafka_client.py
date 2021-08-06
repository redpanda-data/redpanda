# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from __future__ import annotations
from collections.abc import Callable
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import RedpandaService


class KafkaClient:
    @classmethod
    def instances(cls) -> list[Callable[[RedpandaService], KafkaClient]]:
        raise NotImplementedError

    def create_topic(self, spec: TopicSpec):
        raise NotImplementedError

    def delete_topic(self, name: str):
        raise NotImplementedError

    def describe_topic(self, topic: str) -> TopicSpec:
        raise NotImplementedError
