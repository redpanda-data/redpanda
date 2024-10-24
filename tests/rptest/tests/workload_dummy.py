# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import uuid
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.workload_protocol import PWorkload
from rptest.tests.redpanda_test import RedpandaTest


class DummyWorkload(PWorkload):
    def __init__(self, ctx) -> None:
        self.ctx = ctx

    def get_earliest_applicable_release(self):
        self.ctx.logger.info(f"returning None")
        return super().get_earliest_applicable_release()

    def get_latest_applicable_release(self):
        self.ctx.logger.info(f"returning HEAD")
        return super().get_latest_applicable_release()

    def begin(self):
        self.ctx.logger.info("begin: No-op")
        return

    def end(self):
        self.ctx.logger.info("end: no-op")

    def on_partial_cluster_upgrade(self, versions) -> int:
        self.ctx.logger.info(
            f"partial_progress called with versions={ {n.account.hostname: v for n, v in versions.items()} }"
        )
        return super().on_partial_cluster_upgrade(versions)

    def get_workload_name(self):
        return "DummyWorkload"

    def on_cluster_upgraded(self, version: tuple[int, int, int]) -> int:
        versions = [
            self.ctx.redpanda.get_version(n) for n in self.ctx.redpanda.nodes
        ]
        self.ctx.logger.info(f"got {version=}, running on {versions=}")
        return PWorkload.DONE


class MinimalWorkload(PWorkload):
    def __init__(self, ctx: RedpandaTest) -> None:
        self.ctx = ctx
        self.topic = TopicSpec(
            name=f"topic-{self.__class__.__name__}-{str(uuid.uuid4())}",
            replication_factor=3)

    def begin(self):
        self.ctx.client().create_topic(self.topic)

    def end(self):
        self.ctx.client().delete_topic(self.topic.name)

    def on_cluster_upgraded(self, version: tuple[int, int, int]) -> int:
        offset = RpkTool(self.ctx.redpanda).produce(topic=self.topic.name,
                                                    key=f"{version}",
                                                    msg=str(uuid.uuid4()))
        self.ctx.logger.info(f"produced to {self.topic.name} at {offset=}")
        return PWorkload.DONE
