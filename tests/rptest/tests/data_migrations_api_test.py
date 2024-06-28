# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random

from rptest.services.admin import Admin, MigrationAction
from rptest.services.admin import OutboundDataMigration, InboundDataMigration, NamespacedTopic, InboundTopic

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.tests.test import TestContext
from rptest.clients.types import TopicSpec


class DataMigrationsApiTest(RedpandaTest):
    def __init__(self, test_context: TestContext):
        super().__init__(test_context=test_context)
        self.admin = Admin(self.redpanda)

    def get_migrations_map(self, node=None):
        migrations = self.admin.list_data_migrations(node).json()
        return {migration["id"]: migration for migration in migrations}

    def wait_for_migration_state(self, id: int, state: str):
        def migration_in_state():
            for n in self.redpanda.nodes:
                migrations = self.get_migrations_map(n)
                if id not in migrations or migrations[id]["state"] != state:
                    return False
            return True

        wait_until(
            migration_in_state,
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Failed waiting for migration {id} to reach {state} state"
        )

    def create_and_wait(self, migration: InboundDataMigration
                        | OutboundDataMigration):
        reply = self.admin.create_data_migration(migration).json()
        self.logger.info(f"create migration reply: {reply}")

        def migration_is_present(id: int):
            for n in self.redpanda.nodes:
                m = self.get_migrations_map(n)
                if id not in m:
                    return False
            return True

        migration_id = reply["id"]
        wait_until(
            lambda: migration_is_present(migration_id), 30, 2,
            f"Expected migration with id {migration_id} is not present")
        return migration_id

    @cluster(num_nodes=3)
    def test_creating_and_listing_migrations(self):

        topics = [TopicSpec(partition_count=3) for i in range(5)]

        for t in topics:
            self.client().create_topic(t)

        admin = Admin(self.redpanda)
        migrations_map = self.get_migrations_map()

        assert len(migrations_map) == 0, "There should be no data migrations"

        topics_to_migrate = [NamespacedTopic(t.name) for t in topics]
        out_migration = OutboundDataMigration(topics_to_migrate,
                                              consumer_groups=[])

        out_migration_id = self.create_and_wait(out_migration)
        inbound_topics = [
            InboundTopic(
                NamespacedTopic(f"topic-{i}"),
                alias=None if i == 0 else NamespacedTopic(f"topic-{i}-alias"))
            for i in range(3)
        ]
        in_migration = InboundDataMigration(topics=inbound_topics,
                                            consumer_groups=["g-1", "g-2"])

        in_migration_id = self.create_and_wait(in_migration)

        migrations_map = self.get_migrations_map()
        self.logger.info(f"migrations: {migrations_map}")
        assert len(migrations_map) == 2, "There should be two data migrations"

        assert migrations_map[out_migration_id]['state'] == 'planned'

        assert len(
            migrations_map[out_migration_id]['migration']['topics']) == len(
                topics), "migration should contain all topics"

        admin.execute_data_migration_action(out_migration_id,
                                            MigrationAction.prepare)
        self.wait_for_migration_state(out_migration_id, 'preparing')

        admin.execute_data_migration_action(out_migration_id,
                                            MigrationAction.cancel)
        self.wait_for_migration_state(out_migration_id, 'canceling')
