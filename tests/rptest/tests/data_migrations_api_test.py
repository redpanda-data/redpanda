# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
from collections import Counter
from requests.exceptions import ConnectionError

from rptest.services.admin import Admin, MigrationAction
from rptest.services.admin import OutboundDataMigration, InboundDataMigration, NamespacedTopic, InboundTopic

from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.tests.test import TestContext
from rptest.clients.types import TopicSpec
from rptest.tests.e2e_finjector import Finjector


class DataMigrationsApiTest(RedpandaTest):
    log_segment_size = 10 * 1024

    def __init__(self, test_context: TestContext, *args, **kwargs):
        kwargs['si_settings'] = SISettings(
            test_context=test_context,
            log_segment_size=self.log_segment_size,
            cloud_storage_max_connections=5,
            cloud_storage_enable_remote_read=True,
            cloud_storage_enable_remote_write=True,
        )
        super().__init__(test_context=test_context, *args, **kwargs)
        self.admin = Admin(self.redpanda)

    def get_migrations_map(self, node=None):
        migrations = self.admin.list_data_migrations(node).json()
        return {migration["id"]: migration for migration in migrations}

    def on_all_live_nodes(self, migration_id, predicate):
        success_cnt = 0
        exception_cnt = 0
        for n in self.redpanda.nodes:
            try:
                map = self.get_migrations_map(n)
                if migration_id in map and predicate(map[migration_id]):
                    success_cnt += 1
                else:
                    return False
            except ConnectionError:
                exception_cnt += 1
        return success_cnt > exception_cnt

    def wait_for_migration_states(self, id: int, states: list[str]):
        def migration_in_one_of_states():
            return self.on_all_live_nodes(id, lambda m: m["state"] in states)

        wait_until(
            migration_in_one_of_states,
            timeout_sec=90,
            backoff_sec=1,
            err_msg=
            f"Failed waiting for migration {id} to reach on of {states} states"
        )

    def create_and_wait(self, migration: InboundDataMigration
                        | OutboundDataMigration):
        def migration_id_if_exists():
            for n in self.redpanda.nodes:
                for m in self.admin.list_data_migrations(node).json():
                    if m == migration:
                        return m[id]
            return None

        try:
            reply = self.admin.create_data_migration(migration).json()
            self.logger.info(f"create migration reply: {reply}")
            migration_id = reply["id"]
        except requests.exceptions.HTTPError as e:
            maybe_id = migration_id_if_exists()
            if maybe_id is None:
                raise
            migration_id = maybe_id
            self.logger.info(f"create migration failed "
                             f"but migration {migration_id} present: {e}")

        def migration_is_present(id: int):
            return self.on_all_live_nodes(id, lambda m: True)

        wait_until(
            lambda: migration_is_present(migration_id), 30, 2,
            f"Expected migration with id {migration_id} is not present")
        return migration_id

    @cluster(num_nodes=3)
    def test_creating_and_listing_migrations(self):
        self.finjector = Finjector(self.redpanda, self.test_context)

        topics = [TopicSpec(partition_count=3) for i in range(5)]

        for t in topics:
            self.client().create_topic(t)

        admin = Admin(self.redpanda)
        migrations_map = self.get_migrations_map()

        assert len(migrations_map) == 0, "There should be no data migrations"

        with Finjector(self.redpanda, self.scale).finj_thread():
            # out
            outbound_topics = [NamespacedTopic(t.name) for t in topics]
            out_migration = OutboundDataMigration(outbound_topics,
                                                  consumer_groups=[])

            out_migration_id = self.create_and_wait(out_migration)

            migrations_map = self.get_migrations_map()
            self.logger.info(f"migrations: {migrations_map}")
            assert len(
                migrations_map) == 1, "There should be one data migration"

            assert migrations_map[out_migration_id]['state'] == 'planned'

            assert len(migrations_map[out_migration_id]['migration']['topics']
                       ) == len(topics), "migration should contain all topics"

            admin.execute_data_migration_action(out_migration_id,
                                                MigrationAction.prepare)
            self.logger.info('waiting for preparing or prepared')
            self.wait_for_migration_states(out_migration_id,
                                           ['preparing', 'prepared'])
            self.logger.info('waiting for prepared')
            self.wait_for_migration_states(out_migration_id, ['prepared'])
            admin.execute_data_migration_action(out_migration_id,
                                                MigrationAction.execute)
            self.logger.info('waiting for executing or executed')
            self.wait_for_migration_states(out_migration_id,
                                           ['executing', 'executed'])
            self.logger.info('waiting for executed')
            self.wait_for_migration_states(out_migration_id, ['executed'])
            admin.execute_data_migration_action(out_migration_id,
                                                MigrationAction.finish)
            self.logger.info('waiting for cut_over or finished')
            self.wait_for_migration_states(out_migration_id,
                                           ['cut_over', 'finished'])
            self.wait_for_migration_states(out_migration_id, ['finished'])

            # we may be unlucky to query a slow node
            wait_until(lambda: all(self.client().describe_topic(t.name).
                                   partitions == [] for t in topics),
                       timeout_sec=30,
                       backoff_sec=1,
                       err_msg=f"Failed waiting for partitions to disappear")
            # in
            #alias=None if i == 0 else NamespacedTopic(f"topic-{i}-alias"))
            inbound_topics = [
                InboundTopic(NamespacedTopic(t.name),
                             alias=\
                                None if i == 0
                                else NamespacedTopic(f"{t.name}-alias"))
                for i, t in enumerate(topics[:3])
            ]
            in_migration = InboundDataMigration(topics=inbound_topics,
                                                consumer_groups=["g-1", "g-2"])
            in_migration_id = self.create_and_wait(in_migration)

            migrations_map = self.get_migrations_map()
            self.logger.info(f"migrations: {migrations_map}")
            assert len(
                migrations_map) == 2, "There should be two data migrations"

            assert len(
                migrations_map[in_migration_id]['migration']['topics']) == len(
                    inbound_topics), "migration should contain all topics"

            for t in inbound_topics:
                self.logger.info(
                    f"inbound topic: {self.client().describe_topic(t.src_topic.topic)}"
                )

            admin.execute_data_migration_action(in_migration_id,
                                                MigrationAction.prepare)
            self.logger.info('waiting for preparing or prepared')
            self.wait_for_migration_states(in_migration_id,
                                           ['preparing', 'prepared'])
            self.logger.info('waiting for prepared')
            self.wait_for_migration_states(in_migration_id, ['prepared'])
            admin.execute_data_migration_action(in_migration_id,
                                                MigrationAction.execute)
            self.logger.info('waiting for executing or executed')
            self.wait_for_migration_states(in_migration_id,
                                           ['executing', 'executed'])
            self.logger.info('waiting for executed')
            self.wait_for_migration_states(in_migration_id, ['executed'])
            admin.execute_data_migration_action(in_migration_id,
                                                MigrationAction.finish)
            self.logger.info('waiting for cut_over or finished')
            self.wait_for_migration_states(in_migration_id,
                                           ['cut_over', 'finished'])
            self.wait_for_migration_states(in_migration_id, ['finished'])

            for t in topics:
                self.logger.info(
                    f'topic {t.name} is {self.client().describe_topic(t.name)}'
                )

            # TODO: check unhappy scenarios like this
            # admin.execute_data_migration_action(out_migration_id,
            #                                     MigrationAction.cancel)
            # self.wait_for_migration_state(out_migration_id, 'canceling')
        # todo: fix rp_storage_tool to use overridden topic names
        self.redpanda.si_settings.set_expected_damage(
            {"ntr_no_topic_manifest", "missing_segments"})
