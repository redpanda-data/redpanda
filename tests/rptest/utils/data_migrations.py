# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec

from rptest.services.admin import OutboundDataMigration, InboundDataMigration
from requests.exceptions import ConnectionError

import time
import requests


def now():
    return int(time.time() * 1000)


class DataMigrationTestMixin:
    """assumes self.redpanda, self.admin and self.client() present"""

    def wait_partitions_appear(self, topics: list[TopicSpec]):
        # we may be unlucky to query a slow node
        def topic_has_all_partitions(t: TopicSpec):
            part_cnt = len(self.client().describe_topic(t.name).partitions)
            self.redpanda.logger.debug(
                f"topic {t.name} has {part_cnt} partitions out of {t.partition_count} expected"
            )
            return t.partition_count == part_cnt

        def err_msg():
            msg = "Failed waiting for partitions to appear:\n"
            for t in topics:
                msg += f"   {t.name} expected {t.partition_count} partitions, "
                msg += f"got {len(self.client().describe_topic(t.name).partitions)} partitions\n"
            return msg

        wait_until(
            lambda: all(topic_has_all_partitions(t) for t in topics),
            timeout_sec=90,
            backoff_sec=1,
            err_msg=err_msg,
        )

    def wait_partitions_disappear(self, topics: list[str]):
        # we may be unlucky to query a slow node
        wait_until(
            lambda: all(
                self.client().describe_topic(t).partitions == [] for t in topics
            ),
            timeout_sec=90,
            backoff_sec=1,
            err_msg=f"Failed waiting for partitions to disappear",
        )

    def get_migration(self, id, node=None):
        try:
            return self.admin.get_data_migration(id, node).json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise

    def get_migrations_map(self, node=None):
        self.redpanda.logger.debug("calling self.admin.list_data_migrations")
        migrations = self.admin.list_data_migrations(node).json()
        self.redpanda.logger.debug("received self.admin.list_data_migrations result")
        return {migration["id"]: migration for migration in migrations}

    def on_all_live_nodes(self, migration_id, predicate):
        success_cnt = 0
        exception_cnt = 0
        for n in self.redpanda.nodes:
            try:
                map = self.get_migrations_map(n)
                self.redpanda.logger.debug(f"migrations on node {n.name}: {map}")
                list_item = map[migration_id] if migration_id in map else None
                individual = self.get_migration(migration_id, n)

                if predicate(list_item) and predicate(individual):
                    success_cnt += 1
                else:
                    return False
            except ConnectionError:
                exception_cnt += 1
        return success_cnt > exception_cnt

    def validate_timing(self, time_before, happened_at):
        time_now = now()
        self.logger.debug(f"{time_before=}, {happened_at=}, {time_now=}")
        err_ms = 5  # allow for ntp error across nodes
        assert time_before - err_ms <= happened_at <= time_now + err_ms

    def wait_migration_appear(self, migration_id, assure_created_after):
        def migration_present_on_node(m):
            if m is None:
                return False
            self.validate_timing(assure_created_after, m["created_timestamp"])
            return True

        def migration_is_present(id: int):
            return self.on_all_live_nodes(id, migration_present_on_node)

        wait_until(
            lambda: migration_is_present(migration_id),
            timeout_sec=30,
            backoff_sec=2,
            err_msg=f"Expected migration with id {migration_id} is present",
        )

    def create_and_wait(self, migration: InboundDataMigration | OutboundDataMigration):
        def migration_id_if_exists():
            for n in self.redpanda.nodes:
                for m in self.admin.list_data_migrations(n).json():
                    if m == migration:
                        return m[id]
            return None

        time_before_creation = now()
        try:
            reply = self.admin.create_data_migration(migration).json()
            self.redpanda.logger.info(f"create migration reply: {reply}")
            migration_id = reply["id"]
        except requests.exceptions.HTTPError as e:
            maybe_id = migration_id_if_exists()
            if maybe_id is None:
                raise
            migration_id = maybe_id
            self.redpanda.logger.info(
                f"create migration failed but migration {migration_id} present: {e}"
            )

        self.wait_migration_appear(migration_id, time_before_creation)

        return migration_id

    def assure_not_deletable(self, id, node=None):
        try:
            self.admin.delete_data_migration(id, node)
            assert False
        except requests.exceptions.HTTPError:
            pass

    def wait_for_migration_states(
        self, id: int, states: list[str], assure_completed_after: int = 0
    ):
        def migration_in_one_of_states_on_node(m):
            if m is None:
                return False
            completed_at = m.get("completed_timestamp")
            if m["state"] in ("finished", "cancelled"):
                self.validate_timing(assure_completed_after, completed_at)
            else:
                assert "completed_timestamp" not in m
            return m["state"] in states

        def migration_in_one_of_states():
            return self.on_all_live_nodes(id, migration_in_one_of_states_on_node)

        self.logger.info(f"waiting for {' or '.join(states)}")
        wait_until(
            migration_in_one_of_states,
            timeout_sec=90,
            backoff_sec=1,
            err_msg=f"Failed waiting for migration {id} to reach one of {states} states",
        )
        if all(state not in ("planned", "finished", "cancelled") for state in states):
            self.assure_not_deletable(id)
