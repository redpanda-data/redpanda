# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import time
import threading
from typing import Callable, NamedTuple, Literal, List
import typing
from requests.exceptions import ConnectionError
from contextlib import contextmanager, nullcontext

from rptest.services.admin import Admin, MigrationAction
from rptest.services.admin import OutboundDataMigration, InboundDataMigration, NamespacedTopic, InboundTopic

import confluent_kafka as ck

from rptest.services.redpanda import RedpandaServiceBase, SISettings
from ducktape.utils.util import wait_until
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.tests.test import TestContext
from rptest.clients.types import TopicSpec
from rptest.tests.e2e_finjector import Finjector
from rptest.clients.rpk import RpkTool
import requests
import re


def now():
    return round(time.time() * 1000)


def make_namespaced_topic(topic: str) -> NamespacedTopic:
    return NamespacedTopic(topic, random.choice([None, "kafka"]))


class TransferLeadersBackgroundThread:
    def __init__(self, redpanda: RedpandaServiceBase, topic: str):
        self.redpanda = redpanda
        self.logger = redpanda.logger
        self.stop_ev = threading.Event()
        self.topic = topic
        self.admin = Admin(self.redpanda, retry_codes=[503, 504])
        self.thread = threading.Thread(target=lambda: self._loop())
        self.thread.daemon = True

    def __enter__(self):
        self.thread.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop_ev.set()

    def _loop(self):
        while not self.stop_ev.is_set():
            p_id = None
            try:
                partitions = self.admin.get_partitions(namespace="kafka",
                                                       topic=self.topic)
                partition = random.choice(partitions)
                p_id = partition['partition_id']
                self.logger.info(
                    f"Transferring leadership of {self.topic}/{p_id}")
                self.admin.partition_transfer_leadership(namespace="kafka",
                                                         topic=self.topic,
                                                         partition=p_id)
            except Exception as e:
                self.logger.info(
                    f"error transferring leadership of {self.topic}/{p_id} - {e}"
                )


class DataMigrationsTestBase(RedpandaTest):
    MIGRATION_LOG_ALLOW_LIST = [
        'Error during log recovery: cloud_storage::missing_partition_exception',
        re.compile(
            "cloud_storage.*Failed to fetch manifest during finalize().*"),
    ] + Finjector.LOG_ALLOW_LIST

    log_segment_size = 10 * 1024

    def __init__(self,
                 test_context: TestContext,
                 *args,
                 transition_timeout: int = 90,
                 **kwargs):
        kwargs['si_settings'] = SISettings(
            test_context=test_context,
            log_segment_size=self.log_segment_size,
            cloud_storage_max_connections=5,
            cloud_storage_enable_remote_read=True,
            cloud_storage_enable_remote_write=True,
        )
        super().__init__(test_context=test_context, *args, **kwargs)
        self.transition_timeout = transition_timeout
        self.flaky_admin = Admin(self.redpanda, retry_codes=[503, 504])
        self.admin = Admin(self.redpanda)

    @contextmanager
    def flaky_admin_cm(self, other_cm):
        with other_cm:
            old_admin = self.admin
            try:
                self.admin = self.flaky_admin
                yield
            finally:
                self.admin = old_admin

    def finj_thread(self):
        return self.flaky_admin_cm(
            Finjector(self.redpanda, self.scale,
                      max_concurrent_failures=1).finj_thread())

    def tl_thread(self, topic_name):
        if topic_name is None:
            return nullcontext()
        return self.flaky_admin_cm(
            TransferLeadersBackgroundThread(self.redpanda, topic_name))

    def get_migrations_map(self, node=None):
        migrations = self.admin.list_data_migrations(node).json()
        return {migration["id"]: migration for migration in migrations}

    def get_migration(self, id, node=None):
        try:
            return self.admin.get_data_migration(id, node).json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise

    def assure_not_deletable(self, id, node=None):
        try:
            self.admin.delete_data_migration(id, node)
            assert False
        except:
            pass

    def on_all_live_nodes(self, migration_id, predicate):
        success_cnt = 0
        exception_cnt = 0
        for n in self.redpanda.nodes:
            try:
                map = self.get_migrations_map(n)
                self.logger.debug(f"migrations on node {n.name}: {map}")
                list_item = map[migration_id] if migration_id in map else None
                individual = self.get_migration(migration_id, n)

                if predicate(list_item) and predicate(individual):
                    success_cnt += 1
                else:
                    return False
            except ConnectionError:
                exception_cnt += 1
        return success_cnt > exception_cnt

    def wait_for_migration_states(self,
                                  id: int,
                                  states: list[str],
                                  assure_completed_after: int = 0):
        def migration_in_one_of_states_on_node(m):
            if m is None:
                return False
            completed_at = m.get("completed_timestamp")
            self.logger.debug(
                f"{assure_completed_after=}, {completed_at=}, {now()=}")
            if m["state"] in ("finished", "cancelled"):
                assert assure_completed_after <= completed_at <= now()
            else:
                assert "completed_timestamp" not in m
            return m["state"] in states

        def migration_in_one_of_states():
            return self.on_all_live_nodes(id,
                                          migration_in_one_of_states_on_node)

        self.logger.info(f'waiting for {" or ".join(states)}')
        wait_until(
            migration_in_one_of_states,
            timeout_sec=self.transition_timeout,
            backoff_sec=1,
            err_msg=
            f"Failed waiting for migration {id} to reach one of {states} states"
        )
        if all(state not in ('planned', 'finished', 'cancelled')
               for state in states):
            self.assure_not_deletable(id)

    def wait_migration_appear(self, migration_id, assure_created_after):
        def migration_present_on_node(m):
            if m is None:
                return False
            self.logger.debug(
                f"{assure_created_after=}, {m['created_timestamp']=}, {now()=}"
            )
            assert assure_created_after <= m['created_timestamp'] <= now()
            return True

        def migration_is_present(id: int):
            return self.on_all_live_nodes(id, migration_present_on_node)

        wait_until(
            lambda: migration_is_present(migration_id),
            timeout_sec=30,
            backoff_sec=2,
            err_msg=f"Expected migration with id {migration_id} is present")

    def wait_migration_disappear(self, migration_id):
        def migration_is_absent(id: int):
            return self.on_all_live_nodes(id, lambda m: m is None)

        wait_until(
            lambda: migration_is_absent(migration_id),
            timeout_sec=self.transition_timeout,
            backoff_sec=2,
            err_msg=f"Expected migration with id {migration_id} is absent")

    def wait_partitions_appear(self, topics: list[TopicSpec]):
        # we may be unlucky to query a slow node
        def topic_has_all_partitions(t: TopicSpec):
            part_cnt = len(self.client().describe_topic(t.name).partitions)
            self.logger.debug(
                f"topic {t.name} has {part_cnt} partitions out of {t.partition_count} expected"
            )
            return t.partition_count == part_cnt

        def err_msg():
            msg = "Failed waiting for partitions to appear:\n"
            for t in topics:
                msg += f"   {t.name} expected {t.partition_count} partitions, "
                msg += f"got {len(self.client().describe_topic(t.name).partitions)} partitions\n"
            return msg

        wait_until(lambda: all(topic_has_all_partitions(t) for t in topics),
                   timeout_sec=90,
                   backoff_sec=1,
                   err_msg=err_msg)

    def wait_partitions_disappear(self, topics: list[TopicSpec]):
        # we may be unlucky to query a slow node
        wait_until(
            lambda: all(self.client().describe_topic(t.name).partitions == []
                        for t in topics),
            timeout_sec=90,
            backoff_sec=1,
            err_msg=f"Failed waiting for partitions to disappear")

    def create_and_wait(self, migration: InboundDataMigration
                        | OutboundDataMigration):
        def migration_id_if_exists():
            for n in self.redpanda.nodes:
                for m in self.admin.list_data_migrations(n).json():
                    if m == migration:
                        return m[id]
            return None

        time_before_creation = now()
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

        self.wait_migration_appear(migration_id, time_before_creation)

        return migration_id

    def execute_data_migration_action_flaky(self, migration_id, action):
        try:
            self.admin.execute_data_migration_action(migration_id, action)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                # previous attempt might be successful but response lost
                self.logger.info(
                    f"operation {action} on migration {migration_id} failed with {e}, ignoring"
                )
                return
            raise

    def log_topics(self, topic_names):
        for t in topic_names:
            try:
                topic_desc = self.client().describe_topic(t)
            except ck.KafkaException as e:
                self.logger.warn(f"failed to describe topic {t}: {e}")
            else:
                self.logger.info(f"topic {t} is {topic_desc}")

    def check_migrations(self, migration_id, exp_topics_cnt,
                         exp_migrations_cnt):
        """ make sure that, when the migration appears,
            - it its state is planned,
            - it contains this many topics,
            - and also there are that many migrations in total """
        def check():
            migrations_map = self.get_migrations_map()
            self.logger.info(f"migrations: {migrations_map}")
            if migration_id not in migrations_map:
                return False  # finj may make things lag ...
            migration = migrations_map[migration_id]
            # ... but not lie
            assert migration['state'] == 'planned'
            assert len(migration['migration']['topics']) == exp_topics_cnt
            assert len(migrations_map) == exp_migrations_cnt
            return True

        wait_until(check,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg=f"Failed waiting for migration")

    def do_test_creating_and_listing_migrations(self, topics_count,
                                                partitions_count):
        topics = [
            TopicSpec(partition_count=partitions_count)
            for i in range(topics_count)
        ]

        for t in topics:
            self.client().create_topic(t)

        migrations_map = self.get_migrations_map()

        assert len(migrations_map) == 0, "There should be no data migrations"

        with self.finj_thread():
            # out
            outbound_topics = [make_namespaced_topic(t.name) for t in topics]
            out_migration = OutboundDataMigration(outbound_topics,
                                                  consumer_groups=[])

            out_migration_id = self.create_and_wait(out_migration)
            self.check_migrations(out_migration_id, len(topics), 1)

            self.execute_data_migration_action_flaky(out_migration_id,
                                                     MigrationAction.prepare)
            self.wait_for_migration_states(out_migration_id,
                                           ['preparing', 'prepared'])
            self.wait_for_migration_states(out_migration_id, ['prepared'])
            self.execute_data_migration_action_flaky(out_migration_id,
                                                     MigrationAction.execute)
            self.wait_for_migration_states(out_migration_id,
                                           ['executing', 'executed'])
            self.wait_for_migration_states(out_migration_id, ['executed'])
            time_before_final_action = now()
            self.execute_data_migration_action_flaky(out_migration_id,
                                                     MigrationAction.finish)
            self.wait_for_migration_states(out_migration_id,
                                           ['cut_over', 'finished'],
                                           time_before_final_action)
            self.wait_for_migration_states(out_migration_id, ['finished'],
                                           time_before_final_action)

            self.wait_partitions_disappear(topics)

            # in
            inbound_topics = [
                InboundTopic(make_namespaced_topic(t.name),
                             alias=\
                                None if i == 0
                                else make_namespaced_topic(f"{t.name}-alias"))
                for i, t in enumerate(topics[:3])
            ]
            in_migration = InboundDataMigration(topics=inbound_topics,
                                                consumer_groups=["g-1", "g-2"])
            in_migration_id = self.create_and_wait(in_migration)
            self.check_migrations(in_migration_id, len(inbound_topics), 2)

            self.log_topics(t.source_topic_reference.topic
                            for t in inbound_topics)

            self.execute_data_migration_action_flaky(in_migration_id,
                                                     MigrationAction.prepare)
            self.wait_for_migration_states(in_migration_id,
                                           ['preparing', 'prepared'])
            self.wait_for_migration_states(in_migration_id, ['prepared'])
            self.execute_data_migration_action_flaky(in_migration_id,
                                                     MigrationAction.execute)
            self.wait_for_migration_states(in_migration_id,
                                           ['executing', 'executed'])
            self.wait_for_migration_states(in_migration_id, ['executed'])
            time_before_final_action = now()
            self.execute_data_migration_action_flaky(in_migration_id,
                                                     MigrationAction.finish)
            self.wait_for_migration_states(in_migration_id,
                                           ['cut_over', 'finished'],
                                           time_before_final_action)
            self.wait_for_migration_states(in_migration_id, ['finished'],
                                           time_before_final_action)

            self.log_topics(t.name for t in topics)

        # todo: fix rp_storage_tool to use overridden topic names
        self.redpanda.si_settings.set_expected_damage(
            {"ntr_no_topic_manifest", "missing_segments"})
