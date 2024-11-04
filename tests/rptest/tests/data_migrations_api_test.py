# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import threading
import time
from enum import Enum
from typing import Callable, NamedTuple, Literal, List
import typing
from requests.exceptions import ConnectionError
from contextlib import contextmanager, nullcontext

from rptest.services.admin import Admin, MigrationAction
from rptest.services.admin import OutboundDataMigration, InboundDataMigration, NamespacedTopic, InboundTopic

import confluent_kafka as ck

from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService, RedpandaServiceBase, SISettings
from ducktape.utils.util import wait_until
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.tests.test import TestContext
from rptest.clients.types import TopicSpec
from rptest.tests.e2e_finjector import Finjector
from rptest.clients.rpk import RpkTool, RpkException
from ducktape.mark import matrix
import requests
import re

MIGRATION_LOG_ALLOW_LIST = [
    'Error during log recovery: cloud_storage::missing_partition_exception',
    re.compile("cloud_storage.*Failed to fetch manifest during finalize().*"),
] + Finjector.LOG_ALLOW_LIST


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


class CancellationStage(NamedTuple):
    dir: Literal['in', 'out']
    stage: Literal['preparing', 'prepared', 'executing', 'executed']

    @classmethod
    def options(cls, member):
        return typing.get_args(cls.__annotations__[member])


class TmtpdiParams(NamedTuple):
    """parameters for test_migrated_topic_data_integrity"""
    cancellation: CancellationStage | None
    use_alias: bool


def generate_tmptpdi_params() -> List[TmtpdiParams]:
    cancellation_stages = [
        CancellationStage(dir, stage)
        for dir in CancellationStage.options('dir')
        for stage in CancellationStage.options('stage')
    ]
    return [
        TmtpdiParams(cancellation, use_alias)
        for cancellation in [None] + cancellation_stages
        for use_alias in (True, False)
        # alias only affects inbound, pointless to vary if cancel earlier
        if not use_alias or cancellation is None or cancellation.dir == 'in'
    ]


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
        self.flaky_admin = Admin(self.redpanda, retry_codes=[503, 504])
        self.admin = Admin(self.redpanda)
        self.last_producer_id = 0
        self.last_consumer_id = 0

    def get_topic_initial_revision(self, topic_name):
        anomalies = self.admin.get_cloud_storage_anomalies(namespace="kafka",
                                                           topic=topic_name,
                                                           partition=1)
        return anomalies['revision_id']

    def get_ck_producer(self, use_transactional=False):
        self.last_producer_id += 1
        return ck.Producer(
            {'bootstrap.servers': self.redpanda.brokers()} |
            ({
                'transactional.id': f"tx-id-{self.last_producer_id}"
            } if use_transactional else {}),
            logger=self.logger,
            debug='all')

    @contextmanager
    def ck_consumer(self):
        self.last_consumer_id += 1
        consumer = ck.Consumer({
            'group.id': f'group-{self.last_consumer_id}',
            'bootstrap.servers': self.redpanda.brokers(),
            'auto.offset.reset': 'earliest',
            'isolation.level': 'read_committed',
        })
        try:
            yield consumer
        finally:
            consumer.close()

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

    def wait_for_migration_states(self, id: int, states: list[str]):
        def migration_in_one_of_states():
            return self.on_all_live_nodes(
                id, lambda m: m is not None and m["state"] in states)

        self.logger.info(f'waiting for {" or ".join(states)}')
        wait_until(
            migration_in_one_of_states,
            timeout_sec=90,
            backoff_sec=1,
            err_msg=
            f"Failed waiting for migration {id} to reach one of {states} states"
        )
        if all(state not in ('planned', 'finished', 'cancelled')
               for state in states):
            self.assure_not_deletable(id)

    def wait_migration_appear(self, migration_id):
        def migration_is_present(id: int):
            return self.on_all_live_nodes(id, lambda m: m is not None)

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
            timeout_sec=90,
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

        self.wait_migration_appear(migration_id)
        return migration_id

    def assure_not_migratable(self, topic: TopicSpec):
        out_migration = OutboundDataMigration(
            [make_namespaced_topic(topic.name)], consumer_groups=[])
        try:
            self.create_and_wait(out_migration)
            assert False
        except requests.exceptions.HTTPError as e:
            pass

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    def test_listing_inexistent_migration(self):
        try:
            self.get_migration(42)
        except Exception as e:
            # check 404
            self.logger.info("f{e}")
            raise

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    def test_creating_with_topic_no_remote_writes(self):
        self.redpanda.set_cluster_config(
            {"cloud_storage_enable_remote_write": False}, expect_restart=True)
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)
        self.wait_partitions_appear([topic])
        self.redpanda.set_cluster_config(
            {"cloud_storage_enable_remote_write": True}, expect_restart=True)
        self.assure_not_migratable(topic)

    @cluster(
        num_nodes=3,
        log_allow_list=MIGRATION_LOG_ALLOW_LIST + [
            r'/v1/migrations.*Requested feature is disabled',  # cloud storage disabled
        ])
    def test_creating_when_cluster_misconfigured1(self):
        self.creating_when_cluster_misconfigured("cloud_storage_enabled")

    @cluster(
        num_nodes=3,
        log_allow_list=MIGRATION_LOG_ALLOW_LIST + [
            r'/v1/migrations.*Requested feature is disabled',  # cloud storage disabled
            'archival'  # a variety of archival errors is observed
        ])
    def test_creating_when_cluster_misconfigured2(self):
        self.creating_when_cluster_misconfigured(
            "cloud_storage_disable_archiver_manager")

    def creating_when_cluster_misconfigured(self, param_to_disable):
        self.redpanda.set_cluster_config({param_to_disable: False},
                                         expect_restart=True)
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)
        self.assure_not_migratable(topic)
        # for scrubbing to complete
        self.redpanda.set_cluster_config({param_to_disable: True},
                                         expect_restart=True)

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

    def assure_exactly_one_message(self,
                                   topic_name,
                                   predicate=lambda msg: True):
        def format_message(msg):
            if msg is None:
                return None
            if msg.error() is not None:
                return f"{{{msg.error()=}}}"
            return f"{{{msg.key()=}, {msg.value()=}}}"

        def poll_hard(consumer, timeout):
            deadline = time.time() + timeout
            while time.time() <= deadline:
                msg = consumer.poll(timeout)
                if msg is not None and msg.error() is None:
                    break
                self.logger.warn(f"error polling: {format_message(msg)}")
            return msg

        with self.ck_consumer() as consumer:
            consumer.subscribe([topic_name])
            msg = poll_hard(consumer, 20)
            self.logger.debug(f"first msg={format_message(msg)}")
            assert msg.error() is None and predicate(msg)
            msg = poll_hard(consumer, 10)
            self.logger.debug(f"second msg={format_message(msg)}")
            assert msg is None

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    def test_mount_inexistent(self):
        topic = TopicSpec(partition_count=3)

        with self.finj_thread():
            in_migration = InboundDataMigration(
                topics=[InboundTopic(make_namespaced_topic(topic.name))],
                consumer_groups=[])
            in_migration_id = self.create_and_wait(in_migration)

            migrations_map = self.get_migrations_map()
            self.logger.info(f"migrations: {migrations_map}")
            assert len(
                migrations_map) == 1, "There should be one data migration"

            assert len(migrations_map[in_migration_id]['migration']
                       ['topics']) == 1, "migration should contain one topic"

            self.execute_data_migration_action_flaky(in_migration_id,
                                                     MigrationAction.prepare)
            self.wait_for_migration_states(in_migration_id, ['preparing'])
            time.sleep(10)
            # still preparing, i.e. stuck
            self.wait_for_migration_states(in_migration_id, ['preparing'])
            # and the topic is not there
            self.wait_partitions_disappear([topic])

            self.execute_data_migration_action_flaky(in_migration_id,
                                                     MigrationAction.cancel)
            self.wait_for_migration_states(in_migration_id,
                                           ['canceling', 'cancelled'])
            self.wait_for_migration_states(in_migration_id, ['cancelled'])
            # still not there
            self.wait_partitions_disappear([topic])

            self.admin.delete_data_migration(in_migration_id)
            self.wait_migration_disappear(in_migration_id)

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    def test_creating_and_listing_migrations(self):
        topics = [TopicSpec(partition_count=3) for i in range(5)]

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

            migrations_map = self.get_migrations_map()
            self.logger.info(f"migrations: {migrations_map}")
            assert len(
                migrations_map) == 1, "There should be one data migration"

            assert migrations_map[out_migration_id]['state'] == 'planned'

            assert len(migrations_map[out_migration_id]['migration']['topics']
                       ) == len(topics), "migration should contain all topics"

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
            self.execute_data_migration_action_flaky(out_migration_id,
                                                     MigrationAction.finish)
            self.wait_for_migration_states(out_migration_id,
                                           ['cut_over', 'finished'])
            self.wait_for_migration_states(out_migration_id, ['finished'])

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

            migrations_map = self.get_migrations_map()
            self.logger.info(f"migrations: {migrations_map}")
            assert len(
                migrations_map) == 2, "There should be two data migrations"

            assert len(
                migrations_map[in_migration_id]['migration']['topics']) == len(
                    inbound_topics), "migration should contain all topics"

            for t in inbound_topics:
                self.logger.info(
                    f"inbound topic: {self.client().describe_topic(t.source_topic_reference.topic)}"
                )

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
            self.execute_data_migration_action_flaky(in_migration_id,
                                                     MigrationAction.finish)
            self.wait_for_migration_states(in_migration_id,
                                           ['cut_over', 'finished'])
            self.wait_for_migration_states(in_migration_id, ['finished'])

            for t in topics:
                self.logger.info(
                    f'topic {t.name} is {self.client().describe_topic(t.name)}'
                )

        # todo: fix rp_storage_tool to use overridden topic names
        self.redpanda.si_settings.set_expected_damage(
            {"ntr_no_topic_manifest", "missing_segments"})

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    def test_conflicting_names(self):
        def on_delivery(err, msg):
            if err is not None:
                raise ck.KafkaException(err)

        def make_msg(i: int):
            return {
                component: str.encode(f"{component}{i}")
                for component in ('key', 'value')
            }

        topic = TopicSpec(partition_count=3)
        ns_topic = make_namespaced_topic(topic.name)
        producer = self.get_ck_producer()

        # create, populate and unmount 3 topics
        revisions = {}
        for i in range(3):
            self.client().create_topic(topic)
            producer.produce(topic.name, **make_msg(i), callback=on_delivery)
            producer.flush()
            revisions[i] = self.get_topic_initial_revision(topic.name)
            out_migr_id = self.admin.unmount_topics([ns_topic]).json()["id"]
            self.wait_partitions_disappear([topic])
            self.wait_migration_disappear(out_migr_id)

        # mount and consume from them in random order
        cluster_uuid = self.admin.get_cluster_uuid(self.redpanda.nodes[0])
        for i in sorted(range(3), key=lambda element: random.random()):
            hinted_ns_topic = make_namespaced_topic(
                f"{ns_topic.topic}/{cluster_uuid}/{revisions[i]}")
            in_topic = InboundTopic(hinted_ns_topic)
            in_migr_id = self.admin.mount_topics([in_topic]).json()["id"]
            self.wait_partitions_appear([topic])
            self.wait_migration_disappear(in_migr_id)
            expected_msg_predicate = lambda msg: {
                'key': msg.key(),
                'value': msg.value()
            } == make_msg(i)
            self.assure_exactly_one_message(topic.name, expected_msg_predicate)
            self.client().delete_topic(topic.name)

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    def test_higher_level_migration_api(self):
        topics = [TopicSpec(partition_count=3) for i in range(5)]
        for t in topics:
            self.client().create_topic(t)

        producer = self.get_ck_producer(use_transactional=True)
        producer.init_transactions()
        # commit one message
        producer.begin_transaction()
        producer.produce(topics[0].name, key="key1", value="value1")
        producer.commit_transaction()
        # leave second message uncommitted
        producer.begin_transaction()
        producer.produce(topics[0].name, key="key2", value="value2")

        with self.finj_thread():
            # out
            outbound_topics = [make_namespaced_topic(t.name) for t in topics]
            reply = self.admin.unmount_topics(outbound_topics).json()
            self.logger.info(f"create migration reply: {reply}")
            out_migration_id = reply["id"]

            self.logger.info('waiting for partitions be deleted')
            self.wait_partitions_disappear(topics)
            self.logger.info('waiting for migration to be deleted')
            self.wait_migration_disappear(out_migration_id)

            migrations_map = self.get_migrations_map()
            self.logger.info(f"migrations: {migrations_map}")

            # in
            inbound_topics = [
                InboundTopic(make_namespaced_topic(t.name),
                             alias=\
                                None if i == 0
                                else make_namespaced_topic(f"{t.name}-alias"))
                for i, t in enumerate(topics[:3])
            ]
            inbound_topics_spec = [
                TopicSpec(name=(it.alias or it.source_topic_reference).topic,
                          partition_count=3) for it in inbound_topics
            ]
            reply = self.admin.mount_topics(inbound_topics).json()
            self.logger.info(f"create migration reply: {reply}")
            in_migration_id = reply["id"]

            self.logger.info('waiting for partitions to come back')
            self.wait_partitions_appear(inbound_topics_spec)
            self.logger.info('waiting for migration to be deleted')
            self.wait_migration_disappear(in_migration_id)
            for t in topics:
                self.logger.info(
                    f'topic {t.name} is {self.client().describe_topic(t.name)}'
                )
            migrations_map = self.get_migrations_map()
            self.logger.info(f"migrations: {migrations_map}")

        self.assure_exactly_one_message(topics[0].name)

        # todo: fix rp_storage_tool to use overridden topic names
        self.redpanda.si_settings.set_expected_damage(
            {"ntr_no_topic_manifest", "missing_segments"})

    @property
    def msg_size(self):
        return 128

    @property
    def msg_count(self):
        return int(20 * self.producer_throughput / self.msg_size)

    @property
    def producer_throughput(self):
        return 1024 if self.debug_mode else 1024 * 1024

    def start_producer(self, topic):
        class ProducerWrapper:
            def __init__(self, *args, msg_count, **kwargs):
                self.producer = KgoVerifierProducer(
                    *args,
                    tolerate_failed_produce=True,
                    trace_logs=True,
                    **kwargs)
                self.producer.start(clean=False)
                timeout_sec = 120
                wait_until( \
                    lambda: self.producer.produce_status.acked > msg_count,
                    timeout_sec=timeout_sec,
                    backoff_sec=1,
                    err_msg=f"failed to produce {msg_count} messages in {timeout_sec} seconds")

            def stop_if_running(self):
                if self.producer:
                    self.producer.stop()
                    self.acked_records = self.producer.produce_status.acked
                    self.producer.free()
                    self.producer = None

        self.logger.info(
            f"starting kgo-verifier producer with {self.msg_count} messages of size {self.msg_size} and throughput: {self.producer_throughput} bps"
        )
        return ProducerWrapper(
            self.test_context,
            self.redpanda,
            topic,
            self.msg_size,
            100000000,  #we do not want to limit number of messages
            msg_count=self.msg_count,
            rate_limit_bps=self.producer_throughput)

    def start_consumer(self, topic):
        consumer = KgoVerifierConsumerGroupConsumer(self.test_context,
                                                    self.redpanda,
                                                    topic,
                                                    self.msg_size,
                                                    readers=3,
                                                    group_name="test-group",
                                                    trace_logs=True)

        consumer.start(clean=False)
        return consumer

    def _do_validate_topic_operation(self, topic: str, op_name: str,
                                     expected_to_pass: bool,
                                     operation: Callable[[str], typing.Any]):
        self.logger.info(
            f"Validating execution of {op_name} against topic: {topic}")
        success = True
        try:
            operation(topic)
        except Exception as e:
            self.logger.info(
                f"Operation {op_name} executed against topic: {topic} failed - {e}"
            )
            success = False

        assert expected_to_pass == success, f"Operation {op_name} outcome is not " \
            f"expected. Expected to pass: {expected_to_pass}, succeeded: {success}"

    def validate_topic_access(
        self,
        topic: str,
        metadata_locked: bool,
        read_blocked: bool,
        produce_blocked: bool,
        test_add_partitions: bool = True,
        assert_topic_present: bool = True,
    ):
        rpk = RpkTool(self.redpanda)
        if assert_topic_present:
            assert topic in rpk.list_topics(), \
                f"validated topic {topic} must be present"

        if test_add_partitions:
            self._do_validate_topic_operation(
                topic=topic,
                op_name="add_partitions",
                expected_to_pass=not metadata_locked,
                operation=lambda topic: rpk.add_partitions(topic, 33))

        def _alter_cfg(topic):
            rpk.alter_topic_config(topic, TopicSpec.PROPERTY_FLUSH_MS, 2000)
            rpk.delete_topic_config(topic, TopicSpec.PROPERTY_FLUSH_MS)

        self._do_validate_topic_operation(topic=topic,
                                          op_name="alter_topic_configuration",
                                          expected_to_pass=not metadata_locked,
                                          operation=_alter_cfg)

        self._do_validate_topic_operation(
            topic=topic,
            op_name="read",
            expected_to_pass=not read_blocked,
            operation=lambda topic: rpk.consume(topic=topic, n=1, offset=0))

        # check if topic is writable only if it is expected to be blocked not to disturb the verifiers.
        if produce_blocked:
            # check if topic is readable
            self._do_validate_topic_operation(
                topic=topic,
                op_name="produce",
                expected_to_pass=not produce_blocked,
                operation=lambda topic: rpk.produce(
                    topic=topic, key="test-key", msg='test-msg'))

    def consume_and_validate(self, topic_name, expected_records):
        consumer = self.start_consumer(topic=topic_name)

        def check():
            self.logger.info(
                f"consumer={id(consumer)}, consumer._status={consumer._status}, expected_records={expected_records}"
            )
            return consumer._status.validator.valid_reads >= expected_records

        wait_until(
            check,
            timeout_sec=180,
            backoff_sec=0.5,
            err_msg=f"Error waiting for consumer to see all {expected_records} "
            f"produced messages, seeing {consumer._status}",
        )
        consumer.wait()
        consumer.stop()

    def cancel(self, migration_id, topic_name):
        self.admin.execute_data_migration_action(migration_id,
                                                 MigrationAction.cancel)
        self.wait_for_migration_states(migration_id, ['cancelled'])
        self.admin.delete_data_migration(migration_id)

    def assert_no_topics(self):
        rpk = RpkTool(self.redpanda)
        topics = list(rpk.list_topics())
        self.logger.info(f"topic list: {topics}")

        assert len(topics) == 0, \
            "outbound migration complete, inbound migration not complete " \
            "and not in progress, so the topic should be removed"

    def cancel_outbound(self, migration_id, topic_name, producer):
        self.cancel(migration_id, topic_name)
        producer.stop_if_running()
        self.consume_and_validate(topic_name, producer.acked_records)
        self.validate_topic_access(topic=topic_name,
                                   metadata_locked=False,
                                   read_blocked=False,
                                   produce_blocked=False,
                                   assert_topic_present=False,
                                   test_add_partitions=True)

    def cancel_inbound(self, migration_id, topic_name):
        self.cancel(migration_id, topic_name)
        self.assert_no_topics()

    @cluster(
        num_nodes=4,
        log_allow_list=MIGRATION_LOG_ALLOW_LIST + [
            # dropping a topic while transferring its leadership
            '/transfer_leadership\] reason - seastar::abort_requested_exception',
            '/transfer_leadership\] reason - seastar::broken_named_semaphore',
            '/transfer_leadership\] reason - seastar::gate_closed_exception',
        ])
    @matrix(transfer_leadership=[True, False],
            params=generate_tmptpdi_params())
    def test_migrated_topic_data_integrity(self, transfer_leadership: bool,
                                           params: TmtpdiParams):
        cancellation = params.cancellation
        rpk = RpkTool(self.redpanda)
        self.redpanda.si_settings.set_expected_damage(
            {"ntr_no_topic_manifest", "missing_segments"})

        workload_topic = TopicSpec(partition_count=32)

        self.client().create_topic(workload_topic)

        producer = self.start_producer(workload_topic.name)

        tl_topic_name = workload_topic.name if transfer_leadership else None
        with self.tl_thread(tl_topic_name):
            workload_ns_topic = make_namespaced_topic(workload_topic.name)
            out_migration = OutboundDataMigration(topics=[workload_ns_topic],
                                                  consumer_groups=[])
            out_migration_id = self.create_and_wait(out_migration)

            self.admin.execute_data_migration_action(out_migration_id,
                                                     MigrationAction.prepare)
            if cancellation == CancellationStage('out', 'preparing'):
                self.wait_for_migration_states(out_migration_id,
                                               ['preparing', 'prepared'])
                return self.cancel_outbound(out_migration_id,
                                            workload_topic.name, producer)

            self.validate_topic_access(topic=workload_topic.name,
                                       metadata_locked=True,
                                       read_blocked=False,
                                       produce_blocked=False)

            self.wait_for_migration_states(out_migration_id, ['prepared'])

            self.validate_topic_access(topic=workload_topic.name,
                                       metadata_locked=True,
                                       read_blocked=False,
                                       produce_blocked=False)
            if cancellation == CancellationStage('out', 'prepared'):
                return self.cancel_outbound(out_migration_id,
                                            workload_topic.name, producer)

            self.admin.execute_data_migration_action(out_migration_id,
                                                     MigrationAction.execute)
            if cancellation == CancellationStage('out', 'executing'):
                self.wait_for_migration_states(out_migration_id,
                                               ['executing', 'executed'])
                return self.cancel_outbound(out_migration_id,
                                            workload_topic.name, producer)

            self.validate_topic_access(topic=workload_topic.name,
                                       metadata_locked=True,
                                       read_blocked=False,
                                       produce_blocked=True)

            self.wait_for_migration_states(out_migration_id, ['executed'])
            self.validate_topic_access(topic=workload_topic.name,
                                       metadata_locked=True,
                                       read_blocked=False,
                                       produce_blocked=True)
            if cancellation == CancellationStage('out', 'executed'):
                return self.cancel_outbound(out_migration_id,
                                            workload_topic.name, producer)

            self.admin.execute_data_migration_action(out_migration_id,
                                                     MigrationAction.finish)

            self.validate_topic_access(topic=workload_topic.name,
                                       metadata_locked=True,
                                       read_blocked=True,
                                       produce_blocked=True,
                                       assert_topic_present=False)

            self.wait_for_migration_states(out_migration_id,
                                           ['cut_over', 'finished'])

            producer.stop_if_running()

            self.assert_no_topics()

            self.wait_for_migration_states(out_migration_id, ['finished'])
            self.validate_topic_access(topic=workload_topic.name,
                                       metadata_locked=True,
                                       read_blocked=True,
                                       produce_blocked=True,
                                       assert_topic_present=False)

            self.admin.delete_data_migration(out_migration_id)

        # attach topic back
        inbound_topic_name = "aliased-workload-topic" if params.use_alias else workload_topic.name
        alias = None
        if params.use_alias:
            alias = make_namespaced_topic(topic=inbound_topic_name)

        tl_topic_name = inbound_topic_name if transfer_leadership else None
        with self.tl_thread(tl_topic_name):
            remounted = False
            # two cycles max: to cancel halfway and to complete + check e2e
            while not remounted:
                in_migration = InboundDataMigration(topics=[
                    InboundTopic(source_topic_reference=workload_ns_topic,
                                 alias=alias)
                ],
                                                    consumer_groups=[])
                in_migration_id = self.create_and_wait(in_migration)

                # check if topic that is being migrated can not be created even if
                # migration has not yet been prepared
                self._do_validate_topic_operation(
                    inbound_topic_name,
                    "creation",
                    expected_to_pass=False,
                    operation=lambda topic: rpk.create_topic(topic=topic,
                                                             replicas=3))
                self.admin.execute_data_migration_action(
                    in_migration_id, MigrationAction.prepare)

                if cancellation == CancellationStage('in', 'preparing'):
                    cancellation = None
                    self.wait_for_migration_states(in_migration_id,
                                                   ['preparing', 'prepared'])
                    self.cancel_inbound(in_migration_id, inbound_topic_name)
                    continue

                self.validate_topic_access(topic=inbound_topic_name,
                                           metadata_locked=True,
                                           read_blocked=True,
                                           produce_blocked=True,
                                           assert_topic_present=False)

                self.wait_for_migration_states(in_migration_id, ['prepared'])

                self.validate_topic_access(topic=inbound_topic_name,
                                           metadata_locked=True,
                                           read_blocked=True,
                                           produce_blocked=True)

                if cancellation == CancellationStage('in', 'prepared'):
                    cancellation = None
                    self.cancel_inbound(in_migration_id, inbound_topic_name)
                    continue

                topics = list(rpk.list_topics())
                self.logger.info(
                    f"topic list after inbound migration is prepared: {topics}"
                )
                assert inbound_topic_name in topics, "workload topic should be present after the inbound migration is prepared"

                self.admin.execute_data_migration_action(
                    in_migration_id, MigrationAction.execute)
                if cancellation == CancellationStage('in', 'executing'):
                    cancellation = None
                    self.wait_for_migration_states(in_migration_id,
                                                   ['executing', 'executed'])
                    self.cancel_inbound(in_migration_id, inbound_topic_name)
                    continue

                self.validate_topic_access(topic=inbound_topic_name,
                                           metadata_locked=True,
                                           read_blocked=True,
                                           produce_blocked=True)

                self.wait_for_migration_states(in_migration_id, ['executed'])

                self.validate_topic_access(topic=inbound_topic_name,
                                           metadata_locked=True,
                                           read_blocked=True,
                                           produce_blocked=True)
                if cancellation == CancellationStage('in', 'executed'):
                    cancellation = None
                    self.cancel_inbound(in_migration_id, inbound_topic_name)
                    continue

                self.admin.execute_data_migration_action(
                    in_migration_id, MigrationAction.finish)

                self.wait_for_migration_states(in_migration_id, ['finished'])
                self.admin.delete_data_migration(in_migration_id)
                # now the topic should be fully operational
                self.consume_and_validate(inbound_topic_name,
                                          producer.acked_records)
                self.validate_topic_access(topic=inbound_topic_name,
                                           metadata_locked=False,
                                           read_blocked=False,
                                           produce_blocked=False)
                remounted = True

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    def test_list_mountable_topics(self):
        topics = [TopicSpec(partition_count=3) for i in range(5)]

        for t in topics:
            self.client().create_topic(t)

        admin = Admin(self.redpanda)
        list_mountable_res = admin.list_mountable_topics().json()
        assert len(list_mountable_res["topics"]
                   ) == 0, "There should be no mountable topics"

        outbound_topics = [make_namespaced_topic(t.name) for t in topics]
        reply = self.admin.unmount_topics(outbound_topics).json()
        self.logger.info(f"create migration reply: {reply}")

        self.logger.info('waiting for partitions be deleted')
        self.wait_partitions_disappear(topics)

        list_mountable_res = admin.list_mountable_topics().json()
        assert len(list_mountable_res["topics"]) == len(
            topics), "There should be mountable topics"

        initial_names = [t.name for t in topics]
        mountable_topic_names = [
            t["topic"] for t in list_mountable_res["topics"]
        ]
        assert set(initial_names) == set(mountable_topic_names), \
            f"Initial topics: {initial_names} should match mountable topics: {mountable_topic_names}"

        for t in list_mountable_res["topics"]:
            assert t["ns"] == "kafka", f"namespace is not set correctly: {t}"
            assert t["topic_location"] != t["topic"] and "/" in t[
                "topic_location"], f"topic location is not set correctly: {t}"

        # Mount 3 topics based on the mountable topics response. This ensures
        # that the response is correct/usable.
        # The first 2 are mounted by name, the third by location.
        inbound_topics = [
            InboundTopic(NamespacedTopic(topic=t["topic"], namespace=t["ns"]))
            for t in list_mountable_res["topics"][:2]
        ] + [
            InboundTopic(
                NamespacedTopic(
                    topic=list_mountable_res["topics"][2]["topic_location"],
                    namespace=list_mountable_res["topics"][2]["ns"]))
        ]
        mount_resp = self.admin.mount_topics(inbound_topics).json()

        # Build expectations based on original topic specs that match the
        # mountable topics response.
        expected_topic_specs = []
        for t in list_mountable_res["topics"][:3]:
            expected_topic_specs.append(
                TopicSpec(name=t["topic"], partition_count=3))

        self.wait_partitions_appear(expected_topic_specs)

        # Wait for the migration to complete. This guarantees that the mount manifests
        # are deleted.
        self.wait_migration_disappear(mount_resp["id"])

        list_mountable_res = admin.list_mountable_topics().json()
        assert len(list_mountable_res["topics"]
                   ) == 2, "There should be 2 mountable topics"
