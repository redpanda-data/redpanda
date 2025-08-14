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
from enum import Enum
from typing import Callable, Literal, List, TypedDict, get_type_hints
import typing
from contextlib import contextmanager, nullcontext

from rptest.services.admin import Admin, MigrationAction
from rptest.services.admin import OutboundDataMigration, InboundDataMigration, NamespacedTopic, InboundTopic

import confluent_kafka as ck

from ducktape.mark import matrix, ignore
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService, RedpandaServiceBase, SISettings, make_redpanda_service
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.tests.e2e_finjector import Finjector
from rptest.clients.rpk import RpkTool, RpkException
from rptest.util import bg_thread_cm
from rptest.utils.data_migrations import DataMigrationTestMixin
import requests
import re

MIGRATION_LOG_ALLOW_LIST = [
    'Error during log recovery: cloud_storage::missing_partition_exception',
    'skipping group metadata, group is blocked'
] + Finjector.LOG_ALLOW_LIST


def make_namespaced_topic(topic: str) -> NamespacedTopic:
    return NamespacedTopic(topic, random.choice([None, "kafka"]))


def now():
    return int(time.time() * 1000)


@bg_thread_cm
def TransferLeadersBackgroundThread(redpanda: RedpandaServiceBase, topic: str):
    logger = redpanda.logger
    admin = Admin(redpanda, retry_codes=[503, 504])
    while (yield):
        p_id = None
        try:
            partitions = admin.get_partitions(namespace="kafka", topic=topic)
            partition = random.choice(partitions)
            p_id = partition['partition_id']
            logger.info(f"Transferring leadership of {topic}/{p_id}")
            admin.partition_transfer_leadership(namespace="kafka",
                                                topic=topic,
                                                partition=p_id)
        except Exception as e:
            logger.info(
                f"error transferring leadership of {topic}/{p_id} - {e}")


class CancellationStage(TypedDict):
    dir: Literal['in', 'out']
    stage: Literal['preparing', 'prepared', 'executing', 'executed']


class TmtpdiParams(TypedDict):
    """parameters for test_migrated_topic_data_integrity"""
    cancellation: CancellationStage | None
    use_alias: bool


def TypedDictMemberOptions(cls, member):
    return typing.get_args(get_type_hints(cls)[member])


def generate_tmptpdi_params() -> List[TmtpdiParams]:
    cancellation_stages = [
        CancellationStage(dir=dir, stage=stage)
        for dir in TypedDictMemberOptions(CancellationStage, 'dir')
        for stage in TypedDictMemberOptions(CancellationStage, 'stage')
    ]
    return [
        TmtpdiParams(cancellation=cancellation, use_alias=use_alias)
        for cancellation in [None] + cancellation_stages
        for use_alias in (True, False)
        # alias only affects inbound, pointless to vary if cancel earlier
        if not use_alias or cancellation is None or cancellation['dir'] == 'in'
    ]


class DataMigrationsApiTest(RedpandaTest, DataMigrationTestMixin):
    log_segment_size = 10 * 1024

    def __init__(self, test_context: TestContext, *args, **kwargs):
        kwargs['si_settings'] = SISettings(
            test_context=test_context,
            log_segment_size=self.log_segment_size,
            cloud_storage_max_connections=5,
            cloud_storage_enable_remote_read=True,
            cloud_storage_enable_remote_write=True,
        )
        RedpandaTest.__init__(self, test_context=test_context, *args, **kwargs)
        self.flaky_admin = Admin(self.redpanda, retry_codes=[503, 504])
        self.admin = Admin(self.redpanda)
        self.last_producer_id = 0
        self.last_consumer_id = 0

    def validate_timing(self, time_before, happened_at):
        time_now = now()
        self.logger.debug(f"{time_before=}, {happened_at=}, {time_now=}")
        err_ms = 5  # allow for ntp error across nodes
        assert time_before - err_ms <= happened_at <= time_now + err_ms

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
    def ck_consumer(self, group=None):
        if group is None:
            self.last_consumer_id += 1
            group = f'group-{self.last_consumer_id}'
        consumer = ck.Consumer({
            'debug': 'all',
            'log_level': 7,
            'logger': self.logger,
            'session.timeout.ms': 6000,
            'group.id': group,
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
        self.logger.info("switching to flaky admin")
        old_admin = self.admin
        try:
            self.admin = self.flaky_admin
            with other_cm:
                yield
        finally:
            self.logger.info("switching to non-flaky admin")
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

    def wait_migration_disappear(self, migration_id):
        def migration_is_absent(id: int):
            return self.on_all_live_nodes(id, lambda m: m is None)

        wait_until(
            lambda: migration_is_absent(migration_id),
            timeout_sec=90,
            backoff_sec=2,
            err_msg=f"Expected migration with id {migration_id} is absent")

    def assure_not_migratable(self, topic: TopicSpec, expected_response=None):
        out_migration = OutboundDataMigration(
            [make_namespaced_topic(topic.name)], consumer_groups=[])
        try:
            self.create_and_wait(out_migration)
            assert False
        except requests.exceptions.HTTPError as e:
            if expected_response is not None:
                assert e.response.json() == expected_response

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    def test_listing_inexistent_migration(self):
        assert self.get_migration(42) is None

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    def test_outbound_missing_topic(self):
        topic = TopicSpec(partition_count=3)
        self.assure_not_migratable(topic, {
            "message": "Topic does not exists",
            "code": 400
        })

    @cluster(
        num_nodes=3,
        log_allow_list=MIGRATION_LOG_ALLOW_LIST + [
            "Requested operation can not be executed as the resource is undergoing data migration"
        ])
    def test_conflicting_migrations(self):
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)
        self.wait_partitions_appear([topic])
        out1 = OutboundDataMigration([make_namespaced_topic(topic.name)],
                                     consumer_groups=[])
        self.create_and_wait(out1)
        self.assure_not_migratable(
            topic, {
                "message":
                "Unexpected cluster error: Requested operation can not be executed as the resource is undergoing data migration",
                "code": 500
            })

    @cluster(num_nodes=3,
             log_allow_list=MIGRATION_LOG_ALLOW_LIST +
             ["The topic has already been created"])
    def test_inbound_existing_topic(self):
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)
        self.wait_partitions_appear([topic])
        in_migration = InboundDataMigration(
            [InboundTopic(make_namespaced_topic(topic.name))],
            consumer_groups=[])
        try:
            self.create_and_wait(in_migration)
            assert False
        except requests.exceptions.HTTPError as e:
            assert e.response.json() == {
                "message":
                "Unexpected cluster error: The topic has already been created",
                "code": 500
            }

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    def test_creating_with_topic_no_remote_writes(self):
        self.redpanda.set_cluster_config(
            {"cloud_storage_enable_remote_write": False}, expect_restart=True)
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)
        self.wait_partitions_appear([topic])
        self.redpanda.set_cluster_config(
            {"cloud_storage_enable_remote_write": True}, expect_restart=True)
        self.assure_not_migratable(
            topic, {
                "message":
                "Data migration contains resources that are not eligible",
                "code": 400
            })

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    def test_creating_with_topic_wrong_namespace(self):
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)
        self.wait_partitions_appear([topic])
        out_migration = OutboundDataMigration(
            [NamespacedTopic(topic.name, "bad_namespace")], consumer_groups=[])
        try:
            self.create_and_wait(out_migration)
            assert False
        except requests.exceptions.HTTPError as e:
            assert e.response.json() == {
                "message":
                "Data migration contains resources that are not eligible",
                "code": 400
            }

    @cluster(
        num_nodes=3,
        log_allow_list=MIGRATION_LOG_ALLOW_LIST + [
            r'/v1/migrations.*Requested feature is disabled',  # cloud storage disabled
        ])
    def test_creating_when_cluster_misconfigured1(self):
        self.creating_when_cluster_misconfigured(
            "cloud_storage_enabled", {
                "message":
                "Unexpected cluster error: Requested feature is disabled",
                "code": 500
            })

    @cluster(
        num_nodes=3,
        log_allow_list=MIGRATION_LOG_ALLOW_LIST + [
            r'/v1/migrations.*Requested feature is disabled',  # cloud storage disabled
            'archival'  # a variety of archival errors is observed
        ])
    def test_creating_when_cluster_misconfigured2(self):
        self.creating_when_cluster_misconfigured(
            "cloud_storage_disable_archiver_manager", {
                "message": "Data migrations are disabled for this cluster",
                "code": 400
            })

    def creating_when_cluster_misconfigured(self, param_to_disable,
                                            expected_error):
        self.redpanda.set_cluster_config({param_to_disable: False},
                                         expect_restart=True)
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)
        self.assure_not_migratable(topic, expected_error)
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

    @cluster(num_nodes=3,
             log_allow_list=MIGRATION_LOG_ALLOW_LIST +
             ["Labeled topic manifest download resulted in listing error"])
    def test_mount_inexistent(self):
        topic = TopicSpec(partition_count=3)

        with self.finj_thread():
            in_migration = InboundDataMigration(
                topics=[InboundTopic(make_namespaced_topic(topic.name))],
                consumer_groups=[])
            in_migration_id = self.create_and_wait(in_migration)
            self.check_migrations(in_migration_id, 1, 1)

            self.execute_data_migration_action_flaky(in_migration_id,
                                                     MigrationAction.prepare)
            self.wait_for_migration_states(in_migration_id, ['preparing'])
            time.sleep(10)
            # still preparing, i.e. stuck
            self.wait_for_migration_states(in_migration_id, ['preparing'])
            # and the topic is not there
            self.wait_partitions_disappear([topic.name])

            time_before_final_action = now()
            self.execute_data_migration_action_flaky(in_migration_id,
                                                     MigrationAction.cancel)
            self.wait_for_migration_states(in_migration_id,
                                           ['canceling', 'cancelled'],
                                           time_before_final_action)
            self.wait_for_migration_states(in_migration_id, ['cancelled'],
                                           time_before_final_action)
            # still not there
            self.wait_partitions_disappear([topic.name])

            self.admin.delete_data_migration(in_migration_id)
            self.wait_migration_disappear(in_migration_id)

    def toggle_license(self, on: bool):
        ENV_KEY = '__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE'
        if on:
            self.redpanda.unset_environment([ENV_KEY])
        else:
            self.redpanda.set_environment({ENV_KEY: '1'})
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                            use_maintenance_mode=False)

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    def test_creating_and_listing_migrations(self):
        self.do_test_creating_and_listing_migrations(False)

    @cluster(
        num_nodes=3,
        log_allow_list=MIGRATION_LOG_ALLOW_LIST + [
            # license violation
            r'/v1/migrations.*Requested feature is disabled',
        ])
    def test_creating_and_listing_migrations_wo_license(self):
        self.do_test_creating_and_listing_migrations(True)

    def do_test_creating_and_listing_migrations(self, try_wo_license: bool):
        topics = [TopicSpec(partition_count=3) for i in range(5)]

        for t in topics:
            self.client().create_topic(t)

        migrations_map = self.get_migrations_map()
        assert len(migrations_map) == 0, "There should be no data migrations"

        if try_wo_license:
            time.sleep(2)  # make sure test harness can see Redpanda is live
            self.toggle_license(on=False)
            self.assure_not_migratable(
                topics[0], {
                    "message":
                    "Unexpected cluster error: Requested feature is disabled",
                    "code": 500
                })
            self.toggle_license(on=True)

        with nullcontext() if try_wo_license else self.finj_thread():
            # out
            outbound_topics = [make_namespaced_topic(t.name) for t in topics]
            out_migration = OutboundDataMigration(outbound_topics,
                                                  consumer_groups=[])
            out_migration_id = self.create_and_wait(out_migration)

            if try_wo_license:
                self.toggle_license(on=False)

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

            self.wait_partitions_disappear([t.name for t in topics])

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
            self.logger.info(f'{try_wo_license=}')
            if try_wo_license:
                self.toggle_license(on=True)
            in_migration_id = self.create_and_wait(in_migration)
            if try_wo_license:
                self.toggle_license(on=False)
            self.check_migrations(in_migration_id, len(inbound_topics), 2)

            self.log_topics(t.source_topic_reference.topic
                            for t in inbound_topics)

            self.execute_data_migration_action_flaky(in_migration_id,
                                                     MigrationAction.prepare)
            if try_wo_license:
                self.wait_for_migration_states(in_migration_id, ['preparing'])
                time.sleep(5)
                # stuck as a topic cannot be created without license
                self.wait_for_migration_states(in_migration_id, ['preparing'])
                self.toggle_license(on=True)
                self.wait_for_migration_states(in_migration_id, ['prepared'])
                self.toggle_license(on=False)
            else:
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
            self.wait_partitions_disappear([topic.name])
            self.wait_migration_disappear(out_migr_id)

        # mount and consume from them in random order
        cluster_uuid = self.admin.get_cluster_uuid(self.redpanda.nodes[0])
        for i in sorted(range(3), key=lambda element: random.random()):
            # TODO: the proper way to get the source_topic_reference is to use the info
            # provided by outbound migration json.
            source_topic_ref = f"{ns_topic.topic}/{cluster_uuid}/{revisions[i]}"
            in_topic = InboundTopic(make_namespaced_topic(source_topic_ref))
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

        # out
        outbound_topics = [make_namespaced_topic(t.name) for t in topics]
        reply = self.admin.unmount_topics(outbound_topics).json()
        self.logger.info(f"create migration reply: {reply}")
        out_migration_id = reply["id"]
        with self.finj_thread():
            self.logger.info('waiting for partitions be deleted')
            self.wait_partitions_disappear([t.name for t in topics])
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
        with self.finj_thread():
            self.logger.info('waiting for partitions to come back')
            self.wait_partitions_appear(inbound_topics_spec)
            self.logger.info('waiting for migration to be deleted')
            self.wait_migration_disappear(in_migration_id)
            self.log_topics(t.name for t in topics)
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

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                self.stop_if_running()

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

    def _do_validate_operation(self, topic: str | None, group: str | None,
                               op_name: str, expected_to_pass: bool | None,
                               operation: Callable[[str | None, str | None],
                                                   typing.Any]):
        if expected_to_pass is None:
            return

        self.logger.info(f"Validating execution of {op_name}"
                         f" against {topic=} {group=}")
        try:
            result = operation(topic, group)
        except Exception as e:
            self.logger.info(f"Operation {op_name} executed against"
                             f" {topic=} {group=} failed - {e}")
            self.logger.exception(e)
            result = False  # assume real op cannot return False
        success = result is not False

        assert expected_to_pass == success, f"Operation {op_name} outcome is not " \
            f"expected. {expected_to_pass=}, {success=}, {result=}"

    def validate_entities_access(self, entities: dict[str, str | None],
                                 expect_present: bool | None,
                                 expect_metadata_changeable: bool | None,
                                 expect_readable: bool | None,
                                 expect_writable: bool | None):
        rpk = RpkTool(self.redpanda)

        topic = entities['topic']
        group = entities['group']

        if expect_present is not None:
            assert expect_present == (topic in rpk.list_topics()), \
                f"validated topic {topic} must be present"

        self._do_validate_operation(
            **entities,
            op_name="add_partitions",
            expected_to_pass=expect_metadata_changeable,
            operation=lambda topic, _: rpk.add_partitions(topic, 33))

        def _alter_cfg(topic, _):
            rpk.alter_topic_config(topic, TopicSpec.PROPERTY_FLUSH_MS, 2000)
            rpk.delete_topic_config(topic, TopicSpec.PROPERTY_FLUSH_MS)

        self._do_validate_operation(
            **entities,
            op_name="alter_topic_configuration",
            expected_to_pass=expect_metadata_changeable,
            operation=_alter_cfg)

        self._do_validate_operation(
            **entities,
            op_name="read_without_group",
            expected_to_pass=expect_readable,
            operation=lambda topic, _: rpk.consume(topic=topic, n=1, offset=0))

        if group is not None:

            def read_with_group(topic, group):
                while True:
                    try:
                        with self.ck_consumer(group) as consumer:
                            consumer.subscribe([topic])
                            msg = consumer.poll(20)
                            if msg is None or msg.error() is not None:
                                raise ck.KafkaException(
                                    f"Failed to read from topic {topic} with group {group}: {msg and msg.error()}"
                                )
                        break
                    except ck.KafkaException as e:
                        if "Failed to fetch committed offsets for 0 partition" in str(
                                e.args[0]):
                            self.logger.info(
                                f"Hit https://github.com/confluentinc/librdkafka/issues/4963 bug, retrying"
                            )
                            continue
                        raise

            self._do_validate_operation(**entities,
                                        op_name="read_with_group",
                                        expected_to_pass=expect_writable,
                                        operation=read_with_group)

        # check if topic is writable only if it is expected to be blocked not to disturb the verifiers.
        if expect_writable:
            expect_writable = None

        self._do_validate_operation(
            **entities,
            op_name="produce",
            expected_to_pass=expect_writable,
            operation=lambda topic, _: rpk.produce(
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
        time_before_final_action = now()
        self.admin.execute_data_migration_action(migration_id,
                                                 MigrationAction.cancel)
        self.wait_for_migration_states(migration_id, ['cancelled'],
                                       time_before_final_action)
        self.admin.delete_data_migration(migration_id)

    def assert_no_topics(self):
        rpk = RpkTool(self.redpanda)
        topics = list(rpk.list_topics())
        self.logger.info(f"topic list: {topics}")

        assert len(topics) == 0, \
            "outbound migration complete, inbound migration not complete " \
            "and not in progress, so the topic should be removed"

    def cancel_outbound(self, migration_id, entities, producer):
        topic_name = entities['topic']
        self.cancel(migration_id, topic_name)
        producer.stop_if_running()
        self.consume_and_validate(topic_name, producer.acked_records)
        self.validate_entities_access(entities=entities,
                                      expect_present=True,
                                      expect_metadata_changeable=True,
                                      expect_readable=True,
                                      expect_writable=True)

    def cancel_inbound(self, migration_id, topic_name):
        self.cancel(migration_id, topic_name)
        self.assert_no_topics()

    @bg_thread_cm
    def transaction_producer_thread(self, topic: str):
        producer = self.get_ck_producer(use_transactional=True)
        producer.init_transactions()
        while (yield):
            try:
                producer.begin_transaction()
                producer.produce(topic, key="key1", value="value1")
                producer.commit_transaction()
            except Exception as e:
                self.logger.info(f"error producing with a transaction: {e}")

    def ensure_no_inflight_transactions(self, topic_name, partition):
        producers = self.admin.get_producers_state(namespace="kafka",
                                                   topic=topic_name,
                                                   partition=partition)
        self.redpanda.logger.debug(f"{producers=}")
        for producer in producers.get('producers', []):
            assert 'transaction_begin_offset' not in producer, \
                "unexpected transaction in progress"

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    @matrix(transfer_leadership=[True])
    def test_transactions(self, transfer_leadership: bool):
        workload_topic = TopicSpec(partition_count=1)
        self.client().create_topic(workload_topic)
        tl_topic_name = workload_topic.name if transfer_leadership else None
        with self.tl_thread(tl_topic_name):
            with self.transaction_producer_thread(workload_topic.name):
                out_migration = OutboundDataMigration(
                    topics=[make_namespaced_topic(workload_topic.name)],
                    consumer_groups=[])
                out_migration_id = self.create_and_wait(out_migration)

                self.admin.execute_data_migration_action(
                    out_migration_id, MigrationAction.prepare)
                self.wait_for_migration_states(out_migration_id, ['prepared'])
                self.admin.execute_data_migration_action(
                    out_migration_id, MigrationAction.execute)
                self.wait_for_migration_states(out_migration_id, ['executed'])

                self.ensure_no_inflight_transactions(workload_topic.name, 0)

    @cluster(
        num_nodes=4,
        log_allow_list=MIGRATION_LOG_ALLOW_LIST + [
            # dropping a topic while transferring its leadership
            '/transfer_leadership\] reason - seastar::abort_requested_exception',
            '/transfer_leadership\] reason - seastar::broken_named_semaphore',
            '/transfer_leadership\] reason - seastar::gate_closed_exception',
        ])
    @matrix(transfer_leadership=[True, False],
            include_groups=[True, False],
            params=generate_tmptpdi_params())
    def test_migrated_topic_data_integrity(self, include_groups: bool,
                                           transfer_leadership: bool,
                                           params: TmtpdiParams):
        cancellation = params['cancellation']
        use_alias = params['use_alias']
        rpk = RpkTool(self.redpanda)
        self.redpanda.si_settings.set_expected_damage(
            {"ntr_no_topic_manifest", "missing_segments"})

        workload_topic = TopicSpec(partition_count=32)

        self.client().create_topic(workload_topic)

        with self.start_producer(workload_topic.name) as producer:
            group = "consumer-group-id" if include_groups else None
            groups = [group] if group else []
            entities = {
                'topic': workload_topic.name,
                'group': group,
            }

            tl_topic_name = workload_topic.name if transfer_leadership else None
            with self.tl_thread(tl_topic_name):
                workload_ns_topic = make_namespaced_topic(workload_topic.name)
                out_migration = OutboundDataMigration(
                    topics=[workload_ns_topic], consumer_groups=groups)
                out_migration_id = self.create_and_wait(out_migration)

                self.admin.execute_data_migration_action(
                    out_migration_id, MigrationAction.prepare)
                if cancellation == CancellationStage(dir='out',
                                                     stage='preparing'):
                    self.wait_for_migration_states(out_migration_id,
                                                   ['preparing', 'prepared'])
                    return self.cancel_outbound(out_migration_id, entities,
                                                producer)

                self.validate_entities_access(entities=entities,
                                              expect_present=True,
                                              expect_metadata_changeable=False,
                                              expect_readable=True,
                                              expect_writable=True)

                self.wait_for_migration_states(out_migration_id, ['prepared'])

                self.validate_entities_access(entities=entities,
                                              expect_present=True,
                                              expect_metadata_changeable=False,
                                              expect_readable=True,
                                              expect_writable=True)
                if cancellation == CancellationStage(dir='out',
                                                     stage='prepared'):
                    return self.cancel_outbound(out_migration_id, entities,
                                                producer)

                self.admin.execute_data_migration_action(
                    out_migration_id, MigrationAction.execute)
                if cancellation == CancellationStage(dir='out',
                                                     stage='executing'):
                    self.wait_for_migration_states(out_migration_id,
                                                   ['executing', 'executed'])
                    return self.cancel_outbound(out_migration_id, entities,
                                                producer)

                self.validate_entities_access(entities=entities,
                                              expect_present=True,
                                              expect_metadata_changeable=False,
                                              expect_readable=True,
                                              expect_writable=None)

                self.wait_for_migration_states(out_migration_id, ['executed'])
                self.validate_entities_access(entities=entities,
                                              expect_present=True,
                                              expect_metadata_changeable=False,
                                              expect_readable=True,
                                              expect_writable=False)
                if cancellation == CancellationStage(dir='out',
                                                     stage='executed'):
                    return self.cancel_outbound(out_migration_id, entities,
                                                producer)

                time_before_final_action = now()
                self.admin.execute_data_migration_action(
                    out_migration_id, MigrationAction.finish)

                self.validate_entities_access(entities=entities,
                                              expect_present=None,
                                              expect_metadata_changeable=False,
                                              expect_readable=False,
                                              expect_writable=False)

                self.wait_for_migration_states(out_migration_id,
                                               ['cut_over', 'finished'],
                                               time_before_final_action)

                producer.stop_if_running()

                self.assert_no_topics()

                self.wait_for_migration_states(out_migration_id, ['finished'],
                                               time_before_final_action)
                self.validate_entities_access(entities=entities,
                                              expect_present=False,
                                              expect_metadata_changeable=False,
                                              expect_readable=False,
                                              expect_writable=False)
                self.admin.delete_data_migration(out_migration_id)
                self.validate_entities_access(entities=entities,
                                              expect_present=False,
                                              expect_metadata_changeable=False,
                                              expect_readable=False,
                                              expect_writable=False)

            # attach topic back
            if use_alias:
                inbound_topic_name = "aliased-workload-topic"
                alias = make_namespaced_topic(topic=inbound_topic_name)
            else:
                inbound_topic_name = workload_topic.name
                alias = None

            entities = {
                'topic': inbound_topic_name,
                'group': group,
            }

            tl_topic_name = inbound_topic_name if transfer_leadership else None
            with self.tl_thread(tl_topic_name):
                remounted = False
                # two cycles max: to cancel halfway and to complete + check e2e
                while not remounted:
                    in_migration = InboundDataMigration(
                        topics=[InboundTopic(workload_ns_topic, alias=alias)],
                        consumer_groups=groups)
                    in_migration_id = self.create_and_wait(in_migration)

                    # check if topic that is being migrated can not be created even if
                    # migration has not yet been prepared
                    self._do_validate_operation(
                        inbound_topic_name,
                        None,
                        "creation",
                        expected_to_pass=False,
                        operation=lambda topic, _: rpk.create_topic(
                            topic=topic, replicas=3))
                    self.admin.execute_data_migration_action(
                        in_migration_id, MigrationAction.prepare)

                    if cancellation == CancellationStage(dir='in',
                                                         stage='preparing'):
                        cancellation = None
                        self.wait_for_migration_states(
                            in_migration_id, ['preparing', 'prepared'])
                        self.cancel_inbound(in_migration_id,
                                            inbound_topic_name)
                        continue

                    self.validate_entities_access(
                        entities=entities,
                        expect_present=None,
                        expect_metadata_changeable=False,
                        expect_readable=False,
                        expect_writable=False)

                    self.wait_for_migration_states(in_migration_id,
                                                   ['prepared'])

                    self.validate_entities_access(
                        entities=entities,
                        expect_present=True,
                        expect_metadata_changeable=False,
                        expect_readable=False,
                        expect_writable=False)

                    if cancellation == CancellationStage(dir='in',
                                                         stage='prepared'):
                        cancellation = None
                        self.cancel_inbound(in_migration_id,
                                            inbound_topic_name)
                        continue

                    topics = list(rpk.list_topics())
                    self.logger.info(
                        f"topic list after inbound migration is prepared: {topics}"
                    )
                    assert inbound_topic_name in topics, "workload topic should be present after the inbound migration is prepared"

                    self.admin.execute_data_migration_action(
                        in_migration_id, MigrationAction.execute)
                    if cancellation == CancellationStage(dir='in',
                                                         stage='executing'):
                        cancellation = None
                        self.wait_for_migration_states(
                            in_migration_id, ['executing', 'executed'])
                        self.cancel_inbound(in_migration_id,
                                            inbound_topic_name)
                        continue

                    self.validate_entities_access(
                        entities=entities,
                        expect_present=True,
                        expect_metadata_changeable=False,
                        expect_readable=False,
                        expect_writable=False)

                    self.wait_for_migration_states(in_migration_id,
                                                   ['executed'])

                    self.validate_entities_access(
                        entities=entities,
                        expect_present=True,
                        expect_metadata_changeable=False,
                        expect_readable=False,
                        expect_writable=False)

                    if cancellation == CancellationStage(dir='in',
                                                         stage='executed'):
                        cancellation = None
                        self.cancel_inbound(in_migration_id,
                                            inbound_topic_name)
                        continue

                    time_before_final_action = now()
                    self.admin.execute_data_migration_action(
                        in_migration_id, MigrationAction.finish)

                    self.wait_for_migration_states(in_migration_id,
                                                   ['finished'],
                                                   time_before_final_action)
                    self.admin.delete_data_migration(in_migration_id)
                    # now the topic should be fully operational
                    self.consume_and_validate(inbound_topic_name,
                                              producer.acked_records)
                    remounted = True

            # rejoining a migrated group isn't stable with leadership transfers
            self.validate_entities_access(entities=entities,
                                          expect_present=True,
                                          expect_metadata_changeable=True,
                                          expect_readable=True,
                                          expect_writable=True)

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
        self.wait_partitions_disappear([t.name for t in topics])

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


class DataMigrationsMultiClusterTest(RedpandaTest, DataMigrationTestMixin):
    log_segment_size = 10 * 1024
    msg_size = 128

    def __init__(self, test_context: TestContext, *args, **kwargs):
        kwargs['si_settings'] = SISettings(
            test_context=test_context,
            log_segment_size=self.log_segment_size,
            cloud_storage_max_connections=5,
        )
        RedpandaTest.__init__(self, test_context=test_context, *args, **kwargs)
        self.producer = None
        self.extra_clusters = []

    @property
    def producer_throughput(self):
        return 16 * 1024 if self.debug_mode else 1024 * 1024

    def start_producer(self, topic, redpanda) -> None:
        assert self.producer is None
        self.producer = KgoVerifierProducer(
            context=self.test_context,
            redpanda=redpanda,
            topic=topic,
            msg_size=self.msg_size,
            msg_count=100_000_000,
            rate_limit_bps=self.producer_throughput,
            tolerate_failed_produce=True,
            trace_logs=True,
        )

        self.producer.start()
        self.producer.wait_for_acks(1000, timeout_sec=60, backoff_sec=2)

    def stop_producer(self) -> int:
        "return the number of acked messages"
        if self.producer is None:
            return

        self.producer.stop()
        acked = self.producer.produce_status.acked
        self.producer.free()
        self.logger.info(f"stopped producer, {acked=}")
        self.producer = None
        return acked

    def start_consumer(self, topic,
                       redpanda) -> KgoVerifierConsumerGroupConsumer:
        consumer = KgoVerifierConsumerGroupConsumer(self.test_context,
                                                    redpanda,
                                                    topic,
                                                    self.msg_size,
                                                    readers=3,
                                                    group_name="test-group",
                                                    trace_logs=True)

        consumer.start()
        return consumer

    def consume(self, topic, redpanda, msg_count) -> None:
        consumer = self.start_consumer(topic, redpanda)
        self.logger.info(f"expecting to consume {msg_count} messages")
        consumer.wait_total_reads(msg_count, timeout_sec=60, backoff_sec=1)
        consumer.stop()
        consumer.free()

    def start_extra_cluster(self, num_brokers=3):
        si_settings = SISettings(
            self.test_context,
            bypass_bucket_creation=True,
            log_segment_size=self.log_segment_size,
            cloud_storage_max_connections=5,
        )
        si_settings.reset_cloud_storage_bucket(
            self.si_settings.cloud_storage_bucket)

        cluster = make_redpanda_service(
            self.test_context,
            num_brokers=num_brokers,
            si_settings=si_settings,
        )
        self.extra_clusters.append(cluster)

        cluster.start()
        return cluster

    @cluster(num_nodes=10)
    def test_basic(self):
        """
        Test that basic functionality like producing and consuming works when we
        migrate topics between different clusters.
        """

        # After-test cloud storage integrity checker gets confused by topic aliases,
        # so disable it. TODO: support it properly in the checker.
        self.redpanda.si_settings.set_expected_damage(
            {"ntr_no_topic_manifest", "missing_segments"})

        n_partitions = 10
        topic_name = "foo"
        workload_topic = TopicSpec(name=topic_name,
                                   partition_count=n_partitions)
        workload_ns_topic = make_namespaced_topic(topic_name)

        alias_name = topic_name + "-alias"
        alias_ns_topic = make_namespaced_topic(alias_name)

        # To test various combinations of remote location and topic name being
        # same/different from local cluster uuid and topic name, we test the
        # following scenario:
        # foo (cluster 1) -> foo-alias (cluster 2) -> foo-alias (cluster 3)

        dest_redpanda = self.start_extra_cluster()

        self.client().create_topic(workload_topic)
        self.logger.info(f"created topic {workload_topic}")

        self.start_producer(workload_topic.name, self.redpanda)
        self.migrate_between_clusters([workload_ns_topic],
                                      self.redpanda,
                                      dest_redpanda,
                                      aliases=[alias_ns_topic])
        total_acked = self.stop_producer()

        def wait_for_offsets(redpanda, topic_name, expected):
            def predicate():
                rpk = RpkTool(redpanda)
                partitions = list(rpk.describe_topic(topic_name))
                if len(partitions) != n_partitions:
                    return False
                total = sum(p.high_watermark for p in partitions)
                self.logger.info(
                    f"topic {topic_name} offsets: {total=}, {expected=}")
                return total == expected

            wait_until(
                predicate,
                timeout_sec=15,
                backoff_sec=1,
                err_msg="timed out waiting for offsets of migrated topic")

        wait_for_offsets(dest_redpanda, alias_name, total_acked)

        self.logger.info("producing more to the second cluster")
        self.start_producer(alias_name, dest_redpanda)

        self.consume(alias_name,
                     redpanda=dest_redpanda,
                     msg_count=total_acked +
                     self.producer.produce_status.acked)

        another_redpanda = self.start_extra_cluster()
        self.migrate_between_clusters([alias_ns_topic], dest_redpanda,
                                      another_redpanda)

        total_acked += self.stop_producer()
        wait_for_offsets(another_redpanda, alias_name, total_acked)

        self.logger.info("producing more to the third cluster")
        self.start_producer(alias_name, another_redpanda)
        total_acked += self.stop_producer()

        self.consume(alias_name,
                     redpanda=another_redpanda,
                     msg_count=total_acked)
