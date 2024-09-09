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
from typing import Callable
import typing
from requests.exceptions import ConnectionError

from rptest.services.admin import Admin, MigrationAction
from rptest.services.admin import OutboundDataMigration, InboundDataMigration, NamespacedTopic, InboundTopic

from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService, RedpandaServiceBase, SISettings
from ducktape.utils.util import wait_until
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.tests.test import TestContext
from rptest.clients.types import TopicSpec
from rptest.tests.e2e_finjector import Finjector
from rptest.clients.rpk import RpkTool
from ducktape.mark import matrix
from contextlib import nullcontext
import requests

MIGRATION_LOG_ALLOW_LIST = [
    'Error during log recovery: cloud_storage::missing_partition_exception',
]


class TransferLeadersBackgroundThread:
    def __init__(self, redpanda: RedpandaServiceBase, topic: str):
        self.redpanda = redpanda
        self.logger = redpanda.logger
        self.stop_ev = threading.Event()
        self.topic = topic
        self.admin = Admin(self.redpanda)
        self.thread = threading.Thread(target=lambda: self._loop())
        self.thread.daemon = True

    def __enter__(self):
        self.thread.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop_ev.set()

    def _loop(self):
        while not self.stop_ev.is_set():
            partitions = self.admin.get_partitions(namespace="kafka",
                                                   topic=self.topic)
            partition = random.choice(partitions)
            p_id = partition['partition_id']
            self.logger.info(f"Transferring leadership of {self.topic}/{p_id}")
            try:
                self.admin.partition_transfer_leadership(namespace="kafka",
                                                         topic=self.topic,
                                                         partition=p_id)
            except Exception as e:
                self.logger.info(
                    f"error transferring leadership of {self.topic}/{p_id} - {e}"
                )


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

        def migration_is_present(id: int):
            return self.on_all_live_nodes(id, lambda m: True)

        wait_until(
            lambda: migration_is_present(migration_id), 30, 2,
            f"Expected migration with id {migration_id} is not present")
        return migration_id

    @cluster(num_nodes=3, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
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
                self.producer = KgoVerifierProducer(*args, **kwargs)
                self.producer.start(clean=False)
                wait_until( \
                    lambda: self.producer.produce_status.acked > msg_count,
                    timeout_sec=120,
                    backoff_sec=1)

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
                                                    group_name="test-group")

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
        wait_until(
            lambda: \
                consumer._status.validator.valid_reads >= expected_records,
            timeout_sec=180,
            backoff_sec=0.5,
            err_msg=
            f"Error waiting for consumer to see all {expected_records} produced messages",
        )
        consumer.wait()
        consumer.stop()
        #self.redpanda.si_settings.set_expected_damage(
        #    {"ntr_no_topic_manifest", "missing_segments"})

    def cancel(self, migration_id, topic_name):
        admin = Admin(self.redpanda)
        admin.execute_data_migration_action(migration_id,
                                            MigrationAction.cancel)
        self.logger.info('waiting for cancelled')
        self.wait_for_migration_states(migration_id, ['cancelled'])
        admin.delete_data_migration(migration_id)

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

    @cluster(num_nodes=4, log_allow_list=MIGRATION_LOG_ALLOW_LIST)
    @matrix(use_alias=[True, False],
            transfer_leadership=[True, False],
            cancellation=[None] +
            [(dir, stage) for dir in ('in', 'out')
             for stage in ('preparing', 'prepared', 'executing', 'executed')])
    def test_migrated_topic_data_integrity(self, use_alias,
                                           transfer_leadership, cancellation):
        rpk = RpkTool(self.redpanda)
        self.redpanda.si_settings.set_expected_damage(
            {"ntr_no_topic_manifest", "missing_segments"})

        workload_topic = TopicSpec(partition_count=32)

        self.client().create_topic(workload_topic)

        producer = self.start_producer(workload_topic.name)

        out_tl_thread = TransferLeadersBackgroundThread(
            self.redpanda,
            workload_topic.name) if transfer_leadership else nullcontext()
        with out_tl_thread:
            admin = Admin(self.redpanda)
            workload_ns_topic = NamespacedTopic(workload_topic.name)
            out_migration = OutboundDataMigration(topics=[workload_ns_topic],
                                                  consumer_groups=[])
            out_migration_id = self.create_and_wait(out_migration)

            admin.execute_data_migration_action(out_migration_id,
                                                MigrationAction.prepare)
            if cancellation == ('out', 'preparing'):
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
            if cancellation == ('out', 'prepared'):
                return self.cancel_outbound(out_migration_id,
                                            workload_topic.name, producer)

            admin.execute_data_migration_action(out_migration_id,
                                                MigrationAction.execute)
            if cancellation == ('out', 'executing'):
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
            if cancellation == ('out', 'executed'):
                return self.cancel_outbound(out_migration_id,
                                            workload_topic.name, producer)

            admin.execute_data_migration_action(out_migration_id,
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

            admin.delete_data_migration(out_migration_id)

        # attach topic back
        inbound_topic_name = "aliased-workload-topic" if use_alias else workload_topic.name
        alias = None
        if use_alias:
            alias = NamespacedTopic(topic=inbound_topic_name)

        in_tl_thread = TransferLeadersBackgroundThread(
            self.redpanda,
            inbound_topic_name) if transfer_leadership else nullcontext()
        with in_tl_thread:
            remounted = False
            # two cycles max: to cancel halfway and to complete + check e2e
            while not remounted:
                in_migration = InboundDataMigration(topics=[
                    InboundTopic(src_topic=workload_ns_topic, alias=alias)
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
                admin.execute_data_migration_action(in_migration_id,
                                                    MigrationAction.prepare)

                if cancellation == ('in', 'preparing'):
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

                if cancellation == ('in', 'prepared'):
                    cancellation = None
                    self.cancel_inbound(in_migration_id, inbound_topic_name)
                    continue

                topics = list(rpk.list_topics())
                self.logger.info(
                    f"topic list after inbound migration is prepared: {topics}"
                )
                assert inbound_topic_name in topics, "workload topic should be present after the inbound migration is prepared"

                admin.execute_data_migration_action(in_migration_id,
                                                    MigrationAction.execute)
                if cancellation == ('in', 'executing'):
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
                if cancellation == ('in', 'executed'):
                    cancellation = None
                    self.cancel_inbound(in_migration_id, inbound_topic_name)
                    continue

                admin.execute_data_migration_action(in_migration_id,
                                                    MigrationAction.finish)

                self.logger.info('waiting for finished')
                self.wait_for_migration_states(in_migration_id, ['finished'])
                admin.delete_data_migration(in_migration_id)
                # now the topic should be fully operational
                self.consume_and_validate(inbound_topic_name,
                                          producer.acked_records)
                self.validate_topic_access(topic=inbound_topic_name,
                                           metadata_locked=False,
                                           read_blocked=False,
                                           produce_blocked=False)
                remounted = True
