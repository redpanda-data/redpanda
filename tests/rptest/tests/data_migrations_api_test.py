# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from typing import NamedTuple, List, Literal
import random
import typing
import threading
from rptest.services.admin import Admin, MigrationAction
from rptest.services.admin import OutboundDataMigration, InboundDataMigration, NamespacedTopic, InboundTopic
from rptest.services.redpanda import RedpandaServiceBase

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from ducktape.mark import matrix, parametrize, ok_to_fail
from contextlib import nullcontext
from rptest.tests.e2e_finjector import Finjector
from rptest.tests.data_migrations import DataMigrationsTest, make_namespaced_topic

import requests


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


class CancellationStage(NamedTuple):
    dir: Literal['in', 'out']
    stage: Literal['preparing', 'prepared', 'executing', 'executed']


class TmtpdiParams(NamedTuple):
    """parameters for test_migrated_topic_data_integrity"""
    cancellation: CancellationStage | None
    use_alias: bool


def generate_tmptpdi_params() -> List[TmtpdiParams]:
    cancellation_stages = [
        CancellationStage(dir, stage)
        for dir in typing.get_args(CancellationStage.dir)
        for stage in typing.get_args(CancellationStage.stage)
    ]
    return [
        TmtpdiParams(cancellation, use_alias)
        for cancellation in [None] + cancellation_stages
        for use_alias in (True, False)
        # alias only affects inbound, pointless to vary if cancel earlier
        if not use_alias or cancellation is None or cancellation.dir == 'in'
    ]


class DataMigrationsApiTest(DataMigrationsTest):
    def assure_not_migratable(self, topic: TopicSpec):
        out_migration = OutboundDataMigration(
            [make_namespaced_topic(topic.name)], consumer_groups=[])
        try:
            self.create_and_wait(out_migration)
            assert False
        except requests.exceptions.HTTPError as e:
            pass

    @cluster(num_nodes=3,
             log_allow_list=DataMigrationsTest.MIGRATION_LOG_ALLOW_LIST)
    def test_listing_inexistent_migration(self):
        try:
            self.get_migration(42)
        except Exception as e:
            # check 404
            self.logger.info("f{e}")
            raise

    @cluster(num_nodes=3,
             log_allow_list=DataMigrationsTest.MIGRATION_LOG_ALLOW_LIST)
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
        log_allow_list=DataMigrationsTest.MIGRATION_LOG_ALLOW_LIST + [
            r'/v1/migrations.*Requested feature is disabled',  # cloud storage disabled
        ])
    def test_creating_when_cluster_misconfigured1(self):
        self.creating_when_cluster_misconfigured("cloud_storage_enabled")

    @cluster(
        num_nodes=3,
        log_allow_list=DataMigrationsTest.MIGRATION_LOG_ALLOW_LIST + [
            r'/v1/migrations.*Requested feature is disabled',  # cloud storage disabled
            'archival'  # a variety of archival errors is observed
        ])
    def test_creating_when_cluster_misconfigured2(self):
        self.creating_when_cluster_misconfigured("cloud_storage_enabled")

    def creating_when_cluster_misconfigured(self, param_to_disable):
        self.redpanda.set_cluster_config({param_to_disable: False},
                                         expect_restart=True)
        topic = TopicSpec(partition_count=3)
        self.client().create_topic(topic)
        self.assure_not_migratable(topic)
        # for scrubbing to complete
        self.redpanda.set_cluster_config({param_to_disable: True},
                                         expect_restart=True)

    @cluster(num_nodes=3,
             log_allow_list=DataMigrationsTest.MIGRATION_LOG_ALLOW_LIST +
             Finjector.LOG_ALLOW_LIST)
    @parametrize(topics_count=5, partitions_count=3)
    def test_creating_and_listing_migrations(self, topics_count,
                                             partitions_count):
        self.do_test_creating_and_listing_migrations(topics_count,
                                                     partitions_count)

    @cluster(num_nodes=3,
             log_allow_list=DataMigrationsTest.MIGRATION_LOG_ALLOW_LIST +
             Finjector.LOG_ALLOW_LIST)
    def test_higher_level_migration_api(self):
        topics = [TopicSpec(partition_count=3) for i in range(5)]

        for t in topics:
            self.client().create_topic(t)

        with Finjector(self.redpanda, self.scale).finj_thread():
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
                TopicSpec(name=(it.alias or it.src_topic).topic,
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

        # todo: fix rp_storage_tool to use overridden topic names
        self.redpanda.si_settings.set_expected_damage(
            {"ntr_no_topic_manifest", "missing_segments"})

    @ok_to_fail
    @cluster(num_nodes=4,
             log_allow_list=DataMigrationsTest.MIGRATION_LOG_ALLOW_LIST +
             Finjector.LOG_ALLOW_LIST)
    @matrix(transfer_leadership=[True, False],
            params=generate_tmptpdi_params())
    def test_migrated_topic_data_integrity(self, transfer_leadership: bool,
                                           params: TmtpdiParams):
        with Finjector(self.redpanda, self.scale).finj_thread():
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
                workload_ns_topic = make_namespaced_topic(workload_topic.name)
                out_migration = OutboundDataMigration(
                    topics=[workload_ns_topic], consumer_groups=[])
                out_migration_id = self.create_and_wait(out_migration)

                self.execute_data_migration_action(out_migration_id,
                                                   MigrationAction.prepare)
                if params.cancellation == CancellationStage(
                        'out', 'preparing'):
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
                if params.cancellation == CancellationStage('out', 'prepared'):
                    return self.cancel_outbound(out_migration_id,
                                                workload_topic.name, producer)

                self.execute_data_migration_action(out_migration_id,
                                                   MigrationAction.execute)
                if params.cancellation == CancellationStage(
                        'out', 'executing'):
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
                if params.cancellation == CancellationStage('out', 'executed'):
                    return self.cancel_outbound(out_migration_id,
                                                workload_topic.name, producer)

                self.execute_data_migration_action(out_migration_id,
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
            inbound_topic_name = "aliased-workload-topic" if params.use_alias else workload_topic.name
            alias = None
            if params.use_alias:
                alias = make_namespaced_topic(topic=inbound_topic_name)

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
                    self.execute_data_migration_action(in_migration_id,
                                                       MigrationAction.prepare)

                    if params.cancellation == CancellationStage(
                            'in', 'preparing'):
                        cancellation = None
                        self.wait_for_migration_states(
                            in_migration_id, ['preparing', 'prepared'])
                        self.cancel_inbound(in_migration_id,
                                            inbound_topic_name)
                        continue

                    self.validate_topic_access(topic=inbound_topic_name,
                                               metadata_locked=True,
                                               read_blocked=True,
                                               produce_blocked=True,
                                               assert_topic_present=False)

                    self.wait_for_migration_states(in_migration_id,
                                                   ['prepared'])

                    self.validate_topic_access(topic=inbound_topic_name,
                                               metadata_locked=True,
                                               read_blocked=True,
                                               produce_blocked=True)

                    if params.cancellation == CancellationStage(
                            'in', 'prepared'):
                        cancellation = None
                        self.cancel_inbound(in_migration_id,
                                            inbound_topic_name)
                        continue

                    topics = list(rpk.list_topics())
                    self.logger.info(
                        f"topic list after inbound migration is prepared: {topics}"
                    )
                    assert inbound_topic_name in topics, "workload topic should be present after the inbound migration is prepared"

                    self.execute_data_migration_action(in_migration_id,
                                                       MigrationAction.execute)
                    if params.cancellation == CancellationStage(
                            'in', 'executing'):
                        cancellation = None
                        self.wait_for_migration_states(
                            in_migration_id, ['executing', 'executed'])
                        self.cancel_inbound(in_migration_id,
                                            inbound_topic_name)
                        continue

                    self.validate_topic_access(topic=inbound_topic_name,
                                               metadata_locked=True,
                                               read_blocked=True,
                                               produce_blocked=True)

                    self.wait_for_migration_states(in_migration_id,
                                                   ['executed'])

                    self.validate_topic_access(topic=inbound_topic_name,
                                               metadata_locked=True,
                                               read_blocked=True,
                                               produce_blocked=True)
                    if params.cancellation == CancellationStage(
                            'in', 'executed'):
                        cancellation = None
                        self.cancel_inbound(in_migration_id,
                                            inbound_topic_name)
                        continue

                    self.execute_data_migration_action(in_migration_id,
                                                       MigrationAction.finish)

                    self.wait_for_migration_states(in_migration_id,
                                                   ['finished'])
                    admin.delete_data_migration(in_migration_id)
                    # now the topic should be fully operational
                    self.consume_and_validate(inbound_topic_name,
                                              producer.acked_records)
                    self.validate_topic_access(topic=inbound_topic_name,
                                               metadata_locked=False,
                                               read_blocked=False,
                                               produce_blocked=False)
                    remounted = True
