# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from concurrent.futures import ThreadPoolExecutor
import json
import time
import requests
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec

import random
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.redpanda_installer import RedpandaInstaller

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
import confluent_kafka as ck
from rptest.services.admin import Admin

from rptest.utils.mode_checks import skip_debug_mode
from ducktape.mark import matrix


class TxCoordinatorMigrationTest(RedpandaTest):
    def __init__(self, test_context):
        self.partition_count = 8
        self.initial_tx_manager_partitions = 4
        self.producer_count = 100
        super(TxCoordinatorMigrationTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf={
                                 'transaction_coordinator_partitions':
                                 self.initial_tx_manager_partitions
                             })

    def _tx_id(self, idx):
        return f"test-producer-{idx}"

    def _populate_tx_coordinator(self, topic):
        def delivery_callback(err, msg):
            if err:
                assert False, "failed to deliver message: %s" % err

        for i in range(self.producer_count):
            producer = ck.Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'transactional.id': self._tx_id(i),
            })
            producer.init_transactions()
            producer.begin_transaction()
            for m in range(random.randint(1, 50)):
                producer.produce(topic,
                                 f"p-{i}-key-{m}",
                                 f"p-{i}-value-{m}",
                                 random.randint(0, self.partition_count - 1),
                                 callback=delivery_callback)
            producer.commit_transaction()
            producer.flush()

    def _get_tx_id_mapping(self):
        mapping = {}
        admin = Admin(self.redpanda)
        for idx in range(self.producer_count):
            c = admin.find_tx_coordinator(self._tx_id(idx))
            mapping[self._tx_id(
                idx)] = f"{c['ntp']['topic']}/{c['ntp']['partition']}"

        return mapping

    def _get_all_txs(self):
        all_tx = Admin(self.redpanda).get_all_transactions()

        def process_tx(tx):
            tx_filtered = {}
            for k in ['transactional_id', 'pid', 'etag']:
                tx_filtered[k] = tx[k]

            return tx_filtered

        return [
            process_tx(tx)
            for tx in sorted(all_tx, key=lambda tx: tx['transactional_id'])
        ]

    def _get_tx_manager_topic_meta(self):
        """_summary_
            Collects partition metadata i.e. mapping between partition id and 
            raft group, this mapping changes when topic is deleted and recreated
        
        """
        metadata = Admin(self.redpanda).get_partitions(
            namespace="kafka_internal", topic="tx")

        return {(p['partition_id'], p['raft_group_id']) for p in metadata}

    def _migrate_until_success(self):
        migrator = self._random_node()

        def _is_finished():
            try:
                status = json.loads(
                    admin.get_tx_manager_recovery_status(migrator).text)
                self.logger.info(f"Migration status: {status}")
                return status['required'] == False and status[
                    'in_progress'] == False
            except Exception as e:
                return False

        admin = Admin(self.redpanda, retries_amount=0)
        finished = False
        cnt = 0
        max_failures = 5
        fi = FailureInjector(self.redpanda)

        with ThreadPoolExecutor(max_workers=1) as executor:
            start = time.time()
            while not finished:
                cnt += 1

                if time.time() > (start + 120):
                    self.logger.error(
                        f"error executing migration, giving up after {cnt} retires"
                    )
                    break

                try:
                    if cnt < max_failures:
                        executor.submit(lambda: fi.inject_failure(
                            FailureSpec(FailureSpec.FAILURE_KILL, migrator, 0))
                                        )
                    admin.migrate_tx_manager_in_recovery(migrator)
                    finished = _is_finished()
                    if not finished:
                        time.sleep(0.5)

                except Exception as e:
                    self.logger.info(f"Migration error: {e}")
                    time.sleep(0.5)

    def setUp(self):
        # defer starting redpanda
        pass

    def _start_redpanda_service(self, use_previous_version: bool):

        if not use_previous_version:
            self.redpanda.start()
            return

        installer = self.redpanda._installer
        previous_version = installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)
        installer.install(self.redpanda.nodes, previous_version)
        self.redpanda.start()

    def _assert_tx_lists_are_equal(self, expected_txs, new_txs):
        assert len(expected_txs) == len(
            new_txs
        ), f"different size of expected and new transactions lists (expected = {len(expected_txs)}, new = {len(new_txs)})"

        for expected, new_tx in zip(expected_txs, new_txs):
            assert expected == new_tx, \
                f"Tx mismatch. expected: {expected}, current_tx: {new_tx}"

    def _random_node(self):
        return random.choice(self.redpanda.nodes)

    @skip_debug_mode
    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(with_failures=[True, False], upgrade=[True, False])
    def test_migrating_tx_manager_coordinator(self, with_failures, upgrade):

        self._start_redpanda_service(use_previous_version=upgrade)

        admin = Admin(self.redpanda)
        topic = TopicSpec(partition_count=self.partition_count)
        self.client().create_topic(topic)

        # check that migration isn't possible when Redpanda is not running in recovery mode
        try:
            admin.migrate_tx_manager_in_recovery(self._random_node())
            assert False, "migrate tx manager endpoint is not supposed to be present in normal mode"
        except requests.exceptions.HTTPError as e:
            assert e.response.status_code == 404, "migrate tx manager endpoint is not supposed to be present in normal mode"

        self._populate_tx_coordinator(topic=topic.name)
        initial_mapping = self._get_tx_id_mapping()
        self.logger.info(f"Initial tx_id mapping {initial_mapping}")
        initial_metadata = self._get_tx_manager_topic_meta()
        self.logger.info(f"Initial topic metadata {initial_metadata}")

        initial_txs = self._get_all_txs()
        self.logger.info(f"Initial transactions: {initial_txs}")
        assert len(initial_txs) == self.producer_count

        if upgrade:
            installer = self.redpanda._installer
            installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        # verify state after upgrade
        upgraded_txs = self._get_all_txs()
        self.logger.info(f"Upgraded transactions: {initial_txs}")
        self._assert_tx_lists_are_equal(initial_txs, upgraded_txs)

        self.redpanda.restart_nodes(
            self.redpanda.nodes,
            override_cfg_params={"recovery_mode_enabled": True})

        # with the same number of partitions the operation should be a no op
        admin.migrate_tx_manager_in_recovery(self._random_node())
        metadata = self._get_tx_manager_topic_meta()

        assert initial_metadata == metadata, "when number of partitions is the same metadata should be identical"
        new_partition_count = 32
        # change number of partitions
        self.redpanda.set_cluster_config(
            {"transaction_coordinator_partitions": new_partition_count})

        if not with_failures:
            # try migrating partitions once again
            admin.migrate_tx_manager_in_recovery(self._random_node())
        else:
            self._migrate_until_success()

        new_tp_metadata = self._get_tx_manager_topic_meta()
        assert len(
            new_tp_metadata
        ) == new_partition_count, "Tx manager topic after migration is expected to have new number of partitions"

        # restart back in normal mode
        self.redpanda.restart_nodes(
            self.redpanda.nodes,
            override_cfg_params={"recovery_mode_enabled": False})

        new_mapping = self._get_tx_id_mapping()
        self.logger.info(f"New tx_id mapping {new_mapping}")
        migrated_txs = self._get_all_txs()

        used_partitions = set()
        for t_id, p in new_mapping.items():
            used_partitions.add(p)

        assert len(new_mapping) == len(
            initial_mapping
        ), f"All tx ids should be present after repartitioning"

        assert len(used_partitions) > self.initial_tx_manager_partitions
        self.logger.info(f"Migrated transactions: {migrated_txs}")
        self._assert_tx_lists_are_equal(initial_txs, migrated_txs)
