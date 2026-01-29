# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import random
import string
from enum import Enum
from threading import Lock, Semaphore, Thread
from time import sleep, time

import confluent_kafka as ck
from ducktape.errors import TimeoutError
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    RESTART_LOG_ALLOW_LIST,
    RedpandaService,
    MetricsEndpoint,
)
from rptest.services.redpanda_installer import (
    RedpandaInstaller,
    RedpandaVersionLine,
    RedpandaVersionTriple,
    wait_for_num_versions,
    ver_string,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.tests.log_compaction_test import LogCompactionTxRemovalMixin


class TxUpgradeTestBase(RedpandaTest):
    """
    Basic test verifying if mapping between transaction coordinator and transaction_id is preserved across the upgrades
    """

    def __init__(self, test_context, extra_rp_conf={}):
        super(TxUpgradeTestBase, self).__init__(
            test_context=test_context, num_brokers=3, extra_rp_conf=extra_rp_conf
        )
        self.partition_count = 10
        self.msg_sent = 0
        self.producers_count = 100
        self.transaction_timeout_ms = 10000
        self.installer = self.redpanda._installer

    def setUp(self):
        super(TxUpgradeTestBase, self).setUp()

    def _tx_id(self, idx):
        return f"test-producer-{idx}"

    def _populate_tx_coordinator(self, topic):
        def delivery_callback(err, msg):
            if err:
                assert False, "failed to deliver message: %s" % err

        for i in range(self.producers_count):
            producer = ck.Producer(
                {
                    "bootstrap.servers": self.redpanda.brokers(),
                    "transactional.id": self._tx_id(i),
                    "transaction.timeout.ms": self.transaction_timeout_ms,
                }
            )
            producer.init_transactions()
            producer.begin_transaction()
            for m in range(random.randint(1, 50)):
                producer.produce(
                    topic,
                    f"p-{i}-key-{m}",
                    f"p-{i}-value-{m}",
                    random.randint(0, self.partition_count - 1),
                    callback=delivery_callback,
                )
            producer.commit_transaction()
            producer.flush()

    def _produce_with_transactions(self, topic, retries=10, timeout_sec=300):
        deadline = time() + timeout_sec
        while retries > 0 and time() < deadline:
            retries -= 1
            try:
                self._populate_tx_coordinator(topic)
                break
            except Exception as e:
                self.logger.debug(
                    f"Caught exception {e} while trying to produce to topic {topic}. {retries} retries left."
                )
                pass
            sleep(1)
        else:
            assert False, f"Failed to produce to topic {topic}"

    def _get_tx_id_mapping(self):
        mapping = {}
        admin = Admin(self.redpanda)
        for idx in range(self.producers_count):
            c = admin.find_tx_coordinator(self._tx_id(idx))
            mapping[self._tx_id(idx)] = f"{c['ntp']['topic']}/{c['ntp']['partition']}"

        return mapping


class TxUpgradeTest(TxUpgradeTestBase):
    """
    Basic test verifying if mapping between transaction coordinator and transaction_id is preserved across the upgrades
    """

    def __init__(self, test_context):
        super(TxUpgradeTest, self).__init__(test_context=test_context)

    def setUp(self):
        self.old_version = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD
        )

        self.old_version_str = ver_string(self.old_version)
        self.installer.install(self.redpanda.nodes, self.old_version)
        super(TxUpgradeTest, self).setUp()

    @skip_debug_mode
    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def upgrade_does_not_change_tx_coordinator_assignment_test(self):
        topic = TopicSpec(partition_count=self.partition_count)
        self.client().create_topic(topic)

        self._populate_tx_coordinator(topic=topic.name)
        initial_mapping = self._get_tx_id_mapping()
        self.logger.info(f"Initial mapping {initial_mapping}")

        first_node = self.redpanda.nodes[0]
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str in unique_versions, unique_versions

        # Upgrade one node to the head version.
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes([first_node])
        unique_versions = wait_for_num_versions(self.redpanda, 2)
        assert self.old_version_str in unique_versions, unique_versions
        assert self._get_tx_id_mapping() == initial_mapping, (
            "Mapping changed after upgrading one of the nodes"
        )

        # verify if txs are handled correctly with mixed versions
        self._populate_tx_coordinator(topic.name)

        # Only once we upgrade the rest of the nodes do we converge on the new
        # version.
        self.redpanda.restart_nodes(self.redpanda.nodes)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str not in unique_versions, unique_versions
        assert self._get_tx_id_mapping() == initial_mapping, (
            "Mapping changed after full upgrade"
        )


class TxUpgradeCompactionTest(TxUpgradeTestBase, LogCompactionTxRemovalMixin):
    """
    Test validating interaction between compaction and transactions during rolling-restart upgrades (including mixed-version node cluster interaction)
    """

    def __init__(self, test_context):
        self.extra_rp_conf = {
            "log_compaction_interval_ms": 4000,
            "log_segment_size": 2 * 1024**2,  # 2 MiB
            "compacted_log_segment_size": 1024**2,  # 1 MiB
            # Trigger tombstone removal quickly
            "storage_target_replay_bytes": 100,
            "log_segment_ms": 60,
        }

        super(TxUpgradeCompactionTest, self).__init__(
            test_context=test_context, extra_rp_conf=self.extra_rp_conf
        )

        self.transaction_timeout_ms = 2000

    def setUp(self):
        # Version before `may_have_transactional_batches` was added.
        self.initial_version: RedpandaVersionTriple = self.installer.latest_for_line(
            RedpandaVersionLine((25, 1))
        )[0]
        self.installer.install(self.redpanda.nodes, self.initial_version)
        super(TxUpgradeCompactionTest, self).setUp()

    def get_complete_sliding_window_rounds(self):
        return self.redpanda.metric_sum(
            metric_name="vectorized_storage_log_complete_sliding_window_rounds_total",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name,
        )

    def wait_for_sliding_window_compaction(self):
        self.prev_sliding_window_rounds = None

        def compaction_has_completed():
            new_sliding_window_rounds = self.get_complete_sliding_window_rounds()
            res = self.prev_sliding_window_rounds == new_sliding_window_rounds
            self.prev_sliding_window_rounds = new_sliding_window_rounds
            return res

        wait_until(
            compaction_has_completed,
            timeout_sec=120,
            backoff_sec=self.extra_rp_conf["log_compaction_interval_ms"] / 1000 * 4,
            err_msg="Compaction did not stabilize.",
        )

    @skip_debug_mode
    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def upgrade_with_compaction_test(self):
        self.topic_spec = TopicSpec(
            partition_count=self.partition_count,
            delete_retention_ms=3000,
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
            min_cleanable_dirty_ratio=0.0,
        )
        self.client().create_topic(self.topic_spec)

        prev_version_str = ver_string(self.initial_version)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert prev_version_str in unique_versions, unique_versions

        for new_version in self.installer.upgrade_path_to_head(self.initial_version):
            self._produce_with_transactions(topic=self.topic_spec.name)
            initial_mapping = self._get_tx_id_mapping()
            self.logger.info(f"Initial mapping {initial_mapping}")

            first_node = self.redpanda.nodes[0]

            self.installer.install(self.redpanda.nodes, new_version)

            # Upgrade & restart one node to the new version.
            self.redpanda.restart_nodes([first_node])
            unique_versions = wait_for_num_versions(self.redpanda, 2)
            assert prev_version_str in unique_versions, unique_versions
            assert self._get_tx_id_mapping() == initial_mapping, (
                "Mapping changed after upgrading one of the nodes"
            )

            # verify if txs are handled correctly with mixed versions
            self._produce_with_transactions(topic=self.topic_spec.name)

            # Only once we upgrade the rest of the nodes do we converge on the new
            # version.
            self.redpanda.restart_nodes(self.redpanda.nodes)
            unique_versions = wait_for_num_versions(self.redpanda, 1)
            assert prev_version_str not in unique_versions, unique_versions
            assert self._get_tx_id_mapping() == initial_mapping, (
                "Mapping changed after full upgrade"
            )
            prev_version_str = ver_string(new_version)

        # Once we have upgraded to the newest version, enable tx batch removal.
        self.redpanda.set_cluster_config(
            {"log_compaction_tx_batch_removal_enabled": True}
        )

        # One last round of producing
        self._produce_with_transactions(topic=self.topic_spec.name)

        # Restart the redpanda broker to roll segments
        self.redpanda.restart_nodes(self.redpanda.nodes)

        self.wait_for_sliding_window_compaction()

        def produce_func():
            producer = ck.Producer({"bootstrap.servers": self.redpanda.brokers()})

            def random_string(n=5):
                return "".join(random.choice(string.ascii_letters) for _ in range(n))

            for i in range(0, 10000):
                producer.produce(
                    topic=self.topic_spec.name,
                    key=random_string(),
                    value=random_string(1024),
                )
            producer.flush()

        self.wait_for_all_tx_batches_removed(produce_func)


class TxUpgradeRevertTest(RedpandaTest):
    """Tests that the local snapshot is compatible after the upgrade is reverted"""

    class TxStateGenerator:
        """A traffic generating utility for transactions. Traffic can be paused and resumed as needed to see a consistent snapshot
        of the transactions and tally the state as seen by clients vs the brokers."""

        def __init__(
            self,
            num_producers: int,
            topic_name: str,
            num_partitions: int,
            redpanda: RedpandaService,
        ) -> None:
            self.num_producers = num_producers
            self.topic_name = topic_name
            self.tx_id_counter = 0
            self.redpanda = redpanda
            self.num_partitions = num_partitions
            self.tx_states = {}
            # Populate initial states
            for p in range(0, num_partitions):
                self.tx_states[p] = dict()
            self.stopped = False
            self.admin = Admin(self.redpanda)
            self.lock = Lock()
            self.thread = Thread(target=self.start_workload, daemon=True)
            self.semaphore = Semaphore(num_producers)
            self.workload_paused = False
            self.failed = False
            self.thread.start()

        def __enter__(self):
            return self

        def __exit__(self, type, value, traceback):
            self.resume()
            self.stop()
            self.thread.join(timeout=30)
            assert not self.failed, (
                "A subset of transactional producers failed, check test log output"
            )
            self.redpanda.logger.debug(
                json.dumps(self.tx_states, sort_keys=True, indent=4)
            )

        class TxState(str, Enum):
            INIT = ("init",)
            BEGIN = ("begin",)
            PRODUCED = ("produced",)
            COMMITTED = ("committed",)
            ABORTED = ("aborted",)

        def random_string(self):
            return "".join(random.choice(string.ascii_letters) for _ in range(5))

        def pause(self):
            self.workload_paused = True
            for _ in range(0, self.num_producers):
                self.semaphore.acquire()
            self.redpanda.logger.info("Paused workload")

        def resume(self):
            self.workload_paused = False
            self.semaphore.release(self.num_producers)
            self.redpanda.logger.info("Workload unpaused")

        def stop(self):
            self.stopped = True

        def tx_id(self):
            with self.lock:
                id = str(self.tx_id_counter)
                self.tx_id_counter += 1
                return id

        def do_transaction(self, producer: ck.Producer, partitions: list[int]):
            producer.begin_transaction()
            yield self.TxState.BEGIN

            for partition in partitions:
                producer.produce(
                    topic=self.topic_name,
                    value=self.random_string(),
                    key=self.random_string(),
                    partition=partition,
                )
            producer.flush()
            yield self.TxState.PRODUCED

            if random.choice([True, False]):
                producer.commit_transaction()
                yield self.TxState.COMMITTED
            else:
                producer.abort_transaction()
                yield self.TxState.ABORTED

        def update_tx_state(
            self, producer_id, state, partitions: list[int], sequence: int
        ):
            with self.lock:
                for p in partitions:
                    self.tx_states[p][producer_id] = dict(
                        state=state, sequence=sequence
                    )

        def dump_debug_transaction_state(self):
            self.redpanda.logger.debug("---- test producer state state ----")
            self.redpanda.logger.debug(
                json.dumps(self.tx_states, sort_keys=True, indent=4)
            )
            self.redpanda.logger.debug("----- broker partition state ----")
            for partition in range(0, self.num_partitions):
                partition_txes = self.admin.get_transactions(
                    topic=self.topic_name, partition=partition, namespace="kafka"
                )
                self.redpanda.logger.debug(partition_txes)

        def random_transaction(self):
            id = self.tx_id()
            producer = ck.Producer(
                {
                    "bootstrap.servers": self.redpanda.brokers(),
                    "transactional.id": id,
                    "transaction.timeout.ms": 1000000,
                }
            )

            producer.init_transactions()
            self.update_tx_state(
                producer_id=id, state=self.TxState.INIT, partitions=[], sequence=-1
            )

            sequence = 0
            try:
                while not self.stopped:
                    sleep(random.randint(1, 10) / 1000.0)
                    if self.workload_paused:
                        continue
                    with self.semaphore:
                        partitions = random.sample(
                            range(0, self.num_partitions), random.randint(0, 5)
                        )
                        for state in self.do_transaction(
                            producer=producer, partitions=partitions
                        ):
                            self.update_tx_state(
                                id, state, partitions, sequence=sequence
                            )
                        sequence += 1
            except Exception:
                self.failed = True
                self.dump_debug_transaction_state()
                self.redpanda.logger.error(
                    f"Exception running transactions with producer {id}", exc_info=True
                )

        def start_workload(self):
            producers = []
            for producer in range(0, self.num_producers):
                t = Thread(target=self.random_transaction)
                t.start()
                producers.append(t)

            for producer in producers:
                producer.join()

        def validate_active_tx_states(self):
            def do_check():
                for p in range(0, self.num_partitions):
                    self.redpanda.logger.debug(
                        f"Validating partition tx state for {self.topic_name}/{p}"
                    )
                    rp_tx_state = self.admin.get_transactions(
                        topic=self.topic_name, partition=p, namespace="kafka"
                    ).get("active_transactions", [])
                    local_tx_state = self.tx_states[p]
                    local_active_pids = [
                        int(pid)
                        for pid, tx_state in local_tx_state.items()
                        if tx_state["state"] in ["begin", "produced"]
                    ]
                    local_active_pids.sort()
                    rp_active_pids = [
                        int(tx["producer_id"]["id"]) for tx in rp_tx_state
                    ]
                    rp_active_pids.sort()
                    self.redpanda.logger.debug(
                        f"Local pids: {rp_active_pids}, broker reported: {local_active_pids}"
                    )
                    return rp_active_pids == local_active_pids

            try:
                wait_until(
                    do_check,
                    timeout_sec=20,
                    backoff_sec=2,
                    err_msg="Invalid active transaction state, check log for details",
                )
            except TimeoutError as e:
                self.dump_debug_transaction_state()
                raise e

    def __init__(self, test_context):
        super(TxUpgradeRevertTest, self).__init__(
            test_context=test_context, num_brokers=3
        )
        self.installer = self.redpanda._installer
        self.partition_count = 10
        self.msg_sent = 0
        self.producers_count = 100

    def setUp(self):
        self.old_version = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD
        )

        self.old_version_str = (
            f"v{self.old_version[0]}.{self.old_version[1]}.{self.old_version[2]}"
        )
        # Install and upgrade from an older version.
        self.installer.install(self.redpanda.nodes, self.old_version)
        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        super(TxUpgradeRevertTest, self).setUp()

    def install_one_node(self, node, version, topic):
        node_idx = self.redpanda.idx(node)
        # Drain leadership of the node to be upgraded to ensure tx partitions are flushed
        # This is a (unfortunate) hack to workaround transaction coordinator's inability
        # to survive restarts. Here we drain all partition leadership (which ensures everything
        # is flushed to disk) before we upgrade/restart.
        self.rpk.cluster_maintenance_enable(node=node_idx, wait=True)
        self.installer.install([node], version)
        self.redpanda.restart_nodes([node])
        # Disable maintenance mode
        self.rpk.cluster_maintenance_disable(node=node_idx)
        self.admin.await_stable_leader(topic=topic, replication=3, timeout_s=30)

    @skip_debug_mode
    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_snapshot_compatibility(self):
        """Test validates that a broker can be upgraded and downgraded while keeping the transaction state consistent.
        Particularly the snapshot state should be compatible across these operations."""
        partition_count = 50
        topic = TopicSpec(partition_count=partition_count)
        self.client().create_topic(topic)
        with self.TxStateGenerator(
            num_producers=20,
            topic_name=topic.name,
            num_partitions=partition_count,
            redpanda=self.redpanda,
        ) as traffic:
            # Populate some transactions state.
            sleep(30)
            # Pause the workload and upgrade one of the nodes
            traffic.pause()
            traffic.validate_active_tx_states()
            first_node = self.redpanda.nodes[0]
            wait_for_num_versions(self.redpanda, 1)
            # do the upgrade
            self.install_one_node(first_node, RedpandaInstaller.HEAD, topic.name)
            wait_for_num_versions(self.redpanda, 2)
            traffic.validate_active_tx_states()
            # Ensure things can progress from where they were paused.
            traffic.resume()
            sleep(30)
            # Downgrade the node again
            traffic.pause()
            traffic.validate_active_tx_states()
            self.install_one_node(first_node, self.old_version, topic.name)
            wait_for_num_versions(self.redpanda, 1)
            traffic.validate_active_tx_states()
            # Ensure progress
            traffic.resume()
            sleep(30)
