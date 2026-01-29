# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import time
from random import shuffle
from threading import Condition, Thread

import requests
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.kcl import KCL
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin, PartitionDetails, Replica
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import RedpandaService, SISettings
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result
from rptest.utils.node_operations import NodeDecommissionWaiter


class ControllerLeadershipTransferInjector:
    """
    Utility that injects a controller leadership change contiuously with
    a given time frequency. Provides the ability to pause/resume transfers.
    """

    def __init__(self, redpanda: RedpandaService, frequency_s: int = 5):
        self.redpanda = redpanda
        self.frequency_s = frequency_s
        self.admin = self.redpanda._admin
        self.logger = self.redpanda.logger
        self.stop_leadership_transfer = False
        self.pause_leadership_transfer = False
        self.num_successful_transfers = 0

        self.resume_leadership_transfer = Condition()
        self.leadership_transfer_paused = Condition()

        self.thread = Thread(target=self._transfer_loop)
        self.thread.start()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stop_leadership_transfer = True
        self.resume()
        self.thread.join(timeout=30)
        assert self.num_successful_transfers, (
            "Not a single successful controller leadership transfer"
        )

    def pause(self):
        self.logger.debug("Pausing controller leadership transfers")
        self.pause_leadership_transfer = True
        with self.leadership_transfer_paused:
            self.leadership_transfer_paused.wait()

    def resume(self):
        self.logger.debug("Resuming controller leadership transfers")
        self.pause_leadership_transfer = False
        with self.resume_leadership_transfer:
            self.resume_leadership_transfer.notify()

    def _transfer_loop(self):
        while not self.stop_leadership_transfer:
            try:
                controller = self.redpanda.controller()
                assert controller, "No controller available"
                controller_id = self.redpanda.node_id(controller)
                candidates = []
                for n in self.redpanda.started_nodes():
                    id = self.redpanda.node_id(n)
                    if id != controller_id:
                        candidates.append(id)
                new_controller = random.choice(candidates)
                self.admin.partition_transfer_leadership(
                    namespace="redpanda",
                    topic="controller",
                    partition="0",
                    target_id=new_controller,
                )
                self.num_successful_transfers += 1
            except Exception as e:
                self.logger.debug(e, exc_info=True)
                pass
            if self.pause_leadership_transfer:
                with self.leadership_transfer_paused:
                    self.leadership_transfer_paused.notify()
                with self.resume_leadership_transfer:
                    self.resume_leadership_transfer.wait()
            time.sleep(self.frequency_s)


class PartitionForceReconfigurationTest(EndToEndTest, PartitionMovementMixin):
    """
    Tests that trigger a force partition reconfiguration to down size the
    replica count forcefully. Validates post reconfiguration state.
    """

    def __init__(self, test_context, *args, **kwargs):
        super(PartitionForceReconfigurationTest, self).__init__(
            test_context, *args, **kwargs
        )

    SPEC = TopicSpec(name="topic", replication_factor=5)
    WAIT_TIMEOUT_S = 60

    def _start_redpanda(self, acks=-1):
        self.start_redpanda(
            num_nodes=7, extra_rp_conf={"internal_topic_replication_factor": 7}
        )
        self.client().create_topic(self.SPEC)
        self.topic = self.SPEC.name
        # Wait for initial leader
        self.redpanda._admin.await_stable_leader(
            topic=self.topic, replication=5, timeout_s=self.WAIT_TIMEOUT_S
        )
        # Start a producer at the desired acks level
        self.start_producer(acks=acks)
        self.await_num_produced(min_records=10000)

    def _wait_until_no_leader(self):
        """Scrapes the debug endpoints of all replicas and checks if any of the replicas think they are the leader"""

        def no_leader():
            state = self.redpanda._admin.get_partition_state("kafka", self.topic, 0)
            if "replicas" not in state.keys() or len(state["replicas"]) == 0:
                return True
            for r in state["replicas"]:
                assert "raft_state" in r.keys()
                if r["raft_state"]["is_leader"]:
                    return False
            return True

        wait_until(
            no_leader, timeout_sec=30, backoff_sec=1, err_msg="Partition has a leader"
        )

    def _alive_nodes(self):
        return [n.account.hostname for n in self.redpanda.started_nodes()]

    def _stop_majority_nodes(self, replication=5):
        """
        Stops a random majority of nodes hosting partition 0 of test topic.
        """
        assert self.redpanda

        def _get_details():
            d = self.redpanda._admin._get_stable_configuration(
                hosts=self._alive_nodes(), topic=self.topic, replication=replication
            )
            if d is None:
                return (False, None)
            return (True, d)

        partition_details = wait_until_result(
            _get_details, timeout_sec=30, backoff_sec=2
        )

        replicas = partition_details.replicas
        shuffle(replicas)
        mid = len(replicas) // 2 + 1
        (killed, alive) = (replicas[0:mid], replicas[mid:])
        for replica in killed:
            node = self.redpanda.get_node_by_id(replica.node_id)
            assert node
            self.logger.debug(f"Stopping node with node_id: {replica.node_id}")
            self.redpanda.stop_node(node)
        # The partition should be leaderless.
        self._wait_until_no_leader()
        return (killed, alive)

    def _do_reconfigure(self, replicas, force: bool):
        try:
            if force:
                self.redpanda._admin.force_set_partition_replicas(
                    topic=self.topic, partition=0, replicas=replicas
                )
            else:
                self.redpanda._admin.set_partition_replicas(
                    topic=self.topic, partition=0, replicas=replicas
                )
            return True
        except requests.exceptions.RetryError:
            return False
        except requests.exceptions.ConnectionError:
            return False
        except requests.exceptions.HTTPError:
            return False

    def reconfigure(self, new_replicas, force: bool):
        replicas = [
            dict(node_id=replica.node_id, core=replica.core) for replica in new_replicas
        ]
        self.redpanda.logger.info(f"Force reconfiguring to: {replicas}")
        self.redpanda.wait_until(
            lambda: self._do_reconfigure(replicas=replicas, force=force),
            timeout_sec=60,
            backoff_sec=2,
            err_msg=f"Unable to force reconfigure {self.topic}/0 to {replicas}",
        )

    def _force_reconfiguration(self, replicas):
        self.reconfigure(new_replicas=replicas, force=True)

    def _reconfiguration(self, replicas):
        self.reconfigure(new_replicas=replicas, force=False)

    def _cancel_reconfiguration(self):
        def do_cancel():
            try:
                self.redpanda._admin.cancel_partition_move(
                    topic=self.topic,
                    partition=0,
                    node=random.choice(self.redpanda.started_nodes()),
                )
                return True
            except requests.exceptions.RetryError:
                return False
            except requests.exceptions.ConnectionError:
                return False
            except requests.exceptions.HTTPError:
                return False

        self.redpanda.wait_until(
            do_cancel,
            timeout_sec=60,
            backoff_sec=2,
            err_msg=f"Unable to cancel reconfiguration for {self.topic}/0",
        )

    def _wait_for_reconfigurations(self):
        def has_reconfigurations():
            try:
                return (
                    len(
                        self.redpanda._admin.list_reconfigurations(
                            node=random.choice(self.redpanda.started_nodes())
                        )
                    )
                    > 0
                )
            except Exception:
                return False

        self.redpanda.wait_until(
            has_reconfigurations,
            timeout_sec=60,
            backoff_sec=2,
            err_msg=f"Timed out waiting for reconfigurations for {self.topic}/0",
        )

    def _start_consumer(self):
        self.start_consumer()
        # Wait for all consumer offsets partitions to have a stable leadership.
        # With lost nodes on debug builds, this seems to take time to converge.
        for part in range(0, 16):
            self.redpanda._admin.await_stable_leader(
                topic="__consumer_offsets",
                partition=part,
                timeout_s=self.WAIT_TIMEOUT_S,
                backoff_s=2,
                hosts=self._alive_nodes(),
            )

    @cluster(num_nodes=9)
    @matrix(acks=[-1, 1], restart=[True, False], controller_snapshots=[True, False])
    def test_basic_reconfiguration(self, acks, restart, controller_snapshots):
        self._start_redpanda(acks=acks)

        if controller_snapshots:
            self.redpanda.set_cluster_config({"controller_snapshot_max_age_sec": 1})
        else:
            self.redpanda._admin.put_feature(
                "controller_snapshots", {"state": "disabled"}
            )

        # Kill a majority of nodes
        (killed, alive) = self._stop_majority_nodes()

        self._force_reconfiguration(alive)
        # Leadership should be stabilized
        self.redpanda._admin.await_stable_leader(
            topic=self.topic,
            replication=len(alive),
            hosts=self._alive_nodes(),
            timeout_s=self.WAIT_TIMEOUT_S,
        )

        self._start_consumer()
        if controller_snapshots:
            # Wait for few seconds to make sure snapshots
            # happen.
            time.sleep(3)
        if restart:
            reduced_replica_set_size = len(alive)
            alive_nodes_before = self._alive_nodes()
            # Restart the killed nodes
            for replica in killed:
                node = self.redpanda.get_node_by_id(replica.node_id)
                assert node
                self.logger.debug(f"Restarting node with node_id: {replica.node_id}")
                self.redpanda.start_node(node=node)
            alive_nodes_after = self._alive_nodes()
            assert len(alive_nodes_before) < len(alive_nodes_after)
            # The partition should still remain with the reduced replica
            # set size even after all the nodes are started back.
            self.redpanda._admin.await_stable_leader(
                topic=self.topic,
                replication=reduced_replica_set_size,
                hosts=alive_nodes_after,
                timeout_s=self.WAIT_TIMEOUT_S,
            )
        if acks == -1:
            self.run_validation()

    @cluster(num_nodes=5)
    @matrix(controller_snapshots=[True, False])
    def test_reconfiguring_with_dead_node(self, controller_snapshots):
        self.start_redpanda(num_nodes=5)
        assert self.redpanda

        if controller_snapshots:
            self.redpanda.set_cluster_config({"controller_snapshot_max_age_sec": 1})
        else:
            self.redpanda._admin.put_feature(
                "controller_snapshots", {"state": "disabled"}
            )

        self.topic = "topic"
        self.client().create_topic(TopicSpec(name=self.topic, replication_factor=3))

        # Kill majority nodes of the ntp
        (killed, alive) = self._stop_majority_nodes(replication=3)
        killed = killed[0]

        # Reconfigure with one of the killed nodes
        self._force_reconfiguration(alive + [killed])

        if controller_snapshots:
            # Wait for snapshots to catchup.
            time.sleep(3)

        # Started the killed node back up.
        self.redpanda.start_node(node=self.redpanda.get_node_by_id(killed.node_id))
        # New group should include the killed node.
        self.redpanda._admin.await_stable_leader(
            topic=self.topic,
            replication=len(alive) + 1,
            hosts=self._alive_nodes(),
            timeout_s=self.WAIT_TIMEOUT_S,
        )

    @cluster(num_nodes=5)
    # If cancellation is True, cancels the in progress stuck move and then force reconfigures
    @matrix(cancellation=[True, False])
    def test_reconfiguring_in_progress_move(self, cancellation):
        """
        Tests that a partition move stuck in progress can be reconfigured. Additionally also checks that a stuck cancellation can be reconfigured."""
        self.start_redpanda(num_nodes=5)
        assert self.redpanda

        self.topic = "topic"
        self.client().create_topic(TopicSpec(name=self.topic, replication_factor=3))

        (killed, alive) = self._stop_majority_nodes(replication=3)

        assert alive, "At least one replica should be alive"

        # Issue a regular partition move while the partition is leaderless.
        # this should be stuck
        self._reconfiguration(alive)
        self._wait_for_reconfigurations()
        if cancellation:
            # Cancel the stuck move
            self._cancel_reconfiguration()

        # Randomness here may choose existing replicas or a totally new set of replicas.
        # Both are interesting cases to test.
        new_replicas = [
            Replica(dict(node_id=self.redpanda.node_id(replica), core=0))
            for replica in random.sample(self.redpanda.started_nodes(), 3)
        ]

        self._force_reconfiguration(new_replicas)

        self.redpanda._admin.await_stable_leader(
            topic=self.topic,
            replication=3,
            hosts=self._alive_nodes(),
            timeout_s=self.WAIT_TIMEOUT_S,
        )

    @cluster(num_nodes=5)
    def test_reconfiguring_force_reconfiguration(self):
        """
        Test ensures that a stuck force reconfiguration can be force reconfigured"""
        self.start_redpanda(num_nodes=5)
        assert self.redpanda

        self.topic = "topic"
        self.client().create_topic(TopicSpec(name=self.topic, replication_factor=3))

        (killed, alive) = self._stop_majority_nodes(replication=3)

        # This would never finish
        self._force_reconfiguration([killed[0]])
        self._wait_for_reconfigurations()

        new_replica_count = random.choice([1, 3])
        new_replicas = [
            Replica(dict(node_id=self.redpanda.node_id(replica), core=0))
            for replica in random.sample(
                self.redpanda.started_nodes(), new_replica_count
            )
        ]
        self._force_reconfiguration(new_replicas)
        self.redpanda._admin.await_stable_leader(
            topic=self.topic,
            replication=new_replica_count,
            hosts=self._alive_nodes(),
            timeout_s=self.WAIT_TIMEOUT_S,
        )

    @cluster(num_nodes=7)
    @matrix(target_replica_set_size=[1, 3])
    def test_reconfiguring_all_replicas_lost(self, target_replica_set_size):
        self.start_redpanda(num_nodes=4)
        assert self.redpanda

        # create a topic with rf = 1
        self.topic = "topic"
        self.client().create_topic(TopicSpec(name=self.topic, replication_factor=1))

        kcl = KCL(self.redpanda)

        # produce some data.
        self.start_producer(acks=1)
        self.await_num_produced(min_records=10000)
        self.producer.stop()

        def get_stable_lso():
            def get_lso():
                try:
                    partitions = kcl.list_offsets([self.topic])
                    if len(partitions) == 0:
                        return -1
                    return partitions[0].end_offset
                except Exception:
                    return -1

            wait_until(
                lambda: get_lso() != -1,
                timeout_sec=30,
                backoff_sec=1,
                err_msg=f"Partition {self.topic}/0 couldn't achieve a stable lso",
            )

            return get_lso()

        lso = get_stable_lso()
        assert lso >= 10001, f"Partition {self.topic}/0 has incorrect lso {lso}"

        # kill the broker hosting the replica
        (killed, alive) = self._stop_majority_nodes(replication=1)
        assert len(killed) == 1
        assert len(alive) == 0

        self._wait_until_no_leader()

        # force reconfigure to target replica set size
        assert target_replica_set_size <= len(self._alive_nodes())
        new_replicas = [
            Replica(dict(node_id=self.redpanda.node_id(replica), core=0))
            for replica in self.redpanda.started_nodes()[:target_replica_set_size]
        ]
        self._force_reconfiguration(new_replicas)

        self.redpanda._admin.await_stable_leader(
            topic=self.topic,
            replication=target_replica_set_size,
            hosts=self._alive_nodes(),
            timeout_s=self.WAIT_TIMEOUT_S,
        )

        # Ensure it is empty
        lso = get_stable_lso()
        assert lso == 0, f"Partition {self.topic}/0 has incorrect lso {lso}"

        # check if we can produce/consume with new replicas from a client perspective
        self.start_producer()
        self.await_num_produced(min_records=10000)
        self.start_consumer()
        self.run_validation()

    @cluster(num_nodes=5)
    def test_reconfigure_away_from_leader(self):
        """
        Regression test for a force reconfiguration deadlock

        Previously, a force reconfiguration away from the partition leader may deadlock

        This would happen because:
        The previous leader would not step down. This allowed it to continue sending
        heartbeats which would squash the leadership ambition of any voter in the new config.

        Additionally: shutdown of a partitions which are leaving a replica set only occurs
        on update_finish, which only occurs when a new leader has been elected.

        The confluence of these two factors allowed the force_reconfiguration to stall forever.

        This test recreates this scenario by doing the following:
        given a cluster
        [A B C D E]
        and a replica placement for a partition
        [A B C D E]
        [F L F    ] where F is a follower and L is a leader

        kill one of the followers
        [A B C D E]
        [    x    ]

        and then launch a force reconfiguration from [A B C] to [A D E]
        the new configuration now looks like
        [A B C D E]
        [V     L L] where V is voter and L is learner

        A should be able to vote itself leader and finish the reconfiguration even though it
        was not the leader at the time of force reconfiguration.
        """
        self.start_redpanda(
            num_nodes=5,
            extra_rp_conf={
                "partition_autobalancing_mode": "continuous",
                "enable_leader_balancer": False,
            },
        )
        self.topic = "my_topic"
        self.client().create_topic(
            TopicSpec(name=self.topic, replication_factor=3, partition_count=1)
        )

        admin = self.redpanda._admin
        rpk = RpkTool(self.redpanda)
        replicas = []
        for p in rpk.describe_topic(self.topic, tolerant=True):
            replicas = [int(r) for r in p.replicas]

        self.logger.info(f"Test topic {self.topic} replicas: {replicas}")

        leader_id = admin.await_stable_leader(topic=self.topic, partition=0)
        assert leader_id > 0, "should have found leader id"

        non_leader_replicas = list(set(replicas) - {leader_id})
        random.shuffle(non_leader_replicas)
        assert len(non_leader_replicas) == 2, "invariant"
        non_leader_replica, to_kill_replica = non_leader_replicas

        # find two other replicas that are NOT part of the current configuration
        all_node_ids = {
            self.redpanda.node_id(node) for node in self.redpanda.started_nodes()
        }
        non_replica_node_ids = list(all_node_ids - set(replicas))

        # we want to reconfigure to one voter and two learners, but the voter should NOT be the current
        # leader
        assert len(non_replica_node_ids) == 2, "invariant"
        reconfiguration_target = non_replica_node_ids + [non_leader_replica]

        self.logger.info(
            f"Force reconfiguration of {self.topic}/0 from {replicas} to {reconfiguration_target} with leader {leader_id}"
        )

        # stop to kill replica
        node_to_stop = self.redpanda.get_node_by_id(to_kill_replica)
        self.logger.debug(f"Stopping node: {to_kill_replica}")
        self.redpanda.stop_node(node_to_stop)

        target_replicas = [
            Replica(dict(node_id=node_id, core=0)) for node_id in reconfiguration_target
        ]
        self._force_reconfiguration(target_replicas)

        def force_reconfigure_complete():
            partition_details: PartitionDetails | None = (
                admin._get_stable_configuration(
                    hosts=[node.name for node in self.redpanda.started_nodes()],
                    topic=self.topic,
                    partition=0,
                )
            )
            if not partition_details:
                return False

            # movement in flight
            if partition_details.status == "in_progress":
                return False

            current_nodes = {replica.node_id for replica in partition_details.replicas}
            # not on the right nodes
            if current_nodes != set(reconfiguration_target):
                return False

            # misplaced leader
            current_leader = partition_details.leader
            if current_leader not in reconfiguration_target:
                return False

            return True

        # force reconfiguration should complete in far less that 30s, any more
        # and it is likely deadlocked
        wait_until(
            condition=force_reconfigure_complete,
            timeout_sec=30,
            backoff_sec=3,
            err_msg="force reconfiguration failed to complete in 30 seconds",
            retry_on_exc=True,
        )


class NodeWiseRecoveryTest(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        super(NodeWiseRecoveryTest, self).__init__(
            test_context,
            si_settings=SISettings(
                log_segment_size=1024 * 1024,
                test_context=test_context,
                fast_uploads=True,
                retention_local_strict=True,
            ),
            extra_rp_conf={
                "partition_autobalancing_mode": "continuous",
                "enable_leader_balancer": False,
            },
            num_brokers=5,
            *args,
            **kwargs,
        )
        self.default_timeout_sec = 120
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda, retries_amount=20, retry_codes=[503, 504])

    def _alive_nodes(self):
        return [n.account.hostname for n in self.redpanda.started_nodes()]

    def setUp(self):
        # add node config override not to spawn a new cluster with empty seed servers
        self.redpanda.start(auto_assign_node_id=True, omit_seeds_on_idx_one=True)

    def collect_topic_partition_states(self, topic):
        states = {}
        for p in self.rpk.describe_topic(topic, tolerant=True):
            states[p.id] = self.admin.get_partition_state(
                namespace="kafka",
                topic=topic,
                partition=p.id,
                node=self.redpanda.get_node_by_id(p.leader),
            )
        return states

    def get_topic_partition_high_watermarks(self, topic):
        return {p.id: p.high_watermark for p in self.rpk.describe_topic(topic)}

    def get_topic_partition_status(self, topic, partitions):
        return {
            p: self.admin.get_partition_state(
                namespace="kafka", topic=topic, partition=p
            )
            for p in range(partitions)
        }

    def produce_until_log_eviction(self, topic):
        msg_size = 512

        self.producer = KgoVerifierProducer(
            self.test_context, self.redpanda, topic, msg_size, 10000000
        )

        self.producer.start(clean=False)

        def start_offset_advanced():
            states = self.collect_topic_partition_states(topic)
            if len(states) == 0:
                return False

            return all(
                r["start_offset"] > 0 for s in states.values() for r in s["replicas"]
            )

        wait_until(
            start_offset_advanced,
            timeout_sec=self.default_timeout_sec,
            backoff_sec=0.1,
            err_msg="Error waiting for start offset to advance",
            retry_on_exc=True,
        )

        self.producer.stop()
        self.producer.clean()
        self.producer.free()

    def wait_for_final_manifest_uploads(self, topic, fraction_uploaded: float = 0.8):
        def all_uploaded():
            for p in self.rpk.describe_topic(topic):
                status = self.admin.get_partition_cloud_storage_status(
                    topic=topic,
                    partition=p.id,
                    node=self.redpanda.get_node_by_id(p.leader),
                )
                if ("ms_since_last_manifest_upload" not in status) or status[
                    "metadata_update_pending"
                ]:
                    self.logger.debug(
                        f"Pending manifest update: {status['ms_since_last_manifest_upload']=} {status['metadata_update_pending']=}"
                    )
                    return False
                if int(status["cloud_log_last_offset"]) < fraction_uploaded * int(
                    status["local_log_last_offset"]
                ):
                    self.logger.debug(
                        f"{topic}/{p.id}: {status['cloud_log_last_offset']=} vs {status['local_log_last_offset']=}"
                    )
                    return False
            return True

        wait_until(
            all_uploaded,
            timeout_sec=self.default_timeout_sec,
            backoff_sec=1,
            err_msg="Error waiting for partition manifests upload",
        )

    def wait_for_no_reconfigurations(self):
        def no_pending_force_reconfigurations():
            status = self.admin.get_partition_balancer_status()
            return status["partitions_pending_force_recovery_count"] == 0

        wait_until(
            no_pending_force_reconfigurations,
            timeout_sec=self.default_timeout_sec,
            backoff_sec=3,
            err_msg="reported force recovery count is non zero",
            retry_on_exc=True,
        )

    @cluster(num_nodes=6)
    @matrix(dead_node_count=[1, 2])
    def test_node_wise_recovery(self, dead_node_count):
        num_topics = 20
        # Create a mix of rf=1 and 3 topics.
        topics = []
        for i in range(0, num_topics):
            rf = 3 if i % 2 == 0 else 1
            parts = random.randint(1, 3)
            with_ts = random.choice([True, False])
            spec = TopicSpec(
                name=f"topic-{i}",
                replication_factor=rf,
                partition_count=parts,
                redpanda_remote_read=with_ts,
                redpanda_remote_write=with_ts,
            )
            topics.append(spec)
            self.client().create_topic(spec)
            self.client().alter_topic_config(
                spec.name,
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
                2 * 1024 * 1024,
            )

        admin = self.redpanda._admin

        to_kill_nodes = random.sample(self.redpanda.started_nodes(), dead_node_count)
        to_kill_node_ids = [int(self.redpanda.node_id(n)) for n in to_kill_nodes]
        expected_fraction_uploaded = 0.8
        for t in topics:
            self.produce_until_log_eviction(t.name)
        for t in topics:
            self.wait_for_final_manifest_uploads(
                t.name, fraction_uploaded=expected_fraction_uploaded
            )

        partitions_lost_majority = admin.get_majority_lost_partitions_from_nodes(
            dead_brokers=to_kill_node_ids
        )
        # collect topic partition high watermarks before recovery
        initial_topic_hws = {
            t.name: self.get_topic_partition_high_watermarks(t.name) for t in topics
        }

        self.logger.debug(f"Stopping nodes: {to_kill_node_ids}")
        self.redpanda.for_nodes(to_kill_nodes, self.redpanda.stop_node)

        def controller_available():
            controller = self.redpanda.controller()
            return (
                controller is not None
                and self.redpanda.node_id(controller) not in to_kill_node_ids
            )

        wait_until(
            controller_available,
            timeout_sec=self.default_timeout_sec,
            backoff_sec=3,
            err_msg="Controller not available",
        )

        def make_recovery_payload(
            defunct_nodes: list[int], partitions_lost_majority: dict
        ):
            return {
                "dead_nodes": defunct_nodes,
                "partitions_to_force_recover": partitions_lost_majority,
            }

        payload = make_recovery_payload(to_kill_node_ids, partitions_lost_majority)
        self.logger.debug(f"payload: {payload}")

        surviving_node = random.choice(
            [
                n
                for n in self.redpanda.started_nodes()
                if self.redpanda.node_id(n) not in to_kill_node_ids
            ]
        )

        self.logger.debug(f"recovering from: {to_kill_node_ids}")
        self._rpk = RpkTool(self.redpanda)
        # issue a node wise recovery
        self._rpk.force_partition_recovery(
            from_nodes=to_kill_node_ids, to_node=surviving_node
        )

        with ControllerLeadershipTransferInjector(self.redpanda) as transfers:

            def no_majority_lost_partitions():
                try:
                    transfers.pause()
                    wait_until(
                        controller_available,
                        timeout_sec=self.default_timeout_sec,
                        backoff_sec=3,
                        err_msg="Controller not available",
                    )
                    lost_majority = admin.get_majority_lost_partitions_from_nodes(
                        dead_brokers=to_kill_node_ids, node=surviving_node, timeout=3
                    )
                    self.logger.debug(f"Partitions with lost majority: {lost_majority}")
                    return len(lost_majority) == 0
                except Exception as e:
                    self.logger.debug(e, exc_info=True)
                    return False
                finally:
                    transfers.resume()

            # Wait until there are no partition assignments with majority loss due to dead nodes.
            wait_until(
                no_majority_lost_partitions,
                timeout_sec=self.default_timeout_sec,
                backoff_sec=3,
                err_msg="Node wise recovery failed",
            )

            def get_partition_balancer_status(predicate):
                try:
                    status = admin.get_partition_balancer_status()
                    return predicate(status)
                except Exception as e:
                    self.logger.debug(e, exc_info=True)
                    return None

            def no_pending_force_reconfigurations():
                try:
                    transfers.pause()
                    wait_until(
                        controller_available,
                        timeout_sec=self.default_timeout_sec,
                        backoff_sec=3,
                        err_msg="Controller not available",
                    )
                    # Wait for balancer tick to run so the data is populated.
                    wait_until(
                        lambda: get_partition_balancer_status(
                            lambda s: s["status"] != "starting"
                        ),
                        timeout_sec=self.default_timeout_sec,
                        backoff_sec=3,
                        err_msg="Balancer tick did not run in time",
                    )
                    return get_partition_balancer_status(
                        lambda s: s["partitions_pending_force_recovery_count"] == 0
                    )
                except Exception as e:
                    self.logger.debug(e, exc_info=True)
                    return -1
                finally:
                    transfers.resume()

            wait_until(
                no_pending_force_reconfigurations,
                timeout_sec=self.default_timeout_sec,
                backoff_sec=3,
                err_msg="reported force recovery count is non zero",
            )

            # Ensure every partition has a stable leader.
            for topic in topics:
                for part in range(0, topic.partition_count):
                    self.redpanda._admin.await_stable_leader(
                        topic=topic.name,
                        partition=part,
                        timeout_s=self.default_timeout_sec,
                        backoff_s=2,
                        hosts=self._alive_nodes(),
                    )
        topic_hws_after_recovery = {
            t.name: self.get_topic_partition_high_watermarks(t.name) for t in topics
        }

        for t in topics:
            for partition_id, initial_hw in initial_topic_hws[t.name].items():
                final_hw = topic_hws_after_recovery[t.name][partition_id]
                self.logger.info(
                    f"partition {t}/{partition_id} replicas initial high watermark: {initial_hw} final high watermark: {final_hw}"
                )
                if t.redpanda_remote_write or t.replication_factor == 3:
                    assert (
                        expected_fraction_uploaded * initial_hw
                        <= final_hw
                        <= initial_hw
                    ), (
                        f"partition {t.name}/{partition_id}: {initial_hw=} vs {final_hw=}"
                    )

    @cluster(num_nodes=6)
    @matrix(wait_for_final_manifest_uploads=[False, True])
    def test_recovery_local_data_missing(self, wait_for_final_manifest_uploads):
        self.redpanda._disable_cloud_storage_diagnostics = True

        topic = TopicSpec(
            replication_factor=3,
            partition_count=1,
            redpanda_remote_read=True,
            redpanda_remote_write=True,
        )
        local_retention = 50 * 1024 * 1024  # 50 MiB
        self.client().create_topic(topic)
        self.client().alter_topic_config(
            topic.name, TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES, local_retention
        )

        admin = self.redpanda._admin
        rpk = RpkTool(self.redpanda)
        replicas = []
        for p in rpk.describe_topic(topic.name, tolerant=True):
            replicas = [int(r) for r in p.replicas]

        self.logger.info(f"Test topic {topic.name} replicas: {replicas}")

        # produce initial data
        self.produce_until_log_eviction(topic.name)
        if wait_for_final_manifest_uploads:
            self.wait_for_final_manifest_uploads(topic.name)
        else:
            self.client().alter_topic_config(topic.name, "redpanda.remote.write", False)

        # collect topic partition high watermarks before recovery
        initial_status = self.get_topic_partition_status(topic.name, 1)
        self.logger.info(f"Initial partition status: {initial_status}")

        # stop first partition replica
        node_to_stop_id = replicas[0]
        node_to_stop = self.redpanda.get_node_by_id(node_to_stop_id)
        self.logger.debug(f"Stopping node: {node_to_stop_id}")
        self.redpanda.stop_node(node_to_stop)
        msg_size = 512
        total_bytes = 75 * 1024 * 1024  # 75 MiB
        msg_cnt = total_bytes // msg_size
        # produce more data while node 0 is stopped
        KgoVerifierProducer.oneshot(
            self.test_context, self.redpanda, topic.name, msg_size, msg_cnt
        )

        if wait_for_final_manifest_uploads:
            self.wait_for_final_manifest_uploads(topic.name)

        status_2nd_step = self.get_topic_partition_status(topic.name, 1)

        # stop other two replicas
        to_kill_node_ids = replicas[1:]
        to_kill_nodes = [self.redpanda.get_node_by_id(i) for i in to_kill_node_ids]
        self.logger.debug(f"Stopping nodes: {to_kill_node_ids}")
        self.redpanda.for_nodes(to_kill_nodes, self.redpanda.stop_node)

        # start the first replica back up, it is outdated now
        self.redpanda.start_node(
            node_to_stop, auto_assign_node_id=True, omit_seeds_on_idx_one=True
        )

        # start the remaining replicas with empty disks
        for node in to_kill_nodes:
            self.redpanda.clean_node(
                node, preserve_logs=True, preserve_current_install=True
            )
            self.redpanda.start_node(
                node, auto_assign_node_id=True, omit_seeds_on_idx_one=True
            )

        partitions_lost_majority = admin.get_majority_lost_partitions_from_nodes(
            dead_brokers=to_kill_node_ids
        )
        self.logger.info(f"Partitions with majority loss: {partitions_lost_majority}")

        self.logger.debug(f"recovering from: {to_kill_node_ids}")
        # issue a node wise recovery
        rpk.force_partition_recovery(from_nodes=to_kill_node_ids)
        self.wait_for_no_reconfigurations()
        # wait for quiescence
        self.redpanda._admin.await_stable_leader(
            topic=topic.name,
            partition=0,
            timeout_s=self.default_timeout_sec,
            backoff_s=2,
            hosts=self._alive_nodes(),
        )

        status_after = self.get_topic_partition_status(
            topic.name, topic.partition_count
        )

        for to_decom in to_kill_node_ids:
            self.admin.decommission_broker(id=to_decom)
            waiter = NodeDecommissionWaiter(
                self.redpanda, to_decom, self.logger, progress_timeout=60
            )
            waiter.wait_for_removal()

        self.logger.info(f"Final partition status: {status_after}")
        cloud_offset = max(
            [r["next_cloud_offset"] for r in status_2nd_step[0]["replicas"]]
        )

        # recovered high watermark must be greater than the cloud offset
        recovered_hw = min([r["high_watermark"] for r in status_after[0]["replicas"]])

        assert recovered_hw >= cloud_offset
