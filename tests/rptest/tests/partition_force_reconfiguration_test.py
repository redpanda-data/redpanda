# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import requests
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.end_to_end import EndToEndTest
from rptest.clients.rpk import RpkTool
from ducktape.mark import ignore, matrix
from ducktape.utils.util import wait_until
from random import shuffle
import time
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.services.admin import Admin, Replica
from rptest.clients.kcl import KCL
from threading import Thread, Condition
from rptest.services.redpanda import RedpandaService, SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result


class ControllerLeadershipTransferInjector():
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
        assert self.num_successful_transfers, "Not a single successful controller leadership transfer"

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
                    target_id=new_controller)
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
        super(PartitionForceReconfigurationTest,
              self).__init__(test_context, *args, **kwargs)

    SPEC = TopicSpec(name="topic", replication_factor=5)
    WAIT_TIMEOUT_S = 60

    def _start_redpanda(self, acks=-1):
        self.start_redpanda(
            num_nodes=7,
            extra_rp_conf={"internal_topic_replication_factor": 7})
        self.client().create_topic(self.SPEC)
        self.topic = self.SPEC.name
        # Wait for initial leader
        self.redpanda._admin.await_stable_leader(topic=self.topic,
                                                 replication=5,
                                                 timeout_s=self.WAIT_TIMEOUT_S)
        # Start a producer at the desired acks level
        self.start_producer(acks=acks)
        self.await_num_produced(min_records=10000)

    def _wait_until_no_leader(self):
        """Scrapes the debug endpoints of all replicas and checks if any of the replicas think they are the leader"""
        def no_leader():
            state = self.redpanda._admin.get_partition_state(
                "kafka", self.topic, 0)
            if "replicas" not in state.keys() or len(state["replicas"]) == 0:
                return True
            for r in state["replicas"]:
                assert "raft_state" in r.keys()
                if r["raft_state"]["is_leader"]:
                    return False
            return True

        wait_until(no_leader,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="Partition has a leader")

    def _alive_nodes(self):
        return [n.account.hostname for n in self.redpanda.started_nodes()]

    def _stop_majority_nodes(self, replication=5):
        """
        Stops a random majority of nodes hosting partition 0 of test topic.
        """
        assert self.redpanda

        def _get_details():
            d = self.redpanda._admin._get_stable_configuration(
                hosts=self._alive_nodes(),
                topic=self.topic,
                replication=replication)
            if d is None:
                return (False, None)
            return (True, d)

        partition_details = wait_until_result(_get_details,
                                              timeout_sec=30,
                                              backoff_sec=2)

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

    def _do_force_reconfiguration(self, replicas):
        try:
            self.redpanda._admin.force_set_partition_replicas(
                topic=self.topic, partition=0, replicas=replicas)
            return True
        except requests.exceptions.RetryError:
            return False
        except requests.exceptions.ConnectionError:
            return False
        except requests.exceptions.HTTPError:
            return False

    def _force_reconfiguration(self, new_replicas):
        replicas = [
            dict(node_id=replica.node_id, core=replica.core)
            for replica in new_replicas
        ]
        self.redpanda.logger.info(f"Force reconfiguring to: {replicas}")
        self.redpanda.wait_until(
            lambda: self._do_force_reconfiguration(replicas=replicas),
            timeout_sec=60,
            backoff_sec=2,
            err_msg=f"Unable to force reconfigure {self.topic}/0 to {replicas}"
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
                hosts=self._alive_nodes())

    @cluster(num_nodes=9)
    @matrix(acks=[-1, 1],
            restart=[True, False],
            controller_snapshots=[True, False])
    def test_basic_reconfiguration(self, acks, restart, controller_snapshots):
        self._start_redpanda(acks=acks)

        if controller_snapshots:
            self.redpanda.set_cluster_config(
                {"controller_snapshot_max_age_sec": 1})
        else:
            self.redpanda._admin.put_feature("controller_snapshots",
                                             {"state": "disabled"})

        # Kill a majority of nodes
        (killed, alive) = self._stop_majority_nodes()

        self._force_reconfiguration(alive)
        # Leadership should be stabilized
        self.redpanda._admin.await_stable_leader(topic=self.topic,
                                                 replication=len(alive),
                                                 hosts=self._alive_nodes())

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
                self.logger.debug(
                    f"Restarting node with node_id: {replica.node_id}")
                self.redpanda.start_node(node=node)
            alive_nodes_after = self._alive_nodes()
            assert len(alive_nodes_before) < len(alive_nodes_after)
            # The partition should still remain with the reduced replica
            # set size even after all the nodes are started back.
            self.redpanda._admin.await_stable_leader(
                topic=self.topic,
                replication=reduced_replica_set_size,
                hosts=alive_nodes_after,
                timeout_s=self.WAIT_TIMEOUT_S)
        if acks == -1:
            self.run_validation()

    @cluster(num_nodes=5)
    @matrix(controller_snapshots=[True, False])
    def test_reconfiguring_with_dead_node(self, controller_snapshots):
        self.start_redpanda(num_nodes=5)
        assert self.redpanda

        if controller_snapshots:
            self.redpanda.set_cluster_config(
                {"controller_snapshot_max_age_sec": 1})
        else:
            self.redpanda._admin.put_feature("controller_snapshots",
                                             {"state": "disabled"})

        self.topic = "topic"
        self.client().create_topic(
            TopicSpec(name=self.topic, replication_factor=3))

        # Kill majority nodes of the ntp
        (killed, alive) = self._stop_majority_nodes(replication=3)
        killed = killed[0]

        # Reconfigure with one of the killed nodes
        self._force_reconfiguration(alive + [killed])

        if controller_snapshots:
            # Wait for snapshots to catchup.
            time.sleep(3)

        # Started the killed node back up.
        self.redpanda.start_node(
            node=self.redpanda.get_node_by_id(killed.node_id))
        # New group should include the killed node.
        self.redpanda._admin.await_stable_leader(topic=self.topic,
                                                 replication=len(alive) + 1,
                                                 hosts=self._alive_nodes(),
                                                 timeout_s=self.WAIT_TIMEOUT_S)

    @cluster(num_nodes=7)
    @matrix(target_replica_set_size=[1, 3])
    def test_reconfiguring_all_replicas_lost(self, target_replica_set_size):
        self.start_redpanda(num_nodes=4)
        assert self.redpanda

        # create a topic with rf = 1
        self.topic = "topic"
        self.client().create_topic(
            TopicSpec(name=self.topic, replication_factor=1))

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
                except:
                    return -1

            wait_until(
                lambda: get_lso() != -1,
                timeout_sec=30,
                backoff_sec=1,
                err_msg=
                f"Partition {self.topic}/0 couldn't achieve a stable lso")

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
            Replica(dict(node_id=self.redpanda.node_id(replica), core=0)) for
            replica in self.redpanda.started_nodes()[:target_replica_set_size]
        ]
        self._force_reconfiguration(new_replicas=new_replicas)

        self.redpanda._admin.await_stable_leader(
            topic=self.topic,
            replication=target_replica_set_size,
            hosts=self._alive_nodes(),
            timeout_s=self.WAIT_TIMEOUT_S)

        # Ensure it is empty
        lso = get_stable_lso()
        assert lso == 0, f"Partition {self.topic}/0 has incorrect lso {lso}"

        # check if we can produce/consume with new replicas from a client perspective
        self.start_producer()
        self.await_num_produced(min_records=10000)
        self.start_consumer()
        self.run_validation()


class NodeWiseRecoveryTest(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        super(NodeWiseRecoveryTest,
              self).__init__(test_context,
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
                             **kwargs)
        self.default_timeout_sec = 60
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    def _alive_nodes(self):
        return [n.account.hostname for n in self.redpanda.started_nodes()]

    def collect_topic_partition_states(self, topic):
        states = {}
        for p in self.rpk.describe_topic(topic):
            states[p.id] = self.admin.get_partition_state(
                namespace="kafka",
                topic=topic,
                partition=p.id,
                node=self.redpanda.get_node_by_id(p.leader))
        return states

    def get_topic_partition_high_watermarks(self, topic):
        return {p.id: p.high_watermark for p in self.rpk.describe_topic(topic)}

    def produce_until_segments_removed(self, topic):
        msg_size = 512

        self.producer = KgoVerifierProducer(self.test_context, self.redpanda,
                                            topic, msg_size, 10000000)

        self.producer.start(clean=False)

        def all_cloud_offsets_advanced():
            states = self.collect_topic_partition_states(topic)

            return all(r['next_cloud_offset'] >= 1000 for s in states.values()
                       for r in s['replicas'])

        wait_until(
            all_cloud_offsets_advanced,
            timeout_sec=self.default_timeout_sec,
            backoff_sec=1,
            err_msg="Error waiting for retention to prefix truncate partitions"
        )

        self.producer.stop()
        self.producer.clean()
        self.producer.free()

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
            spec = TopicSpec(name=f"topic-{i}",
                             replication_factor=rf,
                             partition_count=parts,
                             redpanda_remote_read=with_ts,
                             redpanda_remote_write=with_ts)
            topics.append(spec)
            self.client().create_topic(spec)
            self.client().alter_topic_config(
                spec.name, TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
                2 * 1024 * 1024)

        admin = self.redpanda._admin

        to_kill_nodes = random.sample(self.redpanda.started_nodes(),
                                      dead_node_count)
        to_kill_node_ids = [
            int(self.redpanda.node_id(n)) for n in to_kill_nodes
        ]
        for t in topics:
            self.produce_until_segments_removed(t.name)
        self.redpanda.wait_for_manifest_uploads()

        partitions_lost_majority = admin.get_majority_lost_partitions_from_nodes(
            dead_brokers=to_kill_node_ids)
        # collect topic partition high watermarks before recovery
        initial_topic_hws = {
            t.name: self.get_topic_partition_high_watermarks(t.name)
            for t in topics
        }

        self.logger.debug(f"Stopping nodes: {to_kill_node_ids}")
        self.redpanda.for_nodes(to_kill_nodes, self.redpanda.stop_node)

        def controller_available():
            controller = self.redpanda.controller()
            return controller is not None and self.redpanda.node_id(
                controller) not in to_kill_node_ids

        wait_until(controller_available,
                   timeout_sec=self.default_timeout_sec,
                   backoff_sec=3,
                   err_msg="Controller not available")

        def make_recovery_payload(defunct_nodes: list[int],
                                  partitions_lost_majority: dict):
            return {
                "dead_nodes": defunct_nodes,
                "partitions_to_force_recover": partitions_lost_majority
            }

        payload = make_recovery_payload(to_kill_node_ids,
                                        partitions_lost_majority)
        self.logger.debug(f"payload: {payload}")

        surviving_node = random.choice([
            n for n in self.redpanda.started_nodes()
            if self.redpanda.node_id(n) not in to_kill_node_ids
        ])

        self.logger.debug(f"recovering from: {to_kill_node_ids}")
        self._rpk = RpkTool(self.redpanda)
        # issue a node wise recovery
        self._rpk.force_partition_recovery(from_nodes=to_kill_node_ids,
                                           to_node=surviving_node)

        with ControllerLeadershipTransferInjector(self.redpanda) as transfers:

            def no_majority_lost_partitions():
                try:
                    transfers.pause()
                    wait_until(controller_available,
                               timeout_sec=self.default_timeout_sec,
                               backoff_sec=3,
                               err_msg="Controller not available")
                    lost_majority = admin.get_majority_lost_partitions_from_nodes(
                        dead_brokers=to_kill_node_ids,
                        node=surviving_node,
                        timeout=3)
                    self.logger.debug(
                        f"Partitions with lost majority: {lost_majority}")
                    return len(lost_majority) == 0
                except Exception as e:
                    self.logger.debug(e, exc_info=True)
                    return False
                finally:
                    transfers.resume()

            # Wait until there are no partition assignments with majority loss due to dead nodes.
            wait_until(no_majority_lost_partitions,
                       timeout_sec=self.default_timeout_sec,
                       backoff_sec=3,
                       err_msg="Node wise recovery failed")

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
                    wait_until(controller_available,
                               timeout_sec=self.default_timeout_sec,
                               backoff_sec=3,
                               err_msg="Controller not available")
                    # Wait for balancer tick to run so the data is populated.
                    wait_until(lambda: get_partition_balancer_status(
                        lambda s: s["status"] != "starting"),
                               timeout_sec=self.default_timeout_sec,
                               backoff_sec=3,
                               err_msg="Balancer tick did not run in time")
                    return get_partition_balancer_status(lambda s: s[
                        "partitions_pending_force_recovery_count"] == 0)
                except Exception as e:
                    self.logger.debug(e, exc_info=True)
                    return -1
                finally:
                    transfers.resume()

            wait_until(no_pending_force_reconfigurations,
                       timeout_sec=self.default_timeout_sec,
                       backoff_sec=3,
                       err_msg="reported force recovery count is non zero")

            # Ensure every partition has a stable leader.
            for topic in topics:
                for part in range(0, topic.partition_count):
                    self.redpanda._admin.await_stable_leader(
                        topic=topic.name,
                        partition=part,
                        timeout_s=self.default_timeout_sec,
                        backoff_s=2,
                        hosts=self._alive_nodes())
        topic_hws_after_recovery = {
            t.name: self.get_topic_partition_high_watermarks(t.name)
            for t in topics
        }

        for t in topics:
            for partition_id, initial_hw in initial_topic_hws[t.name].items():
                final_hw = topic_hws_after_recovery[t.name][partition_id]
                self.logger.info(
                    f"partition {t}/{partition_id} replicas initial high watermark: {initial_hw} final high watermark: {final_hw}"
                )
                if t.redpanda_remote_write or t.replication_factor == 3:
                    assert 0.8 * initial_hw <= final_hw <= initial_hw
