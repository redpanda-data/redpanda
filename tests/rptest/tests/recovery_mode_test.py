# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import tempfile
import dataclasses

from ducktape.utils.util import wait_until
from requests.exceptions import HTTPError

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings
from rptest.services.cluster import cluster
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.admin import Admin
from rptest.services.rpk_producer import RpkProducer
from rptest.util import wait_until_result


def _produce(test, topic, msg_count=1000, partition=None):
    producer = RpkProducer(context=test.test_context,
                           redpanda=test.redpanda,
                           topic=topic,
                           msg_size=4096,
                           msg_count=msg_count,
                           partition=partition)
    try:
        producer.run()
    finally:
        producer.free()


def assert_rpk_fails(cmd, error_msg):
    try:
        cmd()
    except RpkException:
        pass
    else:
        assert False, error_msg


class RecoveryModeTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(*args,
                         test_ctx,
                         num_brokers=4,
                         si_settings=SISettings(test_ctx),
                         **kwargs)

    def setUp(self):
        # start the nodes manually
        pass

    @cluster(num_nodes=5)
    def test_recovery_mode(self):
        """Test that after restarting the cluster in recovery mode, produce/consume is forbidden,
        but metadata operations are still possible.
        """

        # start the 3-node cluster

        seed_nodes = self.redpanda.nodes[0:3]
        joiner_node = self.redpanda.nodes[3]
        self.redpanda.set_seed_servers(seed_nodes)

        self.redpanda.start(nodes=seed_nodes,
                            auto_assign_node_id=True,
                            omit_seeds_on_idx_one=False)

        # create a couple of topics and produce some data

        rpk = RpkTool(self.redpanda)
        rpk.create_topic("mytopic1", partitions=5, replicas=3)
        rpk.create_topic("mytopic2", partitions=5, replicas=3)

        _produce(self, "mytopic1")

        partitions = list(rpk.describe_topic("mytopic1", tolerant=False))
        assert len(partitions) == 5
        assert sum(p.high_watermark for p in partitions) == 1000

        # consume and create some consumer groups

        rpk.consume('mytopic1', n=500, group='mygroup1', quiet=True)
        assert rpk.group_list_names() == ['mygroup1']
        group1 = rpk.group_describe('mygroup1')
        assert group1.total_lag == 500
        rpk.consume('mytopic1', n=1000, group='mygroup2', quiet=True)

        # restart the cluster in recovery mode

        self.redpanda.restart_nodes(
            seed_nodes,
            auto_assign_node_id=True,
            omit_seeds_on_idx_one=False,
            override_cfg_params={"recovery_mode_enabled": True})
        self.redpanda.wait_for_membership(first_start=False)

        # check that describe, produce, consume return errors

        partitions = list(rpk.describe_topic("mytopic1", tolerant=True))
        assert len(partitions) == 5
        assert all(p.load_error is not None for p in partitions)

        assert_rpk_fails(lambda: rpk.produce('mytopic1', 'key', 'msg'),
                         "producing should fail")

        # rpk will retry indefinitely even in the presence of non-retryable errors,
        # so just wait for the timeout to occur.
        assert_rpk_fails(
            lambda: rpk.consume('mytopic1', n=1000, quiet=True, timeout=10),
            "consuming should fail")

        assert_rpk_fails(
            lambda: rpk.consume(
                'mytopic1', n=1000, group='mygroup3', quiet=True, timeout=10),
            "group consuming should fail")

        # check consumer group ops

        assert rpk.group_list_names() == ['mygroup1', 'mygroup2']

        with tempfile.NamedTemporaryFile() as tf:
            # seek to the beginning of all partitions
            for i in range(5):
                tf.write(f"mytopic1 {i} 0\n".encode())
            tf.flush()
            rpk.group_seek_to_file('mygroup1', tf.name)

        group1 = rpk.group_describe('mygroup1', tolerant=True)
        assert sum(p.current_offset for p in group1.partitions) == 0

        group2 = rpk.group_describe('mygroup2', tolerant=True)
        assert sum(p.current_offset for p in group2.partitions) == 1000

        rpk.group_delete('mygroup2')
        assert rpk.group_list_names() == ['mygroup1']

        # check topic ops

        ## alter arbitrary topic config
        rpk.alter_topic_config('mytopic1', 'compression.type', 'snappy')
        wait_until(lambda: rpk.describe_topic_configs('mytopic1')[
            'compression.type'][0] == 'snappy',
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="failed to alter topic config")

        rpk.delete_topic('mytopic2')
        wait_until(lambda: list(rpk.list_topics()) == ['mytopic1'],
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="failed to delete topic")

        # check that a new node can join the cluster

        self.redpanda.start(nodes=[joiner_node], auto_assign_node_id=True)
        self.redpanda.wait_for_membership(first_start=True)

        # restart the cluster back in normal mode

        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    auto_assign_node_id=True)
        self.redpanda.wait_for_membership(first_start=False)

        # check that topic ops effects are still in place

        assert list(rpk.list_topics()) == ['mytopic1']
        for node in self.redpanda.storage().nodes:
            assert len(node.partitions('kafka', 'mytopic2')) == 0

        # check that altered topic config remains in place
        assert rpk.describe_topic_configs(
            'mytopic1')['compression.type'][0] == 'snappy'

        # check that produce and consume work

        _produce(self, "mytopic1")

        def partitions_ready():
            partitions = list(rpk.describe_topic("mytopic1", tolerant=False))
            return (len(partitions) == 5, partitions)

        partitions = wait_until_result(
            partitions_ready,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="failed to wait until partitions become ready")
        assert sum(p.high_watermark for p in partitions) == 2000

        assert rpk.group_list_names() == ['mygroup1']

        consumed = rpk.consume('mytopic1',
                               n=2000,
                               group='mygroup1',
                               quiet=True).rstrip().split('\n')
        # check that group seek was successful
        assert len(consumed) == 2000

    @cluster(num_nodes=5)
    def test_rolling_restart(self):
        self.redpanda.start()

        # create a topic and produce some data

        rpk = RpkTool(self.redpanda)
        rpk.create_topic("mytopic1", partitions=5, replicas=3)
        _produce(self, "mytopic1", msg_count=1000)

        partitions = list(rpk.describe_topic("mytopic1", tolerant=False))
        assert len(partitions) == 5
        assert sum(p.high_watermark for p in partitions) == 1000

        # consume and create some consumer groups

        rpk.consume('mytopic1', n=500, group='mygroup1', quiet=True)
        assert rpk.group_list_names() == ['mygroup1']
        group1 = rpk.group_describe('mygroup1')
        assert group1.total_lag == 500
        rpk.consume('mytopic1', n=1000, group='mygroup2', quiet=True)

        # do a rolling restart to recovery mode

        for node in self.redpanda.nodes:
            self.redpanda.restart_nodes(
                [node], override_cfg_params={"recovery_mode_enabled": True})
            self.redpanda.wait_for_membership(first_start=False)

        # restart back in normal mode

        self.redpanda.restart_nodes(self.redpanda.nodes)
        self.redpanda.wait_for_membership(first_start=False)


@dataclasses.dataclass(frozen=True)
class PartitionInfo:
    ns: str
    topic: str
    partition_id: int
    disabled: bool

    def from_json(json):
        return PartitionInfo(**dict(
            (f.name, json[f.name]) for f in dataclasses.fields(PartitionInfo)))


class DisablingPartitionsTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         num_brokers=4,
                         extra_rp_conf={"controller_snapshot_max_age_sec": 5},
                         **kwargs)

    def sync(self):
        admin = Admin(self.redpanda)
        first = self.redpanda.nodes[0]
        rest = self.redpanda.nodes[1:]

        def equal_everywhere():
            first_res = admin.get_cluster_partitions(node=first)
            return all(
                admin.get_cluster_partitions(node=n) == first_res
                for n in rest)

        # give some time for controller updates to propagate
        wait_until(
            equal_everywhere,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="failed to wait for partitions metadata to equalize")

    @cluster(num_nodes=4)
    def test_apis(self):
        rpk = RpkTool(self.redpanda)
        admin = Admin(self.redpanda)

        try:
            admin.set_partitions_disabled(ns="redpanda", topic="controller")
        except HTTPError as e:
            assert e.response.status_code == 400
        else:
            assert False, "disabling internal topics should fail"

        topics = ["mytopic1", "mytopic2", "mytopic3", "mytopic4"]
        for topic in topics:
            rpk.create_topic(topic, partitions=3, replicas=3)

        admin.set_partitions_disabled(ns="kafka", topic="mytopic1")

        for p in [1, 2]:
            admin.set_partitions_disabled(ns="kafka",
                                          topic="mytopic2",
                                          partition=p)
        admin.set_partitions_disabled(ns="kafka",
                                      topic="mytopic2",
                                      partition=2,
                                      value=False)

        admin.set_partitions_disabled(ns="kafka", topic="mytopic3")
        admin.set_partitions_disabled(ns="kafka",
                                      topic="mytopic3",
                                      partition=1,
                                      value=False)

        admin.set_partitions_disabled(ns="kafka",
                                      topic="mytopic4",
                                      partition=0)
        admin.set_partitions_disabled(ns="kafka",
                                      topic="mytopic4",
                                      value=False)

        self.sync()

        def pi(topic_partition, disabled=False):
            topic, partition = topic_partition.split('/')
            return PartitionInfo('kafka', topic, int(partition), disabled)

        all_partitions = [
            pi('mytopic1/0', True),
            pi('mytopic1/1', True),
            pi('mytopic1/2', True),
            pi('mytopic2/0', False),
            pi('mytopic2/1', True),
            pi('mytopic2/2', False),
            pi('mytopic3/0', True),
            pi('mytopic3/1', False),
            pi('mytopic3/2', True),
            pi('mytopic4/0', False),
            pi('mytopic4/1', False),
            pi('mytopic4/2', False),
        ]

        def filtered(topic, partition, disabled):
            def filter(p):
                if topic is not None and p.topic != topic:
                    return False
                if partition is not None and p.partition_id != partition:
                    return False
                if disabled is not None and p.disabled != disabled:
                    return False
                return True

            res = [p for p in all_partitions if filter(p)]
            if partition is not None:
                assert len(res) == 1
                res = res[0]

            return res

        def get(topic=None, partition=None, disabled=None):
            if topic is None and partition is None:
                ns = None
            else:
                ns = "kafka"
            if partition is None:
                json = admin.get_cluster_partitions(ns=ns,
                                                    topic=topic,
                                                    disabled=disabled)

                return [PartitionInfo.from_json(p) for p in json]
            else:
                json = admin.get_partition(ns, topic, partition)
                return PartitionInfo.from_json(json)

        def check_everything():
            for topic in [None] + topics:
                if topic is None:
                    partitions = [None]
                else:
                    partitions = [None] + list(range(3))

                for partition in partitions:
                    if partition is None:
                        disabled_list = [None, True, False]
                    else:
                        disabled_list = [None]

                    for disabled in disabled_list:
                        filter = (topic, partition, disabled)
                        expected = filtered(*filter)
                        got = get(*filter)
                        self.logger.debug(f"{filter=} {got=} {expected=}")
                        assert got == expected

        check_everything()

        for n in self.redpanda.nodes:
            self.redpanda.wait_for_controller_snapshot(n)

        self.redpanda.restart_nodes(self.redpanda.nodes)
        self.redpanda.wait_for_membership(first_start=False)

        check_everything()

    @cluster(num_nodes=5)
    def test_disable(self):
        """
        Test that disabled partitions are shut down and that producing/consuming
        to them errors out, while producing/consuming to other partitions is
        still possible.
        """

        rpk = RpkTool(self.redpanda)
        admin = Admin(self.redpanda)

        topics = ["mytopic1", "mytopic2"]
        for topic in topics:
            rpk.create_topic(topic, partitions=2, replicas=3)

        def get_node_partitions(node):
            return [
                f'{p["topic"]}/{p["partition_id"]}'
                for p in admin.get_partitions(node=node)
                if p["topic"].startswith("mytopic")
            ]

        def get_partition_counts():
            ret = dict()
            for n in self.redpanda.nodes:
                for p in get_node_partitions(n):
                    ret[p] = ret.setdefault(p, 0) + 1
            self.logger.debug(f"partition counts: {ret}")
            return ret

        def all_created():
            pc = get_partition_counts()
            return len(pc) == 4 and all(c == 3 for c in pc.values())

        wait_until(all_created,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="failed to wait until all partitions are created")

        _produce(self, "mytopic1")
        _produce(self, "mytopic2")

        def get_hwms(topic=None):
            ts = topics if topic is None else [topic]
            ret = dict()
            for topic in ts:
                for p in rpk.describe_topic(topic):
                    ret[f"{topic}/{p.id}"] = p.high_watermark
            return ret

        orig_hwms = get_hwms()
        assert len(orig_hwms) == 4
        for topic in topics:
            assert sum(hwm for ntp, hwm in orig_hwms.items()
                       if ntp.startswith(topic)) == 1000

        self.logger.info("disabling partitions")

        admin.set_partitions_disabled(ns="kafka",
                                      topic="mytopic1",
                                      partition=1)
        admin.set_partitions_disabled(ns="kafka", topic="mytopic2")

        def all_disabled_shut_down():
            pc = get_partition_counts()
            return set(pc.keys()) == {"mytopic1/0"} and all(
                c == 3 for c in pc.values())

        wait_until(
            all_disabled_shut_down,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="failed to wait until all disabled partitions are shut down"
        )

        hwms2 = get_hwms()
        assert len(hwms2) == 1
        assert hwms2["mytopic1/0"] == orig_hwms["mytopic1/0"]

        # test that producing to disabled partitions fails, while producing to
        # the remaining partition is still possible

        _produce(self, "mytopic1", partition=0)

        assert_rpk_fails(
            lambda: rpk.produce('mytopic1', 'key', 'msg', partition=1),
            "producing should fail")

        assert_rpk_fails(lambda: rpk.produce('mytopic2', 'key', 'msg'),
                         "producing should fail")

        hwms3 = get_hwms()
        assert len(hwms2) == 1
        assert hwms3["mytopic1/0"] == hwms2["mytopic1/0"] + 1000

        # the same with consuming

        assert len(
            rpk.consume(
                'mytopic1', n=hwms3["mytopic1/0"],
                quiet=True).rstrip().split('\n')) == hwms3["mytopic1/0"]

        assert_rpk_fails(
            lambda: rpk.consume(
                'mytopic1', n=hwms3["mytopic1/0"] + 1, timeout=10),
            "consuming should fail")

        assert_rpk_fails(lambda: rpk.consume('mytopic2', n=1, timeout=10),
                         "consuming should fail")

        self.logger.info("restarting cluster")

        self.redpanda.restart_nodes(self.redpanda.nodes)
        self.redpanda.wait_for_membership(first_start=False)

        def all_have_leaders():
            for n in self.redpanda.started_nodes():
                all_partitions_have_leaders = all([
                    partition['leader'] != -1
                    for partition in admin.get_partitions(node=n)
                ])
                if not all_partitions_have_leaders:
                    return False
            return True

        wait_until(
            all_have_leaders,
            30,
            backoff_sec=1,
            err_msg="Failed waiting for partition leadership to stabilize")

        # test that partitions are still disabled after restart

        assert all_disabled_shut_down()

        assert get_hwms() == hwms3

        self.logger.info("enabling partitions back")

        admin.set_partitions_disabled(ns="kafka",
                                      topic="mytopic1",
                                      value=False)
        admin.set_partitions_disabled(ns="kafka",
                                      topic="mytopic2",
                                      value=False)

        wait_until(all_created,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="failed to wait until all partitions are created")

        wait_until(lambda: set(get_hwms().keys()) == set(orig_hwms.keys()),
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="failed to wait until all leaders elected")

        # test that producing and consuming works after re-enabling all partitions

        hwms4 = get_hwms()
        assert set(hwms4.keys()) == set(orig_hwms.keys())
        for ntp, hwm in hwms4.items():
            if ntp == "mytopic1/0":
                assert hwm == orig_hwms[ntp] + 1000
            else:
                assert hwm == orig_hwms[ntp]

        _produce(self, "mytopic1")
        _produce(self, "mytopic2")

        assert sum(hwm for hwm in get_hwms("mytopic1").values()) == 3000
        assert sum(hwm for hwm in get_hwms("mytopic2").values()) == 2000

        rpk.consume('mytopic1', n=3000, quiet=True)
        rpk.consume('mytopic2', n=2000, quiet=True)
