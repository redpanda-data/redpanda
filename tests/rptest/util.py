# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
from contextlib import contextmanager
from requests.exceptions import HTTPError

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.services.storage import Segment

from ducktape.errors import TimeoutError
import time


class Scale:
    KEY = "scale"
    LOCAL = "local"
    CI = "ci"
    RELEASE = "release"
    DEFAULT = LOCAL
    SCALES = (LOCAL, CI, RELEASE)

    def __init__(self, context):
        self._scale = context.globals.get(Scale.KEY,
                                          Scale.DEFAULT).strip().lower()

        if self._scale not in Scale.SCALES:
            raise RuntimeError(
                f"Invalid scale {self._scale}. Available: {Scale.SCALES}")

    def __str__(self):
        return self._scale

    @property
    def local(self):
        return self._scale == Scale.LOCAL

    @property
    def ci(self):
        return self._scale == Scale.CI

    @property
    def release(self):
        return self._scale == Scale.RELEASE


def wait_until(condition,
               timeout_sec,
               backoff_sec=.1,
               err_msg="",
               retry_on_exc=False):
    start = time.time()
    stop = start + timeout_sec
    last_exception = None
    while time.time() < stop:
        try:
            if condition():
                return
            else:
                last_exception = None
        except BaseException as e:
            last_exception = e
            if not retry_on_exc:
                raise e
        time.sleep(backoff_sec)

    # it is safe to call Exception from None - will be just treated as a normal exception
    raise TimeoutError(
        err_msg() if callable(err_msg) else err_msg) from last_exception


def wait_until_result(condition, *args, **kwargs):
    """
    a near drop-in replacement for ducktape's wait_util except that when
    the condition passes a result from the condition is passed back to the
    caller.

    when a non-tuple is returned from the condition the behavior is the same as
    wait_until and the final value will be returned to the caller.

    when a tuple is returned the first element is used as the value of the
    conditionl, and the remaining elements in the tuple are returned to the
    caller. special cases for {1,2}-tuple are handled for convenience:

       (cond,)          -> None
       (cond,e1)        -> e1
       (cond,e1,e2,...) -> (e1,e2,...)
    """
    res = None

    def wrapped_condition():
        nonlocal res
        cond = condition()
        if isinstance(cond, tuple):
            head, *tail = cond
            if not tail:
                res = None
            elif len(tail) == 1:
                res = tail[0]
            else:
                res = tail
            return head
        res = cond
        return res

    wait_until(wrapped_condition, *args, **kwargs)
    return res


def segments_count(redpanda, topic, partition_idx):
    storage = redpanda.storage()
    topic_partitions = storage.partitions("kafka", topic)

    return map(
        lambda p: len(p.segments),
        filter(lambda p: p.num == partition_idx, topic_partitions),
    )


def produce_until_segments(redpanda,
                           topic,
                           partition_idx,
                           count,
                           acks=-1,
                           record_size=1024):
    """
    Produce into the topic until given number of segments will appear
    """
    kafka_tools = KafkaCliTools(redpanda)

    def done():
        kafka_tools.produce(topic, 10000, record_size=record_size, acks=acks)
        topic_partitions = segments_count(redpanda, topic, partition_idx)
        partitions = []
        for p in topic_partitions:
            partitions.append(p >= count)
        return all(partitions)

    wait_until(done,
               timeout_sec=120,
               backoff_sec=2,
               err_msg="Segments were not created")


def wait_for_removal_of_n_segments(redpanda, topic: str, partition_idx: int,
                                   n: int,
                                   original_snapshot: dict[str,
                                                           list[Segment]]):
    """
    Wait until 'n' segments of a partition that are present in the
    provided snapshot are removed by all brokers.

    :param redpanda: redpanda service used by the test
    :param topic: topic to wait on
    :param partition_idx: index of partition to wait on
    :param n: number of removed segments to wait for
    :param original_snapshot: snapshot of segments to compare against
    """
    def segments_removed():
        current_snapshot = redpanda.storage(all_nodes=True).segments_by_node(
            "kafka", topic, 0)

        redpanda.logger.debug(
            f"Current segment snapshot for topic {topic}: {current_snapshot}")

        # Check how many of the original segments were removed
        # for each of the nodes in the provided snapshot.
        for node, original_segs in original_snapshot.items():
            assert node in current_snapshot
            current_segs_names = [s.name for s in current_snapshot[node]]

            removed_segments = 0
            for s in original_segs:
                if s.name not in current_segs_names:
                    removed_segments += 1

            if removed_segments < n:
                return False

        return True

    wait_until(segments_removed,
               timeout_sec=180,
               backoff_sec=5,
               err_msg="Segments were not removed from all nodes")


def wait_for_segments_removal(redpanda, topic, partition_idx, count):
    """
    Wait until only given number of segments will left in a partitions
    """
    def done():
        topic_partitions = segments_count(redpanda, topic, partition_idx)
        partitions = []
        for p in topic_partitions:
            partitions.append(p <= count)
        return all(partitions)

    try:
        wait_until(done,
                   timeout_sec=120,
                   backoff_sec=5,
                   err_msg="Segments were not removed")
    except:
        # On errors, dump listing of the storage location
        for node in redpanda.nodes:
            redpanda.logger.error(f"Storage listing on {node.name}:")
            for line in node.account.ssh_capture(f"find {redpanda.DATA_DIR}"):
                redpanda.logger.error(line.strip())

        raise


@contextmanager
def expect_exception(exception_klass, validator):
    """
    :param exception_klass: the expected exception type
    :param validator: a callable that is expected to return true when passed the exception
    :return: None.  Raises on unexpected exception or no exception.
    """
    try:
        yield
    except exception_klass as e:
        if not validator(e):
            raise
    else:
        raise RuntimeError("Expected an exception!")


def expect_http_error(status_code: int):
    """
    Context manager for HTTP calls expected to result in an HTTP exception
    carrying a particular status code.

    :param status_code: expected HTTP status code
    :return: None.  Raises on unexpected exception, no exception, or unexpected status code.
    """
    return expect_exception(HTTPError,
                            lambda e: e.response.status_code == status_code)


def inject_remote_script(node, script_name):
    """
    Copy a script from the remote_scripts/ directory onto
    a remote node, ready for execution.

    :return: The path of the script on the remote node
    """
    this_dir = os.path.dirname(os.path.realpath(__file__))

    scripts_dir = os.path.join(this_dir, "remote_scripts")
    assert os.path.exists(scripts_dir)
    script_path = os.path.join(scripts_dir, script_name)
    assert os.path.exists(script_path)

    remote_path = os.path.join("/tmp", script_name)
    node.account.copy_to(script_path, remote_path)

    return remote_path


def get_cluster_license():
    license = os.environ.get("REDPANDA_SAMPLE_LICENSE", None)
    if license is None:
        is_ci = os.environ.get("CI", "false")
        if is_ci == "true":
            raise RuntimeError(
                "Expected REDPANDA_SAMPLE_LICENSE variable to be set in this environment"
            )

    return license


class firewall_blocked:
    """Temporary firewall barrier that isolates set of redpanda
    nodes from the ip-address"""
    def __init__(self, nodes, blocked_port):
        self._nodes = nodes
        self._port = blocked_port

    def __enter__(self):
        """Isolate certain ips from the nodes using firewall rules"""
        cmd = []
        cmd.append(f"iptables -A INPUT -p tcp --sport {self._port} -j DROP")
        cmd.append(f"iptables -A OUTPUT -p tcp --dport {self._port} -j DROP")
        cmd = " && ".join(cmd)
        for node in self._nodes:
            node.account.ssh_output(cmd, allow_fail=False)

    def __exit__(self, type, value, traceback):
        """Remove firewall rules that isolate ips from the nodes"""
        cmd = []
        cmd.append(f"iptables -D INPUT -p tcp --sport {self._port} -j DROP")
        cmd.append(f"iptables -D OUTPUT -p tcp --dport {self._port} -j DROP")
        cmd = " && ".join(cmd)
        for node in self._nodes:
            node.account.ssh_output(cmd, allow_fail=False)
