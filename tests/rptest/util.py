# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import pprint
from contextlib import contextmanager
from typing import Optional

from ducktape.utils.util import wait_until
from requests.exceptions import HTTPError

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.services.storage import Segment


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


def produce_total_bytes(redpanda, topic, bytes_to_produce, acks=-1):
    kafka_tools = KafkaCliTools(redpanda)

    def done():
        nonlocal bytes_to_produce

        kafka_tools.produce(topic, 10000, 1024, acks=acks)
        bytes_to_produce -= 10000 * 1024
        return bytes_to_produce < 0

    wait_until(done,
               timeout_sec=60,
               backoff_sec=1,
               err_msg="f{bytes_to_produce} bytes still left to produce")


def produce_until_segments(redpanda,
                           topic,
                           partition_idx,
                           count,
                           acks=-1,
                           record_size=1024,
                           batch_size=10000):
    """
    Produce into the topic until given number of segments will appear
    """
    kafka_tools = KafkaCliTools(redpanda)

    def done():
        kafka_tools.produce(topic,
                            batch_size,
                            record_size=record_size,
                            acks=acks)
        topic_partitions = segments_count(redpanda, topic, partition_idx)
        partitions = []
        for p in topic_partitions:
            partitions.append(p >= count)
        return all(partitions)

    wait_until(done,
               timeout_sec=120,
               backoff_sec=2,
               err_msg="Segments were not created")


def wait_until_segments(redpanda,
                        topic,
                        partition_idx,
                        count,
                        timeout_sec=180):
    def done():
        topic_partitions = segments_count(redpanda, topic, partition_idx)
        redpanda.logger.debug(
            f'wait_until_segments: '
            f'segment count: {list(segments_count(redpanda, topic, partition_idx))}'
        )
        return all([p >= count for p in topic_partitions])

    wait_until(done,
               timeout_sec=timeout_sec,
               backoff_sec=2,
               err_msg=f"{count} segments were not created")


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
            "kafka", topic, partition_idx)

        redpanda.logger.debug(
            f"Current segment snapshot for topic {topic}: {pprint.pformat(current_snapshot, indent=1)}"
        )

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


def wait_for_local_storage_truncate(redpanda,
                                    topic: str,
                                    *,
                                    target_bytes: int,
                                    partition_idx: Optional[int] = None,
                                    timeout_sec: Optional[int] = None,
                                    nodes: Optional[list] = None):
    """
    For use in tiered storage tests: wait until the locally etained data
    size for this partition is below a threshold on all nodes.
    """

    if timeout_sec is None:
        timeout_sec = 120

    def is_truncated():
        storage = redpanda.storage(sizes=True)
        sizes = []
        for node_partition in storage.partitions("kafka", topic):
            if partition_idx is not None and node_partition.num != partition_idx:
                continue

            if nodes is not None and node_partition.node not in nodes:
                continue

            total_size = sum(s.size if s.size else 0
                             for s in node_partition.segments.values())
            redpanda.logger.debug(
                f"  {topic}/{partition_idx} node {node_partition.node.name} local size {total_size} ({len(node_partition.segments)} segments)"
            )
            for s in node_partition.segments.values():
                redpanda.logger.debug(
                    f"    {topic}/{partition_idx} node {node_partition.node.name} {s.name} {s.size}"
                )
            sizes.append(total_size)

        # The segment which is open for appends will differ in Redpanda's internal
        # sizing (exact) vs. what the filesystem reports for a falloc'd file (to the
        # nearest page).  Since our filesystem view may over-estimate the size of
        # the log by a page, adjust the target size by that much.
        threshold = target_bytes + 4096

        # We expect to have measured size on at least one node, or this isn't meaningful
        assert len(sizes) > 0

        return all(s <= threshold for s in sizes)

    wait_until(is_truncated, timeout_sec=timeout_sec, backoff_sec=1)


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


def _get_cluster_license(env_var):
    license = os.environ.get(env_var, None)
    if license is None:
        is_ci = os.environ.get("CI", "false")
        if is_ci == "true":
            raise RuntimeError(
                f"Expected {env_var} variable to be set in this environment")

    return license


def get_cluster_license():
    return _get_cluster_license("REDPANDA_SAMPLE_LICENSE")


def get_second_cluster_license():
    return _get_cluster_license("REDPANDA_SECOND_SAMPLE_LICENSE")


class firewall_blocked:
    """Temporary firewall barrier that isolates set of redpanda
    nodes from the ip-address"""
    def __init__(self, nodes, blocked_port, full_block=False):
        self._nodes = nodes
        self._port = blocked_port

        self.mode_for_input = "sport"
        if full_block:
            self.mode_for_input = "dport"

    def __enter__(self):
        """Isolate certain ips from the nodes using firewall rules"""
        cmd = [
            f"iptables -A INPUT -p tcp --{self.mode_for_input} {self._port} -j DROP",
            f"iptables -A OUTPUT -p tcp --dport {self._port} -j DROP"
        ]
        cmd = " && ".join(cmd)
        for node in self._nodes:
            node.account.ssh_output(cmd, allow_fail=False)

    def __exit__(self, type, value, traceback):
        """Remove firewall rules that isolate ips from the nodes"""
        cmd = [
            f"iptables -D INPUT -p tcp --{self.mode_for_input} {self._port} -j DROP",
            f"iptables -D OUTPUT -p tcp --dport {self._port} -j DROP"
        ]
        cmd = " && ".join(cmd)
        for node in self._nodes:
            node.account.ssh_output(cmd, allow_fail=False)


def search_logs_with_timeout(redpanda, pattern: str, timeout_s: int = 5):
    wait_until(lambda: redpanda.search_log_any(pattern),
               timeout_sec=timeout_s,
               err_msg=f"Failed to find pattern: {pattern}")
