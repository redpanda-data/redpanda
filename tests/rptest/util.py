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

from ducktape.utils.util import wait_until
from rptest.clients.kafka_cli_tools import KafkaCliTools


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


def produce_until_segments(redpanda, topic, partition_idx, count, acks=-1):
    """
    Produce into the topic until given number of segments will appear
    """
    kafka_tools = KafkaCliTools(redpanda)

    def done():
        kafka_tools.produce(topic, 10000, 1024, acks=acks)
        topic_partitions = segments_count(redpanda, topic, partition_idx)
        partitions = []
        for p in topic_partitions:
            partitions.append(p >= count)
        return all(partitions)

    wait_until(done,
               timeout_sec=120,
               backoff_sec=2,
               err_msg="Segments were not created")


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
