import time
from typing import Any, Callable, TypeVar
from ducktape.utils.util import wait_until
from connectrpc.errors import ConnectError, ConnectErrorCode
from rptest.clients.admin.v2 import Admin, metastore_pb, ntp_pb

# Default of the `cloud_topics_num_metastore_partitions` cluster config.
_DEFAULT_NUM_METASTORE_PARTITIONS: int = 3
_INT64_MAX: int = (2**63) - 1

_T = TypeVar("_T")


def _call_with_leader_retry(
    call: Callable[[], _T], timeout_sec: int = 30, backoff_sec: int = 1
) -> _T:
    """Invoke a metastore RPC, retrying through the transient UNAVAILABLE
    ("partition does not have a leader") window that occurs while a metastore
    partition is mid leader election or step-down. Only UNAVAILABLE is retried;
    other errors (e.g. NOT_FOUND) propagate immediately."""
    deadline = time.time() + timeout_sec
    while True:
        try:
            return call()
        except ConnectError as e:
            if e.code != ConnectErrorCode.UNAVAILABLE or time.time() >= deadline:
                raise
            time.sleep(backoff_sec)


def _read_rows(
    admin: Admin,
    metastore_partition: int,
    seek_key: metastore_pb.RowKey | None = None,
    last_key: metastore_pb.RowKey | None = None,
) -> list[metastore_pb.ReadRow]:
    """Paginated read of rows from a single metastore partition, optionally
    bounded by seek_key/last_key."""
    metastore = admin.metastore()
    all_rows: list[metastore_pb.ReadRow] = []
    next_key: str | None = None
    while True:
        req_kwargs: dict[str, Any] = dict(
            metastore_partition=metastore_partition, max_rows=200
        )
        if next_key is not None:
            req_kwargs["raw_seek_key"] = next_key
        elif seek_key is not None:
            req_kwargs["seek_key"] = seek_key
        if last_key is not None:
            req_kwargs["last_key"] = last_key
        resp = _call_with_leader_retry(
            lambda: metastore.read_rows(req=metastore_pb.ReadRowsRequest(**req_kwargs))
        )
        all_rows.extend(resp.rows)
        if not resp.next_key:
            break
        next_key = resp.next_key
    return all_rows


def _resolve_topic_id(admin: Admin, topic: str) -> str:
    """Resolve a cloud topic's name to the topic id used in metastore row
    keys."""
    metastore = admin.metastore()
    after = ""
    while True:
        resp = _call_with_leader_retry(
            lambda: metastore.list_cloud_topics(
                req=metastore_pb.ListCloudTopicsRequest(after_topic_name=after)
            )
        )
        for t in resp.topics:
            if t.topic_name == topic:
                return t.topic_id
        if not resp.has_more or not resp.topics:
            raise ValueError(f"cloud topic {topic} not found in the topic table")
        after = resp.topics[-1].topic_name


def get_l1_extent_lengths(
    admin: Admin,
    topic: str | None = None,
    partition: int | None = None,
    num_metastore_partitions: int = _DEFAULT_NUM_METASTORE_PARTITIONS,
) -> list[int]:
    """Return the byte length (`ExtentValue.len`) of every L1 extent, in no
    particular order. With `topic` (and optionally `partition`) set, only that
    topic's (partition's) extents are returned; otherwise extents from every
    cloud-topic partition are.

    Enumerates the metadata rows (one per managed partition) across the
    metastore partitions, then reads each partition's extents from the same
    metastore partition that holds its metadata.

    `num_metastore_partitions` must reflect the cluster's actual
    `cloud_topics_num_metastore_partitions` config; partitions beyond it
    would be silently skipped.
    """
    assert topic is not None or partition is None, "a partition filter requires a topic"
    topic_id = _resolve_topic_id(admin, topic) if topic is not None else None
    lengths: list[int] = []
    for mp in range(num_metastore_partitions):
        try:
            rows = _read_rows(admin, mp)
        except ConnectError as e:
            if e.code == ConnectErrorCode.NOT_FOUND:
                # The metastore topic has not been created yet: no rows.
                continue
            raise
        for row in rows:
            if not row.key.HasField("metadata"):
                continue
            metadata = row.key.metadata
            if topic_id is not None and metadata.topic_id != topic_id:
                continue
            if partition is not None and metadata.partition_id != partition:
                continue
            seek = metastore_pb.RowKey(
                extent=metastore_pb.ExtentKey(
                    topic_id=metadata.topic_id,
                    partition_id=metadata.partition_id,
                    base_offset=0,
                )
            )
            last = metastore_pb.RowKey(
                extent=metastore_pb.ExtentKey(
                    topic_id=metadata.topic_id,
                    partition_id=metadata.partition_id,
                    base_offset=_INT64_MAX,
                )
            )
            for extent_row in _read_rows(admin, mp, seek_key=seek, last_key=last):
                lengths.append(extent_row.value.extent.len)
    return lengths


def get_l1_extent_lengths_by_partition(
    admin: Admin,
    topic: str,
    num_metastore_partitions: int = _DEFAULT_NUM_METASTORE_PARTITIONS,
) -> dict[int, list[int]]:
    """Like `get_l1_extent_lengths`, but group `topic`'s L1 extent byte
    lengths by partition id. Within each partition the lengths are returned in
    ascending `base_offset` order, so the final element is the trailing
    (highest-offset) extent.

    `num_metastore_partitions` must reflect the cluster's actual
    `cloud_topics_num_metastore_partitions` config; partitions beyond it
    would be silently skipped.
    """
    topic_id = _resolve_topic_id(admin, topic)
    by_partition: dict[int, list[int]] = {}
    for mp in range(num_metastore_partitions):
        try:
            rows = _read_rows(admin, mp)
        except ConnectError as e:
            if e.code == ConnectErrorCode.NOT_FOUND:
                # The metastore topic has not been created yet: no rows.
                continue
            raise
        for row in rows:
            if not row.key.HasField("metadata"):
                continue
            metadata = row.key.metadata
            if metadata.topic_id != topic_id:
                continue
            seek = metastore_pb.RowKey(
                extent=metastore_pb.ExtentKey(
                    topic_id=metadata.topic_id,
                    partition_id=metadata.partition_id,
                    base_offset=0,
                )
            )
            last = metastore_pb.RowKey(
                extent=metastore_pb.ExtentKey(
                    topic_id=metadata.topic_id,
                    partition_id=metadata.partition_id,
                    base_offset=_INT64_MAX,
                )
            )
            lengths = by_partition.setdefault(metadata.partition_id, [])
            for extent_row in _read_rows(admin, mp, seek_key=seek, last_key=last):
                lengths.append(extent_row.value.extent.len)
    return by_partition


def get_l1_partition_size(admin: Admin, topic: str, partition: int) -> int | None:
    """
    Returns the partition size in bytes, or None if the partition
    is not found in the metastore.
    """
    metastore = admin.metastore()
    req = metastore_pb.GetSizeRequest(
        partition=ntp_pb.TopicPartition(topic=topic, partition=partition)
    )
    try:
        response = _call_with_leader_retry(lambda: metastore.get_size(req=req))
        return response.size_bytes
    except ConnectError as e:
        if e.code == ConnectErrorCode.NOT_FOUND:
            return None
        raise


def wait_until_l1_partition_size(
    admin: Admin,
    topic: str,
    partition: int,
    size_cond: Callable[[int], bool],
    timeout_sec: int = 60,
    backoff_sec: int = 5,
):
    """
    Wait until the size of the specificed partition in L1 passes the size_cond
    evaluation parameter. If the partition doesn't exist 0 is evaluated.
    """
    last_size: list[None | int] = [None]

    def pred():
        size = get_l1_partition_size(admin, topic, partition)
        last_size[0] = size
        return size_cond(size or 0)

    wait_until(
        condition=pred,
        timeout_sec=timeout_sec,
        backoff_sec=backoff_sec,
        err_msg=f"Waiting for L1 partition size. Last size {last_size[0]}",
        retry_on_exc=True,
    )
