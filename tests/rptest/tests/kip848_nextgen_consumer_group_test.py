# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
"""
KIP-848 Next-Gen Consumer Group Protocol Integration Tests

Tests the ConsumerGroupHeartbeat API (API key 68) and the
nextgen::coordinator implementation introduced under the feature flag
``enable_kip848_next_gen_consumer_group_protocol``.

The harness uses a thin wire-protocol client that sends raw
ConsumerGroupHeartbeat requests over a TCP socket.  This gives precise
control over member_id and member_epoch — values that a high-level Kafka
client would manage automatically — so every edge case in the coordinator
state machine can be exercised directly.

Test classes
────────────
KIP848FlagOffTest  — verifies the API is gated (flag disabled, default)
KIP848Test         — full lifecycle and error-path tests (flag enabled)
"""

import socket
import struct
import uuid
from dataclasses import dataclass

from ducktape.utils.util import wait_until

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest

# ──────────────────────────────────────────────────────────────────────────────
# Kafka error codes
# ──────────────────────────────────────────────────────────────────────────────

_ERR_NONE = 0
_ERR_UNKNOWN_MEMBER_ID = 25
_ERR_UNSUPPORTED_VERSION = 35
_ERR_FENCED_MEMBER_EPOCH = 110

# Kafka error code 24 (INVALID_GROUP_ID) — used for empty group_id check
_ERR_INVALID_GROUP_ID = 24

# KIP-848 sentinels (§ New Member Registration)
_NEW_MEMBER_EPOCH = -1
_NEW_MEMBER_ID = ""

# Default rebalance timeout sent by the test client
_REBALANCE_TIMEOUT_MS = 5_000

# Feature flag name as it appears in Redpanda configuration
_KIP848_FLAG = "enable_kip848_next_gen_consumer_group_protocol"


# ──────────────────────────────────────────────────────────────────────────────
# Compact (flexible-version) encoding / decoding helpers
#
# Kafka "flexible" APIs use compact types:
#   COMPACT_STRING        → unsigned varint (byte-length + 1), then bytes
#   COMPACT_NULLABLE_STRING → 0 for null, otherwise same as COMPACT_STRING
#   COMPACT_ARRAY         → unsigned varint (element-count + 1), then elements
#                           0 means null array
# ──────────────────────────────────────────────────────────────────────────────


def _encode_uvarint(n: int) -> bytes:
    """Encode a non-negative integer as an unsigned LEB128 varint."""
    result = bytearray()
    while True:
        bits = n & 0x7F
        n >>= 7
        if n:
            result.append(bits | 0x80)
        else:
            result.append(bits)
            break
    return bytes(result)


def _decode_uvarint(data: bytes, offset: int) -> tuple[int, int]:
    """Decode an unsigned LEB128 varint; returns (value, new_offset)."""
    result = 0
    shift = 0
    while True:
        b = data[offset]
        offset += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, offset
        shift += 7


def _encode_compact_string(s: str | None) -> bytes:
    """Encode a COMPACT_NULLABLE_STRING (null → 0x00)."""
    if s is None:
        return _encode_uvarint(0)
    encoded = s.encode("utf-8")
    return _encode_uvarint(len(encoded) + 1) + encoded


def _decode_compact_string(data: bytes, offset: int) -> tuple[str | None, int]:
    """Decode a COMPACT_NULLABLE_STRING; returns (value_or_None, new_offset)."""
    length, offset = _decode_uvarint(data, offset)
    if length == 0:
        return None, offset
    length -= 1
    return data[offset:offset + length].decode("utf-8"), offset + length


def _encode_compact_array_strings(items: list[str] | None) -> bytes:
    """Encode a COMPACT_NULLABLE_ARRAY of COMPACT_STRINGs."""
    if items is None:
        return _encode_uvarint(0)
    result = _encode_uvarint(len(items) + 1)
    for item in items:
        result += _encode_compact_string(item)
    return result


def _skip_tagged_fields(data: bytes, offset: int) -> int:
    """Skip a tagged-field section and return the new offset."""
    count, offset = _decode_uvarint(data, offset)
    for _ in range(count):
        _tag, offset = _decode_uvarint(data, offset)
        size, offset = _decode_uvarint(data, offset)
        offset += size
    return offset


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError(
                f"socket closed after {len(buf)} of {n} bytes")
        buf.extend(chunk)
    return bytes(buf)


# ──────────────────────────────────────────────────────────────────────────────
# Request / Response data classes
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class HeartbeatRequest:
    group_id: str
    member_id: str = _NEW_MEMBER_ID
    member_epoch: int = _NEW_MEMBER_EPOCH
    instance_id: str | None = None
    rack_id: str | None = None
    rebalance_timeout_ms: int = _REBALANCE_TIMEOUT_MS
    subscribed_topic_names: list[str] | None = None
    server_assignor: str | None = None


@dataclass
class HeartbeatResponse:
    throttle_time_ms: int
    error_code: int
    member_id: str | None
    member_epoch: int
    heartbeat_interval_ms: int


# ──────────────────────────────────────────────────────────────────────────────
# Wire-protocol client
# ──────────────────────────────────────────────────────────────────────────────


class _HeartbeatClient:
    """
    Minimal Kafka wire-protocol client for ConsumerGroupHeartbeat (API key 68).

    Opens a single persistent TCP connection and sends/receives requests
    synchronously.  Uses the flexible (compact) encoding required by
    flexibleVersions 0+.

    Usage::

        with _HeartbeatClient("localhost:9092") as c:
            resp = c.heartbeat(HeartbeatRequest(group_id="my-group"))
    """

    _API_KEY: int = 68
    _API_VERSION: int = 0
    _CLIENT_ID: str = "kip848-integration-test"

    def __init__(self, broker: str) -> None:
        host, port_str = broker.rsplit(":", 1)
        self._sock = socket.create_connection((host, int(port_str)),
                                              timeout=10)
        self._correlation_id = 0

    def __enter__(self) -> "_HeartbeatClient":
        return self

    def __exit__(self, *_) -> None:
        self._sock.close()

    def heartbeat(self, req: HeartbeatRequest) -> HeartbeatResponse:
        self._correlation_id += 1
        frame = self._build_request(req, self._correlation_id)
        self._sock.sendall(struct.pack(">I", len(frame)) + frame)
        return self._read_response()

    # ── request encoding ──────────────────────────────────────────────────────

    def _build_request(self, req: HeartbeatRequest,
                       corr_id: int) -> bytes:
        # Request header v2 (required for flexible-version APIs)
        header = struct.pack(">hhi", self._API_KEY, self._API_VERSION,
                             corr_id)
        header += _encode_compact_string(self._CLIENT_ID)
        header += b"\x00"  # empty tagged fields

        body = b""
        body += _encode_compact_string(req.group_id)
        body += _encode_compact_string(req.member_id)
        body += struct.pack(">i", req.member_epoch)
        body += _encode_compact_string(req.instance_id)
        body += _encode_compact_string(req.rack_id)
        body += struct.pack(">i", req.rebalance_timeout_ms)
        body += _encode_compact_array_strings(req.subscribed_topic_names)
        body += _encode_compact_string(req.server_assignor)
        body += _encode_uvarint(0)  # TopicPartitions: null compact array
        body += b"\x00"  # empty tagged fields

        return header + body

    # ── response decoding ─────────────────────────────────────────────────────

    def _read_response(self) -> HeartbeatResponse:
        length = struct.unpack(">I", _recv_exact(self._sock, 4))[0]
        data = _recv_exact(self._sock, length)
        offset = 0

        # Response header v1 (flexible): correlation_id + tagged fields
        offset += 4  # skip correlation_id
        offset = _skip_tagged_fields(data, offset)

        # Response body (per ConsumerGroupHeartbeatResponse schema v0)
        throttle_time_ms = struct.unpack_from(">i", data, offset)[0]
        offset += 4
        error_code = struct.unpack_from(">h", data, offset)[0]
        offset += 2
        _error_message, offset = _decode_compact_string(data, offset)
        member_id, offset = _decode_compact_string(data, offset)
        member_epoch = struct.unpack_from(">i", data, offset)[0]
        offset += 4
        heartbeat_interval_ms = struct.unpack_from(">i", data, offset)[0]
        # offset += 4  (Assignment and trailing tagged fields not needed)

        return HeartbeatResponse(
            throttle_time_ms=throttle_time_ms,
            error_code=error_code,
            member_id=member_id,
            member_epoch=member_epoch,
            heartbeat_interval_ms=heartbeat_interval_ms,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _first_broker(redpanda) -> str:
    return redpanda.brokers().split(",")[0]


# ──────────────────────────────────────────────────────────────────────────────
# Feature-gate test (flag off)
# ──────────────────────────────────────────────────────────────────────────────


class KIP848FlagOffTest(RedpandaTest):
    """Verifies the API is blocked when the feature flag is disabled."""

    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(
            test_ctx,
            num_brokers=1,
            extra_rp_conf={_KIP848_FLAG: False},
            **kwargs,
        )

    @cluster(num_nodes=2)
    def test_returns_unsupported_version_when_disabled(self):
        """
        With the KIP-848 feature flag off, every ConsumerGroupHeartbeat
        must return UNSUPPORTED_VERSION (error 35).  This ensures the API
        is invisible to clients until the operator consciously enables it.
        """
        with _HeartbeatClient(_first_broker(self.redpanda)) as c:
            resp = c.heartbeat(HeartbeatRequest(group_id="gate-test-group"))

        assert resp.error_code == _ERR_UNSUPPORTED_VERSION, (
            f"expected UNSUPPORTED_VERSION ({_ERR_UNSUPPORTED_VERSION}), "
            f"got {resp.error_code}")


# ──────────────────────────────────────────────────────────────────────────────
# Full lifecycle and error-path tests (flag on)
# ──────────────────────────────────────────────────────────────────────────────


class KIP848Test(RedpandaTest):
    """
    Comprehensive integration tests for the KIP-848 nextgen coordinator.

    Each test targets a distinct behaviour of the coordinator state machine
    so that failures point directly to the broken invariant.
    """

    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(
            test_ctx,
            num_brokers=1,
            extra_rp_conf={_KIP848_FLAG: True},
            **kwargs,
        )

    # ── member lifecycle ──────────────────────────────────────────────────────

    @cluster(num_nodes=2)
    def test_new_member_registration(self):
        """
        A first heartbeat using the new-member sentinel values
        (member_epoch = -1, member_id = "") must return a fresh member_id
        and member_epoch = 0, confirming the coordinator accepted the member
        and assigned it an identity.
        """
        with _HeartbeatClient(_first_broker(self.redpanda)) as c:
            resp = c.heartbeat(
                HeartbeatRequest(
                    group_id="reg-group",
                    subscribed_topic_names=["reg-topic"],
                ))

        assert resp.error_code == _ERR_NONE, \
            f"unexpected error code {resp.error_code}"
        assert resp.member_id, \
            "member_id must be non-empty after registration"
        assert resp.member_epoch == 0, \
            f"expected member_epoch=0, got {resp.member_epoch}"
        assert resp.heartbeat_interval_ms > 0, \
            "heartbeat_interval_ms must be positive"

    @cluster(num_nodes=2)
    def test_epoch_advancement(self):
        """
        Each successive heartbeat carrying the current epoch must receive
        an incremented epoch.  Tests three advancement steps to confirm
        the epoch counter is not a one-shot mechanism.
        """
        with _HeartbeatClient(_first_broker(self.redpanda)) as c:
            r0 = c.heartbeat(HeartbeatRequest(group_id="epoch-group"))
            assert r0.error_code == _ERR_NONE
            assert r0.member_epoch == 0
            member_id = r0.member_id

            for expected_epoch in range(1, 4):
                r = c.heartbeat(
                    HeartbeatRequest(
                        group_id="epoch-group",
                        member_id=member_id,
                        member_epoch=expected_epoch - 1,
                    ))
                assert r.error_code == _ERR_NONE, \
                    f"step {expected_epoch}: unexpected error {r.error_code}"
                assert r.member_epoch == expected_epoch, \
                    (f"step {expected_epoch}: "
                     f"expected epoch {expected_epoch}, got {r.member_epoch}")

    @cluster(num_nodes=2)
    def test_fenced_member_epoch(self):
        """
        A heartbeat carrying a stale epoch (lower than the coordinator's
        current epoch for that member) must be rejected with
        FENCED_MEMBER_EPOCH (error 110).
        """
        with _HeartbeatClient(_first_broker(self.redpanda)) as c:
            r0 = c.heartbeat(HeartbeatRequest(group_id="fence-group"))
            assert r0.error_code == _ERR_NONE
            member_id = r0.member_id

            # Advance to epoch 1
            r1 = c.heartbeat(
                HeartbeatRequest(group_id="fence-group",
                                 member_id=member_id,
                                 member_epoch=0))
            assert r1.error_code == _ERR_NONE
            assert r1.member_epoch == 1

            # Replay the old epoch 0 heartbeat — must be fenced
            stale = c.heartbeat(
                HeartbeatRequest(group_id="fence-group",
                                 member_id=member_id,
                                 member_epoch=0))

        assert stale.error_code == _ERR_FENCED_MEMBER_EPOCH, (
            f"expected FENCED_MEMBER_EPOCH ({_ERR_FENCED_MEMBER_EPOCH}), "
            f"got {stale.error_code}")

    @cluster(num_nodes=2)
    def test_unknown_member_id(self):
        """
        A heartbeat with a member_id that was never registered must be
        rejected with UNKNOWN_MEMBER_ID (error 25).  Uses a random UUID
        to guarantee the member_id is not in the group.
        """
        with _HeartbeatClient(_first_broker(self.redpanda)) as c:
            resp = c.heartbeat(
                HeartbeatRequest(
                    group_id="unknown-group",
                    member_id=str(uuid.uuid4()),
                    member_epoch=0,
                ))

        assert resp.error_code == _ERR_UNKNOWN_MEMBER_ID, (
            f"expected UNKNOWN_MEMBER_ID ({_ERR_UNKNOWN_MEMBER_ID}), "
            f"got {resp.error_code}")

    @cluster(num_nodes=2)
    def test_multiple_members_same_group(self):
        """
        Two concurrent members registering in the same group must each
        receive a unique member_id with epoch = 0.  Confirms the
        coordinator correctly scopes member identity within a group.
        """
        with _HeartbeatClient(_first_broker(self.redpanda)) as c1, \
             _HeartbeatClient(_first_broker(self.redpanda)) as c2:
            r1 = c1.heartbeat(HeartbeatRequest(group_id="multi-group"))
            r2 = c2.heartbeat(HeartbeatRequest(group_id="multi-group"))

        assert r1.error_code == _ERR_NONE
        assert r2.error_code == _ERR_NONE
        assert r1.member_epoch == 0
        assert r2.member_epoch == 0
        assert r1.member_id != r2.member_id, \
            "each member must receive a distinct member_id"

    @cluster(num_nodes=2)
    def test_members_isolated_across_groups(self):
        """
        The same member_id registered in one group must be treated as
        unknown in a different group.  Group boundaries are hard isolation
        boundaries in the coordinator.
        """
        with _HeartbeatClient(_first_broker(self.redpanda)) as c:
            r0 = c.heartbeat(HeartbeatRequest(group_id="group-a"))
            assert r0.error_code == _ERR_NONE
            member_id = r0.member_id

            # Use group-a's member_id against group-b
            cross = c.heartbeat(
                HeartbeatRequest(
                    group_id="group-b",
                    member_id=member_id,
                    member_epoch=0,
                ))

        assert cross.error_code == _ERR_UNKNOWN_MEMBER_ID, (
            f"member from group-a must be unknown in group-b, "
            f"got error {cross.error_code}")

    # ── request validation ────────────────────────────────────────────────────

    @cluster(num_nodes=2)
    def test_empty_group_id_rejected(self):
        """
        A heartbeat with an empty group_id must be rejected before the
        coordinator is consulted.  The exact error code may be
        INVALID_GROUP_ID (24) or any non-zero value.
        """
        with _HeartbeatClient(_first_broker(self.redpanda)) as c:
            resp = c.heartbeat(HeartbeatRequest(group_id=""))

        assert resp.error_code != _ERR_NONE, \
            "empty group_id must be rejected with a non-zero error code"

    # ── observability ─────────────────────────────────────────────────────────

    @cluster(num_nodes=2)
    def test_heartbeat_counter_increments(self):
        """
        kafka_nextgen_heartbeat_total must increase after each heartbeat
        processed by the coordinator.  This verifies that Prometheus metrics
        are registered and updated by the live code path.
        """
        def _total() -> float:
            samples = self.redpanda.metrics_sample(
                "kafka_nextgen_heartbeat_total")
            if samples is None:
                return 0.0
            return sum(s.value for s in samples.samples)

        with _HeartbeatClient(_first_broker(self.redpanda)) as c:
            r0 = c.heartbeat(HeartbeatRequest(group_id="metrics-group"))
            assert r0.error_code == _ERR_NONE
            member_id = r0.member_id
            before = _total()

            r1 = c.heartbeat(
                HeartbeatRequest(group_id="metrics-group",
                                 member_id=member_id,
                                 member_epoch=0))
            assert r1.error_code == _ERR_NONE

        wait_until(lambda: _total() > before,
                   timeout_sec=15,
                   backoff_sec=1,
                   err_msg="kafka_nextgen_heartbeat_total did not increment")

    @cluster(num_nodes=2)
    def test_state_transition_counter_increments(self):
        """
        kafka_nextgen_group_state_transitions_total must increase when a
        new member is registered, confirming that state transitions are
        counted and exposed as a metric.
        """
        def _total() -> float:
            samples = self.redpanda.metrics_sample(
                "kafka_nextgen_group_state_transitions_total")
            if samples is None:
                return 0.0
            return sum(s.value for s in samples.samples)

        before = _total()

        with _HeartbeatClient(_first_broker(self.redpanda)) as c:
            resp = c.heartbeat(
                HeartbeatRequest(group_id="transitions-group"))
            assert resp.error_code == _ERR_NONE

        wait_until(
            lambda: _total() > before,
            timeout_sec=15,
            backoff_sec=1,
            err_msg="kafka_nextgen_group_state_transitions_total did not "
            "increment after new member registration")
