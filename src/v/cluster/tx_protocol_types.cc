/**
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/tx_protocol_types.h"

#include "utils/to_string.h"

#include <fmt/format.h>
namespace cluster {

std::ostream& operator<<(std::ostream& o, const commit_tx_request& r) {
    fmt::print(
      o,
      "{{ntp {} pid {} tx_seq {} timeout {}}}",
      r.ntp,
      r.pid,
      r.tx_seq,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const commit_tx_reply& r) {
    fmt::print(o, "{{ec {}}}", r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const abort_tx_request& r) {
    fmt::print(
      o,
      "{{ntp {} pid {} tx_seq {} timeout {}}}",
      r.ntp,
      r.pid,
      r.tx_seq,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const abort_tx_reply& r) {
    fmt::print(o, "{{ec {}}}", r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const begin_group_tx_request& r) {
    fmt::print(
      o,
      "{{ntp {} group_id {} pid {} tx_seq {} timeout {} tm_partition: {}}}",
      r.ntp,
      r.group_id,
      r.pid,
      r.tx_seq,
      r.timeout,
      r.tm_partition);
    return o;
}

std::ostream& operator<<(std::ostream& o, const begin_group_tx_reply& r) {
    fmt::print(o, "{{etag {} ec {}}}", r.etag, r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const prepare_group_tx_request& r) {
    fmt::print(
      o,
      "{{ntp {} group_id {} etag {} pid {} tx_seq {} timeout {}}}",
      r.ntp,
      r.group_id,
      r.etag,
      r.pid,
      r.tx_seq,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const prepare_group_tx_reply& r) {
    fmt::print(o, "{{ec {}}}", r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const commit_group_tx_request& r) {
    fmt::print(
      o,
      "{{ntp {} pid {} tx_seq {} group_id {} timeout {}}}",
      r.ntp,
      r.pid,
      r.tx_seq,
      r.group_id,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const commit_group_tx_reply& r) {
    fmt::print(o, "{{ec {}}}", r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const abort_group_tx_request& r) {
    fmt::print(
      o,
      "{{ntp {} pid {} tx_seq {} group_id {} timeout {}}}",
      r.ntp,
      r.pid,
      r.tx_seq,
      r.group_id,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const abort_group_tx_reply& r) {
    fmt::print(o, "{{ec {}}}", r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const find_coordinator_request& r) {
    fmt::print(o, "{{tid {}}}", r.tid);
    return o;
}

std::ostream& operator<<(std::ostream& o, const find_coordinator_reply& r) {
    fmt::print(
      o, "{{coordinator {} ntp {} ec {}}}", r.coordinator, r.ntp, r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const fetch_tx_reply& r) {
    fmt::print(
      o,
      "{{ec: {}, pid: {}, last_pid: {}, tx_seq: {}, timeout_ms: {}, status: "
      "{}, partitions: {}, groups: {}}}",
      r.ec,
      r.pid,
      r.last_pid,
      r.tx_seq,
      r.timeout_ms.count(),
      r.status,
      r.partitions,
      r.groups);
    return o;
}

std::ostream& operator<<(std::ostream& o, const fetch_tx_reply::tx_group& g) {
    fmt::print(o, "{{etag: {}, group_id: {}}}", g.etag, g.group_id);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const fetch_tx_reply::tx_partition& p) {
    fmt::print(
      o,
      "{{etag: {}, ntp: {}, revision: {}}}",
      p.etag,
      p.ntp,
      p.topic_revision);

    return o;
}

std::ostream&
operator<<(std::ostream& o, const add_partitions_tx_request::topic& t) {
    fmt::print(o, "{{topic: {}, partitions: {}}}", t.name, t.partitions);
    return o;
}

std::ostream& operator<<(std::ostream& o, const begin_tx_request& r) {
    fmt::print(
      o,
      "{{ ntp: {}, pid: {}, tx_seq: {}, tm_partition: {}}}",
      r.ntp,
      r.pid,
      r.tx_seq,
      r.tm_partition);
    return o;
}

std::ostream& operator<<(std::ostream& o, const begin_tx_reply& r) {
    fmt::print(o, "{{ ntp: {}, etag: {}, ec: {} }}", r.ntp, r.etag, r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const prepare_tx_request& r) {
    fmt::print(
      o,
      "{{ ntp: {}, etag: {}, tm: {}, pid: {}, tx_seq: {}, timeout: {} }}",
      r.ntp,
      r.etag,
      r.tm,
      r.pid,
      r.tx_seq,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const prepare_tx_reply& r) {
    fmt::print(o, "{{ ec: {} }}", r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const init_tm_tx_request& r) {
    fmt::print(
      o,
      "{{ tx_id: {}, transaction_timeout_ms: {}, timeout: {} }}",
      r.tx_id,
      r.transaction_timeout_ms,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const init_tm_tx_reply& r) {
    fmt::print(o, "{{ pid: {}, ec: {} }}", r.pid, r.ec);
    return o;
}

std::ostream& operator<<(std::ostream& o, const try_abort_request& r) {
    fmt::print(
      o,
      "{{ tm: {}, pid: {}, tx_seq: {}, timeout: {} }}",
      r.tm,
      r.pid,
      r.tx_seq,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const try_abort_reply& r) {
    fmt::print(
      o,
      "{{ commited: {}, aborted: {}, ec: {} }}",
      bool(r.commited),
      bool(r.aborted),
      r.ec);
    return o;
}
} // namespace cluster
