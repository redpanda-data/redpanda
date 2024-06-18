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
#include "cluster/tm_stm_types.h"

namespace cluster {

std::string_view tx_metadata::get_status() const {
    switch (status) {
    case tx_status::ongoing:
        return "ongoing";
    case tx_status::preparing:
        return "preparing";
    case tx_status::prepared:
        return "prepared";
    case tx_status::aborting:
        return "aborting";
    case tx_status::killed:
        return "killed";
    case tx_status::ready:
        return "ready";
    case tx_status::tombstone:
        return "tombstone";
    }
}

std::string_view tx_metadata::get_kafka_status() const {
    switch (status) {
    case tx_status::ongoing: {
        if (groups.empty() && partitions.empty()) {
            return "Empty";
        }
        return "Ongoing";
    }
    case tx_status::preparing:
        return "Ongoing";
    case tx_status::prepared:
        return "PrepareCommit";
    case tx_status::aborting:
        return "PrepareAbort";
    case tx_status::killed:
        // https://issues.apache.org/jira/browse/KAFKA-6119
        // https://github.com/apache/kafka/commit/501a5e262702bcc043724cb9e1f536e16a66399e
        return "PrepareEpochFence";
    case tx_status::ready:
        return "Empty";
    case tx_status::tombstone:
        return "Dead";
    }
}

std::chrono::milliseconds tx_metadata::get_staleness() const {
    auto now = ss::lowres_system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
      now - last_update_ts);
}

bool tx_metadata::delete_partition(const tx_partition& part) {
    return std::erase_if(
             partitions,
             [part](const auto& partition) {
                 return partition.ntp == part.ntp
                        && partition.etag == part.etag;
             })
           > 0;
}

std::ostream& operator<<(std::ostream& o, tx_status status) {
    switch (status) {
    case ongoing:
        return o << "ongoing";
    case aborting:
        return o << "aborting";
    case preparing:
        return o << "preparing";
    case prepared:
        return o << "prepared";
    case killed:
        return o << "aborting";
    case ready:
        return o << "ready";
    case tombstone:
        return o << "tombstone";
    }
}
std::ostream& operator<<(std::ostream& o, const tx_metadata::tx_partition& tp) {
    fmt::print(
      o,
      "{{ntp: {}, etag: {}, revision: {}}}",
      tp.ntp,
      tp.etag,
      tp.topic_revision);
    return o;
}

std::ostream& operator<<(std::ostream& o, const tx_metadata& tx) {
    fmt::print(
      o,
      "{{id: {}, status: {}, pid: {}, last_pid: {}, etag: {}, seq: {}, "
      "partitions: {}}}",
      tx.id,
      tx.status,
      tx.pid,
      tx.last_pid,
      tx.etag,
      tx.tx_seq,
      fmt::join(tx.partitions, ", "));
    return o;
}

} // namespace cluster
