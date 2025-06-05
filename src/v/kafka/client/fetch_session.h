/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "container/chunked_hash_map.h"
#include "kafka/protocol/schemata/fetch_request.h"
#include "kafka/protocol/schemata/offset_commit_request.h"
#include "model/fundamental.h"

#include <absl/container/node_hash_map.h>

#include <iosfwd>
#include <ostream>

namespace kafka {
struct fetch_response;
}

namespace kafka::client {

/// \brief Maintain state for consumer group fetch session.
class fetch_session {
public:
    fetch_session() = default;
    fetch_session(const fetch_session&) = delete;
    fetch_session(fetch_session&&) = default;
    fetch_session& operator=(const fetch_session&) = delete;
    fetch_session& operator=(fetch_session&&) = default;
    ~fetch_session() = default;

    bool empty() const { return _partitions.empty(); }
    kafka::fetch_session_id id() const { return _id; }
    void id(kafka::fetch_session_id id) { _id = id; }
    kafka::fetch_session_epoch epoch() const { return _epoch; }
    model::offset offset(model::topic_partition_view tpv) const;
    bool apply(fetch_response& res);
    void fill_fetch_add_partition(
      fetch_request&, model::topic_partition_view, model::offset);
    void fill_fetch_complete(fetch_request&);
    std::vector<kafka::offset_commit_request_topic>
    make_offset_commit_request() const;

    friend std::ostream& operator<<(std::ostream& os, const fetch_session&);

private:
    kafka::fetch_session_id _id{kafka::invalid_fetch_session_id};
    kafka::fetch_session_epoch _epoch{kafka::initial_fetch_session_epoch};

    struct partition_state {
        model::offset sent_fetch_offset{};
        model::offset acked_sent_fetch_offset{};

        // TODO: extract this to outside of fetch sessions
        model::offset recv_fetch_offset{};

        friend std::ostream& operator<<(std::ostream& os, partition_state ps) {
            fmt::print(
              os,
              "{{sent_fetch_offset={}, acked_sent_fetch_offset={}, "
              "recv_fetch_offset={}}}",
              ps.sent_fetch_offset,
              ps.acked_sent_fetch_offset,
              ps.recv_fetch_offset);
            return os;
        }
    };

    absl::node_hash_map<
      model::topic,
      absl::node_hash_map<model::partition_id, partition_state>>
      _partitions;

    chunked_hash_map<model::topic, chunked_hash_set<model::partition_id>>
      _next_fetch_added;
};

} // namespace kafka::client
