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

#include "absl/container/node_hash_map.h"
#include "base/format_to.h"
#include "kafka/protocol/schemata/offset_commit_request.h"
#include "model/fundamental.h"

#include <iosfwd>

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

    void reset_offsets() { _offsets.clear(); }
    kafka::fetch_session_id id() const { return _id; }
    void id(kafka::fetch_session_id id) { _id = id; }
    kafka::fetch_session_epoch epoch() const { return _epoch; }
    model::offset offset(model::topic_partition_view tpv) const;

    /// \brief Whether a fetch position is tracked for a partition.
    ///
    /// Distinguishes a genuinely-tracked offset of 0 from an absent one, which
    /// offset() cannot (it returns 0 for both). Used to decide which assigned
    /// partitions are still initializing and need seeding from the committed
    /// offset.
    bool has_offset(model::topic_partition_view tpv) const;

    /// \brief Directly set the tracked fetch offset for a partition, e.g. to
    /// recover from offset_out_of_range by seeking to the log_start_offset
    /// the broker reported (pandaproxy only allows
    /// auto.offset.reset=earliest).
    void reseed(model::topic_partition_view tpv, model::offset new_offset);

    /// \brief Update session state from a fetch response delivered to the
    /// caller.
    ///
    /// The session epoch advances: the broker has processed the request and
    /// advanced its side of the session. Offsets of partitions that returned
    /// records advance too.
    void apply(fetch_response& res);

    /// \brief Update session state from a fetch response that is being
    /// discarded and re-fetched, e.g. because a sibling broker's fetch in the
    /// same round failed.
    ///
    /// The session epoch still advances, to stay in step with the
    /// broker-side session state, but offsets of partitions that returned
    /// records do NOT advance: those records were never delivered to the
    /// caller, and advancing past them would make the retry skip them,
    /// silently losing data.
    void discard(fetch_response& res);

    std::vector<kafka::offset_commit_request_topic>
    make_offset_commit_request() const;

    fmt::iterator format_to(fmt::iterator it) const;

private:
    /// \brief Common session bookkeeping shared by apply() and
    /// discard(): seed/validate the session id and advance the
    /// epoch, since the broker has processed the request regardless of what
    /// happens to the response on our side.
    void update_session_state(const fetch_response& res);

    /// \brief Compact each partition-offset map to its current size, e.g.
    /// after reseed() or apply() may have grown it.
    void rehash_offsets();

    kafka::fetch_session_id _id{kafka::invalid_fetch_session_id};
    kafka::fetch_session_epoch _epoch{kafka::initial_fetch_session_epoch};
    absl::node_hash_map<
      model::topic,
      absl::node_hash_map<model::partition_id, model::offset>>
      _offsets;
};

} // namespace kafka::client
