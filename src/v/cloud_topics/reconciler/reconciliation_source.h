/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/data_plane_api.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"

#include <seastar/core/shared_ptr.hh>

#include <expected>

namespace cluster {
class partition;
}

namespace cloud_topics::reconciler {

// An abstraction for what the reconciler reads from, as well as the source for
// bookkeeping progress that has been made.
class source {
public:
    // Error codes for a reconcilation source.
    enum class errc : uint8_t {
        timeout,
        not_leader,
        shutdown,
        failure,
    };

    source(model::ntp ntp, model::topic_id_partition tidp)
      : _ntp(std::move(ntp))
      , _tidp(tidp) {}
    source(const source&) = delete;
    source(source&&) = delete;
    source& operator=(const source&) = delete;
    source& operator=(source&&) = delete;
    virtual ~source() = default;

    // The NTP for this source
    const model::ntp& ntp() const { return _ntp; }
    // The topic ID + partition for the source
    const model::topic_id_partition& topic_id_partition() const {
        return _tidp;
    }

    // Returns true if there may be new data to reconcile (LSO > LRO).
    virtual bool has_pending_data() = 0;

    // Get the last reconciled offset for this source, or kafka::offset::min()
    // if none.
    virtual kafka::offset last_reconciled_offset() = 0;

    // Set the last reconciled offset for this source.
    //
    // This operation is idempotent, the last reconciled offset never moves
    // back.
    virtual ss::future<std::expected<void, errc>>
    set_last_reconciled_offset(kafka::offset, ss::abort_source&) = 0;

    struct reader_config {
        // The offset to start reading at.
        kafka::offset start_offset;
        // The soft limit for number of bytes to read.
        size_t max_bytes;
        // The abort source for when to stop the reader. The abort source must
        // live as long as the returned reader from `make_reader`.
        ss::abort_source* as;
    };

    // Create a reader for the reconciliation source, data should only be read
    // above `last_reconciled_offset`.
    //
    // It *is* valid for this reader to outlive `source`.
    virtual ss::future<model::record_batch_reader>
      make_reader(reader_config) = 0;

private:
    model::ntp _ntp;
    model::topic_id_partition _tidp;
};

// Make a reconciliation source from L0 components (data plane) and the cluster
// partition (containing metadata).
ss::shared_ptr<source> make_source(
  model::ntp,
  model::topic_id_partition,
  data_plane_api*,
  ss::lw_shared_ptr<cluster::partition>);

} // namespace cloud_topics::reconciler

template<>
struct fmt::formatter<cloud_topics::reconciler::source::errc>
  : fmt::formatter<std::string_view> {
    auto format(
      const cloud_topics::reconciler::source::errc&,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};
