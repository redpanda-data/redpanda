/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cloud_topics/batcher/serializer.h"
#include "cloud_topics/errc.h"
#include "model/record.h"

#include <seastar/core/weak_ptr.hh>

namespace experimental::cloud_topics::details {

using batcher_req_index = named_type<int64_t, struct batcher_req_index_tag>;

// This object is created for every produce request. It may contain
// multiple batches.
template<class Clock>
struct write_request : ss::weakly_referencable<write_request<Clock>> {
    using timestamp_t = Clock::time_point;
    /// Target NTP
    model::ntp ntp;
    /// Monotonically increasing write request index
    batcher_req_index index;
    /// Serialized record batches
    serialized_chunk data_chunk;
    /// Timestamp of the data ingestion
    timestamp_t ingestion_time;
    /// At this point in time we need to reply to the client
    /// with timeout error
    timestamp_t expiration_time;
    /// List of all write requests
    intrusive_list_hook _hook;

    using response_t = result<ss::circular_buffer<model::record_batch>>;
    /// The promise is used to signal to the caller
    /// after the upload is completed
    ss::promise<response_t> response;

    ~write_request() = default;
    write_request(const write_request&) = delete;
    write_request& operator=(const write_request&) = delete;
    write_request(write_request&& other) noexcept = default;
    write_request& operator=(write_request&& other) noexcept = default;

    /// Create write_request that contains buffered data and the promise.
    /// The object can't be copied to another shard directly.
    write_request(
      model::ntp ntp,
      batcher_req_index index,
      serialized_chunk chunk,
      std::chrono::milliseconds timeout);

    size_t size_bytes() const noexcept;

    void set_value(errc e) noexcept;

    void
    set_value(ss::circular_buffer<model::record_batch> placeholders) noexcept;

    bool has_expired() const noexcept;
};

template<class Clock>
using write_request_list
  = intrusive_list<write_request<Clock>, &write_request<Clock>::_hook>;

} // namespace experimental::cloud_topics::details
