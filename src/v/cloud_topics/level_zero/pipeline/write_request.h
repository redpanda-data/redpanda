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

#include "base/seastarx.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/pipeline/serializer.h"
#include "container/chunked_vector.h"
#include "container/intrusive_list_helpers.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/weak_ptr.hh>

#include <expected>

namespace cloud_topics::l0 {

// This object is created for every produce request. It may contain
// multiple batches.
template<class Clock = ss::lowres_clock>
struct write_request : ss::weakly_referencable<write_request<Clock>> {
    using timestamp_t = Clock::time_point;
    /// Target NTP
    model::ntp ntp;
    /// The NTPs topic start epoch for GC (i.e. topic revision id)
    cluster_epoch topic_start_epoch;
    /// Serialized record batches
    serialized_chunk data_chunk;
    /// Timestamp of the data ingestion
    timestamp_t ingestion_time;
    /// At this point in time we need to reply to the client
    /// with timeout error
    timestamp_t expiration_time;
    /// Current pipeline stage
    /// Use write_pipeline::advance_next_stage to change stage.
    pipeline_stage stage;
    /// List of all write requests
    intrusive_list_hook _hook;

    using response_t = std::expected<upload_meta, errc>;
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
      cluster_epoch topic_start_epoch,
      serialized_chunk chunk,
      timestamp_t timeout,
      pipeline_stage stage = unassigned_pipeline_stage);

    size_t size_bytes() const noexcept;

    void set_value(errc e) noexcept;

    void set_value(upload_meta meta) noexcept;

    bool has_expired() const noexcept;
};

template<class Clock>
using write_request_list
  = intrusive_list<write_request<Clock>, &write_request<Clock>::_hook>;

} // namespace cloud_topics::l0
