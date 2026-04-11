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
#include "container/chunked_vector.h"
#include "container/intrusive_list_helpers.h"
#include "model/record.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/weak_ptr.hh>

#include <expected>

namespace cloud_topics::l0 {

/// The result of the read request processing.
/// Contains raft_data batches.
struct dataplane_query_result {
    chunked_vector<model::record_batch> results;
};

/// The query for the data-plane.
/// The meta field contains a bunch of ctp_placeholder batches.
struct dataplane_query {
    size_t output_size_estimate{0};
    chunked_vector<extent_meta> meta;
    allow_materialization_failure allow_mat_failure
      = allow_materialization_failure::no;
};

// This object is created for every fetch request.
// The main processing steps are:
// - materialize placeholder batches
//
// The input is a series of placeholder batches.
template<class Clock = ss::lowres_clock>
struct read_request : ss::weakly_referencable<read_request<Clock>> {
    using timestamp_t = Clock::time_point;
    /// Target NTP
    model::ntp ntp;
    /// Log reader config or timequery config
    dataplane_query query;
    /// Timestamp of the data ingestion
    timestamp_t ingestion_time;
    /// Fetch request processing deadline
    timestamp_t expiration_time;
    /// List of all write requests
    intrusive_list_hook _hook;
    /// Current pipeline stage
    pipeline_stage stage;
    /// Retry chain node inherited from the pipeline
    basic_retry_chain_node<Clock> rtc;
    /// Per-request logger
    basic_retry_chain_logger<Clock> rtc_logger;

    using response_t = std::expected<dataplane_query_result, errc>;

    /// The promise is used to signal to the caller
    /// after the data is fetched
    ss::promise<response_t> response;

    ~read_request() = default;
    read_request(const read_request&) = delete;
    read_request& operator=(const read_request&) = delete;
    read_request(read_request&& other) noexcept = default;
    read_request& operator=(read_request&& other) noexcept = default;

    /// C-tor
    /// \param ntp is a target NTP
    /// \param query is either a reader that contains a bunch of
    ///        placeholder/overlay values
    /// \param read_quota contains semaphore units that represent
    ///        memory that request is allowed to use
    /// \param timeout is a time quota
    /// \param stage is a current pipeline stage (unassigned by default)
    read_request(
      model::ntp ntp,
      dataplane_query query,
      timestamp_t timeout,
      basic_retry_chain_node<Clock>* root,
      pipeline_stage stage = unassigned_pipeline_stage);

    void set_value(errc e) noexcept;

    void set_value(dataplane_query_result result) noexcept;

    bool has_expired() const noexcept;
};

template<class Clock>
using read_request_list
  = intrusive_list<read_request<Clock>, &read_request<Clock>::_hook>;

} // namespace cloud_topics::l0
