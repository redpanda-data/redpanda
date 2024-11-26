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
#include "cloud_topics/core/pipeline_stage.h"
#include "cloud_topics/errc.h"
#include "container/fragmented_vector.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/weak_ptr.hh>

namespace experimental::cloud_topics::core {

struct read_request_fetch_result {
    model::record_batch_reader reader;
    fragmented_vector<model::tx_range> tx;
};

using read_request_timequery_result = storage::timequery_result;

using read_request_result
  = std::variant<read_request_fetch_result, read_request_timequery_result>;
using read_request_query
  = std::variant<storage::log_reader_config, storage::timequery_config>;

// This object is created for every fetch request.
// The main processing steps are:
// - read underlying partition
// - materialize placeholder batches
// - read dl_stm state
// - materialize dl_overlay batches
template<class Clock = ss::lowres_clock>
struct read_request : ss::weakly_referencable<read_request<Clock>> {
    using timestamp_t = Clock::time_point;
    /// Target NTP
    model::ntp ntp;
    /// Log reader config or timequery config
    read_request_query query;
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

    using response_t = checked<read_request_result, errc>;

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
    /// \param query is either a reader config or timequery config
    /// \param read_quota contains semaphore units that represent memory that
    ///        request is allowed to use
    /// \param timeout is a time quota
    /// \param stage is a current pipeline stage (unassigned by default)
    read_request(
      model::ntp ntp,
      read_request_query query,
      std::chrono::milliseconds timeout,
      basic_retry_chain_node<Clock>* root,
      pipeline_stage stage = unassigned_pipeline_stage);

    bool is_timequery() const noexcept;

    void set_value(errc e) noexcept;

    void set_value(read_request_result result) noexcept;

    bool has_expired() const noexcept;

    /// Get log reader config.
    /// This method can only be called if the request is not
    /// a timequery.
    storage::log_reader_config get_log_reader_config() const;
};

template<class Clock>
using read_request_list
  = intrusive_list<read_request<Clock>, &read_request<Clock>::_hook>;

} // namespace experimental::cloud_topics::core
