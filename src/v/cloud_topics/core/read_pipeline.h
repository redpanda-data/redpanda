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

#include "cloud_topics/core/circuit_breaker.h"
#include "cloud_topics/core/event_filter.h"
#include "cloud_topics/core/pipeline_stage.h"
#include "cloud_topics/core/read_request.h"
#include "model/record_batch_reader.h"
#include "ssx/semaphore.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>

namespace experimental::cloud_topics::core {

//
class pipeline_abort_requested : public ss::abort_requested_exception {
public:
    const char* what() const noexcept override {
        return "pipeline abort requested";
    }
};

struct read_pipeline_accessor;

template<class Clock = ss::lowres_clock>
class read_pipeline {
    friend struct read_pipeline_accessor;

public:
    read_pipeline();

    ss::future<model::record_batch_reader> make_reader();

    /// Subscribe to events of certain type
    ///
    /// The returned future will become ready when new data will be added to the
    /// pipeline or when the shutdown even will occur.
    ss::future<event> subscribe(event_filter<Clock>& flt) noexcept;
    ss::future<event>
    subscribe(event_filter<Clock>& flt, ss::abort_source& as) noexcept;

    /// Make log reader config
    ss::future<result<read_request_fetch_result>> make_reader(
      model::ntp ntp,
      storage::log_reader_config cfg,
      std::chrono::milliseconds timeout);

    /// Make timequery
    ss::future<result<read_request_timequery_result>> timequery(
      model::ntp ntp,
      storage::timequery_config cfg,
      std::chrono::milliseconds timeout);

    // TODO: add metadata requests (last term for offset, start offset, etc)

    pipeline_stage register_pipeline_stage() noexcept;

    struct read_requests_list {
        core::read_request_list<Clock> ready;
        bool complete{true};
    };

    /// Get list of read requests which are ready to be processed.
    /// Limit by memory required to handle these requests.
    read_requests_list
    get_fetch_requests(size_t max_bytes, pipeline_stage stage);

    /// Get root retry chain node to use with async
    /// operations.
    basic_retry_chain_node<Clock>& get_root_rtc() { return _root_rtc; }

    /// Register read-path errors
    void register_pipeline_error(errc);

    ss::future<> stop();

private:
    /// Find all timed out fetch requests and remove them from the list
    /// atomically.
    void remove_timed_out_fetch_requests();

    /// Signal all active filters
    void signal(pipeline_stage stage);

    core::read_request_list<Clock> _pending;
    ss::gate _gate;

    // Total size of all fetch requests (estimated using max_bytes)
    size_t _current_size{0};

    // Total bytes went through the pipeline
    size_t _bytes_total{0};

    ssx::named_semaphore<Clock> _mem_quota;

    ss::abort_source _as;
    basic_retry_chain_node<Clock> _root_rtc;
    basic_retry_chain_logger<Clock> _logger;

    event_filter<Clock>::event_filter_list _filters;

    pipeline_stage_container _stages;
    circuit_breaker<Clock> _breaker;
};
} // namespace experimental::cloud_topics::core
