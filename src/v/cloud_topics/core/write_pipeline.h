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

#include "cloud_topics/core/event_filter.h"
#include "cloud_topics/core/pipeline_stage.h"
#include "cloud_topics/core/write_request.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>

#include <exception>
#include <type_traits>

namespace experimental::cloud_topics::core {

struct write_request_process_result {
    /// Iteration should be stopped
    ss::stop_iteration stop_iteration{ss::stop_iteration::yes};
    /// Current write request is fully processed and should be moved to the next
    /// staged
    bool advance_next_stage{false};
};

struct write_pipeline_accessor;

template<class Clock = ss::lowres_clock>
class write_pipeline {
    friend struct write_pipeline_accessor;

public:
    write_pipeline();
    ~write_pipeline();

    /// Add write request to the pipeline
    ss::future<result<model::record_batch_reader>> write_and_debounce(
      model::ntp ntp,
      model::record_batch_reader r,
      std::chrono::milliseconds timeout);

    /// List of write requests which are ready to be uploaded
    struct size_limited_write_req_list {
        /// Write requests list which are ready for
        /// upload or expired.
        core::write_request_list<Clock> ready;
        /// If the batcher contains more write requests which
        /// were not included because the size limit was reached
        /// this field will be set to false.
        bool complete{true};
        /// Total size of all listed write requests.
        size_t size_bytes{0};
    };

    /// Get write requests atomically.
    /// The total size of returned write requests and the stage to which they
    /// belong to should be specified.
    size_limited_write_req_list
    get_write_requests(size_t max_bytes, pipeline_stage stage);

    /// Apply lambda function to every write request at certain stage.
    /// The lambda should return 'write_request_processing_result'.
    /// The 'drop_error_code' is used to ack write requests which has to be
    /// dropped. The write request is not removed but instead the error
    /// is acknowledged which makes the write request go out of scope.
    /// When this happens the write request is unlinked from the list.
    template<class Fn>
    requires std::is_nothrow_invocable_r_v<
      checked<write_request_process_result, errc>,
      Fn,
      write_request<Clock>&>
    void process_stage(Fn&& fn, pipeline_stage stage) {
        std::set<pipeline_stage> stages;
        for (auto& req : _pending) {
            if (req.stage == stage) {
                checked<write_request_process_result, errc> r = fn(req);
                if (r.has_error()) {
                    // Drop write request using the error code from the result
                    req.set_value(r.error());
                    continue;
                }
                if (r.value().advance_next_stage) {
                    req.stage = _stages.next_stage(req.stage);
                    stages.insert(req.stage);
                }
                if (r.value().stop_iteration == ss::stop_iteration::yes) {
                    break;
                }
            }
        }
        // Notify event filters
        for (const auto stage : stages) {
            signal(stage);
        }
    }

    /// Subscribe to events of certain type
    ///
    /// The returned future will become ready when new data will be added to the
    /// pipeline or when the shutdown even will occur.
    ss::future<event> subscribe(event_filter<Clock>& flt) noexcept;
    ss::future<event>
    subscribe(event_filter<Clock>& flt, ss::abort_source& as) noexcept;

    /// Return write request which was already been in the pipeline
    /// before back into the pipeline.
    /// The method allows to reenqueue requests returned by get_write_requests
    /// method.
    void reenqueue(write_request<Clock>&);

    /// Get next pipeline stage id
    pipeline_stage register_pipeline_stage() noexcept;

private:
    /// Find all timed out write requests and remove them from the list
    /// atomically.
    void remove_timed_out_write_requests();

    /// Signal all active filters
    void signal(pipeline_stage stage);

    core::write_request_list<Clock> _pending;
    ss::gate _gate;
    ss::abort_source _as;
    // Current bytes (gauge)
    size_t _current_size{0};
    // Total bytes went through the pipeline
    size_t _bytes_total{0};
    // Semaphore that represents memory budget that we have
    ssx::named_semaphore<Clock> _mem_budget;
    // All waiters
    event_filter<Clock>::event_filter_list _filters;
    // Pipeline stages
    pipeline_stage_container _stages;
};
} // namespace experimental::cloud_topics::core
