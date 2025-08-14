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

#include "cloud_topics/level_zero/pipeline/base_pipeline.h"
#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/pipeline/write_request.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/util/optimized_optional.hh>

#include <exception>
#include <functional>
#include <type_traits>

namespace experimental::cloud_topics::l0 {

struct write_request_process_result {
    /// Iteration should be stopped
    ss::stop_iteration stop_iteration{ss::stop_iteration::yes};
    /// Current write request is fully processed and should be moved to the next
    /// staged
    bool advance_next_stage{false};
};

struct write_pipeline_accessor;

template<class Clock = ss::lowres_clock>
class write_pipeline
  : public base_pipeline<write_request<Clock>, write_pipeline<Clock>, Clock> {
    friend struct write_pipeline_accessor;

public:
    write_pipeline();
    ~write_pipeline();

    ss::sstring pipeline_name() const { return "write_pipeline"; }

    /// Add write request to the pipeline
    ss::future<result<chunked_vector<extent_meta>>> write_and_debounce(
      model::ntp ntp,
      chunked_vector<model::record_batch> batches,
      Clock::time_point timeout);

    using write_requests_list
      = requests_list<write_pipeline<Clock>, write_request<Clock>>;

    struct stage {
        stage(write_pipeline<Clock>*, pipeline_stage);

        bool stopped() const noexcept;

        /// Return write request back into the pipeline.
        /// The write request advances to the next stage of the
        /// pipeline.
        void push_next_stage(write_request<Clock>&);

        /// Extract write requests out of the pipeline atomically.
        /// The caller is responsible for handling each write request.
        /// The request could be either returned using 'push_next_stage'
        /// method.
        write_requests_list pull_write_requests(size_t max_bytes);

        /// Wait until either the 'deadline' is reached or the pipeline
        /// accumulated 'max_bytes' bytes
        ss::future<checked<event, errc>> wait_until(
          size_t max_bytes,
          Clock::time_point deadline,
          ss::abort_source* as = nullptr) noexcept;

        /// Wait until the next write_request is added to the pipeline
        ss::future<checked<event, errc>>
        wait_next(ss::abort_source* as = nullptr) noexcept;

        /// Apply lambda function to every write request at certain stage.
        /// The lambda should return 'write_request_processing_result'.
        /// The 'drop_error_code' is used to ack write requests which has to be
        /// dropped. The write request is not removed but instead the error
        /// is acknowledged which makes the write request go out of scope.
        /// When this happens the write request is unlinked from the list.
        template<class Fn>
        requires std::is_nothrow_invocable_r_v<
          checked<request_processing_result, errc>,
          Fn,
          write_request<Clock>&>
        void process(Fn&& fn) {
            auto next_stage = _parent->next_stage(_ps);
            uint32_t count = 0;
            for (auto& req : _parent->get_pending()) {
                if (req.stage == _ps) {
                    checked<request_processing_result, errc> r = fn(req);
                    if (r.has_error()) {
                        // Drop write request using the error code from the
                        // result
                        req.set_value(r.error());
                        continue;
                    }
                    switch (r.value()) {
                    case request_processing_result::advance_and_continue:
                        req.stage = _parent->next_stage(req.stage);
                        count++;
                        continue;
                    case request_processing_result::advance_and_stop:
                        req.stage = _parent->next_stage(req.stage);
                        count++;
                        break;
                    case request_processing_result::ignore_and_continue:
                        continue;
                    case request_processing_result::ignore_and_stop:
                        break;
                    }
                }
            }
            // Notify event filters
            if (count) {
                _parent->signal(next_stage);
            }
        }

        pipeline_stage id() const noexcept { return _ps; }

    private:
        /// Pick the right abort source to use.
        ///
        /// If the 'maybe_as' is not null use it but also subscribe to the root
        /// abort_source so it'd be aborted if the root is aborted. If
        /// 'maybe_as' is null then use root abort source. The subscription is
        /// nullopt in this case.
        /// The caller should keep the subscription for the duration of the
        /// async call that uses the abort source.
        std::pair<
          ss::optimized_optional<ss::abort_source::subscription>,
          ss::abort_source*>
        choose_abort_source(ss::abort_source* maybe_as);

        write_pipeline<Clock>* _parent;
        pipeline_stage _ps;
    };

    /// Get next pipeline stage id
    stage register_write_pipeline_stage() noexcept;

    void signal(pipeline_stage stage);

    event trigger_event(pipeline_stage stage);

private:
    /// Get write requests atomically.
    /// The total size of returned write requests and the stage to which they
    /// belong to should be specified.
    write_requests_list
    get_write_requests(size_t max_bytes, pipeline_stage stage);

    /// Return write request which was already been in the pipeline
    /// before back into the pipeline.
    /// The method allows to reenqueue requests returned by get_write_requests
    /// method.
    void reenqueue(write_request<Clock>&);

    // Current bytes (gauge)
    size_t _current_size{0};
    // Total bytes went through the pipeline
    size_t _bytes_total{0};
    // Semaphore that represents memory budget that we have
    ssx::named_semaphore<Clock> _mem_budget;
};
} // namespace experimental::cloud_topics::l0
