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

#include "cloud_topics/level_zero/common/level_zero_probe.h"
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

#include <array>
#include <exception>
#include <expected>
#include <functional>
#include <type_traits>

namespace cloud_topics::l0 {

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
    /// The revision id is the topic creation revision id for the ntp.
    ss::future<std::expected<upload_meta, std::error_code>> write_and_debounce(
      model::ntp ntp,
      cluster_epoch min_epoch,
      chunked_vector<model::record_batch> batches,
      Clock::time_point timeout);

    struct prepared_data {
        serialized_chunk data_chunk;
        ss::semaphore_units<ss::named_semaphore_exception_factory, Clock>
          mem_units;
        ss::semaphore_units<ss::named_semaphore_exception_factory, Clock>
          req_units;
    };

    ss::future<std::expected<prepared_data, std::error_code>>
    prepare_write(chunked_vector<model::record_batch> batches);

    ss::future<std::expected<upload_meta, std::error_code>> execute_write(
      model::ntp ntp,
      cluster_epoch min_epoch,
      prepared_data prepped,
      Clock::time_point timeout);

    using write_requests_list
      = requests_list<write_pipeline<Clock>, write_request<Clock>>;

    struct stage {
        stage(write_pipeline<Clock>*, pipeline_stage);

        bool stopped() const noexcept;

        /// Return write request back into the pipeline.
        /// The write request advances to the next stage of the
        /// pipeline.
        /// \param r Write request to reenqueue
        /// \param signal If true signal the next stage that new write request
        void push_next_stage(write_request<Clock>& r, bool signal = true);

        /// Notify the next stage that new write requests are available.
        /// This method should be invoked after 'push_next_stage(..., false)'
        /// to avoid stalling the pipeline.
        void signal_next_stage();

        /// Enqueue a foreign (cross-shard proxied) write request directly to
        /// the next stage without byte accounting. This is used for requests
        /// that were accounted for on a different shard.
        /// \param r Write request to enqueue (must have unassigned stage)
        /// \param signal If true signal the next stage
        void enqueue_foreign_request(write_request<Clock>& r, bool signal);

        /// Extract write requests out of the pipeline atomically.
        /// The caller is responsible for handling each write request.
        /// The request could be either returned using 'push_next_stage'
        /// method.
        /// \param max_bytes Maximum number of bytes to extract
        /// \param max_requests Maximum number of requests to extract
        /// \return List of write requests that were extracted
        write_requests_list pull_write_requests(
          size_t max_bytes,
          size_t max_requests = std::numeric_limits<size_t>::max());

        /// Wait until either the 'deadline' is reached or the pipeline
        /// accumulated 'max_bytes' bytes
        ss::future<std::expected<event, errc>> wait_until(
          size_t max_bytes,
          Clock::time_point deadline,
          ss::abort_source* as = nullptr) noexcept;

        /// Wait until the next write_request is added to the pipeline
        ss::future<std::expected<event, errc>>
        wait_next(ss::abort_source* as = nullptr) noexcept;

        /// Apply lambda function to every write request at certain stage.
        /// The lambda should return 'write_request_processing_result'.
        /// The 'drop_error_code' is used to ack write requests which has to be
        /// dropped. The write request is not removed but instead the error
        /// is acknowledged which makes the write request go out of scope.
        /// When this happens the write request is unlinked from the list.
        template<class Fn>
        requires std::is_nothrow_invocable_r_v<
          std::expected<request_processing_result, errc>,
          Fn,
          write_request<Clock>&>
        void process(Fn&& fn) {
            auto next_stage = _parent->next_stage(_ps);
            uint32_t count = 0;
            for (auto& req : _parent->get_pending()) {
                if (req.stage == _ps) {
                    std::expected<request_processing_result, errc> r = fn(req);
                    if (!r.has_value()) {
                        // Drop write request using the error code from the
                        // result
                        req.set_value(r.error());
                        continue;
                    }
                    switch (r.value()) {
                    case request_processing_result::advance_and_continue:
                        _parent->advance_request_stage(req);
                        count++;
                        continue;
                    case request_processing_result::advance_and_stop:
                        _parent->advance_request_stage(req);
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

        /// Write pipeline aggregates resources so it's easier to
        /// track resources here than per request.
        void register_micro_probe(const micro_probe& p) {
            _parent->_probe.register_micro_probe(p);
        }

        /// Pipeline components can invoke this method to acquire units
        /// before allocating memory.
        auto acquire_mem_units(uint64_t units) {
            return ss::get_units(_parent->_mem_budget, units);
        }

        /// Get pointer to the stage's pending bytes counter
        auto stage_bytes_ref() { return _parent->stage_bytes_ref(_ps); }

        /// Get pointer to the next stage's pending bytes counter.
        /// Unlike next_stage(), this method returns a valid pointer even if
        /// the next stage hasn't been registered yet. The counters are
        /// pre-allocated, so they can be accessed before registration.
        auto next_stage_bytes_ref() {
            return _parent->stage_bytes_ref_by_index(
              _parent->next_stage_index(_ps));
        }

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

    /// Advance a request to the next stage, updating per-stage byte accounting.
    /// This is the canonical way to change a request's stage.
    void advance_request_stage(write_request<Clock>& req);

    /// Get the number of bytes at a specific pipeline stage.
    size_t stage_bytes(pipeline_stage s) const;
    const std::atomic<size_t>* stage_bytes_ref(pipeline_stage s) const;

    /// Get pointer to stage bytes counter by index.
    /// Unlike stage_bytes_ref(pipeline_stage), this method does not require
    /// the stage to be registered. It returns the pointer to the pre-allocated
    /// counter at the given index.
    /// \param index The stage index (0-based)
    /// \return Pointer to the atomic counter, or nullptr if index is invalid
    const std::atomic<size_t>* stage_bytes_ref_by_index(int index) const;

private:
    /// Transfer bytes from one stage to another.
    void
    transfer_stage_bytes(pipeline_stage from, pipeline_stage to, size_t bytes);

    /// Get write requests atomically.
    /// The total size of returned write requests and the stage to which they
    /// belong to should be specified.
    /// \param max_bytes Maximum number of bytes to extract
    /// \param stage Pipeline stage to get write requests from
    /// \param max_requests Maximum number of requests to extract
    /// \return List of write requests that were extracted
    write_requests_list get_write_requests(
      size_t max_bytes,
      pipeline_stage stage,
      size_t max_requests = std::numeric_limits<size_t>::max());

    /// Return write request which was already been in the pipeline
    /// before back into the pipeline.
    /// The method allows to reenqueue requests returned by get_write_requests
    /// method.
    /// \param req Write request to reenqueue
    /// \param signal If true signal the next stage that new write request is
    /// available
    void reenqueue(write_request<Clock>& req, bool signal = true);

    // Bytes per pipeline stage.
    struct alignas(std::hardware_destructive_interference_size)
      padded_atomic_counter {
        // Always updated on the same shard.
        // Can be read from any shard.
        std::atomic<uint64_t> count{0};

        void operator+=(size_t c) { count += c; }

        void operator-=(size_t c) { count -= c; }
    };
    std::array<padded_atomic_counter, max_pipeline_stages> _stage_bytes{};

    /// Sum of bytes across all pipeline stages.
    size_t current_size() const;

    // Total bytes went through the pipeline
    size_t _bytes_total{0};
    // Semaphore that represents memory budget that we have
    ssx::named_semaphore<Clock> _mem_budget;
    ssx::named_semaphore<Clock> _req_budget;

    pipeline_probe _probe;
};
} // namespace cloud_topics::l0
