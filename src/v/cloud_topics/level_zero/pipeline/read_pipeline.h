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
#include "cloud_topics/level_zero/pipeline/circuit_breaker.h"
#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/pipeline/read_request.h"
#include "model/record_batch_reader.h"
#include "ssx/semaphore.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>

namespace experimental::cloud_topics::l0 {

struct read_pipeline_accessor;

template<class Clock = ss::lowres_clock>
class read_pipeline
  : public base_pipeline<read_request<Clock>, read_pipeline<Clock>, Clock> {
    friend struct read_pipeline_accessor;

public:
    read_pipeline();
    using timestamp_t = Clock::time_point;

    /// Query the data plane for the given ntp.
    /// The query is a list of placeholder or overlay batches
    /// that should be materialized.
    /// The result of the query is a reader that contains the
    /// actual raft_data batches.
    ss::future<result<dataplane_query_result>>
    make_reader(model::ntp ntp, dataplane_query query, timestamp_t timeout);

    using read_requests_list
      = requests_list<read_pipeline<Clock>, read_request<Clock>>;

    ss::sstring pipeline_name() const noexcept { return "read_pipeline"; }

    /// The stage of the pipeline that should be used by a single
    class stage {
    public:
        explicit stage(pipeline_stage ps, read_pipeline<Clock>* parent)
          : _ps(ps)
          , _parent(parent) {}

        explicit operator pipeline_stage() const { return _ps; }

        /// Wait until fetch requests are available in the pipeline
        /// stage and return them (the requests are pulled out of
        /// the pipeline).
        ss::future<checked<read_requests_list, errc>>
        pull_fetch_requests(size_t max_bytes) {
            l0::event_filter<Clock> filter(
              l0::event_type::new_read_request, _ps);
            auto event = co_await _parent->subscribe(
              filter, _parent->get_abort_source());
            switch (event.type) {
            case l0::event_type::shutting_down:
                co_return errc::shutting_down;
            case l0::event_type::err_timedout:
                co_return errc::timeout;
            case l0::event_type::new_write_request:
            case l0::event_type::none:
                vassert(false, "Unexpected event type in the read_pipeline");
            case l0::event_type::new_read_request:
                break;
            }
            auto list = _parent->get_fetch_requests(max_bytes, _ps);
            co_return list;
        }

        bool stopped() const noexcept { return _parent->stopped(); }

        basic_retry_chain_node<Clock>& get_root_rtc() noexcept {
            return _parent->get_root_rtc();
        }

        void register_pipeline_error(errc e) {
            _parent->register_pipeline_error(e);
        }

    private:
        pipeline_stage _ps;
        read_pipeline<Clock>* _parent;
    };

    /// Register new pipeline stage
    stage register_read_pipeline_stage() noexcept {
        return stage(this->register_pipeline_stage(), this);
    }

    void signal(pipeline_stage stage);

    event trigger_event(pipeline_stage stage);

private:
    ss::abort_source& get_abort_source() {
        return this->get_root_rtc().root_abort_source();
    }

    /// Return list of fetch requests that can be processed immediately
    read_requests_list
    get_fetch_requests(size_t max_bytes, pipeline_stage stage);

    /// Register read-path errors
    void register_pipeline_error(errc);

    // Total size of all fetch requests (estimated using max_bytes)
    size_t _current_size{0};

    // Total bytes went through the pipeline
    size_t _bytes_total{0};

    ssx::named_semaphore<Clock> _mem_quota;

    circuit_breaker<Clock> _breaker;
};
} // namespace experimental::cloud_topics::l0
