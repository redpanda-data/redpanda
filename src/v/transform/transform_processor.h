/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/transform.h"
#include "seastarx.h"
#include "transform/io.h"
#include "transform/probe.h"
#include "utils/prefix_logger.h"
#include "wasm/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/queue.hh>
#include <seastar/util/noncopyable_function.hh>

namespace transform {

/**
 * A holder of the result of a transform, along with the input offset the batch
 * was read at.
 */
struct transformed_batch {
    model::record_batch batch;
    kafka::offset input_offset;
};

/**
 * A processor is the driver of a transform for a single partition.
 *
 * At it's heart it's a fiber that reads->transforms->writes batches
 * from an input ntp to an output ntp.
 */
class processor {
public:
    using state = model::transform_report::processor::state;
    using state_callback
      = ss::noncopyable_function<void(model::transform_id, model::ntp, state)>;

    processor(
      model::transform_id,
      model::ntp,
      model::transform_metadata,
      ss::shared_ptr<wasm::engine>,
      state_callback,
      std::unique_ptr<source>,
      std::vector<std::unique_ptr<sink>>,
      std::unique_ptr<offset_tracker>,
      probe*);
    processor(const processor&) = delete;
    processor(processor&&) = delete;
    processor& operator=(const processor&) = delete;
    processor& operator=(processor&&) = delete;
    virtual ~processor() = default;
    virtual ss::future<> start();
    virtual ss::future<> stop();

    bool is_running() const;
    model::transform_id id() const;
    const model::ntp& ntp() const;
    const model::transform_metadata& meta() const;
    int64_t current_lag() const;

private:
    ss::future<> run_consumer_loop(kafka::offset);
    ss::future<> run_transform_loop();
    ss::future<> run_producer_loop();
    ss::future<> poll_sleep();
    ss::future<kafka::offset> load_start_offset();
    void report_lag(int64_t);

    template<typename... Future>
    ss::future<> when_all_shutdown(Future&&...);
    ss::future<> handle_processor_task(ss::future<>);

    model::transform_id _id;
    model::ntp _ntp;
    model::transform_metadata _meta;
    ss::shared_ptr<wasm::engine> _engine;
    std::unique_ptr<source> _source;
    std::vector<std::unique_ptr<sink>> _sinks;
    std::unique_ptr<offset_tracker> _offset_tracker;
    state_callback _state_callback;
    probe* _probe;

    ss::queue<model::record_batch> _consumer_transform_pipe;
    ss::queue<transformed_batch> _transform_producer_pipe;

    ss::abort_source _as;
    ss::future<> _task;
    prefix_logger _logger;

    int64_t _last_reported_lag = 0;
};
} // namespace transform
