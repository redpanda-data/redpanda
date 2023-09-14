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
#include <seastar/util/noncopyable_function.hh>

namespace transform {

/**
 * A processor is the driver of a transform for a single partition.
 *
 * At it's heart it's a fiber that reads->transforms->writes batches
 * from an input ntp to an output ntp.
 */
class processor {
public:
    using error_callback = ss::noncopyable_function<void(
      model::transform_id, model::ntp, model::transform_metadata)>;

    processor(
      model::transform_id,
      model::ntp,
      model::transform_metadata,
      std::unique_ptr<wasm::engine>,
      error_callback,
      std::unique_ptr<source>,
      std::vector<std::unique_ptr<sink>>,
      probe*);

    virtual ~processor() = default;
    virtual ss::future<> start();
    virtual ss::future<> stop();

    bool is_running() const;
    model::transform_id id() const;
    const model::ntp& ntp() const;
    const model::transform_metadata& meta() const;

private:
    ss::future<> run_transform_loop();
    ss::future<> do_run_transform_loop();
    ss::future<> transform_batches(model::record_batch_reader::data_t);

    model::transform_id _id;
    model::ntp _ntp;
    model::transform_metadata _meta;
    std::unique_ptr<wasm::engine> _engine;
    std::unique_ptr<source> _source;
    std::vector<std::unique_ptr<sink>> _sinks;
    error_callback _error_callback;
    probe* _probe;

    ss::abort_source _as;
    ss::future<> _task;
    prefix_logger _logger;
};
} // namespace transform
