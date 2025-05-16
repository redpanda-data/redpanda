/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/outcome.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

namespace experimental::cloud_topics {

/// Dataplane API
class api {
public:
    api() = default;

    api(const api&) = delete;
    api& operator=(const api&) = delete;
    api(api&&) noexcept = delete;
    api& operator=(api&&) noexcept = delete;
    virtual ~api() = default;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    /// Write data batches and get back placeholder batches
    virtual ss::future<result<ss::circular_buffer<model::record_batch>>>
    write_and_debounce(
      model::ntp ntp,
      model::record_batch_reader r,
      std::chrono::milliseconds timeout)
      = 0;

    virtual ss::future<result<ss::circular_buffer<model::record_batch>>>
    materialize(
      model::ntp ntp,
      size_t output_size_estimate,
      ss::circular_buffer<model::record_batch> metadata,
      std::chrono::milliseconds timeout)
      = 0;
};

} // namespace experimental::cloud_topics
