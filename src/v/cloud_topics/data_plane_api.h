/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

namespace experimental::cloud_topics {

/// Dataplane API
class data_plane_api {
public:
    data_plane_api() = default;

    data_plane_api(const data_plane_api&) = delete;
    data_plane_api& operator=(const data_plane_api&) = delete;
    data_plane_api(data_plane_api&&) noexcept = delete;
    data_plane_api& operator=(data_plane_api&&) noexcept = delete;
    virtual ~data_plane_api() = default;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    /// Write data batches and get back placeholder batches
    virtual ss::future<result<chunked_vector<extent_meta>>> write_and_debounce(
      model::ntp ntp,
      chunked_vector<model::record_batch> batches,
      model::timeout_clock::time_point deadline)
      = 0;

    virtual ss::future<result<chunked_vector<model::record_batch>>> materialize(
      model::ntp ntp,
      size_t output_size_estimate,
      chunked_vector<extent_meta> metadata,
      model::timeout_clock::time_point timeout)
      = 0;

    /// Cache materialized record batch
    virtual void cache_put(const model::ntp&, const model::record_batch& b) = 0;

    /// Retrieve materialized record batch from cache
    virtual std::optional<model::record_batch>
    cache_get(const model::ntp&, model::offset o) = 0;
};

} // namespace experimental::cloud_topics
