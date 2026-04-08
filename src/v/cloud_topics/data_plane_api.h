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
#include "cloud_topics/types.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <expected>
#include <system_error>

namespace cloud_topics {

// staged_write is a write operation that has been reserved in the pipeline.
// it is decoupled from uploading so that we can provide backpressure before
// accepting more batches into the pipeline
struct staged_write {
    struct batch_data {
        batch_data() = default;
        batch_data(const batch_data&) = default;
        batch_data(batch_data&&) = delete;
        batch_data& operator=(const batch_data&) = default;
        batch_data& operator=(batch_data&&) = delete;
        virtual ~batch_data() = default;
    };

    std::unique_ptr<batch_data> staged;
};

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

    // Reserve the space needed for this write.
    virtual ss::future<std::expected<staged_write, std::error_code>>
    stage_write(chunked_vector<model::record_batch> batches) = 0;

    // Execute this write using the reservation.
    virtual ss::future<std::expected<upload_meta, std::error_code>>
    execute_write(
      model::ntp ntp,
      cluster_epoch min_epoch,
      staged_write reservation,
      model::timeout_clock::time_point deadline) = 0;

    // Materialize extents from the L0 read pipeline.
    // `output_size_estimate` must not exceed `materialize_max_bytes()`.
    // When `allow_mat_failure` is yes, download_not_found (404)
    // errors for individual extents are tolerated: the missing extents are
    // skipped and the result contains fewer batches than requested.
    virtual ss::future<result<chunked_vector<model::record_batch>>> materialize(
      model::ntp ntp,
      size_t output_size_estimate,
      chunked_vector<extent_meta> metadata,
      model::timeout_clock::time_point timeout,
      model::opt_abort_source_t,
      allow_materialization_failure allow_mat_failure) = 0;

    /// Return the maximum bytes that may be requested in a single
    /// materialize() call. This reflects the read pipeline's memory quota.
    /// Callers of `materialize` must limit their requests to stay within this
    /// bound.
    virtual size_t materialize_max_bytes() const = 0;

    /// Cache materialized record batch
    virtual void cache_put(
      const model::topic_id_partition&, const model::record_batch& b) = 0;

    /// Retrieve materialized record batch from cache
    virtual std::optional<model::record_batch>
    cache_get(const model::topic_id_partition&, model::offset o) = 0;

    /// Put batches into the cache and notify the offset monitor when the
    /// inserted batches extend the contiguous range tracked by the monitor.
    virtual void cache_put_ordered(
      const model::topic_id_partition&,
      chunked_vector<model::record_batch> batches) = 0;

    /// Retrieve current cluster epoch
    virtual ss::future<std::optional<cloud_topics::cluster_epoch>>
    get_current_epoch(ss::abort_source* as) = 0;

    /// Invalid the current epoch window if it's below this value.
    virtual ss::future<>
      invalidate_epoch_below(cloud_topics::cluster_epoch) = 0;

    /// Wait until a batch at or beyond \p offset has been added to the cache
    /// for \p tidp, or until timeout/abort. \p last_known seeds a newly
    /// created monitor so that already-committed offsets resolve immediately.
    virtual ss::future<> cache_wait(
      const model::topic_id_partition&,
      model::offset offset,
      model::offset last_known,
      model::timeout_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> as) = 0;
};

} // namespace cloud_topics
