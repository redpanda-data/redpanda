/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "compaction/reducer.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "utils/prefix_logger.h"

namespace cloud_topics::l1 {

/// Base class for L1 object sinks (compaction, leveling) that share the same
/// object-building and multipart-upload logic. Derived classes provide their
/// own `initialize()`, `operator()`, and `finalize()` overrides.
class l1_object_sink : public compaction::sliding_window_reducer::sink {
public:
    l1_object_sink(
      model::topic_id_partition,
      l1::io*,
      l1::metastore*,
      ss::abort_source&,
      config::binding<size_t> max_object_size,
      ss::sstring job_type_name,
      object_builder::options = {});

    /// Called by the source before batches in a new extent range are provided.
    /// Rolls the inflight object if extents are non-contiguous.
    ss::future<> prepare_iteration(kafka::offset) final;

    /// Called by the source after batches in an extent range are provided.
    ss::future<> finish_iteration(kafka::offset, kafka::offset) final;

protected:
    /// Creates the `object_metadata_builder` from the metastore.
    /// Returns false if the builder could not be created.
    ss::future<bool> init_metadata_builder();

    /// Initializes the `_inflight_object` with a multipart upload.
    ss::future<> initialize_builder(kafka::offset);

    /// Finalizes the `_inflight_object`, completes the multipart upload,
    /// and registers the result with the metadata builder.
    ss::future<> flush(kafka::offset);

    /// Aborts the multipart upload, closes the builder, and removes the
    /// pending object from the metadata builder.
    ss::future<> discard_object(
      cloud_storage_clients::multipart_upload_ref,
      std::unique_ptr<object_builder>,
      object_id);

    /// Handles the common finalize preamble: flushes or discards inflight
    /// objects, checks if the builder is empty. Returns true if derived class
    /// should proceed with commit logic, false if there is nothing to commit.
    ss::future<bool> finalize_inflight(bool success);

    model::topic_id_partition _tp;
    io* _io;
    metastore* _metastore;
    ss::abort_source& _as;
    config::binding<size_t> _max_object_size;
    prefix_logger _ctxlog;
    object_builder::options _opts;

    std::unique_ptr<metastore::object_metadata_builder> _metadata_builder;
    bool _any_object_failed{false};

    struct inflight_object {
        cloud_storage_clients::multipart_upload_ref upload;
        std::unique_ptr<object_builder> builder{nullptr};
        object_id oid;
        kafka::offset object_base_offset{};
    };

    std::unique_ptr<inflight_object> _inflight_object{nullptr};
    offset_interval_set _processed_extents;
};

} // namespace cloud_topics::l1
