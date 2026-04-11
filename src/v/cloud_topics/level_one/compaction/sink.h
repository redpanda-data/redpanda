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

#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "compaction/reducer.h"
#include "config/property.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

namespace cloud_topics::l1 {

class compaction_sink : public compaction::sliding_window_reducer::sink {
public:
    compaction_sink(
      model::topic_id_partition,
      const chunked_vector<offset_interval_set::interval>&,
      const offset_interval_set&,
      metastore::compaction_epoch,
      kafka::offset,
      l1::io*,
      l1::metastore*,
      ss::abort_source&,
      config::binding<size_t> max_object_size,
      size_t upload_part_size,
      object_builder::options = {});

    ss::future<bool>
    initialize(compaction::sliding_window_reducer::source&) final;

    // Called by the `source` before batches in a new extent range are provided
    // to the `sink`. This is an asynchronous function because the active L1
    // object may need to be rolled, in case that the next extent range provided
    // is non-contiguous.
    ss::future<> prepare_iteration(kafka::offset) final;

    // Called by the `source` after batches in an extent range are provided
    // to the `sink`.
    ss::future<> finish_iteration(kafka::offset, kafka::offset) final;

    ss::future<ss::stop_iteration>
    operator()(model::record_batch, model::compression) final;

    ss::future<> finalize(bool success) final;

private:
    // The target maximum L1 object size that will be built.
    config::binding<size_t> _max_object_size;

    // The part size used for multipart uploads.
    size_t _upload_part_size;

    // Initializes the `_inflight_object` with a multipart upload.
    ss::future<> initialize_builder(kafka::offset);

    // Finalizes the `_inflight_object`, completes the multipart upload,
    // and registers the result with the metadata builder.
    ss::future<> flush(kafka::offset);

    // Aborts the multipart upload, closes the builder, and removes the
    // pending object from the metadata builder.
    ss::future<> discard_object(
      cloud_storage_clients::multipart_upload_ref,
      std::unique_ptr<object_builder>,
      object_id);

    // Makes a `compact_objects()` request to the `metastore`, using the
    // provided (potentially empty) `compaction_map_t` as the metastore
    // compaction update.
    ss::future<std::expected<void, metastore::errc>>
      do_compact_objects(metastore::compaction_map_t);

    // Finalizes the compaction via `metastore->compact_objects()` without a
    // compaction metadata update.
    ss::future<> compact_objects_without_update();

    // Finalizes the compaction via `metastore->compact_objects()` with a
    // compaction metadata update.
    ss::future<> compact_objects_with_update(
      chunked_vector<metastore::compaction_update::cleaned_range>,
      offset_interval_set);

private:
    model::topic_id_partition _tp;

    // Offset ranges for the contained `topic_id_partition` obtained from the
    // metastore.
    using interval_vec = chunked_vector<offset_interval_set::interval>;
    const interval_vec& _dirty_range_intervals;
    const offset_interval_set& _removable_tombstone_ranges;

    // The expected compaction epoch for the log.
    const metastore::compaction_epoch _expected_compaction_epoch;

    // The start offset of the log.
    kafka::offset _start_offset;

    io* _io;
    metastore* _metastore;
    ss::abort_source& _as;

    const object_builder::options _opts;

    // The metadata builder for the current compaction job, created during
    // `initialize()` from the metastore and used to track new objects.
    std::unique_ptr<metastore::object_metadata_builder> _metadata_builder;

    // Tracks whether any upload failed during this job.
    bool _any_object_failed{false};

    // The L1 object currently being built via multipart upload.
    struct compacted_object {
        cloud_storage_clients::multipart_upload_ref upload;
        std::unique_ptr<object_builder> builder{nullptr};
        object_id oid;
        kafka::offset object_base_offset{};
    };

    std::unique_ptr<compacted_object> _inflight_object{nullptr};

    // The interval set that is populated by extents which have been read by the
    // `source` and written by the `sink`.
    offset_interval_set _processed_extents;

    // Dirty ranges returned by the `metastore` that were indexed during
    // `map_deduplication_iteration`.
    chunked_vector<metastore::compaction_update::cleaned_range>
      _new_cleaned_ranges;

private:
    friend class throwing_compaction_sink;
};

} // namespace cloud_topics::l1
