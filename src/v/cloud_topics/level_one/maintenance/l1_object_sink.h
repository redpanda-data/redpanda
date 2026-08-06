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
#include "model/timestamp.h"
#include "utils/prefix_logger.h"

#include <stdexcept>

namespace cloud_topics::l1 {

class l1_object_sink : public compaction::sliding_window_reducer::sink {
public:
    l1_object_sink(
      model::topic_id_partition,
      l1::io*,
      l1::metastore*,
      ss::abort_source&,
      config::binding<size_t>,
      config::binding<size_t>,
      size_t,
      prefix_logger&,
      object_builder::options = {});

    // Writes a batch into the inflight L1 object, rolling it first if it
    // has reached max_object_size and lazily starting a new object at the
    // batch's offset when there is none (right after a mid-extent roll).
    ss::future<ss::stop_iteration> operator()(model::record_batch) final;

    // Called by the `source` before batches in a new extent range are provided
    // to the `sink`. This is an asynchronous function because the active L1
    // object may need to be rolled, in case that the next extent range provided
    // is non-contiguous. Anchors the new object at the extent base: the
    // extent's first batches may have been deduplicated away, but the
    // object must still claim the extent's full offset span for its commit
    // to replace extents exactly.
    ss::future<> prepare_iteration(kafka::offset) final;

    // Called by the `source` after batches in an extent range are provided
    // to the `sink`. Commits the job's finished output objects incrementally
    // as it goes: commits may only happen at source extent boundaries — the
    // replaced intervals must align exactly with the extents being replaced —
    // so once at least a full object's worth of output has accumulated, the
    // inflight object is cut at the extent boundary this iteration finished
    // and everything finished so far is committed via `commit_objects()`.
    ss::future<> finish_iteration(kafka::offset, kafka::offset) final;

protected:
    /// Creates the `object_metadata_builder` from the metastore.
    ss::future<> init_metadata_builder();

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

    // Commits the passed metadata builder's finished-but-uncommitted
    // objects (counted by _pending_objects/_pending_bytes) to the
    // metastore. Called at source extent boundaries once at least one
    // commit interval's worth of output has accumulated, and again for the
    // tail during finalize_inflight(). Takes ownership of the builder:
    // committed objects must not be resent, so the next
    // initialize_builder() starts a fresh one.
    virtual ss::future<>
      commit_objects(std::unique_ptr<metastore::object_metadata_builder>) = 0;

    // Commits finished output that has not been acknowledged yet via
    // `commit_objects()`, handing it ownership of the consumed metadata
    // builder. No-op if nothing new has finished.
    ss::future<> commit_finished_objects();

    // Handles the common finalize preamble: flushes the inflight object (cut
    // at the last processed extent boundary) on the success path or discards
    // it on the exceptional path, then commits whatever finished output has
    // not been committed yet.
    ss::future<> finalize_inflight(bool success);

    // Returns the commit interval, never below `max_object_size`.
    size_t effective_commit_interval_bytes() const;

protected:
    model::topic_id_partition _tp;
    io* _io;
    metastore* _metastore;
    ss::abort_source& _as;
    // The target maximum L1 object size that will be built.
    config::binding<size_t> _max_object_size;
    // Bytes of finished output to accumulate before committing: at each
    // source extent boundary, once at least this much output is pending,
    // the inflight object is cut and everything finished so far is
    // committed.
    config::binding<size_t> _commit_interval_bytes;
    prefix_logger& _ctxlog;
    // The part size used for multipart uploads.
    size_t _upload_part_size;
    const object_builder::options _opts;

    // Finished output objects registered with the metadata builder but not
    // yet committed, and their total size in bytes. Reset by
    // commit_finished_objects(); the metadata builder holds the objects
    // themselves.
    uint64_t _pending_objects{0};
    uint64_t _pending_bytes{0};

    // The L1 object currently being built via multipart upload.
    struct inflight_object_t {
        cloud_storage_clients::multipart_upload_ref upload;
        std::unique_ptr<object_builder> builder{nullptr};
        object_id oid;
        kafka::offset object_base_offset{};
    };

    std::unique_ptr<inflight_object_t> _inflight_object{nullptr};

    // The interval set that is populated by extents which have been read by the
    // `source` and written by the `sink`.
    offset_interval_set _processed_extents;

    // Number of finish_iteration() calls: source extents for compaction,
    // leveling ranges for leveling (which tracks its own extent count from
    // range metadata instead).
    uint64_t _processed_extent_count{0};

private:
    // The metadata builder for the current compaction job, created during
    // `initialize()` from the metastore and used to track new objects.
    std::unique_ptr<metastore::object_metadata_builder> _metadata_builder;
};

} // namespace cloud_topics::l1
