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

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/compaction/committer.h"
#include "cloud_topics/level_one/compaction/meta.h"
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
      compaction_committer*,
      config::binding<size_t> max_object_size,
      object_builder::options = {});

    ss::future<bool>
    initialize(compaction::sliding_window_reducer::source&) final;

    // Called by the `source` before batches in a new extent range are provided
    // to the `sink`. This is an asynchronous function because the active L1
    // object may need to be rolled, in case that the next extent range provided
    // is non-contiguous. For example, if the extents were
    // `[[0,10],[11,20],[21,30]]`, and the extent [11,20] was deemed ineligible
    // for compaction (due to `min.compaction.lag.ms` or some other reason), the
    // current L1 object composing the range [0,10] would be rolled, and a new
    // L1 object would be started for the range [21,30].
    ss::future<> prepare_iteration(kafka::offset);

    // Called by the `source` after batches in an extent range are provided
    // to the `sink`.
    ss::future<> finish_iteration(kafka::offset, kafka::offset);

    ss::future<ss::stop_iteration>
    operator()(model::record_batch, model::compression) final;

    ss::future<> finalize() final;

private:
    // The target maximum L1 object size that will be built. After this
    // threshold is breached, `needs_roll()` should return `true` and a new L1
    // object will be started.
    config::binding<size_t> _max_object_size;

    // Initializes the `_inflight_object`. It is guaranteed to have a value (!=
    // nullptr) after this function is called, if no exception is thrown.
    ss::future<> initialize_builder(kafka::offset);

    // Finalizes the `_inflight_object` and pushes the built update to the
    // `_committer`.
    ss::future<> flush(kafka::offset);

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

    // The compaction job, if initialized, as returned by the `_committer`.
    compaction_committer::compaction_job* _job{nullptr};

    io* _io;
    compaction_committer* _committer;

    const object_builder::options _opts;

    // The L1 object currently being built.
    struct compacted_object {
        // Both `active_staging_file` and `builder` are guaranteed to have a
        // value for an active `compacted_object`.
        std::unique_ptr<staging_file> active_staging_file{nullptr};
        std::unique_ptr<object_builder> builder{nullptr};
        kafka::offset object_base_offset{};
    };

    std::unique_ptr<compacted_object> _inflight_object{nullptr};

    // The interval set that is populated by extents which have been read by the
    // `source` and written by the `sink`. This is important to know in order to
    // decide which dirty ranges and removable tombstone ranges have actually
    // been processed when finalizing the compaction job with the `_committer`.
    offset_interval_set _processed_extents;

    // Dirty ranges returned by the `metastore` that were indexed during
    // `map_deduplication_iteration`.
    chunked_vector<metastore::compaction_update::cleaned_range>
      _new_cleaned_ranges;
};

} // namespace cloud_topics::l1
