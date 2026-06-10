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
#include "cloud_topics/level_one/maintenance/l1_object_sink.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_zero/notifier/level_zero_notifier.h"
#include "config/property.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "utils/prefix_logger.h"

namespace cloud_topics::l1 {

class compaction_sink : public l1_object_sink {
public:
    compaction_sink(
      model::topic_id_partition,
      model::ntp,
      const chunked_vector<offset_interval_set::interval>&,
      const offset_interval_set&,
      metastore::compaction_epoch,
      kafka::offset,
      l1::io*,
      l1::metastore*,
      ss::abort_source&,
      config::binding<size_t>,
      size_t,
      prefix_logger&,
      object_builder::options = {},
      cloud_topics::level_zero_notifier* = nullptr);

    ss::future<bool>
    initialize(compaction::sliding_window_reducer::source&) final;

    ss::future<ss::stop_iteration> operator()(model::record_batch) final;

    ss::future<> finalize(bool success) final;

private:
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
    // Offset ranges for the contained `topic_id_partition` obtained from the
    // metastore.
    using interval_vec = chunked_vector<offset_interval_set::interval>;
    const interval_vec& _dirty_range_intervals;
    const offset_interval_set& _removable_tombstone_ranges;

    // The expected compaction epoch for the log.
    const metastore::compaction_epoch _expected_compaction_epoch;

    // The start offset of the log.
    kafka::offset _start_offset;

    // Dirty ranges returned by the `metastore` that were indexed during
    // `map_deduplication_iteration`.
    chunked_vector<metastore::compaction_update::cleaned_range>
      _new_cleaned_ranges;

    // Mirror of _tp as a model::ntp, used to notify the level_zero_notifier
    // (and for anything else that needs cross-shard partition lookups by ntp).
    model::ntp _ntp;

    // Receives the new min_allowed_local_threshold floor after a successful
    // finalize(). May be null (no-op).
    cloud_topics::level_zero_notifier* _notifier;

private:
    friend class throwing_compaction_sink;
};

} // namespace cloud_topics::l1
