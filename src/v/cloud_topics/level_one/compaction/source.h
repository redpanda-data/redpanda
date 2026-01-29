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
#include "cloud_topics/level_one/compaction/filter.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/worker_probe.h"
#include "cloud_topics/level_one/metastore/extent_metadata_reader.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"

namespace cloud_topics::l1 {

class compaction_sink;

class compaction_source : public compaction::sliding_window_reducer::source {
public:
    compaction_source(
      model::ntp,
      model::topic_id_partition,
      const chunked_vector<offset_interval_set::interval>&,
      const offset_interval_set&,
      kafka::offset,
      kafka::offset,
      compaction::key_offset_map*,
      std::chrono::milliseconds,
      metastore*,
      io*,
      ss::abort_source&,
      compaction_job_state&,
      compaction_worker_probe&);
    ss::future<> initialize() final;
    ss::future<ss::stop_iteration> map_building_iteration() final;
    ss::future<ss::stop_iteration>
    deduplication_iteration(compaction::sliding_window_reducer::sink&) final;

private:
    // Returns true if the compaction process has been pre-empted to stop.
    bool preempted() const;

private:
    friend compaction_sink;

    const model::ntp _ntp;
    const model::topic_id_partition _tp;

    // Offset ranges for the contained `topic_id_partition` obtained from the
    // metastore.
    using interval_vec = chunked_vector<offset_interval_set::interval>;
    const interval_vec& _dirty_range_intervals;
    const offset_interval_set& _removable_tombstone_ranges;

    // The start offset of the CTP.
    kafka::offset _start_offset;

    // The maximum compactible offset, as pinned by e.g. iceberg translation.
    kafka::offset _max_compactible_offset;

    // Iterator used during `map_building_iteration()` which points into the
    // above vector `_dirty_range_intervals`. Iterating backwards over extents
    // to fill the key-offset map used for de-duplication can allow for more
    // efficient compaction runs when compacting the log.
    interval_vec::const_reverse_iterator _dirty_range_it;

    // The extent metadata reader, which is constructed and immediately used to
    // produce the `_extent_iterator` generator. This object must be kept alive
    // while `_extent_iterator` is in use, but it should not be used directly.
    extent_metadata_reader _extent_reader;

    // The generated constructed from the forward reader for extents of
    // the CTP during `deduplication_iteration()`. The de-duplication pass
    // _must_ be extent aligned as part of a requirement of the `metastore`'s
    // internal state and rules around replacing or compacting existing extents.
    extent_metadata_reader::extent_metadata_generator _extent_iterator;

    // The key-offset map for this run of compaction. Built up from existing
    // data during `map_building_iteration()` by iterating over `_dirty_ranges`
    // and used for removal of old keys in `deduplication_iteration`.
    compaction::key_offset_map* _map;

    // `min.compaction.lag.ms` or the cluster default `min_compaction_lag_ms`
    // for this CTP.
    std::chrono::milliseconds _min_compaction_lag_ms;

    metastore* _metastore;
    io* _io;

    ss::abort_source& _as;
    compaction_job_state& _state;
    compaction_worker_probe& _probe;

    // Dirty ranges returned by the `metastore` that were indexed during
    // `map_deduplication_iteration`.
    chunked_vector<metastore::compaction_update::cleaned_range>
      _new_cleaned_ranges;
};

} // namespace cloud_topics::l1
