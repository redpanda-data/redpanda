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

#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "compaction/filter.h"
#include "compaction/key_offset_map.h"

namespace cloud_topics::l1 {

class compaction_filter : public compaction::filter {
public:
    compaction_filter(
      compaction::sliding_window_reducer::sink&,
      const compaction::key_offset_map&,
      model::ntp,
      const offset_interval_set&);

private:
    ss::future<bool>
    should_keep(const model::record_batch&, const model::record&) const;

    ss::future<> maybe_index_offset_delta(
      const model::record_batch&,
      const model::record&,
      std::vector<int32_t>&) const;

    ss::future<std::vector<int32_t>>
    compute_offset_deltas_to_keep(const model::record_batch&) const final;

    ss::future<std::optional<model::record_batch>>
      filter_batch_with_offset_deltas(
        model::record_batch, std::vector<int32_t>) const final;

private:
    const compaction::key_offset_map& _map;
    const offset_interval_set& _removable_tombstone_ranges;
};

} // namespace cloud_topics::l1
