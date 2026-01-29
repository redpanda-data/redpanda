// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction/utils.h"

#include "compaction/key_offset_map.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"

#include <seastar/core/coroutine.hh>

namespace compaction {

bool is_tx_batch_compaction_enabled(
  ss::sharded<features::feature_table>& feature_table) {
    if (!config::shard_local_cfg().log_compaction_tx_batch_removal_enabled()) {
        // Safety hatch for disabling control batch removal
        return false;
    }
    return feature_table.local().is_active(
      features::feature::coordinated_compaction);
}

bool is_removable_control_batch(
  const model::ntp& ntp,
  const model::record_batch_type batch_type,
  bool remove_user_tx_fence_enabled) {
    // Control batches in consumer offsets are special compared to
    // the ones in data partitions can be safely compacted away.
    // Fence batches can also be immediately removed when seen in the
    // `__consumer_offsets` topic or safely removed from a user topic. However,
    // removal in a user topic is gated by
    // `log_compaction_tx_batch_removal_enabled()`.
    auto is_co_topic = model::is_consumer_offsets_topic(ntp);
    auto tx_fence_removable = batch_type == model::record_batch_type::tx_fence
                              && (is_co_topic || remove_user_tx_fence_enabled);
    return tx_fence_removable
           || batch_type == model::record_batch_type::group_fence_tx
           || batch_type == model::record_batch_type::group_prepare_tx
           || batch_type == model::record_batch_type::group_abort_tx
           || batch_type == model::record_batch_type::group_commit_tx;
}

bool is_filterable(model::record_batch_type t) {
    if (t == model::record_batch_type::compaction_placeholder) {
        return false;
    }
    static const auto filtered_types = model::offset_translator_batch_types();
    auto n = std::count(filtered_types.begin(), filtered_types.end(), t);
    return n == 0;
}

bool is_compactible(const model::record_batch_header& h) {
    // Control batches are filterable but not compactible.
    if (h.attrs.is_control()) {
        return false;
    }
    return is_filterable(h.type);
}

model::record_batch make_placeholder_batch(model::record_batch_header& hdr) {
    model::record_batch_header new_hdr;
    new_hdr.type = model::record_batch_type::compaction_placeholder;
    new_hdr.base_offset = hdr.base_offset;
    new_hdr.last_offset_delta = hdr.last_offset_delta;
    new_hdr.first_timestamp = hdr.first_timestamp;
    new_hdr.max_timestamp = hdr.max_timestamp;
    auto no_records = iobuf{};
    new_hdr.reset_size_checksum_metadata(no_records);
    return model::record_batch(
      new_hdr, std::move(no_records), model::record_batch::tag_ctor_ng{});
}

ss::future<bool> is_latest_record_for_key(
  const key_offset_map& map,
  const model::record_batch& b,
  const model::record& r) {
    const auto o = b.base_offset() + model::offset_delta(r.offset_delta());
    auto key = compaction_key{iobuf_to_bytes(r.key())};

    auto latest_offset_indexed = co_await map.get(key);
    // If the map hasn't indexed the given key, we should keep the
    // key.
    if (!latest_offset_indexed.has_value()) {
        co_return true;
    }
    // We should only keep the record if its offset is equal or higher than
    // that indexed.
    co_return o >= latest_offset_indexed.value();
}

bool log_needs_compaction(
  double dirty_ratio,
  double min_cleanable_dirty_ratio,
  std::optional<model::timestamp> earliest_dirty_ts,
  std::chrono::milliseconds max_lag) {
    if (dirty_ratio >= min_cleanable_dirty_ratio) {
        return true;
    }

    const auto exceed_compact_lag
      = earliest_dirty_ts.has_value()
        && (to_time_point(model::timestamp::now()) - to_time_point(earliest_dirty_ts.value()) > max_lag);

    return exceed_compact_lag;
}

} // namespace compaction
