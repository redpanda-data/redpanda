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
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"

#include <seastar/core/coroutine.hh>

namespace compaction {

bool is_compactible_control_batch(
  const model::ntp& ntp, const model::record_batch_type batch_type) {
    // Control batches in consumer offsets are special compared to
    // the ones in data partitions can be safely compacted away.
    //
    // tx_fence batches  in consumer offsets are special and should be
    // compacted. They were used historically to mark the begin of a transaction
    // but later switched to group_fence_tx.
    return unlikely(
             batch_type == model::record_batch_type::tx_fence
             && model::is_consumer_offsets_topic(ntp))
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

bool is_compactible(
  const model::ntp& ntp, const model::record_batch_header& h) {
    if (h.attrs.is_control() && !is_compactible_control_batch(ntp, h.type)) {
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
    auto key_view = compaction_key{iobuf_to_bytes(r.key())};
    auto key = enhance_key(
      b.header().type, b.header().attrs.is_control(), key_view);
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

} // namespace compaction
