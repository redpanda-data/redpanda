/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "model/record.h"

namespace model {
/**
 * Creates a single batch of the specified type that fills the gap between
 * start_offset and end_offset. BatchType must be either ghost_batch or
 * compaction_placeholder.
 */
template<record_batch_type BatchType>
record_batch
make_placeholder_batch(offset start_offset, offset end_offset, term_id term) {
    static_assert(
      BatchType == record_batch_type::ghost_batch
        || BatchType == record_batch_type::compaction_placeholder,
      "BatchType must be ghost_batch or compaction_placeholder");

    auto delta = end_offset - start_offset;
    auto now = model::timestamp::now();
    model::record_batch_header header = {
      .size_bytes = model::packed_record_batch_header_size,
      .base_offset = start_offset,
      .type = BatchType,
      .crc = 0, // crc computed later
      .attrs = model::record_batch_attributes{} |= model::compression::none,
      .last_offset_delta = static_cast<int32_t>(delta),
      .first_timestamp = now,
      .max_timestamp = now,
      .producer_id = -1,
      .producer_epoch = -1,
      .base_sequence = -1,
      .record_count = static_cast<int32_t>(delta() + 1),
      .ctx = model::record_batch_header::context(term, ss::this_shard_id())};

    model::record_batch batch(
      header, model::record_batch::compressed_records{});

    batch.header().crc = model::crc_record_batch(batch);
    batch.header().header_crc = model::internal_header_only_crc(batch.header());
    return batch;
}

/**
 * Makes multiple batches of the specified type required to fill the gap in a
 * way that max batch size (max of int32_t) is not exceeded. BatchType must be
 * either ghost_batch or compaction_placeholder.
 */
template<record_batch_type BatchType>
std::vector<record_batch>
make_placeholder_batches(offset start_offset, offset end_offset, term_id term) {
    std::vector<model::record_batch> batches;
    while (start_offset <= end_offset) {
        static constexpr model::offset max_batch_size{
          std::numeric_limits<int32_t>::max()};
        // limit max batch size
        const model::offset delta = std::min<model::offset>(
          max_batch_size, end_offset - start_offset);

        batches.push_back(
          make_placeholder_batch<BatchType>(
            start_offset, delta + start_offset, term));
        start_offset = next_offset(batches.back().last_offset());
    }

    return batches;
}

inline record_batch
make_ghost_batch(offset start_offset, offset end_offset, term_id term) {
    return make_placeholder_batch<record_batch_type::ghost_batch>(
      start_offset, end_offset, term);
}

inline std::vector<record_batch>
make_ghost_batches(offset start_offset, offset end_offset, term_id term) {
    return make_placeholder_batches<record_batch_type::ghost_batch>(
      start_offset, end_offset, term);
}

inline record_batch make_compaction_placeholder_batch(
  offset start_offset, offset end_offset, term_id term) {
    return make_placeholder_batch<record_batch_type::compaction_placeholder>(
      start_offset, end_offset, term);
}

inline std::vector<record_batch> make_compaction_placeholder_batches(
  offset start_offset, offset end_offset, term_id term) {
    return make_placeholder_batches<record_batch_type::compaction_placeholder>(
      start_offset, end_offset, term);
}
} // namespace model
