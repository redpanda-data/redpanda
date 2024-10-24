// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/batch_collector.h"

#include "storage/mvlog/errc.h"
#include "storage/mvlog/reader_outcome.h"

namespace storage::experimental::mvlog {

bool batch_collector::should_skip(
  const model::record_batch_header& batch_hdr) const noexcept {
    if (batch_hdr.last_offset() < cfg_.start_offset) {
        return true;
    }
    if (batch_hdr.max_timestamp < cfg_.first_timestamp) {
        return true;
    }
    if (
      cfg_.type_filter.has_value()
      && cfg_.type_filter.value() != batch_hdr.type) {
        return true;
    }
    return false;
}

result<reader_outcome, errc> batch_collector::add_batch(
  model::record_batch_header batch_hdr, iobuf records) noexcept {
    if (unlikely(batch_hdr.base_offset <= last_offset_)) {
        // We're expecting to read through monotonically increasing batches!
        return errc::broken_data_invariant;
    }
    // TODO: add considerations around `strict_max_bytes`, or handle bytes
    // limiting in a lower layer.

    last_offset_ = batch_hdr.last_offset();
    if (batch_hdr.base_offset() > cfg_.max_offset) {
        return reader_outcome::stop;
    }

    if (should_skip(batch_hdr)) {
        return reader_outcome::skip;
    }
    // TODO: add ghost batch building here.

    cur_buffer_size_ += batch_hdr.size_bytes;
    batch_hdr.ctx.term = cur_term_;
    batches_.emplace_back(model::record_batch(
      batch_hdr, std::move(records), model::record_batch::tag_ctor_ng{}));

    // Signal to the caller that the buffer is full, but still accept the batch
    // since we already have the header and records available here.
    return cur_buffer_size_ >= target_max_buffer_size_
             ? reader_outcome::buffer_full
             : reader_outcome::success;
}

result<reader_outcome, errc>
batch_collector::set_term(model::term_id new_term) noexcept {
    if (cur_term_ > new_term) {
        // We're expecting to read through monotonically increasing terms!
        return errc::broken_data_invariant;
    }
    cur_term_ = new_term;
    return reader_outcome::success;
}

} // namespace storage::experimental::mvlog
