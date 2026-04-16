// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "filter.h"

#include "compaction/utils.h"
#include "model/batch_compression.h"
#include "model/record.h"

#include <seastar/core/coroutine.hh>

#include <vector>

namespace compaction {

ss::future<ss::stop_iteration> filter::operator()(model::record_batch b) {
    const auto comp = b.header().attrs.compression();
    if (!b.compressed()) {
        co_return co_await filter_and_rewrite_with_sink(comp, std::move(b));
    }
    auto batch = co_await model::decompress_batch(b);

    co_return co_await filter_and_rewrite_with_sink(comp, std::move(batch));
}

ss::future<std::optional<model::record_batch>>
filter::filter_batch(model::record_batch b) const {
    // do not filter non-removable batch types under any circumstances
    if (!is_filterable(b.header().type)) {
        co_return std::move(b);
    }

    // compute which records to keep
    std::vector<int32_t> offset_deltas = co_await compute_offset_deltas_to_keep(
      b);

    auto ret = co_await filter_batch_with_offset_deltas(
      std::move(b), std::move(offset_deltas));
    co_return ret;
}

ss::future<std::optional<model::record_batch>> filter::do_filter_batch(
  model::record_batch b, std::vector<int32_t> offset_deltas) const {
    // no records to keep
    if (offset_deltas.empty()) {
        co_return std::nullopt;
    }

    // keep all records
    if (offset_deltas.size() == static_cast<size_t>(b.record_count())) {
        co_return std::move(b);
    }

    // filter
    iobuf ret;
    int32_t rec_count = 0;
    std::optional<int64_t> first_timestamp_delta;
    int64_t last_timestamp_delta;
    co_await b.for_each_record_async([&rec_count,
                                      &first_timestamp_delta,
                                      &last_timestamp_delta,
                                      &ret,
                                      &offset_deltas](model::record record) {
        // contains the key
        if (
          std::count(
            offset_deltas.begin(),
            offset_deltas.end(),
            record.offset_delta())) {
            /*
             * TODO when we further optimize lazy record materialization ot
             * make use of views we can avoid this re-encoding by copying or
             * sharing the view. either way, we were building
             * record batch with the uncompressed records so they were being
             * re-encoded.
             */
            if (!first_timestamp_delta) {
                first_timestamp_delta = record.timestamp_delta();
            }
            last_timestamp_delta = record.timestamp_delta();
            model::append_record_to_buffer(ret, record);
            ++rec_count;
        }
    });

    if (rec_count == 0) {
        co_return std::nullopt;
    }

    // There is no need to preserve the timestamp from the original
    // batch after compaction. The FirstTimestamp field therefore always
    // reflects the timestamp of the first record in the batch. If the batch
    // is empty, the FirstTimestamp will be set to -1 (NO_TIMESTAMP).
    //
    // Similarly, the MaxTimestamp field reflects the maximum timestamp of
    // the current records if the timestamp type is CREATE_TIME. For
    // LOG_APPEND_TIME, on the other hand, the MaxTimestamp field reflects
    // the timestamp set by the broker and is preserved after compaction.
    // Additionally, the MaxTimestamp of an empty batch always retains the
    // previous value prior to becoming empty.
    auto& hdr = b.header();
    const auto first_time = model::timestamp(
      hdr.first_timestamp() + first_timestamp_delta.value());
    auto last_time = hdr.max_timestamp;
    if (hdr.attrs.timestamp_type() == model::timestamp_type::create_time) {
        last_time = model::timestamp(first_time() + last_timestamp_delta);
    }
    auto new_hdr = hdr;
    new_hdr.first_timestamp = first_time;
    new_hdr.max_timestamp = last_time;
    new_hdr.record_count = rec_count;
    new_hdr.reset_size_checksum_metadata(ret);
    auto new_batch = model::record_batch(
      new_hdr, std::move(ret), model::record_batch::tag_ctor_ng{});
    co_return new_batch;
}

ss::future<ss::stop_iteration> filter::filter_and_rewrite_with_sink(
  model::compression original, model::record_batch b) {
    ++_stats.batches_processed;
    const auto record_count_before = b.record_count();
    auto to_copy = co_await filter_batch(std::move(b));
    if (to_copy.has_value()) {
        const auto records_to_remove = record_count_before
                                       - to_copy->record_count();
        _stats.records_discarded += records_to_remove;
        bool compactible_batch = is_compactible(to_copy->header());
        if (!compactible_batch) {
            ++_stats.non_compactible_batches;
        }

        co_return co_await _sink(std::move(to_copy).value(), original);
    } else {
        ++_stats.batches_discarded;
        _stats.records_discarded += record_count_before;
    }

    co_return ss::stop_iteration::no;
}

} // namespace compaction
