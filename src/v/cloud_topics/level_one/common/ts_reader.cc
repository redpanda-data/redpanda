/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/ts_reader.h"

#include "bytes/iostream.h"
#include "model/record_batch_types.h"
#include "storage/record_batch_utils.h"

#include <fmt/core.h>

#include <stdexcept>

namespace cloud_topics::l1 {

namespace {

// Whether `batch` belongs to one of the aborted transactions in `aborted`,
// matched by producer identity and (inclusive) log-offset range.
//
// This mirrors kafka::is_aborted (kafka/utils/txn_reader.cc), the canonical
// read_committed matcher. It is replicated here -- rather than depending on
// kafka/utils -- to keep this transitional TS-import code contained within
// cloud_topics and to avoid a build cycle (kafka/utils -> kafka/data ->
// cloud_topics). The set must be ordered with std::greater for the lookup.
bool is_aborted(
  const model::record_batch& batch, const aborted_transactions& aborted) {
    model::producer_identity pid{
      batch.header().producer_id, batch.header().producer_epoch};
    auto it = aborted.lower_bound(
      model::tx_range{pid, batch.base_offset(), model::offset::max()});
    if (it == aborted.end()) {
        return false;
    }
    return it->pid == pid && batch.base_offset() >= it->first
           && batch.last_offset() <= it->last;
}

} // namespace

tiered_storage_object_reader::tiered_storage_object_reader(
  ss::input_stream<char> stream,
  model::offset_delta initial_delta,
  model::term_id term,
  aborted_transactions aborted)
  : _stream(std::move(stream))
  , _running_delta(initial_delta)
  , _term(term)
  , _aborted(std::move(aborted)) {}

ss::future<> tiered_storage_object_reader::close() { return _stream.close(); }

ss::future<object_reader::peek_result> tiered_storage_object_reader::peek() {
    if (!_peeked.has_value() && !_eof) {
        auto next = co_await fetch_next_translated();
        if (next.has_value()) {
            _peeked = std::move(*next);
        } else {
            _eof = true;
        }
    }
    if (_eof) {
        co_return eof{};
    }
    co_return _peeked->header();
}

ss::future<object_reader::result> tiered_storage_object_reader::read_next() {
    if (_peeked.has_value()) {
        auto batch = std::move(*_peeked);
        _peeked.reset();
        co_return std::move(batch);
    }
    if (_eof) {
        co_return eof{};
    }
    auto next = co_await fetch_next_translated();
    if (!next.has_value()) {
        co_return eof{};
    }
    co_return std::move(*next);
}

ss::future<std::optional<model::record_batch>>
tiered_storage_object_reader::fetch_next_translated() {
    static const auto translator_types = model::offset_translator_batch_types();
    for (;;) {
        auto header_buf = co_await read_iobuf_exactly(
          _stream, model::packed_record_batch_header_size);
        if (header_buf.size_bytes() < model::packed_record_batch_header_size) {
            co_return std::nullopt;
        }
        auto header = storage::batch_header_from_disk_iobuf(
          std::move(header_buf));
        auto records_size = static_cast<size_t>(header.size_bytes)
                            - model::packed_record_batch_header_size;
        auto records_buf = co_await read_iobuf_exactly(_stream, records_size);
        if (records_buf.size_bytes() != records_size) {
            throw std::runtime_error(
              fmt::format(
                "truncated TS segment: expected {} record bytes, got {}",
                records_size,
                records_buf.size_bytes()));
        }
        // Non-data batches that the offset translator strips: advance the
        // running delta by the number of log offsets they consume (matching
        // raft::offset_translator) and skip them.
        if (std::ranges::contains(translator_types, header.type)) {
            _running_delta += static_cast<int64_t>(
              header.last_offset_delta + 1);
            continue;
        }
        // Emit only raft_data, like the production cloud read path. Any other
        // batch type (e.g. tx_fence) still consumes a Kafka offset -- leaving a
        // gap -- but is never surfaced to the fetch path, and does not move the
        // delta (it is not an offset-translator type).
        if (header.type != model::record_batch_type::raft_data) {
            continue;
        }
        // Drop transaction control batches (commit/abort markers). They are
        // never surfaced to Kafka clients -- native CT L1 strips them during
        // L0->L1 reconciliation -- so imported reads must do the same. Like an
        // aborted data batch, a control batch consumes a Kafka offset (leaving
        // a gap) but does not move the delta.
        if (header.attrs.is_control()) {
            continue;
        }
        auto batch = model::record_batch(
          header, std::move(records_buf), model::record_batch::tag_ctor_ng{});
        // Drop aborted transactional data so the imported region is
        // committed-only, matching native CT L1 (where L0->L1 reconciliation
        // strips aborts). Matched in raw log-offset space -- where the .tx
        // ranges live -- before the base offset is translated. Like a control
        // batch, an aborted batch consumes a Kafka offset (leaving a gap) but
        // does not move the delta.
        if (header.attrs.is_transactional() && is_aborted(batch, _aborted)) {
            continue;
        }
        batch.header().base_offset = model::offset{
          batch.header().base_offset() - _running_delta()};
        // Stamp the segment's term so the Kafka fetch path reports it as the
        // partition leader epoch (ctx.term is runtime context, not stored on
        // disk, so it is otherwise unset -> serialized as epoch -1).
        batch.set_term(_term);
        co_return batch;
    }
}

} // namespace cloud_topics::l1
