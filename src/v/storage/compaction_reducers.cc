// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction_reducers.h"

#include "base/vlog.h"
#include "compaction/key.h"
#include "compaction/utils.h"
#include "model/batch_compression.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/record_utils.h"
#include "random/generators.h"
#include "storage/compacted_index.h"
#include "storage/compaction_key.h"
#include "storage/index_state.h"
#include "storage/logger.h"
#include "storage/record_batch_utils.h"
#include "storage/segment_utils.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>

#include <boost/range/irange.hpp>

#include <algorithm>
#include <exception>

namespace storage::internal {

ss::future<ss::stop_iteration>
compaction_key_reducer::operator()(compacted_index::entry&& e) {
    using stop_t = ss::stop_iteration;
    const model::offset o = e.offset + model::offset(e.delta);

    auto [begin, end] = _indices.equal_range(_hasher(e.key));
    auto it = std::find_if(begin, end, [&e](const auto& entry) {
        return entry.second.key == e.key;
    });

    if (it != end) {
        if (o > it->second.offset) {
            // cannot be std::max() because _natural_index must be preserved
            it->second.offset = o;
            it->second.natural_index = _natural_index;
        }
    } else {
        // not found - insert
        const auto key_size = e.key.size();
        const auto expected_size = [this, key_size] {
            return idx_mem_usage() + _keys_mem_usage + key_size;
        };

        // if index allocates to much memory remove some entries.
        while (expected_size() >= _max_mem && !_indices.empty()) {
            /**
             * Evict first entry, we use hash function that guarante good
             * randomness so evicting first entry is actually evicting a
             * pseudo random elemnent
             */
            auto mit = _indices.begin();
            _keys_mem_usage -= mit->second.key.size();

            // write the entry again - we ran out of scratch space
            _inverted.add(mit->second.natural_index);
            _indices.erase(mit);
        }
        // TODO: account for short string optimisation here
        _keys_mem_usage += e.key.size();
        // 2. do the insertion
        _indices.emplace(
          _hasher(e.key), value_type(std::move(e.key), o, _natural_index));
    }

    ++_natural_index; // MOST important
    return ss::make_ready_future<stop_t>(stop_t::no);
}
roaring::Roaring compaction_key_reducer::end_of_stream() {
    // TODO: optimization - detect if the index does not need compaction
    // by linear scan of natural_index from 0-N with no gaps.
    for (auto& e : _indices) {
        _inverted.add(e.second.natural_index);
    }
    _inverted.shrinkToFit();
    return std::move(_inverted);
}

ss::future<ss::stop_iteration>
index_copy_reducer::operator()(compacted_index::entry&& e) {
    using stop_t = ss::stop_iteration;
    return _writer->append(std::move(e)).then([] {
        return ss::make_ready_future<stop_t>(stop_t::no);
    });
}

ss::future<ss::stop_iteration>
index_filtered_copy_reducer::operator()(compacted_index::entry&& e) {
    using stop_t = ss::stop_iteration;
    const bool should_add = _bm.contains(_natural_index);
    ++_natural_index;
    if (should_add) {
        return _writer->index(e.key, e.offset, e.delta)
          .then([k = std::move(e.key)] {
              return ss::make_ready_future<stop_t>(stop_t::no);
          });
    }
    return ss::make_ready_future<stop_t>(stop_t::no);
}

ss::future<ss::stop_iteration>
compacted_offset_list_reducer::operator()(compacted_index::entry&& e) {
    using stop_t = ss::stop_iteration;
    const model::offset o = e.offset + model::offset(e.delta);
    _list.add(o);
    return ss::make_ready_future<stop_t>(stop_t::no);
}

ss::future<> copy_data_segment_reducer::maybe_keep_offset(
  const model::record_batch& batch,
  const model::record& r,
  bool is_last_record_in_batch,
  std::vector<int32_t>& offset_deltas) {
    if (co_await _should_keep_fn(batch, r, is_last_record_in_batch)) {
        offset_deltas.push_back(r.offset_delta());
        co_return;
    }
}

model::record_batch copy_data_segment_reducer::make_placeholder_batch(
  model::record_batch_header& hdr) {
    model::record_batch_header new_hdr;
    new_hdr.type = model::record_batch_type::compaction_placeholder;
    new_hdr.base_offset = hdr.base_offset;
    new_hdr.last_offset_delta = hdr.last_offset_delta;
    new_hdr.first_timestamp = hdr.first_timestamp;
    new_hdr.max_timestamp = hdr.max_timestamp;
    // Maintain idempotency related state
    new_hdr.base_sequence = hdr.base_sequence;
    new_hdr.producer_id = hdr.producer_id;
    new_hdr.producer_epoch = hdr.producer_epoch;
    auto no_records = iobuf{};
    new_hdr.reset_size_checksum_metadata(no_records);
    return model::record_batch(
      new_hdr, std::move(no_records), model::record_batch::tag_ctor_ng{});
}

ss::future<std::optional<model::record_batch>>
copy_data_segment_reducer::filter(model::record_batch batch) {
    const auto is_last_batch_in_segment = batch.last_offset()
                                          == _segment_last_offset;
    // Compaction placeholder batches can actually be removed under two
    // conditions- that they are _not_ the last batch in a segment, nor are they
    // the last batch for an idempotent producer. The former can happen if e.g.
    // two segments containing placeholder batches are adjacently merged.
    if (
      (batch.header().type == model::record_batch_type::compaction_placeholder)
      && !is_last_batch_in_segment
      && !_stm_mgr->is_batch_in_idempotent_window(batch.header())) {
        co_return std::nullopt;
    }

    // do not filter non-removable batch types under any circumstances
    if (!compaction::is_filterable(batch.header().type)) {
        co_return std::move(batch);
    }

    // 1. compute which records to keep
    std::vector<int32_t> offset_deltas;
    offset_deltas.reserve(batch.record_count());

    int32_t records_seen = 0;
    co_await batch.for_each_record_async(
      [this, &batch, &offset_deltas, &records_seen](const model::record& r) {
          ++records_seen;
          return maybe_keep_offset(
            batch, r, batch.record_count() == records_seen, offset_deltas);
      });

    if (offset_deltas.empty() && _compaction_placeholder_enabled) {
        auto is_batch_in_idempotent_window
          = _stm_mgr->is_batch_in_idempotent_window(batch.header());
        if (is_last_batch_in_segment || is_batch_in_idempotent_window) {
            // last batch in the segment or for the producer has been compacted
            // away. This is most likely caused by aborted data batches getting
            // compacted away during self compaction of the segment if they are
            // the last batch in the segment. We install a placeholder batch of
            // same size to retain contiguousness of the offset space.
            auto placeholder = make_placeholder_batch(batch.header());
            vlog(
              gclog.debug,
              "installing a placeholder {} for compacted batch: {}",
              placeholder,
              batch);
            co_return placeholder;
        }
    }

    // 2. no record to keep
    if (offset_deltas.empty()) {
        co_return std::nullopt;
    }

    // 3. keep all records
    if (offset_deltas.size() == static_cast<size_t>(batch.record_count())) {
        auto& header = batch.header();
        if (
          header.type == model::record_batch_type::raft_data
          && header.attrs.is_transactional() && !header.attrs.is_control()) {
            if (likely(_unset_transaction_bit_enabled)) {
                vlog(
                  gclog.trace,
                  "Removing transactional bit for raft batch {}",
                  header);
                header.attrs.remove_transactional_type();
                header.reset_size_checksum_metadata(batch.data());
            }
        }
        co_return std::move(batch);
    }

    // 4. filter
    iobuf ret;
    int32_t rec_count = 0;
    std::optional<int64_t> first_timestamp_delta;
    int64_t last_timestamp_delta;
    batch.for_each_record([&rec_count,
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
    // From: DefaultRecordBatch.java
    // On Compaction: Unlike the older message formats, magic v2 and above
    // preserves the first and last offset/sequence numbers from the
    // original batch when the log is cleaned. This is required in order to
    // be able to restore the producer'size() state when the log is reloaded. If
    // we did not retain the last sequence number, then following a
    // partition leader failure, once the new leader has rebuilt the
    // producer state from the log, the next sequence expected number would
    // no longer be in sync with what was written by the client. This would
    // cause an unexpected OutOfOrderSequence error, which is typically
    // fatal. The base sequence number must be preserved for duplicate
    // checking: the broker checks incoming Produce requests for duplicates
    // by verifying that the first and last sequence numbers of the incoming
    // batch match the last from that producer.
    //
    if (rec_count == 0) {
        // TODO:agallego - implement
        //
        // Note that if all of the records in a batch are removed during
        // compaction, the broker may still retain an empty batch header in
        // order to preserve the producer sequence information as described
        // above. These empty batches are retained only until either a new
        // sequence number is written by the corresponding producer or the
        // producerId is expired from lack of activity.
        co_return std::nullopt;
    }

    // There is no similar need to preserve the timestamp from the original
    // batch after compaction. The FirstTimestamp field therefore always
    // reflects the timestamp of the first record in the batch. If the batch is
    // empty, the FirstTimestamp will be set to -1 (NO_TIMESTAMP).
    //
    // Similarly, the MaxTimestamp field reflects the maximum timestamp of the
    // current records if the timestamp type is CREATE_TIME. For
    // LOG_APPEND_TIME, on the other hand, the MaxTimestamp field reflects the
    // timestamp set by the broker and is preserved after compaction.
    // Additionally, the MaxTimestamp of an empty batch always retains the
    // previous value prior to becoming empty.
    //
    auto& hdr = batch.header();
    const auto first_time = model::timestamp(
      hdr.first_timestamp() + first_timestamp_delta.value());
    auto last_time = hdr.max_timestamp;
    if (hdr.attrs.timestamp_type() == model::timestamp_type::create_time) {
        last_time = model::timestamp(first_time() + last_timestamp_delta);
    }
    auto new_hdr = hdr;

    // Remove transactional bit for committed raft data batches.
    if (
      new_hdr.type == model::record_batch_type::raft_data
      && new_hdr.attrs.is_transactional() && !new_hdr.attrs.is_control()) {
        if (likely(_unset_transaction_bit_enabled)) {
            vlog(
              gclog.trace,
              "Removing transactional bit for raft batch {}",
              new_hdr);
            new_hdr.attrs.remove_transactional_type();
        }
    }

    new_hdr.first_timestamp = first_time;
    new_hdr.max_timestamp = last_time;
    new_hdr.record_count = rec_count;
    // Remove compression bit, as this batch isn't compressed. If the original
    // batch is compressed, the caller will re-compress.
    new_hdr.attrs.remove_compression();
    new_hdr.reset_size_checksum_metadata(ret);
    auto new_batch = model::record_batch(
      new_hdr, std::move(ret), model::record_batch::tag_ctor_ng{});
    co_return new_batch;
}

ss::future<ss::stop_iteration> copy_data_segment_reducer::filter_and_append(
  model::compression original, model::record_batch b) {
    ++_stats.batches_processed;
    using stop_t = ss::stop_iteration;
    const auto record_count_before = b.record_count();
    auto is_control_batch = b.header().attrs.is_control();
    auto maybe_batch = co_await filter(std::move(b));
    if (maybe_batch == std::nullopt) {
        ++_stats.batches_discarded;
        if (is_control_batch) {
            ++_stats.control_batches_discarded;
        }
        _stats.records_discarded += record_count_before;
        co_return stop_t::no;
    }
    auto batch = std::move(maybe_batch.value());
    const auto records_to_remove = record_count_before - batch.record_count();
    _stats.records_discarded += records_to_remove;
    bool compactible_batch = compaction::is_compactible(batch.header());
    if (!compactible_batch) {
        ++_stats.non_compactible_batches;
    }
    if (_compacted_idx && compactible_batch) {
        co_await model::for_each_record(
          batch, [&batch, this](const model::record& r) {
              auto& hdr = batch.header();
              return _compacted_idx->index(
                hdr.type,
                hdr.attrs.is_control(),
                r.key(),
                batch.base_offset(),
                r.offset_delta());
          });
    }
    if (original != model::compression::none) {
        batch = co_await model::compress_batch(original, std::move(batch));
    }
    const auto start_pos = _appender->file_byte_offset();
    const auto header_size = batch.header().size_bytes;
    _acc += header_size;
    // do not set broker_timestamp in this index, leave the operation to the
    // caller who has more context
    bool filterable_batch = compaction::is_filterable(batch.header().type);
    if (
      _idx.maybe_index(
        _acc,
        segment_index::default_data_buffer_step,
        start_pos,
        batch.base_offset(),
        batch.last_offset(),
        batch.header().first_timestamp,
        batch.header().max_timestamp,
        std::nullopt,
        _internal_topic
          || batch.header().type == model::record_batch_type::raft_data,
        filterable_batch ? batch.header().record_count : 0)) {
        _acc = 0;
    }
    co_await _appender->append(batch);
    vassert(
      _appender->file_byte_offset() == start_pos + header_size,
      "Size must be deterministic. Expected:{} == {}",
      _appender->file_byte_offset(),
      start_pos + header_size);

    co_return stop_t::no;
}

ss::future<ss::stop_iteration>
copy_data_segment_reducer::operator()(model::record_batch b) {
    if (_inject_failure) {
        throw std::runtime_error("injected error");
    }
    if (_as) {
        _as->check();
    }
    const auto comp = b.header().attrs.compression();
    if (!b.compressed()) {
        co_return co_await filter_and_append(comp, std::move(b));
    }
    b = co_await model::decompress_batch(b);
    co_return co_await filter_and_append(comp, std::move(b));
}

ss::future<ss::stop_iteration>
index_rebuilder_reducer::operator()(model::record_batch b) {
    using stop_t = ss::stop_iteration;
    if (b.compressed()) {
        b = co_await model::decompress_batch(b);
    }
    co_await do_index(std::move(b));
    co_return stop_t::no;
}

ss::future<> index_rebuilder_reducer::do_index(model::record_batch&& b) {
    return ss::do_with(std::move(b), [this](model::record_batch& b) {
        return model::for_each_record(
          b,
          [this,
           bt = b.header().type,
           ctrl = b.header().attrs.is_control(),
           o = b.base_offset()](model::record& r) {
              return _w->index(bt, ctrl, r.key(), o, r.offset_delta());
          });
    });
}

void tx_reducer::refresh_ongoing_aborted_txs(const model::record_batch& b) {
    // refresh the running list of aborted transactions
    auto upto = b.last_offset();
    while (!_aborted_txs.empty() && _aborted_txs.top().first <= upto) {
        const auto& top = _aborted_txs.top();
        _ongoing_aborted_txs[top.pid] = top;
        _aborted_txs.pop();
    }
    // discard any inflight aborted transactions if we encounter an
    // abort batch.
    auto is_tx = b.header().attrs.is_transactional();
    auto is_control = b.header().attrs.is_control();
    if (is_tx && is_control) {
        auto batch_type = _stm_mgr->parse_tx_control_batch(b);
        auto pid = model::producer_identity(
          b.header().producer_id, b.header().producer_epoch);
        if (batch_type == model::control_record_type::tx_abort) {
            _ongoing_aborted_txs.erase(pid);
        }
    }
}

bool tx_reducer::can_discard_tx_data_batch(const model::record_batch& b) {
    if (_transactional_stm_type != stm_type::user_topic_transactional) {
        return false;
    }
    auto is_tx = b.header().attrs.is_transactional();
    auto is_data = b.header().type == model::record_batch_type::raft_data;
    auto is_control = b.header().attrs.is_control();
    auto pid = model::producer_identity(
      b.header().producer_id, b.header().producer_epoch);
    return is_tx && is_data && !is_control
           && _ongoing_aborted_txs.contains(pid);
}

bool tx_reducer::can_discard_consumer_offsets_batch(
  const model::record_batch& b) {
    if (_transactional_stm_type != stm_type::consumer_offsets_transactional) {
        return false;
    }
    // Remove all transaction related batches (including data) because the
    // committed data has already been rewritten as separate raft_data
    // batches, so no need to retain originally written group_prepare_tx
    // batches while the transaction is in progress.
    return compaction::is_removable_control_batch(
      _ntp, b.header().type, _tx_batch_compaction_enabled);
}

ss::future<ss::stop_iteration> tx_reducer::operator()(model::record_batch&& b) {
    if (_transactional_stm_type) {
        _stats.batches_processed++;
        refresh_ongoing_aborted_txs(b);
        if (
          can_discard_tx_data_batch(b)
          || can_discard_consumer_offsets_batch(b)) {
            vlog(
              gclog.trace, "discarded batch during compaction: {}", b.header());
            _stats.batches_discarded++;
            co_return ss::stop_iteration::no;
        }
    }
    co_return co_await _delegate(std::move(b));
}

ss::future<ss::stop_iteration> map_building_reducer::maybe_index_record_in_map(
  const model::record& r,
  model::offset base_offset,
  model::record_batch_type type,
  bool is_control,
  bool& fully_indexed_batch) {
    auto offset = base_offset + model::offset_delta(r.offset_delta());
    if (offset < _start_offset) {
        co_return ss::stop_iteration::no;
    }

    auto key_view = iobuf_to_bytes(r.key());
    auto key = enhance_key(type, is_control, key_view);
    bool success = co_await _map->put(key, offset);

    if (success) {
        co_return ss::stop_iteration::no;
    }

    fully_indexed_batch = false;
    co_return ss::stop_iteration::yes;
}

ss::future<ss::stop_iteration>
map_building_reducer::operator()(model::record_batch batch) {
    bool fully_indexed_batch = true;
    auto& header = batch.header();
    if (!compaction::is_compactible(header)) {
        // There is no point to indexing records in uncompactible batches, since
        // their inclusion in the segment post compaction is irrespective of the
        // map state (see copy_data_segment_reducer::filter()).
        co_return ss::stop_iteration::no;
    }
    if (batch.compressed()) {
        batch = co_await model::decompress_batch(batch);
    }
    // is_control must be false below due to above `is_compactible()` check.
    co_await batch.for_each_record_async(
      [this,
       &fully_indexed_batch,
       base_offset = batch.base_offset(),
       type = header.type,
       is_control = false](
        const model::record& r) -> ss::future<ss::stop_iteration> {
          return maybe_index_record_in_map(
            r, base_offset, type, is_control, fully_indexed_batch);
      });

    if (fully_indexed_batch) {
        co_return ss::stop_iteration::no;
    }
    _fully_indexed_segment = false;
    co_return ss::stop_iteration::yes;
}

} // namespace storage::internal
