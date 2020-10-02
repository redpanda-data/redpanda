#include "storage/compaction_reducers.h"

#include "compression/compression.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "random/generators.h"
#include "storage/index_state.h"
#include "storage/logger.h"
#include "storage/parser_utils.h"
#include "storage/segment_appender_utils.h"
#include "storage/segment_utils.h"
#include "vlog.h"

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>
#include <boost/range/irange.hpp>

#include <algorithm>
#include <exception>

namespace storage::internal {
ss::future<ss::stop_iteration>
truncation_offset_reducer::operator()(compacted_index::entry&& e) {
    using stop_t = ss::stop_iteration;
    const model::offset o = e.offset + model::offset(e.delta);
    if (e.type == compacted_index::entry_type::truncation) {
        auto it = _indices.lower_bound(o);
        _indices.erase(it, _indices.end());
    } else if (e.type == compacted_index::entry_type::key) {
        _indices[o] = _natural_index;
    }
    ++_natural_index; // MOST important
    return ss::make_ready_future<stop_t>(stop_t::no);
}

Roaring truncation_offset_reducer::end_of_stream() {
    Roaring inverted;
    for (auto& [_, natural] : _indices) {
        inverted.add(natural);
    }
    inverted.shrinkToFit();
    return inverted;
}
ss::future<ss::stop_iteration>
compaction_key_reducer::operator()(compacted_index::entry&& e) {
    using stop_t = ss::stop_iteration;
    const model::offset o = e.offset + model::offset(e.delta);
    const bool skip = _to_keep && !_to_keep->contains(_natural_index);
    if (!skip) {
        auto it = _indices.find(e.key);
        if (it != _indices.end()) {
            if (o > it->second.offset) {
                // cannot be std::max() because _natural_index must be preserved
                it->second.offset = o;
                it->second.natural_index = _natural_index;
            }
        } else {
            // not found - insert
            // 1. compute memory usage
            while (_mem_usage + e.key.size() >= _max_mem && !_indices.empty()) {
                auto mit = _indices.begin();
                auto n = random_generators::get_int<size_t>(
                  0, _indices.size() - 1);
                std::advance(mit, n);
                auto node = _indices.extract(mit);
                bytes key = node.key();
                _mem_usage -= key.size();

                // write the entry again - we ran out of scratch space
                _inverted.add(node.mapped().natural_index);
            }
            _mem_usage += e.key.size();
            // 2. do the insertion
            _indices.emplace(std::move(e.key), value_type(o, _natural_index));
        }
    }
    ++_natural_index; // MOST important
    return ss::make_ready_future<stop_t>(stop_t::no);
}
Roaring compaction_key_reducer::end_of_stream() {
    // TODO: optimization - detect if the index does not need compaction
    // by linear scan of natural_index from 0-N with no gaps.
    for (auto& e : _indices) {
        _inverted.add(e.second.natural_index);
    }
    _inverted.shrinkToFit();
    return std::move(_inverted);
}

ss::future<ss::stop_iteration>
index_filtered_copy_reducer::operator()(compacted_index::entry&& e) {
    using stop_t = ss::stop_iteration;
    const bool should_add = _bm.contains(_natural_index);
    ++_natural_index;
    if (should_add) {
        bytes_view bv = e.key;
        return _writer->index(bv, e.offset, e.delta)
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

std::optional<model::record_batch>
copy_data_segment_reducer::filter(model::record_batch&& batch) {
    // 1. compute which records to keep
    const auto base = batch.base_offset();
    std::vector<int32_t> offset_deltas;
    offset_deltas.reserve(batch.record_count());
    batch.for_each_record([this, base, &offset_deltas](const model::record& r) {
        if (should_keep(base, r.offset_delta())) {
            offset_deltas.push_back(r.offset_delta());
        }
    });

    // 2. no record to keep
    if (offset_deltas.empty()) {
        return std::nullopt;
    }

    // 3. keep all records
    if (offset_deltas.size() == static_cast<size_t>(batch.record_count())) {
        return std::move(batch);
    }

    // 4. filter
    model::record_batch::uncompressed_records ret;
    batch.for_each_record([&ret, &offset_deltas](model::record record) {
        // contains the key
        if (std::count(
              offset_deltas.begin(),
              offset_deltas.end(),
              record.offset_delta())) {
            ret.push_back(record.share());
        }
    });
    // From: DefaultRecordBatch.java
    // On Compaction: Unlike the older message formats, magic v2 and above
    // preserves the first and last offset/sequence numbers from the
    // original batch when the log is cleaned. This is required in order to
    // be able to restore the producer's state when the log is reloaded. If
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
    if (ret.empty()) {
        // TODO:agallego - implement
        //
        // Note that if all of the records in a batch are removed during
        // compaction, the broker may still retain an empty batch header in
        // order to preserve the producer sequence information as described
        // above. These empty batches are retained only until either a new
        // sequence number is written by the corresponding producer or the
        // producerId is expired from lack of activity.
        return std::nullopt;
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
    const int32_t rec_count = ret.size();
    const auto& oldh = batch.header();
    const auto first_time = model::timestamp(
      oldh.first_timestamp() + ret.front().timestamp_delta());
    auto last_time = oldh.max_timestamp;
    if (oldh.attrs.timestamp_type() == model::timestamp_type::create_time) {
        last_time = model::timestamp(
          first_time() + ret.back().timestamp_delta());
    }
    auto new_batch = model::record_batch(oldh, std::move(ret));
    auto& h = new_batch.header();
    h.first_timestamp = first_time;
    h.max_timestamp = last_time;
    h.record_count = rec_count;
    reset_size_checksum_metadata(new_batch);
    return new_batch;
}
ss::future<ss::stop_iteration> copy_data_segment_reducer::do_compaction(
  model::compression original, model::record_batch&& b) {
    using stop_t = ss::stop_iteration;
    auto to_copy = filter(std::move(b));
    if (to_copy == std::nullopt) {
        return ss::make_ready_future<stop_t>(stop_t::no);
    }
    return compress_batch(original, std::move(to_copy.value()))
      .then([this](model::record_batch&& b) {
          return ss::do_with(std::move(b), [this](model::record_batch& batch) {
              auto const start_offset = _appender->file_byte_offset();
              auto const header_size = batch.header().size_bytes;
              _acc += header_size;
              if (_idx.maybe_index(
                    _acc,
                    32_KiB,
                    start_offset,
                    batch.base_offset(),
                    batch.last_offset(),
                    batch.header().first_timestamp,
                    batch.header().max_timestamp)) {
                  _acc = 0;
              }
              return storage::write(*_appender, batch)
                .then([this, start_offset, header_size] {
                    vassert(
                      _appender->file_byte_offset()
                        == start_offset + header_size,
                      "Size must be deterministic. Expected:{} == {}",
                      _appender->file_byte_offset(),
                      start_offset + header_size);
                });
          });
      })
      .then([] { return ss::make_ready_future<stop_t>(stop_t::no); });
}

ss::future<ss::stop_iteration>
copy_data_segment_reducer::operator()(model::record_batch&& b) {
    const auto comp = b.header().attrs.compression();
    if (!b.compressed()) {
        return do_compaction(comp, std::move(b));
    }
    return decompress_batch(std::move(b))
      .then([comp, this](model::record_batch&& b) {
          return do_compaction(comp, std::move(b));
      });
}

ss::future<ss::stop_iteration>
index_rebuilder_reducer::operator()(model::record_batch&& b) {
    using stop_t = ss::stop_iteration;
    auto f = ss::now();
    if (!b.compressed()) {
        f = do_index(std::move(b));
    } else {
        f = internal::decompress_batch(std::move(b))
              .then([this](model::record_batch&& b) {
                  return do_index(std::move(b));
              });
    }
    return f.then([] { return ss::make_ready_future<stop_t>(stop_t::no); });
}

ss::future<> index_rebuilder_reducer::do_index(model::record_batch&& b) {
    return ss::do_with(std::move(b), [this](model::record_batch& b) {
        return model::for_each_record(
          b, [this, o = b.base_offset()](model::record& r) {
              return _w->index(r.key(), o, r.offset_delta());
          });
    });
}
} // namespace storage::internal
