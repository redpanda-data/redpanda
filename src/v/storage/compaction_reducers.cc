#include "storage/compaction_reducers.h"

#include "model/record_utils.h"
#include "random/generators.h"
#include "storage/logger.h"
#include "storage/segment_appender_utils.h"
#include "vlog.h"

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>

#include <algorithm>

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
    return _inverted;
}

ss::future<ss::stop_iteration>
index_filtered_copy_reducer::operator()(compacted_index::entry&& e) {
    using stop_t = ss::stop_iteration;
    const bool should_add = _bm.contains(_i);
    ++_i;
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
    for (auto& r : batch) {
        if (should_keep(base, r.offset_delta())) {
            offset_deltas.push_back(r.offset_delta());
        }
    }

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
    for (auto& record : batch) {
        // contains the key
        if (std::count(
              offset_deltas.begin(),
              offset_deltas.end(),
              record.offset_delta())) {
            ret.push_back(record.share());
        }
    }
    auto new_batch = model::record_batch(batch.header(), std::move(ret));
    new_batch.header().crc = model::crc_record_batch(new_batch);
    new_batch.header().header_crc = model::internal_header_only_crc(
      new_batch.header());
    return new_batch;
}

ss::future<ss::stop_iteration>
copy_data_segment_reducer::operator()(model::record_batch&& b) {
    // NOTE: since we do not have transaction support. we don't special case
    // the idempotent producer/transactions
    using stop_t = ss::stop_iteration;
    if (b.compressed()) {
        // TODO / FIXME
        vlog(
          stlog.error,
          "compacted reducer cannot handle compressed batches yet - {}",
          b.header());
        return ss::make_ready_future<stop_t>(stop_t::no);
    }
    auto to_copy = filter(std::move(b));
    if (to_copy == std::nullopt) {
        return ss::make_ready_future<stop_t>(stop_t::no);
    }
    return ss::do_with(
             std::move(to_copy.value()),
             [this](model::record_batch& batch) {
                 return storage::write(*_appender, batch);
             })
      .then([] { return ss::make_ready_future<stop_t>(stop_t::no); });
}
} // namespace storage::internal
