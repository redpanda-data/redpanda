#include "storage/compaction_reducers.h"

#include "random/generators.h"

#include <absl/algorithm/container.h>

namespace storage::internal {
ss::future<ss::stop_iteration>
truncation_offset_reducer::operator()(compacted_index::entry&& e) {
    using stop_t = ss::stop_iteration;
    const model::offset o = e.offset + model::offset(e.delta);
    if (e.type == compacted_index::entry_type::truncation) {
        auto it = _indices.lower_bound(o);
        _indices.erase(it, _indices.end());
    } else {
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
            it->second.offset = o;
            it->second.natural_index = _natural_index;
        } else {
            // not found - insert
            // 1. compute memory usage
            while (_mem_usage + e.key.size() >= max_memory_usage
                   && !_indices.empty()) {
                auto mit = _indices.begin();
                auto n = random_generators::get_int<size_t>(
                  0, _indices.size() - 1);
                std::advance(mit, n);
                auto node = _indices.extract(mit);

                // write the entry again - we ran out of scratch space
                _inverted.add(node.mapped().natural_index);
            }

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
} // namespace storage::internal
