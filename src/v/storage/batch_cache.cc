#include "batch_cache.h"

#include "log_reader.h"

namespace storage {

batch_cache::entry_ptr batch_cache::put(model::record_batch batch) {
    entry* e;
    if (_pool.empty()) {
        e = new entry(std::move(batch));
    } else {
        e = &_pool.front();
        _pool.pop_front();
        e->batch = std::move(batch);
    }
    _lru.push_back(*e);
    return e->weak_from_this();
}

size_t batch_cache::reclaim(size_t size) {
    size_t reclaimed = 0;
    while (reclaimed < size && !_lru.empty()) {
        reclaimed += _lru.front().batch.memory_usage();
        _lru.pop_front_and_dispose([](entry* e) { delete e; });
    }
    while (reclaimed < size && !_pool.empty()) {
        reclaimed += _pool.front().batch.memory_usage();
        _pool.pop_front_and_dispose([](entry* e) { delete e; });
    }
    return reclaimed;
}

std::optional<model::record_batch>
batch_cache_index::get(model::offset offset) {
    if (auto it = find_first_contains(offset); it != _index.end()) {
        _cache.touch(it->second);
        return it->second->batch.share();
    }
    return std::nullopt;
}

batch_cache_index::read_result batch_cache_index::read(
  model::offset offset,
  model::offset max_offset,
  const std::vector<model::record_batch_type>& type_filter,
  size_t max_bytes) {
    read_result ret;
    ret.next_batch = offset;
    if (unlikely(offset > max_offset)) {
        return ret;
    }
    for (auto it = find_first_contains(offset); it != _index.end();) {
        auto& batch = it->second->batch;

        if (filter_batch_type(type_filter, batch.header().type)) {
            ret.batches.emplace_back(batch.share());
            ret.memory_usage += batch.memory_usage();
            _cache.touch(it->second);
        }

        offset = batch.last_offset() + model::offset(1);

        /*
         * we're done in any of the following cases:
         *
         * 1. end of index
         */
        if (++it == _index.end()) {
            break;
        }

        /*
         * 2. cache miss
         * 3. hole in range
         */
        if (!it->second || it->first != offset) {
            // compute the base offset of the next cached batch
            auto next_batch = std::find_if(
              it, _index.end(), [](const index_type::value_type& e) {
                  return bool(e.second);
              });
            if (next_batch != _index.end()) {
                ret.next_cached_batch = next_batch->second->batch.base_offset();
            }
            break;
        }

        /*
         * 4. exceed max offset
         * 5. exceed max bytes
         */
        if (offset > max_offset || ret.memory_usage >= max_bytes) {
            break;
        }
    }

    ret.next_batch = offset;
    return ret;
}

void batch_cache_index::truncate(model::offset offset) {
    if (auto it = find_first(offset); it != _index.end()) {
        // rule out if possible, otherwise always be pessimistic
        if (it->second && !it->second->batch.contains(offset)) {
            ++it;
        }
        std::for_each(it, _index.end(), [this](index_type::value_type& e) {
            _cache.evict(std::move(e.second));
        });
        _index.erase(it, _index.end());
    }
}

} // namespace storage
