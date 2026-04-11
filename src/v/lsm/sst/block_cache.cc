// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/sst/block_cache.h"

#include "container/chunked_hash_map.h"
#include "ssx/semaphore.h"
#include "utils/chunked_kv_cache.h"

#include <seastar/core/coroutine.hh>

namespace lsm::sst {

struct cache_key {
    cache_key(internal::file_id id, block::handle h)
      : id(id)
      , offset(h.offset) {}

    internal::file_id id;
    uint64_t offset = 0;

    bool operator==(const cache_key&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const cache_key& k) {
        return H::combine(std::move(h), k.id, k.offset);
    }
};

class block_cache::impl {
public:
    explicit impl(size_t max_entries, ss::lw_shared_ptr<probe> probe)
      : _cache(compute_cache_config(max_entries))
      , _probe(std::move(probe)) {}

    ~impl() {
        vassert(
          _mu_map.empty(),
          "no locks should be in progress during destruction.");
    }

    ss::future<> lock(internal::file_id id, block::handle h) {
        auto it = _mu_map.find({id, h});
        ssx::semaphore* mu = nullptr;
        if (it == _mu_map.end()) {
            auto inserted = _mu_map.emplace(
              cache_key{id, h},
              std::make_unique<ssx::semaphore>(
                1, "lsm::sst::block_cache::impl"));
            vassert(inserted.second, "expected mutex to be inserted");
            mu = inserted.first->second.get();
        } else {
            mu = it->second.get();
        }
        return mu->wait();
    }
    void unlock(internal::file_id id, block::handle h) noexcept {
        auto it = _mu_map.find({id, h});
        vassert(
          it != _mu_map.end(),
          "unlock must be mirrored with a successful lock call");
        auto& mu = it->second;
        mu->signal();
        // If there are no waiters and no one else now holding the lock, we can
        // cleanup the entry in the map.
        if (mu->waiters() == 0 && mu->available_units() == 1) {
            _mu_map.erase(it);
        }
    }
    void insert(internal::file_id id, block::handle h, block::reader reader) {
        _cache.try_insert({id, h}, ss::make_shared(std::move(reader)));
    }
    std::optional<block::reader> get(internal::file_id id, block::handle h) {
        auto value = _cache.get_value({id, h});
        if (value) {
            _probe->block_cache_hit += 1;
            return std::make_optional(**value);
        } else {
            _probe->block_cache_miss += 1;
            return std::nullopt;
        }
    }

private:
    struct block_cost_fn {
        size_t operator()(const block::reader& reader) noexcept {
            return reader.size_bytes();
        }
    };
    using cache_t = utils::chunked_kv_cache<cache_key, block::reader>;
    static cache_t::config compute_cache_config(size_t max_entries) {
        auto main_cache_size = static_cast<size_t>(
          static_cast<double>(max_entries) * 0.90);
        return cache_t::config{
          .cache_size = main_cache_size,
          .small_size = max_entries - main_cache_size,
        };
    }

    chunked_hash_map<cache_key, std::unique_ptr<ssx::semaphore>> _mu_map;
    cache_t _cache;
    ss::lw_shared_ptr<probe> _probe;
};

block_cache::handle::handle(
  block_cache::impl* cache, internal::file_id id, block::handle handle) noexcept
  : _cache(cache)
  , _id(id)
  , _handle(handle) {}
block_cache::handle::~handle() noexcept {
    if (_cache) {
        _cache->unlock(_id, _handle);
    }
}

void block_cache::handle::insert(block::reader rdr) {
    _cache->insert(_id, _handle, std::move(rdr));
}
std::optional<block::reader> block_cache::handle::get() {
    return _cache->get(_id, _handle);
}

block_cache::block_cache(size_t max_entries, ss::lw_shared_ptr<probe> p)
  : _impl(std::make_unique<impl>(max_entries, std::move(p))) {}

block_cache::~block_cache() = default;

ss::future<block_cache::handle>
block_cache::get(internal::file_id id, block::handle handle) {
    co_await _impl->lock(id, handle);
    co_return block_cache::handle(_impl.get(), id, handle);
}

} // namespace lsm::sst
