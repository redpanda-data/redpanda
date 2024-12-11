/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "container/chunked_hash_map.h"
#include "utils/s3_fifo.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/optimized_optional.hh>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/options.hpp>
#include <boost/intrusive/parent_from_member.hpp>

namespace utils {

/**
 * A basic key-value cache implementation built on top of the s3_fifo::cache.
 */
template<
  typename Key,
  typename Value,
  typename Hash = std::conditional_t<
    detail::has_absl_hash<Key>,
    detail::avalanching_absl_hash<Key>,
    ankerl::unordered_dense::hash<Key>>,
  typename EqualTo = std::equal_to<Key>>
class chunked_kv_cache {
    struct cached_value;
    struct evict;
    using cache_t = s3_fifo::cache<
      cached_value,
      &cached_value::hook,
      evict,
      s3_fifo::default_cache_cost>;

public:
    using config = cache_t::config;

    explicit chunked_kv_cache(config config)
      : _cache{config, evict{*this}} {}

    ~chunked_kv_cache() noexcept = default;

    // These contructors need to be deleted to ensure a stable `this` pointer.
    chunked_kv_cache(chunked_kv_cache&&) = delete;
    chunked_kv_cache& operator=(chunked_kv_cache&&) noexcept = delete;
    chunked_kv_cache(const chunked_kv_cache&) = delete;
    chunked_kv_cache& operator=(const chunked_kv_cache&) noexcept = delete;

    /**
     * Inserts a value for a given key into the cache.
     *
     * Returns true if the value was inserted and false if there was already a
     * value for the given key in the cache.
     */
    bool try_insert(const Key& key, ss::shared_ptr<Value> val);

    /**
     * Gets the key's corresponding value from the cache.
     *
     * Returns std::nullopt if the key doesn't have a value in the cache.
     */
    ss::optimized_optional<ss::shared_ptr<Value>> get_value(const Key& key);

    using cache_stat = struct cache_t::stat;
    /**
     * Cache statistics.
     */
    struct stat : public cache_stat {
        /// Current size of the cache index.
        size_t index_size;
    };

    /**
     * Returns the current cache statistics.
     */
    [[nodiscard]] stat stat() const noexcept;

private:
    using ghost_hook_t = boost::intrusive::list_member_hook<
      boost::intrusive::link_mode<boost::intrusive::safe_link>>;

    struct cached_value {
        Key key;
        ss::shared_ptr<Value> value;
        s3_fifo::cache_hook hook;
        ghost_hook_t ghost_hook;
    };

    using entry_t = std::unique_ptr<cached_value>;
    using ghost_fifo_t = boost::intrusive::list<
      cached_value,
      boost::intrusive::
        member_hook<cached_value, ghost_hook_t, &cached_value::ghost_hook>>;

    chunked_hash_map<Key, entry_t, Hash, EqualTo> _map;
    cache_t _cache;
    ghost_fifo_t _ghost_fifo;

    void gc_ghost_fifo();
};

template<typename Key, typename Value, typename Hash, typename EqualTo>
struct chunked_kv_cache<Key, Value, Hash, EqualTo>::evict {
    chunked_kv_cache& kv_c;

    bool operator()(cached_value& e) noexcept {
        e.value = nullptr;
        kv_c._ghost_fifo.push_back(e);
        return true;
    }
};

template<typename Key, typename Value, typename Hash, typename EqualTo>
bool chunked_kv_cache<Key, Value, Hash, EqualTo>::try_insert(
  const Key& key, ss::shared_ptr<Value> val) {
    gc_ghost_fifo();

    auto e_it = _map.find(key);
    if (e_it == _map.end()) {
        auto [e_it, succ] = _map.try_emplace(
          key, std::make_unique<cached_value>(key, std::move(val)));
        if (!succ) {
            return false;
        }

        _cache.insert(*e_it->second);
        return true;
    }

    auto& entry = *e_it->second;
    if (entry.hook.evicted()) {
        entry.value = std::move(val);
        _ghost_fifo.erase(_ghost_fifo.iterator_to(entry));
        _cache.insert(entry);
        return true;
    }

    return false;
}

template<typename Key, typename Value, typename Hash, typename EqualTo>
ss::optimized_optional<ss::shared_ptr<Value>>
chunked_kv_cache<Key, Value, Hash, EqualTo>::get_value(const Key& key) {
    gc_ghost_fifo();

    auto e_it = _map.find(key);
    if (e_it == _map.end()) {
        return std::nullopt;
    }

    auto& entry = *e_it->second;
    if (entry.hook.evicted()) {
        return std::nullopt;
    }

    entry.hook.touch();
    return entry.value;
}

template<typename Key, typename Value, typename Hash, typename EqualTo>
void chunked_kv_cache<Key, Value, Hash, EqualTo>::gc_ghost_fifo() {
    for (auto it = _ghost_fifo.begin(); it != _ghost_fifo.end();) {
        auto& entry = *it;
        if (_cache.ghost_queue_contains(entry)) {
            // The ghost queue is in fifo-order so any entry that comes after an
            // entry that hasn't been evicted will also not be evicted.
            return;
        }

        it = _ghost_fifo.erase(it);
        _map.erase(entry.key);
    }
}
template<typename Key, typename Value, typename Hash, typename EqualTo>
struct chunked_kv_cache<Key, Value, Hash, EqualTo>::stat
chunked_kv_cache<Key, Value, Hash, EqualTo>::stat() const noexcept {
    struct stat s {
        _cache.stat()
    };
    s.index_size = _map.size();
    return s;
}

} // namespace utils
