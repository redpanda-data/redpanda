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
#include "config/property.h"
#include "container/intrusive_list_helpers.h"

#include <seastar/core/lowres_clock.hh>

#include <absl/container/node_hash_map.h>
#include <boost/intrusive/list_hook.hpp>

#include <algorithm>
#include <exception>

namespace cluster {
using clock_t = ss::lowres_clock;
template<typename Entry>
concept entry_with_timestamp = requires(
  const Entry& const_entry, Entry& entry) {
    typename Entry::clock_type;
    {
        const_entry.get_last_update_timestamp()
    } -> std::same_as<typename Entry::clock_type::time_point>;
    { entry.touch() };
};

/**
 * Post eviction hook is called after an entry has been evicted from the cache.
 *
 * The post_eviction_hook callable may be used to cleanup some additional state
 * depending on application. f.e. it may release resources.
 */
template<typename PostEvictionHookT, typename Entry>
concept post_eviction_hook = requires(
  const PostEvictionHookT& hook, Entry& entry) {
    requires noexcept(hook(entry));
    { hook(entry) } -> std::same_as<void>;
};
/**
 * Simillar to `experimental::io::cache_evictor` it is a callable that is
 * called before an entry is evicted. When the callable returns `true` it may
 * assume that entry is evicted.
 *
 * If an entry is eligible for eviction the function should return true.
 *
 */
template<typename PreEvictionHookT, typename Entry>
concept pre_eviction_hook = requires(
  const PreEvictionHookT func, Entry& entry) {
    requires noexcept(func(entry));
    { func(entry) } -> std::same_as<bool>;
};

/**
 * Default eviction predicate that does not protect entry from being evicted
 */
struct always_allow_to_evict {
    bool operator()(const auto&) const noexcept { return true; };
};

struct noop_post_eviction_hook {
    void operator()(const auto&) const noexcept {};
};

/**
 * Thrown if cache if full i.e. no namespaces can be added to the cache
 */
class cache_full_error final : public std::exception {
public:
    explicit cache_full_error(ss::sstring msg) noexcept
      : _msg(std::move(msg)) {}

    const char* what() const noexcept override { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

/**
 * Internal per namespace cache implementation concept. User can provide
 * different implementation of caching algorithm f.e. LRU, FIFO or SIEVE.
 */
template<
  typename CacheT,
  typename EntryT,
  safe_intrusive_list_hook EntryT::*HookPtr,
  typename PreEvictionHookT,
  typename PostEvictionHookT>
concept namespace_cache = requires(
  CacheT& cache,
  EntryT& entry,
  const PreEvictionHookT& pre_eviction_hook,
  const PostEvictionHookT& post_eviction_hook) {
    { cache.insert(entry) } -> std::same_as<void>;
    {
        cache.evict(pre_eviction_hook, post_eviction_hook)
    } -> std::same_as<bool>;
    { cache.remove(entry) } -> std::same_as<void>;
    { cache.touch(entry) } -> std::same_as<void>;
    { cache.size() } -> std::same_as<size_t>;
};

/**
 * Simple cache with LRU eviction policy
 */
template<
  typename EntryT,
  safe_intrusive_list_hook EntryT::*HookPtr,
  pre_eviction_hook<EntryT> PreEvictionHookT,
  post_eviction_hook<EntryT> PostEvictionHookT>
struct lru_cache {
    using list_type = uncounted_intrusive_list<EntryT, HookPtr>;

    void insert(EntryT&);
    bool evict(const PreEvictionHookT&, const PostEvictionHookT&);
    void remove(const EntryT&);
    void touch(EntryT&);
    size_t size() const { return _size; }
    /**
     * Evicts from cache elements that are older then requested timestamp and
     * the pre_eviction_hook allows to do it.
     */
    template<typename ClockT>
    size_t evict_older_than(
      typename ClockT::time_point deadline,
      const PreEvictionHookT& pre_eviction_hook,
      const PostEvictionHookT& post_eviction_hook)
    requires entry_with_timestamp<EntryT>
    {
        struct comparator {
            bool operator()(
              const EntryT& entry, const ClockT::time_point& time_point) {
                return entry.get_last_update_timestamp() < time_point;
            }
            bool operator()(const EntryT& lhs, const EntryT& rhs) {
                return lhs.get_last_update_timestamp()
                       < rhs.get_last_update_timestamp();
            }
        };
        size_t evicted = 0;
        auto range_end = std::lower_bound(
          _lru.begin(), _lru.end(), deadline, comparator{});
        auto it = _lru.begin();
        while (it != range_end) {
            if (pre_eviction_hook(*it)) {
                auto& entry = *it;
                it = _lru.erase(it);
                --_size;
                post_eviction_hook(entry);
                evicted++;
            } else {
                ++it;
            }
        }
        return evicted;
    }

private:
    size_t _size{0};
    list_type _lru;
};

/**
 * A simple cache that maintains an LRU per namespace.
 *
 * The cache allows to maintain per namespace lru cache with fair distribution
 * of available cache slots across all namespaces. Cache size is
 * controlled by setting maximum number of entries. If adding new namespace to
 * the cache would result in other namespace to have less than
 * min_slots_per_namespace cache slots an exception is thrown.
 *
 * Maximum number of namespaces that cache can handle can be calculated as:
 *
 *  floor(_max_size/_min_slots_per_namespace)
 *
 * With cache PreEvictionHookT template parameter it is possible to prevent some
 * entries from being evicted. A predicate should return 'false' if an entry is
 * not evictable.
 *
 * Eviction policy can be controlled with the CacheT parameter. The CacheT
 * parameter allow to tune per namespace caching policy.
 *
 * For now the only provided eviction policy is a simple LRU. In future we may
 * experience with different caching strategies like f.e. SIEVE or S3-FIFO.
 *
 */
template<
  typename EntryT,
  typename NamespaceT,
  safe_intrusive_list_hook EntryT::*HookPtr,
  pre_eviction_hook<EntryT> PreEvictionHookT = always_allow_to_evict,
  post_eviction_hook<EntryT> PostEvictionHookT = noop_post_eviction_hook,
  namespace_cache<EntryT, HookPtr, PreEvictionHookT, PostEvictionHookT> CacheT
  = lru_cache<EntryT, HookPtr, PreEvictionHookT, PostEvictionHookT>>
class namespaced_cache {
private:
    using namespaces_t
      = absl::node_hash_map<NamespaceT, std::unique_ptr<CacheT>>;

public:
    struct stats {
        size_t total_size{0};
        absl::node_hash_map<NamespaceT, size_t> namespaces;
    };

    /**
     * Creates a namespaced cache with desired maximum size and minimal allowed
     * number of slots per namespace
     */
    explicit namespaced_cache(
      config::binding<size_t> total_max_size,
      config::binding<size_t> min_slots_per_namespace,
      const PreEvictionHookT& pre_eviction_hook = PreEvictionHookT(),
      const PostEvictionHookT& post_eviction_hook = PostEvictionHookT())
      : _max_size(std::move(total_max_size))
      , _min_slots_per_namespace(std::move(min_slots_per_namespace))
      , _pre_eviction_hook(pre_eviction_hook)
      , _post_eviction_hook(post_eviction_hook) {
        _max_size.watch([this] { handle_max_size_change(); });
    }
    /**
     * Inserts an entry into given namespace cache, if there are no slots
     * available this may result in an eviction.
     */
    void insert(const NamespaceT& ns, EntryT& entry);
    /**
     * Explicitly remove an entry from the cache.
     */
    void remove(const NamespaceT& ns, const EntryT& entry);

    /**
     * When touched entry is marked as being used, potentially preventing it
     * from being evicted.
     */
    void touch(const NamespaceT& ns, EntryT& entry);

    /**
     * Evicts next eligible element from the cache, returns true if element was
     * evicted.
     */
    bool evict() {
        // we use _namespaces.end() to indicate there is no hint what namespace
        // to evict from
        return evict(_namespaces.end());
    }

    /**
     * Return basic stats about cache size
     */
    stats get_stats() const;

    /**
     * Returns maximum number of namespaces a cache can handle
     */
    size_t namespace_capacity() const {
        return floor(_max_size() / effective_min_slots_per_namespace());
    }

    void clear() { _namespaces.clear(); }

    /**
     * Removes elements older than the provided deadline, this method is only
     * available if an entry meets the entry_with_timestamp constraint
     */
    template<typename ClockT>
    size_t evict_older_than(typename ClockT::time_point deadline)
    requires entry_with_timestamp<EntryT>
    {
        size_t evicted = 0;
        for (const auto& p : _namespaces) {
            evicted += p.second->template evict_older_than<ClockT>(
              deadline, _pre_eviction_hook, _post_eviction_hook);
        }

        if (evicted > 0) {
            absl::erase_if(_namespaces, [](const namespaces_t::value_type& p) {
                return p.second->size() == 0;
            });
        }
        _size -= evicted;
        return evicted;
    }

private:
    /**
     * An evict method with an iterator being a hint indicating a namespace that
     * the entry should be evicted from if all the namespaces has the same
     * number of entries in a cache.
     *
     * The hint is added to prevent situation in which the entry is evicted from
     * other namespace cache when all eviction eligible namespaces has the same
     * number of entries as the one the entry was inserted to. If this happens
     * the hint is used to evict entry from the same namespace that insert
     * caused the eviction event.
     */
    bool evict(namespaces_t::iterator);

    size_t effective_min_slots_per_namespace() const {
        return std::min(_max_size(), _min_slots_per_namespace());
    }

    void handle_max_size_change();

    namespaces_t _namespaces;

    size_t _size{0};
    config::binding<size_t> _max_size;
    config::binding<size_t> _min_slots_per_namespace;
    PreEvictionHookT _pre_eviction_hook;
    PostEvictionHookT _post_eviction_hook;
};

/**
 * namespaced_cache definitions
 */

template<
  typename EntryT,
  typename NamespaceT,
  safe_intrusive_list_hook EntryT::*HookPtr,
  pre_eviction_hook<EntryT> PreEvictionHookT,
  post_eviction_hook<EntryT> PostEvictionHookT,
  namespace_cache<EntryT, HookPtr, PreEvictionHookT, PostEvictionHookT> CacheT>
bool namespaced_cache<
  EntryT,
  NamespaceT,
  HookPtr,
  PreEvictionHookT,
  PostEvictionHookT,
  CacheT>::evict(namespaces_t::iterator hint) {
    /**
     * linear scan over all namespaces to find the one with the largest number
     * entries in an LRU
     */
    auto max_it = std::max_element(
      _namespaces.begin(),
      _namespaces.end(),
      [](const auto& lhs, const auto& rhs) {
          return lhs.second->size() < rhs.second->size();
      });

    /**
     * If hint is provided and the number of elements in a hint pointed
     * namespace cache is equal to max element size evict from the hint
     * namespace
     */
    if (
      hint != _namespaces.end()
      && max_it->second->size() == hint->second->size()) {
        max_it = hint;
    }

    std::unique_ptr<CacheT>& t_state = max_it->second;
    auto evicted = t_state->evict(_pre_eviction_hook, _post_eviction_hook);
    if (evicted) {
        --_size;
    }
    return evicted;
}

template<
  typename EntryT,
  typename NamespaceT,
  safe_intrusive_list_hook EntryT::*HookPtr,
  pre_eviction_hook<EntryT> PreEvictionHookT,
  post_eviction_hook<EntryT> PostEvictionHookT,
  namespace_cache<EntryT, HookPtr, PreEvictionHookT, PostEvictionHookT> CacheT>
void namespaced_cache<
  EntryT,
  NamespaceT,
  HookPtr,
  PreEvictionHookT,
  PostEvictionHookT,
  CacheT>::insert(const NamespaceT& ns, EntryT& entry) {
    if (
      _namespaces.size() >= namespace_capacity() && !_namespaces.contains(ns)) {
        throw cache_full_error(ssx::sformat(
          "maximum number of namespaces reached. Min number of entries per "
          "namespace: {}, max cache capacity: {}, current namespaces: {}",
          effective_min_slots_per_namespace(),
          _max_size(),
          _namespaces.size()));
    }
    auto [it, _] = _namespaces.try_emplace(ns, std::make_unique<CacheT>());

    /**
     * It is very unlikely that this loop will take more than few iterations to
     * find the eviction spot. Therefore we do not yield here.
     */
    size_t iteration = 0;
    while (_size >= _max_size() && iteration++ < _size) {
        evict(it);
    }
    /**
     * If there was no entry to evict from cache, throw cache full error
     */
    if (_size >= _max_size()) {
        throw cache_full_error(
          "unable to evict entry from cache, cache is at capacity");
    }

    it->second->insert(entry);

    ++_size;
}

template<
  typename EntryT,
  typename NamespaceT,
  safe_intrusive_list_hook EntryT::*HookPtr,
  pre_eviction_hook<EntryT> PreEvictionHookT,
  post_eviction_hook<EntryT> PostEvictionHookT,
  namespace_cache<EntryT, HookPtr, PreEvictionHookT, PostEvictionHookT> CacheT>
void namespaced_cache<
  EntryT,
  NamespaceT,
  HookPtr,
  PreEvictionHookT,
  PostEvictionHookT,
  CacheT>::remove(const NamespaceT& ns, const EntryT& entry) {
    auto it = _namespaces.find(ns);
    if (it == _namespaces.end()) {
        return;
    }
    if (entry._hook.is_linked()) {
        it->second->remove(entry);
        _size--;
    }
    if (it->second->size() == 0) {
        _namespaces.erase(it);
    }
}

template<
  typename EntryT,
  typename NamespaceT,
  safe_intrusive_list_hook EntryT::*HookPtr,
  pre_eviction_hook<EntryT> PreEvictionHookT,
  post_eviction_hook<EntryT> PostEvictionHookT,
  namespace_cache<EntryT, HookPtr, PreEvictionHookT, PostEvictionHookT> CacheT>
void namespaced_cache<
  EntryT,
  NamespaceT,
  HookPtr,
  PreEvictionHookT,
  PostEvictionHookT,
  CacheT>::touch(const NamespaceT& ns, EntryT& entry) {
    auto it = _namespaces.find(ns);
    if (it == _namespaces.end()) {
        return;
    }
    it->second->touch(entry);
}

template<
  typename EntryT,
  typename NamespaceT,
  safe_intrusive_list_hook EntryT::*HookPtr,
  pre_eviction_hook<EntryT> PreEvictionHookT,
  post_eviction_hook<EntryT> PostEvictionHookT,
  namespace_cache<EntryT, HookPtr, PreEvictionHookT, PostEvictionHookT> CacheT>
namespaced_cache<
  EntryT,
  NamespaceT,
  HookPtr,
  PreEvictionHookT,
  PostEvictionHookT,
  CacheT>::stats
namespaced_cache<
  EntryT,
  NamespaceT,
  HookPtr,
  PreEvictionHookT,
  PostEvictionHookT,
  CacheT>::get_stats() const {
    stats ret;
    for (auto& [k, state] : _namespaces) {
        ret.namespaces[k] = state->size();
    }
    ret.total_size = _size;
    return ret;
}

template<
  typename EntryT,
  typename NamespaceT,
  safe_intrusive_list_hook EntryT::*HookPtr,
  pre_eviction_hook<EntryT> PreEvictionHookT,
  post_eviction_hook<EntryT> PostEvictionHookT,
  namespace_cache<EntryT, HookPtr, PreEvictionHookT, PostEvictionHookT> CacheT>
void namespaced_cache<
  EntryT,
  NamespaceT,
  HookPtr,
  PreEvictionHookT,
  PostEvictionHookT,
  CacheT>::handle_max_size_change() {
    /**
     * Evict entries immediately after max size changed and it is smaller than
     * current size
     */
    size_t iteration = 0;
    while (_size > _max_size() && iteration++ < _size) {
        evict(_namespaces.end());
    }
}

/**
 * lru_cache definitions
 */

template<
  typename EntryT,
  safe_intrusive_list_hook EntryT::*HookPtr,
  pre_eviction_hook<EntryT> EvictionPredicate,
  post_eviction_hook<EntryT> PostEvictionHookT>
void lru_cache<EntryT, HookPtr, EvictionPredicate, PostEvictionHookT>::insert(
  EntryT& entry) {
    if constexpr (entry_with_timestamp<EntryT>) {
        entry.touch();
    }
    _lru.push_back(entry);
    ++_size;
}

template<
  typename EntryT,
  safe_intrusive_list_hook EntryT::*HookPtr,
  pre_eviction_hook<EntryT> EvictionPredicate,
  post_eviction_hook<EntryT> PostEvictionHookT>
bool lru_cache<EntryT, HookPtr, EvictionPredicate, PostEvictionHookT>::evict(
  const EvictionPredicate& predicate, const PostEvictionHookT& disposer) {
    for (auto it = _lru.begin(); it != _lru.end(); ++it) {
        if (predicate(*it)) {
            auto& entry = *it;
            _lru.erase(it);
            --_size;
            disposer(entry);
            return true;
        }
    }
    return false;
}

template<
  typename EntryT,
  safe_intrusive_list_hook EntryT::*HookPtr,
  pre_eviction_hook<EntryT> EvictionPredicate,
  post_eviction_hook<EntryT> PostEvictionHookT>
void lru_cache<EntryT, HookPtr, EvictionPredicate, PostEvictionHookT>::remove(
  const EntryT& entry) {
    _lru.erase(_lru.iterator_to(entry));
    --_size;
}

template<
  typename EntryT,
  safe_intrusive_list_hook EntryT::*HookPtr,
  pre_eviction_hook<EntryT> EvictionPredicate,
  post_eviction_hook<EntryT> PostEvictionHookT>
void lru_cache<EntryT, HookPtr, EvictionPredicate, PostEvictionHookT>::touch(
  EntryT& entry) {
    const auto& hook = entry.*HookPtr;
    if (!hook.is_linked()) {
        return;
    }
    _lru.erase(_lru.iterator_to(entry));
    _lru.push_back(entry);
    if constexpr (entry_with_timestamp<EntryT>) {
        entry.touch();
    }
}
} // namespace cluster
