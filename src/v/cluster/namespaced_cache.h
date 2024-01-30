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

#include <absl/container/node_hash_map.h>
#include <boost/intrusive/list_hook.hpp>

#include <exception>

namespace cluster {

/**
 * Disposer is called after an entry has been evicted from the cache.
 *
 * The cache_disposer callable may be used to cleanup some additional state
 * depending on application. f.e. it may release resources.
 */
template<typename PostEvictionHookT, typename Entry>
concept cache_disposer = requires(const PostEvictionHookT& hook, Entry& entry) {
    requires noexcept(hook(entry));
    { hook(entry) } -> std::same_as<void>;
};
/**
 * Simillar to `experimental::io::cache_evictor` it is a callable that is called
 * when an entry is evicted from the cache.
 *
 * If an entry is eligible for eviction the function should return true.
 *
 */
template<typename Predicate, typename Entry>
concept cache_evictor = requires(const Predicate func, Entry& entry) {
    requires noexcept(func(entry));
    { func(entry) } -> std::same_as<bool>;
};

/**
 * Default eviction predicate that does not protect entry from being evicted
 */
struct default_cache_evictor {
    bool operator()(const auto&) const noexcept { return true; };
};

struct noop_disposer {
    void operator()(const auto&) const noexcept {};
};

/**
 * Member type hook to include into an entry to make it possible to use it with
 * intrusive list
 */
using namespaced_cache_hook = boost::intrusive::list_member_hook<
  boost::intrusive::link_mode<boost::intrusive::safe_link>>;

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
  namespaced_cache_hook EntryT::*HookPtr,
  typename EvictionPredicate,
  typename Disposer>
concept namespace_cache = requires(
  CacheT& cache,
  EntryT& entry,
  const EvictionPredicate& predicate,
  const Disposer& disposer) {
    { cache.insert(entry) } -> std::same_as<void>;
    { cache.evict(predicate, disposer) } -> std::same_as<bool>;
    { cache.remove(entry) } -> std::same_as<void>;
    { cache.touch(entry) } -> std::same_as<void>;
    { cache.size() } -> std::same_as<size_t>;
};

/**
 * Simple cache with LRU eviction policy
 */
template<
  typename EntryT,
  namespaced_cache_hook EntryT::*HookPtr,
  cache_evictor<EntryT> EvictionPredicate,
  cache_disposer<EntryT> Disposer>
struct lru_cache {
    using list_type = boost::intrusive::list<
      EntryT,
      boost::intrusive::member_hook<EntryT, namespaced_cache_hook, HookPtr>,
      boost::intrusive::constant_time_size<false>>;

    void insert(EntryT&);
    bool evict(const EvictionPredicate&, const Disposer&);
    void remove(const EntryT&);
    void touch(EntryT&);
    size_t size() const { return _size; }

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
 * With cache CacheEvictorT template parameter it is possible to prevent some
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
  namespaced_cache_hook EntryT::*HookPtr,
  cache_evictor<EntryT> CacheEvictorT = default_cache_evictor,
  cache_disposer<EntryT> DisposerT = noop_disposer,
  namespace_cache<EntryT, HookPtr, CacheEvictorT, DisposerT> CacheT
  = lru_cache<EntryT, HookPtr, CacheEvictorT, DisposerT>>
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
      const CacheEvictorT& evictor = CacheEvictorT(),
      const DisposerT& disposer = DisposerT())
      : _max_size(std::move(total_max_size))
      , _min_slots_per_namespace(std::move(min_slots_per_namespace))
      , _evictor(evictor)
      , _disposer(disposer) {}

    void insert(const NamespaceT& ns, EntryT& entry);
    void remove(const NamespaceT& ns, const EntryT& entry);
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

private:
    bool evict(namespaces_t::iterator);

    size_t effective_min_slots_per_namespace() const {
        return std::min(_max_size(), _min_slots_per_namespace());
    }

    namespaces_t _namespaces;

    size_t _size{0};
    config::binding<size_t> _max_size;
    config::binding<size_t> _min_slots_per_namespace;
    CacheEvictorT _evictor;
    DisposerT _disposer;
};

template<
  typename EntryT,
  typename NamespaceT,
  namespaced_cache_hook EntryT::*HookPtr,
  cache_evictor<EntryT> CacheEvictorT,
  cache_disposer<EntryT> DisposerT,
  namespace_cache<EntryT, HookPtr, CacheEvictorT, DisposerT> CacheT>
bool namespaced_cache<
  EntryT,
  NamespaceT,
  HookPtr,
  CacheEvictorT,
  DisposerT,
  CacheT>::evict(namespaces_t::iterator hint) {
    /**
     * linear scan over all namespaces to find the one with the largest number
     * of producers
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
    auto evicted = t_state->evict(_evictor, _disposer);
    if (evicted) {
        --_size;
    }
    return evicted;
}

/**
 * namespaced_cache definitions
 */

template<
  typename EntryT,
  typename NamespaceT,
  namespaced_cache_hook EntryT::*HookPtr,
  cache_evictor<EntryT> CacheEvictorT,
  cache_disposer<EntryT> DisposerT,
  namespace_cache<EntryT, HookPtr, CacheEvictorT, DisposerT> CacheT>
void namespaced_cache<
  EntryT,
  NamespaceT,
  HookPtr,
  CacheEvictorT,
  DisposerT,
  CacheT>::insert(const NamespaceT& ns, EntryT& entry) {
    if (
      _namespaces.size() >= namespace_capacity() && !_namespaces.contains(ns)) {
        throw cache_full_error(ssx::sformat(
          "maximum number of tenants reached. Min number of entries per "
          "tenant: {}, max cache capacity: {}, current tenants: {}",
          effective_min_slots_per_namespace(),
          _max_size(),
          _namespaces.size()));
    }
    auto [it, _] = _namespaces.try_emplace(ns, std::make_unique<CacheT>());
    while (_size >= _max_size()) {
        evict(it);
    }

    it->second->insert(entry);

    ++_size;
}

template<
  typename EntryT,
  typename NamespaceT,
  namespaced_cache_hook EntryT::*HookPtr,
  cache_evictor<EntryT> CacheEvictorT,
  cache_disposer<EntryT> DisposerT,
  namespace_cache<EntryT, HookPtr, CacheEvictorT, DisposerT> CacheT>
void namespaced_cache<
  EntryT,
  NamespaceT,
  HookPtr,
  CacheEvictorT,
  DisposerT,
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
  namespaced_cache_hook EntryT::*HookPtr,
  cache_evictor<EntryT> CacheEvictorT,
  cache_disposer<EntryT> DisposerT,
  namespace_cache<EntryT, HookPtr, CacheEvictorT, DisposerT> CacheT>
void namespaced_cache<
  EntryT,
  NamespaceT,
  HookPtr,
  CacheEvictorT,
  DisposerT,
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
  namespaced_cache_hook EntryT::*HookPtr,
  cache_evictor<EntryT> CacheEvictorT,
  cache_disposer<EntryT> DisposerT,
  namespace_cache<EntryT, HookPtr, CacheEvictorT, DisposerT> CacheT>
namespaced_cache<
  EntryT,
  NamespaceT,
  HookPtr,
  CacheEvictorT,
  DisposerT,
  CacheT>::stats
namespaced_cache<
  EntryT,
  NamespaceT,
  HookPtr,
  CacheEvictorT,
  DisposerT,
  CacheT>::get_stats() const {
    stats ret;
    for (auto& [k, state] : _namespaces) {
        ret.namespaces[k] = state->size();
    }
    ret.total_size = _size;
    return ret;
}

/**
 * lru_cache definitions
 */

template<
  typename EntryT,
  namespaced_cache_hook EntryT::*HookPtr,
  cache_evictor<EntryT> EvictionPredicate,
  cache_disposer<EntryT> DisposerT>
void lru_cache<EntryT, HookPtr, EvictionPredicate, DisposerT>::insert(
  EntryT& entry) {
    _lru.push_back(entry);
    ++_size;
}

template<
  typename EntryT,
  namespaced_cache_hook EntryT::*HookPtr,
  cache_evictor<EntryT> EvictionPredicate,
  cache_disposer<EntryT> DisposerT>
bool lru_cache<EntryT, HookPtr, EvictionPredicate, DisposerT>::evict(
  const EvictionPredicate& predicate, const DisposerT& disposer) {
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
  namespaced_cache_hook EntryT::*HookPtr,
  cache_evictor<EntryT> EvictionPredicate,
  cache_disposer<EntryT> DisposerT>
void lru_cache<EntryT, HookPtr, EvictionPredicate, DisposerT>::remove(
  const EntryT& entry) {
    _lru.erase(_lru.iterator_to(entry));
    --_size;
}

template<
  typename EntryT,
  namespaced_cache_hook EntryT::*HookPtr,
  cache_evictor<EntryT> EvictionPredicate,
  cache_disposer<EntryT> DisposerT>
void lru_cache<EntryT, HookPtr, EvictionPredicate, DisposerT>::touch(
  EntryT& entry) {
    const auto& hook = entry.*HookPtr;
    if (!hook.is_linked()) {
        return;
    }
    _lru.erase(_lru.iterator_to(entry));
    _lru.push_back(entry);
}
} // namespace cluster
