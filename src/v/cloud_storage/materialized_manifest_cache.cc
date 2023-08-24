/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/materialized_manifest_cache.h"

#include "cloud_storage/cache_service.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/read_path_probes.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/future-util.h"
#include "ssx/semaphore.h"
#include "ssx/sformat.h"
#include "utils/human.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/defer.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/outcome/success_failure.hpp>
#include <fmt/format.h>

#include <exception>
#include <functional>
#include <iterator>
#include <regex>
#include <system_error>
#include <variant>

template<>
struct fmt::formatter<cloud_storage::manifest_cache_key>
  : public fmt::formatter<std::string_view> {
    auto format(const cloud_storage::manifest_cache_key& key, auto& ctx) const {
        const auto& [ntp, o] = key;
        return formatter<std::string_view>::format(
          fmt::format("[{}:{}]", ntp, o), ctx);
    }
};

namespace cloud_storage {

static constexpr size_t max_cache_capacity_bytes = 1_GiB;

materialized_manifest_cache::materialized_manifest_cache(size_t capacity_bytes)
  : _capacity_bytes(capacity_bytes)
  , _sem(max_cache_capacity_bytes, "materialized-manifest-cache") {
    vassert(
      capacity_bytes > 0 && capacity_bytes < max_cache_capacity_bytes,
      "Invalid cache capacity {}, should be non-zero and below 1GiB",
      capacity_bytes);
}

ss::future<ssx::semaphore_units> materialized_manifest_cache::prepare(
  size_t size_bytes,
  retry_chain_logger& ctxlog,
  std::optional<ss::lowres_clock::duration> timeout) {
    ss::gate::holder h(_gate);
    if (size_bytes > _capacity_bytes) {
        vlog(
          ctxlog.debug,
          "Oversized 'put' operation requested. Manifest size is {} bytes, "
          "capacity is {} bytes",
          size_bytes,
          _capacity_bytes);
        // Oversized manifest handling. The manifest could be larger than
        // capacity. If we will not allow this manifest to be added to the
        // cache the subsystem will stall. The only possible solution is to
        // let the manifest into the cache and allow it to evict everything
        // else.
        size_bytes = _capacity_bytes;
    }
    auto maybe_units = ss::try_get_units(_sem, size_bytes);
    if (maybe_units.has_value()) {
        vlog(
          ctxlog.debug,
          "{} units acquired without waiting, {} available",
          size_bytes,
          _sem.available_units());
        // The cache is not full and can grant some capacity without
        // eviction
        co_return std::move(maybe_units.value());
    }
    // The cache is full, try to free up some space. Free at least
    // 'size_bytes' bytes.
    size_t bytes_evicted = 0;
    std::deque<manifest_cache_key> evicted;
    while (bytes_evicted < size_bytes && !_cache.empty()) {
        auto it = _access_order.begin();
        vassert(
          !it->manifest.empty(),
          "Manifest can't be empty, ntp: {}",
          it->manifest.get_ntp());
        auto key = it->get_key();
        auto cit = _cache.find(key);
        vassert(cit != _cache.end(), "Manifest at {} already evicted", key);
        evicted.push_back(key);
        // Invariant: the materialized_manifest is always linked to either
        // _access_order or _eviction_rollback list.
        bytes_evicted += evict(cit, _eviction_rollback, ctxlog);
    }
    // Here the least recently used materialized manifests were evicted to
    // free up 'size_bytes' bytes. But these manifests could still be used
    // by some cursor. We need to wait for them to get released.
    ssx::semaphore_units u;
    try {
        if (timeout.has_value()) {
            u = co_await ss::get_units(_sem, size_bytes, timeout.value());
        } else {
            u = co_await ss::get_units(_sem, size_bytes);
        }
    } catch (const ss::timed_out_error& e) {
        // Operation timed out and we need to return elements stored in
        // the '_eviction_rollback' list back into '_cache'. Only
        // offsets from 'evicted' should be affected.
        vlog(
          ctxlog.debug,
          "Prepare operation timed out, restoring {} spillover "
          "manifest",
          evicted.size());
        for (auto eso : evicted) {
            rollback(eso, ctxlog);
        }
        throw;
    } catch (...) {
        // In case of any other error the elements from
        // '_eviction_rollback' list should be evicted for real
        // (filtered by 'eviction' set).
        vlog(
          ctxlog.error,
          "'{}' error detected, cleaning up eviction list",
          std::current_exception());
        for (auto eso : evicted) {
            discard_rollback_manifest(eso, ctxlog);
        }
        throw;
    }
    co_return u;
}

size_t materialized_manifest_cache::size() const noexcept {
    return _access_order.size() + _eviction_rollback.size();
}

size_t materialized_manifest_cache::size_bytes() const noexcept {
    size_t res = 0;
    for (const auto& m : _access_order) {
        res += m._units.count();
    }
    for (const auto& m : _eviction_rollback) {
        res += m._units.count();
    }
    return res;
}

void materialized_manifest_cache::put(
  ssx::semaphore_units s,
  spillover_manifest manifest,
  retry_chain_logger& ctxlog) {
    vassert(
      !manifest.empty(),
      "Manifest can't be empty, ntp: {}",
      manifest.get_ntp());
    const model::ntp& ntp = manifest.get_ntp();
    auto key = manifest_cache_key(
      ntp, manifest.get_start_offset().value_or(model::offset{}));
    vlog(ctxlog.debug, "Cache PUT key {}, {} units", key, s.count());
    if (!_eviction_rollback.empty()) {
        auto it = lookup_eviction_rollback_list(key);
        if (it != _eviction_rollback.end()) {
            vlog(
              ctxlog.error,
              "Manifest with key {} is being evicted from the "
              "cache",
              key);
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Manifest with key {} is being evicted from the "
              "cache",
              key));
        }
    }
    auto item = ss::make_shared<materialized_manifest>(
      std::move(manifest), std::move(s));
    auto [it, ok] = _cache.insert(std::make_pair(key, std::move(item)));
    if (!ok) {
        // This may indicate a race, log a warning
        vlog(ctxlog.error, "Manifest with key {} is already present", key);
        return;
    }
    _access_order.push_back(*it->second);
}

ss::shared_ptr<materialized_manifest> materialized_manifest_cache::get(
  const manifest_cache_key& key, retry_chain_logger& ctxlog) {
    if (auto it = _cache.find(key); it != _cache.end()) {
        if (promote(it->second)) {
            vlog(ctxlog.debug, "Cache GET will return {}", key);
            return it->second;
        } else {
            vlog(
              ctxlog.debug,
              "Cache GET can't promote item {} because it's evicted",
              key);
        }
    }
    if (!_eviction_rollback.empty()) {
        // Another fiber is waiting for the eviction of some elements.
        // These elements could be stored in the '_eviction_rollback' list
        // until there exist a copy of the shared pointer somewhere. We need
        // to search through the list and return matching manifest if
        // possible. Otherwise, the fiber may re-create the manifest and the
        // other fiber may restore evicted manifest (if the wait on a
        // semaphore will timeout) which will result in conflict.
        auto it = lookup_eviction_rollback_list(key);
        if (it != _eviction_rollback.end()) {
            vlog(
              ctxlog.debug,
              "Cache GET will return {} from eviction rollback",
              key);
            return it->shared_from_this();
        }
    }
    vlog(
      ctxlog.debug,
      "Cache GET will return NULL for offset {}, cache size: {}, rollback "
      "size: {}",
      key,
      _cache.size(),
      _eviction_rollback.size());
    return nullptr;
}

bool materialized_manifest_cache::contains(const manifest_cache_key& key) {
    return _cache.contains(key);
}

bool materialized_manifest_cache::promote(const manifest_cache_key& key) {
    if (auto it = _cache.find(key); it != _cache.end()) {
        return promote(it->second);
    }
    return false;
}

bool materialized_manifest_cache::promote(
  ss::shared_ptr<materialized_manifest>& manifest) {
    if (!manifest->evicted) {
        manifest->_hook.unlink();
        _access_order.push_back(*manifest);
        return true;
    }
    return false;
}

size_t materialized_manifest_cache::remove(
  const manifest_cache_key& key, retry_chain_logger& ctxlog) {
    access_list_t rollback;
    size_t evicted_bytes = 0;
    if (auto it = _cache.find(key); it != _cache.end()) {
        evicted_bytes = evict(it, rollback, ctxlog);
    }
    for (auto& m : rollback) {
        vlog(
          ctxlog.debug,
          "Offloaded spillover manifest with offset {} from memory",
          m.manifest.get_start_offset());
        m._units.return_all();
    }
    return evicted_bytes;
}

ss::future<> materialized_manifest_cache::start() {
    auto num_reserved = max_cache_capacity_bytes - _capacity_bytes;
    if (ss::this_shard_id() == 0) {
        vlog(
          cst_log.info,
          "Starting materialized manifest cache, capacity: {}, reserved: {}",
          human::bytes(_capacity_bytes),
          human::bytes(num_reserved));
    }
    // Should be ready immediately since all units are available
    // before the cache is started.
    _reserved = co_await ss::get_units(_sem, num_reserved);
}

ss::future<> materialized_manifest_cache::stop() {
    _sem.broken();
    return _gate.close();
}

ss::future<> materialized_manifest_cache::set_capacity(
  size_t new_size, std::optional<ss::lowres_clock::duration> timeout) {
    if (new_size == 0 || new_size > max_cache_capacity_bytes) {
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "Invalid capacity value {}", new_size));
    }
    if (new_size == _capacity_bytes) {
        co_return;
    } else if (new_size < _capacity_bytes) {
        // Cache shrinks, we need to evict some elements from the cache
        // if there is not enough space to shrink. We need to acquire
        // the units and add them to reserved semaphore units.
        auto delta = _capacity_bytes - new_size;
        vlog(
          cst_log.debug,
          "Shrinking materialized manifest cache capacity from {} to {}",
          _capacity_bytes,
          new_size);
        ss::abort_source as;
        retry_chain_node tmp_node(as);
        retry_chain_logger rtc_log(cst_log, tmp_node);
        auto u = co_await prepare(delta, rtc_log, timeout);
        _reserved.adopt(std::move(u));
    } else {
        vlog(
          cst_log.debug,
          "Increasing materialized manifest cache capacity from {} to {}",
          _capacity_bytes,
          new_size);
        // Cache grows so we need to release some reserved units.
        auto u = _reserved.split(new_size - _capacity_bytes);
        u.return_all();
    }
    _capacity_bytes = new_size;
    co_return;
}

size_t materialized_manifest_cache::evict(
  map_t::iterator it, access_list_t& rollback, retry_chain_logger& ctxlog) {
    vlog(
      ctxlog.debug,
      "Requested to evict manifest with start offset: {}, use count: {}, "
      "units: {}",
      it->first,
      it->second.use_count(),
      it->second->_units.count());
    auto sz = it->second->_units.count();
    it->second->_hook.unlink();
    it->second->evicted = true;
    rollback.push_back(*it->second);
    _cache.erase(it);
    return sz;
}

materialized_manifest_cache::access_list_t::iterator
materialized_manifest_cache::lookup_eviction_rollback_list(
  const manifest_cache_key& key) {
    return std::find_if(
      _eviction_rollback.begin(),
      _eviction_rollback.end(),
      [key](const materialized_manifest& m) { return m.get_key() == key; });
}

void materialized_manifest_cache::rollback(
  const manifest_cache_key& key, retry_chain_logger& ctxlog) {
    auto it = lookup_eviction_rollback_list(key);
    if (it == _eviction_rollback.end()) {
        vlog(
          ctxlog.debug,
          "Can't rollback eviction of the manifest with key {}",
          key);
        return;
    }
    auto ptr = it->shared_from_this();
    ptr->_hook.unlink();
    auto [_, ok] = _cache.insert(std::make_pair(key, ptr));
    if (!ok) {
        vlog(
          ctxlog.error, "Manifest with key {} has a duplicate in the log", key);
        return;
    }
    ptr->evicted = false;
    _access_order.push_front(*ptr);
    vlog(ctxlog.debug, "Successful rollback of the manifest with key {}", key);
}

void materialized_manifest_cache::discard_rollback_manifest(
  const manifest_cache_key& key, retry_chain_logger& ctxlog) {
    auto it = lookup_eviction_rollback_list(key);
    if (it == _eviction_rollback.end()) {
        vlog(
          ctxlog.error,
          "Can't find manifest with {} in the rollback list",
          key);
    }
    auto ptr = it->shared_from_this();
    ptr->_hook.unlink();
    vlog(ctxlog.debug, "Manifest with key {} removed from rollback list", key);
}

} // namespace cloud_storage
