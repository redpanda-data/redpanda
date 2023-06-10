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
#include "cloud_storage/partition_probe.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/future-util.h"
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

#include <exception>
#include <functional>
#include <iterator>
#include <regex>
#include <system_error>
#include <variant>

namespace cloud_storage {

static constexpr size_t max_cache_capacity_bytes = 1_GiB;

materialized_manifest_cache::materialized_manifest_cache(
  size_t capacity_bytes, retry_chain_logger& parent_logger)
  : _capacity_bytes(capacity_bytes)
  , _ctxlog(parent_logger)
  , _sem(max_cache_capacity_bytes) {
    vassert(
      capacity_bytes > 0 && capacity_bytes < max_cache_capacity_bytes,
      "Invalid cache capacity {}, should be non-zero and below 1GiB",
      capacity_bytes);
}

ss::future<ss::semaphore_units<>> materialized_manifest_cache::prepare(
  size_t size_bytes, std::optional<ss::lowres_clock::duration> timeout) {
    ss::gate::holder h(_gate);
    if (size_bytes > _capacity_bytes) {
        vlog(
          _ctxlog.debug,
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
          _ctxlog.debug,
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
    std::deque<model::offset> evicted;
    while (bytes_evicted < size_bytes && !_cache.empty()) {
        auto it = _access_order.begin();
        vassert(
          !it->manifest.empty(),
          "Manifest can't be empty, ntp: {}",
          it->manifest.get_ntp());
        auto so = it->manifest.get_start_offset().value_or(model::offset{});
        auto cit = _cache.find(so);
        vassert(cit != _cache.end(), "Manifest at {} already evicted", so);
        evicted.push_back(so);
        // Invariant: the materialized_manifest is always linked to either
        // _access_order or _eviction_rollback list.
        bytes_evicted += evict(cit, _eviction_rollback);
    }
    // Here the least recently used materialized manifests were evicted to
    // free up 'size_bytes' bytes. But these manifests could still be used
    // by some cursor. We need to wait for them to get released.
    ss::semaphore_units<> u;
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
          _ctxlog.debug,
          "Prepare operation timed out, restoring {} spillover "
          "manifest",
          evicted.size());
        for (auto eso : evicted) {
            rollback(eso);
        }
        throw;
    } catch (...) {
        // In case of any other error the elements from
        // '_eviction_rollback' list should be evicted for real
        // (filtered by 'eviction' set).
        vlog(
          _ctxlog.error,
          "'{}' error detected, cleaning up eviction list",
          std::current_exception());
        for (auto eso : evicted) {
            discard_rollback_manifest(eso);
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
  ss::semaphore_units<> s, spillover_manifest manifest) {
    vassert(
      !manifest.empty(),
      "Manifest can't be empty, ntp: {}",
      manifest.get_ntp());
    auto so = manifest.get_start_offset().value_or(model::offset{});
    vlog(_ctxlog.debug, "Cache PUT offset {}, {} units", so, s.count());
    if (!_eviction_rollback.empty()) {
        auto it = lookup_eviction_rollback_list(so);
        if (it != _eviction_rollback.end()) {
            vlog(
              _ctxlog.error,
              "Manifest with base offset {} is being evicted from the "
              "cache",
              so);
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Manifest with start offset {} is being evicted from the "
              "cache",
              so));
        }
    }
    auto item = ss::make_shared<materialized_manifest>(
      std::move(manifest), std::move(s));
    auto [it, ok] = _cache.insert(std::make_pair(so, std::move(item)));
    if (!ok) {
        // This may indicate a race, log a warning
        vlog(
          _ctxlog.error, "Manifest with base offset {} is already present", so);
        return;
    }
    _access_order.push_back(*it->second);
}

ss::shared_ptr<materialized_manifest>
materialized_manifest_cache::get(model::offset base_offset) {
    if (auto it = _cache.find(base_offset); it != _cache.end()) {
        if (promote(it->second)) {
            vlog(_ctxlog.debug, "Cache GET will return {}", base_offset);
            return it->second;
        } else {
            vlog(
              _ctxlog.debug,
              "Cache GET can't promote item {} because it's evicted",
              base_offset);
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
        auto it = lookup_eviction_rollback_list(base_offset);
        if (it != _eviction_rollback.end()) {
            vlog(
              _ctxlog.debug,
              "Cache GET will return {} from eviction rollback",
              base_offset);
            return it->shared_from_this();
        }
    }
    vlog(
      _ctxlog.debug,
      "Cache GET will return NULL for offset {}, cache size: {}, rollback "
      "size: {}",
      base_offset,
      _cache.size(),
      _eviction_rollback.size());
    return nullptr;
}

bool materialized_manifest_cache::contains(model::offset base_offset) {
    return _cache.contains(base_offset);
}

bool materialized_manifest_cache::promote(model::offset base) {
    if (auto it = _cache.find(base); it != _cache.end()) {
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

size_t materialized_manifest_cache::remove(model::offset base) {
    access_list_t rollback;
    size_t evicted_bytes = 0;
    if (auto it = _cache.find(base); it != _cache.end()) {
        evicted_bytes = evict(it, rollback);
    }
    for (auto& m : rollback) {
        vlog(
          _ctxlog.debug,
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
          _ctxlog.info,
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
          _ctxlog.debug,
          "Shrinking materialized manifest cache capacity from {} to {}",
          _capacity_bytes,
          new_size);
        auto u = co_await prepare(delta, timeout);
        _reserved.adopt(std::move(u));
    } else {
        vlog(
          _ctxlog.debug,
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
  map_t::iterator it, access_list_t& rollback) {
    vlog(
      _ctxlog.debug,
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
materialized_manifest_cache::lookup_eviction_rollback_list(model::offset o) {
    return std::find_if(
      _eviction_rollback.begin(),
      _eviction_rollback.end(),
      [o](const materialized_manifest& m) {
          return m.manifest.get_start_offset() == o;
      });
}

void materialized_manifest_cache::rollback(model::offset so) {
    auto it = lookup_eviction_rollback_list(so);
    if (it == _eviction_rollback.end()) {
        vlog(
          _ctxlog.debug,
          "Can't rollback eviction of the manifest with start offset {}",
          so);
        return;
    }
    auto ptr = it->shared_from_this();
    ptr->_hook.unlink();
    auto [_, ok] = _cache.insert(std::make_pair(so, ptr));
    if (!ok) {
        vlog(
          _ctxlog.error,
          "Manifest with base offset {} has a duplicate in the log",
          so);
        return;
    }
    ptr->evicted = false;
    _access_order.push_front(*ptr);
    vlog(
      _ctxlog.debug,
      "Successful rollback of the manifest with start offset {}",
      so);
}

void materialized_manifest_cache::discard_rollback_manifest(model::offset so) {
    auto it = lookup_eviction_rollback_list(so);
    if (it == _eviction_rollback.end()) {
        vlog(
          _ctxlog.error,
          "Can't find manifest with start offset {} in the rollback list",
          so);
    }
    auto ptr = it->shared_from_this();
    ptr->_hook.unlink();
    vlog(
      _ctxlog.debug,
      "Manifest with start offset {} removed from rollback list",
      so);
}

} // namespace cloud_storage
