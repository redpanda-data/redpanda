/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/async_manifest_view.h"

#include "cloud_storage/cache_service.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/partition_manifest.h"
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

static ss::sstring to_string(const async_view_search_query_t& t) {
    return ss::visit(
      t,
      [&](model::offset ro) { return ssx::sformat("[offset: {}]", ro); },
      [&](kafka::offset ko) { return ssx::sformat("[kafka offset: {}]", ko); },
      [&](model::timestamp ts) { return ssx::sformat("[timestamp: {}]", ts); });
}

std::ostream& operator<<(std::ostream& s, const async_view_search_query_t& q) {
    s << to_string(q);
    return s;
}

static bool
contains(const partition_manifest& m, const async_view_search_query_t& query) {
    return ss::visit(
      query,
      [&](model::offset o) {
          return o >= m.get_start_offset().value_or(model::offset::max())
                 && o <= m.get_last_offset();
      },
      [&](kafka::offset k) {
          return k >= m.get_start_kafka_offset()
                 && k < m.get_next_kafka_offset();
      },
      [&](model::timestamp t) {
          return m.size() > 0 && t >= m.begin()->base_timestamp
                 && t <= m.last_segment()->max_timestamp;
      });
}

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

std::ostream& operator<<(std::ostream& o, async_manifest_view_cursor_status s) {
    switch (s) {
    case async_manifest_view_cursor_status::empty:
        fmt::print(o, "empty");
        break;
    case async_manifest_view_cursor_status::evicted:
        fmt::print(o, "evicted");
        break;
    case async_manifest_view_cursor_status::materialized_stm:
        fmt::print(o, "materialized_stm");
        break;
    case async_manifest_view_cursor_status::materialized_spillover:
        fmt::print(o, "materialized_spillover");
        break;
    }
    return o;
}

async_manifest_view_cursor::async_manifest_view_cursor(
  async_manifest_view& view,
  model::offset begin,
  model::offset end_inclusive,
  ss::lowres_clock::duration timeout)
  : _view(view)
  , _current(std::monostate())
  , _idle_timeout(timeout)
  , _begin(begin)
  , _end(end_inclusive) {
    _timer.set_callback([this] { on_timeout(); });
}

async_manifest_view_cursor_status
async_manifest_view_cursor::get_status() const {
    return ss::visit(
      _current,
      [](std::monostate) { return async_manifest_view_cursor_status::empty; },
      [](stale_manifest) { return async_manifest_view_cursor_status::evicted; },
      [](std::reference_wrapper<const partition_manifest>) {
          return async_manifest_view_cursor_status::materialized_stm;
      },
      [](const ss::shared_ptr<materialized_manifest>&) {
          return async_manifest_view_cursor_status::materialized_spillover;
      });
}

ss::future<result<bool, error_outcome>>
async_manifest_view_cursor::seek(async_view_search_query_t q) {
    if (std::holds_alternative<model::offset>(q)) {
        auto o = std::get<model::offset>(q);
        if (_begin > o || o > _end) {
            vlog(
              _view._ctxlog.debug,
              "Offset {} out of [{}-{}] range",
              o,
              _begin,
              _end);
            co_return false;
        }
    }
    auto satisfies_query = ss::visit(
      _current,
      [this](std::monostate) {
          vlog(_view._ctxlog.debug, "Manifest is not initialized");
          return false;
      },
      [this](stale_manifest) {
          vlog(_view._ctxlog.debug, "Manifest is stale");
          return false;
      },
      [this, q](std::reference_wrapper<const partition_manifest> p) {
          vlog(
            _view._ctxlog.debug,
            "Seeking STM manifest [{}-{}]",
            p.get().get_start_offset().value(),
            p.get().get_last_offset());
          return contains(p, q);
      },
      [this, q](const ss::shared_ptr<materialized_manifest>& m) {
          vlog(
            _view._ctxlog.debug,
            "Seeking spillover manifest [{}-{}]",
            m->manifest.get_start_offset().value(),
            m->manifest.get_last_offset());
          return contains(m->manifest, q);
      });
    if (satisfies_query) {
        // The seek is to the same manifest so no need to go through the churns
        // of hydrating/materializing/fetching the manifest
        co_return true;
    }
    auto res = co_await _view.get_materialized_manifest(q);
    if (res.has_failure()) {
        vlog(
          _view._ctxlog.error,
          "Failed to seek async_manifest_view_cursor: {}",
          res.error());
        co_return res.as_failure();
    }
    // Check that the manifest fits inside the offset range
    // limit. The check has to be performed after the scheduling
    // point for the list of manifest to be up to date.
    if (unlikely(!manifest_in_range(res.value()))) {
        vlog(
          _view._ctxlog.debug,
          "Manifest is not in the specified range, range: [{}/{}]",
          _begin,
          _end);
        co_return false;
    }
    _current = res.value();
    _timer.rearm(_idle_timeout + ss::lowres_clock::now());
    co_return true;
}

bool async_manifest_view_cursor::manifest_in_range(
  const manifest_section_t& m) {
    return ss::visit(
      m,
      [](std::monostate) { return false; },
      [](stale_manifest) { return false; },
      [this](std::reference_wrapper<const partition_manifest> p) {
          auto so = p.get().get_start_offset().value_or(model::offset{});
          auto lo = p.get().get_last_offset();
          return !(_end < so || _begin > lo);
      },
      [this](const ss::shared_ptr<materialized_manifest>& m) {
          auto so = m->manifest.get_start_offset().value_or(model::offset{});
          auto lo = m->manifest.get_last_offset();
          return !(_end < so || _begin > lo);
      });
}

ss::future<result<bool, error_outcome>> async_manifest_view_cursor::next() {
    static constexpr auto EOS = model::offset{};
    auto next_base_offset = ss::visit(
      _current,
      [](std::monostate) { return EOS; },
      [](stale_manifest sm) { return sm.next_offset; },
      [](std::reference_wrapper<const partition_manifest>) { return EOS; },
      [](const ss::shared_ptr<materialized_manifest>& m) {
          return model::next_offset(m->manifest.get_last_offset());
      });

    if (next_base_offset == EOS || next_base_offset > _end) {
        co_return false;
    }
    auto manifest = co_await _view.get_materialized_manifest(next_base_offset);
    if (manifest.has_failure()) {
        co_return manifest.as_failure();
    }
    if (unlikely(!manifest_in_range(manifest.value()))) {
        co_return false;
    }
    _current = manifest.value();
    _timer.rearm(_idle_timeout + ss::lowres_clock::now());
    co_return true;
}

ss::future<ss::stop_iteration> async_manifest_view_cursor::next_iter() {
    auto res = co_await next();
    if (res.has_failure()) {
        throw std::system_error(res.error());
    }
    co_return res.value() == true ? ss::stop_iteration::yes
                                  : ss::stop_iteration::no;
}

std::optional<std::reference_wrapper<const partition_manifest>>
async_manifest_view_cursor::manifest() const {
    using ret_t
      = std::optional<std::reference_wrapper<const partition_manifest>>;
    return ss::visit(
      _current,
      [](std::monostate) -> ret_t { return std::nullopt; },
      [this](stale_manifest) -> ret_t {
          auto errc = make_error_code(error_outcome::timed_out);
          throw std::system_error(
            errc,
            fmt_with_ctx(
              fmt::format,
              "{} manifest was evicted from the cache",
              _view.get_ntp()));
      },
      [](std::reference_wrapper<const partition_manifest> m) -> ret_t {
          return m;
      },
      [](const ss::shared_ptr<materialized_manifest>& m) -> ret_t {
          return std::ref(m->manifest);
      });
}

void async_manifest_view_cursor::on_timeout() {
    auto next = ss::visit(
      _current,
      [](std::monostate) { return model::offset{}; },
      [](stale_manifest sm) { return sm.next_offset; },
      [](std::reference_wrapper<const partition_manifest>) {
          return model::offset{};
      },
      [this](const ss::shared_ptr<materialized_manifest>& m) {
          if (m->evicted) {
              vlog(
                _view._ctxlog.debug,
                "Spillover manifest {} is being evicted, last offset: {}",
                m->manifest.get_manifest_path(),
                m->manifest.get_last_offset());
              return model::next_offset(m->manifest.get_last_offset());
          } else {
              vlog(
                _view._ctxlog.debug,
                "Spillover manifest {} is not evicted, rearming",
                m->manifest.get_manifest_path());
              return model::offset{};
          }
      });
    if (next != model::offset{}) {
        _current = stale_manifest{.next_offset = next};
    } else {
        _timer.arm(_idle_timeout);
    }
}

async_manifest_view::async_manifest_view(
  ss::sharded<remote>& remote,
  ss::sharded<cache>& cache,
  const partition_manifest& stm_manifest,
  cloud_storage_clients::bucket_name bucket,
  partition_probe& probe)
  : _bucket(bucket)
  , _remote(remote)
  , _cache(cache)
  , _probe(probe)
  , _stm_manifest(stm_manifest)
  , _rtcnode(_as)
  , _ctxlog(cst_log, _rtcnode, _stm_manifest.get_ntp().path())
  , _timeout(
      config::shard_local_cfg().cloud_storage_manifest_upload_timeout_ms.bind())
  , _backoff(config::shard_local_cfg().cloud_storage_initial_backoff_ms.bind())
  , _read_buffer_size(config::shard_local_cfg().storage_read_buffer_size.bind())
  , _readahead_size(
      config::shard_local_cfg().storage_read_readahead_count.bind())
  , _manifest_cache(std::make_unique<materialized_manifest_cache>(
      config::shard_local_cfg().cloud_storage_manifest_cache_size(), _ctxlog))
  , _manifest_meta_size(
      config::shard_local_cfg().cloud_storage_manifest_cache_size.bind())
  , _manifest_meta_ttl(
      config::shard_local_cfg().cloud_storage_manifest_cache_ttl_ms.bind()) {
    _manifest_meta_size.watch([this] {
        ssx::background = ss::with_gate(_gate, [this] {
            vlog(
              _ctxlog.info,
              "Manifest cache capacity will be changed from {} to {}",
              _manifest_cache->get_capacity(),
              _manifest_meta_size());
            return _manifest_cache->set_capacity(_manifest_meta_size());
        });
    });
}

ss::future<> async_manifest_view::start() {
    co_await _manifest_cache->start();
    ssx::background = run_bg_loop();
    co_return;
}

ss::future<> async_manifest_view::stop() {
    _as.request_abort();
    _cvar.broken();
    co_await _manifest_cache->stop();
    co_await _gate.close();
}

ss::future<> async_manifest_view::run_bg_loop() {
    std::exception_ptr exc_ptr;
    ss::gate::holder h(_gate);
    try {
        while (!_as.abort_requested()) {
            co_await _cvar.when(
              [&] { return !_requests.empty() || _as.abort_requested(); });
            _as.check();
            if (!_requests.empty()) {
                auto front = std::move(_requests.front());
                _requests.pop_front();
                try {
                    auto path = get_spillover_manifest_path(front.search_vec);
                    vlog(
                      _ctxlog.debug,
                      "Processing spillover manifest request {}, path: {}",
                      front.search_vec,
                      path);
                    if (is_stm(front.search_vec.base_offset)) {
                        vlog(
                          _ctxlog.warn,
                          "Request {} refers to STM manifest",
                          front.search_vec);
                        // Normally, the request shouldn't contain the STM
                        // request but nothing prevents us from handling this
                        // just in case.
                        front.promise.set_value(std::ref(_stm_manifest));
                        continue;
                    }
                    if (!_manifest_cache->contains(
                          front.search_vec.base_offset)) {
                        // Manifest is not cached and has to be hydrated and/or
                        // materialized.
                        vlog(
                          _ctxlog.debug,
                          "Preparing cache for manifest with {} bytes, path {}",
                          front.search_vec.size_bytes,
                          path);
                        // The timeout is TTL x2 because the cursor is allowed
                        // to hold on to the manifest for up to TTL ms. This
                        // means that waiting exactly TTL milliseconds is not
                        // enough because we need some time for cache to evict
                        // the item and then TTL milliseconds for the cursor
                        // timer to fire.
                        auto u = co_await _manifest_cache->prepare(
                          front.search_vec.size_bytes,
                          _manifest_meta_ttl() * 2);
                        // At this point we have free memory to download the
                        // spillover manifest.
                        auto m_res = co_await materialize_manifest(path);
                        if (m_res.has_failure()) {
                            vlog(
                              _ctxlog.error,
                              "Failed to materialize manifest {}, vec: {}, "
                              "error: "
                              "{}",
                              path,
                              front.search_vec,
                              m_res.error());
                            front.promise.set_value(m_res.as_failure());
                            continue;
                        }
                        // Put newly materialized manifest into the cache
                        auto lso = m_res.value().get_start_offset();
                        vlog(
                          _ctxlog.debug,
                          "Manifest with LSO {} is materialized, using {} "
                          "units to put it into the cache {{cache size: "
                          "{}/{}}}",
                          lso,
                          u.count(),
                          _manifest_cache->size(),
                          _manifest_cache->size_bytes());
                        _manifest_cache->put(
                          std::move(u), std::move(m_res.value()));
                        _probe.set_spillover_manifest_bytes(
                          static_cast<int64_t>(_manifest_cache->size_bytes()));
                        _probe.set_spillover_manifest_instances(
                          static_cast<int32_t>(_manifest_cache->size()));
                        vlog(
                          _ctxlog.debug,
                          "Manifest with LSO {} is cached {{cache size: "
                          "{}/{}}}",
                          lso,
                          _manifest_cache->size(),
                          _manifest_cache->size_bytes());
                    } else {
                        vlog(_ctxlog.debug, "Manifest is already materialized");
                    }
                    auto cached = _manifest_cache->get(
                      front.search_vec.base_offset);
                    front.promise.set_value(cached);
                    vlog(
                      _ctxlog.debug,
                      "Spillover manifest request {} processed successfully, "
                      "found manifest that contains offset range [{}:{}]",
                      front.search_vec,
                      cached->manifest.get_start_offset(),
                      cached->manifest.get_last_offset());
                } catch (const std::system_error& err) {
                    vlog(
                      _ctxlog.error,
                      "Failed processing request {}, exception: {} : {}",
                      front.search_vec,
                      err.code(),
                      err.what());
                    front.promise.set_to_current_exception();
                } catch (...) {
                    vlog(
                      _ctxlog.error,
                      "Failed processing request {}, exception: {}",
                      front.search_vec,
                      std::current_exception());
                    front.promise.set_to_current_exception();
                }
            }
        }
    } catch (const ss::broken_condition_variable&) {
        vlog(_ctxlog.debug, "Broken condition variable exception");
        exc_ptr = std::current_exception();
    } catch (const ss::abort_requested_exception&) {
        vlog(_ctxlog.debug, "Abort requested exception");
        exc_ptr = std::current_exception();
    } catch (const ss::gate_closed_exception&) {
        vlog(_ctxlog.debug, "Gate closed exception");
        exc_ptr = std::current_exception();
    } catch (...) {
        vlog(
          _ctxlog.debug, "Unexpected exception: {}", std::current_exception());
        exc_ptr = std::current_exception();
    }
    if (exc_ptr) {
        // Unblock all readers in case of error
        for (auto& req : _requests) {
            req.promise.set_exception(exc_ptr);
        }
    }
    co_return;
}

ss::future<result<std::unique_ptr<async_manifest_view_cursor>, error_outcome>>
async_manifest_view::get_active(async_view_search_query_t query) noexcept {
    try {
        ss::gate::holder h(_gate);
        if (
          !is_archive(query) && !is_stm(query)
          && !std::holds_alternative<model::timestamp>(query)) {
            // The view should contain manifest below archive start in
            // order to be able to perform retention and advance metadata.
            vlog(
              _ctxlog.debug,
              "query {} is out of valid range, is-archive: {}, is-stm: {}",
              to_string(query),
              is_archive(query),
              is_stm(query));
            co_return error_outcome::out_of_range;
        }
        model::offset begin;
        model::offset end = _stm_manifest.get_last_offset();
        if (_stm_manifest.get_archive_start_offset() == model::offset{}) {
            begin = _stm_manifest.get_start_offset().value_or(begin);
        } else {
            begin = _stm_manifest.get_archive_start_offset();
        }
        auto cursor = std::make_unique<async_manifest_view_cursor>(
          *this, begin, end, _manifest_meta_ttl());
        // This calls 'get_materialized_manifest' internally which
        // could potentially schedule manifest hydration/materialization
        // in the background fiber.
        auto result = co_await cursor->seek(query);
        if (result.has_error()) {
            vlog(
              _ctxlog.error,
              "failed to seek to offset {}, error: {}",
              query,
              result.error());
            co_return result.as_failure();
        }
        if (!result.value()) {
            vlog(
              _ctxlog.debug,
              "failed to seek to offset {}, offset out of valid range",
              query);
            co_return error_outcome::out_of_range;
        }
        co_return cursor;
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to create a cursor: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
}

ss::future<result<std::unique_ptr<async_manifest_view_cursor>, error_outcome>>
async_manifest_view::get_retention_backlog() noexcept {
    try {
        ss::gate::holder h(_gate);
        auto cursor = std::make_unique<async_manifest_view_cursor>(
          *this,
          _stm_manifest.get_archive_clean_offset(),
          model::prev_offset(_stm_manifest.get_archive_start_offset()),
          _manifest_meta_ttl());
        // Query the beginning of the backlog. This will fail if for some reason
        // the spillover manifest doesn't exist in the cloud. To avoid this we
        // should never delete spillover manifests above the
        auto q = _stm_manifest.get_archive_clean_offset();
        auto result = co_await cursor->seek(q);
        if (result.has_error()) {
            vlog(
              _ctxlog.error,
              "failed to seek to offset {} in the retention backlog, error: {}",
              q,
              result.error());
            co_return result.as_failure();
        }
        co_return cursor;
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to create a cursor: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
}

bool async_manifest_view::is_empty() const noexcept {
    return _stm_manifest.size() == 0;
}

bool async_manifest_view::is_archive(async_view_search_query_t o) {
    if (_stm_manifest.get_archive_start_offset() == model::offset{}) {
        return false;
    }
    return ss::visit(
      o,
      [this](model::offset ro) {
          return ro >= _stm_manifest.get_archive_start_offset()
                 && ro < _stm_manifest.get_start_offset().value_or(
                      model::offset::min());
      },
      [this](kafka::offset ko) {
          return ko >= _stm_manifest.get_archive_start_kafka_offset()
                 && ko < _stm_manifest.get_start_kafka_offset().value_or(
                      kafka::offset::min());
      },
      [this](model::timestamp ts) {
          auto bt = _stm_manifest.begin()->base_timestamp;
          return ts < bt;
      });
}

bool async_manifest_view::is_stm(async_view_search_query_t o) {
    return ss::visit(
      o,
      [this](model::offset ro) {
          auto so = _stm_manifest.get_start_offset().value_or(
            model::offset::max());
          return ro >= so;
      },
      [this](kafka::offset ko) {
          auto sko = _stm_manifest.get_start_kafka_offset().value_or(
            kafka::offset::max());
          return ko >= sko;
      },
      [this](model::timestamp ts) {
          auto sm = _stm_manifest.timequery(ts);
          if (!sm.has_value()) {
              return false;
          }
          return sm.value().base_timestamp <= ts
                 && ts <= sm.value().max_timestamp;
      });
}

ss::future<
  result<async_manifest_view::archive_start_offset_advance, error_outcome>>
async_manifest_view::compute_retention(
  std::optional<size_t> size_limit,
  std::optional<std::chrono::milliseconds> time_limit) noexcept {
    archive_start_offset_advance time_result;
    archive_start_offset_advance size_result;
    if (time_limit.has_value()) {
        auto res = co_await time_based_retention(time_limit.value());
        if (res.has_value()) {
            time_result = res.value();
        } else {
            vlog(
              _ctxlog.error,
              "Failed to compute time-based retention",
              res.error());
        }
    }
    if (size_limit.has_value()) {
        auto res = co_await size_based_retention(size_limit.value());
        if (res.has_value()) {
            size_result = res.value();
        } else {
            vlog(
              _ctxlog.error,
              "Failed to compute size-based retention",
              res.error());
        }
    }
    archive_start_offset_advance result;
    if (size_result.offset > time_result.offset) {
        result = size_result;
    } else {
        result = time_result;
    }
    if (
      _stm_manifest.get_start_kafka_offset_override() != kafka::offset{}
      && _stm_manifest.get_start_kafka_offset_override() > result.offset) {
        // The start kafka offset is placed above the retention boundary. We
        // need to adjust retention boundary to remove all data up to start
        // kafka offset.
        vlog(
          _ctxlog.debug,
          "Start kafka offset override {} exceeds computed retention {}",
          _stm_manifest.get_start_kafka_offset_override(),
          result.offset);
        auto r = co_await offset_based_retention();
        if (r.has_error()) {
            vlog(
              _ctxlog.error,
              "Failed to compute offset-based retention",
              r.error());
        }
        result = r.value();
    }
    co_return result;
}

ss::future<
  result<async_manifest_view::archive_start_offset_advance, error_outcome>>
async_manifest_view::offset_based_retention() noexcept {
    archive_start_offset_advance result;
    try {
        auto boundary = _stm_manifest.get_start_kafka_offset_override();
        auto res = co_await get_active(boundary);
        if (res.has_failure() && res.error() != error_outcome::out_of_range) {
            vlog(
              _ctxlog.error,
              "Failed to compute time-based retention {}",
              res.error());
            co_return res.as_failure();
        }
        if (res.has_failure() && res.error() == error_outcome::out_of_range) {
            // The cutoff point is outside of the offset range, no need to
            // do anything
            vlog(
              _ctxlog.debug,
              "There is no segment old enough to be removed by retention");
        } else {
            const auto& manifest = res.value()->manifest()->get();
            vassert(
              !manifest.empty(),
              "{} Spillover manifest can't be empty",
              get_ntp());
            result.offset = manifest.begin()->base_offset;
            result.delta = manifest.begin()->delta_offset;
        }
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to compute retention {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
    co_return result;
}

ss::future<
  result<async_manifest_view::archive_start_offset_advance, error_outcome>>
async_manifest_view::time_based_retention(
  std::chrono::milliseconds time_limit) noexcept {
    archive_start_offset_advance result;

    try {
        auto now = model::timestamp_clock::now();
        auto delta
          = std::chrono::duration_cast<model::timestamp_clock::duration>(
            time_limit);
        auto boundary = model::to_timestamp(now - delta);
        vlog(
          _ctxlog.debug,
          "Computing time-based retention, boundary: {}",
          boundary);
        auto res = co_await get_active(boundary);
        if (res.has_failure() && res.error() != error_outcome::out_of_range) {
            vlog(
              _ctxlog.error,
              "Failed to compute time-based retention {}",
              res.error());
            co_return res.as_failure();
        }
        if (res.has_failure() && res.error() == error_outcome::out_of_range) {
            // The cutoff point is outside of the offset range, no need to
            // do anything
            vlog(
              _ctxlog.debug,
              "There is no segment old enough to be removed by retention");
        } else {
            auto cursor = std::move(res.value());
            auto [offset, delta] = cursor->manifest(
              [boundary](const partition_manifest& manifest) {
                  model::offset prev_offset;
                  model::offset_delta prev_delta;
                  for (const auto& meta : manifest) {
                      if (meta.base_timestamp > boundary) {
                          return std::make_tuple(prev_offset, prev_delta);
                      }
                      prev_offset = meta.base_offset;
                      prev_delta = meta.delta_offset;
                  }
                  return std::make_tuple(
                    model::offset{}, model::offset_delta{});
              });
            result.offset = offset;
            result.delta = delta;
            if (result.offset == model::offset{}) {
                vlog(
                  _ctxlog.debug,
                  "Failed to find the retention boundary, the manifest {} "
                  "doesn't "
                  "have any matching segment",
                  cursor->manifest()->get().get_manifest_path());
            }
        }
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to compute retention {}",
          std::current_exception());
        co_return error_outcome::failure;
    }

    co_return result;
}

ss::future<
  result<async_manifest_view::archive_start_offset_advance, error_outcome>>
async_manifest_view::size_based_retention(size_t size_limit) noexcept {
    archive_start_offset_advance result;
    try {
        auto cloud_log_size = _stm_manifest.cloud_log_size();
        if (cloud_log_size > size_limit) {
            auto to_remove = cloud_log_size - size_limit;
            vlog(
              _ctxlog.debug,
              "Computing size-based retention, log size: {}, limit: {}, {} "
              "bytes will be removed",
              cloud_log_size,
              size_limit,
              to_remove);
            auto res = co_await get_active(
              _stm_manifest.get_archive_start_offset());
            if (res.has_failure()) {
                vlog(
                  _ctxlog.error,
                  "Failed to compute size-based retention {}",
                  res.error());
                co_return res.as_failure();
            }
            auto cursor = std::move(res.value());
            while (to_remove != 0) {
                // We are reading from the spillover manifests until
                // the 'to_remove' value is zero. Every time we read
                // we're advancing the last_* values. The scan shouldn't
                // go to the STM manifest and should only include archive.
                // The end condition is the lambda returned true, otherwise
                // we should keep scanning.
                model::offset last_offset;
                model::offset_delta last_delta;
                auto eof = cursor->manifest(
                  [this, &to_remove, &last_offset, &last_delta](
                    const auto& manifest) mutable {
                      if (
                        _stm_manifest.get_start_offset()
                        == manifest.get_start_offset()) {
                          // We reached the STM manifest
                          return true;
                      }
                      for (const auto& meta : manifest) {
                          if (meta.size_bytes > to_remove) {
                              vlog(_ctxlog.debug, "Retention stop at {}", meta);
                              to_remove = 0;
                              return true;
                          } else {
                              to_remove -= meta.size_bytes;
                              last_offset = meta.base_offset;
                              last_delta = meta.delta_offset;
                              vlog(
                                _ctxlog.debug,
                                "Retention consume {}, remaining bytes: {}",
                                meta,
                                to_remove);
                          }
                      }
                      return false;
                  });
                result.offset = last_offset;
                result.delta = last_delta;
                if (!eof) {
                    auto r = co_await cursor->next();
                    if (
                      r.has_failure()
                      && r.error() == error_outcome::out_of_range) {
                        vlog(
                          _ctxlog.info,
                          "Entire archive is removed by the size-based "
                          "retention");
                    } else if (r.has_failure()) {
                        vlog(
                          _ctxlog.error,
                          "Failed to scan manifest while computing retention "
                          "{}",
                          r.error());
                        co_return r.as_failure();
                    }
                } else {
                    vlog(
                      _ctxlog.debug,
                      "Retention found offset {} with delta {}",
                      result.offset,
                      result.delta);
                    break;
                }
            }
        } else {
            vlog(
              _ctxlog.debug,
              "Log size ({}) is withing the limit ({})",
              cloud_log_size,
              size_limit);
        }
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to compute retention {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
    co_return result;
}

ss::future<result<manifest_section_t, error_outcome>>
async_manifest_view::get_materialized_manifest(
  async_view_search_query_t q) noexcept {
    try {
        ss::gate::holder h(_gate);
        if (is_stm(q)) {
            vlog(_ctxlog.debug, "Query {} matches with STM manifest", q);
            // Fast path for STM reads
            co_return std::ref(_stm_manifest);
        }
        if (
          !is_stm(q) && std::holds_alternative<model::timestamp>(q)
          && _stm_manifest.get_archive_start_offset() == model::offset{}) {
            vlog(_ctxlog.debug, "Using STM manifest for timequery {}", q);
            co_return std::ref(_stm_manifest);
        }
        auto meta = search_spillover_manifests(q);
        if (!meta.has_value()) {
            vlog(_ctxlog.debug, "Can't find requested manifest, {}", q);
            co_return error_outcome::out_of_range;
        }
        vlog(_ctxlog.debug, "Found spillover manifest meta: {}", meta);
        auto res = _manifest_cache->get(meta->base_offset);
        if (res) {
            co_return res;
        }
        // Send materialization request to background loop
        materialization_request_t request{
          .search_vec = *meta,
          ._measurement = _probe.spillover_manifest_latency(),
        };
        auto fut = request.promise.get_future();
        _requests.emplace_back(std::move(request));
        _cvar.signal();
        auto m = co_await std::move(fut);
        if (m.has_failure()) {
            vlog(
              _ctxlog.error,
              "Failed to materialize spillover manifest: {}",
              m.error());
            co_return m.as_failure();
        }
        co_return m.value();
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to materialize spillover manifest: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
}

ss::future<result<spillover_manifest, error_outcome>>
async_manifest_view::hydrate_manifest(
  remote_manifest_path path) const noexcept {
    try {
        spillover_manifest manifest(
          _stm_manifest.get_ntp(), _stm_manifest.get_revision_id());
        retry_chain_node fib(_timeout(), _backoff(), &_rtcnode);
        auto res = co_await _remote.local().download_manifest(
          _bucket, path, manifest, fib);
        if (res != download_result::success) {
            vlog(
              _ctxlog.error,
              "failed to download manifest {}, object key: {}",
              res,
              path);
            co_return error_outcome::manifest_download_error;
        }
        auto [str, len] = co_await manifest.serialize();
        co_await _cache.local().put(
          manifest.get_manifest_path()(),
          str,
          priority_manager::local().shadow_indexing_priority());
        _probe.on_spillover_manifest_hydration();
        co_return std::move(manifest);
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to materialize segment: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
}

namespace {
bool contains(
  const segment_meta& meta, const async_view_search_query_t& query) {
    return ss::visit(
      query,
      [&](model::offset o) {
          return o >= meta.base_offset && o <= meta.committed_offset;
      },
      [&](kafka::offset k) {
          return k >= meta.base_kafka_offset() && k < meta.next_kafka_offset();
      },
      [&](model::timestamp t) {
          return t >= meta.base_timestamp && t <= meta.max_timestamp;
      });
}
} // namespace

std::optional<segment_meta> async_manifest_view::search_spillover_manifests(
  async_view_search_query_t query) const {
    const auto& manifests = _stm_manifest.get_spillover_map();
    // TODO: implement late materialization for filtering
    for (const auto& it : manifests) {
        if (contains(it, query)) {
            return it;
        }
    }
    return std::nullopt;
}

remote_manifest_path async_manifest_view::get_spillover_manifest_path(
  const segment_meta& meta) const {
    spillover_manifest_path_components comp{
      .base = meta.base_offset,
      .last = meta.committed_offset,
      .base_kafka = meta.base_kafka_offset(),
      .next_kafka = meta.next_kafka_offset(),
      .base_ts = meta.base_timestamp,
      .last_ts = meta.max_timestamp,
    };
    return generate_spillover_manifest_path(
      get_ntp(), _stm_manifest.get_revision_id(), comp);
}

ss::future<result<spillover_manifest, error_outcome>>
async_manifest_view::materialize_manifest(
  remote_manifest_path path) const noexcept {
    try {
        auto h = _gate.hold();
        spillover_manifest manifest(
          _stm_manifest.get_ntp(), _stm_manifest.get_revision_id());
        // Perform simple scan of the manifest list
        // Probe cache. If not available or in case of race with cache eviction
        // hydrate manifest from the cloud.
        auto cache_status = co_await _cache.local().is_cached(path());
        switch (cache_status) {
        case cache_element_status::in_progress:
            vlog(_ctxlog.warn, "Concurrent manifest hydration, path {}", path);
            co_return error_outcome::repeat;
        case cache_element_status::not_available: {
            auto res = co_await hydrate_manifest(path);
            if (res.has_failure()) {
                vlog(
                  _ctxlog.error,
                  "failed to download manifest, object key: {}, error: {}",
                  path,
                  res.error());
                co_return error_outcome::manifest_download_error;
            }
            auto manifest = std::move(res.value());
            auto [str, len] = co_await manifest.serialize();
            co_await _cache.local().put(
              manifest.get_manifest_path()(),
              str,
              priority_manager::local().shadow_indexing_priority());
        } break;
        case cache_element_status::available: {
            auto res = co_await _cache.local().get(path());
            if (!res.has_value()) {
                vlog(
                  _ctxlog.warn,
                  "failed to read cached manifest, object key: {}",
                  path);
                // Cache race removed the file after `is_cached` check, the
                // upper layer is supposed to retry the call.
                co_return error_outcome::repeat;
            }
            std::exception_ptr update_err;
            try {
                ss::file_input_stream_options options{
                  .buffer_size = _read_buffer_size(),
                  .read_ahead = static_cast<uint32_t>(_readahead_size()),
                  .io_priority_class
                  = priority_manager::local().shadow_indexing_priority()};
                auto data_stream = ss::make_file_input_stream(
                  res->body, 0, std::move(options));
                co_await manifest.update(std::move(data_stream));
                vlog(
                  _ctxlog.debug,
                  "Manifest is materialized, start offset {}, last offset {}",
                  manifest.get_start_offset(),
                  manifest.get_last_offset());
            } catch (...) {
                vlog(
                  _ctxlog.error,
                  "Error during manifest update: {}",
                  std::current_exception());
                update_err = std::current_exception();
            }
            co_await res->body.close();
            if (update_err) {
                std::rethrow_exception(update_err);
            }
        } break;
        }
        _probe.on_spillover_manifest_materialization();
        co_return manifest;
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to materialize spillover manifest: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
}
} // namespace cloud_storage
