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
#include "storage/chunk_cache.h"

#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"
#include "resource_mgmt/memory_groups.h"

#include <seastar/core/loop.hh>

#include <boost/iterator/counting_iterator.hpp>

namespace storage::internal {

chunk_cache::chunk_cache() noexcept
  : _size_target(memory_groups().chunk_cache_min_memory())
  , _size_limit(memory_groups().chunk_cache_max_memory())
  , _chunk_size(config::shard_local_cfg().append_chunk_size()) {}

ss::future<> chunk_cache::start() {
    setup_metrics();
    const auto num_chunks = memory_groups().chunk_cache_min_memory()
                            / _chunk_size;
    return ss::do_for_each(
      boost::counting_iterator<size_t>(0),
      boost::counting_iterator<size_t>(num_chunks),
      [this](size_t) {
          auto c = ss::make_lw_shared<chunk>(_chunk_size, alignment);
          _size_total += _chunk_size;
          add(c);
      });
}

ss::future<> chunk_cache::stop() {
    _metrics.clear();
    return ss::now();
}

void chunk_cache::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("chunk_cache"),
      {
        sm::make_gauge(
          "total_size_bytes",
          [this] { return _size_total; },
          sm::description("Total size of all segment appender chunks in any "
                          "state, in bytes.")),
        sm::make_gauge(
          "available_size_bytes",
          [this] { return _size_available; },
          sm::description("Total size of all free segment appender chunks in "
                          "the cache, in bytes.")),
        sm::make_counter(
          "wait_count",
          [this] { return _wait_for_chunk_count; },
          sm::description("Count of how many times we had to wait for a chunk "
                          "to become available")),
      });
}

void chunk_cache::add(const chunk_ptr& chunk) {
    if (_size_available >= _size_target) {
        _size_total -= _chunk_size;
        return;
    }
    _chunks.push_back(chunk);
    _size_available += _chunk_size;
    if (_sem.waiters()) {
        _sem.signal();
    }
}

ss::future<chunk_cache::chunk_ptr> chunk_cache::get() {
    // don't steal if there are waiters
    if (!_sem.waiters()) {
        return do_get();
    }

    return wait_and_get();
}

ss::future<chunk_cache::chunk_ptr> chunk_cache::do_get() {
    if (auto c = pop_or_allocate(); c) {
        return ss::make_ready_future<chunk_ptr>(c);
    }

    return wait_and_get();
}

ss::future<chunk_cache::chunk_ptr> chunk_cache::wait_and_get() {
    auto fut = ss::get_units(_sem, 1);
    if (_sem.waiters()) {
        _wait_for_chunk_count++;
    }

    return fut.then([this](ssx::semaphore_units) { return do_get(); });
}

chunk_cache::chunk_ptr chunk_cache::pop_or_allocate() {
    if (!_chunks.empty()) {
        auto c = _chunks.front();
        _chunks.pop_front();
        _size_available -= _chunk_size;
        c->reset();
        return c;
    }
    if (_size_total < _size_limit) {
        auto c = ss::make_lw_shared<chunk>(_chunk_size, alignment);
        _size_total += _chunk_size;
        return c;
    }
    return nullptr;
}

chunk_cache& chunks() {
    static thread_local chunk_cache cache;
    return cache;
}

} // namespace storage::internal
