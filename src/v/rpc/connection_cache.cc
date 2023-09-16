// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "rpc/connection_cache.h"

#include "rpc/backoff_policy.h"

#include <fmt/format.h>

#include <chrono>

namespace rpc {

connection_cache::connection_cache(
  ss::sharded<ss::abort_source>& as,
  std::optional<connection_cache_label> label)
  : _label(std::move(label)) {
    _as_subscription = as.local().subscribe(
      [this]() mutable noexcept { shutdown(); });
}

/// \brief needs to be a future, because mutations may come from different
/// fibers and they need to be synchronized
ss::future<> connection_cache::emplace(
  model::node_id n,
  rpc::transport_configuration c,
  backoff_policy backoff_policy) {
    return _mutex.with([this,
                        n,
                        c = std::move(c),
                        backoff_policy = std::move(backoff_policy)]() mutable {
        if (_cache.find(n) != _cache.end()) {
            return;
        }
        _cache.emplace(
          n,
          ss::make_lw_shared<rpc::reconnect_transport>(
            std::move(c), std::move(backoff_policy), _label, n));
    });
}
ss::future<> connection_cache::remove(model::node_id n) {
    return _mutex
      .with([this, n]() -> transport_ptr {
          auto it = _cache.find(n);
          if (it == _cache.end()) {
              return nullptr;
          }
          auto ptr = it->second;
          _cache.erase(it);
          return ptr;
      })
      .then([](transport_ptr ptr) {
          if (!ptr) {
              return ss::now();
          }
          return ptr->stop().finally([ptr] {});
      });
}

ss::future<> connection_cache::remove_all() {
    auto units = co_await _mutex.get_units();
    auto cache = std::exchange(_cache, {});
    co_await parallel_for_each(cache, [](auto& it) {
        auto& [_, cli] = it;
        return cli->stop();
    });
    cache.clear();
}

/// \brief closes all client connections
ss::future<> connection_cache::do_shutdown() {
    auto units = co_await _mutex.get_units();
    _shutting_down = true;
    // Exchange ensures the cache is invalidated and concurrent
    // accesses wait on the mutex to populate new entries.
    auto cache = std::exchange(_cache, {});
    co_await parallel_for_each(cache, [](auto& it) {
        auto& [_, cli] = it;
        return cli->stop();
    });
    cache.clear();
    // mark mutex as broken to prevent new connections from being created
    // after stop
    _mutex.broken();
}

void connection_cache::shutdown() {
    ssx::spawn_with_gate(_gate, [this] { return do_shutdown(); });
}

ss::future<> connection_cache::stop() {
    shutdown();
    return _gate.close();
}

} // namespace rpc
