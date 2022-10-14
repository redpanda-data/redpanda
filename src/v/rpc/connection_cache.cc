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

connection_cache::connection_cache(std::optional<connection_cache_label> label)
  : _label(std::move(label)) {}

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

/// \brief closes all client connections
ss::future<> connection_cache::stop() {
    return _mutex.with([this]() {
        return parallel_for_each(_cache, [](auto& it) {
            auto& [_, cli] = it;
            return cli->stop();
        });
        _cache.clear();
        // mark mutex as broken to prevent new connections from being created
        // after stop
        _mutex.broken();
    });
}

} // namespace rpc
