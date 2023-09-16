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
#include "rpc/logger.h"

#include <seastar/core/loop.hh>

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
        if (_cache.contains(n)) {
            return;
        }
        _cache.emplace(
          n,
          ss::make_lw_shared<rpc::reconnect_transport>(
            std::move(c), std::move(backoff_policy), _label, n));
    });
}
ss::future<> connection_cache::remove(model::node_id n) {
    return _mutex.with([this, n]() { return _cache.remove(n); });
}

ss::future<> connection_cache::remove_all() {
    auto units = co_await _mutex.get_units();
    co_await _cache.remove_all();
}

/// \brief closes all client connections
ss::future<> connection_cache::do_shutdown() {
    auto units = co_await _mutex.get_units();
    _shutting_down = true;
    // Exchange ensures the cache is invalidated and concurrent
    // accesses wait on the mutex to populate new entries.
    co_await _cache.remove_all();

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

namespace {
std::vector<ss::shard_id>
virtual_nodes(model::node_id self, model::node_id node) {
    std::set<ss::shard_id> owner_shards;
    for (ss::shard_id i = 0; i < ss::smp::count; ++i) {
        auto shard = rpc::connection_cache::shard_for(self, i, node);
        owner_shards.insert(shard);
    }
    return std::vector<ss::shard_id>(owner_shards.begin(), owner_shards.end());
}
} // namespace

ss::future<>
connection_cache::remove_broker_client(model::node_id self, model::node_id id) {
    auto shards = virtual_nodes(self, id);
    vlog(rpclog.debug, "Removing {} TCP client from shards {}", id, shards);
    return ss::do_with(
      std::move(shards), [this, id](std::vector<ss::shard_id>& i) {
          return ss::do_for_each(i, [this, id](ss::shard_id i) {
              return container().invoke_on(
                i, [id](rpc::connection_cache& cache) {
                    return cache.remove(id);
                });
          });
      });
}

ss::future<> connection_cache::update_broker_client(
  model::node_id self,
  model::node_id node,
  net::unresolved_address addr,
  config::tls_config tls_config,
  rpc::backoff_policy backoff) {
    auto shards = virtual_nodes(self, node);
    vlog(rpclog.debug, "Adding {} TCP client on shards:{}", node, shards);

    co_await ss::parallel_for_each(
      shards,
      [this,
       node,
       addr = std::move(addr),
       tls_config = std::move(tls_config),
       backoff = std::move(backoff)](auto shard) {
          return container().invoke_on(
            shard,
            [node, addr, tls_config, backoff](connection_cache& cache) mutable {
                return cache._mutex.with(
                  [&cache,
                   node,
                   tls_config = std::move(tls_config),
                   addr = std::move(addr),
                   backoff = std::move(backoff)]() mutable {
                      return cache._cache.try_add_or_update(
                        node,
                        std::move(addr),
                        std::move(tls_config),
                        std::move(backoff));
                  });
            });
      });
}

} // namespace rpc
