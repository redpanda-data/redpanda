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
  : _max_connections(8)
  , _label(std::move(label)) {
    _as_subscription = as.local().subscribe(
      [this]() mutable noexcept { shutdown(); });
}

connection_cache::connection_cache(
  config::binding<size_t> max_connections,
  ss::sharded<ss::abort_source>& as,
  std::optional<connection_cache_label> label)
  : _max_connections(max_connections())
  , _label(std::move(label)) {
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

/// \brief closes all client connections
ss::future<> connection_cache::do_shutdown() {
    auto units = co_await _mutex.get_units();
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

ss::shard_id connection_cache::shard_for(
  model::node_id src_node,
  ss::shard_id src_shard,
  model::node_id dest_node,
  ss::shard_id total_shards) const {
    if (total_shards <= _max_connections) {
        return src_shard;
    }

    // NOLINTNEXTLINE
    uint64_t connection_hash = 201326611; // magic prime
    boost::hash_combine(connection_hash, std::hash<model::node_id>{}(src_node));
    boost::hash_combine(
      connection_hash, std::hash<model::node_id>{}(dest_node));

    // Find the largest ring_size s.t. ring_size <= total_shards
    // and ring_size % connections_per_node == 0
    uint64_t remainder = total_shards % _max_connections;
    uint64_t ring_size = total_shards - remainder;

    // Vary the starting shard of the ring depending on the connection hash.
    uint64_t ring_start_shard = jump_consistent_hash(
      connection_hash, remainder + 1);
    uint64_t ring_end_shard = (ring_size - 1) + ring_start_shard;

    // Hash any shards not in [ring_start_shard, ring_end_shard] to a point
    // within the range.
    if (src_shard > ring_end_shard || src_shard < ring_start_shard) {
        // NOLINTNEXTLINE
        uint64_t h = 805306457; // another magic prime
        boost::hash_combine(h, std::hash<ss::shard_id>{}(src_shard));
        boost::hash_combine(h, connection_hash);
        src_shard = ring_start_shard + jump_consistent_hash(h, ring_size);
    }

    // Connections are evenly spaced within the ring. The code below determines
    // the spacing and the closest connection to the left of the src_shard.
    uint64_t shards_per_connection = ring_size / _max_connections;
    uint64_t offset = jump_consistent_hash(
      connection_hash, shards_per_connection);
    uint64_t jump = (src_shard + offset + 1) % shards_per_connection;

    // Wrap any underflow to the end of the ring.
    if (src_shard < jump || (src_shard - jump) < ring_start_shard) {
        jump -= (src_shard - ring_start_shard) + 1;
        return ring_end_shard - jump;
    }

    return src_shard - jump;
}
} // namespace rpc
