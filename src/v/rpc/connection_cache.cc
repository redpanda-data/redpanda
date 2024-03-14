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
#include "ssx/semaphore.h"

#include <seastar/core/loop.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/smp.hh>

#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <iterator>
#include <ranges>
#include <vector>

namespace rpc {

connection_allocation_strategy::connection_allocation_strategy(
  unsigned max_connections_per_node, unsigned total_shards)
  : _total_shards(total_shards)
  , _max_connections_per_node(max_connections_per_node)
  , _g(std::random_device()()) {
    _connections_per_shard.reserve(_total_shards);
    for (auto shard : std::views::iota((unsigned)0, _total_shards)) {
        _connections_per_shard.emplace_back(shard, 0);
    }
}

bool connection_allocation_strategy::has_connection_assignments_for(
  model::node_id node) const {
    return _node_to_shards.contains(node);
}

connection_allocation_strategy::changes
connection_allocation_strategy::create_connection_assignments_for(
  model::node_id node) {
    changes ret;

    auto connections_per_node = std::min(
      _total_shards, _max_connections_per_node);

    // The goal of the shuffle then sort below is to randomly
    // order all shards that have the same number of connections.
    std::shuffle(
      _connections_per_shard.begin(), _connections_per_shard.end(), _g);
    // Sort connections per shard in ascending order.
    std::sort(
      _connections_per_shard.begin(),
      _connections_per_shard.end(),
      [](auto a, auto b) { return std::get<1>(a) < std::get<1>(b); });

    auto& n2s = _node_to_shards[node];

    for (unsigned i = 0; i < connections_per_node; i++) {
        auto& cps = _connections_per_shard[i];
        ret.add_connections.emplace_back(node, std::get<0>(cps));
        std::get<1>(cps)++;
        n2s.emplace(std::get<0>(cps));
    }

    get_shard_to_connection_assignments(ret, node);

    return ret;
}

connection_allocation_strategy::changes
connection_allocation_strategy::remove_connection_assignments_for(
  model::node_id node) {
    changes ret;

    if (!has_connection_assignments_for(node)) {
        return ret;
    }

    auto& conn_shards = _node_to_shards[node];

    for (auto shard : conn_shards) {
        auto s_it = std::find_if(
          _connections_per_shard.begin(),
          _connections_per_shard.end(),
          [shard](const auto& t) { return std::get<0>(t) == shard; });

        if (s_it != _connections_per_shard.cend()) {
            std::get<1>(*s_it)--;
            ret.remove_connections.emplace_back(node, std::get<0>(*s_it));
        }
    }

    _node_to_shards.erase(node);

    for (auto shard : std::views::iota((unsigned)0, _total_shards)) {
        ret.remove_mapping.emplace_back(node, shard);
    }

    return ret;
}

void connection_allocation_strategy::get_shard_to_connection_assignments(
  changes& c, model::node_id node) {
    auto& conn_shards = _node_to_shards.at(node);

    // Assign shards that own a connection to use their own connection
    for (auto shard : conn_shards) {
        c.add_mapping.emplace_back(node, shard, shard);
    }

    // Randomly assign the remaining shards to connections.
    std::vector<ss::shard_id> conn_shards_r{
      conn_shards.begin(), conn_shards.end()};
    std::shuffle(conn_shards_r.begin(), conn_shards_r.end(), _g);

    auto j = 0;
    for (auto shard : std::views::iota((unsigned)0, _total_shards)) {
        if (conn_shards.contains(shard)) {
            continue;
        }

        auto connection_idx = j++ % conn_shards_r.size();
        c.add_mapping.emplace_back(node, shard, conn_shards_r[connection_idx]);
    }
}

connection_cache::connection_cache(
  ss::sharded<ss::abort_source>& as,
  std::optional<connection_cache_label> label,
  unsigned connections_per_node)
  : _label(std::move(label)) {
    _as_subscription = as.local().subscribe(
      [this]() mutable noexcept { shutdown(); });
    if (ss::this_shard_id() == _coordinator_shard) {
        _coordinator_state = std::make_unique<coordinator_state>(
          mutex{"connection_cache"},
          connection_allocation_strategy(connections_per_node, ss::smp::count));
    }
}

std::optional<ss::shard_id> connection_cache::shard_for(
  model::node_id, ss::shard_id, model::node_id n, ss::shard_id) const {
    auto shard_it = _connection_map.find(n);
    if (shard_it == _connection_map.end()) {
        return {};
    }

    return shard_it->second;
}

ss::future<> connection_cache::emplace(
  model::node_id n,
  rpc::transport_configuration c,
  backoff_policy backoff_policy) {
    if (_cache.contains(n)) {
        co_return;
    }
    _cache.emplace(
      n,
      ss::make_lw_shared<rpc::reconnect_transport>(
        std::move(c), std::move(backoff_policy), _label, n));
    _connection_map[n] = ss::this_shard_id();
}

/// \brief closes all client connections
ss::future<> connection_cache::do_shutdown() {
    _shutting_down = true;

    if (ss::this_shard_id() == _coordinator_shard) {
        auto units = co_await _coordinator_state->mtx.get_units();
        //  mark mutex as broken to prevent new connections from being created
        //  after stop
        _coordinator_state->mtx.broken();
    }

    co_await _cache.remove_all();
}

void connection_cache::shutdown() {
    ssx::spawn_with_gate(_gate, [this] { return do_shutdown(); });
}

ss::future<> connection_cache::stop() {
    shutdown();
    return _gate.close();
}

ss::future<> connection_cache::apply_changes(
  connection_allocation_strategy::changes changes,
  std::optional<connection_config> config) {
    // Add connections we have the config for.
    if (config) {
        for (auto& con_add : changes.add_connections) {
            if (con_add.node == config->dest_node) {
                co_await container().invoke_on(
                  con_add.shard,
                  [cfg = *config](connection_cache& cache) mutable {
                      if (cache.is_shutting_down()) {
                          return ss::now();
                      }
                      return cache._cache.try_add_or_update(
                        cfg.dest_node,
                        std::move(cfg.addr),
                        std::move(cfg.tls_config),
                        std::move(cfg.backoff));
                  });
            }
        }
    }

    // Add shard mappings
    co_await ss::parallel_for_each(changes.add_mapping, [this](auto map) {
        return add_or_update_connection_location(
          map.src_shard, map.node, map.conn_shard);
    });

    // Remove any shard mappings
    co_await ss::parallel_for_each(changes.remove_mapping, [this](auto map) {
        return remove_connection_location(map.shard, map.node);
    });

    // Remove any connections
    co_await ss::parallel_for_each(
      changes.remove_connections, [this](auto con) {
          return container().invoke_on(con.shard, [con](auto& cache) {
              if (cache.is_shutting_down()) {
                  return ss::now();
              }
              return cache._cache.remove(con.node);
          });
      });
}

ss::future<> connection_cache::remove_broker_client_coordinator(
  model::node_id, model::node_id dest) {
    vassert(ss::this_shard_id() == _coordinator_shard, "not the coordinator");

    if (is_shutting_down()) {
        co_return;
    }
    auto holder = _gate.hold();

    auto units = co_await _coordinator_state->mtx.get_units();
    auto& alloc_strat = _coordinator_state->alloc_strat;

    if (alloc_strat.has_connection_assignments_for(dest)) {
        auto changes = alloc_strat.remove_connection_assignments_for(dest);
        co_await apply_changes(changes, std::nullopt);
    }
}

ss::future<>
connection_cache::remove_broker_client(model::node_id self, model::node_id id) {
    if (is_shutting_down()) {
        co_return;
    }
    auto holder = _gate.hold();

    co_await container().invoke_on(
      _coordinator_shard, [self, id](auto& coordinator) mutable {
          return coordinator.remove_broker_client_coordinator(self, id);
      });
}

ss::future<>
connection_cache::update_broker_client_coordinator(connection_config cfg) {
    vassert(ss::this_shard_id() == _coordinator_shard, "not the coordinator");

    if (is_shutting_down()) {
        co_return;
    }
    auto holder = _gate.hold();

    auto units = co_await _coordinator_state->mtx.get_units();
    auto& alloc_strat = _coordinator_state->alloc_strat;

    if (!alloc_strat.has_connection_assignments_for(cfg.dest_node)) {
        auto changes = alloc_strat.create_connection_assignments_for(
          cfg.dest_node);
        co_await apply_changes(std::move(changes), std::move(cfg));
        co_return;
    }
    const auto& conn_shards = alloc_strat.get_connection_shards(cfg.dest_node);

    co_await ss::parallel_for_each(
      conn_shards, [this, cfg = std::move(cfg)](auto shard) {
          return container().invoke_on(
            shard, [cfg](connection_cache& cache) mutable {
                if (cache.is_shutting_down()) {
                    return ss::now();
                }
                return cache._cache.try_add_or_update(
                  cfg.dest_node,
                  std::move(cfg.addr),
                  std::move(cfg.tls_config),
                  std::move(cfg.backoff));
            });
      });
}

ss::future<> connection_cache::update_broker_client(
  model::node_id self,
  model::node_id node,
  net::unresolved_address addr,
  config::tls_config tls_config,
  rpc::backoff_policy backoff) {
    if (is_shutting_down()) {
        co_return;
    }
    auto holder = _gate.hold();

    connection_cache::connection_config config{
      self, node, std::move(addr), std::move(tls_config), std::move(backoff)};

    co_await container().invoke_on(
      _coordinator_shard,
      [config = std::move(config)](auto& coordinator) mutable {
          return coordinator.update_broker_client_coordinator(
            std::move(config));
      });
}

ss::future<> connection_cache::reset_client_backoff(
  model::node_id self, ss::shard_id src_shard, model::node_id node_id) {
    if (is_shutting_down()) {
        return ss::now();
    }

    auto shard = rpc::connection_cache::shard_for(self, src_shard, node_id);
    if (!shard) {
        return ss::now();
    }

    return ss::with_gate(_gate, [this, node_id, shard] {
        return container().invoke_on(
          *shard, [node_id](rpc::connection_cache& cache) mutable {
              if (cache.is_shutting_down()) {
                  return;
              }
              cache._cache.reset_client_backoff(node_id);
          });
    });
}

} // namespace rpc
