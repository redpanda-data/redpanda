/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/outcome_future_utils.h"
#include "model/metadata.h"
#include "rpc/backoff_policy.h"
#include "rpc/connection_set.h"
#include "rpc/errc.h"
#include "rpc/reconnect_transport.h"
#include "rpc/types.h"
#include "utils/mutex.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <unordered_map>

namespace rpc {

class connection_allocation_strategy {
public:
    connection_allocation_strategy(
      unsigned max_connections_per_node, unsigned total_shards);

    // \brief Returns whether a node has already been assigned connections.
    bool has_connection_assignments_for(model::node_id) const;

    struct connection_assignment_action {
        model::node_id node;
        ss::shard_id shard;
    };
    using remove_mapping_action = connection_assignment_action;

    struct shard_assignment_action {
        model::node_id node;
        ss::shard_id src_shard;
        ss::shard_id conn_shard;
    };

    struct changes {
        std::vector<connection_assignment_action> add_connections;
        std::vector<connection_assignment_action> remove_connections;
        std::vector<shard_assignment_action> add_mapping;
        std::vector<remove_mapping_action> remove_mapping;
    };

    changes create_connection_assignments_for(model::node_id);
    changes remove_connection_assignments_for(model::node_id);

    const absl::flat_hash_set<ss::shard_id>&
    get_connection_shards(model::node_id id) const {
        return _node_to_shards.at(id);
    }

private:
    unsigned _total_shards;
    unsigned _max_connections_per_node;

    absl::flat_hash_map<model::node_id, absl::flat_hash_set<ss::shard_id>>
      _node_to_shards;
    std::vector<std::pair<ss::shard_id, size_t>> _connections_per_shard;

    std::mt19937 _g;

    void get_shard_to_connection_assignments(changes&, model::node_id);
};

class connection_cache final
  : public ss::peering_sharded_service<connection_cache> {
    static constexpr ss::shard_id _coordinator_shard = ss::shard_id{0};

public:
    using transport_ptr = ss::lw_shared_ptr<rpc::reconnect_transport>;
    using underlying = std::unordered_map<model::node_id, transport_ptr>;
    using iterator = typename underlying::iterator;

    explicit connection_cache(
      ss::sharded<ss::abort_source>&,
      std::optional<connection_cache_label> label = std::nullopt,
      unsigned connections_per_node = 8);

    bool contains(model::node_id n) const { return _cache.contains(n); }
    transport_ptr get(model::node_id n) const { return _cache.get(n); }

    /// \brief for unit testing
    ///
    /// Force the connection cache to have an entry for the node on this shard.
    ss::future<>
    emplace(model::node_id n, rpc::transport_configuration c, backoff_policy);

    /// \brief closes all connections
    ss::future<> do_shutdown();
    void shutdown();

    ss::future<> stop();

    /**
     * RPC version to use for newly constructed `transport` objects
     */
    transport_version get_default_transport_version() {
        return _cache.get_default_transport_version();
    }

    void set_default_transport_version(transport_version v) {
        _cache.set_default_transport_version(v);
    }

    template<typename Protocol, typename Func>
    requires requires(Func&& f, Protocol proto) { f(proto); }
    auto with_node_client(
      model::node_id self,
      ss::shard_id src_shard,
      model::node_id node_id,
      timeout_spec connection_timeout,
      Func&& f) {
        using ret_t = result_wrap_t<std::invoke_result_t<Func, Protocol>>;

        if (is_shutting_down()) {
            return ss::futurize<ret_t>::convert(
              rpc::make_error_code(errc::shutting_down));
        }

        auto shard = rpc::connection_cache::shard_for(self, src_shard, node_id);
        if (!shard) {
            return ss::futurize<ret_t>::convert(
              rpc::make_error_code(errc::missing_node_rpc_client));
        }

        return ss::with_gate(
          _gate,
          [this,
           node_id,
           connection_timeout,
           shard,
           f = std::forward<Func>(f)]() mutable {
              return container().invoke_on(
                *shard,
                [node_id, f = std::forward<Func>(f), connection_timeout](
                  connection_cache& cache) mutable {
                    if (cache.is_shutting_down()) {
                        return ss::futurize<ret_t>::convert(
                          rpc::make_error_code(errc::shutting_down));
                    }

                    return cache._cache.with_node_client<Protocol, Func>(
                      node_id, connection_timeout, std::forward<Func>(f));
                });
          });
    }

    template<typename Protocol, typename Func, RpcDurationOrPoint Timeout>
    requires requires(Func&& f, Protocol proto) { f(proto); }
    auto with_node_client(
      model::node_id self,
      ss::shard_id src_shard,
      model::node_id node_id,
      Timeout connection_timeout,
      Func&& f) {
        return with_node_client<Protocol, Func>(
          self,
          src_shard,
          node_id,
          timeout_spec::from_either(connection_timeout),
          std::forward<Func>(f));
    }

    /// If a reconnect_transport is in a backed-off state, reset
    /// it so that the next RPC will be dispatched.  This is useful
    /// when a down node comes back to life: the first time we see
    /// a message from a re-awakened peer, we reset their backoff.
    ss::future<> reset_client_backoff(
      model::node_id self, ss::shard_id src_shard, model::node_id node_id);

    ss::future<> remove_broker_client(model::node_id self, model::node_id id);

    ss::future<> update_broker_client(
      model::node_id self,
      model::node_id node,
      net::unresolved_address addr,
      config::tls_config tls_config,
      rpc::backoff_policy backoff = connection_set::default_backoff_policy());

    std::optional<ss::shard_id> shard_for(
      model::node_id self,
      ss::shard_id src,
      model::node_id node,
      ss::shard_id max_shards = ss::smp::count) const;

private:
    std::optional<connection_cache_label> _label;
    connection_set _cache;
    ss::gate _gate;
    ss::optimized_optional<ss::abort_source::subscription> _as_subscription;
    bool _shutting_down = false;

    bool is_shutting_down() const {
        return _gate.is_closed() || _shutting_down;
    }

    struct coordinator_state {
        mutex mtx; // to add/remove nodes
        connection_allocation_strategy alloc_strat;
    };

    // Only initialized on the coordinator shard
    std::unique_ptr<coordinator_state> _coordinator_state;

    struct connection_config {
        model::node_id self;
        model::node_id dest_node;
        net::unresolved_address addr;
        config::tls_config tls_config;
        rpc::backoff_policy backoff;
    };

    ss::future<> update_broker_client_coordinator(connection_config);
    ss::future<>
    remove_broker_client_coordinator(model::node_id self, model::node_id dest);

    ss::future<> apply_changes(
      connection_allocation_strategy::changes,
      std::optional<connection_config>);

    // Shard-local map that where connections for a given shard are located
    absl::flat_hash_map<model::node_id, ss::shard_id> _connection_map;

    ss::future<> add_or_update_connection_location(
      ss::shard_id dest_shard, model::node_id node, ss::shard_id conn_loc) {
        return container().invoke_on(dest_shard, [node, conn_loc](auto& cache) {
            if (cache.is_shutting_down()) {
                return;
            }
            cache._connection_map[node] = conn_loc;
        });
    }

    ss::future<>
    remove_connection_location(ss::shard_id dest_shard, model::node_id node) {
        return container().invoke_on(dest_shard, [node](auto& cache) {
            if (cache.is_shutting_down()) {
                return;
            }
            cache._connection_map.erase(node);
        });
    }
};

} // namespace rpc
