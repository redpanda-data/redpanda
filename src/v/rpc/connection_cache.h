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
#include "hashing/jump_consistent_hash.h"
#include "model/metadata.h"
#include "outcome.h"
#include "outcome_future_utils.h"
#include "rpc/backoff_policy.h"
#include "rpc/connection_set.h"
#include "rpc/errc.h"
#include "rpc/reconnect_transport.h"
#include "rpc/types.h"
#include "utils/mutex.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <chrono>
#include <unordered_map>

namespace rpc {
class connection_cache final
  : public ss::peering_sharded_service<connection_cache> {
public:
    using transport_ptr = ss::lw_shared_ptr<rpc::reconnect_transport>;
    using underlying = std::unordered_map<model::node_id, transport_ptr>;
    using iterator = typename underlying::iterator;

    static inline ss::shard_id shard_for(
      model::node_id self,
      ss::shard_id src,
      model::node_id node,
      ss::shard_id max_shards = ss::smp::count);

    explicit connection_cache(
      ss::sharded<ss::abort_source>&,
      std::optional<connection_cache_label> label = std::nullopt);

    bool contains(model::node_id n) const { return _cache.contains(n); }
    transport_ptr get(model::node_id n) const { return _cache.get(n); }

    /// \brief needs to be a future, because mutations may come from different
    /// fibers and they need to be synchronized
    ss::future<>
    emplace(model::node_id n, rpc::transport_configuration c, backoff_policy);

    /// \brief removes the node *and* closes the connection
    ss::future<> remove(model::node_id n);

    /// \brief similar to remove but removes all nodes.
    ss::future<> remove_all();

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
        auto shard = rpc::connection_cache::shard_for(self, src_shard, node_id);

        return container().invoke_on(
          shard,
          [node_id, f = std::forward<Func>(f), connection_timeout](
            rpc::connection_cache& cache) mutable {
              if (cache._shutting_down) {
                  return ss::futurize<ret_t>::convert(
                    rpc::make_error_code(errc::shutting_down));
              }
              if (!cache.contains(node_id)) {
                  // No client available
                  return ss::futurize<ret_t>::convert(
                    rpc::make_error_code(errc::missing_node_rpc_client));
              }
              return ss::do_with(
                cache.get(node_id),
                [connection_timeout = connection_timeout.timeout_at(),
                 f = std::forward<Func>(f)](auto& transport_ptr) mutable {
                    return transport_ptr->get_connected(connection_timeout)
                      .then([f = std::forward<Func>(f)](
                              result<ss::lw_shared_ptr<rpc::transport>>
                                transport) mutable {
                          if (!transport) {
                              // Connection error
                              return ss::futurize<ret_t>::convert(
                                transport.error());
                          }
                          return ss::futurize<ret_t>::convert(
                            f(Protocol(transport.value())));
                      });
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
      model::node_id self, ss::shard_id src_shard, model::node_id node_id) {
        auto shard = rpc::connection_cache::shard_for(self, src_shard, node_id);

        return container().invoke_on(
          shard, [node_id](rpc::connection_cache& cache) mutable {
              if (!cache.contains(node_id)) {
                  // No client available
                  return;
              }
              auto recon_transport = cache.get(node_id);
              recon_transport->reset_backoff();
          });
    }

private:
    std::optional<connection_cache_label> _label;
    mutex _mutex; // to add/remove nodes
    connection_set _cache;
    ss::gate _gate;
    ss::optimized_optional<ss::abort_source::subscription> _as_subscription;
    bool _shutting_down = false;
};
inline ss::shard_id connection_cache::shard_for(
  model::node_id self,
  ss::shard_id src_shard,
  model::node_id n,
  ss::shard_id total_shards) {
    if (ss::smp::count <= 8) {
        return src_shard;
    }
    static const constexpr size_t vnodes = 8;
    /// make deterministic - choose 1 prime to mix node_id with
    /// https://planetmath.org/goodhashtableprimes
    static const constexpr std::array<size_t, vnodes> universe{
      {12582917,
       25165843,
       50331653,
       100663319,
       201326611,
       402653189,
       805306457,
       1610612741}};

    // NOLINTNEXTLINE
    size_t h = universe[jump_consistent_hash(src_shard, vnodes)];
    boost::hash_combine(h, std::hash<model::node_id>{}(n));
    boost::hash_combine(h, std::hash<model::node_id>{}(self));
    // use self node id to shift jump_consistent_hash_assignment
    return jump_consistent_hash(h, total_shards);
}

} // namespace rpc
