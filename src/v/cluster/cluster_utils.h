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
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/members_table.h"
#include "cluster/types.h"
#include "config/node_config.h"
#include "config/tls_config.h"
#include "rpc/connection_cache.h"
#include "rpc/rpc_utils.h"
#include "rpc/types.h"

#include <seastar/core/sharded.hh>

#include <concepts>
#include <ranges>
#include <system_error>
#include <utility>

namespace detail {

template<std::ranges::range Rng, typename Fn>
requires std::same_as<
  std::invoke_result_t<Fn, std::ranges::range_value_t<Rng>>,
  cluster::topic_result>
std::vector<cluster::topic_result>
make_error_topic_results(const Rng& topics, Fn fn) {
    std::vector<cluster::topic_result> results;
    results.reserve(topics.size());
    std::transform(
      topics.cbegin(), topics.cend(), std::back_inserter(results), fn);
    return results;
}

template<typename T>
concept has_tp_ns = requires {
    {
        std::declval<T>().tp_ns
    } -> std::convertible_to<const model::topic_namespace&>;
};

template<typename T>
const model::topic_namespace& extract_tp_ns(const T& t) {
    if constexpr (std::same_as<T, model::topic_namespace>) {
        return t;
    } else if constexpr (has_tp_ns<T>) {
        return t.tp_ns;
    } else if constexpr (std::same_as<
                           T,
                           cluster::custom_assignable_topic_configuration>) {
        return t.cfg.tp_ns;
    } else {
        static_assert(always_false_v<T>, "couldn't extract tp_ns");
    }
}

} // namespace detail

namespace config {
struct configuration;
}

namespace cluster {

class metadata_cache;
class partition;

/// Creates the same topic_result for all requests
std::vector<topic_result> make_error_topic_results(
  const std::ranges::range auto& topics, errc error_code) {
    return detail::make_error_topic_results(
      topics, [error_code](const auto& t) {
          return topic_result(detail::extract_tp_ns(t), error_code);
      });
}

ss::future<> update_broker_client(
  model::node_id,
  ss::sharded<rpc::connection_cache>&,
  model::node_id node,
  net::unresolved_address addr,
  config::tls_config);

ss::future<> remove_broker_client(
  model::node_id, ss::sharded<rpc::connection_cache>&, model::node_id);

template<typename Proto, typename Func>
requires requires(Func&& f, Proto c) { f(c); }
auto with_client(
  model::node_id self,
  ss::sharded<rpc::connection_cache>& cache,
  model::node_id id,
  net::unresolved_address addr,
  config::tls_config tls_config,
  rpc::clock_type::duration connection_timeout,
  Func&& f) {
    return update_broker_client(
             self, cache, id, std::move(addr), std::move(tls_config))
      .then([id,
             self,
             &cache,
             f = std::forward<Func>(f),
             connection_timeout]() mutable {
          return cache.local().with_node_client<Proto, Func>(
            self,
            ss::this_shard_id(),
            id,
            connection_timeout,
            std::forward<Func>(f));
      });
}

/// Creates current broker instance using its configuration.
model::broker make_self_broker(const config::node_config& node_cfg);

template<typename Proto, typename Func>
requires requires(Func&& f, Proto c) { f(c); }
auto do_with_client_one_shot(
  net::unresolved_address addr,
  config::tls_config tls_config,
  rpc::clock_type::duration connection_timeout,
  rpc::transport_version v,
  Func&& f) {
    return rpc::maybe_build_reloadable_certificate_credentials(
             std::move(tls_config))
      .then([v,
             f = std::forward<Func>(f),
             connection_timeout,
             addr = std::move(addr)](
              ss::shared_ptr<ss::tls::certificate_credentials>&& cert) mutable {
          auto transport = ss::make_lw_shared<rpc::transport>(
            rpc::transport_configuration{
              .server_addr = std::move(addr),
              .credentials = std::move(cert),
              .disable_metrics = net::metrics_disabled(true),
              .version = v});

          return transport->connect(connection_timeout)
            .then([transport, f = std::forward<Func>(f)]() mutable {
                return ss::futurize_invoke(
                  std::forward<Func>(f), Proto(transport));
            })
            .finally([transport] {
                transport->shutdown();
                return transport->stop().finally([transport] {});
            });
      });
}

bool are_replica_sets_equal(
  const std::vector<model::broker_shard>&,
  const std::vector<model::broker_shard>&);

template<typename Cmd>
ss::future<std::error_code> replicate_and_wait(
  ss::sharded<controller_stm>& stm,
  ss::sharded<ss::abort_source>& as,
  Cmd&& cmd,
  model::timeout_clock::time_point timeout,
  std::optional<model::term_id> term = std::nullopt) {
    return stm.invoke_on(
      controller_stm_shard,
      [cmd = std::forward<Cmd>(cmd), term, &as = as, timeout](
        controller_stm& stm) mutable {
          if (!stm.throttle<Cmd>()) {
              return ss::make_ready_future<std::error_code>(
                errc::throttling_quota_exceeded);
          }

          auto b = serde_serialize_cmd(std::forward<Cmd>(cmd));
          return stm.replicate_and_wait(
            std::move(b), timeout, as.local(), term);
      });
}

custom_assignable_topic_configuration_vector
  without_custom_assignments(topic_configuration_vector);

template<class T>
inline std::vector<T>
subtract(const std::vector<T>& lhs, const std::vector<T>& rhs) {
    std::vector<T> ret;
    std::copy_if(
      lhs.begin(), lhs.end(), std::back_inserter(ret), [&rhs](const T& bs) {
          return std::find(rhs.begin(), rhs.end(), bs) == rhs.end();
      });
    return ret;
}

template<class T>
inline std::vector<T>
union_vectors(const std::vector<T>& lhs, const std::vector<T>& rhs) {
    std::vector<T> ret;
    // Inefficient but constant time for small vectors.
    std::copy_if(
      lhs.begin(), lhs.end(), std::back_inserter(ret), [&ret](const T& bs) {
          return std::find(ret.begin(), ret.end(), bs) == ret.end();
      });
    std::copy_if(
      rhs.begin(), rhs.end(), std::back_inserter(ret), [&ret](const T& bs) {
          return std::find(ret.begin(), ret.end(), bs) == ret.end();
      });
    return ret;
}

template<class T>
inline std::vector<T>
intersect(const std::vector<T>& lhs, const std::vector<T>& rhs) {
    std::vector<T> ret;
    ret.reserve(std::min(lhs.size(), rhs.size()));
    // Inefficient but constant time for inputs.
    std::copy_if(
      lhs.begin(), lhs.end(), std::back_inserter(ret), [&](const T& entry) {
          return std::find(rhs.begin(), rhs.end(), entry) != rhs.end();
      });
    return ret;
}

// Checks if lhs is a proper subset of rhs
inline bool is_proper_subset(
  const std::vector<model::broker_shard>& lhs,
  const std::vector<model::broker_shard>& rhs) {
    auto contains_all = std::all_of(
      lhs.begin(), lhs.end(), [&rhs](const auto& current) {
          return std::find(rhs.begin(), rhs.end(), current) != rhs.end();
      });

    return contains_all && rhs.size() > lhs.size();
}

/**
 * Subtracts second replica set from the first one. Result contains only brokers
 * that node_ids are present in the first list but not the other one
 */
template<class T>
requires std::is_same_v<T, model::broker_shard>
         || std::is_same_v<T, model::node_id>
inline std::vector<model::broker_shard> subtract_replica_sets_by_node_id(
  const std::vector<model::broker_shard>& lhs, const std::vector<T>& rhs) {
    std::vector<model::broker_shard> ret;
    std::copy_if(
      lhs.begin(),
      lhs.end(),
      std::back_inserter(ret),
      [&rhs](const model::broker_shard& lhs_bs) {
          return std::find_if(
                   rhs.begin(),
                   rhs.end(),
                   [&lhs_bs](const T& entry) {
                       if constexpr (std::is_same_v<T, model::broker_shard>) {
                           return entry.node_id == lhs_bs.node_id;
                       }
                       return entry == lhs_bs.node_id;
                   })
                 == rhs.end();
      });
    return ret;
}

// check if replica set contains a node
inline bool contains_node(
  const std::vector<model::broker_shard>& replicas, model::node_id id) {
    return std::find_if(
             replicas.begin(),
             replicas.end(),
             [id](const model::broker_shard& bs) { return bs.node_id == id; })
           != replicas.end();
}

inline std::optional<ss::shard_id>
find_shard_on_node(const replicas_t& replicas, model::node_id node) {
    for (const auto& bs : replicas) {
        if (bs.node_id == node) {
            return bs.shard;
        }
    }
    return std::nullopt;
}

/// Calculates expected log revision of a partition with replicas assignment
/// determined by partition_replicas_view on a particular node (if the partition
/// is expected to be there)
std::optional<model::revision_id> log_revision_on_node(
  const topic_table::partition_replicas_view&, model::node_id);

/// Calculates the partition placement target (i.e. log revision and shard id)
/// on a particular node of a partition with replicas assignment determined by
/// partition_replicas_view (including effects of an in-progress or cancelled
/// update if present).
std::optional<shard_placement_target> placement_target_on_node(
  const topic_table::partition_replicas_view&, model::node_id);

// check if replica is moving from node
inline bool moving_from_node(
  model::node_id node,
  const std::vector<model::broker_shard>& previous_replicas,
  const std::vector<model::broker_shard>& result_replicas) {
    if (!contains_node(previous_replicas, node)) {
        return false;
    }
    if (!contains_node(result_replicas, node)) {
        return true;
    }
    return false;
}

// check if replica is moving to node
inline bool moving_to_node(
  model::node_id node,
  const std::vector<model::broker_shard>& previous_replicas,
  const std::vector<model::broker_shard>& result_replicas) {
    if (contains_node(previous_replicas, node)) {
        return false;
    }
    if (contains_node(result_replicas, node)) {
        return true;
    }
    return false;
}

cluster::errc map_update_interruption_error_code(std::error_code);

partition_state get_partition_state(ss::lw_shared_ptr<cluster::partition>);
partition_raft_state get_partition_raft_state(consensus_ptr);
std::vector<partition_stm_state> get_partition_stm_state(consensus_ptr);

/**
 * Check that the configuration is valid, if not return a string with the
 * error cause.
 *
 * @param current_brokers current broker vector
 * @param to_update broker being added
 * @return std::optional<ss::sstring> - present if there is an error, nullopt
 * otherwise
 */
std::optional<ss::sstring> check_result_configuration(
  const members_table::cache_t& current_brokers,
  const model::broker& to_update);

/// Copies the state of all persisted stms from source kvs
ss::future<> copy_persistent_stm_state(
  model::ntp ntp,
  storage::kvstore& source_kvs,
  ss::shard_id target_shard,
  ss::sharded<storage::api>&);

ss::future<> remove_persistent_stm_state(model::ntp ntp, storage::kvstore&);

/// Copies all bits of partition kvstore state from source kvstore to kvstore on
/// target shard.
ss::future<> copy_persistent_state(
  const model::ntp&,
  raft::group_id,
  storage::kvstore& source_kvs,
  ss::shard_id target_shard,
  ss::sharded<storage::api>&);

/// Removes all bits of partition kvstore state in source kvstore.
ss::future<> remove_persistent_state(
  const model::ntp&, raft::group_id, storage::kvstore& source_kvs);

} // namespace cluster
