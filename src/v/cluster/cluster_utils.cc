// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_utils.h"

#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "rpc/backoff_policy.h"
#include "rpc/types.h"
#include "vlog.h"

#include <seastar/core/future.hh>

#include <chrono>

namespace cluster {
patch<broker_ptr> calculate_changed_brokers(
  std::vector<broker_ptr> new_list, std::vector<broker_ptr> old_list) {
    patch<broker_ptr> patch;
    auto compare_by_id = [](const broker_ptr& lhs, const broker_ptr& rhs) {
        return *lhs < *rhs;
    };
    // updated/added brokers
    std::sort(new_list.begin(), new_list.end(), compare_by_id);
    std::sort(old_list.begin(), old_list.end(), compare_by_id);
    std::set_difference(
      std::cbegin(new_list),
      std::cend(new_list),
      std::cbegin(old_list),
      std::cend(old_list),
      std::back_inserter(patch.additions),
      [](const broker_ptr& lhs, const broker_ptr& rhs) {
          return *lhs != *rhs;
      });
    // removed brokers
    std::set_difference(
      std::cbegin(old_list),
      std::cend(old_list),
      std::cbegin(new_list),
      std::cend(new_list),
      std::back_inserter(patch.deletions),
      compare_by_id);

    return patch;
}

std::vector<ss::shard_id>
virtual_nodes(model::node_id self, model::node_id node) {
    std::set<ss::shard_id> owner_shards;
    for (ss::shard_id i = 0; i < ss::smp::count; ++i) {
        auto shard = rpc::connection_cache::shard_for(self, i, node);
        owner_shards.insert(shard);
    }
    return std::vector<ss::shard_id>(owner_shards.begin(), owner_shards.end());
}

ss::future<> remove_broker_client(
  model::node_id self,
  ss::sharded<rpc::connection_cache>& clients,
  model::node_id id) {
    auto shards = virtual_nodes(self, id);
    vlog(clusterlog.debug, "Removing {} TCP client from shards {}", id, shards);
    return ss::do_with(
      std::move(shards), [id, &clients](std::vector<ss::shard_id>& i) {
          return ss::do_for_each(i, [id, &clients](ss::shard_id i) {
              return clients.invoke_on(i, [id](rpc::connection_cache& cache) {
                  return cache.remove(id);
              });
          });
      });
}

ss::future<> maybe_create_tcp_client(
  rpc::connection_cache& cache,
  model::node_id node,
  unresolved_address rpc_address,
  config::tls_config tls_config) {
    return rpc_address.resolve().then([node,
                                       &cache,
                                       tls_config = std::move(tls_config)](
                                        ss::socket_address new_addr) mutable {
        auto f = ss::now();
        if (cache.contains(node)) {
            // client is already there, check if configuration changed
            if (cache.get(node)->server_address() == new_addr) {
                // If configuration did not changed, do nothing
                return f;
            }
            // configuration changed, first remove the client
            f = cache.remove(node);
        }
        // there is no client in cache, create new
        return f.then([&cache,
                       node,
                       new_addr,
                       tls_config = std::move(tls_config)]() mutable {
            return maybe_build_reloadable_certificate_credentials(
                     std::move(tls_config))
              .then([&cache, node, new_addr](
                      ss::shared_ptr<ss::tls::certificate_credentials>&& cert) {
                  return cache.emplace(
                    node,
                    rpc::transport_configuration{
                      .server_addr = new_addr,
                      .credentials = cert,
                      .disable_metrics = rpc::metrics_disabled(
                        config::shard_local_cfg().disable_metrics)},
                    rpc::make_exponential_backoff_policy<rpc::clock_type>(
                      std::chrono::seconds(1), std::chrono::seconds(60)));
              });
        });
    });
}

ss::future<> add_one_tcp_client(
  ss::shard_id owner,
  ss::sharded<rpc::connection_cache>& clients,
  model::node_id node,
  unresolved_address addr,
  config::tls_config tls_config) {
    return clients.invoke_on(
      owner,
      [node, rpc_address = std::move(addr), tls_config = std::move(tls_config)](
        rpc::connection_cache& cache) mutable {
          return maybe_create_tcp_client(
            cache, node, std::move(rpc_address), std::move(tls_config));
      });
}

ss::future<> update_broker_client(
  model::node_id self,
  ss::sharded<rpc::connection_cache>& clients,
  model::node_id node,
  unresolved_address addr,
  config::tls_config tls_config) {
    auto shards = virtual_nodes(self, node);
    vlog(clusterlog.debug, "Adding {} TCP client on shards:{}", node, shards);
    return ss::do_with(
      std::move(shards),
      [&clients, node, addr, tls_config = std::move(tls_config)](
        std::vector<ss::shard_id>& shards) mutable {
          return ss::do_for_each(
            shards,
            [node, addr, &clients, tls_config = std::move(tls_config)](
              ss::shard_id shard) mutable {
                return add_one_tcp_client(
                  shard, clients, node, addr, tls_config);
            });
      });
}

std::vector<topic_result> create_topic_results(
  const std::vector<model::topic_namespace>& topics, errc error_code) {
    std::vector<topic_result> results;
    results.reserve(topics.size());
    std::transform(
      std::cbegin(topics),
      std::cend(topics),
      std::back_inserter(results),
      [error_code](const model::topic_namespace& t) {
          return topic_result(t, error_code);
      });
    return results;
}

model::broker make_self_broker(const config::configuration& cfg) {
    auto kafka_addr = cfg.advertised_kafka_api();
    auto rpc_addr = cfg.advertised_rpc_api();
    return model::broker(
      model::node_id(cfg.node_id),
      kafka_addr,
      rpc_addr,
      cfg.rack,
      // FIXME: Fill broker properties with all the information
      model::broker_properties{.cores = ss::smp::count});
}

void log_certificate_reload_event(
  ss::logger& log,
  const char* system_name,
  const std::unordered_set<ss::sstring>& updated,
  const std::exception_ptr& eptr) {
    if (eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch (...) {
            vlog(
              log.error,
              "{} credentials reload error {}",
              system_name,
              std::current_exception());
        }
    } else {
        for (const auto& name : updated) {
            vlog(
              log.info,
              "{} key or certificate file "
              "updated - {}",
              system_name,
              name);
        }
    }
}

} // namespace cluster
