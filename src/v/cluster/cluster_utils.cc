// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_utils.h"

#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "raft/errc.h"
#include "rpc/backoff_policy.h"
#include "rpc/types.h"
#include "vlog.h"

#include <seastar/core/future.hh>

#include <chrono>

namespace cluster {

/**
 * Broker patch should included all nodes which properties changed as additions
 * (connections may require update after addresses changed, etc) and nodes that
 * were deleted as deletions. We keep `membership_state` in model::broker. This
 * state must not be included into comparision when calculating added brokers.
 * Previous implementation used equality operator from `model::broker` type
 * which lead to propagating broker added events when their internal state was
 * updated.
 */
patch<broker_ptr> calculate_changed_brokers(
  const std::vector<broker_ptr>& new_list,
  const std::vector<broker_ptr>& old_list) {
    patch<broker_ptr> patch;

    for (auto br : new_list) {
        auto it = std::find_if(
          old_list.begin(), old_list.end(), [&br](const broker_ptr& ptr) {
              // compare only those properties which have to cause update
              return br->id() == ptr->id()
                     && br->kafka_advertised_listeners()
                          == ptr->kafka_advertised_listeners()
                     && br->rpc_address() == ptr->rpc_address()
                     && br->properties() == ptr->properties();
          });

        if (it == old_list.end()) {
            patch.additions.push_back(br);
        }
    }

    for (auto br : old_list) {
        auto it = std::find_if(
          new_list.begin(), new_list.end(), [&br](const broker_ptr& ptr) {
              // it is enough to compare ids since other properties does not
              // influence deletion
              return br->id() == ptr->id();
          });

        if (it == new_list.end()) {
            patch.deletions.push_back(br);
        }
    }

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
  net::unresolved_address rpc_address,
  config::tls_config tls_config) {
    auto f = ss::now();
    if (cache.contains(node)) {
        // client is already there, check if configuration changed
        if (cache.get(node)->server_address() == rpc_address) {
            // If configuration did not changed, do nothing
            return f;
        }
        // configuration changed, first remove the client
        f = cache.remove(node);
    }
    // there is no client in cache, create new
    return f.then([&cache,
                   node,
                   rpc_address = std::move(rpc_address),
                   tls_config = std::move(tls_config)]() mutable {
        return maybe_build_reloadable_certificate_credentials(
                 std::move(tls_config))
          .then(
            [&cache, node, rpc_address = std::move(rpc_address)](
              ss::shared_ptr<ss::tls::certificate_credentials>&& cert) mutable {
                return cache.emplace(
                  node,
                  rpc::transport_configuration{
                    .server_addr = std::move(rpc_address),
                    .credentials = cert,
                    .disable_metrics = net::metrics_disabled(
                      config::shard_local_cfg().disable_metrics),
                    .version = cache.get_default_transport_version()},
                  rpc::make_exponential_backoff_policy<rpc::clock_type>(
                    std::chrono::seconds(1), std::chrono::seconds(15)));
            });
    });
}

ss::future<> add_one_tcp_client(
  ss::shard_id owner,
  ss::sharded<rpc::connection_cache>& clients,
  model::node_id node,
  net::unresolved_address addr,
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
  net::unresolved_address addr,
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

model::broker make_self_broker(const config::node_config& node_cfg) {
    auto kafka_addr = node_cfg.advertised_kafka_api();
    auto rpc_addr = node_cfg.advertised_rpc_api();

    // Calculate memory size
    const auto shard_mem = ss::memory::stats();
    uint64_t total_mem = shard_mem.total_memory() * ss::smp::count;
    // If memory is <1GB, we'll return zero.  That case is already
    // handled when reading this field (e.g. in
    // `partition_allocator::check_cluster_limits`) because earlier redpanda
    // versions always returned zero here.
    uint32_t total_mem_gb = total_mem >> 30;

    // Calculate disk size
    auto space_info = std::filesystem::space(
      config::node().data_directory().path);
    // As for memory, if disk_gb is zero this is handled like a legacy broker.
    uint32_t disk_gb = space_info.capacity >> 30;

    // If this node hasn't been configured with a node ID, use -1 to indicate
    // that we don't yet know it yet. This shouldn't be used during the normal
    // operation of a broker, and instead should be used to indicate a broker
    // that needs to be assigned a node ID when it first starts up.
    model::node_id node_id = node_cfg.node_id() == std::nullopt
                               ? model::unassigned_node_id
                               : *node_cfg.node_id();
    return model::broker(
      node_id,
      kafka_addr,
      rpc_addr,
      node_cfg.rack,
      model::broker_properties{
        // TODO: populate or remote etc_props, mount_paths
        .cores = ss::smp::count,
        .available_memory_gb = total_mem_gb,
        .available_disk_gb = disk_gb});
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

bool has_local_replicas(
  model::node_id self, const std::vector<model::broker_shard>& replicas) {
    return std::find_if(
             std::cbegin(replicas),
             std::cend(replicas),
             [self](const model::broker_shard& bs) {
                 return bs.node_id == self && bs.shard == ss::this_shard_id();
             })
           != replicas.cend();
}

bool are_replica_sets_equal(
  const std::vector<model::broker_shard>& lhs,
  const std::vector<model::broker_shard>& rhs) {
    auto l_sorted = lhs;
    auto r_sorted = rhs;
    static const auto cmp =
      [](const model::broker_shard& lhs, const model::broker_shard& rhs) {
          return lhs.node_id < rhs.node_id;
      };
    std::sort(l_sorted.begin(), l_sorted.end(), cmp);
    std::sort(r_sorted.begin(), r_sorted.end(), cmp);

    return l_sorted == r_sorted;
}

std::vector<custom_assignable_topic_configuration>
without_custom_assignments(std::vector<topic_configuration> topics) {
    std::vector<custom_assignable_topic_configuration> assignable_topics;
    assignable_topics.reserve(topics.size());
    std::transform(
      topics.begin(),
      topics.end(),
      std::back_inserter(assignable_topics),
      [](topic_configuration cfg) {
          return custom_assignable_topic_configuration(std::move(cfg));
      });
    return assignable_topics;
}

cluster::errc map_update_interruption_error_code(std::error_code ec) {
    if (ec.category() == cluster::error_category()) {
        return cluster::errc(ec.value());
    } else if (ec.category() == raft::error_category()) {
        switch (raft::errc(ec.value())) {
        case raft::errc::success:
            return errc::success;
        case raft::errc::not_leader:
            return errc::not_leader_controller;
        case raft::errc::timeout:
            return errc::timeout;
        case raft::errc::disconnected_endpoint:
        case raft::errc::exponential_backoff:
        case raft::errc::non_majority_replication:
        case raft::errc::vote_dispatch_error:
        case raft::errc::append_entries_dispatch_error:
        case raft::errc::replicated_entry_truncated:
        case raft::errc::leader_flush_failed:
        case raft::errc::leader_append_failed:
        case raft::errc::configuration_change_in_progress:
        case raft::errc::node_does_not_exists:
        case raft::errc::leadership_transfer_in_progress:
        case raft::errc::transfer_to_current_leader:
        case raft::errc::node_already_exists:
        case raft::errc::invalid_configuration_update:
        case raft::errc::not_voter:
        case raft::errc::invalid_target_node:
        case raft::errc::shutting_down:
        case raft::errc::replicate_batcher_cache_error:
        case raft::errc::group_not_exists:
        case raft::errc::replicate_first_stage_exception:
            return errc::replication_error;
        }
        __builtin_unreachable();
    } else if (ec.category() == rpc::error_category()) {
        switch (rpc::errc(ec.value())) {
        case rpc::errc::success:
            return errc::success;
        case rpc::errc::client_request_timeout:
            return errc::timeout;
        case rpc::errc::disconnected_endpoint:
        case rpc::errc::exponential_backoff:
        case rpc::errc::missing_node_rpc_client:
        case rpc::errc::service_error:
        case rpc::errc::method_not_found:
        case rpc::errc::version_not_supported:
            return errc::replication_error;
        }
        __builtin_unreachable();
    } else {
        vlog(
          clusterlog.warn,
          "mapping {} error to uknown update interruption error",
          ec.message());
        return errc::unknown_update_interruption_error;
    }
}

partition_allocation_domain
get_allocation_domain(const model::topic_namespace_view tp_ns) {
    if (tp_ns == model::kafka_consumer_offsets_nt) {
        return partition_allocation_domains::consumer_offsets;
    }
    return partition_allocation_domains::common;
}



std::optional<ss::sstring> check_result_configuration(
  const members_table::broker_cache_t& current_brokers,
  const model::broker& to_update) {
    for (const auto& [id, current] : current_brokers) {
        if (id == to_update.id()) {
            /**
             * do no allow to decrease node core count
             */
            if (
              current.broker.properties().cores
              > to_update.properties().cores) {
                return "core count must not decrease on any broker";
            }
            continue;
        }

        /**
         * validate if any two of the brokers would listen on the same addresses
         * after applying configuration update
         */
        if (current.broker.rpc_address() == to_update.rpc_address()) {
            // error, nodes would listen on the same rpc addresses
            return fmt::format(
              "duplicate rpc endpoint {} with existing node {}",
              to_update.rpc_address(),
              id);
        }
        for (auto& current_ep : current.broker.kafka_advertised_listeners()) {
            auto any_is_the_same = std::any_of(
              to_update.kafka_advertised_listeners().begin(),
              to_update.kafka_advertised_listeners().end(),
              [&current_ep](const model::broker_endpoint& ep) {
                  return current_ep == ep;
              });
            // error, kafka endpoint would point to the same addresses
            if (any_is_the_same) {
                return fmt::format(
                  "duplicate kafka advertised endpoint {} with existing node "
                  "{}",
                  current_ep,
                  id);
                ;
            }
        }
    }
    return {};
}
} // namespace cluster
