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
#include "cluster/partition.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "raft/errc.h"
#include "rpc/backoff_policy.h"
#include "rpc/types.h"
#include "storage/kvstore.h"
#include "vlog.h"

#include <seastar/core/future.hh>

#include <chrono>

namespace detail {

ss::sstring
stm_snapshot_key(const ss::sstring& snapshot_name, const model::ntp& ntp) {
    return ssx::sformat("{}/{}", snapshot_name, ntp);
}

ss::future<> do_move_persistent_stm_state(
  ss::sstring snapshot_name,
  model::ntp ntp,
  ss::shard_id source_shard,
  ss::shard_id target_shard,
  ss::sharded<storage::api>& api) {
    using state_ptr = std::unique_ptr<iobuf>;
    using state_fptr = ss::foreign_ptr<state_ptr>;

    const auto key_as_str = stm_snapshot_key(snapshot_name, ntp);
    bytes key;
    key.append(
      reinterpret_cast<const uint8_t*>(key_as_str.begin()), key_as_str.size());

    state_fptr snapshot = co_await api.invoke_on(
      source_shard, [key](storage::api& api) {
          const auto ks = storage::kvstore::key_space::stms;
          auto snapshot_data = api.kvs().get(ks, key);
          auto snapshot_ptr = !snapshot_data ? nullptr
                                             : std::make_unique<iobuf>(
                                               std::move(*snapshot_data));
          return ss::make_foreign<state_ptr>(std::move(snapshot_ptr));
      });

    if (snapshot) {
        co_await api.invoke_on(
          target_shard,
          [key, snapshot = std::move(snapshot)](storage::api& api) {
              const auto ks = storage::kvstore::key_space::stms;
              return api.kvs().put(ks, key, snapshot->copy());
          });

        co_await api.invoke_on(source_shard, [key](storage::api& api) {
            const auto ks = storage::kvstore::key_space::stms;
            return api.kvs().remove(ks, key);
        });
    }
}

ss::future<> move_persistent_stm_state(
  model::ntp ntp,
  ss::shard_id source_shard,
  ss::shard_id target_shard,
  ss::sharded<storage::api>& api) {
    static const std::vector<ss::sstring> stm_snapshot_names{
      cluster::archival_stm_snapshot,
      cluster::tm_stm_snapshot,
      cluster::id_allocator_snapshot,
      cluster::rm_stm_snapshot};

    return ss::parallel_for_each(
      stm_snapshot_names,
      [ntp, source_shard, target_shard, &api](
        const ss::sstring& snapshot_name) {
          return do_move_persistent_stm_state(
            snapshot_name, ntp, source_shard, target_shard, api);
      });
}

} // namespace detail

namespace cluster {

ss::future<> remove_broker_client(
  model::node_id self,
  ss::sharded<rpc::connection_cache>& clients,
  model::node_id id) {
    return clients.local().remove_broker_client(self, id);
}

ss::future<> update_broker_client(
  model::node_id self,
  ss::sharded<rpc::connection_cache>& clients,
  model::node_id node,
  net::unresolved_address addr,
  config::tls_config tls_config) {
    return clients.local().update_broker_client(
      self, node, std::move(addr), std::move(tls_config));
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
        case rpc::errc::connection_timeout:
            return errc::timeout;
        case rpc::errc::shutting_down:
            return errc::shutting_down;
        case rpc::errc::disconnected_endpoint:
        case rpc::errc::exponential_backoff:
        case rpc::errc::missing_node_rpc_client:
        case rpc::errc::service_error:
        case rpc::errc::method_not_found:
        case rpc::errc::version_not_supported:
        case rpc::errc::unknown:
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

partition_state get_partition_state(ss::lw_shared_ptr<partition> partition) {
    partition_state state{};
    if (unlikely(!partition)) {
        return state;
    }
    state.start_offset = partition->raft_start_offset();
    state.committed_offset = partition->committed_offset();
    state.last_stable_offset = partition->last_stable_offset();
    state.high_water_mark = partition->high_watermark();
    state.dirty_offset = partition->dirty_offset();
    state.latest_configuration_offset
      = partition->get_latest_configuration_offset();
    state.revision_id = partition->get_revision_id();
    state.log_size_bytes = partition->size_bytes();
    state.non_log_disk_size_bytes = partition->non_log_disk_size_bytes();
    state.is_read_replica_mode_enabled
      = partition->is_read_replica_mode_enabled();
    state.is_remote_fetch_enabled = partition->is_remote_fetch_enabled();
    state.is_cloud_data_available = partition->cloud_data_available();
    state.read_replica_bucket = partition->is_read_replica_mode_enabled()
                                  ? partition->get_read_replica_bucket()()
                                  : "";
    if (state.is_cloud_data_available) {
        state.start_cloud_offset = partition->start_cloud_offset();
        state.next_cloud_offset = partition->next_cloud_offset();
    } else {
        state.start_cloud_offset = state.next_cloud_offset = model::offset{-1};
    }
    state.raft_state = get_partition_raft_state(partition->raft());
    return state;
}

partition_raft_state get_partition_raft_state(consensus_ptr ptr) {
    partition_raft_state raft_state{};
    if (unlikely(!ptr)) {
        return raft_state;
    }
    raft_state.node = ptr->self().id();
    raft_state.term = ptr->term();
    raft_state.offset_translator_state = fmt::format(
      "{}", *(ptr->get_offset_translator_state()));
    raft_state.group_configuration = fmt::format("{}", ptr->config());
    raft_state.confirmed_term = ptr->confirmed_term();
    raft_state.flushed_offset = ptr->flushed_offset();
    raft_state.commit_index = ptr->committed_offset();
    raft_state.majority_replicated_index = ptr->majority_replicated_index();
    raft_state.visibility_upper_bound_index
      = ptr->visibility_upper_bound_index();
    raft_state.last_quorum_replicated_index
      = ptr->last_quorum_replicated_index();
    raft_state.last_snapshot_index = ptr->last_snapshot_index();
    raft_state.last_snapshot_term = ptr->last_snapshot_term();
    raft_state.received_snapshot_index = ptr->received_snapshot_index();
    raft_state.received_snapshot_bytes = ptr->received_snapshot_bytes();
    raft_state.has_pending_flushes = ptr->has_pending_flushes();
    raft_state.is_leader = ptr->is_leader();
    raft_state.is_elected_leader = ptr->is_elected_leader();

    const auto& fstats = ptr->get_follower_stats();
    if (ptr->is_elected_leader() && fstats.size() > 0) {
        using follower_state = partition_raft_state::follower_state;
        std::vector<follower_state> followers;
        followers.reserve(fstats.size());
        for (const auto& fstat : fstats) {
            const auto& md = fstat.second;
            follower_state state;
            state.node = md.node_id.id();
            state.last_dirty_log_index = md.last_dirty_log_index;
            state.last_flushed_log_index = md.last_flushed_log_index;
            state.match_index = md.match_index;
            state.next_index = md.next_index;
            state.last_sent_offset = md.last_sent_offset;
            state.heartbeats_failed = md.heartbeats_failed;
            state.is_learner = md.is_learner;
            state.is_recovering = md.is_recovering;
            state.ms_since_last_heartbeat
              = std::chrono::duration_cast<std::chrono::milliseconds>(
                  raft::clock_type::now() - md.last_received_reply_timestamp)
                  .count();
            state.last_sent_seq = md.last_sent_seq;
            state.last_received_seq = md.last_received_seq;
            state.last_successful_received_seq
              = md.last_successful_received_seq;
            state.suppress_heartbeats = md.are_heartbeats_suppressed();
            followers.push_back(std::move(state));
        }
        raft_state.followers = std::move(followers);
    }
    raft_state.stms = get_partition_stm_state(ptr);
    return raft_state;
}

std::vector<partition_stm_state> get_partition_stm_state(consensus_ptr ptr) {
    std::vector<partition_stm_state> result;
    if (unlikely(!ptr) || unlikely(!ptr->log()->stm_manager())) {
        return result;
    }
    const auto& stms = ptr->log()->stm_manager()->stms();
    result.reserve(stms.size());
    for (const auto& stm : stms) {
        partition_stm_state state;
        state.name = stm->name();
        state.last_applied_offset = stm->last_applied();
        state.max_collectible_offset = stm->last_applied();
        result.push_back(std::move(state));
    }
    return result;
}

std::optional<ss::sstring> check_result_configuration(
  const members_table::cache_t& current_brokers,
  const model::broker& new_configuration) {
    auto it = current_brokers.find(new_configuration.id());
    if (it == current_brokers.end()) {
        return fmt::format(
          "broker {} does not exists in list of cluster members",
          new_configuration.id());
    }
    auto& current_configuration = it->second.broker;
    /**
     * do no allow to decrease node core count
     */
    if (
      current_configuration.properties().cores
      > new_configuration.properties().cores) {
        return fmt::format(
          "core count must not decrease on any broker, currently configured "
          "core count: {}, requested core count: {}",
          current_configuration.properties().cores,
          new_configuration.properties().cores);
    }
    /**
     * When cluster member configuration changes Redpanda by default doesn't
     * allow the change if a new cluster configuration would have two
     * listeners pointing to the same address. This was in conflict with the
     * logic of join request which didn't execute validation of resulting
     * configuration. Change the validation logic to only check configuration
     * fields which were changed and are going to be updated.
     */
    const bool rpc_address_changed = current_configuration.rpc_address()
                                     != new_configuration.rpc_address();
    std::vector<model::broker_endpoint> changed_endpoints;
    for (auto& new_ep : new_configuration.kafka_advertised_listeners()) {
        auto it = std::find_if(
          current_configuration.kafka_advertised_listeners().begin(),
          current_configuration.kafka_advertised_listeners().end(),
          [&new_ep](const model::broker_endpoint& ep) {
              return ep.name == new_ep.name;
          });

        if (
          it == current_configuration.kafka_advertised_listeners().end()
          || it->address != new_ep.address) {
            changed_endpoints.push_back(new_ep);
        }
    }

    for (const auto& [id, current] : current_brokers) {
        if (id == new_configuration.id()) {
            continue;
        }

        /**
         * validate if any two of the brokers would listen on the same addresses
         * after applying configuration update
         */
        if (
          rpc_address_changed
          && current.broker.rpc_address() == new_configuration.rpc_address()) {
            // error, nodes would listen on the same rpc addresses
            return fmt::format(
              "duplicate rpc endpoint {} with existing node {}",
              new_configuration.rpc_address(),
              id);
        }

        for (auto& changed_ep : changed_endpoints) {
            auto any_is_the_same = std::any_of(
              current.broker.kafka_advertised_listeners().begin(),
              current.broker.kafka_advertised_listeners().end(),
              [&changed_ep](const model::broker_endpoint& ep) {
                  return changed_ep == ep;
              });
            // error, kafka endpoint would point to the same addresses
            if (any_is_the_same) {
                return fmt::format(
                  "duplicated kafka advertised endpoint {} with existing node "
                  "{}",
                  changed_ep,
                  id);
                ;
            }
        }
    }
    return {};
}
} // namespace cluster
