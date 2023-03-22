// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_cache.h"

#include "cluster/fwd.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timestamp.h"
#include "storage/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>

#include <fmt/format.h>

#include <algorithm>
#include <iterator>
#include <optional>

namespace cluster {

metadata_cache::metadata_cache(
  ss::sharded<topic_table>& tp,
  ss::sharded<members_table>& m,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<health_monitor_frontend>& health_monitor)
  : _topics_state(tp)
  , _members_table(m)
  , _leaders(leaders)
  , _health_monitor(health_monitor) {}

std::vector<model::topic_namespace> metadata_cache::all_topics() const {
    return _topics_state.local().all_topics();
}

void fill_partition_leaders(
  partition_leaders_table& leaders, model::topic_metadata& tp_md) {
    for (auto& p : tp_md.partitions) {
        p.leader_node = leaders.get_leader(tp_md.tp_ns, p.id);
    }
}

std::optional<model::topic>
metadata_cache::get_source_topic(model::topic_namespace_view tp) const {
    auto& topics_map = _topics_state.local().topics_map();
    auto mt = topics_map.find(tp);
    if (mt == topics_map.end() || mt->second.is_topic_replicable()) {
        return std::nullopt;
    }
    return mt->second.get_source_topic();
}

std::optional<cluster::topic_metadata>
metadata_cache::get_topic_metadata(model::topic_namespace_view tp) const {
    return _topics_state.local().get_topic_metadata(tp);
}

std::optional<std::reference_wrapper<const cluster::topic_metadata>>
metadata_cache::get_topic_metadata_ref(model::topic_namespace_view tn) const {
    return _topics_state.local().get_topic_metadata_ref(tn);
}

std::optional<model::topic_metadata> metadata_cache::get_model_topic_metadata(
  model::topic_namespace_view tp, metadata_cache::with_leaders leaders) const {
    auto md = _topics_state.local().get_topic_metadata(tp);
    if (!md) {
        return std::nullopt;
    }

    model::topic_metadata metadata(md->get_configuration().tp_ns);
    metadata.partitions.reserve(md->get_assignments().size());
    for (const auto& p_as : md->get_assignments()) {
        metadata.partitions.push_back(p_as.create_partition_metadata());
    }

    if (leaders) {
        fill_partition_leaders(_leaders.local(), metadata);
    }

    return metadata;
}
std::optional<topic_configuration>
metadata_cache::get_topic_cfg(model::topic_namespace_view tp) const {
    return _topics_state.local().get_topic_cfg(tp);
}

std::optional<model::timestamp_type>
metadata_cache::get_topic_timestamp_type(model::topic_namespace_view tp) const {
    return _topics_state.local().get_topic_timestamp_type(tp);
}

const topic_table::underlying_t& metadata_cache::all_topics_metadata() const {
    return _topics_state.local().all_topics_metadata();
}

std::optional<node_metadata>
metadata_cache::get_node_metadata(model::node_id nid) const {
    return _members_table.local().get_node_metadata(nid);
}

const members_table::cache_t& metadata_cache::nodes() const {
    return _members_table.local().nodes();
}

size_t metadata_cache::node_count() const {
    return _members_table.local().node_count();
}

ss::future<std::vector<node_metadata>> metadata_cache::alive_nodes() const {
    std::vector<node_metadata> brokers;
    auto res = co_await _health_monitor.local().get_nodes_status(
      config::shard_local_cfg().metadata_status_wait_timeout_ms()
      + model::timeout_clock::now());
    if (!res) {
        // if we were not able to refresh the cache, return all brokers
        // (controller may be unreachable)
        co_return _members_table.local().node_list();
    }

    std::set<model::node_id> brokers_with_health;
    for (auto& st : res.value()) {
        brokers_with_health.insert(st.id);
        if (st.is_alive) {
            auto broker = _members_table.local().get_node_metadata(st.id);
            if (broker) {
                brokers.push_back(std::move(*broker));
            }
        }
    }

    // Corner case during node joins:
    // If a node appears in the members table but not in the health report,
    // presume it is newly added and assume it is alive.  This avoids
    // newly added nodes being inconsistently excluded from metadata
    // responses until all nodes' health caches update.
    for (const auto& [id, broker] : _members_table.local().nodes()) {
        if (!brokers_with_health.contains(id)) {
            brokers.push_back(broker);
        }
    }

    co_return !brokers.empty() ? brokers : _members_table.local().node_list();
}

std::vector<node_metadata> metadata_cache::all_nodes() const {
    return _members_table.local().node_list();
}

std::vector<model::node_id> metadata_cache::node_ids() const {
    return _members_table.local().node_ids();
}

bool metadata_cache::should_reject_writes() const {
    return _health_monitor.local().get_cluster_disk_health()
           == storage::disk_space_alert::degraded;
}

bool metadata_cache::contains(
  model::topic_namespace_view tp, const model::partition_id pid) const {
    return _topics_state.local().contains(tp, pid);
}

bool metadata_cache::contains(model::topic_namespace_view tp) const {
    return _topics_state.local().contains(tp);
}

topic_table::topic_state metadata_cache::get_topic_state(
  model::topic_namespace_view tp, model::revision_id id) const {
    return _topics_state.local().get_topic_state(tp, id);
}

ss::future<model::node_id> metadata_cache::get_leader(
  const model::ntp& ntp,
  ss::lowres_clock::time_point tout,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return _leaders.local().wait_for_leader(ntp, tout, as);
}

std::optional<model::node_id>
metadata_cache::get_leader_id(const model::ntp& ntp) const {
    return _leaders.local().get_leader(ntp);
}
std::optional<cluster::leader_term> metadata_cache::get_leader_term(
  model::topic_namespace_view tp_ns, model::partition_id pid) const {
    return _leaders.local().get_leader_term(tp_ns, pid);
}

std::optional<model::node_id> metadata_cache::get_leader_id(
  model::topic_namespace_view tp_ns, model::partition_id p_id) const {
    return _leaders.local().get_leader(tp_ns, p_id);
}

std::optional<model::node_id> metadata_cache::get_previous_leader_id(
  model::topic_namespace_view tp_ns, model::partition_id p_id) const {
    return _leaders.local().get_previous_leader(tp_ns, p_id);
}

/// If present returns a leader of raft0 group
std::optional<model::node_id> metadata_cache::get_controller_leader_id() {
    return _leaders.local().get_leader(model::controller_ntp);
}

void metadata_cache::reset_leaders() { _leaders.local().reset(); }

ss::future<> metadata_cache::refresh_health_monitor() {
    co_await _health_monitor.local().refresh_info();
}

cluster::partition_leaders_table::leaders_info_t
metadata_cache::get_leaders() const {
    return _leaders.local().get_leaders();
}

void metadata_cache::set_is_node_isolated_status(bool is_node_isolated) {
    _is_node_isolated = is_node_isolated;
}

bool metadata_cache::is_node_isolated() { return _is_node_isolated; }

/**
 * hard coded defaults
 */
model::compression metadata_cache::get_default_compression() const {
    return config::shard_local_cfg().log_compression_type();
}
model::cleanup_policy_bitflags
metadata_cache::get_default_cleanup_policy_bitflags() const {
    return config::shard_local_cfg().log_cleanup_policy();
}
model::compaction_strategy
metadata_cache::get_default_compaction_strategy() const {
    return model::compaction_strategy::offset;
}
model::timestamp_type metadata_cache::get_default_timestamp_type() const {
    return config::shard_local_cfg().log_message_timestamp_type();
}
/**
 * We use configuration directly to access default topic properties, in future
 * those values are going to be runtime configurable
 */
size_t metadata_cache::get_default_segment_size() const {
    return config::shard_local_cfg().log_segment_size();
}
size_t metadata_cache::get_default_compacted_topic_segment_size() const {
    return config::shard_local_cfg().compacted_log_segment_size();
}
std::optional<size_t> metadata_cache::get_default_retention_bytes() const {
    return config::shard_local_cfg().retention_bytes();
}
std::optional<std::chrono::milliseconds>
metadata_cache::get_default_retention_duration() const {
    return config::shard_local_cfg().delete_retention_ms();
}
std::optional<size_t>
metadata_cache::get_default_retention_local_target_bytes() const {
    return config::shard_local_cfg().retention_local_target_bytes_default();
}
std::chrono::milliseconds
metadata_cache::get_default_retention_local_target_ms() const {
    return config::shard_local_cfg().retention_local_target_ms_default();
}

uint32_t metadata_cache::get_default_batch_max_bytes() const {
    return config::shard_local_cfg().kafka_batch_max_bytes();
}

model::shadow_indexing_mode
metadata_cache::get_default_shadow_indexing_mode() const {
    model::shadow_indexing_mode m = model::shadow_indexing_mode::disabled;
    if (config::shard_local_cfg().cloud_storage_enable_remote_write()) {
        m = model::shadow_indexing_mode::archival;
    }
    if (config::shard_local_cfg().cloud_storage_enable_remote_read()) {
        m = model::add_shadow_indexing_flag(
          m, model::shadow_indexing_mode::fetch);
    }
    return m;
}

std::optional<std::chrono::milliseconds>
metadata_cache::get_default_segment_ms() const {
    return config::shard_local_cfg().log_segment_ms();
}
topic_properties metadata_cache::get_default_properties() const {
    topic_properties tp;
    tp.compression = {get_default_compression()};
    tp.cleanup_policy_bitflags = {get_default_cleanup_policy_bitflags()};
    tp.compaction_strategy = {get_default_compaction_strategy()};
    tp.timestamp_type = {get_default_timestamp_type()};
    tp.segment_size = {get_default_segment_size()};
    tp.retention_bytes = tristate<size_t>({get_default_retention_bytes()});
    tp.retention_duration = tristate<std::chrono::milliseconds>(
      {get_default_retention_duration()});
    tp.recovery = {false};
    tp.shadow_indexing = {get_default_shadow_indexing_mode()};
    tp.batch_max_bytes = get_default_batch_max_bytes();
    tp.retention_local_target_bytes = tristate{
      get_default_retention_local_target_bytes()};
    tp.retention_local_target_ms = tristate<std::chrono::milliseconds>{
      get_default_retention_local_target_ms()};

    return tp;
}

std::optional<partition_assignment>
metadata_cache::get_partition_assignment(const model::ntp& ntp) const {
    return _topics_state.local().get_partition_assignment(ntp);
}

std::optional<std::vector<model::broker_shard>>
metadata_cache::get_previous_replica_set(const model::ntp& ntp) const {
    return _topics_state.local().get_previous_replica_set(ntp);
}

const topic_table::updates_t& metadata_cache::updates_in_progress() const {
    return _topics_state.local().updates_in_progress();
}

bool metadata_cache::is_update_in_progress(const model::ntp& ntp) const {
    return _topics_state.local().is_update_in_progress(ntp);
}

} // namespace cluster
