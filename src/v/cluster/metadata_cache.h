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

#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>

namespace cluster {

/// Metadata cache provides all information required to fill Kafka metadata
/// response. MetadataCache is the facade over cluster state distributed in
/// separate components. The metadata cache core-affinity is independent from
/// the actual state location as the Metadata cache facade, for simplicity, is
/// instantiated on every core. MetadaCache itself does not hold any state
///```plain
///
///   Kafka API                  Kafka Proxy
///       +                           +
///       |                           |
///       |                           |
///       |                           |
///       |                           |
/// +-----v---------------------------v--------+
/// |                                          |
/// |         Metadata Cache (facade)          |
/// |                                          |
/// +---+----------+------------+----------+---+
///     |          |            |          |
/// +---v---+  +---v----+  +----v----+  +--v---+
/// |Members|  | Topics |  | Leaders |  |Health|
/// +-------+  +--------+  +---------+  +------+
class metadata_cache {
public:
    using with_leaders = ss::bool_class<struct with_leaders_tag>;
    metadata_cache(
      ss::sharded<topic_table>&,
      ss::sharded<data_migrations::migrated_resources>&,
      ss::sharded<members_table>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<health_monitor_frontend>&);

    ss::future<> stop() { return ss::now(); }

    /// Returns list of all topics that exists in the cluster.
    std::vector<model::topic_namespace> all_topics() const;

    ///\brief Returns metadata of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<model::topic_metadata> get_model_topic_metadata(
      model::topic_namespace_view, with_leaders = with_leaders::yes) const;
    ///\brief Returns metadata of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<cluster::topic_metadata>
      get_topic_metadata(model::topic_namespace_view) const;

    ///\brief Returns reference to metadata of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    ///
    /// IMPORTANT: remember not to use the reference when its lifetime has to
    /// span across multiple scheduling point. Reference returning method is
    /// provided not to copy metadata object when used by synchronous parts of
    /// code
    std::optional<std::reference_wrapper<const cluster::topic_metadata>>
      get_topic_metadata_ref(model::topic_namespace_view) const;

    ///\brief Returns configuration of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<topic_configuration>
      get_topic_cfg(model::topic_namespace_view) const;

    ///\brief Returns topics timestamp type
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<model::timestamp_type>
      get_topic_timestamp_type(model::topic_namespace_view) const;

    const topic_table::underlying_t& all_topics_metadata() const;

    /// Returns all brokers, returns copy as the content of broker can change
    const members_table::cache_t& nodes() const;

    /// Returns curent broker count
    size_t node_count() const;

    /// Returns all brokers, returns copy as the content of broker can change
    ss::future<std::vector<node_metadata>> alive_nodes() const;

    /// Return all brokers
    std::vector<node_metadata> all_nodes() const;

    /// Returns all broker ids
    std::vector<model::node_id> node_ids() const;

    /// Returns single broker if exists in cache,returns copy as the content of
    /// broker can change
    std::optional<node_metadata> get_node_metadata(model::node_id) const;
    std::optional<model::rack_id> get_node_rack_id(model::node_id) const;

    bool should_reject_writes() const;

    /// Check whether migrations block topic writes/reads
    bool should_reject_reads(model::topic_namespace_view) const;
    bool should_reject_writes(model::topic_namespace_view) const;

    bool contains(model::topic_namespace_view, model::partition_id) const;
    bool contains(model::topic_namespace_view) const;
    topic_table::topic_state
      get_topic_state(model::topic_namespace_view, model::revision_id) const;

    bool contains(const model::ntp& ntp) const {
        return contains(model::topic_namespace_view(ntp), ntp.tp.partition);
    }

    std::optional<model::node_id> get_leader_id(const model::ntp&) const;
    std::optional<cluster::leader_term>
      get_leader_term(model::topic_namespace_view, model::partition_id) const;

    std::optional<model::node_id>
      get_leader_id(model::topic_namespace_view, model::partition_id) const;

    std::optional<model::node_id> get_previous_leader_id(
      model::topic_namespace_view, model::partition_id) const;
    /// Returns metadata of all topics in cache internal format
    // const cache_t& all_metadata() const { return _cache; }

    /**
     * Return the leader of a partition with a timeout.
     *
     * If the partition leader is set then the leader's node id is returned as a
     * ready future. Otherwise, wait up to the specified timeout for a leader to
     * be elected.
     */
    ss::future<model::node_id> get_leader(
      const model::ntp&,
      ss::lowres_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>> = std::nullopt);

    /// If present returns a leader of raft0 group
    std::optional<model::node_id> get_controller_leader_id();

    void reset_leaders();
    ss::future<> refresh_health_monitor();

    /**
     * Get a snapshot of leaders from the partition_leaders_table.
     *
     * @throws if concurrent modification is detected.
     */
    ss::future<cluster::partition_leaders_table::leaders_info_t>
    get_leaders() const;

    void set_is_node_isolated_status(bool is_node_isolated);
    bool is_node_isolated();

    model::compression get_default_compression() const;
    model::cleanup_policy_bitflags get_default_cleanup_policy_bitflags() const;
    model::compaction_strategy get_default_compaction_strategy() const;
    model::timestamp_type get_default_timestamp_type() const;
    size_t get_default_segment_size() const;
    size_t get_default_compacted_topic_segment_size() const;
    std::optional<size_t> get_default_retention_bytes() const;
    std::optional<std::chrono::milliseconds>
    get_default_retention_duration() const;
    std::optional<size_t> get_default_retention_local_target_bytes() const;
    std::chrono::milliseconds get_default_retention_local_target_ms() const;
    std::optional<size_t>
    get_default_initial_retention_local_target_bytes() const;
    std::optional<std::chrono::milliseconds>
    get_default_initial_retention_local_target_ms() const;
    model::shadow_indexing_mode get_default_shadow_indexing_mode() const;
    uint32_t get_default_batch_max_bytes() const;
    std::optional<std::chrono::milliseconds> get_default_segment_ms() const;
    bool get_default_record_key_schema_id_validation() const;
    pandaproxy::schema_registry::subject_name_strategy
    get_default_record_key_subject_name_strategy() const;
    bool get_default_record_value_schema_id_validation() const;
    pandaproxy::schema_registry::subject_name_strategy
    get_default_record_value_subject_name_strategy() const;

    topic_properties get_default_properties() const;
    std::optional<partition_assignment>
    get_partition_assignment(const model::ntp& ntp) const;
    std::optional<std::vector<model::broker_shard>>
    get_previous_replica_set(const model::ntp& ntp) const;
    const topic_table::updates_t& updates_in_progress() const;
    bool is_update_in_progress(const model::ntp& ntp) const;

    bool is_disabled(model::topic_namespace_view, model::partition_id) const;
    const topic_disabled_partitions_set*
      get_topic_disabled_set(model::topic_namespace_view) const;

    std::optional<model::write_caching_mode>
      get_topic_write_caching_mode(model::topic_namespace_view) const;

private:
    ss::sharded<topic_table>& _topics_state;
    ss::sharded<data_migrations::migrated_resources>& _migrated_resources;
    ss::sharded<members_table>& _members_table;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<health_monitor_frontend>& _health_monitor;

    bool _is_node_isolated{false};
};
} // namespace cluster
