/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "cluster/fwd.h"
#include "cluster/health_monitor_types.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/scheduling/types.h"
#include "cluster/types.h"
#include "config/replicas_preference.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/metadata.h"

#include <chrono>
#include <functional>

namespace cluster {
class partition_balancer_planner_accessor;

enum ntp_reassignment_type : int8_t { regular, force };

struct ntp_reassignment {
    model::ntp ntp;
    allocated_partition allocated;
    reconfiguration_policy reconfiguration_policy;
    ntp_reassignment_type type;
};

struct planner_config {
    model::partition_autobalancing_mode mode;
    // Used in two places:
    // - If node disk usage goes over this ratio planner will actively move
    // partitions away from the node.
    // - Planner won't plan a move that will result in destination node(s) going
    // over this ratio.
    double max_disk_usage_ratio;
    // Max number of actions that can be scheduled in one planning iteration
    size_t max_concurrent_actions;
    // If a node is unresponsive for more than node_availability_timeout_sec,
    // begin moving partitions off of that node
    std::chrono::seconds node_availability_timeout_sec;
    // If a node is unresponsive for more than decommission timeout, launch a
    // decommission operation against it
    std::chrono::seconds decommission_timeout;
    // If the user manually requested a rebalance (not connected to node
    // addition)
    bool ondemand_rebalance_requested = false;
    // Fallocation step used to calculate upperbound for partition size
    size_t segment_fallocation_step;
    // Threshold for minimum size of partition that is going to be prioritized
    // for movement, partitions with size smaller than threshold will have the
    // lowest priority
    size_t min_partition_size_threshold;
    // Timeout after which node is claimed unresponsive i.e. it doesn't respond
    // the request but it is not yet considered as a violation of partition
    // balancing rules
    std::chrono::milliseconds node_responsiveness_timeout;
    // If true, prioritize balancing topic-wise number of
    // partitions on each node, as opposed to balancing the total number of
    // partitions.
    bool topic_aware = false;
    // Timeout after which a node is considered for automatic decommissioning
    std::optional<std::chrono::seconds> node_autodecommission_timeout;

    // If true, expects nodes to report their space management statistics in the
    // health report.
    bool space_management_enabled = false;
};

class partition_balancer_planner {
public:
    partition_balancer_planner(
      planner_config config,
      partition_balancer_state& state,
      partition_allocator& partition_allocator);

    enum class status {
        empty,
        actions_planned,
        waiting_for_reports,
        missing_sizes,
    };

    struct plan_data {
        partition_balancer_violations violations;
        chunked_vector<ntp_reassignment> reassignments;
        chunked_vector<model::ntp> cancellations;
        chunked_hash_map<model::ntp, reallocation_failure_details>
          reallocation_failures;
        std::optional<model::node_id> maybe_node_to_autodecommission;
        bool counts_rebalancing_finished = false;
        size_t failed_actions_count = 0;
        size_t last_pinning_violations_count = 0;
        status status = status::empty;

        void maybe_add_reallocation_failure();
    };

    ss::future<plan_data>
    plan_actions(const cluster_health_report&, ss::abort_source&);

private:
    class request_context;
    class partition;
    class reassignable_partition;
    class force_reassignable_partition;
    class moving_partition;
    class immutable_partition;

    void init_per_node_state(
      const cluster_health_report&, request_context&, plan_data&);

    ss::future<> init_ntp_sizes_from_health_report(
      const cluster_health_report& health_report, request_context&);
    ss::future<> init_topic_node_counts(request_context&);

    /// Returns a pair of (total, free) bytes on a given node.
    std::pair<uint64_t, uint64_t> get_node_bytes_info(const node::local_state&);

    static ss::future<> get_node_drain_actions(
      request_context&,
      const absl::flat_hash_set<model::node_id>&,
      change_reason reason);

    /// Per-topic state carried from detection into repair. The preference
    /// is copied (not referenced into topic_table) so it survives co_await
    /// points during the repair phase even if the topic is deleted or its
    /// properties change mid-iteration.
    struct topic_pinning_plan {
        config::replicas_preference preference;
        std::vector<uint32_t> ideal;
        absl::btree_set<model::partition_id> violating_partitions;
    };

    /// Result of the detection pass. `count` is the total violating
    /// partitions across all topics; `per_topic` maps each topic with at
    /// least one violation to its precomputed plan. Callers use `any()`
    /// to short-circuit the planner's early-exit predicate without
    /// iterating the map.
    struct pinning_violations {
        bool any() const { return count > 0; }
        size_t count{0};
        chunked_hash_map<model::topic_namespace, topic_pinning_plan> per_topic;
    };

    /// Detection pass: walk the pinned-topics cache, compute the ideal
    /// pinning for each topic, and record every partition whose current
    /// replica set violates it. Synchronous; reads only `state().topics()`,
    /// `state().members()`, and `state().is_rack_awareness_enabled()`.
    /// Safe to call before `init_ntp_sizes_from_health_report` — it does
    /// not inspect partition sizes or the allocator.
    pinning_violations detect_pinning_violations(request_context& ctx);

    static ss::future<> get_rack_constraint_repair_actions(request_context&);
    static ss::future<> get_replica_pinning_repair_actions(
      request_context&, plan_data&, const pinning_violations&);
    /// Logs a warning when the topic's preferred-group capacity is below the
    /// replication factor. Signals a structural (topology vs preference vs RF)
    /// mismatch where pinning cannot be fully satisfied.
    static void warn_if_pinning_capacity_insufficient(
      const model::topic_namespace& tp_ns,
      const config::replicas_preference& pref,
      const std::vector<uint32_t>& capacity_by_rack_group,
      int16_t replication_factor);
    /// Pick the worst replica: highest-group wins, unpreferred beats any
    /// preferred group, last-seen wins on ties. Returns the replica's node id
    /// and its rack group (numeric_limits<uint32_t>::max() if unpreferred).
    static std::pair<model::node_id, uint32_t> find_worst_replica_and_group(
      const std::vector<model::broker_shard>& replicas,
      const config::replicas_preference& pref,
      const members_table& members);
    /// Attempt to move \p worst_replica off its current node to satisfy the
    /// topic's replicas_preference. On failure, records a reallocation
    /// failure. On success, applies an anti-spurious-move guard that reverts
    /// any move which does not strictly improve the replica's pinning group.
    static void try_repair_replica_pinning(
      request_context& ctx,
      reassignable_partition& rpart,
      model::node_id worst_replica,
      uint32_t previous_rack_group,
      const config::replicas_preference& pref,
      const members_table& members);
    static ss::future<> get_full_node_actions(request_context&);
    static ss::future<> get_counts_rebalancing_actions(request_context&);
    static ss::future<> get_force_repair_actions(request_context&);
    static void get_auto_decommission_actions(
      request_context&, const cluster_health_report& health_report);

    static size_t calculate_full_disk_partition_move_priority(
      model::node_id, const reassignable_partition&, const request_context&);

    using node_liveness_ref
      = std::reference_wrapper<const node_liveness_report>;
    struct auto_decom_node_report {
        std::chrono::milliseconds uptime;
        node_liveness_ref liveness_report;
    };

    using auto_decom_report_map
      = absl::flat_hash_map<model::node_id, auto_decom_node_report>;
    struct do_get_auto_decommission_actions_params {
        std::chrono::seconds node_autodecommission_time;
        auto_decom_report_map auto_decom_report_map;
        absl::flat_hash_map<model::node_id, rpc::clock_type::duration>
          node_boot_time_map;
        absl::flat_hash_set<model::node_id> cluster_members;
        absl::flat_hash_set<model::node_id> decommissioning_nodes;
        absl::flat_hash_set<model::node_id> maintenance_mode_nodes;
    };
    static absl::flat_hash_set<model::node_id> do_get_auto_decommission_actions(
      const do_get_auto_decommission_actions_params& params) noexcept;

    // choose a candidate node to decom, or skip
    static std::optional<model::node_id>
    do_postprocess_auto_decommission_actions(
      const absl::flat_hash_set<model::node_id>&
        candidate_nodes_to_decommission,
      const absl::flat_hash_set<model::node_id>& decommissioning_nodes);

    /// Count live nodes per rack from the members table. With rack awareness
    /// enabled, each rack contributes 1 (rack presence); disabled, each rack
    /// contributes the number of nodes it hosts. Callers use this as the
    /// per-tick source of truth for replica-pinning capacity calculations.
    static absl::flat_hash_map<model::rack_id, uint32_t>
    build_rack_node_counts(const members_table& members, bool rack_awareness);

    /// Capacity (node count, per the precomputed map) available in each
    /// priority group of the preference.
    static std::vector<uint32_t> compute_pinning_capacity(
      const config::replicas_preference& pref,
      const absl::flat_hash_map<model::rack_id, uint32_t>& rack_node_counts);

    /// Fill-then-overflow ideal: greedily fill group 0 up to its capacity,
    /// then group 1, etc. Any remaining slots are marked unpreferred via
    /// numeric_limits<uint32_t>::max() sentinels.
    static std::vector<uint32_t> compute_ideal_pinning_assignment(
      size_t replication_factor,
      const std::vector<uint32_t>& capacity_per_group);

    /// True if the sorted per-replica group assignment doesn't match the
    /// fill-then-overflow ideal.
    static bool is_pinning_violated(
      const std::vector<model::broker_shard>& replicas,
      const config::replicas_preference& pref,
      const std::vector<uint32_t>& ideal,
      const members_table& members);

    planner_config _config;
    partition_balancer_state& _state;
    partition_allocator& _partition_allocator;

    friend class ::cluster::partition_balancer_planner_accessor;
};

} // namespace cluster

template<>
struct fmt::formatter<cluster::ntp_reassignment_type> : fmt::formatter<int> {
    auto
    format(cluster::ntp_reassignment_type t, fmt::format_context& ctx) const {
        return fmt::formatter<int>::format(static_cast<int>(t), ctx);
    }
};
