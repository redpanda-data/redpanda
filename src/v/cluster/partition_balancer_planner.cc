/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/partition_balancer_planner.h"

#include "base/vlog.h"
#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/node_status_table.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/scheduling/allocation_state.h"
#include "cluster/scheduling/constraints.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/scheduling/types.h"
#include "cluster/types.h"
#include "container/chunked_hash_map.h"
#include "model/namespace.h"
#include "random/generators.h"
#include "ssx/sformat.h"

#include <seastar/core/sstring.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/defer.hh>

#include <functional>
#include <optional>

namespace cluster {

namespace {

hard_constraint
distinct_from(const absl::flat_hash_set<model::node_id>& nodes) {
    class impl : public hard_constraint::impl {
    public:
        explicit impl(const absl::flat_hash_set<model::node_id>& nodes)
          : _nodes(nodes) {}

        hard_constraint_evaluator make_evaluator(
          const allocated_partition&,
          std::optional<model::node_id>) const final {
            return [this](const allocation_node& node) {
                return !_nodes.contains(node.id());
            };
        }

        ss::sstring name() const final {
            return ssx::sformat(
              "distinct from nodes: {}",
              std::vector(_nodes.begin(), _nodes.end()));
        }

    private:
        const absl::flat_hash_set<model::node_id>& _nodes;
    };

    return hard_constraint(std::make_unique<impl>(nodes));
}

} // namespace

partition_balancer_planner::partition_balancer_planner(
  planner_config config,
  partition_balancer_state& state,
  partition_allocator& partition_allocator)
  : _config(config)
  , _state(state)
  , _partition_allocator(partition_allocator) {
    _config.soft_max_disk_usage_ratio = std::min(
      _config.soft_max_disk_usage_ratio, _config.hard_max_disk_usage_ratio);
}

class partition_balancer_planner::request_context {
public:
    std::vector<model::node_id> all_nodes;
    absl::flat_hash_set<model::node_id> all_unavailable_nodes;
    absl::flat_hash_set<model::node_id> timed_out_unavailable_nodes;
    absl::flat_hash_set<model::node_id> decommissioning_nodes;
    absl::flat_hash_map<model::node_id, node_disk_space> node_disk_reports;

    ss::future<> for_each_partition(
      ss::noncopyable_function<ss::stop_iteration(partition&)>);
    ss::future<> for_each_replica_random_order(
      ss::noncopyable_function<ss::stop_iteration(partition&, model::node_id)>);
    ss::future<> with_partition(
      const model::ntp&, ss::noncopyable_function<void(partition&)>);

    const partition_balancer_state& state() const { return _parent._state; }
    partition_balancer_state& state() { return _parent._state; }

    const allocation_state::underlying_t& allocation_nodes() const {
        return _parent._partition_allocator.state().allocation_nodes();
    }

    const planner_config& config() const { return _parent._config; }

    bool can_add_cancellation() const {
        return _force_reassignments.size() + _reassignments.size()
                 + _cancellations.size()
               < config().max_concurrent_actions;
    }

    bool can_add_reassignment() const {
        if (
          state().topics().updates_in_progress().size() + _reassignments.size()
            + _force_reassignments.size()
          >= config().max_concurrent_actions) {
            return false;
        } else {
            return _force_reassignments.size() + _reassignments.size()
                     + _cancellations.size()
                   < config().max_concurrent_actions;
        }
    }

    ss::future<> maybe_yield() {
        co_await ss::coroutine::maybe_yield();
        _as.check();
    }

    static reconfiguration_policy
    map_change_reason_to_policy(change_reason reason) {
        switch (reason) {
        // fast movement
        case change_reason::node_decommissioning:
        case change_reason::partition_count_rebalancing:
            return reconfiguration_policy::target_initial_retention;
        // full retention
        case change_reason::node_unavailable:
        case change_reason::rack_constraint_repair:
        case change_reason::disk_full:
            return reconfiguration_policy::full_local_retention;
        }
    }

    static reconfiguration_policy update_reconfiguration_policy(
      reconfiguration_policy current, change_reason reason) {
        /**
         * If current policy requires full local retention, simply leave it as
         * is
         */
        if (current == reconfiguration_policy::full_local_retention) {
            return current;
        }

        if (
          reason == change_reason::node_unavailable
          || reason == change_reason::rack_constraint_repair) {
            // require more strict reconfiguration policy
            return reconfiguration_policy::full_local_retention;
        }

        return current;
    }

private:
    struct reassignment_info {
        allocated_partition partition;
        reconfiguration_policy reconfiguration_policy;
    };

    friend class partition_balancer_planner;

    request_context(partition_balancer_planner& parent, ss::abort_source& as)
      : _parent(parent)
      , _as(as) {}

    bool all_reports_received() const;

    template<typename Visitor>
    auto do_with_partition(
      const model::ntp& ntp, const partition_assignment& assignment, Visitor&);

    void collect_actions(plan_data&);

    // returns true if the failure can be logged
    bool increment_failure_count();

    void increment_missing_size_count() { _partitions_with_missing_size++; }

    void
    report_decommission_reallocation_failure(model::node_id, const model::ntp&);

    struct partition_sizes {
        absl::flat_hash_map<model::node_id, size_t> current;

        // Max non-reclaimable size of all replicas. This will be used as a size
        // assigned to new replicas (we assume that in the worst case the target
        // replica will be able to reduce the partition size up to this size).
        size_t non_reclaimable = 0;

        size_t get_current(model::node_id id) const {
            auto it = current.find(id);
            if (it != current.end()) {
                return it->second;
            }
            return 0;
        }

        friend std::ostream&
        operator<<(std::ostream& o, const partition_sizes& ps) {
            fmt::print(
              o,
              "{{current: {}, non_reclaimable: {}}}",
              ps.current,
              ps.non_reclaimable);
            return o;
        }
    };

    partition_balancer_planner& _parent;
    chunked_hash_map<model::ntp, partition_sizes> _ntp2sizes;
    chunked_hash_map<
      model::topic_namespace,
      node2count_t,
      model::topic_namespace_hash,
      model::topic_namespace_eq>
      _topic2node_counts;
    absl::node_hash_map<model::ntp, reassignment_info> _reassignments;
    absl::node_hash_map<model::ntp, allocated_partition> _force_reassignments;
    size_t _failed_actions_count = 0;
    // we track missing partition size info separately as it requires force
    // refresh of health report
    size_t _partitions_with_missing_size = 0;
    // Tracks ntps with allocation failures grouped by decommissioning node.
    static constexpr size_t max_ntps_with_reallocation_falures = 25;
    absl::flat_hash_map<model::node_id, absl::btree_set<model::ntp>>
      _decommission_realloc_failures;
    absl::node_hash_set<model::ntp> _cancellations;
    bool _counts_rebalancing_finished = false;
    ss::abort_source& _as;
};

std::pair<uint64_t, uint64_t> partition_balancer_planner::get_node_bytes_info(
  const node::local_state& node_state) {
    const auto& state = node_state.log_data_size;
    vlog(
      clusterlog.debug,
      "getting node disk information from node: {}",
      node_state);

    if (likely(state)) {
        // It is okay to account reclaimable space as free space even through
        // the space is not readily available. During the partition move if the
        // disk is filled up, trimmer cleans up reclaimable space to make room
        // for the move.
        auto target_size = state.value().data_target_size;
        auto current_size = state.value().data_current_size;
        auto reclaimable_size = state.value().data_reclaimable_size;

        auto total_free = 0UL;
        if (likely(target_size > current_size)) {
            // to avoid any underflow with substraction with misreporting
            total_free = target_size - current_size + reclaimable_size;
        } else {
            auto overage = current_size - target_size;
            if (reclaimable_size > overage) {
                total_free = reclaimable_size - overage;
            }
        }

        /**
         * In the initial phase reported space may be incorrect as not all
         * partition storage information are reported. Clamp down to actual
         * disk free space + reclaimable size. This way we will never go beyond
         * the actual free space but when all reported values are up to date
         * the calculated free space should always be smaller then or equal to
         * free space to reclaimable space.
         */
        total_free = std::min(
          reclaimable_size + node_state.data_disk.free, total_free);
        // Clamp to total available target size.
        total_free = std::min(target_size, total_free);
        return std::make_pair(target_size, total_free);
    }
    // This can happen in the following scenarios
    // - During an upgrade - not yet upgradaded health report (temporary state).
    // - No space management configuration in place, fall back to disk usage.
    // - Configuration in place but local monitor has not yet computed the
    //   usage information, eg: right after broker bootstrap.
    return std::make_pair(
      node_state.data_disk.total, node_state.data_disk.free);
}

void partition_balancer_planner::init_per_node_state(
  const cluster_health_report& health_report,
  request_context& ctx,
  plan_data& result) {
    const auto now = rpc::clock_type::now();
    for (const auto& [id, broker] : _state.members().nodes()) {
        if (
          broker.state.get_membership_state()
          == model::membership_state::removed) {
            continue;
        }

        ctx.all_nodes.push_back(id);

        if (
          broker.state.get_membership_state()
          == model::membership_state::draining) {
            vlog(clusterlog.debug, "node {}: decommissioning", id);
            ctx.decommissioning_nodes.insert(id);
        }
        auto node_status = _state.node_status().get_node_status(id);
        // node status is not yet available, wait for it to be updated
        if (!node_status) {
            continue;
        }
        auto time_since_last_seen = now - node_status->last_seen;

        vlog(
          clusterlog.debug,
          "node {}: {} ms since last heartbeat",
          id,
          std::chrono::duration_cast<std::chrono::milliseconds>(
            time_since_last_seen)
            .count());

        if (time_since_last_seen > _config.node_responsiveness_timeout) {
            vlog(
              clusterlog.info,
              "node {} is unresponsive, time since last status reply: {} ms",
              id,
              time_since_last_seen / 1ms);
            ctx.all_unavailable_nodes.insert(id);
        }

        if (time_since_last_seen > _config.node_availability_timeout_sec) {
            ctx.timed_out_unavailable_nodes.insert(id);

            if (
              _config.mode == model::partition_autobalancing_mode::continuous) {
                model::timestamp unavailable_since = model::to_timestamp(
                  model::timestamp_clock::now()
                  - std::chrono::duration_cast<
                    model::timestamp_clock::duration>(time_since_last_seen));

                result.violations.unavailable_nodes.emplace_back(
                  id, unavailable_since);
            }
        }
    }

    for (const auto& node_report : health_report.node_reports) {
        const auto [total, free] = get_node_bytes_info(
          node_report->local_state);
        ctx.node_disk_reports.emplace(
          node_report->id,
          node_disk_space(node_report->id, total, total - free));
    }

    for (model::node_id id : ctx.all_nodes) {
        auto disk_it = ctx.node_disk_reports.find(id);
        if (disk_it == ctx.node_disk_reports.end()) {
            vlog(clusterlog.info, "node {}: no disk report", id);
            continue;
        }

        const auto& disk = disk_it->second;
        double used_space_ratio = disk.original_used_ratio();
        vlog(
          clusterlog.debug,
          "node {}: bytes used: {}, bytes total: {}, used ratio: {:.4}",
          id,
          disk.used,
          disk.total,
          used_space_ratio);

        if (
          _config.mode == model::partition_autobalancing_mode::continuous
          && used_space_ratio > _config.soft_max_disk_usage_ratio) {
            result.violations.full_nodes.emplace_back(
              id, uint32_t(used_space_ratio * 100.0));
        }
    }
}

ss::future<> partition_balancer_planner::init_ntp_sizes_from_health_report(
  const cluster_health_report& health_report, request_context& ctx) {
    for (const auto& node_report : health_report.node_reports) {
        for (const auto& [tp_ns, partitions] : node_report->topics) {
            for (const auto& partition : partitions) {
                model::ntp ntp{tp_ns.ns, tp_ns.tp, partition.id};
                size_t reclaimable = partition.reclaimable_size_bytes.value_or(
                  0);
                vlog(
                  clusterlog.trace,
                  "ntp {} on node {}: size {}, reclaimable: {}",
                  ntp,
                  node_report->id,
                  human::bytes(partition.size_bytes),
                  human::bytes(reclaimable));

                auto& sizes = ctx._ntp2sizes[ntp];
                sizes.current[node_report->id] = partition.size_bytes;

                size_t non_reclaimable = 0;
                if (reclaimable < partition.size_bytes) {
                    non_reclaimable = partition.size_bytes - reclaimable;
                } else {
                    // This is a bit weird, assume that the partition can be
                    // reduced to 1 empty segment.
                    non_reclaimable = ctx.config().segment_fallocation_step;
                }

                // assume that the "true" non-reclaimable size is the max of all
                // replicas.
                sizes.non_reclaimable = std::max(
                  sizes.non_reclaimable, non_reclaimable);
            }
            co_await ctx.maybe_yield();
        }
    }

    // Add moving partitions contribution to batch size and node disk sizes.
    const auto& in_progress_updates = _state.topics().updates_in_progress();
    for (const auto& [ntp, update] : in_progress_updates) {
        auto size_it = ctx._ntp2sizes.find(ntp);
        if (size_it == ctx._ntp2sizes.end()) {
            continue;
        }
        const auto& sizes = size_it->second;

        auto moving_from = subtract(
          update.get_previous_replicas(), update.get_target_replicas());
        auto moving_to = subtract(
          update.get_target_replicas(), update.get_previous_replicas());

        switch (update.get_state()) {
        case reconfiguration_state::in_progress:
        case reconfiguration_state::force_update:
            for (const auto& bs : moving_from) {
                auto node_it = ctx.node_disk_reports.find(bs.node_id);
                if (node_it != ctx.node_disk_reports.end()) {
                    node_it->second.released += sizes.get_current(bs.node_id);
                }
            }

            for (const auto& bs : moving_to) {
                auto node_it = ctx.node_disk_reports.find(bs.node_id);
                if (node_it != ctx.node_disk_reports.end()) {
                    size_t current = sizes.get_current(bs.node_id);
                    if (current < sizes.non_reclaimable) {
                        node_it->second.assigned
                          += (sizes.non_reclaimable - current);
                    }
                }
            }

            break;
        case reconfiguration_state::cancelled:
        case reconfiguration_state::force_cancelled:
            for (const auto& bs : moving_to) {
                auto node_it = ctx.node_disk_reports.find(bs.node_id);
                if (node_it != ctx.node_disk_reports.end()) {
                    node_it->second.released += sizes.get_current(bs.node_id);
                }
            }
            break;
        }
    }

    for (const auto& [id, disk] : ctx.node_disk_reports) {
        vlog(
          clusterlog.trace,
          "after processing in-progress updates, node id {} disk: {}",
          id,
          disk);
    }
}

ss::future<>
partition_balancer_planner::init_topic_node_counts(request_context& ctx) {
    const auto& topics = _state.topics();
    for (auto it = topics.topics_iterator_begin();
         it != topics.topics_iterator_end();
         ++it) {
        auto& counts = ctx._topic2node_counts[it->first];
        const auto& assignments = it->second.get_assignments();
        for (const auto& [_, p_as] : assignments) {
            for (const auto& bs : p_as.replicas) {
                counts[bs.node_id] += 1;
            }
            // NOTE: we can't use ssx::async_for_each_counter here, as there
            // would be no way to call it.check() immediately after maybe_yield
            // (and before we check the range bounds).
            co_await ss::coroutine::maybe_yield();
            it.check();
        }
    }
}

bool partition_balancer_planner::request_context::all_reports_received() const {
    for (auto id : all_nodes) {
        if (
          !all_unavailable_nodes.contains(id)
          && !node_disk_reports.contains(id)) {
            return false;
        }
    }
    return true;
}

bool partition_balancer_planner::request_context::increment_failure_count() {
    static constexpr size_t max_logged_failures = 50;

    ++_failed_actions_count;
    if (_failed_actions_count <= max_logged_failures) {
        return true;
    } else if (_failed_actions_count == max_logged_failures + 1) {
        vlog(
          clusterlog.info,
          "too many balancing action failures, won't log anymore");
        return false;
    } else {
        return false;
    }
}

void partition_balancer_planner::request_context::
  report_decommission_reallocation_failure(
    model::node_id node, const model::ntp& ntp) {
    auto& ntps = _decommission_realloc_failures.try_emplace(node).first->second;
    if (ntps.size() < max_ntps_with_reallocation_falures) {
        ntps.emplace(ntp);
    }
}

static bool has_quorum(
  const absl::flat_hash_set<model::node_id>& all_unavailable_nodes,
  const std::vector<model::broker_shard>& current_replicas) {
    // Check that nodes quorum is available
    size_t available_nodes_amount = std::count_if(
      current_replicas.begin(),
      current_replicas.end(),
      [&](const model::broker_shard& bs) {
          return !all_unavailable_nodes.contains(bs.node_id);
      });
    if (available_nodes_amount * 2 < current_replicas.size()) {
        return false;
    }
    return true;
}

class partition_balancer_planner::reassignable_partition {
public:
    const model::ntp& ntp() const { return _ntp; }
    const std::vector<model::broker_shard>& replicas() const {
        return (
          _reallocated ? _reallocated->partition.replicas() : _orig_replicas);
    };

    bool is_original(model::node_id replica) const {
        if (_reallocated) {
            return _reallocated->partition.is_original(replica);
        } else {
            return contains_node(_orig_replicas, replica);
        }
    }

    const request_context::partition_sizes& sizes() const { return _sizes; }

    result<reallocation_step> move_replica(
      model::node_id replica,
      double max_disk_usage_ratio,
      partition_balancer_planner::change_reason reason);

    void revert(const reallocation_step&);

private:
    friend class request_context;

    reassignable_partition(
      model::ntp ntp,
      const request_context::partition_sizes& sizes,
      std::optional<request_context::reassignment_info> reallocated,
      const std::vector<model::broker_shard>& orig_replicas,
      request_context& ctx)
      : _ntp(std::move(ntp))
      , _sizes(sizes)
      , _reallocated(std::move(reallocated))
      , _orig_replicas(orig_replicas)
      , _ctx(ctx) {}

    bool has_changes() const {
        return _reallocated && _reallocated->partition.has_changes();
    }

    allocation_constraints
    get_allocation_constraints(double max_disk_usage_ratio) const;

private:
    model::ntp _ntp;
    const request_context::partition_sizes& _sizes;
    std::optional<request_context::reassignment_info> _reallocated;
    const std::vector<model::broker_shard>& _orig_replicas;
    request_context& _ctx;
};

class partition_balancer_planner::force_reassignable_partition {
public:
    const model::ntp& ntp() const { return _ntp; }
    const std::vector<model::broker_shard>& replicas() const {
        return _original_assignment.replicas;
    };

    void force_move_dead_replicas(double max_disk_usage_ratio);

private:
    friend class request_context;

    force_reassignable_partition(
      model::ntp ntp,
      std::optional<request_context::partition_sizes> sizes,
      const partition_assignment& assignment,
      std::vector<model::node_id> nodes_to_remove,
      request_context& ctx)
      : _ntp(std::move(ntp))
      , _sizes(std::move(sizes))
      , _original_assignment(assignment)
      , _nodes_to_remove(nodes_to_remove.begin(), nodes_to_remove.end())
      , _ctx(ctx) {}

    allocation_constraints
    get_allocation_constraints(double max_disk_usage_ratio) const;

    model::ntp _ntp;
    std::optional<request_context::partition_sizes> _sizes;
    const partition_assignment& _original_assignment;
    absl::flat_hash_set<model::node_id> _nodes_to_remove;
    request_context& _ctx;
    result<allocated_partition> _reallocation = errc::allocation_error;
};

class partition_balancer_planner::moving_partition {
public:
    const model::ntp& ntp() const { return _ntp; }
    const std::vector<model::broker_shard>& replicas() const {
        return (_cancel_requested ? _orig_replicas : _replicas);
    }

    const std::vector<model::broker_shard>& orig_replicas() const {
        return _orig_replicas;
    }

    bool cancel_requested() const { return _cancel_requested; }

    void request_cancel(partition_balancer_planner::change_reason reason) {
        if (!_cancel_requested) {
            vlog(
              clusterlog.info,
              "ntp: {}, cancelling move {} -> {}, reason: {}",
              ntp(),
              orig_replicas(),
              replicas(),
              reason);

            _ctx._cancellations.insert(_ntp);
            _cancel_requested = true;

            auto moving_to = subtract(_replicas, _orig_replicas);
            auto moving_from = subtract(_orig_replicas, _replicas);

            // adjust topic node counts

            auto& node_counts = _ctx._topic2node_counts.at(
              model::topic_namespace_view(ntp()));
            for (const auto& bs : moving_to) {
                auto& count = node_counts.at(bs.node_id);
                count -= 1;
                if (count == 0) {
                    node_counts.erase(bs.node_id);
                }
            }
            for (const auto& bs : moving_from) {
                node_counts[bs.node_id] += 1;
            }

            // Adjust partition contribution to final disk space
            auto sizes_it = _ctx._ntp2sizes.find(_ntp);
            if (sizes_it != _ctx._ntp2sizes.end()) {
                const auto& sizes = sizes_it->second;
                for (const auto& bs : moving_to) {
                    auto node_it = _ctx.node_disk_reports.find(bs.node_id);
                    if (node_it != _ctx.node_disk_reports.end()) {
                        node_it->second.released += sizes.get_current(
                          bs.node_id);
                        vlog(
                          clusterlog.trace,
                          "after cancelling, node id {} disk: {}",
                          node_it->first,
                          node_it->second);
                    }
                }
            }

            // TODO: adjust contribution to final partition counts
        }
    }

    void report_failure(
      std::string_view reason,
      partition_balancer_planner::change_reason change_reason) {
        if (_ctx.increment_failure_count()) {
            vlog(
              clusterlog.info,
              "[ntp {}, replicas: {}]: can't change replicas with "
              "cancellation: {} (change reason: {})",
              _ntp,
              _replicas,
              reason,
              change_reason);
        }
    }

private:
    friend class request_context;

    moving_partition(
      model::ntp ntp,
      const std::vector<model::broker_shard>& replicas,
      const std::vector<model::broker_shard>& orig_replicas,
      request_context& ctx)
      : _ntp(std::move(ntp))
      , _replicas(replicas)
      , _orig_replicas(orig_replicas)
      , _cancel_requested(ctx._cancellations.contains(_ntp))
      , _ctx(ctx) {}

private:
    model::ntp _ntp;
    const std::vector<model::broker_shard>& _replicas;
    const std::vector<model::broker_shard>& _orig_replicas;
    bool _cancel_requested;
    request_context& _ctx;
};

/// Partition that we for some reason cannot do anything about.
class partition_balancer_planner::immutable_partition {
public:
    const model::ntp& ntp() const { return _ntp; }
    const std::vector<model::broker_shard>& replicas() const {
        return _replicas;
    }

    enum class immutability_reason {
        // not enough replicas on live nodes, reassignment unlikely to succeed
        no_quorum,
        // no partition size information
        no_size_info,
        // partition reconfiguration
        reconfiguration_state,
        // can't add more actions
        batch_full,
        // disabled by user
        disabled,
    };

    immutability_reason reason() const { return _reason; }

    void
    report_failure(partition_balancer_planner::change_reason change_reason) {
        ss::sstring reason;
        switch (_reason) {
        case immutability_reason::batch_full:
            // don't log full batch failures (if the batch is full, we are
            // not stalling), and do not increment failure count
            return;
        case immutability_reason::no_quorum:
            reason = "no raft quorum";
            break;
        case immutability_reason::no_size_info:
            reason = "partition size information unavailable";
            _ctx.increment_missing_size_count();
            break;
        case immutability_reason::reconfiguration_state:
            reason = ssx::sformat(
              "reconfiguration in progress, state: {}", _reconfiguration_state);
            break;
        case immutability_reason::disabled:
            reason = "partition disabled by user";
            break;
        }

        const bool can_log = _ctx.increment_failure_count();
        if (!can_log) {
            return;
        }

        vlog(
          clusterlog.info,
          "[ntp {}, replicas: {}]: can't change replicas: {} (change "
          "reason: {})",
          _ntp,
          _replicas,
          reason,
          change_reason);
    }

private:
    friend class request_context;

    immutable_partition(
      model::ntp ntp,
      const std::vector<model::broker_shard>& replicas,
      immutability_reason reason,
      std::optional<reconfiguration_state> state,
      request_context& ctx)
      : _ntp(std::move(ntp))
      , _replicas(replicas)
      , _reason(reason)
      , _reconfiguration_state(state)
      , _ctx(ctx) {}

private:
    model::ntp _ntp;
    const std::vector<model::broker_shard>& _replicas;
    immutability_reason _reason;
    std::optional<reconfiguration_state> _reconfiguration_state;
    request_context& _ctx;
};

class partition_balancer_planner::partition {
public:
    const model::ntp& ntp() const {
        return std::visit(
          [](const auto& p) -> const model::ntp& { return p.ntp(); }, _variant);
    }

    const std::vector<model::broker_shard>& replicas() const {
        return std::visit(
          [](const auto& p) -> const std::vector<model::broker_shard>& {
              return p.replicas();
          },
          _variant);
    }

    template<typename... Visitors>
    auto match_variant(Visitors&&... vs) {
        return ss::visit(_variant, std::forward<Visitors>(vs)...);
    }

private:
    friend class partition_balancer_planner::request_context;

    template<typename T>
    partition(T&& variant)
      : _variant(std::forward<T>(variant)) {}

    std::variant<
      reassignable_partition,
      force_reassignable_partition,
      moving_partition,
      immutable_partition>
      _variant;
};

template<typename Visitor>
auto partition_balancer_planner::request_context::do_with_partition(
  const model::ntp& ntp,
  const partition_assignment& assignment,
  Visitor& visitor) {
    const bool is_disabled = _parent._state.topics().is_disabled(ntp);
    const auto& orig_replicas = assignment.replicas;
    auto in_progress_it = _parent._state.topics().updates_in_progress().find(
      ntp);
    if (in_progress_it != _parent._state.topics().updates_in_progress().end()) {
        const auto& replicas = in_progress_it->second.get_target_replicas();
        const auto& orig_replicas
          = in_progress_it->second.get_previous_replicas();
        auto state = in_progress_it->second.get_state();

        if (is_disabled) {
            partition part{immutable_partition{
              ntp,
              replicas,
              immutable_partition::immutability_reason::disabled,
              state,
              *this}};
            return visitor(part);
        }

        if (state == reconfiguration_state::in_progress) {
            if (can_add_cancellation()) {
                partition part{
                  moving_partition{ntp, replicas, orig_replicas, *this}};
                return visitor(part);
            } else {
                partition part{immutable_partition{
                  ntp,
                  replicas,
                  immutable_partition::immutability_reason::batch_full,
                  state,
                  *this}};
                return visitor(part);
            }
        } else {
            partition part{immutable_partition{
              ntp,
              replicas,
              immutable_partition::immutability_reason::reconfiguration_state,
              state,
              *this}};
            return visitor(part);
        }
    }

    if (is_disabled) {
        partition part{immutable_partition{
          ntp,
          orig_replicas,
          immutable_partition::immutability_reason::disabled,
          std::nullopt,
          *this}};
        return visitor(part);
    }

    auto reassignment_it = _reassignments.find(ntp);

    if (reassignment_it == _reassignments.end() && !can_add_reassignment()) {
        partition part{immutable_partition{
          ntp,
          orig_replicas,
          immutable_partition::immutability_reason::batch_full,
          std::nullopt,
          *this}};
        return visitor(part);
    }

    // check if the ntp is to be force reconfigured.
    auto topic_md = _parent._state.topics().get_topic_metadata_ref(
      model::topic_namespace_view{ntp});
    const auto& force_reconfigurable_partitions
      = _parent._state.topics().partitions_to_force_recover();
    auto force_it = force_reconfigurable_partitions.find(ntp);
    if (topic_md && force_it != force_reconfigurable_partitions.end()) {
        auto topic_revision = topic_md.value().get().get_revision();
        const auto& entries = force_it->second;
        auto it = std::find_if(
          entries.begin(), entries.end(), [&](const auto& entry) {
              return entry.topic_revision == topic_revision
                     && are_replica_sets_equal(entry.assignment, orig_replicas);
          });

        if (it != entries.end()) {
            auto size_it = _ntp2sizes.find(ntp);
            std::optional<request_context::partition_sizes> sizes;
            if (size_it != _ntp2sizes.end()) {
                sizes = size_it->second;
            }
            partition part{force_reassignable_partition{
              ntp, sizes, assignment, it->dead_nodes, *this}};

            auto deferred = ss::defer([&] {
                auto& force_reassignable
                  = std::get<force_reassignable_partition>(part._variant);
                if (force_reassignable._reallocation) {
                    _force_reassignments.emplace(
                      ntp, std::move(force_reassignable._reallocation.value()));
                }
            });
            return visitor(part);
        }
    };

    auto size_it = _ntp2sizes.find(ntp);
    if (size_it == _ntp2sizes.end()) {
        partition part{immutable_partition{
          ntp,
          orig_replicas,
          immutable_partition::immutability_reason::no_size_info,
          std::nullopt,
          *this}};
        return visitor(part);
    }

    if (!has_quorum(all_unavailable_nodes, orig_replicas)) {
        partition part{immutable_partition{
          ntp,
          orig_replicas,
          immutable_partition::immutability_reason::no_quorum,
          std::nullopt,
          *this}};
        return visitor(part);
    }

    std::optional<reassignment_info> reallocated;
    if (reassignment_it != _reassignments.end()) {
        // borrow the allocated_partition object
        reallocated = std::move(reassignment_it->second);
    }

    partition part{reassignable_partition{
      ntp, size_it->second, std::move(reallocated), orig_replicas, *this}};
    auto deferred = ss::defer([&] {
        auto& reassignable = std::get<reassignable_partition>(part._variant);
        // insert or return part._reallocated to reassignments
        if (reassignable.has_changes()) {
            if (reassignment_it != _reassignments.end()) {
                reassignment_it->second = std::move(*reassignable._reallocated);
            } else {
                _reassignments.emplace(
                  ntp, std::move(*reassignable._reallocated));
            }
        } else if (reassignment_it != _reassignments.end()) {
            // We no longer need to reassign this partition (presumably due to
            // revert)
            _reassignments.erase(reassignment_it);
        }
    });

    return visitor(part);
}

ss::future<> partition_balancer_planner::request_context::for_each_partition(
  ss::noncopyable_function<ss::stop_iteration(partition&)> visitor) {
    const auto& topics = _parent._state.topics();
    for (auto it = topics.topics_iterator_begin();
         it != topics.topics_iterator_end();
         ++it) {
        const auto& assignments = it->second.get_assignments();
        for (const auto& [_, assignment] : assignments) {
            auto ntp = model::ntp(it->first.ns, it->first.tp, assignment.id);
            auto stop = do_with_partition(ntp, assignment, visitor);
            if (stop == ss::stop_iteration::yes) {
                co_return;
            }
            co_await maybe_yield();
            it.check();
        }
    }
}

ss::future<>
partition_balancer_planner::request_context::for_each_replica_random_order(
  ss::noncopyable_function<ss::stop_iteration(partition&, model::node_id)>
    visitor) {
    auto start_rev = state().topics().topics_map_revision();

    struct item {
        const model::topic_namespace* tp_ns;
        const partition_assignment* assignment;
        model::node_id node;
    };

    fragmented_vector<item> replicas;
    for (const auto& t : _parent._state.topics().topics_map()) {
        for (const auto& [_, a] : t.second.get_assignments()) {
            auto reassignment_it = _reassignments.find(
              model::ntp(t.first.ns, t.first.tp, a.id));
            const auto& ntp_replicas
              = reassignment_it != _reassignments.end()
                  ? reassignment_it->second.partition.replicas()
                  : a.replicas;

            for (const auto& bs : ntp_replicas) {
                replicas.push_back(item{
                  .tp_ns = &t.first, .assignment = &a, .node = bs.node_id});
            }
            co_await maybe_yield();
            state().topics().check_topics_map_stable(start_rev);
        }
    }

    std::shuffle(
      replicas.begin(), replicas.end(), random_generators::internal::gen);

    for (const auto& repl : replicas) {
        state().topics().check_topics_map_stable(start_rev);
        model::ntp ntp(repl.tp_ns->ns, repl.tp_ns->tp, repl.assignment->id);
        auto part_visitor = [&visitor, node = repl.node](partition& part) {
            return visitor(part, node);
        };
        auto stop = do_with_partition(
          std::move(ntp), *(repl.assignment), part_visitor);
        if (stop == ss::stop_iteration::yes) {
            co_return;
        }
        co_await maybe_yield();
    }
}

ss::future<> partition_balancer_planner::request_context::with_partition(
  const model::ntp& ntp, ss::noncopyable_function<void(partition&)> visitor) {
    auto topic = model::topic_namespace_view(ntp);
    auto topic_meta = _parent._state.topics().get_topic_metadata_ref(topic);
    if (!topic_meta) {
        vlog(clusterlog.warn, "topic {} not found", topic);
        co_return;
    }
    auto it = topic_meta->get().get_assignments().find(ntp.tp.partition);
    if (it == topic_meta->get().get_assignments().end()) {
        vlog(
          clusterlog.warn,
          "partition {} of topic {} not found",
          ntp.tp.partition,
          topic);
        co_return;
    }

    do_with_partition(ntp, it->second, visitor);
}

allocation_constraints
partition_balancer_planner::reassignable_partition::get_allocation_constraints(
  double max_disk_usage_ratio) const {
    allocation_constraints constraints;

    // hard constraints

    // Add constraint on partition max_disk_usage_ratio overfill
    size_t upper_bound_for_partition_size
      = _sizes.non_reclaimable + _ctx.config().segment_fallocation_step;
    constraints.add(disk_not_overflowed_by_partition(
      max_disk_usage_ratio,
      upper_bound_for_partition_size,
      _ctx.node_disk_reports));

    // Add constraint on unavailable nodes
    if (!_ctx.timed_out_unavailable_nodes.empty()) {
        constraints.add(distinct_from(_ctx.timed_out_unavailable_nodes));
    }

    // Add constraint on decommissioning nodes
    if (!_ctx.decommissioning_nodes.empty()) {
        constraints.add(distinct_from(_ctx.decommissioning_nodes));
    }

    // soft constraints

    if (_ctx.config().topic_aware) {
        // Add constraint for balanced topic-wise replica counts
        constraints.add(min_count_in_map(
          "min topic-wise count",
          _ctx._topic2node_counts.at(model::topic_namespace_view(ntp()))));
    }

    // Add constraint for balanced total replica counts
    constraints.ensure_new_level();
    constraints.add(max_final_capacity());

    // Add constraint on least disk usage
    constraints.ensure_new_level();
    constraints.add(least_disk_filled(
      max_disk_usage_ratio,
      upper_bound_for_partition_size,
      _ctx.node_disk_reports));

    return constraints;
}

result<reallocation_step>
partition_balancer_planner::reassignable_partition::move_replica(
  model::node_id replica,
  double max_disk_usage_ratio,
  partition_balancer_planner::change_reason reason) {
    if (!_reallocated) {
        _reallocated = request_context::reassignment_info{
          .partition = _ctx._parent._partition_allocator
                         .make_allocated_partition(_ntp, replicas()),
          .reconfiguration_policy
          = request_context::map_change_reason_to_policy(reason)};
    }

    // Verify that we are moving only original replicas. This assumption
    // simplifies the code considerably (although in the future nothing stops us
    // from supporting moving already moved replicas several times).
    vassert(
      _reallocated->partition.is_original(replica),
      "ntp {}: trying to move replica {} which was already reassigned earlier",
      _ntp,
      replica);

    auto constraints = get_allocation_constraints(max_disk_usage_ratio);
    auto moved = _ctx._parent._partition_allocator.reallocate_replica(
      _reallocated->partition, replica, std::move(constraints));
    if (!moved) {
        if (_ctx.increment_failure_count()) {
            vlog(
              clusterlog.info,
              "ntp {} (sizes: {}, current replicas: {}): "
              "attempt to move replica {} (reason: {}) failed, error: {}",
              _ntp,
              _sizes,
              replicas(),
              replica,
              reason,
              moved.error().message());
        }
        return moved;
    }

    model::node_id new_node = moved.value().current().node_id;
    if (new_node != replica) {
        vlog(
          clusterlog.info,
          "ntp {} (sizes: {}, orig replicas: {}): "
          "scheduling replica move {} -> {}, reason: {}",
          _ntp,
          _sizes,
          _orig_replicas,
          replica,
          new_node,
          reason);

        /**
         * Reallocation may require policy update as previous reason might have
         * been different.
         */
        _reallocated->reconfiguration_policy
          = request_context::update_reconfiguration_policy(
            _reallocated->reconfiguration_policy, reason);

        {
            // adjust topic node counts
            auto& node_counts = _ctx._topic2node_counts.at(
              model::topic_namespace_view(ntp()));
            auto& prev_count = node_counts.at(replica);
            prev_count -= 1;
            if (prev_count == 0) {
                node_counts.erase(replica);
            }
            node_counts[new_node] += 1;
        }

        auto from_it = _ctx.node_disk_reports.find(replica);
        if (from_it != _ctx.node_disk_reports.end()) {
            from_it->second.released += _sizes.get_current(replica);

            vlog(
              clusterlog.trace,
              "after scheduling move, node id {} disk: {}",
              from_it->first,
              from_it->second);
        }

        auto to_it = _ctx.node_disk_reports.find(new_node);
        if (to_it != _ctx.node_disk_reports.end()) {
            if (_reallocated->partition.is_original(new_node)) {
                to_it->second.released -= _sizes.get_current(new_node);
            } else {
                to_it->second.assigned += _sizes.non_reclaimable;
            }

            vlog(
              clusterlog.trace,
              "after scheduling move, node id {} disk: {}",
              to_it->first,
              to_it->second);
        }
    }

    return moved;
}

void partition_balancer_planner::reassignable_partition::revert(
  const reallocation_step& move) {
    vassert(_reallocated, "ntp {}: _reallocated must be present", _ntp);
    vassert(
      move.previous(),
      "ntp {}: trying to revert move without previous replica",
      _ntp);
    vassert(
      _reallocated->partition.is_original(move.previous()->node_id),
      "ntp {}: move {}->{} should have been from original node",
      _ntp,
      move.previous(),
      move.current());

    auto err = _reallocated->partition.try_revert(move);
    vassert(err == errc::success, "ntp {}: revert error: {}", _ntp, err);
    vlog(
      clusterlog.info,
      "ntp {}: reverted previously scheduled move {} -> {}",
      _ntp,
      move.previous()->node_id,
      move.current().node_id);

    {
        // adjust topic node counts
        auto& node_counts = _ctx._topic2node_counts.at(
          model::topic_namespace_view(ntp()));
        auto& cur_count = node_counts.at(move.current().node_id);
        cur_count -= 1;
        if (cur_count == 0) {
            node_counts.erase(move.current().node_id);
        }
        node_counts[move.previous()->node_id] += 1;
    }

    // adjust partition disk contribution

    auto from_it = _ctx.node_disk_reports.find(move.previous()->node_id);
    if (from_it != _ctx.node_disk_reports.end()) {
        from_it->second.released -= _sizes.get_current(
          move.previous()->node_id);

        vlog(
          clusterlog.trace,
          "after reverting move, node id {} disk: {}",
          from_it->first,
          from_it->second);
    }

    auto to_it = _ctx.node_disk_reports.find(move.current().node_id);
    if (to_it != _ctx.node_disk_reports.end()) {
        if (_reallocated->partition.is_original(move.current().node_id)) {
            to_it->second.released += _sizes.get_current(
              move.current().node_id);
        } else {
            to_it->second.assigned -= _sizes.non_reclaimable;
        }

        vlog(
          clusterlog.trace,
          "after reverting move, node id {} disk: {}",
          to_it->first,
          to_it->second);
    }
}

allocation_constraints
partition_balancer_planner::force_reassignable_partition::
  get_allocation_constraints(double max_disk_usage_ratio) const {
    allocation_constraints constraints;

    constraints.add(distinct_from(_nodes_to_remove));

    if (_sizes) {
        // Add constraint on partition max_disk_usage_ratio overfill
        size_t upper_bound_for_partition_size
          = _sizes.value().non_reclaimable
            + _ctx.config().segment_fallocation_step;
        constraints.add(disk_not_overflowed_by_partition(
          max_disk_usage_ratio,
          upper_bound_for_partition_size,
          _ctx.node_disk_reports));

        // Add constraint on least disk usage
        constraints.add(least_disk_filled(
          max_disk_usage_ratio,
          upper_bound_for_partition_size,
          _ctx.node_disk_reports));
    }

    // Add constraint on unavailable nodes
    if (!_ctx.timed_out_unavailable_nodes.empty()) {
        constraints.add(distinct_from(_ctx.timed_out_unavailable_nodes));
    }

    // Add constraint on decommissioning nodes
    if (!_ctx.decommissioning_nodes.empty()) {
        constraints.add(distinct_from(_ctx.decommissioning_nodes));
    }

    return constraints;
}

void partition_balancer_planner::force_reassignable_partition::
  force_move_dead_replicas(double max_disk_usage_ratio) {
    const auto& replicas = _original_assignment.replicas;
    std::vector<model::node_id> replicas_to_remove;
    replicas_to_remove.reserve(replicas.size());
    for (auto& replica : _original_assignment.replicas) {
        if (_nodes_to_remove.contains(replica.node_id)) {
            replicas_to_remove.push_back(replica.node_id);
        }
    }
    vlog(
      clusterlog.debug,
      "Attempt to force reconfigure {} with replicas {}, replicas to be "
      "removed: {}",
      ntp(),
      replicas,
      replicas_to_remove);
    auto constraints = get_allocation_constraints(max_disk_usage_ratio);
    node2count_t* node2count = nullptr;
    if (_ctx.config().topic_aware) {
        node2count = &_ctx._topic2node_counts.at(
          model::topic_namespace_view(_ntp.ns, _ntp.tp.topic));
    }

    _reallocation = _ctx._parent._partition_allocator.reallocate_partition(
      _ntp, replicas, replicas_to_remove, std::move(constraints), node2count);
    if (_reallocation.has_error()) {
        if (_ctx.increment_failure_count()) {
            vlog(
              clusterlog.info,
              "allocation failure when force moving partition, ntp: {}, "
              "replicas {}, error: {}",
              _ntp,
              replicas,
              _reallocation.error());
        }
        return;
    }

    vlog(
      clusterlog.debug,
      "Attempt to force reconfigure {} with replicas {}, replicas to be "
      "removed: {} "
      "successful, new assignment: {}",
      ntp(),
      replicas,
      replicas_to_remove,
      _reallocation.value().replicas());

    auto& new_assignment = _reallocation.value().replicas();
    auto replicas_added = subtract(new_assignment, replicas);
    auto replicas_removed = subtract(replicas, new_assignment);

    // adjust partition disk usage contribution

    if (_sizes) {
        auto& sizes = _sizes.value();
        for (auto& replica : replicas_removed) {
            auto it = _ctx.node_disk_reports.find(replica.node_id);
            if (it == _ctx.node_disk_reports.end()) {
                continue;
            }
            it->second.released += sizes.get_current(replica.node_id);
        }
        for (auto& replica : replicas_added) {
            auto it = _ctx.node_disk_reports.find(replica.node_id);
            if (it == _ctx.node_disk_reports.end()) {
                continue;
            }
            it->second.assigned += sizes.non_reclaimable;
        }
    }

    // no need to adjust topic node counts, it has already been done by
    // partition_allocator::reallocate_partition
}

/*
 * Function is trying to move ntp out of unavailable nodes
 * It can move to nodes that are violating soft_max_disk_usage_ratio constraint
 */
ss::future<> partition_balancer_planner::get_node_drain_actions(
  request_context& ctx,
  const absl::flat_hash_set<model::node_id>& nodes,
  partition_balancer_planner::change_reason reason) {
    if (nodes.empty()) {
        co_return;
    }

    if (std::all_of(nodes.begin(), nodes.end(), [&](model::node_id id) {
            auto it = ctx.allocation_nodes().find(id);
            return it != ctx.allocation_nodes().end()
                   && it->second->final_partitions()() == 0;
        })) {
        // all nodes already drained
        co_return;
    }

    co_await ctx.for_each_partition([&](partition& part) {
        std::vector<model::node_id> to_move;
        for (const auto& bs : part.replicas()) {
            if (nodes.contains(bs.node_id)) {
                to_move.push_back(bs.node_id);
            }
        }

        if (to_move.empty()) {
            return ss::stop_iteration::no;
        }

        part.match_variant(
          [&](reassignable_partition& part) {
              for (const auto& replica : to_move) {
                  if (part.is_original(replica)) {
                      auto result = part.move_replica(
                        replica,
                        ctx.config().hard_max_disk_usage_ratio,
                        reason);
                      if (!result) {
                          ctx.report_decommission_reallocation_failure(
                            replica, part.ntp());
                      }
                  }
              }
          },
          [&](moving_partition& part) {
              if (part.cancel_requested()) {
                  return;
              }

              absl::flat_hash_set<model::node_id> previous_replicas_set;
              for (const auto& r : part.orig_replicas()) {
                  previous_replicas_set.insert(r.node_id);
              }

              for (const auto& r : to_move) {
                  if (!previous_replicas_set.contains(r)) {
                      // makes sense to cancel
                      part.request_cancel(reason);
                      break;
                  }
              }
          },
          [&](force_reassignable_partition&) {},
          [&](immutable_partition& part) { part.report_failure(reason); });

        return ss::stop_iteration::no;
    });
}

/// Try to fix ntps that have several replicas in one rack (these ntps can
/// appear because rack awareness constraint is not a hard constraint, e.g. when
/// a rack dies and we move all replicas that resided on dead nodes to live
/// ones).
///
/// We go over all such ntps (a list maintained by partition_balancer_state) and
/// if the number of currently live racks is more than the number of racks that
/// the ntp is replicated to, we try to schedule a move. For each rack we
/// arbitrarily choose the first appearing replica to remain there (note: this
/// is probably not optimal choice).
ss::future<> partition_balancer_planner::get_rack_constraint_repair_actions(
  request_context& ctx) {
    if (ctx.state().ntps_with_broken_rack_constraint().empty()) {
        co_return;
    }

    absl::flat_hash_set<model::rack_id> available_racks;
    for (auto node_id : ctx.all_nodes) {
        if (!ctx.timed_out_unavailable_nodes.contains(node_id)) {
            auto rack = ctx.state().members().get_node_rack_id(node_id);
            if (rack) {
                available_racks.insert(*rack);
            }
        }
    }

    auto it = ctx.state().ntps_with_broken_rack_constraint_it_begin();
    while (it != ctx.state().ntps_with_broken_rack_constraint_it_end()) {
        if (!ctx.can_add_reassignment()) {
            co_return;
        }

        co_await ctx.with_partition(*it, [&](partition& part) {
            std::vector<model::node_id> to_move;
            absl::flat_hash_set<model::rack_id> cur_racks;
            for (const auto& bs : part.replicas()) {
                auto rack = ctx.state().members().get_node_rack_id(bs.node_id);
                if (rack) {
                    auto [it, inserted] = cur_racks.insert(*rack);
                    if (!inserted) {
                        to_move.push_back(bs.node_id);
                    }
                }
            }

            if (to_move.empty()) {
                return;
            }

            if (available_racks.size() <= cur_racks.size()) {
                // Can't repair the constraint if we don't have an available
                // rack to place a replica there.
                return;
            }

            part.match_variant(
              [&](reassignable_partition& part) {
                  for (const auto& replica : to_move) {
                      if (part.is_original(replica)) {
                          // only move replicas that haven't been moved for
                          // other reasons
                          (void)part.move_replica(
                            replica,
                            ctx.config().soft_max_disk_usage_ratio,
                            change_reason::rack_constraint_repair);
                      }
                  }
              },
              [](immutable_partition& part) {
                  part.report_failure(change_reason::rack_constraint_repair);
              },
              [](moving_partition&) {},
              [](force_reassignable_partition&) {});
        });
        ++it;
    }
}

/**
 * This is the place where we decide about the order in which partitions will be
 * moved in the case when node disk is being full.
 */
size_t partition_balancer_planner::calculate_full_disk_partition_move_priority(
  model::node_id node_id,
  const reassignable_partition& p,
  const request_context& ctx) {
    /**
     * Definition of priority tiers:
     *
     *  - default (not internal one and not the ono that is bellow the size
     *    threshold) partition
     *      [max_priority,min_default_priority]
     *
     *  - internal partition
     *      (min_default_priority, min_internal_partition_priority]
     *
     *  - small partition (which size is bellow the size threshold)
     *        (min_internal_partition_priority, min_small_partition_priority]
     */
    enum priority_tiers : size_t {
        max_priority = 1000000,
        min_default_priority = 500000,
        min_internal_partition_priority = 300000,
        min_small_partition_priority = 100000,
        min_priority = 0
    };

    auto it = ctx.node_disk_reports.find(node_id);
    if (it == ctx.node_disk_reports.end()) {
        return min_priority;
    }

    static constexpr size_t default_range
      = priority_tiers::max_priority - priority_tiers::min_default_priority;

    // clamp partition size with the total disk space to have well defined
    // behavior in case the size is incorrectly reported, this is required as we
    // normalize the size with disk capacity and we do not want the ration of
    // p_size/disk_capacity to be larger than 1.0.
    const size_t partition_size = std::min(
      p.sizes().get_current(node_id), it->second.total);

    if (partition_size < ctx.config().min_partition_size_threshold) {
        static constexpr size_t range
          = priority_tiers::min_internal_partition_priority - 1
            - priority_tiers::min_small_partition_priority;

        // prioritize from largest to smallest one
        return (range * partition_size)
                 / ctx.config().min_partition_size_threshold
               + min_small_partition_priority;
    }
    /**
     * Assign internal partitions to its priority tier, order from smallest to
     * largest one (the same as all other partitions)
     */
    if (
      p.ntp().ns == model::kafka_internal_namespace
      || p.ntp().tp.topic == model::kafka_consumer_offsets_topic) {
        static constexpr size_t range
          = priority_tiers::min_default_priority - 1
            - priority_tiers::min_internal_partition_priority;

        return (range - (range * partition_size) / it->second.total)
               + priority_tiers::min_internal_partition_priority;
    }

    // normalize and offset to match the default partition priority tier, where
    // max value would represent a partition that is of the full disk capacity
    // size. We subtract it from the max priority to prioritize smallest
    // partitions.
    return (default_range - (default_range * partition_size) / it->second.total)
           + priority_tiers::min_default_priority;
}

/*
 * Function is trying to move ntps out of node that are violating
 * soft_max_disk_usage_ratio. It takes nodes in reverse used space ratio order.
 * For each node it is trying to collect set of partitions to move. Partitions
 * are selected in ascending order of their size.
 *
 * If more than one replica in a group is on a node violating disk usage
 * constraints, we try to reallocate all such replicas. Some of reallocation
 * requests can fail, we just move those replicas that we can.
 */
ss::future<>
partition_balancer_planner::get_full_node_actions(request_context& ctx) {
    std::vector<const node_disk_space*> sorted_full_nodes;
    for (const auto& kv : ctx.node_disk_reports) {
        const auto* node_disk = &kv.second;
        if (
          node_disk->final_used_ratio()
          > ctx.config().soft_max_disk_usage_ratio) {
            sorted_full_nodes.push_back(node_disk);
        }
    }
    std::sort(
      sorted_full_nodes.begin(),
      sorted_full_nodes.end(),
      [](const auto* lhs, const auto* rhs) {
          return lhs->final_used_ratio() > rhs->final_used_ratio();
      });

    if (sorted_full_nodes.empty()) {
        co_return;
    }

    auto find_full_node = [&](model::node_id id) -> const node_disk_space* {
        auto it = ctx.node_disk_reports.find(id);
        if (it == ctx.node_disk_reports.end()) {
            return nullptr;
        } else if (
          it->second.final_used_ratio()
          > ctx.config().soft_max_disk_usage_ratio) {
            return &it->second;
        } else {
            return nullptr;
        }
    };

    // build an index of move candidates: full node -> movement priority -> ntp
    absl::flat_hash_map<
      model::node_id,
      absl::btree_multimap<size_t, model::ntp, std::greater<>>>
      full_node2priority2ntp;
    co_await ctx.for_each_partition([&](partition& part) {
        part.match_variant(
          [&](reassignable_partition& part) {
              std::vector<model::node_id> replicas_on_full_nodes;
              for (const auto& bs : part.replicas()) {
                  model::node_id replica = bs.node_id;
                  if (part.is_original(replica) && find_full_node(replica)) {
                      replicas_on_full_nodes.push_back(replica);
                  }
              }

              for (model::node_id node_id : replicas_on_full_nodes) {
                  full_node2priority2ntp[node_id].emplace(
                    calculate_full_disk_partition_move_priority(
                      node_id, part, ctx),
                    part.ntp());
              }
          },
          [](auto&) {});

        return ss::stop_iteration::no;
    });

    // move partitions, starting from partitions with replicas on the most full
    // node
    for (const auto* node_disk : sorted_full_nodes) {
        if (!ctx.can_add_reassignment()) {
            co_return;
        }

        auto ntp_index_it = full_node2priority2ntp.find(node_disk->node_id);
        if (ntp_index_it == full_node2priority2ntp.end()) {
            // no eligible partitions, skip node
            continue;
        }

        for (const auto& [score, ntp_to_move] : ntp_index_it->second) {
            if (!ctx.can_add_reassignment()) {
                co_return;
            }
            if (
              node_disk->final_used_ratio()
              < ctx.config().soft_max_disk_usage_ratio) {
                break;
            }

            co_await ctx.with_partition(ntp_to_move, [&](partition& part) {
                part.match_variant(
                  [&](reassignable_partition& part) {
                      struct full_node_replica {
                          model::node_id node_id;
                          double final_used_ratio;
                      };
                      std::vector<full_node_replica> full_node_replicas;

                      for (const auto& r : part.replicas()) {
                          if (
                            ctx.timed_out_unavailable_nodes.contains(r.node_id)
                            || !part.is_original(r.node_id)) {
                              continue;
                          }

                          const auto* full_node = find_full_node(r.node_id);
                          if (full_node) {
                              full_node_replicas.push_back(full_node_replica{
                                .node_id = r.node_id,
                                .final_used_ratio
                                = full_node->final_used_ratio()});
                          }
                      }

                      // Try to reallocate replicas starting from the most full
                      // node
                      std::sort(
                        full_node_replicas.begin(),
                        full_node_replicas.end(),
                        [](const auto& lhs, const auto& rhs) {
                            return lhs.final_used_ratio > rhs.final_used_ratio;
                        });

                      for (const auto& replica : full_node_replicas) {
                          (void)part.move_replica(
                            replica.node_id,
                            ctx.config().soft_max_disk_usage_ratio,
                            change_reason::disk_full);
                      }
                  },
                  [](auto&) {});
            });
        }
    }
}

ss::future<> partition_balancer_planner::get_counts_rebalancing_actions(
  request_context& ctx) {
    if (!ctx.config().ondemand_rebalance_requested) {
        if (ctx.state().nodes_to_rebalance().empty()) {
            co_return;
        }

        if (ctx.config().mode < model::partition_autobalancing_mode::node_add) {
            ctx._counts_rebalancing_finished = true;
            co_return;
        }
    }

    if (!ctx.can_add_reassignment()) {
        co_return;
    }

    // {topic-wise count, total count}
    using scores_t = std::array<double, 2>;

    // lower score is better
    auto calc_scores = [&](const model::ntp& ntp, model::node_id id) {
        auto node_it = ctx.allocation_nodes().find(id);
        if (node_it == ctx.allocation_nodes().end()) {
            throw balancer_tick_aborted_exception{
              fmt::format("node id: {} disappeared", id)};
        }
        const auto& alloc_node = *node_it->second;

        double topic_count = 0;
        if (ctx.config().topic_aware) {
            const auto& counts = ctx._topic2node_counts.at(
              model::topic_namespace_view(ntp));
            topic_count = double(counts.at(id)) / alloc_node.max_capacity();
        }

        auto total_count = double(alloc_node.final_partitions())
                           / alloc_node.max_capacity();

        return scores_t{topic_count, total_count};
    };

    auto scores_cmp_less = [](const scores_t& left, const scores_t& right) {
        for (size_t i = 0; i < left.size(); ++i) {
            auto l = left[i];
            auto r = right[i];
            if (l < r - 1e-8) {
                return true;
            }
            if (l > r + 1e-8) {
                return false;
            }
        }
        // (approximately) equal
        return false;
    };

    // Reaches its minimum of 1.0 when replica counts (scaled by the node
    // capacity) are equal across all nodes.
    auto calc_objective = [&]() {
        double sum = 0;
        double sum_sq = 0;
        for (const auto& id : ctx.all_nodes) {
            auto it = ctx.allocation_nodes().find(id);
            if (it == ctx.allocation_nodes().end()) {
                throw balancer_tick_aborted_exception{
                  fmt::format("node id: {} disappeared", id)};
            }
            auto count = double(it->second->final_partitions())
                         / it->second->max_capacity();
            sum += count;
            sum_sq += count * count;
        }

        if (sum == 0) {
            return 1.0;
        }

        return ctx.all_nodes.size() * sum_sq / (sum * sum);
    };

    double orig_objective = calc_objective();

    // The algorithm is simple: just go over all replicas and try to move them
    // to a better node (this is driven by allocation constraints). If we
    // haven't been able to improve the objective, this means that we've reached
    // (local) optimum and rebalance can be finished.

    bool should_stop = true;
    co_await ctx.for_each_replica_random_order(
      [&](partition& part, model::node_id node) {
          if (!ctx.can_add_reassignment()) {
              // Finish early, even though in theory we could add more replica
              // moves to existing reassignments. The reason is that this will
              // bias the algorithm towards adding more moves to partitions
              // that we already reassigned, which we want to avoid (to avoid
              // formation of isolated replica subsets).
              should_stop = false;
              return ss::stop_iteration::yes;
          }

          part.match_variant(
            [&](reassignable_partition& part) {
                if (!part.is_original(node)) {
                    return;
                }

                auto scores_before = calc_scores(part.ntp(), node);

                auto res = part.move_replica(
                  node,
                  ctx.config().soft_max_disk_usage_ratio,
                  change_reason::partition_count_rebalancing);
                if (!res) {
                    return;
                }

                if (res.value().current().node_id != node) {
                    auto scores_after = calc_scores(
                      part.ntp(), res.value().current().node_id);

                    if (!scores_cmp_less(scores_after, scores_before)) {
                        // unnecessary move, doesn't improve the scores
                        // (probably moved to another node with the same
                        // number of partitions)
                        part.revert(res.value());
                    } else {
                        should_stop = false;
                    }
                }
            },
            [&](immutable_partition& p) {
                p.report_failure(change_reason::partition_count_rebalancing);
                should_stop = false;
            },
            [](auto&) {});

          return ss::stop_iteration::no;
      });

    double cur_objective = calc_objective();
    vlog(
      clusterlog.info,
      "counts rebalancing objective: {:.6} -> {:.6}",
      orig_objective,
      cur_objective);

    auto all_nodes_healthy = [&] {
        // don't count rebalance as finished if not all nodes are fully
        // available - if they become healthy later, partition distribution
        // won't be optimal.
        if (!ctx.all_unavailable_nodes.empty()) {
            return false;
        }
        for (model::node_id id : ctx.all_nodes) {
            if (!ctx.node_disk_reports.contains(id)) {
                return false;
            }
        }
        return true;
    };

    if (should_stop && all_nodes_healthy()) {
        ctx._counts_rebalancing_finished = true;
    }
}

ss::future<>
partition_balancer_planner::get_force_repair_actions(request_context& ctx) {
    if (ctx.state().topics().partitions_to_force_recover().empty()) {
        co_return;
    }

    auto it = ctx.state().topics().partitions_to_force_recover_it_begin();
    while (it != ctx.state().topics().partitions_to_force_recover_it_end()) {
        if (!ctx.can_add_reassignment()) {
            co_return;
        }
        co_await ctx.with_partition(it->first, [&](partition& part) {
            part.match_variant(
              [&](force_reassignable_partition& part) {
                  part.force_move_dead_replicas(
                    ctx.config().hard_max_disk_usage_ratio);
              },
              [&](reassignable_partition&) {},
              [&](moving_partition&) {
                  // ignore, wait for it to be canceled / finished.
              },
              [&](immutable_partition&) {});
        });
        ++it;
    }
}

void partition_balancer_planner::request_context::collect_actions(
  partition_balancer_planner::plan_data& result) {
    result.reassignments.reserve(_reassignments.size());
    for (auto& [ntp, reallocated_meta] : _reassignments) {
        result.reassignments.push_back(ntp_reassignment{
          .ntp = ntp,
          .allocated = std::move(reallocated_meta.partition),
          .reconfiguration_policy = reallocated_meta.reconfiguration_policy,
          .type = ntp_reassignment_type::regular});
    }

    for (auto& [ntp, reallocated] : _force_reassignments) {
        result.reassignments.push_back(ntp_reassignment{
          .ntp = ntp,
          .allocated = std::move(reallocated),
          .type = ntp_reassignment_type::force});
    }

    result.failed_actions_count = _failed_actions_count;

    result.cancellations.reserve(_cancellations.size());
    std::move(
      _cancellations.begin(),
      _cancellations.end(),
      std::back_inserter(result.cancellations));

    result.counts_rebalancing_finished = _counts_rebalancing_finished;

    result.decommission_realloc_failures = std::move(
      _decommission_realloc_failures);

    if (
      !result.cancellations.empty() || !result.reassignments.empty()
      || result.counts_rebalancing_finished) {
        result.status = status::actions_planned;
    }
}

ss::future<partition_balancer_planner::plan_data>
partition_balancer_planner::plan_actions(
  const cluster_health_report& health_report, ss::abort_source& as) {
    request_context ctx(*this, as);
    plan_data result;

    init_per_node_state(health_report, ctx, result);

    if (!ctx.all_reports_received()) {
        result.status = status::waiting_for_reports;
        co_return result;
    }

    if (
      result.violations.is_empty() && ctx.decommissioning_nodes.empty()
      && _state.ntps_with_broken_rack_constraint().empty()
      && _state.nodes_to_rebalance().empty()
      && _state.topics().partitions_to_force_recover().empty()
      && !_config.ondemand_rebalance_requested) {
        result.status = status::empty;
        co_return result;
    }

    co_await init_ntp_sizes_from_health_report(health_report, ctx);
    co_await init_topic_node_counts(ctx);

    co_await get_node_drain_actions(
      ctx, ctx.decommissioning_nodes, change_reason::node_decommissioning);

    if (ctx.config().mode == model::partition_autobalancing_mode::continuous) {
        co_await get_node_drain_actions(
          ctx,
          ctx.timed_out_unavailable_nodes,
          change_reason::node_unavailable);
        co_await get_full_node_actions(ctx);
        co_await get_rack_constraint_repair_actions(ctx);
    }
    co_await get_counts_rebalancing_actions(ctx);
    co_await get_force_repair_actions(ctx);

    ctx.collect_actions(result);
    if (ctx._partitions_with_missing_size > 0) {
        result.status = status::missing_sizes;
    }
    co_return result;
}

std::ostream&
operator<<(std::ostream& o, partition_balancer_planner::change_reason r) {
    switch (r) {
    case partition_balancer_planner::change_reason::rack_constraint_repair:
        fmt::print(o, "rack_constraint_repair");
        break;
    case partition_balancer_planner::change_reason::partition_count_rebalancing:
        fmt::print(o, "partition_count_rebalancing");
        break;
    case partition_balancer_planner::change_reason::node_unavailable:
        fmt::print(o, "node_unavailable");
        break;
    case partition_balancer_planner::change_reason::node_decommissioning:
        fmt::print(o, "node_decommissioning");
        break;
    case partition_balancer_planner::change_reason::disk_full:
        fmt::print(o, "disk_full");
        break;
    }
    return o;
}

} // namespace cluster
