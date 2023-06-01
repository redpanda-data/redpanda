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

#include "cluster/cluster_utils.h"
#include "cluster/members_table.h"
#include "cluster/node_status_table.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/scheduling/constraints.h"
#include "cluster/scheduling/types.h"
#include "model/namespace.h"
#include "ssx/sformat.h"

#include <seastar/core/sstring.hh>
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

        hard_constraint_evaluator
        make_evaluator(const replicas_t&) const final {
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
    size_t num_nodes_in_maintenance = 0;
    absl::flat_hash_set<model::node_id> decommissioning_nodes;
    absl::flat_hash_map<model::node_id, node_disk_space> node_disk_reports;

    void for_each_partition(
      ss::noncopyable_function<ss::stop_iteration(partition&)>);
    void with_partition(
      const model::ntp&, ss::noncopyable_function<void(partition&)>);

    const partition_balancer_state& state() const { return _parent._state; }

    const planner_config& config() const { return _parent._config; }

    bool can_add_cancellation() const {
        return _reassignments.size() + _cancellations.size()
               < config().max_concurrent_actions;
    }

    bool can_add_reassignment() const {
        if (_planned_moves_size_bytes >= config().movement_disk_size_batch) {
            return false;
        } else if (
          state().topics().updates_in_progress().size() + _reassignments.size()
          >= config().max_concurrent_actions) {
            return false;
        } else {
            return _reassignments.size() + _cancellations.size()
                   < config().max_concurrent_actions;
        }
    }

private:
    friend class partition_balancer_planner;

    request_context(partition_balancer_planner& parent)
      : _parent(parent) {}

    bool all_reports_received() const;

    template<typename Visitor>
    auto do_with_partition(
      const model::ntp& ntp,
      const std::vector<model::broker_shard>& orig_replicas,
      Visitor&);

    void collect_actions(plan_data&);

    // returns true if the failure can be logged
    bool increment_failure_count();

private:
    partition_balancer_planner& _parent;
    absl::node_hash_map<model::ntp, size_t> _ntp2size;
    absl::node_hash_map<model::ntp, absl::flat_hash_map<model::node_id, size_t>>
      _moving_ntp2replica_sizes;
    absl::node_hash_map<model::ntp, allocated_partition> _reassignments;
    uint64_t _planned_moves_size_bytes = 0;
    size_t _failed_actions_count = 0;
    absl::node_hash_set<model::ntp> _cancellations;
};

void partition_balancer_planner::init_per_node_state(
  const cluster_health_report& health_report,
  request_context& ctx,
  plan_data& result) const {
    auto const now = rpc::clock_type::now();
    for (const auto& [id, broker] : _state.members().nodes()) {
        if (
          broker.state.get_membership_state()
          == model::membership_state::removed) {
            continue;
        }

        ctx.all_nodes.push_back(id);

        if (
          broker.state.get_maintenance_state()
          == model::maintenance_state::active) {
            vlog(clusterlog.debug, "node {}: in maintenance", id);
            ctx.num_nodes_in_maintenance += 1;
        }

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
        const uint64_t total = node_report.local_state.data_disk.total;
        const uint64_t free = node_report.local_state.data_disk.free;

        ctx.node_disk_reports.emplace(
          node_report.id, node_disk_space(node_report.id, total, total - free));
    }

    for (const auto& [id, disk] : ctx.node_disk_reports) {
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

void partition_balancer_planner::init_ntp_sizes_from_health_report(
  const cluster_health_report& health_report, request_context& ctx) {
    for (const auto& node_report : health_report.node_reports) {
        for (const auto& tp_ns : node_report.topics) {
            for (const auto& partition : tp_ns.partitions) {
                model::ntp ntp{tp_ns.tp_ns.ns, tp_ns.tp_ns.tp, partition.id};
                auto& ntp_size = ctx._ntp2size[ntp];
                ntp_size = std::max(ntp_size, partition.size_bytes);

                if (_state.topics().is_update_in_progress(ntp)) {
                    ctx._moving_ntp2replica_sizes[ntp][node_report.id]
                      = partition.size_bytes;
                }
            }
        }
    }

    // Add moving partitions contribution to batch size and node disk sizes.
    for (const auto& [ntp, replica2size] : ctx._moving_ntp2replica_sizes) {
        const auto& update = _state.topics().updates_in_progress().at(ntp);

        auto moving_from = subtract_replica_sets(
          update.get_previous_replicas(), update.get_target_replicas());
        auto moving_to = subtract_replica_sets(
          update.get_target_replicas(), update.get_previous_replicas());

        size_t max_size = ctx._ntp2size.at(ntp);

        switch (update.get_state()) {
        case reconfiguration_state::in_progress:
        case reconfiguration_state::force_update:
            ctx._planned_moves_size_bytes += max_size;

            for (const auto& bs : moving_from) {
                auto node_it = ctx.node_disk_reports.find(bs.node_id);
                if (node_it != ctx.node_disk_reports.end()) {
                    auto size_it = replica2size.find(bs.node_id);
                    size_t replica_size
                      = (size_it != replica2size.end() ? size_it->second : max_size);
                    node_it->second.released += replica_size;
                }
            }

            for (const auto& bs : moving_to) {
                auto node_it = ctx.node_disk_reports.find(bs.node_id);
                if (node_it != ctx.node_disk_reports.end()) {
                    auto size_it = replica2size.find(bs.node_id);
                    size_t replica_size
                      = (size_it != replica2size.end() ? size_it->second : 0);
                    node_it->second.assigned += (max_size - replica_size);
                }
            }

            break;
        case reconfiguration_state::cancelled:
        case reconfiguration_state::force_cancelled:
            for (const auto& bs : moving_to) {
                auto node_it = ctx.node_disk_reports.find(bs.node_id);
                if (node_it != ctx.node_disk_reports.end()) {
                    auto size_it = replica2size.find(bs.node_id);
                    if (size_it != replica2size.end()) {
                        node_it->second.released += size_it->second;
                    }
                }
            }
            break;
        }
    }
}

bool partition_balancer_planner::request_context::all_reports_received() const {
    for (auto id : all_nodes) {
        if (
          !all_unavailable_nodes.contains(id)
          && !node_disk_reports.contains(id)) {
            vlog(clusterlog.info, "No disk report for node {}", id);
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
        return (_reallocated ? _reallocated->replicas() : _orig_replicas);
    };

    bool is_original(model::node_id replica) const {
        return !_reallocated || _reallocated->is_original(replica);
    }

    size_t size_bytes() const { return _size_bytes; }

    result<model::broker_shard> move_replica(
      model::node_id replica,
      double max_disk_usage_ratio,
      std::string_view reason);

private:
    friend class request_context;

    reassignable_partition(
      model::ntp ntp,
      size_t size_bytes,
      std::optional<allocated_partition> reallocated,
      const std::vector<model::broker_shard>& orig_replicas,
      request_context& ctx)
      : _ntp(std::move(ntp))
      , _size_bytes(size_bytes)
      , _reallocated(std::move(reallocated))
      , _orig_replicas(orig_replicas)
      , _ctx(ctx) {}

    bool has_changes() const {
        return _reallocated && _reallocated->has_changes();
    }

    allocation_constraints
    get_allocation_constraints(double max_disk_usage_ratio) const;

private:
    model::ntp _ntp;
    size_t _size_bytes;
    std::optional<allocated_partition> _reallocated;
    const std::vector<model::broker_shard>& _orig_replicas;
    request_context& _ctx;
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

    void request_cancel(std::string_view reason) {
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

            // Adjust partition contribution to final disk space
            auto size_it = _ctx._moving_ntp2replica_sizes.find(_ntp);
            if (size_it != _ctx._moving_ntp2replica_sizes.end()) {
                const auto& replica2size = size_it->second;
                auto moving_to = subtract_replica_sets(
                  _replicas, _orig_replicas);
                for (const auto& bs : moving_to) {
                    auto node_it = _ctx.node_disk_reports.find(bs.node_id);
                    if (node_it != _ctx.node_disk_reports.end()) {
                        auto size_it = replica2size.find(bs.node_id);
                        if (size_it != replica2size.end()) {
                            node_it->second.released += size_it->second;
                        }
                    }
                }
            }

            // TODO: adjust contribution to final partition counts
        }
    }

    void
    report_failure(std::string_view reason, std::string_view change_reason) {
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
    };

    immutability_reason reason() const { return _reason; }

    void report_failure(std::string_view change_reason) {
        bool can_log = _ctx.increment_failure_count();
        if (!can_log) {
            return;
        }

        ss::sstring reason;
        switch (_reason) {
        case immutability_reason::no_quorum:
            reason = "no raft quorum";
            break;
        case immutability_reason::no_size_info:
            reason = "partition size information unavailable";
            break;
        case immutability_reason::reconfiguration_state:
            reason = ssx::sformat(
              "reconfiguration in progress, state: {}", _reconfiguration_state);
            break;
        case immutability_reason::batch_full:
            // don't log full batch failures (if the batch is full, we are
            // not stalling)
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

    std::variant<reassignable_partition, moving_partition, immutable_partition>
      _variant;
};

template<typename Visitor>
auto partition_balancer_planner::request_context::do_with_partition(
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& orig_replicas,
  Visitor& visitor) {
    auto in_progress_it = _parent._state.topics().updates_in_progress().find(
      ntp);
    if (in_progress_it != _parent._state.topics().updates_in_progress().end()) {
        const auto& replicas = in_progress_it->second.get_target_replicas();
        const auto& orig_replicas
          = in_progress_it->second.get_previous_replicas();
        auto state = in_progress_it->second.get_state();

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

    size_t size_bytes = 0;
    auto size_it = _ntp2size.find(ntp);
    if (size_it != _ntp2size.end()) {
        size_bytes = size_it->second;
    } else {
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

    std::optional<allocated_partition> reallocated;
    if (reassignment_it != _reassignments.end()) {
        // borrow the allocated_partition object
        reallocated = std::move(reassignment_it->second);
    }

    partition part{reassignable_partition{
      ntp, size_bytes, std::move(reallocated), orig_replicas, *this}};
    auto deferred = ss::defer([&] {
        auto& reassignable = std::get<reassignable_partition>(part._variant);
        // insert or return part._reallocated to reassignments
        if (reassignment_it != _reassignments.end()) {
            reassignment_it->second = std::move(*reassignable._reallocated);
        } else if (
          reassignable._reallocated
          && reassignable._reallocated->has_changes()) {
            _reassignments.emplace(ntp, std::move(*reassignable._reallocated));
            _planned_moves_size_bytes += reassignable._size_bytes;
        }
    });

    return visitor(part);
}

void partition_balancer_planner::request_context::for_each_partition(
  ss::noncopyable_function<ss::stop_iteration(partition&)> visitor) {
    for (const auto& t : _parent._state.topics().topics_map()) {
        for (const auto& a : t.second.get_assignments()) {
            auto ntp = model::ntp(t.first.ns, t.first.tp, a.id);
            auto stop = do_with_partition(ntp, a.replicas, visitor);
            if (stop == ss::stop_iteration::yes) {
                return;
            }
        }
    }
}

void partition_balancer_planner::request_context::with_partition(
  const model::ntp& ntp, ss::noncopyable_function<void(partition&)> visitor) {
    auto topic = model::topic_namespace_view(ntp);
    auto topic_meta = _parent._state.topics().get_topic_metadata_ref(topic);
    if (!topic_meta) {
        vlog(clusterlog.warn, "topic {} not found", topic);
        return;
    }
    auto it = topic_meta->get().get_assignments().find(ntp.tp.partition);
    if (it == topic_meta->get().get_assignments().end()) {
        vlog(
          clusterlog.warn,
          "partition {} of topic {} not found",
          ntp.tp.partition,
          topic);
        return;
    }

    do_with_partition(ntp, it->replicas, visitor);
}

allocation_constraints
partition_balancer_planner::reassignable_partition::get_allocation_constraints(
  double max_disk_usage_ratio) const {
    allocation_constraints constraints;

    // Add constraint on least disk usage
    constraints.add(
      least_disk_filled(max_disk_usage_ratio, _ctx.node_disk_reports));

    // Add constraint on partition max_disk_usage_ratio overfill
    size_t upper_bound_for_partition_size
      = _size_bytes + _ctx.config().segment_fallocation_step;
    constraints.add(disk_not_overflowed_by_partition(
      max_disk_usage_ratio,
      upper_bound_for_partition_size,
      _ctx.node_disk_reports));

    // Add constraint on unavailable nodes
    constraints.add(distinct_from(_ctx.timed_out_unavailable_nodes));

    // Add constraint on decommissioning nodes
    if (!_ctx.decommissioning_nodes.empty()) {
        constraints.add(distinct_from(_ctx.decommissioning_nodes));
    }

    return constraints;
}

result<model::broker_shard>
partition_balancer_planner::reassignable_partition::move_replica(
  model::node_id replica,
  double max_disk_usage_ratio,
  std::string_view reason) {
    if (!_reallocated) {
        _reallocated
          = _ctx._parent._partition_allocator.make_allocated_partition(
            replicas(), get_allocation_domain(_ntp));
    }

    auto constraints = get_allocation_constraints(max_disk_usage_ratio);
    auto moved = _ctx._parent._partition_allocator.reallocate_replica(
      *_reallocated, replica, std::move(constraints));
    if (!moved) {
        if (_ctx.increment_failure_count()) {
            vlog(
              clusterlog.info,
              "ntp {} (size: {}, current replicas: {}): attempt to move "
              "replica {} (reason: {}) failed, error: {}",
              _ntp,
              _size_bytes,
              replicas(),
              replica,
              reason,
              moved.error().message());
        }
        return moved;
    }

    if (moved.value().node_id != replica) {
        vlog(
          clusterlog.info,
          "ntp {} (size: {}, orig replicas: {}): scheduling replica move "
          "{} -> {}, reason: {}",
          _ntp,
          _size_bytes,
          _orig_replicas,
          replica,
          moved.value(),
          reason);

        auto from_it = _ctx.node_disk_reports.find(replica);
        if (from_it != _ctx.node_disk_reports.end()) {
            from_it->second.released += _size_bytes;
        }

        auto to_it = _ctx.node_disk_reports.find(moved.value().node_id);
        if (to_it != _ctx.node_disk_reports.end()) {
            to_it->second.assigned += _size_bytes;
        }
    } else {
        // TODO: revert?
    }

    return moved;
}

/*
 * Function is trying to move ntp out of unavailable nodes
 * It can move to nodes that are violating soft_max_disk_usage_ratio constraint
 */
void partition_balancer_planner::get_node_drain_actions(
  request_context& ctx,
  const absl::flat_hash_set<model::node_id>& nodes,
  std::string_view reason) {
    if (nodes.empty()) {
        return;
    }

    ctx.for_each_partition([&](partition& part) {
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
                      // ignore result
                      (void)part.move_replica(
                        replica,
                        ctx.config().hard_max_disk_usage_ratio,
                        reason);
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
void partition_balancer_planner::get_rack_constraint_repair_actions(
  request_context& ctx) {
    if (ctx.state().ntps_with_broken_rack_constraint().empty()) {
        return;
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

    for (const auto& ntp : ctx.state().ntps_with_broken_rack_constraint()) {
        if (!ctx.can_add_reassignment()) {
            return;
        }

        ctx.with_partition(ntp, [&](partition& part) {
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
                            ctx.config().hard_max_disk_usage_ratio,
                            "rack constraint repair");
                      }
                  }
              },
              [](immutable_partition& part) {
                  part.report_failure("rack constraint repair");
              },
              [](moving_partition&) {});
        });
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
    const auto partition_size = std::min(p.size_bytes(), it->second.total);

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
void partition_balancer_planner::get_full_node_actions(request_context& ctx) {
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
        return;
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
    ctx.for_each_partition([&](partition& part) {
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
            return;
        }

        auto ntp_index_it = full_node2priority2ntp.find(node_disk->node_id);
        if (ntp_index_it == full_node2priority2ntp.end()) {
            // no eligible partitions, skip node
            continue;
        }

        for (const auto& [score, ntp_to_move] : ntp_index_it->second) {
            if (!ctx.can_add_reassignment()) {
                return;
            }
            if (
              node_disk->final_used_ratio()
              < ctx.config().soft_max_disk_usage_ratio) {
                break;
            }

            ctx.with_partition(ntp_to_move, [&](partition& part) {
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
                            "full_nodes");
                      }
                  },
                  [](auto&) {});
            });
        }
    }
}

void partition_balancer_planner::request_context::collect_actions(
  partition_balancer_planner::plan_data& result) {
    result.reassignments.reserve(_reassignments.size());
    for (auto& [ntp, reallocated] : _reassignments) {
        result.reassignments.push_back(
          ntp_reassignment{.ntp = ntp, .allocated = std::move(reallocated)});
    }

    result.failed_actions_count = _failed_actions_count;

    result.cancellations.reserve(_cancellations.size());
    std::move(
      _cancellations.begin(),
      _cancellations.end(),
      std::back_inserter(result.cancellations));

    if (!result.cancellations.empty() || !result.reassignments.empty()) {
        result.status = status::actions_planned;
    }
}

partition_balancer_planner::plan_data partition_balancer_planner::plan_actions(
  const cluster_health_report& health_report) {
    request_context ctx(*this);
    plan_data result;

    init_per_node_state(health_report, ctx, result);

    if (!ctx.all_reports_received()) {
        result.status = status::waiting_for_reports;
        return result;
    }

    if (
      result.violations.is_empty() && ctx.decommissioning_nodes.empty()
      && _state.ntps_with_broken_rack_constraint().empty()) {
        result.status = status::empty;
        return result;
    }

    init_ntp_sizes_from_health_report(health_report, ctx);

    get_node_drain_actions(ctx, ctx.decommissioning_nodes, "decommission");

    if (ctx.config().mode == model::partition_autobalancing_mode::continuous) {
        if (ctx.num_nodes_in_maintenance == 0) {
            get_node_drain_actions(
              ctx, ctx.timed_out_unavailable_nodes, "unavailable nodes");
            get_rack_constraint_repair_actions(ctx);
            get_full_node_actions(ctx);
        } else if (!result.violations.is_empty()) {
            result.status = status::waiting_for_maintenance_end;
        }
    }

    ctx.collect_actions(result);
    return result;
}

} // namespace cluster
