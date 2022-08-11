#include "cluster/members_backend.h"

#include "cluster/controller_api.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/members_frontend.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/later.hh>
#include <seastar/util/optimized_optional.hh>

#include <absl/container/node_hash_set.h>
#include <fmt/ostream.h>

#include <cstdint>
#include <exception>
#include <functional>
#include <optional>
#include <ostream>
#include <vector>

namespace cluster {

members_backend::members_backend(
  ss::sharded<cluster::topics_frontend>& topics_frontend,
  ss::sharded<topic_table>& topics,
  ss::sharded<partition_allocator>& allocator,
  ss::sharded<members_table>& members,
  ss::sharded<controller_api>& api,
  ss::sharded<members_manager>& members_manager,
  ss::sharded<members_frontend>& members_frontend,
  consensus_ptr raft0,
  ss::sharded<ss::abort_source>& as)
  : _topics_frontend(topics_frontend)
  , _topics(topics)
  , _allocator(allocator)
  , _members(members)
  , _api(api)
  , _members_manager(members_manager)
  , _members_frontend(members_frontend)
  , _raft0(raft0)
  , _as(as)
  , _retry_timeout(config::shard_local_cfg().members_backend_retry_ms()) {
    _retry_timer.set_callback([this] { start_reconciliation_loop(); });
}

ss::future<> members_backend::stop() {
    vlog(clusterlog.info, "Stopping Members Backend...");
    _retry_timer.cancel();
    _new_updates.broken();
    return _bg.close();
}

void members_backend::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cluster:members:backend"),
      {
        sm::make_gauge(
          "queued_node_operations",
          [this] { return _updates.size(); },
          sm::description("Number of queued node operations")),
      });
}
void members_backend::start() {
    setup_metrics();
    ssx::spawn_with_gate(_bg, [this] {
        return ss::do_until(
          [this] { return _as.local().abort_requested(); },
          [this] { return handle_updates(); });
    });
    _retry_timer.arm(_retry_timeout);
}

ss::future<> members_backend::handle_updates() {
    /**
     * wait for updates from members manager, after update is received we
     * translate it into reallocation meta, reallocation meta represents a
     * partition that needs reallocation
     */
    using updates_t = std::vector<members_manager::node_update>;
    return _members_manager.local()
      .get_node_updates()
      .then([this](updates_t updates) {
          return _lock.with([this, updates = std::move(updates)]() mutable {
              return ss::do_with(
                std::move(updates), [this](updates_t& updates) {
                    return ss::do_for_each(
                      updates, [this](members_manager::node_update update) {
                          return handle_single_update(update);
                      });
                });
          });
      })
      .handle_exception([](const std::exception_ptr& e) {
          vlog(clusterlog.trace, "error waiting for members updates - {}", e);
      });
}

void members_backend::handle_single_update(
  members_manager::node_update update) {
    using update_t = members_manager::node_update_type;
    vlog(clusterlog.debug, "membership update received: {}", update);
    switch (update.type) {
    case update_t::recommissioned:
        handle_recommissioned(update);
        _updates.emplace_back(update);
        _new_updates.signal();
        return;
    case update_t::reallocation_finished:
        handle_reallocation_finished(update.id);
        return;
    case update_t::added:
        stop_node_decommissioning(update.id);
        _updates.emplace_back(update);
        _new_updates.signal();
        return;
    case update_t::decommissioned:
        stop_node_addition(update.id);
        _decommission_command_revision.emplace(
          update.id, model::revision_id(update.offset));
        _updates.emplace_back(update);
        _new_updates.signal();
        return;
    }
    __builtin_unreachable();
}

void members_backend::start_reconciliation_loop() {
    ssx::background = ssx::spawn_with_gate_then(_bg, [this] {
                          vlog(clusterlog.warn, "reconcile() starting");
                          return reconcile().finally([this] {
                              if (!_as.local().abort_requested()) {
                                  vlog(
                                    clusterlog.warn,
                                    "reconcile() ending (rearmed)");
                                  _retry_timer.arm(_retry_timeout);
                              } else {
                                  vlog(
                                    clusterlog.warn,
                                    "reconcile() ending (not rearmed)");
                              }
                          });
                      }).handle_exception([](const std::exception_ptr& e) {
        // just log an exception, will retry
        vlog(
          clusterlog.warn,
          "error encountered while handling cluster state reconciliation - {}",
          e);
    });
}

bool is_in_replica_set(
  const std::vector<model::broker_shard>& replicas, model::node_id id) {
    return std::any_of(
      replicas.cbegin(), replicas.cend(), [id](const model::broker_shard& bs) {
          return id == bs.node_id;
      });
}

void members_backend::calculate_reallocations(update_meta& meta) {
    switch (meta.update.type) {
    case members_manager::node_update_type::decommissioned:
        calculate_reallocations_after_decommissioned(meta);
        return;
    case members_manager::node_update_type::added:
        calculate_reallocations_after_node_added(meta);
        return;
    case members_manager::node_update_type::recommissioned:
        calculate_reallocations_after_recommissioned(meta);
        return;
    default:
        return;
    }
}
/**
 * Simple helper class representing how many replicas have to be moved from the
 * node.
 */
struct replicas_to_move {
    replicas_to_move(model::node_id id, uint32_t left)
      : id(id)
      , left_to_move(left) {}

    model::node_id id;
    uint32_t left_to_move;
    friend std::ostream&
    operator<<(std::ostream& o, const replicas_to_move& r) {
        fmt::print(o, "{{id: {}, left_to_move: {}}}", r.id, r.left_to_move);
        return o;
    }
};

void members_backend::calculate_reallocations_after_decommissioned(
  members_backend::update_meta& meta) const {
    // reallocate all partitions for which any of replicas is placed on
    // decommissioned node
    for (const auto& [tp_ns, cfg] : _topics.local().topics_map()) {
        if (!cfg.is_topic_replicable()) {
            continue;
        }

        for (const auto& pas : cfg.get_assignments()) {
            model::ntp ntp(tp_ns.ns, tp_ns.tp, pas.id);
            if (is_in_replica_set(pas.replicas, meta.update.id)) {
                auto previous_replica_set
                  = _topics.local().get_previous_replica_set(ntp);
                // update in progress, request cancel
                if (previous_replica_set) {
                    partition_reallocation reallocation(std::move(ntp));
                    reallocation.state = reallocation_state::request_cancel;
                    reallocation.current_replica_set = pas.replicas;
                    reallocation.new_replica_set = previous_replica_set.value();
                    meta.partition_reallocations.push_back(
                      std::move(reallocation));
                } else {
                    partition_reallocation reallocation(
                      std::move(ntp), pas.replicas.size());
                    reallocation.replicas_to_remove.emplace(meta.update.id);
                    meta.partition_reallocations.push_back(
                      std::move(reallocation));
                }
            }
        }
    }
}
void members_backend::calculate_reallocations_after_node_added(
  members_backend::update_meta& meta) const {
    if (
      config::shard_local_cfg().partition_autobalancing_mode()
      == model::partition_autobalancing_mode::off) {
        return;
    }
    auto& topics = _topics.local().topics_map();
    struct node_info {
        size_t replicas_count;
        size_t max_capacity;
    };
    // 1. count current node allocations
    absl::node_hash_map<model::node_id, node_info> node_replicas;

    size_t total_replicas = 0;
    for (const auto& [id, n] : _allocator.local().state().allocation_nodes()) {
        if (!node_replicas.contains(id)) {
            node_replicas.emplace(
              id,
              node_info{
                .replicas_count = 0,
                .max_capacity = n->max_capacity(),
              });
        }
        auto it = node_replicas.find(id);
        it->second.replicas_count += n->allocated_partitions();
        total_replicas += n->allocated_partitions();
    }

    // 2. calculate number of replicas per node leading to even replica per
    // node distribution
    auto target_replicas_per_node
      = total_replicas / _allocator.local().state().available_nodes();

    // 3. calculate how many replicas have to be moved from each node
    std::vector<replicas_to_move> to_move_from_node;
    for (auto& [id, info] : node_replicas) {
        auto to_move = info.replicas_count
                       - std::min(
                         target_replicas_per_node, info.replicas_count);
        to_move_from_node.emplace_back(id, to_move);
    }

    auto all_empty = std::all_of(
      to_move_from_node.begin(),
      to_move_from_node.end(),
      [](const replicas_to_move& m) { return m.left_to_move == 0; });
    // nothing to do, exit early
    if (all_empty) {
        return;
    }

    auto cmp = [](const replicas_to_move& lhs, const replicas_to_move& rhs) {
        return lhs.left_to_move < rhs.left_to_move;
    };

    // 4. Pass over all partition metadata once, try to move until we reach even
    // number of partitions per core on each node
    for (auto& [tp_ns, metadata] : topics) {
        // do not try to move internal partitions
        if (
          tp_ns.ns == model::kafka_internal_namespace
          || tp_ns.ns == model::redpanda_ns) {
            continue;
        }
        if (!metadata.is_topic_replicable()) {
            continue;
        }
        // do not move topics that were created after node was added, they are
        // allocated with new cluster capacity
        if (metadata.get_revision() > meta.update.offset()) {
            vlog(
              partlog.info,
              "skipping reallocating topic {}, its revision {} is greater than "
              "node update {}",
              tp_ns,
              metadata.get_revision(),
              meta.update.offset);
            continue;
        }
        for (const auto& p : metadata.get_assignments()) {
            std::erase_if(to_move_from_node, [](const replicas_to_move& v) {
                return v.left_to_move == 0;
            });

            std::sort(to_move_from_node.begin(), to_move_from_node.end(), cmp);
            for (auto& to_move : to_move_from_node) {
                if (
                  is_in_replica_set(p.replicas, to_move.id)
                  && to_move.left_to_move > 0) {
                    partition_reallocation reallocation(
                      model::ntp(tp_ns.ns, tp_ns.tp, p.id), p.replicas.size());

                    reallocation.replicas_to_remove.emplace(to_move.id);
                    meta.partition_reallocations.push_back(
                      std::move(reallocation));
                    to_move.left_to_move--;
                    break;
                }
            }
        }
    }

    vlog(
      partlog.warn,
      "For {} generated {} reallocations, target_replicas_per_node: {}, "
      "total_replicas: {}",
      meta.update,
      target_replicas_per_node,
      total_replicas,
      meta.partition_reallocations.size());
}

std::vector<model::ntp> members_backend::ntps_moving_from_node_older_than(
  model::node_id node, model::revision_id revision) const {
    std::vector<model::ntp> ret;

    for (const auto& [ntp, state] : _topics.local().in_progress_updates()) {
        if (state.update_revision < revision) {
            continue;
        }
        if (!contains_node(state.previous_replicas, node)) {
            continue;
        }

        auto current_assignment = _topics.local().get_partition_assignment(ntp);
        if (unlikely(!current_assignment)) {
            continue;
        }

        if (!contains_node(current_assignment->replicas, node)) {
            ret.push_back(ntp);
        }
    }
    return ret;
}

void members_backend::calculate_reallocations_after_recommissioned(
  update_meta& meta) const {
    auto it = _decommission_command_revision.find(meta.update.id);
    vassert(
      it != _decommission_command_revision.end(),
      "members backend should hold a revision of nodes being decommissioned, "
      "node_id: {}",
      meta.update.id);
    auto ntps = ntps_moving_from_node_older_than(meta.update.id, it->second);
    // reallocate all partitions for which any of replicas is placed on
    // decommissioned node
    meta.partition_reallocations.reserve(ntps.size());
    for (auto& ntp : ntps) {
        partition_reallocation reallocation(ntp);
        reallocation.state = reallocation_state::request_cancel;
        auto current_assignment = _topics.local().get_partition_assignment(ntp);
        auto previous_replica_set = _topics.local().get_previous_replica_set(
          ntp);
        if (
          !current_assignment.has_value()
          || !previous_replica_set.has_value()) {
            continue;
        }
        reallocation.current_replica_set = std::move(
          current_assignment->replicas);
        reallocation.new_replica_set = std::move(*previous_replica_set);

        meta.partition_reallocations.push_back(std::move(reallocation));
    }
}

ss::future<> members_backend::reconcile() {
    // if nothing to do, wait
    co_await _new_updates.wait([this] { return !_updates.empty(); });
    vlog(clusterlog.warn, "reconcile() found {} updates", _updates.size());
    auto u = co_await _lock.get_units();
    vlog(clusterlog.warn, "reconcile() got lock");

    // remove stored revisions of previous decommissioning nodes, this will only
    // happen when update is finished and it is either decommissioning or
    // recommissioning of a node
    for (const auto& meta : _updates) {
        const bool is_decommission
          = meta.update.type
            == members_manager::node_update_type::decommissioned;
        const bool is_recommission
          = meta.update.type
            == members_manager::node_update_type::recommissioned;

        if (meta.finished && (is_decommission || is_recommission)) {
            _decommission_command_revision.erase(meta.update.id);
        }
    }
    // remove finished updates
    std::erase_if(
      _updates, [](const update_meta& meta) { return meta.finished; });

    if (!_raft0->is_elected_leader() || _updates.empty()) {
        vlog(
          clusterlog.warn,
          "reconcile() early exit, leader: {}, empty(): {}",
          _raft0->is_elected_leader(),
          _updates.empty());
        co_return;
    }

    // use linearizable barrier to make sure leader is up to date and all
    // changes are applied

    vlog(clusterlog.warn, "reconcile() before barrier");
    auto barrier_result = co_await _raft0->linearizable_barrier();
    vlog(clusterlog.warn, "reconcile() after barrier");
    if (
      barrier_result.has_error()
      || barrier_result.value() < _raft0->dirty_offset()) {
        if (!barrier_result) {
            vlog(
              partlog.info,
              "error waiting for all raft0 updates to be applied - {}",
              barrier_result.error().message());
            vlog(
              clusterlog.warn,
              "reconcile() error waiting for all raft0 updates to be applied - "
              "{}",
              barrier_result.error().message());

        } else {
            vlog(
              clusterlog.warn,
              "reconcile() waiting for all raft0 updates to be applied - "
              "barrier offset: "
              "{}, raft_0 dirty offset: {}",
              barrier_result.value(),
              _raft0->dirty_offset());
        }
        co_return;
    }

    // process one update at a time
    vassert(!_updates.empty(), "_updates was empty");
    auto& meta = _updates.front();

    // leadership changed, drop not yet requested reallocations to make sure
    // there is no stale state
    auto current_term = _raft0->term();
    if (_last_term != current_term) {
        vlog(
          clusterlog.warn,
          "reconcile() leadership changed {} {}",
          _last_term,
          current_term);
        for (auto& reallocation : meta.partition_reallocations) {
            if (reallocation.state == reallocation_state::reassigned) {
                reallocation.release_assignment_units();
                reallocation.new_replica_set.clear();
                reallocation.state = reallocation_state::initial;
            }
        }
    }
    _last_term = current_term;

    vlog(
      clusterlog.warn,
      "reconcile() try_to_finish for {} with {} reallocs, {} updates remaining",
      meta.update,
      meta.partition_reallocations.size(),
      _updates.size());

    co_await try_to_finish_update(meta);

    vlog(
      clusterlog.warn,
      "[update: {}] reconciliation loop - reallocations: {}, finished: {}",
      meta.update,
      meta.partition_reallocations,
      meta.finished);

    // calculate necessary reallocations
    if (meta.partition_reallocations.empty()) {
        calculate_reallocations(meta);
        // if there is nothing to reallocate, just finish this update
        vlog(
          partlog.warn,
          "[update: {}] calculated reallocations: {}",
          meta.update,
          meta.partition_reallocations);
        if (
          meta.partition_reallocations.empty()
          && meta.update.type == members_manager::node_update_type::added) {
            auto err = co_await _members_frontend.local()
                         .finish_node_reallocations(meta.update.id);
            if (!err) {
                meta.finished = true;
            }
            vlog(
              partlog.warn,
              "[update: {}] no needed reallocations, finished: {}",
              meta.update,
              meta.finished);
            co_return;
        }
    }

    // execute reallocations
    co_await ss::parallel_for_each(
      meta.partition_reallocations,
      [this](partition_reallocation& reallocation) {
          return reallocate_replica_set(reallocation);
      });

    // remove those decommissioned nodes which doesn't have any pending
    // reallocations
    if (meta.update.type == members_manager::node_update_type::decommissioned) {
        auto node = _members.local().get_broker(meta.update.id);
        if (!node) {
            vlog(
              clusterlog.debug,
              "reconcile: node {} is gone, returning",
              meta.update.id);
            co_return;
        }
        const auto is_draining = node.value()->get_membership_state()
                                 == model::membership_state::draining;
        const auto all_reallocations_finished = std::all_of(
          meta.partition_reallocations.begin(),
          meta.partition_reallocations.end(),
          [](const partition_reallocation& r) {
              return r.state == reallocation_state::finished;
          });
        const bool updates_in_progress
          = _topics.local().has_updates_in_progress();

        const auto allocator_empty = _allocator.local().is_empty(
          meta.update.id);
        if (
          is_draining && all_reallocations_finished && allocator_empty
          && !updates_in_progress) {
            // we can safely discard the result since action is going to be
            // retried if it fails
            vlog(
              clusterlog.info,
              "[update: {}] decommissioning finished, removing node from "
              "cluster",
              meta.update);
            // workaround: https://github.com/redpanda-data/redpanda/issues/891
            std::vector<model::node_id> ids{meta.update.id};
            co_await _raft0
              ->remove_members(std::move(ids), model::revision_id{0})
              .discard_result();
        } else {
            // Decommissioning still in progress
            vlog(
              clusterlog.info,
              "[update: {}] decommissioning in progress. draining: {} "
              "all_reallocations_finished: {}, allocator_empty: {} "
              "updates_in_progress:{}",
              meta.update,
              is_draining,
              all_reallocations_finished,
              allocator_empty,
              updates_in_progress);
            if (!allocator_empty && all_reallocations_finished) {
                // recalculate reallocations
                vlog(
                  clusterlog.info,
                  "[update: {}] decommissioning in progress. recalculating "
                  "reallocations",
                  meta.update);
                calculate_reallocations(meta);
            }
        }
    }
}

ss::future<>
members_backend::try_to_finish_update(members_backend::update_meta& meta) {
    // broker was removed, finish
    if (!_members.local().contains(meta.update.id)) {
        vlog(
          clusterlog.warn,
          "reconcile() finished update for {} as it was removed",
          meta.update);
        meta.finished = true;
    }

    // topic was removed, mark reallocation as finished
    for (auto& reallocation : meta.partition_reallocations) {
        if (!_topics.local().contains(
              model::topic_namespace_view(reallocation.ntp),
              reallocation.ntp.tp.partition)) {
            reallocation.state = reallocation_state::finished;
        }
    }
    // we do not have to check if all reallocations are finished, we will finish
    // the update when node will be removed
    if (meta.update.type == members_manager::node_update_type::decommissioned) {
        co_return;
    }

    auto is_finished = [](const partition_reallocation& r) {
        return r.state == reallocation_state::finished;
    };

    // if all reallocations are propagate reallocation finished event and mark
    // update as finished
    const auto all_reallocations_finished = std::all_of(
      meta.partition_reallocations.begin(),
      meta.partition_reallocations.end(),
      is_finished);

    const auto finished_count = std::count_if(
      meta.partition_reallocations.begin(),
      meta.partition_reallocations.end(),
      is_finished);

    vlog(
      clusterlog.warn,
      "reconcile() try_to_finish() {}: {}/{} finished",
      meta.update,
      finished_count,
      meta.partition_reallocations.size());

    if (all_reallocations_finished && !meta.partition_reallocations.empty()) {
        vlog(
          clusterlog.warn,
          "reconcile() try_to_finish() {}: begin finish_node_reallocations",
          meta.update);
        auto err = co_await _members_frontend.local().finish_node_reallocations(
          meta.update.id);
        vlog(
          clusterlog.warn,
          "reconcile() try_to_finish() {}: end finish_node_reallocations: {}",
          meta.update,
          err);
        if (!err) {
            meta.finished = true;
        }
    }
}

void members_backend::reassign_replicas(
  partition_assignment& current_assignment,
  partition_reallocation& reallocation) {
    vlog(
      clusterlog.debug,
      "[ntp: {}, {} -> -]  trying to reassign partition replicas",
      reallocation.ntp,
      current_assignment.replicas);

    // remove nodes that are going to be reassigned from current assignment.
    std::erase_if(
      current_assignment.replicas,
      [&reallocation](const model::broker_shard& bs) {
          return reallocation.replicas_to_remove.contains(bs.node_id);
      });

    auto res = _allocator.local().reallocate_partition(
      reallocation.constraints.value(), current_assignment);
    if (res.has_value()) {
        reallocation.set_new_replicas(std::move(res.value()));
    }
}

ss::future<> members_backend::reallocate_replica_set(
  members_backend::partition_reallocation& meta) {
    auto current_assignment = _topics.local().get_partition_assignment(
      meta.ntp);
    // topic was deleted, we are done with reallocation
    if (!current_assignment) {
        meta.state = reallocation_state::finished;
        co_return;
    }

    switch (meta.state) {
    case reallocation_state::initial: {
        meta.current_replica_set = current_assignment->replicas;
        // initial state, try to reassign partition replicas

        reassign_replicas(*current_assignment, meta);
        if (meta.new_replica_set.empty()) {
            // if partition allocator failed to reassign partitions return
            // and wait for next retry
            vlog(
              clusterlog.warn,
              "[ntp: {}, {} -> -] new partition assignment failed (empty)",
              meta.ntp,
              meta.new_replica_set);
            co_return;
        }
        // success, update state and move on
        meta.state = reallocation_state::reassigned;
        vlog(
          clusterlog.warn,
          "[ntp: {}, {} -> {}] new partition assignment calculated "
          "successfully",
          meta.ntp,
          meta.current_replica_set,
          meta.new_replica_set);
        [[fallthrough]];
    }
    case reallocation_state::reassigned: {
        vassert(
          !meta.new_replica_set.empty(),
          "reallocation meta in reassigned state must have new_assignment");
        vlog(
          clusterlog.warn,
          "[ntp: {}, {} -> {}] dispatching request to move partition",
          meta.ntp,
          meta.current_replica_set,
          meta.new_replica_set);
        // request topic partition move
        std::error_code error
          = co_await _topics_frontend.local().move_partition_replicas(
            meta.ntp,
            meta.new_replica_set,
            model::timeout_clock::now() + _retry_timeout);
        if (error) {
            vlog(
              clusterlog.warn,
              "[ntp: {}, {} -> {}] partition move error: {}",
              meta.ntp,
              meta.current_replica_set,
              meta.new_replica_set,
              error.message());
            co_return;
        }
        // success, update state and move on
        meta.state = reallocation_state::requested;
        meta.release_assignment_units();
        [[fallthrough]];
    }
    case reallocation_state::requested: {
        // wait for partition replicas to be moved
        auto reconciliation_state
          = co_await _api.local().get_reconciliation_state(meta.ntp);
        vlog(
          clusterlog.warn,
          "[ntp: {}, {} -> {}] reconciliation state: {}, pending operations: "
          "{}",
          meta.ntp,
          meta.current_replica_set,
          meta.new_replica_set,
          reconciliation_state.status(),
          reconciliation_state.pending_operations());
        if (reconciliation_state.status() != reconciliation_status::done) {
            co_return;
        }
        meta.state = reallocation_state::finished;
        [[fallthrough]];
    }
    case reallocation_state::finished:
        co_return;
    case reallocation_state::request_cancel: {
        std::error_code error
          = co_await _topics_frontend.local().cancel_moving_partition_replicas(
            meta.ntp, model::timeout_clock::now() + _retry_timeout);
        if (error) {
            vlog(
              clusterlog.warn,
              "[ntp: {}, {} -> {}] partition reconfiguration cancellation "
              "error: {}",
              meta.ntp,
              meta.current_replica_set,
              meta.new_replica_set,
              error.message());
            if (error == errc::no_update_in_progress) {
                // mark reallocation as finished, reallocations will be
                // recalculated if required
                meta.state = reallocation_state::finished;
            }
            co_return;
        }
        // success, update state and move on
        meta.state = reallocation_state::cancelled;
        [[fallthrough]];
    }
    case reallocation_state::cancelled: {
        auto reconciliation_state
          = co_await _api.local().get_reconciliation_state(meta.ntp);
        vlog(
          clusterlog.warn,
          "[ntp: {}, {} -> {}] reconciliation state: {}, pending operations: "
          "{}",
          meta.ntp,
          meta.current_replica_set,
          meta.new_replica_set,
          reconciliation_state.status(),
          reconciliation_state.pending_operations());
        if (reconciliation_state.status() != reconciliation_status::done) {
            co_return;
        }
        meta.state = reallocation_state::finished;
        co_return;
    };
    }
}

void members_backend::handle_recommissioned(
  const members_manager::node_update& update) {
    stop_node_decommissioning(update.id);
}

void members_backend::stop_node_decommissioning(model::node_id id) {
    if (_members.local().get_broker(id) == std::nullopt) {
        return;
    }
    // remove all pending decommissioned updates for this node
    std::erase_if(_updates, [id](update_meta& meta) {
        return meta.update.id == id
               && meta.update.type
                    == members_manager::node_update_type::decommissioned;
    });
}

void members_backend::stop_node_addition(model::node_id id) {
    // remove all pending added updates for current node
    std::erase_if(_updates, [id](update_meta& meta) {
        return meta.update.id == id
               && meta.update.type == members_manager::node_update_type::added;
    });
}

void members_backend::handle_reallocation_finished(model::node_id id) {
    // remove all pending added node updates for this node
    std::erase_if(_updates, [id](update_meta& meta) {
        return meta.update.id == id
               && meta.update.type == members_manager::node_update_type::added;
    });
}

std::ostream&
operator<<(std::ostream& o, const members_backend::partition_reallocation& r) {
    fmt::print(
      o,
      "{{ntp: {}, constraints: {},  allocated: {}, state: "
      "{},replicas_to_remove: [",
      r.ntp,
      r.constraints,
      !r.new_replica_set.empty(),
      r.state);

    if (!r.replicas_to_remove.empty()) {
        auto it = r.replicas_to_remove.begin();
        fmt::print(o, "{}", *it);
        ++it;
        for (; it != r.replicas_to_remove.end(); ++it) {
            fmt::print(o, ", {}", *it);
        }
    }
    fmt::print(o, "]}}");
    return o;
}
std::ostream&
operator<<(std::ostream& o, const members_backend::reallocation_state& state) {
    switch (state) {
    case members_backend::reallocation_state::initial:
        return o << "initial";
    case members_backend::reallocation_state::reassigned:
        return o << "reassigned";
    case members_backend::reallocation_state::requested:
        return o << "requested";
    case members_backend::reallocation_state::finished:
        return o << "finished";
    case members_backend::reallocation_state::request_cancel:
        return o << "request_cancel";
    case members_backend::reallocation_state::cancelled:
        return o << "cancelled";
    }

    __builtin_unreachable();
}
} // namespace cluster
