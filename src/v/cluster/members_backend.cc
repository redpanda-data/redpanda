#include "cluster/members_backend.h"

#include "cluster/controller_api.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/members_frontend.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/scheduling/allocation_strategy.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "prometheus/prometheus_sanitize.h"
#include "random/generators.h"

#include <seastar/core/coroutine.hh>

namespace cluster {
namespace {

void reassign_replicas(
  const model::ntp& ntp,
  partition_allocator& allocator,
  partition_assignment current_assignment,
  members_backend::partition_reallocation& reallocation) {
    auto res = allocator.reallocate_partition(
      model::topic_namespace{ntp.ns, ntp.tp.topic},
      reallocation.constraints.value(),
      current_assignment,
      get_allocation_domain(ntp),
      std::vector(
        reallocation.replicas_to_remove.begin(),
        reallocation.replicas_to_remove.end()));
    if (res.has_value()) {
        reallocation.set_new_replicas(std::move(res.value()));
    } else {
        vlog(
          clusterlog.info,
          "failed to reallocate partition {} with assignment {}, error: {}",
          ntp,
          current_assignment.replicas,
          res.error());
    }
}

} // namespace

members_backend::members_backend(
  ss::sharded<cluster::topics_frontend>& topics_frontend,
  ss::sharded<topic_table>& topics,
  ss::sharded<partition_allocator>& allocator,
  ss::sharded<members_table>& members,
  ss::sharded<controller_api>& api,
  ss::sharded<members_manager>& members_manager,
  ss::sharded<members_frontend>& members_frontend,
  ss::sharded<features::feature_table>& features,
  consensus_ptr raft0,
  ss::sharded<ss::abort_source>& as)
  : _topics_frontend(topics_frontend)
  , _topics(topics)
  , _allocator(allocator)
  , _members(members)
  , _api(api)
  , _members_manager(members_manager)
  , _members_frontend(members_frontend)
  , _features(features)
  , _raft0(raft0)
  , _as(as)
  , _retry_timeout(config::shard_local_cfg().members_backend_retry_ms())
  , _max_concurrent_reallocations(
      config::shard_local_cfg()
        .partition_autobalancing_concurrent_moves.bind()) {
    setup_metrics();
    ssx::spawn_with_gate(_bg, [this] {
        return ss::do_until(
          [this] { return _as.local().abort_requested(); },
          [this] { return handle_updates(); });
    });
}

ss::future<> members_backend::stop() {
    vlog(clusterlog.info, "Stopping Members Backend...");
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
    start_reconciliation_loop();
    ssx::spawn_with_gate(_bg, [this] { return reconcile_raft0_updates(); });
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
    vlog(clusterlog.debug, "membership update received: {}", update);
    switch (update.type) {
    case node_update_type::recommissioned: {
        stop_node_decommissioning(update.id);
        _updates.emplace_back(update);
        _new_updates.signal();
        return;
    }
    case node_update_type::reallocation_finished:
        handle_reallocation_finished(update.id);
        return;
    case node_update_type::added:
        stop_node_decommissioning(update.id);
        _raft0_updates.push_back(update);
        _new_updates.broadcast();
        return;
    case node_update_type::decommissioned:
        _updates.emplace_back(update);
        stop_node_addition(update.id);
        _new_updates.broadcast();
        return;
    case node_update_type::removed:
        // remove all pending updates for this node
        std::erase_if(_updates, [id = update.id](update_meta& meta) {
            return meta.update.id == id;
        });
        _raft0_updates.push_back(update);
        _new_updates.broadcast();
        return;
    case node_update_type::interrupted:
        model::node_id id = update.id;
        // remove all pending updates for this node
        std::erase_if(
          _updates, [id](update_meta& meta) { return meta.update.id == id; });
        _raft0_updates.erase(
          std::remove_if(
            _raft0_updates.begin(),
            _raft0_updates.end(),
            [id](auto& update) { return update.id == id; }),
          _raft0_updates.end());
        return;
    }

    __builtin_unreachable();
}

void members_backend::start_reconciliation_loop() {
    ssx::spawn_with_gate(_bg, [this] { return reconciliation_loop(); });
}

ss::future<> members_backend::reconciliation_loop() {
    while (!_as.local().abort_requested()) {
        try {
            auto ec = co_await reconcile();
            vlog(
              clusterlog.trace, "reconciliation loop result: {}", ec.message());
            if (!ec) {
                continue;
            }
        } catch (...) {
            vlog(
              clusterlog.info,
              "error encountered while handling cluster state reconciliation - "
              "{}",
              std::current_exception());
        }
        // when an error occurred wait before next retry
        co_await ss::sleep_abortable(_retry_timeout, _as.local());
    }
}

ss::future<> members_backend::calculate_reallocations(update_meta& meta) {
    // update caused by node event i.e. addition/decommissioning/recommissioning
    switch (meta.update.type) {
    case node_update_type::decommissioned:
    case node_update_type::added:
        // let partition balancer take care of it
        co_return;
    case node_update_type::recommissioned:
        co_await calculate_reallocations_after_recommissioned(meta);
        co_return;
    case node_update_type::reallocation_finished:
    case node_update_type::removed:
    case node_update_type::interrupted:
        co_return;
    }
}

std::vector<model::ntp> members_backend::ntps_moving_from_node_older_than(
  model::node_id node, model::revision_id revision) const {
    std::vector<model::ntp> ret;

    for (const auto& [ntp, state] : _topics.local().updates_in_progress()) {
        if (state.get_update_revision() < revision) {
            continue;
        }
        if (!contains_node(state.get_previous_replicas(), node)) {
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

ss::future<> members_backend::calculate_reallocations_after_recommissioned(
  update_meta& meta) {
    vassert(
      meta.update.decommission_update_revision,
      "Decommission update revision must be present for recommission "
      "update "
      "metadata");

    auto ntps = ntps_moving_from_node_older_than(
      meta.update.id, meta.update.decommission_update_revision.value());
    // reallocate all partitions for which any of replicas is placed on
    // decommissioned node
    meta.partition_reallocations.reserve(ntps.size());
    for (auto& ntp : ntps) {
        // skip over reallocations that are already present
        if (meta.partition_reallocations.contains(ntp)) {
            continue;
        }
        partition_reallocation reallocation;
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

        meta.partition_reallocations.emplace(
          std::move(ntp), std::move(reallocation));
    }
    co_return;
}

ss::future<std::error_code> members_backend::reconcile() {
    // if nothing to do, wait
    co_await _new_updates.wait([this] { return !_updates.empty(); });
    auto u = co_await _lock.get_units();

    // remove finished updates
    std::erase_if(
      _updates, [](const update_meta& meta) { return meta.finished; });

    if (_updates.empty()) {
        co_return errc::success;
    }

    if (!_raft0->is_elected_leader()) {
        co_return errc::not_leader;
    }

    // use linearizable barrier to make sure leader is up to date and all
    // changes are applied
    auto barrier_result = co_await _raft0->linearizable_barrier();
    if (
      barrier_result.has_error()
      || barrier_result.value() < _raft0->dirty_offset()) {
        if (!barrier_result) {
            vlog(
              clusterlog.debug,
              "error waiting for all raft0 updates to be applied - {}",
              barrier_result.error().message());
            co_return barrier_result.error();
        } else {
            vlog(
              clusterlog.trace,
              "waiting for all raft0 updates to be applied - barrier "
              "offset: "
              "{}, raft_0 dirty offset: {}",
              barrier_result.value(),
              _raft0->dirty_offset());
        }
        co_return errc::not_leader;
    }

    // process one update at a time
    vassert(!_updates.empty(), "_updates was empty");
    auto& meta = _updates.front();

    // leadership changed, drop not yet requested reallocations to make sure
    // there is no stale state
    auto current_term = _raft0->term();
    if (_last_term != current_term) {
        for (auto& [_, reallocation] : meta.partition_reallocations) {
            if (reallocation.state == reallocation_state::reassigned) {
                reallocation.release_allocated();
                reallocation.new_replica_set.clear();
                reallocation.state = reallocation_state::initial;
            }
        }
    }
    _last_term = current_term;

    co_await try_to_finish_update(meta);

    vlog(
      clusterlog.info,
      "[update: {}] reconciliation loop - pending reallocation count: {}, "
      "finished: {}",
      meta.update,
      meta.partition_reallocations.size(),
      meta.finished);

    // if update is finished, exit early
    if (meta.finished) {
        co_return errc::success;
    }

    // calculate necessary reallocations
    if (meta.partition_reallocations.size() < _max_concurrent_reallocations()) {
        co_await calculate_reallocations(meta);
        // if there is nothing to reallocate, just finish this update
        vlog(
          clusterlog.info,
          "[update: {}] calculated reallocations: {}",
          meta.update,
          meta.partition_reallocations);
        if (should_stop_rebalancing_update(meta)) {
            auto err = co_await _members_frontend.local()
                         .finish_node_reallocations(meta.update.id);
            if (err) {
                vlog(
                  clusterlog.info,
                  "[update: {}] reconciliation loop - error finishing "
                  "update - {}",
                  meta.update,
                  err.message());
                co_return err;
            }
            meta.finished = true;

            vlog(
              clusterlog.debug,
              "[update: {}] no need reallocations, finished: {}",
              meta.update,
              meta.finished);
            co_return errc::success;
        }
    }

    // execute reallocations
    co_await ss::parallel_for_each(
      meta.partition_reallocations, [this](auto& pair) {
          return reconcile_reallocation_state(pair.first, pair.second);
      });

    // remove those decommissioned nodes which doesn't have any pending
    // reallocations
    if (meta.update.type == node_update_type::decommissioned) {
        auto node = _members.local().get_node_metadata_ref(meta.update.id);
        if (!node) {
            vlog(
              clusterlog.debug,
              "reconcile: node {} is gone, returning",
              meta.update.id);
            co_return errc::success;
        }
        const auto is_draining = node->get().state.get_membership_state()
                                 == model::membership_state::draining;

        const auto allocator_empty = _allocator.local().is_empty(
          meta.update.id);
        if (is_draining && allocator_empty) {
            // we can safely discard the result since action is going to be
            // retried if it fails
            vlog(
              clusterlog.info,
              "[update: {}] decommissioning finished, removing node from "
              "cluster",
              meta.update);
            co_await do_remove_node(meta.update.id);
        } else {
            // Decommissioning still in progress
            vlog(
              clusterlog.info,
              "[update: {}] decommissioning in progress. "
              "draining: {}, allocator_empty: {}",
              meta.update,
              is_draining,
              allocator_empty);
        }
    }
    // remove finished reallocations
    absl::erase_if(meta.partition_reallocations, [](const auto& r) {
        return r.second.state == reallocation_state::finished;
    });

    co_return errc::update_in_progress;
}

ss::future<std::error_code> members_backend::do_remove_node(model::node_id id) {
    if (!_features.local().is_active(
          features::feature::membership_change_controller_cmds)) {
        return _raft0->remove_member(id, model::revision_id{0});
    }
    return _members_frontend.local().remove_node(id);
}

bool members_backend::should_stop_rebalancing_update(
  const members_backend::update_meta& meta) const {
    // do not finish decommissioning and recommissioning updates as they
    // have strict stop conditions
    using update_t = node_update_type;
    if (
      meta.update.type == update_t::decommissioned
      || meta.update.type == update_t::reallocation_finished) {
        return false;
    }

    return meta.partition_reallocations.empty();
}

ss::future<>
members_backend::try_to_finish_update(members_backend::update_meta& meta) {
    // broker was removed, finish
    if (!_members.local().contains(meta.update.id)) {
        meta.finished = true;
    }

    // topic was removed, mark reallocation as finished
    for (auto& [ntp, reallocation] : meta.partition_reallocations) {
        if (!_topics.local().contains(
              model::topic_namespace_view(ntp), ntp.tp.partition)) {
            reallocation.state = reallocation_state::finished;
        }
    }
    // we do not have to check if all reallocations are finished, we will
    // finish the update when node will be removed
    if (meta.update.type == node_update_type::decommissioned) {
        co_return;
    }

    // if all reallocations are propagate reallocation finished event and
    // mark update as finished
    const auto all_reallocations_finished = std::all_of(
      meta.partition_reallocations.begin(),
      meta.partition_reallocations.end(),
      [](const auto& r) {
          return r.second.state == reallocation_state::finished;
      });

    if (all_reallocations_finished && !meta.partition_reallocations.empty()) {
        auto err = co_await _members_frontend.local().finish_node_reallocations(
          meta.update.id);
        if (!err) {
            meta.finished = true;
        }
    }
}

ss::future<> members_backend::reconcile_reallocation_state(
  const model::ntp& ntp, members_backend::partition_reallocation& meta) {
    auto current_assignment = _topics.local().get_partition_assignment(ntp);
    // topic was deleted, we are done with reallocation
    if (!current_assignment) {
        meta.state = reallocation_state::finished;
        co_return;
    }

    switch (meta.state) {
    case reallocation_state::initial: {
        meta.current_replica_set = current_assignment->replicas;
        // initial state, try to reassign partition replicas

        reassign_replicas(
          ntp, _allocator.local(), std::move(*current_assignment), meta);
        if (meta.new_replica_set.empty()) {
            // if partition allocator failed to reassign partitions return
            // and wait for next retry
            co_return;
        }
        // success, update state and move on
        meta.state = reallocation_state::reassigned;
        vlog(
          clusterlog.info,
          "[ntp: {}, {} -> {}] new partition assignment calculated "
          "successfully",
          ntp,
          meta.current_replica_set,
          meta.new_replica_set);
        [[fallthrough]];
    }
    case reallocation_state::reassigned: {
        vassert(
          !meta.new_replica_set.empty(),
          "reallocation meta in reassigned state must have new_assignment");
        vlog(
          clusterlog.info,
          "[ntp: {}, {} -> {}] dispatching request to move partition",
          ntp,
          meta.current_replica_set,
          meta.new_replica_set);
        // request topic partition move
        std::error_code error
          = co_await _topics_frontend.local().move_partition_replicas(
            ntp,
            meta.new_replica_set,
            model::timeout_clock::now() + _retry_timeout);
        if (error) {
            vlog(
              clusterlog.info,
              "[ntp: {}, {} -> {}] partition move error: {}",
              ntp,
              meta.current_replica_set,
              meta.new_replica_set,
              error.message());
            if (error == errc::update_in_progress) {
                // Skip meta for this partition as it as already moving
                meta.state = reallocation_state::finished;
            }
            co_return;
        }
        // success, update state and move on
        meta.state = reallocation_state::requested;
        meta.release_allocated();
        [[fallthrough]];
    }
    case reallocation_state::requested: {
        // wait for partition replicas to be moved
        auto reconciliation_state
          = co_await _api.local().get_reconciliation_state(ntp);
        vlog(
          clusterlog.info,
          "[ntp: {}, {} -> {}] reconciliation state: {}, pending "
          "operations: "
          "{}",
          ntp,
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
            ntp, model::timeout_clock::now() + _retry_timeout);
        if (error) {
            vlog(
              clusterlog.info,
              "[ntp: {}, {} -> {}] partition reconfiguration cancellation "
              "error: {}",
              ntp,
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
          = co_await _api.local().get_reconciliation_state(ntp);
        vlog(
          clusterlog.info,
          "[ntp: {}, {} -> {}] reconciliation state: {}, pending "
          "operations: "
          "{}",
          ntp,
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

void members_backend::stop_node_decommissioning(model::node_id id) {
    if (!_members.local().contains(id)) {
        return;
    }
    // remove all pending decommissioned updates for this node
    std::erase_if(_updates, [id](update_meta& meta) {
        return meta.update.id == id
               && meta.update.type == node_update_type::decommissioned;
    });
}

void members_backend::stop_node_addition(model::node_id id) {
    using update_t = node_update_type;
    // remove all pending added updates for current node
    std::erase_if(_updates, [id](update_meta& meta) {
        return meta.update.id == id && meta.update.type == update_t::added;
    });

    // sort updates to prioritize decommissions/recommissions over node
    // additions, use stable sort to keep de/recommissions order
    static auto is_de_or_recommission = [](const update_meta& meta) {
        return meta.update.type == update_t::decommissioned
               || meta.update.type == update_t::recommissioned;
    };

    std::stable_sort(
      _updates.begin(),
      _updates.end(),
      [](const update_meta& lhs, const update_meta& rhs) {
          return is_de_or_recommission(lhs) && !is_de_or_recommission(rhs);
      });
}

void members_backend::handle_reallocation_finished(model::node_id id) {
    // remove all pending added node updates for this node
    std::
      erase_if(
        _updates, [id](update_meta& meta) {
            return meta.update.id == id
                 && (meta.update.type
                      == node_update_type::added
               || meta.update.type
                    == node_update_type::recommissioned);
        });
}

ss::future<> members_backend::reconcile_raft0_updates() {
    vlog(clusterlog.trace, "starting raft 0 reconciliation");
    while (!_as.local().abort_requested()) {
        co_await _new_updates.wait([this] { return !_raft0_updates.empty(); });
        vlog(
          clusterlog.trace, "raft_0 updates_size: {}", _raft0_updates.size());
        // check the _raft0_updates as the predicate may not longer hold
        if (_raft0_updates.empty()) {
            continue;
        }

        auto update = _raft0_updates.front();

        if (!update.need_raft0_update) {
            vlog(clusterlog.trace, "skipping raft 0 update: {}", update);
            _raft0_updates.pop_front();
            continue;
        }
        vlog(clusterlog.trace, "processing raft 0 update: {}", update);
        auto err = co_await update_raft0_configuration(update);
        if (err) {
            vlog(
              clusterlog.trace,
              "raft 0 update {} returned an error - {}",
              update,
              err.message());
            co_await ss::sleep_abortable(200ms, _as.local());
            continue;
        }

        _raft0_updates.pop_front();
    }
}

ss::future<std::error_code> members_backend::update_raft0_configuration(
  const members_manager::node_update& update) {
    model::revision_id revision(update.offset);
    auto cfg = _raft0->config();
    if (cfg.revision_id() > model::revision_id(update.offset)) {
        co_return errc::success;
    }
    if (update.type == node_update_type::added) {
        if (cfg.contains(raft::vnode(update.id, raft0_revision))) {
            vlog(
              clusterlog.debug,
              "node {} is already part of raft0 configuration",
              update.id);
            co_return errc::success;
        }
        co_return co_await add_to_raft0(update.id, revision);
    } else if (update.type == node_update_type::removed) {
        if (!cfg.contains(raft::vnode(update.id, raft0_revision))) {
            vlog(
              clusterlog.debug,
              "node {} is already removed from raft0 configuration",
              update.id);
            co_return errc::success;
        }

        co_return co_await remove_from_raft0(update.id, revision);
    }

    co_return errc::success;
}

ss::future<std::error_code>
members_backend::add_to_raft0(model::node_id id, model::revision_id revision) {
    if (!_raft0->is_leader()) {
        co_return errc::not_leader;
    }

    vlog(clusterlog.info, "adding node {} to raft0 configuration", id);
    co_return co_await _raft0->add_group_member(
      raft::vnode(id, raft0_revision), revision);
}

ss::future<std::error_code> members_backend::remove_from_raft0(
  model::node_id id, model::revision_id revision) {
    if (!_raft0->is_leader()) {
        co_return errc::not_leader;
    }
    vlog(clusterlog.info, "removing node {} from raft0 configuration", id);
    co_return co_await _raft0->remove_member(
      raft::vnode(id, raft0_revision), revision);
}

std::ostream&
operator<<(std::ostream& o, const members_backend::partition_reallocation& r) {
    fmt::print(
      o,
      "{{constraints: {},  allocated: {}, state: {},replicas_to_remove: [",
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
