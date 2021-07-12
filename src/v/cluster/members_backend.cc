#include "cluster/members_backend.h"

#include "cluster/controller_api.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/later.hh>

#include <absl/container/node_hash_set.h>

#include <exception>
#include <optional>
#include <vector>

namespace cluster {

members_backend::members_backend(
  ss::sharded<cluster::topics_frontend>& topics_frontend,
  ss::sharded<topic_table>& topics,
  ss::sharded<partition_allocator>& allocator,
  ss::sharded<members_table>& members,
  ss::sharded<controller_api>& api,
  ss::sharded<members_manager>& members_manager,
  consensus_ptr raft0,
  ss::sharded<ss::abort_source>& as)
  : _topics_frontend(topics_frontend)
  , _topics(topics)
  , _allocator(allocator)
  , _members(members)
  , _api(api)
  , _members_manager(members_manager)
  , _raft0(raft0)
  , _as(as)
  , _retry_timeout(config::shard_local_cfg().members_backend_retry_ms()) {
    _retry_timer.set_callback([this] { start_reconciliation_loop(); });
}

ss::future<> members_backend::stop() {
    _retry_timer.cancel();
    _new_updates.broken();
    return _bg.close();
}

void members_backend::start() {
    (void)ss::with_gate(_bg, [this] {
        return ss::do_until(
          [this] { return _as.local().abort_requested(); },
          [this] { return handle_updates(); });
    });
    _retry_timer.arm(_retry_timeout);
}

ss::future<> members_backend::handle_updates() {
    /**
     * wait for updates from members manager, after update is received we
     * translate it into realocation meta, reallocation meta represents a
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

ss::future<>
members_backend::handle_single_update(members_manager::node_update update) {
    return _lock.with([this, update]() mutable {
        // if node was recommissioned simply remove all decomissioning updates
        if (update.type == members_manager::node_update_type::recommissioned) {
            handle_recommissioned(update);
            return;
        }

        _updates.emplace_back(update);
        _new_updates.signal();
    });
}

void members_backend::start_reconciliation_loop() {
    (void)ss::with_gate(_bg, [this] {
        return reconcile().finally([this] {
            if (!_as.local().abort_requested()) {
                _retry_timer.arm(_retry_timeout);
            }
        });
    }).handle_exception([](const std::exception_ptr& e) {
        // just log an exception, will retry
        vlog(
          clusterlog.trace,
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
        // reallocate all partitons for which any of replicas is placed on
        // decommissioned node
        for (const auto& [tp_ns, cfg] : _topics.local().topics_map()) {
            for (auto& pas : cfg.assignments) {
                if (is_in_replica_set(pas.replicas, meta.update.id)) {
                    meta.partition_reallocations.emplace_back(
                      model::ntp(tp_ns.ns, tp_ns.tp, pas.id));
                }
            }
        }
        return;
    default:
        return;
    }
}

ss::future<> members_backend::reconcile() {
    // if nothing to do, wait
    co_await _new_updates.wait([this] { return !_updates.empty(); });

    auto u = co_await _lock.get_units();

    // remove finished updates
    std::erase_if(
      _updates, [](const update_meta& meta) { return meta.finished; });

    if (!_raft0->is_leader() || _updates.empty()) {
        co_return;
    }
    // process one update at a time
    auto& meta = _updates.front();
    try_to_finish_update(meta);

    // calculate necessary reallocations
    if (meta.partition_reallocations.empty()) {
        calculate_reallocations(meta);
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
            co_return;
        }
        const auto is_draining = node.value()->get_membership_state()
                                 == model::membership_state::draining;
        if (is_draining && _allocator.local().is_empty(meta.update.id)) {
            // we can safely discard the result since action is going to be
            // retried if it fails

            // workaround: https://github.com/vectorizedio/redpanda/issues/891
            std::vector<model::node_id> ids{meta.update.id};
            co_await _raft0
              ->remove_members(std::move(ids), model::revision_id{0})
              .discard_result();
        }
    }
}

void members_backend::try_to_finish_update(
  members_backend::update_meta& meta) {
    // broker was removed, finish
    if (!_members.local().contains(meta.update.id)) {
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
        return;
    }

    // if all reallocations are finished mark update as finished
    const auto all_reallocations_finished = std::all_of(
      meta.partition_reallocations.begin(),
      meta.partition_reallocations.end(),
      [](const partition_reallocation& r) {
          return r.state == reallocation_state::finished;
      });

    if (all_reallocations_finished) {
        meta.finished = true;
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
        // initial state, try to reassign partition replicas
        vlog(
          clusterlog.info,
          "trying to reassign partition {} replicas, current assignment: {}",
          meta.ntp,
          *current_assignment);
        auto new_assignment
          = _allocator.local().reassign_decommissioned_replicas(
            *current_assignment);
        if (new_assignment.has_value()) {
            meta.new_assignment = std::move(new_assignment.value());
        }

        if (!meta.new_assignment) {
            // if partiton allocator failed to reassign partitions return and
            // wait for next retry
            co_return;
        }
        // success, update state and move on
        meta.state = reallocation_state::reassigned;
        [[fallthrough]];
    }
    case reallocation_state::reassigned: {
        vassert(
          meta.new_assignment,
          "reallocation meta in reassigned state must have new_assignment");
        vlog(
          clusterlog.info,
          "requesting partition {} replicas move to {}",
          meta.ntp,
          meta.new_assignment->get_assignments());
        // request topic partition move
        std::error_code error
          = co_await _topics_frontend.local().move_partition_replicas(
            meta.ntp,
            meta.new_assignment->get_assignments().begin()->replicas,
            model::timeout_clock::now() + _retry_timeout);
        if (error) {
            vlog(clusterlog.debug, "partition move error: {}", error.message());
            co_return;
        }
        // success, update state and move on
        meta.state = reallocation_state::requested;
        [[fallthrough]];
    }
    case reallocation_state::requested: {
        vlog(
          clusterlog.info,
          "waiting for partition {} replicas to be moved",
          meta.ntp);
        // wait for partition replicas to be moved
        auto reconciliation_state
          = co_await _api.local().get_reconciliation_state(meta.ntp);
        vlog(
          clusterlog.trace,
          "reconciliation state for {} - {}, status: {}",
          meta.ntp,
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
    };
}

void members_backend::handle_recommissioned(
  const members_manager::node_update& update) {
    if (_members.local().get_broker(update.id) == std::nullopt) {
        return;
    }
    // remove all pending decommissioned updates for this node
    std::erase_if(_updates, [id = update.id](update_meta& meta) {
        return meta.update.id == id
               && meta.update.type
                    == members_manager::node_update_type::decommissioned;
    });
}

} // namespace cluster
