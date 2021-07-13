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
    return _members_manager.local()
      .get_node_update()
      .then([this](members_manager::node_update update) {
          return _lock.with([this, update]() mutable {
              switch (update.type) {
              case members_manager::node_update_type::decommissioned:
                  handle_decommissioned(update);
                  break;
              case members_manager::node_update_type::recommissioned:
                  handle_recommissioned(update);
                  break;
              default:
                  return;
              }
              _new_updates.signal();
          });
      })
      .handle_exception([](const std::exception_ptr& e) {
          vlog(clusterlog.trace, "error waiting for members updates - {}", e);
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

ss::future<> members_backend::reconcile() {
    // if nothing to do, wait
    co_await _new_updates.wait([this] {
        return !_reallocations.empty()
               || !_members.local().get_decommissioned().empty();
    });

    auto u = co_await _lock.get_units();

    for (auto& meta : _reallocations) {
        mark_done_as_finished(meta);
    }

    if (!_raft0->is_leader()) {
        co_return;
    }

    // execute reallocations
    co_await ss::parallel_for_each(
      _reallocations,
      [this](reallocation_meta& meta) { return reallocate_replica_set(meta); });

    // remove finished meta
    std::erase_if(_reallocations, [](const reallocation_meta& meta) {
        return meta.state == reallocation_state::finished;
    });

    // remove those decommissioned nodes which doesn't have any pending
    // reallocations
    std::vector<model::node_id> to_remove;
    for (auto& id : _members.local().get_decommissioned()) {
        auto has_active_reallocations = std::any_of(
          _reallocations.cbegin(),
          _reallocations.cend(),
          [id](const reallocation_meta& meta) { return id == meta.update.id; });

        if (!has_active_reallocations && _allocator.local().is_empty(id)) {
            to_remove.push_back(id);
        }
    }

    // no nodes to remove from cluster
    if (to_remove.empty()) {
        co_return;
    }
    vlog(clusterlog.info, "removing decommissioned nodes: {}", to_remove);
    // we are not interested in the result as we check
    // configuration in next step
    co_await _raft0->remove_members(to_remove, model::revision_id{0})
      .discard_result();

    auto cfg = _raft0->config();
    // we have to wait until raft0 configuration will be fully up to date.
    if (cfg.type() != raft::configuration_type::simple) {
        co_return;
    }

    co_return;
}
void members_backend::mark_done_as_finished(
  members_backend::reallocation_meta& meta) {
    // broker was removed, finish
    if (!_members.local().contains(meta.update.id)) {
        meta.state = reallocation_state::finished;
    }
    // topic was removed
    if (!_topics.local().contains(
          model::topic_namespace_view(meta.ntp), meta.ntp.tp.partition)) {
        meta.state = reallocation_state::finished;
    }
}

ss::future<> members_backend::reallocate_replica_set(reallocation_meta& meta) {
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

bool is_in_replica_set(
  const std::vector<model::broker_shard>& replicas, model::node_id id) {
    auto it = std::find_if(
      replicas.cbegin(), replicas.cend(), [id](const model::broker_shard& bs) {
          return id == bs.node_id;
      });

    return it != replicas.cend();
}

void members_backend::handle_decommissioned(
  const members_manager::node_update& update) {
    if (_members.local().get_broker(update.id) == std::nullopt) {
        return;
    }
    for (const auto& [tp_ns, cfg] : _topics.local().topics_map()) {
        for (auto& pas : cfg.assignments) {
            if (is_in_replica_set(pas.replicas, update.id)) {
                auto ntp = model::ntp(tp_ns.ns, tp_ns.tp, pas.id);
                _reallocations.emplace_back(update, ntp);
            }
        }
    }
}

void members_backend::handle_recommissioned(
  const members_manager::node_update& update) {
    if (_members.local().get_broker(update.id) == std::nullopt) {
        return;
    }
    // remove all pending realocations for this node related with
    std::erase_if(_reallocations, [id = update.id](reallocation_meta& meta) {
        return meta.update.id == id
               && meta.update.type
                    == members_manager::node_update_type::decommissioned;
    });
}

} // namespace cluster
