#include "cluster/decommissioning_monitor.h"

#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_allocator.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/later.hh>

#include <absl/container/node_hash_set.h>

namespace cluster {

decommissioning_monitor::decommissioning_monitor(
  ss::sharded<cluster::topics_frontend>& fr,
  ss::sharded<topic_table>& tt,
  ss::sharded<partition_allocator>& pal,
  consensus_ptr raft0,
  ss::sharded<ss::abort_source>& as)
  : _topics_frontend(fr)
  , _topics(tt)
  , _allocator(pal)
  , _raft0(raft0)
  , _as(as)
  , _retry_timeout(config::shard_local_cfg().decomission_monitor_retry_ms()) {
    _retry_timer.set_callback([this] { dispatch_decommissioning(); });
}

ss::future<> decommissioning_monitor::stop() {
    _retry_timer.cancel();
    return _bg.close();
}

void decommissioning_monitor::start() { _retry_timer.arm(_retry_timeout); }

void decommissioning_monitor::dispatch_decommissioning() {
    (void)ss::with_gate(_bg, [this] {
        return try_decommission().finally([this] {
            if (!_bg.is_closed()) {
                _retry_timer.arm(_retry_timeout);
            }
        });
    }).handle_exception([](const std::exception_ptr& e) {
        // just log an exception, will retry
        vlog(
          clusterlog.trace,
          "error encountered while handling nodes decomissioning - {}",
          e);
    });
}

ss::future<> decommissioning_monitor::try_decommission() {
    // nothing to do, just return
    if (_decommissioned.empty() && _reallocations.empty()) {
        co_return;
    }

    if (!_raft0->is_leader()) {
        co_return;
    }

    co_await ss::parallel_for_each(
      _reallocations,
      [this](reallocation_meta& meta) { return reallocate_replica_set(meta); });

    // remove finished meta
    std::erase_if(_reallocations, [](const reallocation_meta& meta) {
        return meta.is_finished;
    });

    auto to_remove = co_await _allocator.invoke_on(
      partition_allocator::shard, [](partition_allocator& allocator) {
          std::vector<model::node_id> ids;
          for (const auto& [id, node] : allocator.allocation_nodes()) {
              if (node->is_decommissioned() && node->empty()) {
                  ids.push_back(id);
              }
          }
          return ids;
      });

    // no nodes to reamove from cluster
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

    // check if node was actually removed from raft0 configuration
    // removing node from the decommissioned list finishes the decommissioning
    // process
    std::erase_if(_decommissioned, [&cfg](model::node_id id) {
        return !cfg.contains_broker(id);
    });

    co_return;
}

bool is_replica_set_up_to_date(
  const cluster::topic_configuration_assignment& cfg,
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& requested_replica_set) {
    auto it = std::find_if(
      cfg.assignments.begin(),
      cfg.assignments.end(),
      [pid = ntp.tp.partition](const partition_assignment& pas) {
          return pid == pas.id;
      });
    vassert(
      it != cfg.assignments.end(),
      "Reallocated partition {} have to exists ",
      ntp);

    return it->replicas == requested_replica_set;
}

ss::future<>
decommissioning_monitor::reallocate_replica_set(reallocation_meta& meta) {
    // ask partition allocator for reallocation of
    // replicas
    vlog(clusterlog.debug, "reallocating partition {}", meta.ntp);

    if (!meta.new_assignment) {
        meta.new_assignment = co_await _allocator.invoke_on(
          partition_allocator::shard,
          [current = meta.current_assignment](
            partition_allocator& allocator) mutable {
              return allocator.reallocate_decommissioned_replicas(current);
          });
    }
    // was unable to assign partitition replicas, do nothing
    if (!meta.new_assignment) {
        co_return;
    }

    vlog(
      clusterlog.debug,
      "moving replicas of {} to {}",
      meta.ntp,
      meta.new_assignment->get_assignments().begin()->replicas);
    std::error_code ec
      = co_await _topics_frontend.local().move_partition_replicas(
        meta.ntp,
        meta.new_assignment->get_assignments().begin()->replicas,
        model::timeout_clock::now() + _retry_timeout);

    vlog(clusterlog.debug, "moving replicas result: {}", ec.message());
    auto it = _topics.local().topics_map().find(
      model::topic_namespace_view(meta.ntp));
    // topic was deleted, we are done with assignment
    if (it == _topics.local().topics_map().end()) {
        meta.is_finished = true;
        co_return;
    }
    meta.is_finished = is_replica_set_up_to_date(
      it->second,
      meta.ntp,
      meta.new_assignment->get_assignments().begin()->replicas);
}

bool is_in_replica_set(
  const std::vector<model::broker_shard>& replicas,
  const absl::node_hash_set<model::node_id>& ids) {
    auto it = std::find_if(
      std::cbegin(replicas),
      std::cend(replicas),
      [&ids](const model::broker_shard& bs) {
          return ids.contains(bs.node_id);
      });

    return it != std::cend(replicas);
}

bool decommissioning_monitor::is_decommissioned(model::node_id id) const {
    return std::find(_decommissioned.cbegin(), _decommissioned.cend(), id)
           != _decommissioned.cend();
}

void decommissioning_monitor::decommission(std::vector<model::node_id> ids) {
    absl::node_hash_set<model::node_id> ids_set;
    for (auto id : ids) {
        if (!is_decommissioned(id)) {
            ids_set.emplace(id);
            _decommissioned.push_back(id);
        }
    }

    for (const auto& [tp_ns, cfg] : _topics.local().topics_map()) {
        for (auto& pas : cfg.assignments) {
            if (is_in_replica_set(pas.replicas, ids_set)) {
                _reallocations.emplace_back(
                  model::ntp(tp_ns.ns, tp_ns.tp, pas.id), pas);
            }
        }
    }
}

} // namespace cluster
