// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller_backend.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "outcome.h"
#include "raft/configuration.h"
#include "raft/types.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <absl/container/flat_hash_set.h>

/// Class that contains the controller state, for now we will have single
/// controller backend

/// on every core, sharded
namespace cluster {

controller_backend::controller_backend(
  ss::sharded<topic_table>& tp_state,
  ss::sharded<shard_table>& st,
  ss::sharded<partition_manager>& pm,
  ss::sharded<members_table>& members,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<ss::abort_source>& as)
  : _topics(tp_state)
  , _shard_table(st)
  , _partition_manager(pm)
  , _members_table(members)
  , _partition_leaders_table(leaders)
  , _self(model::node_id(config::shard_local_cfg().node_id))
  , _data_directory(config::shard_local_cfg().data_directory().as_sstring())
  , _as(as) {}

ss::future<> controller_backend::stop() {
    _housekeeping_timer.cancel();
    return _gate.close();
}

ss::future<> controller_backend::start() {
    start_topics_reconciliation_loop();
    _housekeeping_timer.set_callback([this] { housekeeping(); });
    _housekeeping_timer.arm_periodic(std::chrono::seconds(1));
    return ss::now();
}

ss::future<> controller_backend::fetch_deltas() {
    return _topics.local()
      .wait_for_changes(_as.local())
      .then([this](deltas_t deltas) {
          return ss::with_semaphore(
            _topics_sem, 1, [this, deltas = std::move(deltas)]() mutable {
                for (auto& d : deltas) {
                    auto ntp = d.ntp;
                    _topic_deltas[ntp].push_back(std::move(d));
                }
            });
      });
}

void controller_backend::start_topics_reconciliation_loop() {
    (void)ss::with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _as.local().abort_requested(); },
          [this] {
              return fetch_deltas()
                .then([this] { return reconcile_topics(); })
                .handle_exception([](const std::exception_ptr& e) {
                    vlog(
                      clusterlog.error,
                      "Error while reconciling topics - {}",
                      e);
                });
          });
    });
}

void controller_backend::housekeeping() {
    if (!_topic_deltas.empty() && _topics_sem.available_units() > 0) {
        (void)ss::with_gate(_gate, [this] { return reconcile_topics(); });
    }
}

ss::future<> controller_backend::reconcile_ntp(deltas_t& deltas) {
    return ss::do_with(
      bool{false},
      deltas.cbegin(),
      [this, &deltas](bool& stop, deltas_t::const_iterator& it) {
          return ss::do_until(
                   [&stop, &it, &deltas] {
                       return stop || it == deltas.cend();
                   },
                   [this, &it, &stop] {
                       return execute_partitition_op(*it).then(
                         [&it, &stop](std::error_code ec) {
                             if (ec) {
                                 stop = true;
                                 return;
                             }
                             it++;
                         });
                   })
            .then([&deltas, &it] {
                // remove finished tasks
                deltas.erase(deltas.cbegin(), it);
            });
      });
}

// caller must hold _topics_sem lock
ss::future<> controller_backend::reconcile_topics() {
    return ss::with_semaphore(_topics_sem, 1, [this] {
        if (_topic_deltas.empty()) {
            return ss::now();
        }
        // reconcile NTPs in parallel
        return ss::parallel_for_each(
                 _topic_deltas.begin(),
                 _topic_deltas.end(),
                 [this](underlying_t::value_type& ntp_deltas) {
                     return reconcile_ntp(ntp_deltas.second);
                 })
          .then([this] {
              // cleanup empty NTP keys
              for (auto it = _topic_deltas.cbegin();
                   it != _topic_deltas.cend();) {
                  if (it->second.empty()) {
                      _topic_deltas.erase(it++);
                  } else {
                      ++it;
                  }
              }
          });
    });
}

bool has_local_replicas(
  model::node_id self, const std::vector<model::broker_shard>& replicas) {
    return std::find_if(
             std::cbegin(replicas),
             std::cend(replicas),
             [self](const model::broker_shard& bs) {
                 return bs.node_id == self && bs.shard == ss::this_shard_id();
             })
           != replicas.cend();
}

std::vector<model::broker> create_brokers_set(
  const std::vector<model::broker_shard>& replicas,
  cluster::members_table& members) {
    std::vector<model::broker> brokers;
    brokers.reserve(replicas.size());
    std::transform(
      std::cbegin(replicas),
      std::cend(replicas),
      std::back_inserter(brokers),
      [&members](const model::broker_shard& bs) {
          auto br = members.get_broker(bs.node_id);
          if (!br) {
              throw std::logic_error(
                fmt::format("Replica node {} is not available", bs.node_id));
          }
          return *br->get();
      });
    return brokers;
}

ss::future<std::error_code>
controller_backend::execute_partitition_op(const topic_table::delta& delta) {
    /**
     * Revision is derived from delta offset, i.e. offset of a command that
     * the delta is derived from. The offset is always monotonically
     * increasing together with cluster state evelotion hence it is perfect
     * as a source of revision_id
     */
    vlog(clusterlog.trace, "Executing operation: {}", delta);
    model::revision_id rev(delta.offset());
    // new partitions

    // only create partitions for this backend
    // partitions created on current shard at this node
    switch (delta.type) {
    case topic_table::delta::op_type::add:
        if (!has_local_replicas(_self, delta.p_as.replicas)) {
            return ss::make_ready_future<std::error_code>(errc::success);
        }
        return create_partition(
          delta.ntp,
          delta.p_as.group,
          rev,
          create_brokers_set(delta.p_as.replicas, _members_table.local()));
    case topic_table::delta::op_type::del:
        return delete_partition(delta.ntp, rev);
    case topic_table::delta::op_type::update:
        return process_partition_update(delta.ntp, delta.p_as, rev);
    }
    __builtin_unreachable();
}

ss::future<std::error_code> controller_backend::process_partition_update(
  model::ntp ntp, const partition_assignment& p_as, model::revision_id rev) {
    // if there is no local replica in replica set but,
    // partition with requested ntp exists on this broker core
    // it has to be removed after new configuration is stable
    auto partition = _partition_manager.local().get(ntp);
    if (!has_local_replicas(_self, p_as.replicas)) {
        // we do not have local replicas and partition does not
        // exists, it is ok
        if (!partition) {
            return ss::make_ready_future<std::error_code>(errc::success);
        }
        // this partition has to be removed eventually

        return update_partition_replica_set(ntp, p_as.replicas)
          .then([this, ntp, rev](std::error_code ec) {
              if (!ec) {
                  return delete_partition(ntp, rev);
              }
              return ss::make_ready_future<std::error_code>(ec);
          });
    }

    // partition already exists, update configuration
    if (partition) {
        return update_partition_replica_set(ntp, p_as.replicas);
    }
    // create partition with empty configuration. Configuration
    // will be populated during node recovery
    return create_partition(ntp, p_as.group, rev, {});
}

bool is_configuration_up_to_date(
  const std::vector<model::broker_shard>& bs,
  const raft::group_configuration& cfg) {
    absl::flat_hash_set<model::node_id> all_ids;
    // we are only interested in final configuration
    if (cfg.old_config()) {
        return false;
    }
    cfg.for_each_voter(
      [&all_ids](raft::vnode nid) { all_ids.emplace(nid.id()); });
    cfg.for_each_learner(
      [&all_ids](raft::vnode nid) { all_ids.emplace(nid.id()); });
    // there is different number of brokers in group configuration
    if (all_ids.size() != bs.size()) {
        return false;
    }

    for (auto& b : bs) {
        all_ids.emplace(b.node_id);
    }
    return all_ids.size() == bs.size();
}

ss::future<std::error_code> controller_backend::update_partition_replica_set(
  const model::ntp& ntp, const std::vector<model::broker_shard>& replicas) {
    vlog(
      clusterlog.trace,
      "updating partition {} replicas with {}",
      ntp,
      replicas);
    /**
     * Following scenarios can happen in here:
     * - node is a leader for current partition => just update config
     * - node is not a leader for current partition => check if config is
     * equal to requested, if not return failure
     */
    auto partition = _partition_manager.local().get(ntp);
    // we are the leader, update configuration
    if (partition->is_leader()) {
        auto brokers = create_brokers_set(replicas, _members_table.local());
        // TODO: replace with correct revision_id, this feature is not used yet.
        //       The `update_partition_replica` set method is never called.
        //       We have to fix this before enabling partition moving
        return partition->update_replica_set(
          std::move(brokers), model::revision_id(0));
    }
    // not the leader, wait for configuration update, only declare success
    // when configuration was actually updated
    if (!is_configuration_up_to_date(
          replicas, partition->group_configuration())) {
        return ss::make_ready_future<std::error_code>(errc::not_leader);
    }
    return ss::make_ready_future<std::error_code>(errc::success);
}

ss::future<> controller_backend::add_to_shard_table(
  model::ntp ntp, raft::group_id raft_group, uint32_t shard) {
    // update shard_table: broadcast
    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), raft_group, shard](shard_table& s) mutable {
          s.insert(std::move(ntp), shard);
          s.insert(raft_group, shard);
      });
}

ss::future<std::error_code> controller_backend::create_partition(
  model::ntp ntp,
  raft::group_id group_id,
  model::revision_id rev,
  std::vector<model::broker> members) {
    auto cfg = _topics.local().get_topic_cfg(model::topic_namespace_view(ntp));

    if (!cfg) {
        // partition was already removed, do nothing
        return ss::make_ready_future<std::error_code>(errc::success);
    }

    auto f = ss::now();
    // handle partially created topic
    if (likely(_partition_manager.local().get(ntp).get() == nullptr)) {
        // we use offset as an ntp_id as it is always increasing and it
        // increases while ntp is being created again
        f = _partition_manager.local()
              .manage(
                cfg->make_ntp_config(_data_directory, ntp.tp.partition, rev),
                group_id,
                std::move(members))
              .discard_result();
    }

    return f
      .then([this, ntp = std::move(ntp), group_id]() mutable {
          // we create only partitions that belongs to current shard
          return add_to_shard_table(
            std::move(ntp), group_id, ss::this_shard_id());
      })
      .then([] { return make_error_code(errc::success); });
}

ss::future<std::error_code>
controller_backend::delete_partition(model::ntp ntp, model::revision_id rev) {
    auto part = _partition_manager.local().get(ntp);
    if (unlikely(part.get() == nullptr)) {
        return ss::make_ready_future<std::error_code>(errc::success);
    }
    // partition was already recreated with greater rev, do nothing
    if (unlikely(part->get_revision_id() > rev)) {
        return ss::make_ready_future<std::error_code>(errc::success);
    }
    auto group_id = part->group();

    return _shard_table
      .invoke_on_all(
        [ntp, group_id](shard_table& st) mutable { st.erase(ntp, group_id); })
      .then([this, ntp] {
          return _partition_leaders_table.invoke_on_all(
            [ntp](partition_leaders_table& leaders) {
                leaders.remove_leader(ntp);
            });
      })
      .then([this, ntp = std::move(ntp)] {
          // remove partition
          return _partition_manager.local().remove(ntp);
      })
      .then([] { return make_error_code(errc::success); });
}

} // namespace cluster
