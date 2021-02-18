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
#include "cluster/partition.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "outcome.h"
#include "raft/group_configuration.h"
#include "raft/types.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <absl/container/flat_hash_set.h>

#include <exception>

/// Class that contains the controller state, for now we will have single
/// controller backend

/// on every core, sharded
namespace cluster {

bool is_configuration_up_to_date(
  model::node_id self,
  const ss::lw_shared_ptr<partition>& partition,
  const std::vector<model::broker_shard>& bs,
  model::revision_id change_revision) {
    auto group_cfg = partition->group_configuration();
    auto configuration_committed = partition->get_latest_configuration_offset()
                                   <= partition->committed_offset();

    // group configuration revision is older than expected, this configuration
    // isn't up to date.
    if (group_cfg.revision_id() < change_revision) {
        return false;
    }

    auto it = std::find_if(
      bs.cbegin(), bs.cend(), [self](const model::broker_shard& b) {
          return b.node_id == self;
      });
    bool includes_self = it != bs.cend();

    /*
     * if configuration includes current node, we expect configuration to be
     * fully up to date, as the node will continue to receive all the raft group
     * requests. This is why we claim configuration as not being up to date when
     * it is joint configuration type.
     *
     * NOTE: why include_self matters
     *
     * If the node is not included in current configuration there is no gurante
     * that it will receive configuration that was moving consensus from JOINT
     * to SIMPLE. Also if the node is removed from replica set we will only
     * remove the partiton after other node claim update as finished.
     *
     */
    if (includes_self && group_cfg.type() == raft::configuration_type::joint) {
        return false;
    }
    /*
     * if replica set is a leader it must have configuration committed i.e. it
     * was successfully replicated to majority of followers.
     */
    if (partition->is_leader() && !configuration_committed) {
        return false;
    }

    /**
     * at this point we just compare the configuration broker ids if they are
     * the same as expected, we claim configuration as being up to date
     */
    absl::flat_hash_set<model::node_id> all_ids;
    for (auto& id : group_cfg.current_config().voters) {
        all_ids.emplace(id.id());
    }
    /**
     * if current configuration doesn't include current node it may not be fully
     * updated as current node may stop receiving updates. Therefore we use
     * learners to calculate is replica set is up to date. (If configuration
     * contains current node it will receive all updates so eventually all the
     * nodes will become a voters)
     */
    if (!includes_self) {
        for (auto& id : group_cfg.current_config().learners) {
            all_ids.emplace(id.id());
        }
    }

    // there is different number of brokers in group configuration
    if (all_ids.size() != bs.size()) {
        return false;
    }

    for (auto& b : bs) {
        all_ids.emplace(b.node_id);
    }
    return all_ids.size() == bs.size();
}

controller_backend::controller_backend(
  ss::sharded<topic_table>& tp_state,
  ss::sharded<shard_table>& st,
  ss::sharded<partition_manager>& pm,
  ss::sharded<members_table>& members,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<topics_frontend>& frontend,
  ss::sharded<ss::abort_source>& as)
  : _topics(tp_state)
  , _shard_table(st)
  , _partition_manager(pm)
  , _members_table(members)
  , _partition_leaders_table(leaders)
  , _topics_frontend(frontend)
  , _self(model::node_id(config::shard_local_cfg().node_id))
  , _data_directory(config::shard_local_cfg().data_directory().as_sstring())
  , _as(as) {}

ss::future<> controller_backend::stop() {
    _housekeeping_timer.cancel();
    return _gate.close();
}

ss::future<> controller_backend::start() {
    return bootstrap_controller_backend().then([this] {
        start_topics_reconciliation_loop();
        _housekeeping_timer.set_callback([this] { housekeeping(); });
        _housekeeping_timer.arm_periodic(std::chrono::seconds(1));
    });
}

ss::future<> controller_backend::bootstrap_controller_backend() {
    if (!_topics.local().has_pending_changes()) {
        vlog(clusterlog.trace, "no pending changes, skipping bootstrap");
        return ss::now();
    }

    return fetch_deltas().then([this] {
        return ss::with_semaphore(
          _topics_sem, 1, [this] { return do_bootstrap(); });
    });
}

ss::future<> controller_backend::do_bootstrap() {
    return ss::parallel_for_each(
      _topic_deltas.begin(),
      _topic_deltas.end(),
      [this](underlying_t::value_type& ntp_deltas) {
          return bootstrap_ntp(ntp_deltas.first, ntp_deltas.second);
      });
}

std::vector<topic_table::delta> calculate_bootstrap_deltas(
  model::node_id self, std::vector<topic_table::delta>&& deltas) {
    std::vector<topic_table::delta> result_delta;
    // no deltas, do nothing
    if (deltas.empty()) {
        return result_delta;
    }

    using op_t = topic_table::delta::op_type;
    auto it = deltas.rbegin();

    while (it != deltas.rend()) {
        // if current update was finished and we have no broker/core local
        // replicas, just stop
        if (
          it->type == op_t::update_finished
          && !has_local_replicas(self, it->p_as.replicas)) {
            break;
        }
        // if next operation doesn't contain local replicas we terminate lookup,
        // i.e. we have to execute current opeartion as it has to create
        // partition with correct offset
        if (auto next = std::next(it); next != deltas.rend()) {
            if (
              next->type == op_t::update_finished
              && !has_local_replicas(self, next->p_as.replicas)) {
                break;
            }
        }
        // if partition have to be created deleted locally that is the last
        // operation
        if (it->type == op_t::add || it->type == op_t::del) {
            break;
        }
        ++it;
    }

    // if there are no deltas to reconcile, return empty vector
    if (it == deltas.rend()) {
        return result_delta;
    }

    auto start = std::next(it).base();
    result_delta.reserve(std::distance(start, deltas.end()));
    std::move(start, deltas.end(), std::back_inserter(result_delta));
    return result_delta;
}

ss::future<>
controller_backend::bootstrap_ntp(const model::ntp& ntp, deltas_t& deltas) {
    vlog(clusterlog.trace, "bootstrapping {}", ntp);
    // find last delta that has to be applied
    auto bootstrap_deltas = calculate_bootstrap_deltas(
      _self, std::move(deltas));

    // apply all deltas follwing the one found previously
    deltas = std::move(bootstrap_deltas);
    return reconcile_ntp(deltas);
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
        (void)ss::with_gate(_gate, [this] {
            return reconcile_topics();
        }).handle_exception([](const std::exception_ptr& e) {
            // we ignore the exception as controller backend will retry in next
            // loop
            vlog(clusterlog.warn, "error during reconciliation - {}", e);
        });
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
    using op_t = topic_table::delta::op_type;
    /**
     * Revision is derived from delta offset, i.e. offset of a command that
     * the delta is derived from. The offset is always monotonically
     * increasing together with cluster state evelotion hence it is perfect
     * as a source of revision_id
     */
    vlog(clusterlog.trace, "executing ntp: {} opeartion: {}", delta.ntp, delta);
    model::revision_id rev(delta.offset());
    // new partitions

    // only create partitions for this backend
    // partitions created on current shard at this node
    switch (delta.type) {
    case op_t::add:
        if (!has_local_replicas(_self, delta.p_as.replicas)) {
            return ss::make_ready_future<std::error_code>(errc::success);
        }
        return create_partition(
          delta.ntp,
          delta.p_as.group,
          rev,
          create_brokers_set(delta.p_as.replicas, _members_table.local()));
    case op_t::del:
        return delete_partition(delta.ntp, rev);
    case op_t::update:
        return process_partition_update(delta.ntp, delta.p_as, rev);
    case op_t::update_finished:
        return finish_partition_update(delta.ntp, delta.p_as, rev);
    }
    __builtin_unreachable();
}

ss::future<std::error_code> controller_backend::process_partition_update(
  model::ntp ntp, const partition_assignment& current, model::revision_id rev) {
    vlog(
      clusterlog.trace,
      "processing partiton {} update command with replicas {}",
      ntp,
      current.replicas);

    auto partition = _partition_manager.local().get(ntp);

    /*
     * current change is obsolete, configuration is already updated with
     * more recent change, do nothing, even if this node is required for the new
     * revision to catch up we will keep the partition alive until one of the
     * other nodes will send update_finished command
     */
    if (partition && partition->group_configuration().revision_id() > rev) {
        vlog(
          clusterlog.trace,
          "found newer revision for {}, finishing update to: {}",
          ntp,
          current.replicas);
        co_return std::error_code(errc::success);
    }
    /**
     * if there is no local replica in replica set but,
     * partition with requested ntp exists on this broker core
     * it has to be removed, we can not remove the partition immediately as it
     * may be required for other nodes to recover. The partiton is removed after
     * update is finished on other nodes
     */

    if (!has_local_replicas(_self, current.replicas)) {
        /**
         * if no replicas are expected on current node/shard and partition
         * doesn't exists, the update is finished
         */
        if (!partition) {
            co_return std::error_code(errc::success);
        }
        /**
         * in this case the partition is moved away from this node/shard, we may
         * have to update configuration as this node may be a leader. If update
         * is successful we finish it, removal will be done after all nodes are
         * caught up - if config is already up to date
         * `update_partition_replica_set` will return success.
         */
        auto ec = co_await update_partition_replica_set(
          ntp, current.replicas, rev);

        co_return ec;
    }

    /**
     * in this situation partition is expected to exists on current
     * broker/shard.
     *
     * If partition already exists, update its configuration and wait for it to
     * be applied
     */
    if (partition) {
        auto ec = co_await update_partition_replica_set(
          ntp, current.replicas, rev);

        if (!ec) {
            /**
             *  After one of the replicas find the configuration to be
             * successfully replicate i.e. all nodes are caught up, we can mark
             * update as finished allowing deletion of replica on the nodes
             * where it is not longer needed.
             *
             * NOTE: deletion is safe as we are already running with new
             * configuration so old replicas are not longer needed.
             */
            if (current.replicas.front().node_id == _self) {
                co_return co_await dispatch_update_finished(
                  std::move(ntp), current);
            }
            co_return ec;
        }

        /**
         * If update wasn't successfull (f.e. current node is not a leader),
         * wait for next iteration.
         */
        co_return std::error_code(errc::waiting_for_recovery);
    }

    /**
     * We expect partition replica to exists on current broker/shard. Create
     * partiton. we relay on raft recovery to populate partion configuration.
     */
    vlog(clusterlog.trace, "creating partition {} replica", ntp);
    auto ec = co_await create_partition(ntp, current.group, rev, {});
    // wait for recovery, we will mark partition as updated in next
    // controller backend reconciliation loop pass
    if (!ec) {
        co_return std::error_code(errc::waiting_for_recovery);
    }
    co_return ec;
}

/**
 * notifies the topics frontend that partition update has been finished, all
 * the interested nodes can now safely remove unnecessary partition replicas.
 */
ss::future<std::error_code> controller_backend::dispatch_update_finished(
  model::ntp ntp, partition_assignment assignment) {
    vlog(clusterlog.trace, "dispatching update finished event for {}", ntp);
    return ss::with_gate(
      _gate,
      [this,
       ntp = std::move(ntp),
       assignment = std::move(assignment)]() mutable {
          return _topics_frontend.local().finish_moving_partition_replicas(
            std::move(ntp),
            std::move(assignment.replicas),
            model::timeout_clock::now()
              + config::shard_local_cfg().replicate_append_timeout_ms());
      });
}

ss::future<std::error_code> controller_backend::finish_partition_update(
  model::ntp ntp, const partition_assignment& current, model::revision_id rev) {
    vlog(
      clusterlog.trace, "processing partition {} finished update command", ntp);
    // if there is no local replica in replica set but,
    // partition with requested ntp exists on this broker core
    // it has to be removed after new configuration is stable

    if (has_local_replicas(_self, current.replicas)) {
        return ss::make_ready_future<std::error_code>(errc::success);
    }

    auto partition = _partition_manager.local().get(ntp);
    // we do not have local replicas and partition does not
    // exists, it is ok
    if (!partition) {
        return ss::make_ready_future<std::error_code>(errc::success);
    }
    vlog(clusterlog.trace, "removing partition {} replica", ntp);
    return delete_partition(std::move(ntp), rev);
}

ss::future<std::error_code> controller_backend::update_partition_replica_set(
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& replicas,
  model::revision_id rev) {
    /**
     * Following scenarios can happen in here:
     * - node is a leader for current partition => just update config
     * - node is not a leader for current partition => check if config is
     *   equal to requested, if not return failure
     */
    auto partition = _partition_manager.local().get(ntp);
    // wait for configuration update, only declare success
    // when configuration was actually updated
    if (is_configuration_up_to_date(_self, partition, replicas, rev)) {
        return ss::make_ready_future<std::error_code>(errc::success);
    }
    // we are the leader, update configuration
    if (partition->is_leader()) {
        auto brokers = create_brokers_set(replicas, _members_table.local());
        vlog(
          clusterlog.debug,
          "updating partition {} replica set with {}",
          ntp,
          replicas);

        auto f = partition->update_replica_set(std::move(brokers), rev);
        return ss::with_timeout(
                 model::timeout_clock::now() + std::chrono::seconds(5),
                 std::move(f))
          .then_wrapped([](ss::future<std::error_code> f) {
              try {
                  return f.get0();
              } catch (const ss::timed_out_error& e) {
                  return make_error_code(errc::timeout);
              }
          })
          .then([this, partition, replicas, rev](std::error_code) {
              if (is_configuration_up_to_date(
                    _self, partition, replicas, rev)) {
                  return make_error_code(errc::success);
              }
              return make_error_code(errc::waiting_for_recovery);
          });
    }

    return ss::make_ready_future<std::error_code>(errc::not_leader);
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
    auto partition = _partition_manager.local().get(ntp);
    // no partition exists, create one
    if (likely(!partition)) {
        // we use offset as an rev as it is always increasing and it
        // increases while ntp is being created again
        f = _partition_manager.local()
              .manage(
                cfg->make_ntp_config(_data_directory, ntp.tp.partition, rev),
                group_id,
                std::move(members))
              .discard_result();
    } else {
        // old partition still exists, wait for it to be removed
        if (partition->get_revision_id() < rev) {
            return ss::make_ready_future<std::error_code>(
              errc::partition_already_exists);
        }
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
