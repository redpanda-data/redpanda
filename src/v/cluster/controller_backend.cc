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
#include "cluster/errc.h"
#include "cluster/fwd.h"
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
#include "vassert.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/later.hh>

#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>

#include <algorithm>
#include <exception>

/// Class that contains the controller state, for now we will have single
/// controller backend

/// on every core, sharded
namespace cluster {

inline bool contains_node(
  model::node_id id, const std::vector<model::broker_shard>& replicas) {
    return std::any_of(
      replicas.cbegin(), replicas.cend(), [id](const model::broker_shard& bs) {
          return bs.node_id == id;
      });
}

std::error_code check_configuration_update(
  model::node_id self,
  const ss::lw_shared_ptr<partition>& partition,
  const std::vector<model::broker_shard>& bs,
  model::revision_id change_revision) {
    auto group_cfg = partition->group_configuration();
    vlog(
      clusterlog.trace,
      "checking if partition {} configuration {} is up to date with {}",
      partition->ntp(),
      group_cfg,
      bs);
    auto configuration_committed = partition->get_latest_configuration_offset()
                                   <= partition->committed_offset();

    // group configuration revision is older than expected, this configuration
    // isn't up to date.
    if (group_cfg.revision_id() < change_revision) {
        vlog(
          clusterlog.trace,
          "partition {} configuration revision '{}' is smaller than requested "
          "update revision '{}'",
          partition->ntp(),
          group_cfg.revision_id(),
          change_revision);
        return errc::partition_configuration_revision_not_updated;
    }
    bool includes_self = contains_node(self, bs);

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
        vlog(
          clusterlog.trace,
          "partition {} contains current node and its consensus configuration "
          "is still in joint state",
          partition->ntp());
        return errc::partition_configuration_in_joint_mode;
    }
    /*
     * if replica set is a leader it must have configuration committed i.e. it
     * was successfully replicated to majority of followers.
     */
    if (partition->is_leader() && !configuration_committed) {
        vlog(
          clusterlog.trace,
          "current node is partition {} leader, waiting for configuration to "
          "be committed",
          partition->ntp());
        return errc::partition_configuration_leader_config_not_committed;
    }

    /**
     * at this point we just compare the configuration broker ids if they are
     * the same as expected, we claim configuration as being up to date
     */
    absl::flat_hash_set<model::node_id> all_ids;
    for (auto& id : group_cfg.current_config().voters) {
        all_ids.emplace(id.id());
    }

    // there is different number of brokers in group configuration
    if (all_ids.size() != bs.size()) {
        vlog(
          clusterlog.trace,
          "requested replica set {} differs from partition replica set: {}",
          bs,
          group_cfg.brokers());
        return errc::partition_configuration_differs;
    }

    for (auto& b : bs) {
        all_ids.emplace(b.node_id);
    }

    if (all_ids.size() != bs.size()) {
        vlog(
          clusterlog.trace,
          "requested replica set {} differs from partition replica set: {}",
          bs,
          group_cfg.brokers());
        return errc::partition_configuration_differs;
    }

    return errc::success;
}

controller_backend::controller_backend(
  ss::sharded<topic_table>& tp_state,
  ss::sharded<shard_table>& st,
  ss::sharded<partition_manager>& pm,
  ss::sharded<members_table>& members,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<topics_frontend>& frontend,
  ss::sharded<storage::api>& storage,
  ss::sharded<ss::abort_source>& as)
  : _topics(tp_state)
  , _shard_table(st)
  , _partition_manager(pm)
  , _members_table(members)
  , _partition_leaders_table(leaders)
  , _topics_frontend(frontend)
  , _storage(storage)
  , _self(model::node_id(config::shard_local_cfg().node_id))
  , _data_directory(config::shard_local_cfg().data_directory().as_sstring())
  , _housekeeping_timer_interval(
      config::shard_local_cfg().controller_backend_housekeeping_interval_ms())
  , _as(as) {}

ss::future<> controller_backend::stop() {
    _housekeeping_timer.cancel();
    return _gate.close();
}

ss::future<> controller_backend::start() {
    return bootstrap_controller_backend().then([this] {
        start_topics_reconciliation_loop();
        _housekeeping_timer.set_callback([this] { housekeeping(); });
        _housekeeping_timer.arm(_housekeeping_timer_interval);
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
  model::node_id self, const std::vector<topic_table::delta>& deltas) {
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
          && !has_local_replicas(self, it->new_assignment.replicas)) {
            break;
        }
        // if next operation doesn't contain local replicas we terminate lookup,
        // i.e. we have to execute current opeartion as it has to create
        // partition with correct offset
        if (auto next = std::next(it); next != deltas.rend()) {
            if (
              next->type == op_t::update_finished
              && !has_local_replicas(self, next->new_assignment.replicas)) {
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
bool is_cross_core_update(model::node_id self, const topic_table_delta& delta) {
    if (!delta.previous_assignment) {
        return false;
    }
    return contains_node(self, delta.previous_assignment->replicas)
           && !has_local_replicas(self, delta.previous_assignment->replicas);
}
ss::future<>
controller_backend::bootstrap_ntp(const model::ntp& ntp, deltas_t& deltas) {
    vlog(clusterlog.trace, "bootstrapping {}", ntp);
    // find last delta that has to be applied
    auto bootstrap_deltas = calculate_bootstrap_deltas(_self, deltas);

    // if empty do nothing
    if (bootstrap_deltas.empty()) {
        return ss::now();
    }

    auto& first_delta = bootstrap_deltas.front();
    // if first operation is a cross core update, find initial revision on
    // current node and store it in bootstrap map
    using op_t = topic_table::delta::op_type;
    if (is_cross_core_update(_self, first_delta)) {
        // find opeartion that created current partition on this node
        auto it = std::find_if(
          deltas.rbegin(),
          deltas.rend(),
          [this](const topic_table_delta& delta) {
              if (delta.type == op_t::add) {
                  return true;
              }
              return delta.type == op_t::update_finished
                     && !contains_node(_self, delta.new_assignment.replicas);
          });

        vassert(
          it != deltas.rend(),
          "if partition {} was moved from different core it had to exists on "
          "current node previously",
          ntp);
        // if we found update finished operation it is preceeding operation that
        // created partition on current node
        if (it->type == op_t::update_finished) {
            vassert(
              it != deltas.rbegin(),
              "update finished delta {} must have preceeding operation",
              *it);
            it = std::prev(it);
        }
        // persist revision in order to create partition with correct revision
        _bootstrap_revisions[ntp] = model::revision_id(it->offset());
    }
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
    (void)ss::with_gate(_gate, [this] {
        auto f = ss::now();
        if (!_topic_deltas.empty() && _topics_sem.available_units() > 0) {
            f = reconcile_topics();
        }
        return f.finally([this] {
            if (!_gate.is_closed()) {
                _housekeeping_timer.arm(_housekeeping_timer_interval);
            }
        });
    }).handle_exception([](const std::exception_ptr& e) {
        // we ignore the exception as controller backend will retry in next
        // loop
        vlog(clusterlog.warn, "error during reconciliation - {}", e);
    });
}

ss::future<> controller_backend::reconcile_ntp(deltas_t& deltas) {
    bool stop = false;
    auto it = deltas.begin();
    while (!(stop || it == deltas.end())) {
        try {
            auto ec = co_await execute_partitition_op(*it);
            if (ec) {
                if (it->type == topic_table_delta::op_type::update) {
                    /**
                     * do not skip cross core partition updates waiting for
                     * partition to be shut down on the other core
                     */
                    if (ec == errc::wating_for_partition_shutdown) {
                        continue;
                    }
                    /**
                     * check if pending update isn't already finished, if so it
                     * is safe to proceed to the next step
                     */
                    auto fit = std::find_if(
                      it, deltas.end(), [&it](const topic_table::delta& d) {
                          return d.type
                                   == topic_table::delta::op_type::
                                     update_finished
                                 && d.new_assignment.replicas
                                      == it->new_assignment.replicas;
                      });

                    // The only delta types permitted between `update` and
                    // `update_finished` are `update_properties` or `del`. Apply
                    // any intervening deltas of these types and skip the cursor
                    // ahead to the `update_finished`
                    if (fit != deltas.end()) {
                        while (++it != fit) {
                            vlog(
                              clusterlog.trace,
                              "executing (during update) ntp: {} operation: {}",
                              it->ntp,
                              *it);
                            if (
                              it->type
                              == topic_table_delta::op_type::
                                update_properties) {
                                co_await process_partition_properties_update(
                                  it->ntp, it->new_assignment);
                            } else if (
                              it->type == topic_table_delta::op_type::del) {
                                co_await delete_partition(
                                  it->ntp, model::revision_id{it->offset});
                            } else {
                                vassert(
                                  false, "Invalid delta during topic update");
                            }
                        }
                        continue;
                    }
                }
                vlog(
                  clusterlog.info,
                  "partition operation {} result: {}",
                  *it,
                  ec.message());
                stop = true;
                continue;
            }
            vlog(clusterlog.info, "partition operation {} finished", *it);
        } catch (...) {
            vlog(
              clusterlog.error,
              "exception while executing partiton operation: {} - {}",
              *it,
              std::current_exception());
            stop = true;
            continue;
        }
        ++it;
    }
    deltas.erase(deltas.begin(), it);
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

std::optional<ss::shard_id> get_target_shard(
  model::node_id id, const std::vector<model::broker_shard>& replicas) {
    auto it = std::find_if(
      replicas.cbegin(), replicas.cend(), [id](const model::broker_shard& bs) {
          return bs.node_id == id;
      });

    return it != replicas.cend() ? std::optional<ss::shard_id>(it->shard)
                                 : std::nullopt;
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
        if (!has_local_replicas(_self, delta.new_assignment.replicas)) {
            return ss::make_ready_future<std::error_code>(errc::success);
        }
        return create_partition(
          delta.ntp,
          delta.new_assignment.group,
          rev,
          create_brokers_set(
            delta.new_assignment.replicas, _members_table.local()));
    case op_t::add_non_replicable:
        if (!has_local_replicas(_self, delta.new_assignment.replicas)) {
            return ss::make_ready_future<std::error_code>(errc::success);
        }
        return create_non_replicable_partition(delta.ntp, rev);
    case op_t::del:
        return delete_partition(delta.ntp, rev).then([] {
            return std::error_code(errc::success);
        });
    case op_t::del_non_replicable:
        return delete_non_replicable_partition(delta.ntp, rev).then([] {
            return std::error_code(errc::success);
        });
    case op_t::update:
        vassert(
          delta.previous_assignment,
          "update delta must have previous assignment, current delta: {}",
          delta);
        return process_partition_update(
          delta.ntp, delta.new_assignment, *delta.previous_assignment, rev);
    case op_t::update_finished:
        return finish_partition_update(delta.ntp, delta.new_assignment, rev)
          .then([] { return std::error_code(errc::success); });
    case op_t::update_properties:
        return process_partition_properties_update(
                 delta.ntp, delta.new_assignment)
          .then([] { return std::error_code(errc::success); });
    }
    __builtin_unreachable();
}

ss::future<std::optional<controller_backend::cross_shard_move_request>>
controller_backend::ask_remote_shard_for_initail_rev(
  model::ntp ntp, ss::shard_id shard) {
    using ret_t = std::optional<controller_backend::cross_shard_move_request>;
    return container().invoke_on(
      shard, [ntp = std::move(ntp)](controller_backend& remote) {
          if (auto it = remote._cross_shard_requests.find(ntp);
              it != remote._cross_shard_requests.end()) {
              ret_t ret{std::move(it->second)};
              remote._cross_shard_requests.erase(it);
              return ret;
          }
          return ret_t{};
      });
}

bool are_assignments_equal(
  const partition_assignment& requested, const partition_assignment& previous) {
    if (requested.id != previous.id || requested.group != previous.group) {
        return false;
    }
    return are_replica_sets_equal(requested.replicas, previous.replicas);
}

model::node_id first_with_assignment_change(
  const partition_assignment& requested, const partition_assignment& previous) {
    absl::node_hash_map<model::node_id, model::broker_shard> prev_map;
    prev_map.reserve(previous.replicas.size());
    for (auto& replica : previous.replicas) {
        prev_map.emplace(replica.node_id, replica);
    }

    auto it = std::find_if(
      requested.replicas.begin(),
      requested.replicas.end(),
      [&](const model::broker_shard& bs) {
          auto it = prev_map.find(bs.node_id);
          return it == prev_map.end() || it->second != bs;
      });
    // there are no changed assignements
    if (it == requested.replicas.end()) {
        return requested.replicas.begin()->node_id;
    }
    return it->node_id;
}

ss::future<std::error_code> controller_backend::process_partition_update(
  model::ntp ntp,
  const partition_assignment& requested,
  const partition_assignment& previous,
  model::revision_id rev) {
    vlog(
      clusterlog.trace,
      "processing partiton {} update command with replicas {}",
      ntp,
      requested.replicas);

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
          requested.replicas);
        co_return std::error_code(errc::success);
    }
    /**
     * if there is no local replica in replica set but,
     * partition with requested ntp exists on this broker core
     * it has to be removed, we can not remove the partition immediately as it
     * may be required for other nodes to recover. The partiton is removed after
     * update is finished on other nodes
     */

    if (!has_local_replicas(_self, requested.replicas)) {
        /**
         * if no replicas are expected on current node/shard and partition
         * doesn't exists, the update is finished
         */
        if (!partition) {
            co_return std::error_code(errc::success);
        }

        /**
         * Cross core partition migration
         *
         * Current partition have to be migrated to other shard of the current
         * node, in this situation we have to
         * 1) shutdown partition instance
         * 2) create instance on target remote core
         */
        if (contains_node(_self, requested.replicas)) {
            co_return co_await shutdown_on_current_shard(std::move(ntp), rev);
        }

        /**
         * in this case the partition is moved away from this node/shard, we may
         * have to update configuration as this node may be a leader. If update
         * is successful we finish it, removal will be done after all nodes are
         * caught up - if config is already up to date
         * `update_partition_replica_set` will return success.
         */
        auto ec = co_await update_partition_replica_set(
          ntp, requested.replicas, rev);

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
        // if requested assignment is equal to current one, just finish the
        // update
        if (are_assignments_equal(requested, previous)) {
            if (requested.replicas.front().node_id == _self) {
                co_return co_await dispatch_update_finished(
                  std::move(ntp), requested);
            }
        }
        auto ec = co_await update_partition_replica_set(
          ntp, requested.replicas, rev);

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
            if (first_with_assignment_change(requested, previous) == _self) {
                co_return co_await dispatch_update_finished(
                  std::move(ntp), requested);
            }
            co_return ec;
        }

        /**
         * If update wasn't successfull (f.e. current node is not a leader),
         * wait for next iteration.
         */
        co_return ec;
    }

    /**
     * Check if this NTP was moved from other shard on current broker, if so we
     * have to wait for it to be shutdown. Partition creation will be dispatched
     * to target partition manager from the core where the partition is
     * currently present. On this core we will just wait for partition to be
     * created.
     */
    if (
      contains_node(_self, previous.replicas)
      && !has_local_replicas(_self, previous.replicas)) {
        auto previous_shard = get_target_shard(_self, previous.replicas);
        std::optional<model::revision_id> initial_revision;
        std::vector<model::broker> initial_brokers;
        if (auto it = _bootstrap_revisions.find(ntp);
            it != _bootstrap_revisions.end()) {
            initial_revision = it->second;
        } else {
            vlog(
              clusterlog.trace,
              "waiting for cross core move information from shard {}, for {}",
              *previous_shard,
              ntp);
            // ask previous controller for partition initial revision
            auto x_core_move_req = co_await ask_remote_shard_for_initail_rev(
              ntp, *previous_shard);
            if (!x_core_move_req) {
                co_return errc::wating_for_partition_shutdown;
            }
            initial_revision = x_core_move_req->revision;
            std::copy(
              x_core_move_req->initial_configuration.brokers().begin(),
              x_core_move_req->initial_configuration.brokers().end(),
              std::back_inserter(initial_brokers));
        }
        if (initial_revision) {
            vlog(
              clusterlog.trace,
              "creating partition {} from shard {}",
              ntp,
              previous_shard);
            co_await raft::details::move_persistent_state(
              requested.group, *previous_shard, ss::this_shard_id(), _storage);
            auto ec = co_await create_partition(
              ntp,
              requested.group,
              *initial_revision,
              std::move(initial_brokers));

            if (ec) {
                co_return ec;
            }
            // finally remove bootstarp revision
            _bootstrap_revisions.erase(ntp);
        }

        co_return errc::waiting_for_recovery;
    }
    /**
     * We expect partition replica to exists on current broker/shard. Create
     * partiton. we relay on raft recovery to populate partion
     * configuration.
     */
    auto ec = co_await create_partition(ntp, requested.group, rev, {});
    // wait for recovery, we will mark partition as updated in next
    // controller backend reconciliation loop pass
    if (!ec) {
        co_return errc::waiting_for_recovery;
    }
    co_return ec;
}

ss::future<> controller_backend::process_partition_properties_update(
  model::ntp ntp, partition_assignment assignment) {
    /**
     * No core local replicas are expected to exists, do nothing
     */
    if (!has_local_replicas(_self, assignment.replicas)) {
        co_return;
    }

    auto partition = _partition_manager.local().get(ntp);

    // partition doesn't exists, it must already have been removed, do nothing
    if (!partition) {
        co_return;
    }
    auto cfg = _topics.local().get_topic_cfg(model::topic_namespace_view(ntp));
    // configuration doesn't exists, topic was removed
    if (!cfg) {
        co_return;
    }
    vlog(
      clusterlog.trace,
      "updating {} configuration with properties: {}",
      ntp,
      cfg->properties);
    co_await partition->update_configuration(cfg->properties);
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

ss::future<> controller_backend::finish_partition_update(
  model::ntp ntp, const partition_assignment& current, model::revision_id rev) {
    vlog(
      clusterlog.trace, "processing partition {} finished update command", ntp);
    // if there is no local replica in replica set but,
    // partition with requested ntp exists on this broker core
    // it has to be removed after new configuration is stable

    if (has_local_replicas(_self, current.replicas)) {
        return ss::make_ready_future<>();
    }

    auto partition = _partition_manager.local().get(ntp);
    // we do not have local replicas and partition does not
    // exists, it is ok
    if (!partition) {
        return ss::make_ready_future<>();
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
    auto update_ec = check_configuration_update(
      _self, partition, replicas, rev);
    if (!update_ec) {
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
              return check_configuration_update(
                _self, partition, replicas, rev);
          });
    }

    return ss::make_ready_future<std::error_code>(errc::not_leader);
}

ss::future<> controller_backend::add_to_shard_table(
  model::ntp ntp,
  raft::group_id raft_group,
  uint32_t shard,
  model::revision_id revision) {
    // update shard_table: broadcast
    vlog(
      clusterlog.trace, "adding {} to shard table at {}", revision, ntp, shard);
    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), raft_group, shard, revision](
        shard_table& s) mutable {
          s.update(ntp, raft_group, shard, revision);
      });
}

ss::future<> controller_backend::add_to_shard_table(
  model::ntp ntp, ss::shard_id shard, model::revision_id revision) {
    vlog(
      clusterlog.trace, "adding {} to shard table at {}", revision, ntp, shard);
    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), shard, revision](shard_table& s) mutable {
          vassert(
            s.update_shard(ntp, shard, revision),
            "Newer revision for non-replicable ntp exists");
      });
}

ss::future<> controller_backend::remove_from_shard_table(
  model::ntp ntp, raft::group_id raft_group, model::revision_id revision) {
    // update shard_table: broadcast

    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), raft_group, revision](shard_table& s) mutable {
          s.erase(ntp, raft_group, revision);
      });
}

ss::future<> controller_backend::delete_non_replicable_partition(
  model::ntp ntp, model::revision_id rev) {
    vlog(clusterlog.trace, "removing {} from shard table at {}", ntp, rev);
    co_await _shard_table.invoke_on_all(
      [ntp, rev](shard_table& st) { st.erase(ntp, rev); });
    auto log = _storage.local().log_mgr().get(ntp);
    if (log && log->config().get_revision() < rev) {
        co_await _storage.local().log_mgr().remove(ntp);
    }
}

ss::future<std::error_code> controller_backend::create_non_replicable_partition(
  model::ntp ntp, model::revision_id rev) {
    auto cfg = _topics.local().get_topic_cfg(model::topic_namespace_view(ntp));
    if (!cfg) {
        // partition was already removed, do nothing
        co_return errc::success;
    }
    vassert(
      !_storage.local().log_mgr().get(ntp),
      "Log exists for missing entry in topics_table");
    auto ntp_cfg = cfg->make_ntp_config(_data_directory, ntp.tp.partition, rev);
    co_await _storage.local().log_mgr().manage(std::move(ntp_cfg));
    co_await add_to_shard_table(std::move(ntp), ss::this_shard_id(), rev);
    co_return errc::success;
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
      .then([this, ntp = std::move(ntp), group_id, rev]() mutable {
          // we create only partitions that belongs to current shard
          return add_to_shard_table(
            std::move(ntp), group_id, ss::this_shard_id(), rev);
      })
      .then([] { return make_error_code(errc::success); });
}
controller_backend::cross_shard_move_request::cross_shard_move_request(
  model::revision_id rev, raft::group_configuration cfg)
  : revision(rev)
  , initial_configuration(std::move(cfg)) {}

ss::future<std::error_code> controller_backend::shutdown_on_current_shard(
  model::ntp ntp, model::revision_id rev) {
    vlog(clusterlog.trace, "cross core move, shutting down partition: {}", ntp);
    auto partition = _partition_manager.local().get(ntp);
    // partition doesn't exists it was deleted
    if (!partition) {
        co_return errc::partition_not_exists;
    }
    auto gr = partition->group();
    auto init_rev = partition->get_ntp_config().get_revision();

    try {
        // remove from shard table
        co_await remove_from_shard_table(ntp, gr, rev);
        // shutdown partition
        co_await _partition_manager.local().shutdown(ntp);
        // after partition is stopped emplace cross shard request.
        auto [it, success] = _cross_shard_requests.emplace(
          ntp,
          cross_shard_move_request(init_rev, partition->group_configuration()));
        vlog(clusterlog.trace, "cross core move, partition {} stopped", ntp);
        vassert(
          success,
          "only one cross shard request is allowed to be pending for single "
          "ntp, current request: {}, ntp: {}, revision: {}",
          it->second,
          ntp,
          rev);
        co_return errc::success;
    } catch (...) {
        /**
         * If partition shutdown failed we should crash, this error is
         * unrecoverable
         */
        vassert(
          false,
          "error shutting down {} partition at revision {}, error: {}, "
          "terminating",
          ntp,
          rev,
          std::current_exception());
    }
}

ss::future<>
controller_backend::delete_partition(model::ntp ntp, model::revision_id rev) {
    auto part = _partition_manager.local().get(ntp);
    if (unlikely(part.get() == nullptr)) {
        return ss::make_ready_future<>();
    }

    // partition was already recreated with greater rev, do nothing
    if (unlikely(part->get_revision_id() > rev)) {
        return ss::make_ready_future<>();
    }

    auto group_id = part->group();

    return _shard_table
      .invoke_on_all([ntp, group_id, rev](shard_table& st) mutable {
          st.erase(ntp, group_id, rev);
      })
      .then([this, ntp] {
          return _partition_leaders_table.invoke_on_all(
            [ntp](partition_leaders_table& leaders) {
                leaders.remove_leader(ntp);
            });
      })
      .then([this, ntp = std::move(ntp)] {
          // remove partition
          return _partition_manager.local().remove(ntp);
      });
}

std::vector<topic_table::delta>
controller_backend::list_ntp_deltas(const model::ntp& ntp) const {
    if (auto it = _topic_deltas.find(ntp); it != _topic_deltas.end()) {
        return it->second;
    }

    return {};
}

} // namespace cluster
