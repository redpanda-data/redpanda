// Copyright 2020 Redpanda Data, Inc.
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
#include "config/node_config.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "outcome.h"
#include "prometheus/prometheus_sanitize.h"
#include "raft/group_configuration.h"
#include "raft/types.h"
#include "ssx/future-util.h"
#include "vassert.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
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
namespace {

inline bool contains_node(
  model::node_id id, const std::vector<model::broker_shard>& replicas) {
    return std::any_of(
      replicas.cbegin(), replicas.cend(), [id](const model::broker_shard& bs) {
          return bs.node_id == id;
      });
}

bool is_cross_core_update(model::node_id self, const topic_table_delta& delta) {
    if (!delta.previous_replica_set) {
        return false;
    }
    return contains_node(self, *delta.previous_replica_set)
           && !has_local_replicas(self, *delta.previous_replica_set);
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

std::vector<raft::broker_revision> create_brokers_set(
  const std::vector<model::broker_shard>& replicas,
  const absl::flat_hash_map<model::node_id, model::revision_id>&
    replica_revisions,
  cluster::members_table& members) {
    std::vector<raft::broker_revision> brokers;
    brokers.reserve(replicas.size());

    std::transform(
      std::cbegin(replicas),
      std::cend(replicas),
      std::back_inserter(brokers),
      [&members, &replica_revisions](const model::broker_shard& bs) {
          auto br = members.get_broker(bs.node_id);
          if (!br) {
              throw std::logic_error(
                fmt::format("Replica node {} is not available", bs.node_id));
          }
          auto rev_it = replica_revisions.find(bs.node_id);
          vassert(
            rev_it != replica_revisions.end(),
            "revision for broker {} must be present in replica revisions map. "
            "revisions map size: {}",
            bs.node_id,
            replica_revisions.size());
          return raft::broker_revision{
            .broker = *br->get(), .rev = rev_it->second};
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

bool are_assignments_equal(
  const partition_assignment& requested,
  const std::vector<model::broker_shard>& previous) {
    return are_replica_sets_equal(requested.replicas, previous);
}

bool are_configuration_replicas_up_to_date(
  const raft::group_configuration& cfg,
  const std::vector<model::broker_shard>& requested_replicas) {
    absl::flat_hash_set<model::node_id> all_ids;
    all_ids.reserve(requested_replicas.size());

    for (auto& id : cfg.current_config().voters) {
        all_ids.emplace(id.id());
    }

    // there is different number of brokers in group configuration
    if (all_ids.size() != requested_replicas.size()) {
        return false;
    }

    for (auto& b : requested_replicas) {
        all_ids.emplace(b.node_id);
    }

    return all_ids.size() == requested_replicas.size();
}
} // namespace

std::error_code check_configuration_update(
  model::node_id self,
  const ss::lw_shared_ptr<partition>& partition,
  const std::vector<model::broker_shard>& bs,
  model::revision_id change_revision) {
    auto group_cfg = partition->group_configuration();
    vlog(
      clusterlog.trace,
      "[{}] checking if configuration {} is up to date with {}",
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
          "[{}] configuration revision '{}' is smaller than requested "
          "update revision '{}'",
          partition->ntp(),
          group_cfg.revision_id(),
          change_revision);
        return errc::partition_configuration_revision_not_updated;
    }
    const bool includes_self = contains_node(self, bs);

    /*
     * if configuration includes current node, we expect configuration to be
     * fully up to date, as the node will continue to receive all the raft group
     * requests. This is why we claim configuration as not being up to date when
     * it is joint configuration type.
     *
     * NOTE: why include_self matters
     *
     * If the node is not included in current configuration there is no
     * guarantee that it will receive configuration that was moving consensus
     * from JOINT to SIMPLE. Also if the node is removed from replica set we
     * will only remove the partition after other node claim update as finished.
     *
     */
    if (
      includes_self
      && group_cfg.get_state() != raft::configuration_state::simple) {
        vlog(
          clusterlog.trace,
          "[{}] contains current node and consensus configuration is still "
          "in joint state",
          partition->ntp());
        return errc::partition_configuration_in_joint_mode;
    }
    /*
     * if replica set is a leader it must have configuration committed i.e. it
     * was successfully replicated to majority of followers.
     */
    if (partition->is_elected_leader() && !configuration_committed) {
        vlog(
          clusterlog.trace,
          "[{}] current node is a leader, waiting for configuration to be "
          "committed",
          partition->ntp());
        return errc::partition_configuration_leader_config_not_committed;
    }

    /**
     * at this point we just compare the configuration broker ids if they are
     * the same as expected, we claim configuration as being up to date
     */
    if (!are_configuration_replicas_up_to_date(group_cfg, bs)) {
        vlog(
          clusterlog.trace,
          "[{}] requested replica set {} differs from partition replica set: "
          "{}",
          partition->ntp(),
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
  , _self(*config::node().node_id())
  , _data_directory(config::node().data_directory().as_sstring())
  , _housekeeping_timer_interval(
      config::shard_local_cfg().controller_backend_housekeeping_interval_ms())
  , _as(as) {}

ss::future<> controller_backend::stop() {
    vlog(clusterlog.info, "Stopping Controller Backend...");
    _housekeeping_timer.cancel();
    return _gate.close();
}

void controller_backend::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cluster:controller"),
      {
        sm::make_gauge(
          "pending_partition_operations",
          [this] { return _topic_deltas.size(); },
          sm::description(
            "Number of partitions with ongoing/requested operations")),
      });
}

ss::future<> controller_backend::start() {
    setup_metrics();
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
    return ss::max_concurrent_for_each(
      _topic_deltas.begin(),
      _topic_deltas.end(),
      1024,
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
        // i.e. we have to execute current operation as it has to create
        // partition with correct offset
        if (auto next = std::next(it); next != deltas.rend()) {
            if (
              (next->type == op_t::update_finished || next->type == op_t::add)
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

ss::future<>
controller_backend::bootstrap_ntp(const model::ntp& ntp, deltas_t& deltas) {
    // find last delta that has to be applied
    auto bootstrap_deltas = calculate_bootstrap_deltas(_self, deltas);
    vlog(
      clusterlog.trace,
      "[{}] bootstrapping with deltas {}",
      ntp,
      bootstrap_deltas);

    // if empty do nothing
    if (bootstrap_deltas.empty()) {
        return ss::now();
    }

    auto& first_delta = bootstrap_deltas.front();
    vlog(
      clusterlog.info,
      "[{}] Bootstrapping deltas: first - {}, last - {}",
      ntp,
      first_delta,
      bootstrap_deltas.back());
    // if first operation is a cross core update, find initial revision on
    // current node and store it in bootstrap map
    using op_t = topic_table::delta::op_type;
    if (is_cross_core_update(_self, first_delta)) {
        vlog(
          clusterlog.trace,
          "[{}] first bootstrap delta is a cross core update, looking for "
          "initial revision",
          ntp);
        // find operation that created current partition on this node
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
        // if we found update finished operation it is preceding operation that
        // created partition on current node
        vlog(
          clusterlog.trace,
          "[{}] first operation that doesn't contain current node - {}",
          ntp,
          *it);
        /**
         * At this point we may have two situations
         * 1. replica was created on current node shard when partition was
         *    created, with addition delta, in this case `it` contains this
         *    addition delta.
         *
         * 2. replica was moved to this node with `update` delta type, in this
         *    case `it` contains either `update_finished` delta from previous
         *    operation or `add` delta from previous operation. In this case
         *    operation does not contain current node and we need to execute
         *    operation that is following the found one as this is the first
         *    operation that created replica on current node
         *
         */
        if (!contains_node(_self, it->new_assignment.replicas)) {
            vassert(
              it != deltas.rbegin(),
              "operation {} must have following operation that created a "
              "replica on current node",
              *it);
            it = std::prev(it);
        }
        vlog(
          clusterlog.trace, "[{}] initial revision source delta: {}", ntp, *it);
        // persist revision in order to create partition with correct revision
        _bootstrap_revisions[ntp] = model::revision_id(it->offset());
    }
    // apply all deltas following the one found previously
    deltas = std::move(bootstrap_deltas);
    return reconcile_ntp(deltas);
}

ss::future<> controller_backend::fetch_deltas() {
    return _topics.local()
      .wait_for_changes(_as.local())
      .then([this](fragmented_vector<topic_table::delta> deltas) {
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
    ssx::spawn_with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _as.local().abort_requested(); },
          [this] {
              return fetch_deltas()
                .then([this] { return reconcile_topics(); })
                .handle_exception_type(
                  [](const ss::abort_requested_exception&) {
                      // Shutting down: don't log this exception as an error
                      vlog(
                        clusterlog.debug,
                        "Abort requested while reconciling topics");
                  })
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
    ssx::background
      = ssx::spawn_with_gate_then(_gate, [this] {
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

namespace {
using deltas_t = std::vector<cluster::topic_table_delta>;
deltas_t::iterator
find_interrupting_operation(deltas_t::iterator current_it, deltas_t& deltas) {
    /**
     * Find interrupting operation following the one that is currently
     * processed. Following rules apply:
     *
     * - all operations i.e. update, cancel_update, and force abort must be
     * interrupted by deletion
     *
     * - update & cancel update operations may be interrupted by
     * force_abort_update operation
     *
     * - update operation may be interrupted by cancel_update operation or
     * force_abort_update operation
     *
     */

    // only reconfiguration operations may be interrupted
    if (!current_it->is_reconfiguration_operation()) {
        return deltas.end();
    }

    return std::find_if(
      current_it, deltas.end(), [&current_it](const topic_table::delta& d) {
          switch (d.type) {
          case topic_table::delta::op_type::del:
              return true;
          case topic_table::delta::op_type::cancel_update:
              return current_it->type == topic_table::delta::op_type::update
                     && d.new_assignment.replicas
                          == current_it->previous_replica_set;
          case topic_table::delta::op_type::force_abort_update:
              return (
                current_it->type == topic_table::delta::op_type::update
                && d.new_assignment.replicas
                     == current_it->previous_replica_set) || (
                current_it->type == topic_table::delta::op_type::cancel_update
                && d.new_assignment.replicas
                     == current_it->new_assignment.replicas);
          default:
              return false;
          }
      });
}
ss::future<std::error_code> revert_configuration_update(
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& replicas,
  const topic_table_delta::revision_map_t& replica_revisions,
  model::revision_id rev,
  ss::lw_shared_ptr<partition> p,
  members_table& members) {
    auto brokers = create_brokers_set(replicas, replica_revisions, members);
    vlog(
      clusterlog.debug,
      "[{}] reverting already finished reconfiguration. Revision: {}, replica "
      "set: {}",
      ntp,
      rev,
      replicas);
    co_return co_await p->update_replica_set(std::move(brokers), rev);
}
} // namespace

ss::future<> controller_backend::reconcile_ntp(deltas_t& deltas) {
    bool stop = false;
    auto it = deltas.begin();
    while (!(stop || it == deltas.end())) {
        // start_topics_reconciliation_loop will catch this during shutdown
        _as.local().check();

        if (has_non_replicable_op_type(*it)) {
            /// This if statement has nothing to do with correctness and is only
            /// here to reduce the amount of unnecessary logging emitted by the
            /// controller_backend for events that it eventually will not handle
            /// anyway.
            ++it;
            continue;
        }
        auto interrupt_it = find_interrupting_operation(it, deltas);
        if (interrupt_it != deltas.end()) {
            vlog(
              clusterlog.trace,
              "[{}] cancelling current: {} operation with: {}",
              it->ntp,
              *it,
              *interrupt_it);
            while (it != interrupt_it) {
                if (it->type == topic_table_delta::op_type::update_properties) {
                    co_await process_partition_properties_update(
                      it->ntp, it->new_assignment);
                }
                ++it;
            }
        }
        try {
            auto ec = co_await execute_partition_op(*it);
            if (ec) {
                if (it->is_reconfiguration_operation()) {
                    /**
                     * do not skip cross core partition updates waiting for
                     * partition to be shut down on the other core
                     */
                    if (ec == errc::waiting_for_partition_shutdown) {
                        stop = true;
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

                    /**
                     * Some of the deltas may exist in between reconfiguration
                     * operation and update_finished command. Those are:
                     * `update_properties`, `delete`, `cancel_update`, and
                     * `force_abort_update`.
                     *
                     * Execute `update_properties` immediately or just skip
                     * current reconfiguration update.
                     */
                    if (fit != deltas.end()) {
                        while (++it != fit) {
                            vlog(
                              clusterlog.trace,
                              "[{}] executing (during update) operation: {}",
                              it->ntp,
                              *it);
                            if (
                              it->type
                              == topic_table_delta::op_type::
                                update_properties) {
                                co_await process_partition_properties_update(
                                  it->ntp, it->new_assignment);
                            } else if (
                              it->type == topic_table_delta::op_type::del
                              || it->type
                                   == topic_table_delta::op_type::cancel_update
                              || it->type
                                   == topic_table_delta::op_type::
                                     force_abort_update) {
                                break;
                            } else {
                                vassert(
                                  false,
                                  "Invalid delta during topic update - {}",
                                  *it);
                            }
                        }
                        continue;
                    }
                }
                vlog(
                  clusterlog.info,
                  "[{}] result: {} operation: {{type: {}, revision: {}, "
                  "assignment: {}, previous assignment: {}}}",
                  it->ntp,
                  ec.message(),
                  it->type,
                  it->offset,
                  it->new_assignment,
                  it->previous_replica_set);
                stop = true;
                continue;
            }
            vlog(
              clusterlog.debug,
              "[{}] finished operation: {{type: {}, revision: {}, assignment: "
              "{}, previous assignment: {}}}",
              it->ntp,
              it->type,
              it->offset,
              it->new_assignment,
              it->previous_replica_set);
        } catch (ss::gate_closed_exception const&) {
            vlog(
              clusterlog.debug,
              "[{}] gate_closed-exception while executing partition operation: "
              "{}",
              it->ntp,
              *it);
            stop = true;
            continue;
        } catch (ss::sleep_aborted const&) {
            stop = true;
            continue;
        } catch (ss::abort_requested_exception const&) {
            stop = true;
            continue;
        } catch (...) {
            vlog(
              clusterlog.error,
              "[{}] exception while executing partition operation: {} - {}",
              it->ntp,
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
        return ss::max_concurrent_for_each(
                 _topic_deltas.begin(),
                 _topic_deltas.end(),
                 1024,
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

ss::future<std::error_code>
controller_backend::execute_partition_op(const topic_table::delta& delta) {
    using op_t = topic_table::delta::op_type;
    /**
     * Revision is derived from delta offset, i.e. offset of a command that
     * the delta is derived from. The offset is always monotonically
     * increasing together with cluster state evolution hence it is perfect
     * as a source of revision_id
     */
    vlog(
      clusterlog.trace,
      "[{}] executing operation: {{type: {}, revision: {}, assignment: {}, "
      "previous assignment: {}}}",
      delta.ntp,
      delta.type,
      delta.offset,
      delta.new_assignment,
      delta.previous_replica_set);
    const model::revision_id rev(delta.offset());
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
        [[fallthrough]];
    case op_t::del_non_replicable:
        vassert(
          false,
          "controller_backend attempted to process an event that should only "
          "be handled by coproc::reconciliation_backend");
    case op_t::del:
        return delete_partition(delta.ntp, rev, partition_removal_mode::global)
          .then(
            [this, &delta, rev]() { return cleanup_orphan_files(delta, rev); })
          .then([] { return std::error_code(errc::success); });
    case op_t::update:
    case op_t::force_abort_update:
    case op_t::cancel_update:
        vassert(
          delta.previous_replica_set,
          "reconfiguration delta must have previous replica set, current "
          "delta: {}",
          delta);
        vassert(
          delta.replica_revisions,
          "replica revisions map must be present in reconfiguration type "
          "delta: {}",
          delta);
        return process_partition_reconfiguration(
          delta.type,
          delta.ntp,
          delta.new_assignment,
          *delta.previous_replica_set,
          *delta.replica_revisions,
          rev);
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
controller_backend::acquire_cross_shard_move_request(
  model::ntp ntp, ss::shard_id shard) {
    using ret_t = std::optional<controller_backend::cross_shard_move_request>;
    return container().invoke_on(
      shard, [ntp = std::move(ntp)](controller_backend& remote) {
          if (auto it = remote._cross_shard_requests.find(ntp);
              it != remote._cross_shard_requests.end()) {
              ret_t ret{it->second};
              remote._cross_shard_requests.erase(it);
              return ret;
          }
          return ret_t{};
      });
}

ss::future<> controller_backend::release_cross_shard_move_request(
  model::ntp ntp,
  ss::shard_id shard,
  controller_backend::cross_shard_move_request token) {
    return container().invoke_on(
      shard,
      [ntp = std::move(ntp),
       token = std::move(token)](controller_backend& remote) mutable {
          remote._cross_shard_requests.emplace(ntp, std::move(token));
      });
}

ss::future<std::error_code>
controller_backend::process_partition_reconfiguration(
  topic_table_delta::op_type type,
  model::ntp ntp,
  const partition_assignment& target_assignment,
  const std::vector<model::broker_shard>& previous_replicas,
  const topic_table_delta::revision_map_t& replica_revisions,
  model::revision_id rev) {
    vlog(
      clusterlog.trace,
      "[{}] processing reconfiguration {} command with target replicas: {}, "
      "previous replica set: {}, revision: {}",
      ntp,
      type,
      target_assignment.replicas,
      previous_replicas,
      rev);

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
          "[{}] found newer revision, finishing reconfiguration to: {}",
          ntp,
          target_assignment.replicas);
        co_return std::error_code(errc::success);
    }
    /**
     * Check if target assignment has node and core local replicas
     */
    if (!has_local_replicas(_self, target_assignment.replicas)) {
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
        if (contains_node(_self, target_assignment.replicas)) {
            co_return co_await shutdown_on_current_shard(std::move(ntp), rev);
        }

        /**
         * in this case the partition is moved away from this node/shard, we may
         * have to execute reconfiguration as this node may be a leader.
         * Reconfiguration may involve triggering, cancelling or aborting
         * configuration change.
         *
         */
        co_return co_await execute_reconfiguration(
          type, ntp, target_assignment.replicas, replica_revisions, rev);
    }
    const auto cross_core_move = contains_node(_self, previous_replicas)
                                 && !has_local_replicas(
                                   _self, previous_replicas);
    /**
     * in this situation partition is expected to
     * exists on current broker/shard.
     *
     * If partition already exists, update its
     * configuration and wait for it to be applied
     */
    if (partition) {
        // if requested assignment is equal to current one, just finish the
        // update it is an noop
        if (are_assignments_equal(target_assignment, previous_replicas)) {
            if (target_assignment.replicas.front().node_id == _self) {
                co_return co_await dispatch_update_finished(
                  std::move(ntp), target_assignment);
            }
        }
        // try executing reconfiguration, this method will return success if
        // partition configuration is up to date with requested assignment
        auto ec = co_await execute_reconfiguration(
          type, ntp, target_assignment.replicas, replica_revisions, rev);
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
            if (can_finish_update(
                  partition->get_leader_id(),
                  type,
                  target_assignment.replicas,
                  previous_replicas)) {
                co_return co_await dispatch_update_finished(
                  std::move(ntp), target_assignment);
            }
            co_return ec;
        }

        /**
         * If update wasn't successful (f.e. current node is not a leader),
         * wait for next iteration.
         */
        co_return ec;
    }
    // partition is requested to exists on current shard and it is going to be
    // moved from the other core
    if (cross_core_move) {
        auto previous_shard = get_target_shard(_self, previous_replicas);

        co_return co_await create_partition_from_remote_shard(
          ntp, *previous_shard, target_assignment);
    }
    /**
     * Cancelling partition movement may only be executed before the update
     * finished. We only remove partition replicas when processing
     * `update_finished` delta. This is why partitions will never have to be
     * created with cancel/abort type of deltas.
     */
    vassert(
      type == topic_table_delta::op_type::update,
      "Invalid reconciliation loop state. Partition replicas should not be "
      "removed before finishing update, ntp: {}, current operation: {}, "
      "target_assignment: {}",
      ntp,
      type,
      target_assignment);
    /**
     * We expect partition replica to exists on current broker/shard. Create
     * partition. we relay on raft recovery to populate partition
     * configuration.
     */
    auto ec = co_await create_partition(ntp, target_assignment.group, rev, {});
    // wait for recovery, we will mark partition as updated in next
    // controller backend reconciliation loop pass
    if (!ec) {
        co_return errc::waiting_for_recovery;
    }
    co_return ec;
}

bool controller_backend::can_finish_update(
  std::optional<model::node_id> current_leader,
  topic_table_delta::op_type update_type,
  const std::vector<model::broker_shard>& current_replicas,
  const std::vector<model::broker_shard>& previous_replicas) {
    // force abort update may be finished by any node
    if (update_type == topic_table_delta::op_type::force_abort_update) {
        return current_leader == _self;
        ;
    }
    // update may be finished by a node that was added to replica set
    if (!has_local_replicas(_self, previous_replicas)) {
        return true;
    }
    // finally if no nodes were added to replica set one of the current replicas
    // may finish an update

    auto added_nodes = subtract_replica_sets(
      current_replicas, previous_replicas);
    if (added_nodes.empty()) {
        return has_local_replicas(_self, current_replicas);
    }

    return false;
}

ss::future<std::error_code>
controller_backend::create_partition_from_remote_shard(
  model::ntp ntp,
  ss::shard_id previous_shard,
  partition_assignment requested_assignment) {
    std::optional<model::revision_id> initial_revision;
    std::vector<model::broker> initial_brokers;
    std::optional<cross_shard_move_request> x_shard_req;

    if (auto it = _bootstrap_revisions.find(ntp);
        it != _bootstrap_revisions.end()) {
        initial_revision = it->second;
    } else {
        vlog(
          clusterlog.trace,
          "[{}] waiting for cross core move information from "
          "shard {}",
          ntp,
          previous_shard);
        // ask previous controller for partition initial
        // revision
        x_shard_req = co_await acquire_cross_shard_move_request(
          ntp, previous_shard);

        /*
         * Special case for cancelling x-core partitions movements
         *
         * We may hit a situation in which partition was shutdown on the current
         * core but not yet created on the remote core and then the process was
         * cancelled revision will not be available on the remote core. We must
         * check the current core as this is the one that shutdown the
         * partition.
         */
        if (!x_shard_req) {
            x_shard_req = co_await acquire_cross_shard_move_request(
              ntp, ss::this_shard_id());
            if (x_shard_req) {
                previous_shard = ss::this_shard_id();
            }
        }

        if (!x_shard_req) {
            co_return errc::waiting_for_partition_shutdown;
        }
        initial_revision = x_shard_req->revision;
        std::copy(
          x_shard_req->initial_configuration.brokers().begin(),
          x_shard_req->initial_configuration.brokers().end(),
          std::back_inserter(initial_brokers));
    }

    if (!initial_revision) {
        co_return errc::waiting_for_partition_shutdown;
    }

    std::error_code result = errc::waiting_for_recovery;
    bool has_error = false;
    try {
        vlog(
          clusterlog.trace,
          "[{}] creating partition from shard {}",
          ntp,
          previous_shard);
        if (previous_shard != ss::this_shard_id()) {
            co_await raft::details::move_persistent_state(
              requested_assignment.group,
              previous_shard,
              ss::this_shard_id(),
              _storage);
            co_await raft::offset_translator::move_persistent_state(
              requested_assignment.group,
              previous_shard,
              ss::this_shard_id(),
              _storage);
        }

        auto ec = co_await create_partition(
          ntp,
          requested_assignment.group,
          *initial_revision,
          std::move(initial_brokers));

        if (ec) {
            has_error = true;
            result = ec;
        } else {
            // finally remove bootstrap revision
            _bootstrap_revisions.erase(ntp);
        }
    } catch (...) {
        has_error = true;
        vlog(
          clusterlog.warn,
          "[{}] failed to create partition from shard {}",
          ntp,
          previous_shard);
    }
    /**
     * Release cross shard move request for subsequent retries
     */
    if (has_error) {
        co_await release_cross_shard_move_request(
          ntp, previous_shard, *x_shard_req);
    }

    co_return result;
}

ss::future<std::error_code> controller_backend::execute_reconfiguration(
  topic_table_delta::op_type type,
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& replica_set,
  const topic_table_delta::revision_map_t& replica_revisions,
  model::revision_id revision) {
    switch (type) {
    case topic_table_delta::op_type::update:
        co_return co_await update_partition_replica_set(
          ntp, replica_set, replica_revisions, revision);
    case topic_table_delta::op_type::cancel_update:
        co_return co_await cancel_replica_set_update(
          ntp, replica_set, replica_revisions, revision);
    case topic_table_delta::op_type::force_abort_update:
        co_return co_await force_abort_replica_set_update(
          ntp, replica_set, replica_revisions, revision);
    default:
        vassert(
          false, "delta of type {} is not partition reconfiguration", type);
    }
    __builtin_unreachable();
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

    // partition doesn't exists, it must already have been removed, do
    // nothing
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
      "[{}] updating configuration with properties: {}",
      ntp,
      cfg->properties);
    co_await partition->update_configuration(cfg->properties);
}

/**
 * notifies the topics frontend that partition update has been finished, all
 * the interested nodes can now safely remove unnecessary partition
 * replicas.
 */
ss::future<std::error_code> controller_backend::dispatch_update_finished(
  model::ntp ntp, partition_assignment assignment) {
    vlog(clusterlog.trace, "[{}] dispatching update finished event for", ntp);
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
      clusterlog.trace,
      "[{}] processing finished update command, revision: {}",
      ntp,
      rev);
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

    vlog(
      clusterlog.trace,
      "[{}] removing partition replica, revision: {}",
      ntp,
      rev);
    return delete_partition(
      std::move(ntp), rev, partition_removal_mode::local_only);
}

template<typename Func>
ss::future<std::error_code>
controller_backend::apply_configuration_change_on_leader(
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& replicas,
  model::revision_id rev,
  Func&& func) {
    auto partition = _partition_manager.local().get(ntp);
    if (!partition) {
        co_return errc::partition_not_exists;
    }
    // wait for configuration update, only declare success
    // when configuration was actually updated
    auto update_ec = check_configuration_update(
      _self, partition, replicas, rev);

    if (!update_ec) {
        co_return errc::success;
    }
    // we are the leader, update configuration
    if (partition->is_leader()) {
        auto f = func(partition);
        try {
            // TODO: use configurable timeout here
            auto err = co_await ss::with_timeout(
              model::timeout_clock::now() + std::chrono::seconds(5),
              std::move(f));
            if (err) {
                co_return err;
            }
        } catch (const ss::timed_out_error& e) {
            co_return make_error_code(errc::timeout);
        }
        co_return check_configuration_update(_self, partition, replicas, rev);
    }

    co_return errc::not_leader;
}

ss::future<std::error_code> controller_backend::cancel_replica_set_update(
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& replicas,
  const topic_table_delta::revision_map_t& replica_revisions,
  model::revision_id rev) {
    /**
     * Following scenarios can happen in here:
     * - node is a leader for current partition => cancel raft group
     * reconfiguration
     * - node is not a leader for current partition => check if config is
     *   equal to requested, if not return failure
     */

    return apply_configuration_change_on_leader(
      ntp,
      replicas,
      rev,
      [this, &ntp, rev, replicas, &replica_revisions](
        ss::lw_shared_ptr<partition> p) {
          const auto current_cfg = p->group_configuration();
          // we do not have to request update/cancellation twice
          if (current_cfg.revision_id() == rev) {
              return ss::make_ready_future<std::error_code>(
                errc::waiting_for_recovery);
          }

          const auto raft_cfg_update_finished
            = current_cfg.get_state() == raft::configuration_state::simple;

          // raft already finished its part, we need to move replica back
          if (raft_cfg_update_finished) {
              return revert_configuration_update(
                ntp,
                replicas,
                replica_revisions,
                rev,
                std::move(p),
                _members_table.local());
          } else {
              vlog(clusterlog.debug, "[{}] cancelling reconfiguration", ntp);
              return p->cancel_replica_set_update(rev);
          }
      });
}

ss::future<std::error_code> controller_backend::force_abort_replica_set_update(
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& replicas,
  const topic_table_delta::revision_map_t& replica_revisions,
  model::revision_id rev) {
    /**
     * Force abort configuration change for each of the partition replicas.
     */
    auto partition = _partition_manager.local().get(ntp);
    if (!partition) {
        co_return errc::partition_not_exists;
    }
    const auto current_cfg = partition->group_configuration();

    // wait for configuration update, only declare success
    // when configuration was actually updated
    auto update_ec = check_configuration_update(
      _self, partition, replicas, rev);

    if (!update_ec) {
        co_return errc::success;
    }

    // we do not have to request update/cancellation twice
    if (current_cfg.revision_id() == rev) {
        co_return errc::waiting_for_recovery;
    }

    const auto raft_cfg_update_finished = current_cfg.get_state()
                                          == raft::configuration_state::simple;

    if (raft_cfg_update_finished) {
        co_return co_await apply_configuration_change_on_leader(
          ntp,
          replicas,
          rev,
          [this, rev, &replicas, &ntp, &replica_revisions](
            ss::lw_shared_ptr<cluster::partition> p) {
              return revert_configuration_update(
                ntp,
                replicas,
                replica_revisions,
                rev,
                std::move(p),
                _members_table.local());
          });

    } else {
        auto ec = co_await partition->force_abort_replica_set_update(rev);

        if (ec) {
            co_return ec;
        }
        auto current_leader = partition->get_leader_id();
        if (!current_leader.has_value() || current_leader == _self) {
            co_return check_configuration_update(
              _self, partition, replicas, rev);
        }

        co_return errc::waiting_for_recovery;
    }
}

ss::future<std::error_code> controller_backend::update_partition_replica_set(
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& replicas,
  const topic_table_delta::revision_map_t& replica_revisions,
  model::revision_id rev) {
    /**
     * Following scenarios can happen in here:
     * - node is a leader for current partition => just update config
     * - node is not a leader for current partition => check if config is
     *   equal to requested, if not return failure
     */
    return apply_configuration_change_on_leader(
      ntp,
      replicas,
      rev,
      [this, rev, &replicas, &ntp, &replica_revisions](
        ss::lw_shared_ptr<partition> p) {
          auto it = _topics.local().all_topics_metadata().find(
            model::topic_namespace_view(ntp));
          // no longer in progress
          if (it == _topics.local().all_topics_metadata().end()) {
              return ss::make_ready_future<std::error_code>(errc::success);
          }

          auto brokers = create_brokers_set(
            replicas, replica_revisions, _members_table.local());
          vlog(
            clusterlog.debug,
            "[{}] updating replica set with {}",
            ntp,
            replicas);

          return p->update_replica_set(std::move(brokers), rev);
      });
}

ss::future<> controller_backend::add_to_shard_table(
  model::ntp ntp,
  raft::group_id raft_group,
  uint32_t shard,
  model::revision_id revision) {
    // update shard_table: broadcast
    vlog(
      clusterlog.trace,
      "[{}] adding to shard table at shard {} with revision {}",
      ntp,
      shard,
      revision);
    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), raft_group, shard, revision](
        shard_table& s) mutable {
          s.update(ntp, raft_group, shard, revision);
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

    // initial revision of the partition on the moment when it was created
    // the value is used by shadow indexing
    // if topic is read replica, the value from remote topic manifest is
    // used
    auto initial_rev = _topics.local().get_initial_revision(ntp);
    if (!initial_rev) {
        return ss::make_ready_future<std::error_code>(errc::topic_not_exists);
    }
    // no partition exists, create one
    if (likely(!partition)) {
        std::optional<s3::bucket_name> read_replica_bucket;
        if (cfg->is_read_replica()) {
            read_replica_bucket = s3::bucket_name(
              cfg->properties.read_replica_bucket.value());
        }
        // we use offset as an rev as it is always increasing and it
        // increases while ntp is being created again
        f = _partition_manager.local()
              .manage(
                cfg->make_ntp_config(
                  _data_directory, ntp.tp.partition, rev, initial_rev.value()),
                group_id,
                std::move(members),
                cfg->properties.remote_topic_properties,
                read_replica_bucket)
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
    vlog(clusterlog.trace, "[{}] cross core move, shutting down", ntp);
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
        vlog(clusterlog.trace, "[{}] cross core move, stopped", ntp);
        vassert(
          success,
          "only one cross shard request is allowed to be pending for "
          "single "
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

ss::future<> controller_backend::cleanup_orphan_files(
  const topic_table::delta& delta, model::revision_id rev) {
    if (!has_local_replicas(_self, delta.new_assignment.replicas)) {
        return ss::now();
    }
    return _storage.local().log_mgr().remove_orphan(
      _data_directory, delta.ntp, rev);
}

ss::future<> controller_backend::delete_partition(
  model::ntp ntp, model::revision_id rev, partition_removal_mode mode) {
    // The partition leaders table contains partition leaders for all
    // partitions accross the cluster. For this reason, when deleting a
    // partition (i.e. removal mode is global), we need to delete from the table
    // regardless of whether a replica of 'ntp' is present on the node.
    if (mode == partition_removal_mode::global) {
        co_await _partition_leaders_table.invoke_on_all(
          [ntp, rev](partition_leaders_table& leaders) {
              leaders.remove_leader(ntp, rev);
          });
    }

    auto part = _partition_manager.local().get(ntp);
    // partition is not replicated locally or it was already recreated with
    // greater rev, do nothing
    if (part.get() == nullptr || part->get_revision_id() > rev) {
        co_return;
    }

    auto group_id = part->group();

    co_await _shard_table.invoke_on_all(
      [ntp, group_id, rev](shard_table& st) mutable {
          st.erase(ntp, group_id, rev);
      });

    co_await _partition_manager.local().remove(ntp, mode);
}

std::vector<topic_table::delta>
controller_backend::list_ntp_deltas(const model::ntp& ntp) const {
    if (auto it = _topic_deltas.find(ntp); it != _topic_deltas.end()) {
        return it->second;
    }

    return {};
}

} // namespace cluster
