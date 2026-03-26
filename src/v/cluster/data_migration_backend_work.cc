/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/topic_manifest_downloader.h"
#include "cloud_storage/topic_mount_handler.h"
#include "cluster/data_migration_backend.h"
#include "cluster/partition_leaders_table.h"
#include "config/node_config.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "data_migration_frontend.h"
#include "data_migration_types.h"
#include "data_migration_worker.h"
#include "errc.h"
#include "fwd.h"
#include "logger.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "ssx/async_algorithm.h"
#include "ssx/future-util.h"
#include "topic_configuration.h"
#include "topic_table.h"
#include "topics_frontend.h"
#include "types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>

#include <chrono>
#include <exception>
#include <memory>
#include <optional>
#include <ranges>

using namespace std::chrono_literals;

namespace cluster::data_migrations {
namespace {
template<class TryFunc>
ss::future<errc> retry_loop(retry_chain_node& rcn, TryFunc try_func) {
    while (true) {
        errc ec;
        try {
            ec = co_await try_func();
            if (
              ec == cluster::errc::success
              || ec == cluster::errc::shutting_down) {
                co_return ec;
            }
        } catch (...) {
            vlog(
              cluster::data_migrations::dm_log.warn,
              "caught exception in retry loop: {}",
              std::current_exception());
            ec = errc::topic_operation_error;
        }
        if (auto perm = rcn.retry(); perm.is_allowed) {
            co_await ss::sleep_abortable(perm.delay, *perm.abort_source);
        } else {
            co_return ec;
        }
    }
}
} // namespace

ss::future<errc>
backend::delete_topic(const model::topic_namespace& nt, retry_chain_node& rcn) {
    return retry_loop(rcn, [this, &nt, &rcn]() {
        return _topics_frontend
          .delete_topic_after_migration(nt, rcn.get_deadline())
          .then([&nt](errc ec) {
              if (ec == errc::topic_not_exists) {
                  vlog(dm_log.warn, "topic {} missing, ignoring", nt);
                  return errc::success;
              }
              return ec;
          });
    });
}

ss::future<errc> backend::unmount_not_existing_topic(
  const model::topic_namespace& nt,
  const cloud_storage::topic_manifest& manifest,
  retry_chain_node& rcn) {
    return retry_loop(rcn, [this, &nt, &manifest, &rcn] {
        return do_unmount_not_existing_topic(nt, manifest, rcn);
    });
}

ss::future<errc> backend::do_unmount_not_existing_topic(
  const model::topic_namespace& nt,
  const cloud_storage::topic_manifest& manifest,
  retry_chain_node& rcn) {
    auto& cfg = manifest.get_topic_config();
    if (!cfg) {
        vlog(
          dm_log.warn,
          "topic {} configuration missing in manifest, cannot unmount",
          nt);
        co_return errc::topic_operation_error;
    }

    auto rev_id = manifest.get_revision();

    auto umnt_res = co_await _topic_mount_handler->get().unmount_topic(
      *cfg, rev_id, rcn);
    if (umnt_res == cloud_storage::topic_unmount_result::success) {
        co_return errc::success;
    }
    vlog(dm_log.warn, "failed to unmount topic {}: {}", nt, umnt_res);
    co_return errc::topic_operation_error;
}

ss::future<errc> backend::unmount_topic(
  const model::topic_namespace& nt, retry_chain_node& rcn) {
    return retry_loop(
      rcn, [this, &nt, &rcn] { return do_unmount_topic(nt, rcn); });
}

ss::future<errc> backend::do_unmount_topic(
  const model::topic_namespace& nt, retry_chain_node& rcn) {
    vlog(dm_log.trace, "trying to unmount {} topic", nt);
    auto cfg = _topic_table.get_topic_cfg(nt);
    if (!cfg) {
        vlog(dm_log.warn, "topic {} missing, ignoring", nt);
        co_return errc::success;
    }

    auto rev_id = _topic_table.get_initial_revision(nt);
    if (!rev_id) {
        vlog(dm_log.warn, "topic {} missing, ignoring", nt);
        co_return errc::success;
    }

    auto umnt_res = co_await _topic_mount_handler->get().unmount_topic(
      *cfg, *rev_id, rcn);
    if (umnt_res == cloud_storage::topic_unmount_result::success) {
        co_return errc::success;
    }
    vlog(dm_log.warn, "failed to unmount topic {}: {}", nt, umnt_res);
    co_return errc::topic_operation_error;
}

void backend::to_advance_if_done(mrstate_cit_t it) {
    auto& rs = it->second;
    if (rs.outstanding_topics.empty()) {
        auto sought_state = *rs.scope.sought_state;
        auto [ar_it, ins] = _advance_requests.try_emplace(
          it->first, sought_state);
        if (!ins && ar_it->second.sought_state < sought_state) {
            ar_it->second = advance_info(sought_state);
        }
        _migration_states.erase(it);
    } else {
        vlog(
          dm_log.trace,
          "outstanding topics for migration {}: [{}]",
          it->first,
          fmt::join(rs.outstanding_topics | std::views::keys, ", "));
    }
}

ss::future<> backend::advance(id migration_id, state sought_state) {
    std::error_code ec;
    if (sought_state == state::deleted) {
        ec = co_await _frontend.remove_migration(migration_id);
    } else {
        ec = co_await _frontend.update_migration_state(
          migration_id, sought_state);
    }
    bool success = ec == make_error_code(errc::success);
    vlogl(
      dm_log,
      success ? ss::log_level::debug : ss::log_level::warn,
      "request to advance migration {} into state {} has "
      "been processed with error code {}",
      migration_id,
      sought_state,
      ec);
    if (!success) {
        co_await ss::sleep_abortable(5s, _as);
        auto it = _advance_requests.find(migration_id);
        if (
          it != _advance_requests.end()
          && it->second.sought_state == sought_state) {
            it->second.sent = false;
            wakeup();
        }
    }
}

void backend::spawn_advances() {
    for (auto& [migration_id, advance_info] : _advance_requests) {
        if (advance_info.sent) {
            continue;
        }
        advance_info.sent = true;
        auto sought_state = advance_info.sought_state;
        ssx::spawn_with_gate(_gate, [this, migration_id, sought_state]() {
            return advance(migration_id, sought_state);
        });
    }
}

ss::future<> backend::handle_raft0_leadership_update() {
    auto units = co_await _mutex.get_units(_as);
    vlog(
      dm_log.trace,
      "_raft0_leader_term={}, _coordinator_term={}",
      _raft0_leader_term,
      _coordinator_term);
    if (_raft0_leader_term == _coordinator_term) {
        // multiple leadership updates have been handled in an earlier call
        co_return;
    }

    auto old_coordinator_term = _coordinator_term;
    _coordinator_term = _raft0_leader_term;

    // We need to restart coordinating if another node have been a leader and
    // potentially a coordinator between our coordinatorship terms.
    // This is to recollect metadata of affected topics. While
    // data_migrated_resources guards from topic metadata changes in presence of
    // an active migration, another coordinator may have start or complete
    // migrations between our terms.

    if (old_coordinator_term) {
        vlog(dm_log.debug, "stepping down as a coordinator");
        // stop topic-scoped work
        co_await abort_all_topic_work();
        // stop coordinating
        for (auto& [id, mrstate] : _migration_states) {
            co_await ssx::async_for_each(
              mrstate.outstanding_topics | std::views::values,
              std::mem_fn(&topic_reconciliation_state::clear));
        }
        _nodes_to_retry.clear();
        _node_states.clear();
        _topic_work_to_retry.clear();
    }

    if (_coordinator_term) {
        vlog(dm_log.debug, "stepping up as a coordinator");
        // start coordinating
        for (auto& [id, mrstate] : _migration_states) {
            for (auto& [nt, tstate] : mrstate.outstanding_topics) {
                co_await reconcile_existing_topic(
                  nt, tstate, id, mrstate.scope, mrstate.revision_id, false);
            }
        }
        wakeup();
    }
}

ss::future<> backend::handle_migration_update(id id) {
    vlog(dm_log.debug, "received data migration {} notification", id);
    auto units = co_await _mutex.get_units(_as);
    vlog(dm_log.debug, "lock acquired for data migration {} notification", id);

    auto new_ref = _table.get_migration(id);
    // copying as it may go from the table on scheduling points
    auto new_metadata = new_ref.transform(
      [](const auto& new_ref) { return new_ref.get().copy(); });
    auto new_state = new_metadata.transform(
      [](const auto& md) { return md.state; });
    vlog(dm_log.debug, "migration {} new state is {}", id, new_state);

    work_scope new_scope;
    if (new_metadata) {
        new_scope = get_work_scope(*new_metadata);
    }

    std::optional<partition_consumer_group_map_t> group_map;

    // forget about the migration if it went forward or is gone
    auto old_it = _migration_states.find(id);
    if (old_it != _migration_states.cend()) {
        const migration_reconciliation_state& old_mrstate = old_it->second;
        vlog(
          dm_log.debug,
          "migration {} old sought state is {}",
          id,
          old_mrstate.scope.sought_state);
        vassert(
          !new_scope.sought_state
            || new_scope.sought_state >= old_mrstate.scope.sought_state,
          "migration state went from seeking {} back seeking to seeking {}",
          old_mrstate.scope.sought_state,
          new_state);
        vlog(dm_log.debug, "dropping migration {} reconciliation state", id);
        group_map.emplace(std::move(*old_it->second.partition_group_map));
        co_await drop_migration_reconciliation_rstate(old_it);
    }
    // delete old advance requests
    if (auto it = _advance_requests.find(id); it != _advance_requests.end()) {
        if (!new_state || it->second.sought_state <= new_state) {
            _advance_requests.erase(it);
        }
    }
    // create new state if needed
    if (new_scope.sought_state) {
        vlog(dm_log.debug, "creating migration {} reconciliation state", id);
        auto new_it = _migration_states.emplace_hint(
          old_it,
          id,
          migration_reconciliation_state{new_scope, new_metadata->revision_id});
        if (
          new_scope.topic_work_needed
          || new_scope.any_partition_work_needed()) {
            if (group_map) {
                new_it->second.partition_group_map = std::move(*group_map);
            }
            co_await reconcile_migration(new_it->second, *new_metadata);
        } else {
            // yes it is done as there is nothing to do
            to_advance_if_done(new_it);
        }
    }

    if (new_scope.sought_state && _coordinator_term) {
        wakeup();
    }
}

ss::future<> backend::process_delta(cluster::topic_table_ntp_delta&& delta) {
    vlog(dm_log.debug, "processing topic table delta={}", delta);

    if (
      delta.type == topic_table_ntp_delta_type::added
      || delta.type == topic_table_ntp_delta_type::removed) {
        // it can be only ourselves, as partition changes are not allowed when
        // the topic migration is in one of the states tracked here
        co_return;
    }

    model::topic_namespace nt{delta.ntp.ns, delta.ntp.tp.topic};
    auto it = _topic_migration_map.find(nt);
    if (it == _topic_migration_map.end()) {
        co_return;
    }

    vassert(
      delta.type == topic_table_ntp_delta_type::replicas_updated
        || delta.type == topic_table_ntp_delta_type::disabled_flag_updated,
      "topic {} altered with topic_table_delta_type={} during "
      "migrations {}",
      nt,
      delta.type,
      it->second);

    for (const auto migration_id : it->second) {
        // coordination
        auto& mrstate = _migration_states.find(migration_id)->second;
        if (
          !mrstate.scope.partition_work_needed(nt)
          && !mrstate.scope.topic_work_needed) {
            continue;
        }
        auto& tstate = mrstate.outstanding_topics.at(nt);
        clear_tstate_belongings({nt, migration_id}, tstate);
        tstate.clear();
        // We potentially re-enqueue an already coordinated partition here.
        // The first RPC reply will clear it.
        co_await reconcile_existing_topic(
          nt, tstate, migration_id, mrstate.scope, mrstate.revision_id, false);

        // local partition work
        if (has_local_replica(delta.ntp)) {
            const auto& mrstate = _migration_states.at(migration_id);
            _local_work_states[nt][delta.ntp.tp.partition].try_emplace(
              migration_id,
              *mrstate.scope.sought_state,
              mrstate.revision_id,
              migrated_replica_status::waiting_for_rpc);
        } else {
            // find an entry in the nested structure
            auto rwstates4topic_it = _local_work_states.find(nt);
            if (rwstates4topic_it == _local_work_states.end()) {
                continue;
            }
            auto& rwstates4topic = rwstates4topic_it->second;
            auto rwstates4partition_it = rwstates4topic.find(
              delta.ntp.tp.partition);
            if (rwstates4partition_it == rwstates4topic.end()) {
                continue;
            }
            auto& rwstates4partition = rwstates4partition_it->second;
            auto rwstate_it = rwstates4partition.find(migration_id);
            if (rwstate_it == rwstates4partition.end()) {
                continue;
            }
            // stop work for it
            if (rwstate_it->second.shard) {
                stop_partition_work(delta.ntp, *rwstate_it);
            }
            // delete entry from the nested structure
            rwstates4partition.erase(rwstate_it);
            if (!rwstates4partition.empty()) {
                continue;
            }
            rwstates4topic.erase(rwstates4partition_it);
            if (!rwstates4topic.empty()) {
                continue;
            }
            _local_work_states.erase(rwstates4topic_it);
        }
    }
}

void backend::handle_shard_update(
  const model::ntp& ntp, raft::group_id, std::optional<ss::shard_id> shard) {
    if (auto maybe_rwstates = get_replica_work_states(ntp)) {
        for (auto& rwstate : maybe_rwstates->get()) {
            if (rwstate.second.status == migrated_replica_status::can_run) {
                update_partition_shard(ntp, rwstate, shard);
            }
        }
    }
}

ss::future<check_ntp_states_reply>
backend::check_ntp_states_locally(check_ntp_states_request req) {
    vlog(dm_log.debug, "processing node request {}", req);
    check_ntp_states_reply reply;
    co_await ssx::async_for_each(
      req.sought_states, [this, &reply](const auto& ntp_req) {
          vlog(
            dm_log.trace,
            "received an RPC to promote ntp {} to state {} for migration {}",
            ntp_req.ntp,
            ntp_req.state,
            ntp_req.migration);
          // due to async notification processing we may get fresher state
          // than we have in rwstate; this is fine
          const auto maybe_migration = _table.get_migration(ntp_req.migration);
          if (!maybe_migration) {
              // migration either not yet there or gone, and we cannot tell
              // for sure => no reply
              vlog(
                dm_log.trace,
                "migration {} not found, ignoring",
                ntp_req.migration);
              return;
          }

          const auto& metadata = maybe_migration->get();
          if (metadata.state >= ntp_req.state) {
              vlog(
                dm_log.trace,
                "migration {} already in state {}, no partition work needed",
                ntp_req.migration,
                metadata.state);
              // report progress migration-wise, whether or not made by us
              reply.actual_states.push_back(
                {.ntp = ntp_req.ntp,
                 .migration = metadata.id,
                 .state = metadata.state});
              return;
          }

          model::topic_namespace_view ntp_view{ntp_req.ntp};
          auto& rwstate
            = *_local_work_states[ntp_view][ntp_req.ntp.tp.partition]
                 .try_emplace(
                   ntp_req.migration,
                   ntp_req.state,
                   std::nullopt,
                   migrated_replica_status::waiting_for_controller_update)
                 .first;

          if (ntp_req.state > rwstate.second.sought_state) {
              // RPC request indicates that partition work has already
              // progressed to a later state. Stop current work and wait for
              // the controller update.
              if (rwstate.second.shard) {
                  stop_partition_work(ntp_req.ntp, rwstate);
              }
              rwstate.second = {
                ntp_req.state,
                std::nullopt,
                migrated_replica_status::waiting_for_controller_update};
          } else if (ntp_req.state < rwstate.second.sought_state) {
              vlog(
                dm_log.warn,
                "migration_id={} got RPC to move ntp {} to state {}, but "
                "current replica work state is {}, ignoring",
                ntp_req.migration,
                ntp_req.ntp,
                ntp_req.state,
                rwstate.second);
              return;
          }

          switch (rwstate.second.status) {
          case migrated_replica_status::waiting_for_controller_update:
              break;
          case migrated_replica_status::waiting_for_rpc:
              // raft0 and RPC agree => time to do it!
              rwstate.second.status = migrated_replica_status::can_run;
              [[fallthrough]];
          case migrated_replica_status::can_run: {
              auto new_shard = _shard_table.shard_for(ntp_req.ntp);
              update_partition_shard(ntp_req.ntp, rwstate, new_shard);
              break;
          }
          case migrated_replica_status::done:
              reply.actual_states.push_back(
                {.ntp = ntp_req.ntp,
                 .migration = metadata.id,
                 .state = ntp_req.state});
          }
      });

    vlog(dm_log.debug, "node request reply: {}", reply);
    co_return reply;
}

void backend::update_partition_shard(
  const model::ntp& ntp,
  rwstate_entry& rwstate,
  std::optional<ss::shard_id> new_shard) {
    vlog(
      dm_log.trace,
      "for ntp {} for migration {} seeking state {} updating shard: {} => "
      "{}",
      ntp,
      rwstate.first,
      rwstate.second.sought_state,
      rwstate.second.shard,
      new_shard);
    if (new_shard != rwstate.second.shard) {
        if (rwstate.second.shard) {
            stop_partition_work(ntp, rwstate);
        }
        rwstate.second.shard = new_shard;
        if (new_shard) {
            start_partition_work(ntp, rwstate);
        }
    }
}

void backend::clear_tstate_belongings(
  const topic_namespace_migration& tnm,
  const topic_reconciliation_state& tstate) {
    const auto& partitions = tstate.outstanding_partitions;
    for (const auto& [partition, nodes] : partitions) {
        for (const model::node_id& node : nodes) {
            auto ns_it = _node_states.find(node);
            vassert(
              ns_it != _node_states.end(),
              "node {} not found in node_states when clearing tstate "
              "belongings for topic {} migration {}",
              node,
              tnm.nt,
              tnm.migration);
            ns_it->second.erase(
              {{tnm.nt.ns, tnm.nt.tp, partition}, tnm.migration});
            if (ns_it->second.empty()) {
                _nodes_to_retry.erase(node);
                _node_states.erase(ns_it);
            }
        }
    }
    _topic_work_to_retry.erase(tnm);
}

ss::future<>
backend::drop_migration_reconciliation_rstate(mrstate_cit_t rs_it) {
    const auto& topics = rs_it->second.outstanding_topics;

    co_await ss::parallel_for_each(
      topics,
      [this, migration_id = rs_it->first](
        const topic_map_t::value_type& topic_map_entry) {
          return clear_tstate(migration_id, topic_map_entry);
      });
    _migration_states.erase(rs_it);
}

ss::future<> backend::clear_tstate(
  id migration_id, const topic_map_t::value_type& topic_map_entry) {
    const auto& [nt, tstate] = topic_map_entry;
    clear_tstate_belongings({nt, migration_id}, tstate);
    auto topic_work_it = _local_work_states.find(nt);
    if (topic_work_it != _local_work_states.end()) {
        auto& topic_work_state = topic_work_it->second;
        co_await ssx::async_for_each(
          topic_work_state,
          [this, migration_id, &nt](auto& rwstates4topic_entry) {
              auto& [partition_id, rwstates4partition] = rwstates4topic_entry;
              auto rwstate_it = rwstates4partition.find(migration_id);
              if (rwstate_it != rwstates4partition.end()) {
                  if (rwstate_it->second.shard) {
                      stop_partition_work(
                        model::ntp(nt.ns, nt.tp, partition_id), *rwstate_it);
                  }
              }
          });
    }
    auto it = _active_topic_work_states.find(nt);
    if (it != _active_topic_work_states.end()) {
        it->second->rcn().request_abort();
        co_await it->second->future();
    }

    remove_from_topic_migration_map(nt, migration_id);
}

ss::future<> backend::reconcile_existing_topic(
  const model::topic_namespace& nt,
  topic_reconciliation_state& tstate,
  id migration,
  work_scope scope,
  model::revision_id revision_id,
  bool schedule_local_partition_work) {
    if (!schedule_local_partition_work && !_coordinator_term) {
        vlog(
          dm_log.debug,
          "not tracking topic {} transition towards state {} as part of "
          "migration {}",
          nt,
          scope.sought_state,
          migration);
        co_return;
    }
    vlog(
      dm_log.debug,
      "tracking topic {} transition towards state {} as part of "
      "migration {}, schedule_local_work={}, _coordinator_term={}",
      nt,
      scope.sought_state,
      migration,
      schedule_local_partition_work,
      _coordinator_term);
    auto now = model::timeout_clock::now();
    if (scope.partition_work_needed(nt)) {
        co_await ssx::async_for_each(
          get_topic_assignments(nt, migration),
          [this,
           nt,
           &tstate,
           migration,
           scope,
           revision_id,
           now,
           schedule_local_partition_work](const auto& assignment) {
              model::ntp ntp{nt.ns, nt.tp, assignment.id};
              auto nodes = assignment.replicas
                           | std::views::transform(
                             &model::broker_shard::node_id);
              if (_coordinator_term) {
                  auto [it, ins] = tstate.outstanding_partitions.emplace(
                    std::piecewise_construct,
                    std::tuple{assignment.id},
                    std::tuple{nodes.begin(), nodes.end()});
                  vassert(
                    ins,
                    "tried to repeatedly track partition {} "
                    "as part of migration {}",
                    ntp,
                    migration);
              }
              for (const auto& node_id : nodes) {
                  if (_coordinator_term) {
                      auto [it, ins] = _node_states[node_id].emplace(
                        ntp, migration);
                      vassert(
                        ins,
                        "tried to track partition {} on node {} as part of "
                        "migration {}, while it is already tracked as part "
                        "of migration {}",
                        ntp,
                        node_id,
                        migration,
                        it->migration);
                      _nodes_to_retry.insert_or_assign(node_id, now);
                  }
                  if (schedule_local_partition_work && _self == node_id) {
                      vlog(
                        dm_log.debug,
                        "tracking ntp {} transition towards state {} as "
                        "part of migration {}",
                        ntp,
                        scope.sought_state,
                        migration);
                      auto [it, _]
                        = _local_work_states[nt][assignment.id].try_emplace(
                          migration,
                          *scope.sought_state,
                          revision_id,
                          migrated_replica_status::waiting_for_rpc);
                      auto& rwstate = it->second;
                      if (rwstate.sought_state < *scope.sought_state) {
                          if (it->second.shard) {
                              stop_partition_work(ntp, *it);
                          }
                          rwstate = {
                            *scope.sought_state,
                            revision_id,
                            migrated_replica_status::waiting_for_rpc};
                      }
                      if (rwstate.sought_state == *scope.sought_state) {
                          switch (rwstate.status) {
                          case migrated_replica_status::
                            waiting_for_controller_update:
                              rwstate.status = migrated_replica_status::can_run;
                              rwstate.revision_id = revision_id;
                              [[fallthrough]];
                          case migrated_replica_status::can_run: {
                              auto new_shard = _shard_table.shard_for(ntp);
                              update_partition_shard(ntp, *it, new_shard);
                              break;
                          }
                          case migrated_replica_status::waiting_for_rpc:
                          case migrated_replica_status::done:
                              break;
                          }
                      }
                  }
              }
          });
    }
    if (_coordinator_term && scope.topic_work_needed) {
        tstate.topic_scoped_work_needed = true;
        _topic_work_to_retry.insert_or_assign({nt, migration}, now);
    }
}

result<backend::partition_consumer_group_map_t, errc>
backend::build_migration_group_map(const migration_metadata& metadata) const {
    partition_consumer_group_map_t ret;
    const auto& groups = std::visit(
      [](const auto& migration) -> const chunked_vector<consumer_group>& {
          return migration.groups;
      },
      metadata.migration);

    for (const auto& group : groups) {
        auto partition = _group_proxy->partition_for(group);
        if (!partition) {
            vlog(
              dm_log.warn,
              "cannot find partition for consumer group {} in migration {}",
              group,
              metadata.id);
            return errc::partition_not_exists;
        }
        auto [it, ins] = ret.try_emplace(*partition);
        it->second.push_back(group);
    }
    return ret;
}

ss::future<> backend::reconcile_migration(
  migration_reconciliation_state& mrstate, const migration_metadata& metadata) {
    vlog(
      dm_log.debug,
      "tracking migration {} transition towards state {}",
      metadata.id,
      mrstate.scope.sought_state);

    auto res = build_migration_group_map(metadata);
    /**
     * This is a fatal error as we cannot proceed with consumer group
     * migration without being able to build the partition -> groups map.
     */
    vassert(
      !res.has_error(),
      "failed to build migration group map while reconciling migration - "
      "error: {}",
      res.error());

    mrstate.partition_group_map.emplace(std::move(res.value()));

    co_await std::visit(
      [this, migration_id = metadata.id, &mrstate](
        const auto& migration) mutable {
          return ss::do_with(
            // poor man's `migration.topic_nts() | std::views::enumerate`
            std::views::transform(
              migration.topic_nts(),
              [index = -1](const auto& nt) mutable {
                  return std::forward_as_tuple(++index, nt);
              }),
            [this, migration_id, &mrstate](auto& enumerated_nts) {
                return ss::do_for_each(
                  enumerated_nts,
                  [this, migration_id, &mrstate](const auto& idx_nt) {
                      auto& [idx, nt] = idx_nt;
                      return reconcile_topic(migration_id, idx, nt, mrstate);
                  });
            });
      },
      metadata.migration);
}

ss::future<> backend::reconcile_topic(
  const id migration_id,
  size_t idx_in_migration,
  const model::topic_namespace& nt,
  migration_reconciliation_state& mrstate) {
    if (
      !mrstate.scope.topic_work_needed
      && !mrstate.scope.partition_work_needed(nt)) {
        co_return;
    }
    auto& tstate = mrstate.outstanding_topics[nt];
    tstate.idx_in_migration = idx_in_migration;
    _topic_migration_map[nt].insert(migration_id);
    co_return co_await reconcile_existing_topic(
      nt, tstate, migration_id, mrstate.scope, mrstate.revision_id, true);
}

std::optional<std::reference_wrapper<backend::partition_work_state_t>>
backend::get_replica_work_states(const model::ntp& ntp) {
    model::topic_namespace nt{ntp.ns, ntp.tp.topic};
    if (auto it = _local_work_states.find(nt); it != _local_work_states.end()) {
        auto& topic_work_state = it->second;
        auto rwstate_it = topic_work_state.find(ntp.tp.partition);
        if (rwstate_it != topic_work_state.end()) {
            return rwstate_it->second;
        }
    }
    return std::nullopt;
}

const inbound_topic& backend::get_inbound_topic(
  const model::topic_namespace_view& nt,
  const inbound_migration& im,
  id migration_id) const {
    auto it = _migration_states.find(migration_id);
    vassert(
      it != _migration_states.end(),
      "migration {} not found in migration states",
      migration_id);
    auto idx = it->second.outstanding_topics.at(nt).idx_in_migration;
    vlog(
      dm_log.trace,
      "get_inbound_topic: migration {}, topic {}, idx {}, topics: {}",
      migration_id,
      nt,
      idx,
      im.topics);
    return im.topics[idx];
}

inbound_partition_work_info backend::get_partition_work_info(
  const model::ntp& ntp, const inbound_migration& im, id migration_id) const {
    if (model::topic_namespace_view{ntp} == model::kafka_consumer_offsets_nt) {
        const auto& mrstate = _migration_states.find(migration_id)->second;
        return {
          .groups = mrstate.partition_group_map->at(ntp.tp.partition).copy()};
    }
    const auto& inbound_topic = get_inbound_topic(
      {ntp.ns, ntp.tp.topic}, im, migration_id);
    return {
      .source = inbound_topic.source_topic_name,
      .cloud_storage_location = inbound_topic.cloud_storage_location};
}

outbound_partition_work_info backend::get_partition_work_info(
  const model::ntp& ntp, const outbound_migration& om, id migration_id) const {
    outbound_partition_work_info ret = {.copy_to = om.copy_to};

    if (model::topic_namespace_view{ntp} == model::kafka_consumer_offsets_nt) {
        const auto& mrstate = _migration_states.find(migration_id)->second;
        ret.groups = mrstate.partition_group_map->at(ntp.tp.partition).copy();
    }

    return ret;
}

partition_work_info backend::get_partition_work_info(
  const model::ntp& ntp, const migration_metadata& metadata) const {
    return std::visit(
      [this, &ntp, &metadata](auto& migration) -> partition_work_info {
          return get_partition_work_info(ntp, migration, metadata.id);
      },
      metadata.migration);
}

inbound_topic_work_info backend::get_topic_work_info(
  const model::topic_namespace& nt,
  const inbound_migration& im,
  id migration_id) const {
    if (nt == model::kafka_consumer_offsets_nt) {
        return {};
    }
    const auto& inbound_topic = get_inbound_topic(nt, im, migration_id);

    return {
      .source = inbound_topic.alias
                  ? std::make_optional(inbound_topic.source_topic_name)
                  : std::nullopt,
      .cloud_storage_location = inbound_topic.cloud_storage_location};
}

outbound_topic_work_info backend::get_topic_work_info(
  const model::topic_namespace&, const outbound_migration& om, id) const {
    return {om.copy_to};
}

topic_work_info backend::get_topic_work_info(
  const model::topic_namespace& nt, const migration_metadata& metadata) const {
    return std::visit(
      [this, &nt, &metadata](auto& migration) -> topic_work_info {
          return get_topic_work_info(nt, migration, metadata.id);
      },
      metadata.migration);
}

void backend::start_partition_work(
  const model::ntp& ntp, const rwstate_entry& rwstate) {
    vlog(
      dm_log.trace,
      "while working on migration {}, asking worker on shard "
      "{} to advance ntp {} to state {}",
      rwstate.first,
      rwstate.second.shard,
      ntp,
      rwstate.second.sought_state);
    const auto maybe_migration = _table.get_migration(rwstate.first);
    if (!maybe_migration) {
        vlog(dm_log.trace, "migration {} gone, ignoring", rwstate.first);
        return;
    }

    partition_work work{
      .migration_id = rwstate.first,
      .sought_state = rwstate.second.sought_state,
      .revision_id = rwstate.second.get_revision_id(),
      .info = get_partition_work_info(ntp, maybe_migration->get())};

    ssx::spawn_with_gate(
      _gate, [this, &ntp, &rwstate, work = std::move(work)]() mutable {
          return _worker
            .invoke_on(
              *rwstate.second.shard,
              &worker::perform_partition_work,
              model::ntp{ntp},
              std::move(work))
            .then([this, ntp = ntp, rwstate](errc ec) mutable {
                if (ec == errc::success) {
                    vlog(
                      dm_log.trace,
                      "as part of migration {} worker on shard {} has "
                      "advanced ntp {} to state {}",
                      rwstate.first,
                      rwstate.second.shard,
                      ntp,
                      rwstate.second.sought_state);
                    on_partition_work_completed(
                      std::move(ntp),
                      rwstate.first,
                      rwstate.second.sought_state);
                } else {
                    // worker should always retry unless we instructed
                    // it to abort or it is shutting down
                    vlog(
                      dm_log.warn,
                      "while working on migration {} worker on shard "
                      "{} stopped trying to advance ntp {} to state {}",
                      rwstate.first,
                      rwstate.second.shard,
                      std::move(ntp),
                      rwstate.second.sought_state);
                }
            });
      });
}

void backend::stop_partition_work(
  model::ntp ntp, const rwstate_entry& rwstate) {
    vlog(
      dm_log.info,
      "while working on migration {}, asking worker on shard "
      "{} to stop trying to advance ntp {} to state {}",
      rwstate.first,
      rwstate.second.shard,
      ntp,
      rwstate.second.sought_state);
    ssx::spawn_with_gate(
      _gate,
      [this,
       &ntp,
       id = rwstate.first,
       shard = *rwstate.second.shard,
       state = rwstate.second.sought_state] {
          return _worker.invoke_on(
            shard, &worker::abort_partition_work, std::move(ntp), id, state);
      });
}

void backend::on_partition_work_completed(
  model::ntp&& ntp, id migration, state state) {
    auto maybe_rwstates = get_replica_work_states(ntp);
    if (!maybe_rwstates) {
        return;
    }
    auto rwstate_it = maybe_rwstates->get().find(migration);
    if (rwstate_it == maybe_rwstates->get().end()) {
        return;
    }
    auto& rwstate = rwstate_it->second;
    if (rwstate.sought_state == state) {
        rwstate.status = migrated_replica_status::done;
        rwstate.shard = std::nullopt;
    }
}

bool backend::has_local_replica(const model::ntp& ntp) {
    auto maybe_assignment = _topic_table.get_partition_assignment(ntp);
    if (!maybe_assignment) {
        return false;
    }
    for (const auto& replica : maybe_assignment->replicas) {
        if (_self == replica.node_id) {
            return true;
        }
    }
    return false;
}

backend::work_scope
backend::get_work_scope(const migration_metadata& metadata) {
    return std::visit(
      [&metadata](const auto& migration) {
          migration_direction_tag<std::decay_t<decltype(migration)>> tag;
          auto scope = get_work_scope(tag, metadata);
          if (migration.auto_advance && !scope.sought_state) {
              switch (metadata.state) {
              case state::planned:
                  scope.sought_state = state::preparing;
                  break;
              case state::prepared:
                  scope.sought_state = state::executing;
                  break;
              case state::executed:
                  scope.sought_state = state::cut_over;
                  break;
              case state::finished:
                  scope.sought_state = state::deleted;
                  break;
              case state::cancelled:
                  // An auto-advance migration can only be cancelled manually if
                  // it got stuck. Let's not deleted it automatically in case
                  // we'd like to investigate how it happened.
                  break;
              case state::deleted:
                  vunreachable("A migration cannot be in a deleted state");
              case state::preparing:
              case state::executing:
              case state::cut_over:
              case state::canceling:
                  vassert(
                    false,
                    "Work scope not found for migration {} transient state {}",
                    metadata.id,
                    metadata.state);
              }
          }
          return scope;
      },
      metadata.migration);
}

backend::work_scope backend::get_work_scope(
  migration_direction_tag<inbound_migration>,
  const migration_metadata& metadata) {
    switch (metadata.state) {
    case state::preparing:
        return {
          .sought_state = state::prepared,
          .data_partition_work_needed = false,
          .co_partition_work_needed = false,
          .topic_work_needed = true,
          .needs_entity_state_update = false,
          .wait_for_partition_work_to_finish = false,
        };
    case state::executing:
        return {
          .sought_state = state::executed,
          .data_partition_work_needed = false,
          .co_partition_work_needed = false,
          .topic_work_needed = true,
          .needs_entity_state_update = true,
          .wait_for_partition_work_to_finish = false,
        };
    case state::cut_over:
        return {
          .sought_state = state::finished,
          .data_partition_work_needed = false,
          .co_partition_work_needed = false,
          .topic_work_needed = true,
          .needs_entity_state_update = false,
          .wait_for_partition_work_to_finish = false,
        };
    case state::canceling:
        return {
          .sought_state = state::cancelled,
          .data_partition_work_needed = false,
          .co_partition_work_needed = false,
          .topic_work_needed = true,
          .needs_entity_state_update = false,
          .wait_for_partition_work_to_finish = false,
        };
    default:
        return {
          .sought_state = {},
          .data_partition_work_needed = false,
          .co_partition_work_needed = false,
          .topic_work_needed = false,
          .needs_entity_state_update = false,
          .wait_for_partition_work_to_finish = false,
        };
    };
}

backend::work_scope backend::get_work_scope(
  migration_direction_tag<outbound_migration>,
  const migration_metadata& metadata) {
    switch (metadata.state) {
    case state::preparing:
        return {
          .sought_state = state::prepared,
          .data_partition_work_needed = true,
          .co_partition_work_needed = false,
          .topic_work_needed = false,
          .needs_entity_state_update = false,
          .wait_for_partition_work_to_finish = false,
        };
    case state::executing:
        return {
          .sought_state = state::executed,
          .data_partition_work_needed = true,
          .co_partition_work_needed = true,
          .topic_work_needed = true,
          .needs_entity_state_update = false,
          .wait_for_partition_work_to_finish = false};
    case state::cut_over:
        return {
          .sought_state = state::finished,
          .data_partition_work_needed = false,
          .co_partition_work_needed = true,
          .topic_work_needed = true,
          .needs_entity_state_update = false,
          .wait_for_partition_work_to_finish = true,
        };
    case state::canceling:
        return {
          .sought_state = state::cancelled,
          .data_partition_work_needed = true,
          .co_partition_work_needed = true,
          .topic_work_needed = false,
          .needs_entity_state_update = false,
          .wait_for_partition_work_to_finish = false,
        };
    default:
        return {
          .sought_state = {},
          .data_partition_work_needed = false,
          .co_partition_work_needed = false,
          .topic_work_needed = false,
          .needs_entity_state_update = false,
          .wait_for_partition_work_to_finish = false,
        };
    };
}

void backend::topic_reconciliation_state::clear() {
    outstanding_partitions.clear();
    topic_scoped_work_needed = false;
    topic_scoped_work_done = false;
}

backend::topic_scoped_work_state::topic_scoped_work_state()
  : _as()
  , _rcn(
      _as,
      ss::lowres_clock::now() + retry_chain_node::milliseconds_uint16_t::max(),
      2s) {}

backend::topic_scoped_work_state::~topic_scoped_work_state() {
    vassert(_promise.available(), "Cannot drop state for a running work");
}

retry_chain_node& backend::topic_scoped_work_state::rcn() { return _rcn; }

void backend::topic_scoped_work_state::set_value(errc ec) {
    _promise.set_value(ec);
}

ss::future<errc> backend::topic_scoped_work_state::future() {
    return _promise.get_shared_future();
}

std::ostream&
operator<<(std::ostream& os, const backend::replica_work_state& rws) {
    fmt::print(
      os,
      "{{sought_state: {}, shard: {}, status: {}}}",
      rws.sought_state,
      rws.shard,
      rws.status);
    return os;
}

chunked_vector<partition_assignment>
backend::get_topic_assignments(const model::topic_namespace& nt, const id id) {
    auto maybe_assignments = _topic_table.get_topic_assignments(nt);
    if (!maybe_assignments) {
        // A lagging non-leader may encounter a outbound migration that
        // has already been completed in the cluster, and its topics are gone.
        // If it is a controller or even a leader it will step down soon, so we
        // are not worried we have no data.
        // TODO: In theory, there is a race condition possible here if a new
        // topic with the same name was created shortly after the original one
        // was migrated away. We probably should remember topic initial
        // revisions when creating a migration.
        vlogl(
          dm_log,
          _raft0_leader_term ? ss::log_level::error : ss::log_level::warn,
          "topic {} not found in topic table for migration {}",
          nt,
          id);
        return {};
    }
    auto assignments = std::views::values(std::move(*maybe_assignments));

    if (nt == model::kafka_consumer_offsets_nt) {
        const auto& mrstate = _migration_states.find(id)->second;
        vassert(
          mrstate.partition_group_map,
          "group map not found for migration {}",
          id);

        auto filtered = std::move(assignments)
                        | std::views::filter(
                          [&group_map = *mrstate.partition_group_map](
                            const auto& assignment) {
                              return group_map.contains(assignment.id);
                          });
        return std::move(filtered) | std::views::as_rvalue
               | std::ranges::to<chunked_vector<partition_assignment>>();
    } else {
        return std::move(assignments) | std::views::as_rvalue
               | std::ranges::to<chunked_vector<partition_assignment>>();
    }
}

} // namespace cluster::data_migrations
