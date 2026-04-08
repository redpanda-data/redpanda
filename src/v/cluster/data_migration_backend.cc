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
#include "cluster/data_migration_backend.h"

#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/topic_manifest_downloader.h"
#include "cloud_storage/topic_mount_handler.h"
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

bool backend::ntp_migration::operator==(const ntp_migration& other) const {
    return ntp == other.ntp && migration == other.migration;
}

bool backend::topic_namespace_migration::operator==(
  const topic_namespace_migration& other) const {
    return nt == other.nt && migration == other.migration;
}

backend::backend(
  migrations_table& table,
  frontend& frontend,
  router& router,
  ss::sharded<worker>& worker,
  partition_leaders_table& leaders_table,
  topics_frontend& topics_frontend,
  topic_table& topic_table,
  shard_table& shard_table,
  ss::shared_ptr<group_proxy> group_proxy,
  std::optional<std::reference_wrapper<cloud_storage::remote>>
    cloud_storage_api,
  std::optional<std::reference_wrapper<cloud_storage::topic_mount_handler>>
    topic_mount_handler,
  ss::abort_source& as)
  : _self(*config::node().node_id())
  , _table(table)
  , _frontend(frontend)
  , _worker(worker)
  , _router(router)
  , _leaders_table(leaders_table)
  , _topics_frontend(topics_frontend)
  , _topic_table(topic_table)
  , _shard_table(shard_table)
  , _group_proxy(std::move(group_proxy))
  , _cloud_storage_api(cloud_storage_api)
  , _topic_mount_handler(topic_mount_handler)
  , _as(as) {}

ss::future<> backend::start() {
    vlog(dm_log.info, "backend starting");
    vassert(
      ss::this_shard_id() == data_migrations_shard, "Called on wrong shard");

    auto leader_term = _leaders_table.get_leader_term(model::controller_ntp);
    if (leader_term && leader_term->leader == _self) {
        _raft0_leader_term = _coordinator_term = leader_term->term;
    }

    _plt_raft0_leadership_notification_id
      = _leaders_table.register_leadership_change_notification(
        model::controller_ntp,
        [this](
          const model::ntp&,
          model::term_id term,
          model::node_id leader_node_id) {
            std::optional<model::term_id> new_term_if_leader
              = (leader_node_id == _self) ? std::make_optional(term)
                                          : std::nullopt;
            vlog(
              dm_log.trace,
              "_raft0_leader_term={}, new_term_if_leader={}",
              _raft0_leader_term,
              new_term_if_leader);

            if (!new_term_if_leader && !_raft0_leader_term) {
                // remaining a non-leader
                return;
            }
            if (
              new_term_if_leader && _raft0_leader_term
              && ((*new_term_if_leader)() - (*_raft0_leader_term)() == 1)) {
                // remaining a leader, no other leaders between our terms
                _raft0_leader_term = _coordinator_term = new_term_if_leader;
                return;
            }
            _raft0_leader_term = new_term_if_leader;
            ssx::spawn_with_gate(
              _gate, [this]() { return handle_raft0_leadership_update(); });
        });

    _topic_table_notification_id = _topic_table.register_ntp_delta_notification(
      [this](topic_table::ntp_delta_range_t deltas) {
          _unprocessed_deltas.reserve(
            _unprocessed_deltas.size() + deltas.size());
          for (const auto& delta : deltas) {
              _unprocessed_deltas.push_back(delta);
          }
          wakeup();
      });

    _shard_notification_id = _shard_table.register_notification(
      [this](
        const model::ntp& ntp,
        raft::group_id g,
        std::optional<ss::shard_id> shard) {
          handle_shard_update(ntp, g, shard);
      });

    if (_cloud_storage_api) {
        _table_notification_id = _table.register_notification([this](id id) {
            ssx::spawn_with_gate(
              _gate, [this, id]() { return handle_migration_update(id); });
        });

        // process those that were already there when we subscribed
        for (auto id : _table.get_migrations()) {
            co_await handle_migration_update(id);
        }

        ssx::repeat_until_gate_closed_or_aborted(
          _gate, _as, [this]() { return loop_once(); });

        vlog(dm_log.info, "backend started");
    } else {
        vlog(
          dm_log.info,
          "backend not started as cloud_storage_api is not available");
    }
}

ss::future<> backend::stop() {
    vlog(dm_log.info, "backend stopping");
    _mutex.broken();
    co_await abort_all_topic_work();
    _sem.broken();
    _timer.cancel();
    _shard_table.unregister_delta_notification(_shard_notification_id);
    _topic_table.unregister_ntp_delta_notification(
      _topic_table_notification_id);
    _leaders_table.unregister_leadership_change_notification(
      model::controller_ntp, _plt_raft0_leadership_notification_id);
    if (_cloud_storage_api) {
        _table.unregister_notification(_table_notification_id);
    }
    co_await _worker.invoke_on_all(&worker::stop);
    co_await _gate.close();
    vlog(dm_log.info, "backend stopped");
}

ss::future<result<entities_status, errc>>
backend::get_entities_status(id migration_id) {
    // for safe async iteration
    try {
        auto units = co_await _mutex.get_units(_as);

        if (!_coordinator_term) {
            vlog(dm_log.warn, "called on non-coordinator node {}", _self);
            co_return errc::not_leader_controller;
        }

        const auto& maybe_meta = _table.get_migration(migration_id);
        if (!maybe_meta) {
            vlog(dm_log.trace, "migration {} gone, ignoring", migration_id);
            co_return errc::data_migration_not_exists;
        }
        const auto& meta = maybe_meta->get();

        if (!std::holds_alternative<outbound_migration>(meta.migration)) {
            vlog(dm_log.warn, "migration {} is not outbound", migration_id);
            co_return errc::data_migration_not_exists;
        }

        if (meta.state != state::executed) {
            vlog(
              dm_log.warn,
              "get_entities_status: migration {} is not in executed "
              "state, current state: {}",
              migration_id,
              meta.state);
            co_return errc::invalid_data_migration_state;
        }

        result<entities_status, errc> ret{entities_status{}};

        auto holder = _gate.hold();
        auto group_map_result = build_migration_group_map(meta);

        /**
         * If we failed to build the group map, it might be because the
         * consumer groups topic does not exist yet. Try to create it and
         * build the map again.
         */
        if (group_map_result.has_error()) {
            co_await _group_proxy->assure_topic_exists(
              model::time_from_now(10s));
            group_map_result = build_migration_group_map(meta);
        }

        if (group_map_result.has_error()) {
            vlog(
              dm_log.warn,
              "get_entities_status: failed to build group map for migration "
              "{}: {}",
              migration_id,
              group_map_result.error());
            co_return errc::leadership_changed;
        }
        chunked_vector<partition_consumer_group_map_t::value_type>
          groups_by_partition(
            std::from_range, group_map_result.value() | std::views::as_rvalue);
        vlog(
          dm_log.debug,
          "get_entities_status: migration {}, groups by partition: {}",
          migration_id,
          groups_by_partition.size());
        errc last_errc = errc::success;
        co_await ss::parallel_for_each(
          std::move(groups_by_partition),
          [this, &ret, &last_errc](auto&& pair) {
              // TODO: retry per-partition
              auto&& [pid, groups] = pair;
              return _router
                .get_group_offsets(
                  get_group_offsets_request(pid, std::move(groups)))
                .then([&ret, pid, &last_errc](get_group_offsets_reply&& reply) {
                    if (!ret.has_value()) {
                        // broken by one of the previous results
                        return;
                    }
                    if (reply.ec != errc::success) {
                        vlog(
                          dm_log.warn,
                          "get_group_offsets for partition {} failed: {}",
                          pid,
                          reply.ec);
                        last_errc = reply.ec;
                    } else {
                        std::ranges::move(
                          std::move(reply.group_offsets),
                          std::back_inserter(ret.assume_value().groups));
                    }
                });
          });
        if (last_errc != errc::success) {
            co_return last_errc;
        }

        co_return ret;
    } catch (const ss::abort_requested_exception&) {
        co_return errc::shutting_down;
    }
}

ss::future<errc>
backend::set_entities_status(id migration_id, entities_status status) {
    // for safe async iteration
    try {
        auto units = co_await _mutex.get_units(_as);
        vlog(
          dm_log.trace,
          "set_entities_status for {} with: {}",
          migration_id,
          status);
        if (!_coordinator_term) {
            vlog(dm_log.warn, "called on non-coordinator node {}", _self);
            co_return errc::not_leader_controller;
        }

        const auto& maybe_meta = _table.get_migration(migration_id);
        if (!maybe_meta) {
            vlog(dm_log.trace, "migration {} gone, ignoring", migration_id);
            co_return errc::data_migration_not_exists;
        }
        const auto& meta = maybe_meta->get();

        if (!std::holds_alternative<inbound_migration>(meta.migration)) {
            vlog(dm_log.warn, "migration {} is not inbound", migration_id);
            co_return errc::data_migration_not_exists;
        }
        const auto& migration = std::get<inbound_migration>(meta.migration);

        switch (meta.state) {
        case state::executing: {
            auto migration_it = _migration_states.find(migration_id);
            if (migration_it == _migration_states.end()) {
                vlog(
                  dm_log.warn,
                  "reconciliation state for migration {} not found",
                  migration_id);
                // assume we did not start to reconcile yet
                co_return errc::invalid_data_migration_state;
            }
            auto& mrstate = migration_it->second;
            if (mrstate.scope.sought_state != state::executed) {
                // reconciliation is ahead
                co_return errc::success;
            }

            vassert(
              mrstate.partition_group_map,
              "partition group map must be filled");

            // valid, as guarded by mutex
            auto groups_topic_rstate_it = mrstate.outstanding_topics.find(
              model::kafka_consumer_offsets_nt);
            bool group_topic_outstanding = groups_topic_rstate_it
                                           != mrstate.outstanding_topics.end();
            if (!group_topic_outstanding) {
                vlog(
                  dm_log.debug,
                  "kafka consumer offsets topic does not require approval"
                  "in migration {}, probably already done",
                  migration_id);
            } else {
                // reverse map is more to make sure we have data for exactly
                // required groups rather than for lookup
                chunked_hash_map<kafka::group_id, model::partition_id> rev_map;
                rev_map.reserve(migration.groups.size());
                for (const auto& [pid, groups] : *mrstate.partition_group_map) {
                    co_await ssx::async_for_each(
                      groups, [&rev_map, pid](const kafka::group_id& group) {
                          rev_map[group] = pid;
                      });
                }

                chunked_hash_map<model::partition_id, group_offsets_snapshot>
                  requests;
                requests.reserve(mrstate.partition_group_map->size());
                for (auto p : *mrstate.partition_group_map | std::views::keys) {
                    requests[p].offsets_topic_pid = p;
                };
                co_await ssx::async_for_each(
                  std::move(status.groups),
                  [&rev_map, &requests, migration_id](group_offsets& group) {
                      kafka::group_id gid{group.group_id};
                      if (
                        auto it = rev_map.find(gid);
                        likely(it != rev_map.end())) {
                          auto pid = it->second;
                          requests[pid].groups.push_back(std::move(group));
                      } else {
                          vlog(
                            dm_log.warn,
                            "set_entities_status: group {} is not part of "
                            "migration {}",
                            group.group_id,
                            migration_id);
                      }
                  });

                errc last_error = errc::success;
                co_await ss::parallel_for_each(
                  *mrstate.partition_group_map,
                  [&requests, this, &last_error](const auto& pair) {
                      auto& [pid, groups] = pair;
                      auto& request = requests.at(pid);
                      if (request.groups.empty()) {
                          vlog(
                            dm_log.debug,
                            "set_entities_status: no groups for partition "
                            "{}",
                            pid);
                          return ss::now();
                      }
                      return _router
                        .set_group_offsets(
                          set_group_offsets_request{std::move(request)})
                        .then([&last_error](set_group_offsets_reply&& reply) {
                            if (reply.ec != cluster::errc::success) {
                                vlog(
                                  dm_log.warn,
                                  "set_group_offsets failed: {}",
                                  reply.ec);
                                last_error = reply.ec;
                            }
                        });
                  });
                if (last_error != errc::success) {
                    co_return last_error;
                }

                mrstate.entities_ready = true;
                schedule_topic_work(
                  {model::kafka_consumer_offsets_nt, migration_id});
            }

            // 3) persist all-approved state
            units.return_all();
            wakeup();
            vlog(
              dm_log.debug, "set_entities_status: migration={}", migration_id);
            co_return errc::success;
        }
        case state::executed:
            // already ahead
            co_return errc::success;
        default:
            vlog(
              dm_log.warn,
              "get_entities_status: migration {} is not in executing or "
              "executed state, current state: {}",
              migration_id,
              meta.state);
            co_return errc::invalid_data_migration_state;
        }
    } catch (const ss::abort_requested_exception&) {
        co_return errc::shutting_down;
    }
}

ss::future<> backend::loop_once() {
    try {
        co_await _sem.wait(_as);
        _sem.consume(_sem.available_units());
        {
            auto units = co_await _mutex.get_units(_as);
            co_await work_once();
        }
    } catch (...) {
        const auto& e = std::current_exception();
        vlogl(
          dm_log,
          ssx::is_shutdown_exception(e) ? ss::log_level::trace
                                        : ss::log_level::warn,
          "Exception in migration backend main loop: {}",
          e);
    }
}

void backend::schedule_topic_work_if_partitions_ready(
  const model::topic_namespace& tp_ns, mrstate_cit_t rs_it) {
    const auto& outstanding_topics = rs_it->second.outstanding_topics;
    auto it = outstanding_topics.find(tp_ns);
    if (it == outstanding_topics.end()) {
        // topic already gone, it didn't need to wait for partition work
        return;
    }

    if (it->second.all_partitions_ready()) {
        schedule_topic_work({tp_ns, rs_it->first});
    }
}

ss::future<> backend::work_once() {
    vlog(dm_log.info, "begin backend work cycle");
    // process pending deltas
    auto unprocessed_deltas = std::move(_unprocessed_deltas);
    for (auto&& delta : unprocessed_deltas) {
        co_await process_delta(std::move(delta));
    }

    // process RPC responses
    auto rpc_responses = std::move(_rpc_responses);
    for (const auto& [node_id, response] : rpc_responses) {
        co_await ssx::async_for_each(
          response.actual_states, [this](const auto& ntp_resp) {
              if (auto rs_it = get_rstate(ntp_resp.migration, ntp_resp.state)) {
                  mark_migration_step_done_for_ntp(*rs_it, ntp_resp.ntp);
                  schedule_topic_work_if_partitions_ready(
                    {ntp_resp.ntp.ns, ntp_resp.ntp.tp.topic}, *rs_it);
                  // advance if done as a last step as it may invalidate the
                  // reconciliation state iterator.
                  to_advance_if_done(*rs_it);
              }
          });
    }

    // process topic work results
    auto topic_work_results = std::move(_topic_work_results);

    chunked_vector<topic_namespace_migration> retriable_topic_work;
    co_await ssx::async_for_each(
      topic_work_results, [this, &retriable_topic_work](auto& result) {
          if (auto rs_it = get_rstate(result.migration, result.sought_state)) {
              switch (result.ec) {
              case errc::success:
                  mark_migration_step_done_for_nt(*rs_it, result.nt);
                  to_advance_if_done(*rs_it);
                  break;
              case errc::shutting_down:
                  break;
              default:
                  // any other errors deemed retryable
                  vlog(
                    dm_log.info,
                    "as part of migration {}, topic work for moving nt {} to "
                    "state {} returned {}, retrying",
                    result.migration,
                    result.nt,
                    result.sought_state,
                    result.ec);
                  retriable_topic_work.push_back(
                    {std::move(result.nt), result.migration});
              }
          }
      });

    auto next_tick = model::timeout_clock::time_point::max();

    // prepare RPC and topic work requests
    auto now = model::timeout_clock::now();
    chunked_vector<model::node_id> to_send_rpc;
    for (const auto& [node_id, deadline] : _nodes_to_retry) {
        if (deadline <= now) {
            to_send_rpc.push_back(node_id);
        } else {
            next_tick = std::min(deadline, next_tick);
        }
    }
    chunked_vector<topic_namespace_migration> to_schedule_topic_work;
    co_await ssx::async_for_each(
      _topic_work_to_retry,
      [&to_schedule_topic_work, &next_tick, now](const auto& entry) {
          const auto& [tnm, deadline] = entry;
          if (deadline <= now) {
              to_schedule_topic_work.push_back(tnm);
          } else {
              next_tick = std::min(deadline, next_tick);
          }
      });
    _topic_work_to_retry.clear();

    // defer RPC retries and topic work
    // todo: configure timeout
    auto new_deadline = now + 500ms;
    for (const auto& node_id : rpc_responses | std::views::keys) {
        if (_node_states.contains(node_id)) {
            _nodes_to_retry.try_emplace(node_id, new_deadline);
            next_tick = std::min(next_tick, new_deadline);
        }
    }
    co_await ssx::async_for_each(
      retriable_topic_work,
      [this, &next_tick, new_deadline](topic_namespace_migration& tnm) {
          if (!_migration_states.contains(tnm.migration)) {
              return;
          }
          auto& mrstate = _migration_states.find(tnm.migration)->second;
          auto& tstate = mrstate.outstanding_topics[tnm.nt];
          if (
            tstate.topic_scoped_work_needed && !tstate.topic_scoped_work_done) {
              _topic_work_to_retry.try_emplace(std::move(tnm), new_deadline);
              next_tick = std::min(next_tick, new_deadline);
          }
      });

    // schedule fibers
    for (auto node_id : to_send_rpc) {
        _nodes_to_retry.erase(node_id);
        co_await send_rpc(node_id);
    }
    co_await ssx::async_for_each(
      to_schedule_topic_work, [this](topic_namespace_migration& tnm) {
          vlog(dm_log.debug, "rescheduling topic {} work", tnm.nt);
          return schedule_topic_work(std::move(tnm));
      });
    spawn_advances();
    if (next_tick == model::timeout_clock::time_point::max()) {
        _timer.cancel();
    } else {
        _timer.rearm(next_tick);
    }
    vlog(dm_log.info, "end backend work cycle");
}

void backend::wakeup() { _sem.signal(1 - _sem.available_units()); }

std::optional<backend::mrstate_it_t>
backend::get_rstate(id migration, state expected_sought_state) {
    auto rs_it = _migration_states.find(migration);
    if (rs_it == _migration_states.end()) {
        // migration gone, ignore
        return std::nullopt;
    }
    migration_reconciliation_state& rs = rs_it->second;
    if (rs.scope.sought_state > expected_sought_state) {
        // migration advanced since then, ignore
        return std::nullopt;
    }
    return rs_it;
}

void backend::mark_migration_step_done_for_ntp(
  mrstate_it_t rs_it, const model::ntp& ntp) {
    auto& rs_topics = rs_it->second.outstanding_topics;
    auto rs_topic_it = rs_topics.find({ntp.ns, ntp.tp.topic});
    if (rs_topic_it != rs_topics.end()) {
        auto& tstate = rs_topic_it->second;
        auto& rs_parts = tstate.outstanding_partitions;
        auto rs_part_it = rs_parts.find(ntp.tp.partition);
        if (rs_part_it != rs_parts.end()) {
            for (const auto& affected_node_id : rs_part_it->second) {
                auto nstate_it = _node_states.find(affected_node_id);
                vassert(
                  nstate_it != _node_states.end(),
                  "node state must exist for node {}",
                  affected_node_id);
                nstate_it->second.erase({ntp, rs_it->first});
                if (nstate_it->second.empty()) {
                    _node_states.erase(nstate_it);
                    _nodes_to_retry.erase(affected_node_id);
                }
            }
            rs_parts.erase(rs_part_it);
            erase_tstate_if_done(rs_it, rs_topic_it);
        }
    }
}

void backend::mark_migration_step_done_for_nt(
  mrstate_it_t rs_it, const model::topic_namespace& nt) {
    auto& rs_topics = rs_it->second.outstanding_topics;
    auto rs_topic_it = rs_topics.find(nt);
    if (rs_topic_it != rs_topics.end()) {
        auto& tstate = rs_topic_it->second;
        tstate.topic_scoped_work_done = true;
        erase_tstate_if_done(rs_it, rs_topic_it);
    }
}

void backend::remove_from_topic_migration_map(
  const model::topic_namespace& nt, id migration) {
    auto tmm_it = _topic_migration_map.find(nt);
    vassert(
      tmm_it != _topic_migration_map.end(),
      "topic migration map must have entry for nt {}",
      nt);
    tmm_it->second.erase(migration);
    if (tmm_it->second.empty()) {
        _topic_migration_map.erase(tmm_it);
    }
}

void backend::erase_tstate_if_done(
  mrstate_it_t rs_it, topic_map_t::iterator it) {
    auto& tstate = it->second;
    bool done
      = tstate.outstanding_partitions.empty()
        && (!tstate.topic_scoped_work_needed || tstate.topic_scoped_work_done);
    if (done) {
        remove_from_topic_migration_map(it->first, rs_it->first);
        rs_it->second.outstanding_topics.erase(it);
    }
}

ss::future<> backend::send_rpc(model::node_id node_id) {
    check_ntp_states_request req;
    co_await ssx::async_for_each(
      _node_states[node_id], [this, &req](const ntp_migration& ntpm) {
          req.sought_states.push_back(
            {.ntp = ntpm.ntp,
             .migration = ntpm.migration,
             .state = *_migration_states.find(ntpm.migration)
                         ->second.scope.sought_state});
      });

    ssx::spawn_with_gate(
      _gate, [this, node_id, req = std::move(req)]() mutable {
          vlog(dm_log.debug, "sending RPC to node {}: {}", node_id, req);
          ss::future<check_ntp_states_reply> reply
            = (_self == node_id) ? check_ntp_states_locally(std::move(req))
                                 : _frontend.check_ntp_states_on_foreign_node(
                                     node_id, std::move(req));
          return reply.then([node_id, this](check_ntp_states_reply&& reply) {
              vlog(
                dm_log.debug, "got RPC response from {}: {}", node_id, reply);
              _rpc_responses[node_id] = std::move(reply);
              return wakeup();
          });
      });
}

void backend::schedule_topic_work(topic_namespace_migration tnm) {
    auto& mrstate = _migration_states.at(tnm.migration);
    auto& tstate = mrstate.outstanding_topics.at(tnm.nt);
    vlog(
      dm_log.trace,
      "maybe scheduling topic work migration_id={} nt={}, "
      "tstate.topic_work_needed={}, tstate.topic_scoped_work_done={}, "
      "entities_ready={}",
      tnm.migration,
      tnm.nt,
      tstate.topic_scoped_work_needed,
      tstate.topic_scoped_work_done,
      mrstate.entities_ready);
    if (!tstate.topic_scoped_work_needed || tstate.topic_scoped_work_done) {
        return;
    }

    if (tnm.nt == model::kafka_consumer_offsets_nt && !mrstate.entities_ready) {
        // groups topic work must be scheduled only after entities are ready
        return;
    }
    if (
      mrstate.scope.wait_for_partition_work_to_finish
      && !tstate.all_partitions_ready()) {
        // waiting for partitions to finish first
        vlog(
          dm_log.trace, "waiting for partitions to finish for nt={}", tnm.nt);
        return;
    }
    const auto maybe_migration = _table.get_migration(tnm.migration);
    if (!maybe_migration) {
        vlog(dm_log.trace, "migration {} gone, ignoring", tnm.migration);
        return;
    }
    topic_work tw{
      .migration_id = tnm.migration,
      .sought_state = *mrstate.scope.sought_state,
      .info = get_topic_work_info(tnm.nt, maybe_migration->get())};

    ssx::spawn_with_gate(
      _gate, [this, nt = std::move(tnm.nt), tw = std::move(tw)]() mutable {
          return do_topic_work(std::move(nt), std::move(tw))
            .then([this](topic_work_result&& twr) {
                _topic_work_results.push_back(std::move(twr));
                return wakeup();
            });
      });
}

ss::future<backend::topic_work_result>
backend::do_topic_work(model::topic_namespace nt, topic_work tw) noexcept {
    auto tsws = ss::make_lw_shared<topic_scoped_work_state>();
    while (true) {
        auto [it, ins] = _active_topic_work_states.try_emplace(nt);
        if (ins) {
            it->second = tsws;
            break;
        }
        // delete existing work's entry
        auto [_, old_tsws] = _active_topic_work_states.extract(it);
        old_tsws->rcn().request_abort();
        // wait for existing work to complete
        vlog(
          dm_log.info, "waiting for older topic work on nt={} to complete", nt);
        auto old_ec = co_await old_tsws->future();
        vlog(
          dm_log.info,
          "older topic work on nt={} completed with errc={}",
          nt,
          old_ec);
        // carry over cached manifest, if any
        if (old_tsws->cached_topic_manifest().has_value()) {
            it->second->cache_topic_manifest(
              std::move(*old_tsws).release_cached_topic_manifest().value());
        }
    }

    errc ec;
    try {
        vlog(dm_log.debug, "doing topic work {} on nt={}", tw, nt);
        ec = co_await std::visit(
          [this, &nt, &tw, tsws](auto& info) mutable {
              return do_topic_work(nt, tw.sought_state, info, std::move(tsws));
          },
          tw.info);
        vlog(
          dm_log.debug,
          "completed topic work {} on nt={}, result={}",
          tw,
          nt,
          ec);
    } catch (...) {
        vlog(
          dm_log.warn,
          "exception occurred during topic work {} on nt={}",
          tw,
          nt,
          std::current_exception());
        ec = errc::topic_operation_error;
    }

    auto it = _active_topic_work_states.find(nt);
    if (it == _active_topic_work_states.end()) {
        vlog(dm_log.info, "topic work state for nt {} disappeared", nt);
    } else if (it->second != tsws) {
        vlog(
          dm_log.info,
          "topic work state for nt {} was superseded by another task",
          nt);
    } else {
        // only remove relevant entry
        _active_topic_work_states.erase(it);
    }
    // but we have a handle to the state in any case
    tsws->set_value(ec);

    co_return topic_work_result{
      .nt = std::move(nt),
      .migration = tw.migration_id,
      .sought_state = tw.sought_state,
      .ec = ec,
    };
}

ss::future<errc> backend::do_topic_work(
  const model::topic_namespace& nt,
  state sought_state,
  const inbound_topic_work_info& itwi,
  tsws_lwptr_t tsws) {
    auto& rcn = tsws->rcn();
    // this switch should be in accordance to the logic in get_work_scope
    if (nt == model::kafka_consumer_offsets_nt) {
        co_return errc::success;
    }
    switch (sought_state) {
    case state::prepared: {
        auto result = co_await maybe_download_topic_manifest(
          nt, itwi.source, itwi.cloud_storage_location, tsws);
        if (result.has_error()) {
            co_return result.error();
        }

        co_return co_await retry_loop(rcn, [this, &nt, &rcn, &result] {
            return prepare_mount_topic(nt, result.value(), rcn);
        });
    }
    case state::executed: {
        auto result = co_await maybe_download_topic_manifest(
          nt, itwi.source, itwi.cloud_storage_location, tsws);
        if (result.has_error()) {
            co_return result.error();
        }

        co_return co_await retry_loop(rcn, [this, &nt, &rcn, &result] {
            return confirm_mount_topic(nt, result.value(), rcn);
        });
    }
    case state::finished: {
        auto result = co_await maybe_download_topic_manifest(
          nt, itwi.source, itwi.cloud_storage_location, tsws);
        if (result.has_error()) {
            co_return result.error();
        }

        co_return co_await retry_loop(rcn, [this, &nt, &itwi, &rcn, &result] {
            return create_topic(nt, itwi.source, result.value(), rcn);
        });
    }
    case state::cancelled: {
        auto result = co_await maybe_download_topic_manifest(
          nt, itwi.source, itwi.cloud_storage_location, tsws);
        if (result.has_error()) {
            if (result.error() == errc::topic_not_exists) {
                // topic manifest missing, nothing to unmount
                vlog(
                  dm_log.info,
                  "topic {} manifest missing, nothing to unmount",
                  nt);
                co_return errc::success;
            }

            vlog(dm_log.warn, "topic {} manifest download failed", nt);
            co_return errc::topic_operation_error;
        }
        auto& manifest = result.value().get();
        auto& cfg = manifest.get_topic_config();
        if (!cfg) {
            vlog(
              dm_log.warn,
              "topic {} configuration missing in manifest, cannot unmount",
              nt);
            co_return errc::topic_operation_error;
        }
        // attempt to unmount first
        auto unmount_res = co_await unmount_not_existing_topic(
          nt, manifest, rcn);
        if (unmount_res != errc::success) {
            vlog(
              dm_log.warn, "failed to unmount topic {}: {}", nt, unmount_res);
        }
        // drop topic in any case
        auto drop_res = co_await delete_topic(nt, rcn);
        if (drop_res != errc::success) {
            vlog(dm_log.warn, "failed to drop topic {}: {}", nt, drop_res);
            co_return drop_res;
        }
        co_return errc::success;
    }
    default:
        vassert(
          false,
          "unknown topic work requested when transitioning inbound migration "
          "state to {}",
          sought_state);
    }
}

ss::future<errc> backend::do_topic_work(
  const model::topic_namespace& nt,
  state sought_state,
  const outbound_topic_work_info&,
  tsws_lwptr_t tsws) {
    auto& rcn = tsws->rcn();
    // this switch should be in accordance to the logic in get_work_scope
    switch (sought_state) {
    case state::executed: {
        if (nt == model::kafka_consumer_offsets_nt) {
            co_return errc::success;
        }
        co_return co_await unmount_topic(nt, rcn);
    }
    case state::finished: {
        if (nt == model::kafka_consumer_offsets_nt) {
            co_return errc::success;
        }
        // delete
        co_return co_await delete_topic(nt, rcn);
    }
    default:
        vassert(
          false,
          "unknown topic work requested when transitioning outbound migration "
          "state to {}",
          sought_state);
    }
}

ss::future<> backend::abort_all_topic_work() {
    for (auto& [nt, tsws] : _active_topic_work_states) {
        tsws->rcn().request_abort();
    }
    while (!_active_topic_work_states.empty()) {
        vlog(
          dm_log.info,
          "waiting for {} topic work states to complete",
          _active_topic_work_states.size());

        co_await _active_topic_work_states.begin()->second->future();

        vlog(dm_log.info, "one topic work state completed");
    }
}
ss::future<
  result<std::reference_wrapper<const cloud_storage::topic_manifest>, errc>>
backend::maybe_download_topic_manifest(
  const model::topic_namespace& nt,
  const std::optional<model::topic_namespace>& original_nt,
  const std::optional<cloud_storage_location>& storage_location,
  tsws_lwptr_t tsws) {
    if (tsws->cached_topic_manifest()) {
        vlog(
          dm_log.trace,
          "using cached topic manifest for topic {} (location {})",
          original_nt.value_or(nt),
          storage_location);
        co_return *tsws->cached_topic_manifest();
    }
    vlog(
      dm_log.debug,
      "downloading topic manifest for inbound migration topic {} (location {})",
      original_nt.value_or(nt),
      storage_location);
    // download manifest
    const auto& bucket_prop = cloud_storage::configuration::get_bucket_config();
    auto maybe_bucket = bucket_prop.value();
    if (!maybe_bucket) {
        co_return errc::topic_operation_error;
    }
    cloud_storage::topic_manifest_downloader tmd(
      cloud_storage_clients::bucket_name{*maybe_bucket},
      storage_location ? std::make_optional(storage_location->hint)
                       : std::nullopt,
      original_nt.value_or(nt),
      _cloud_storage_api->get()); // checked in frontend::data_migrations_active

    auto backoff = std::chrono::duration_cast<model::timestamp_clock::duration>(
      tsws->rcn().get_backoff());
    cloud_storage::topic_manifest tm;
    auto download_res = co_await tmd.download_manifest(
      tsws->rcn(), tsws->rcn().get_deadline(), backoff, &tm);
    if (!download_res.has_value()) {
        vlog(
          dm_log.warn,
          "failed to download manifest for topic {} (storage_location {}): {}",
          original_nt.value_or(nt),
          storage_location,
          download_res);
        co_return errc::topic_operation_error;
    }
    auto download_result = download_res.value();
    switch (download_result) {
    case cloud_storage::find_topic_manifest_outcome::success:
        tsws->cache_topic_manifest(std::move(tm));
        co_return tsws->cached_topic_manifest().value();
    case cloud_storage::find_topic_manifest_outcome::no_matching_manifest:
        vlog(
          dm_log.warn,
          "no matching manifest found for topic {} (storage_location {})",
          original_nt.value_or(nt),
          storage_location);
        // map not matching
        co_return errc::topic_not_exists;
    case cloud_storage::find_topic_manifest_outcome::
      multiple_matching_manifests:
        vlog(
          dm_log.warn,
          "multiple matching manifests found for topic {} (storage_location "
          "{})",
          original_nt.value_or(nt),
          storage_location);
        co_return errc::topic_operation_error;
    }
}

ss::future<errc> backend::create_topic(
  const model::topic_namespace& local_nt,
  const std::optional<model::topic_namespace>& original_nt,
  const cloud_storage::topic_manifest& manifest,
  retry_chain_node& rcn) {
    auto maybe_cfg = manifest.get_topic_config();
    if (!maybe_cfg) {
        co_return errc::topic_invalid_config;
    }

    cluster::topic_configuration topic_to_create_cfg(
      local_nt.ns,
      local_nt.tp,
      maybe_cfg->partition_count,
      maybe_cfg->replication_factor,
      maybe_cfg->tp_id);
    auto& topic_properties = topic_to_create_cfg.properties;

    // copy all properties
    topic_properties = maybe_cfg->properties;

    // override specific ones
    topic_to_create_cfg.is_migrated = true;
    if (!topic_properties.remote_topic_namespace_override) {
        topic_properties.remote_topic_namespace_override = original_nt;
    }
    topic_properties.remote_topic_properties.emplace(
      manifest.get_revision(), maybe_cfg->partition_count);
    topic_properties.shadow_indexing = model::shadow_indexing_mode::full;
    topic_properties.recovery = true;
    topic_properties.read_replica = {};
    topic_properties.read_replica_bucket = {};

    custom_assignable_topic_configuration_vector cfg_vector;
    cfg_vector.push_back(
      custom_assignable_topic_configuration(std::move(topic_to_create_cfg)));
    auto ct_res = co_await _topics_frontend.create_topics(
      std::move(cfg_vector), rcn.get_deadline());
    auto ec = ct_res[0].ec;
    if (ec == errc::topic_already_exists) {
        // make topic creation idempotent
        vlog(dm_log.info, "topic {} already exists, fine", ct_res[0].tp_ns);
        co_return errc::success;
    }
    if (ec != errc::success) {
        vlog(dm_log.warn, "failed to create topic {}: {}", ct_res[0].tp_ns, ec);
    }
    co_return ec;
}

ss::future<errc> backend::prepare_mount_topic(
  const model::topic_namespace& nt,
  const cloud_storage::topic_manifest& manifest,
  retry_chain_node& rcn) {
    auto& cfg = manifest.get_topic_config();
    if (!cfg) {
        vlog(
          dm_log.warn,
          "topic {} configuration missing in manifest, cannot prepare mount",
          nt);
        co_return errc::topic_operation_error;
    }
    // Cloud topics can't be unmounted now, but maybe someone will try to
    // use this code to mount a cloud topic later code unmounts.
    if (cfg->is_cloud_topic()) {
        vlog(
          dm_log.warn, "topic {} is a cloud topic and cannot be mounted", nt);
        co_return errc::topic_invalid_config;
    }
    vlog(
      dm_log.info,
      "trying to prepare mount topic, cfg={}, rev_id={}",
      manifest.get_topic_config(),
      manifest.get_revision());
    auto mnt_res = co_await _topic_mount_handler->get().prepare_mount_topic(
      cfg.value(), manifest.get_revision(), rcn);
    if (mnt_res == cloud_storage::topic_mount_result::mount_manifest_exists) {
        co_return errc::success;
    }
    vlog(dm_log.warn, "failed to prepare mount topic {}: {}", nt, mnt_res);
    co_return errc::topic_operation_error;
}

ss::future<errc> backend::confirm_mount_topic(
  const model::topic_namespace& nt,
  const cloud_storage::topic_manifest& manifest,
  retry_chain_node& rcn) {
    auto& cfg = manifest.get_topic_config();
    if (!cfg) {
        vlog(
          dm_log.warn,
          "topic {} configuration missing in manifest, cannot prepare mount",
          nt);
        co_return errc::topic_operation_error;
    }
    vlog(
      dm_log.info,
      "trying to commit mount topic, cfg={}, rev_id={}",
      manifest.get_topic_config(),
      manifest.get_revision());

    vlog(
      dm_log.info,
      "trying to confirm mount topic, cfg={}, rev_id={}",
      manifest.get_topic_config(),
      manifest.get_revision());
    auto mnt_res = co_await _topic_mount_handler->get().confirm_mount_topic(
      *cfg, manifest.get_revision(), rcn);
    if (
      mnt_res
      != cloud_storage::topic_mount_result::mount_manifest_not_deleted) {
        co_return errc::success;
    }
    vlog(dm_log.warn, "failed to confirm mount topic {}: {}", nt, mnt_res);
    co_return errc::topic_operation_error;
}

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
