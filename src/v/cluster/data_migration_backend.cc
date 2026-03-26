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
                      if (auto it = rev_map.find(gid);
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

} // namespace cluster::data_migrations
