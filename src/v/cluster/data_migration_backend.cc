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
#include "container/chunked_vector.h"
#include "data_migration_frontend.h"
#include "data_migration_types.h"
#include "data_migration_worker.h"
#include "errc.h"
#include "fwd.h"
#include "logger.h"
#include "model/fundamental.h"
#include "model/ktp.h"
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

backend::backend(
  migrations_table& table,
  frontend& frontend,
  ss::sharded<worker>& worker,
  partition_leaders_table& leaders_table,
  topics_frontend& topics_frontend,
  topic_table& topic_table,
  shard_table& shard_table,
  group_proxy& group_proxy,
  std::optional<std::reference_wrapper<cloud_storage::remote>>
    cloud_storage_api,
  std::optional<std::reference_wrapper<cloud_storage::topic_mount_handler>>
    topic_mount_handler,
  ss::abort_source& as)
  : _self(*config::node().node_id())
  , _table(table)
  , _frontend(frontend)
  , _worker(worker)
  , _leaders_table(leaders_table)
  , _topics_frontend(topics_frontend)
  , _topic_table(topic_table)
  , _shard_table(shard_table)
  , _group_proxy(group_proxy)
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
                  mark_migration_step_done_for_ntp(
                    (*rs_it)->second, ntp_resp.ntp);
                  to_advance_if_done(*rs_it);
              }
          });
    }

    // process topic work results
    auto topic_work_results = std::move(_topic_work_results);
    chunked_vector<model::topic_namespace> retriable_topic_work;
    co_await ssx::async_for_each(
      topic_work_results, [this, &retriable_topic_work](auto& result) {
          if (auto rs_it = get_rstate(result.migration, result.sought_state)) {
              switch (result.ec) {
              case errc::success:
                  mark_migration_step_done_for_nt((*rs_it)->second, result.nt);
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
                  retriable_topic_work.push_back(std::move(result.nt));
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
    chunked_vector<model::topic_namespace> to_schedule_topic_work;
    co_await ssx::async_for_each(
      _topic_work_to_retry,
      [&to_schedule_topic_work, &next_tick, now](const auto& entry) {
          const auto& [nt, deadline] = entry;
          if (deadline <= now) {
              to_schedule_topic_work.push_back(nt);
          } else {
              next_tick = std::min(deadline, next_tick);
          }
      });

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
      retriable_topic_work, [this, &next_tick, new_deadline](const auto& nt) {
          auto it = _topic_migration_map.find(nt);
          if (it == _topic_migration_map.end()) {
              return;
          }
          auto migration_id = it->second;

          auto& mrstate = _migration_states.find(migration_id)->second;
          auto& tstate = mrstate.outstanding_topics[nt];
          if (
            tstate.topic_scoped_work_needed && !tstate.topic_scoped_work_done) {
              _topic_work_to_retry.try_emplace(std::move(nt), new_deadline);
              next_tick = std::min(next_tick, new_deadline);
          }
      });

    // schedule fibers
    for (auto node_id : to_send_rpc) {
        _nodes_to_retry.erase(node_id);
        co_await send_rpc(node_id);
    }
    for (const auto& nt : to_schedule_topic_work) {
        _topic_work_to_retry.erase(nt);
        co_await schedule_topic_work(nt);
    }
    spawn_advances();
    if (next_tick == model::timeout_clock::time_point::max()) {
        _timer.cancel();
    } else {
        _timer.rearm(next_tick);
    }
    vlog(dm_log.info, "end backend work cycle");
}

void backend::wakeup() { _sem.signal(1 - _sem.available_units()); }

std::optional<backend::migration_reconciliation_states_t::iterator>
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
  migration_reconciliation_state& rs, const model::ntp& ntp) {
    auto& rs_topics = rs.outstanding_topics;
    auto rs_topic_it = rs_topics.find({ntp.ns, ntp.tp.topic});
    if (rs_topic_it != rs_topics.end()) {
        auto& tstate = rs_topic_it->second;
        auto& rs_parts = tstate.outstanding_partitions;
        auto rs_part_it = rs_parts.find(ntp.tp.partition);
        if (rs_part_it != rs_parts.end()) {
            for (const auto& affected_node_id : rs_part_it->second) {
                auto nstate_it = _node_states.find(affected_node_id);
                nstate_it->second.erase(ntp);
                if (nstate_it->second.empty()) {
                    _node_states.erase(nstate_it);
                    _nodes_to_retry.erase(affected_node_id);
                }
            }
            rs_parts.erase(rs_part_it);
            erase_tstate_if_done(rs, rs_topic_it);
        }
    }
}

void backend::mark_migration_step_done_for_nt(
  migration_reconciliation_state& rs, const model::topic_namespace& nt) {
    auto& rs_topics = rs.outstanding_topics;
    auto rs_topic_it = rs_topics.find(nt);
    if (rs_topic_it != rs_topics.end()) {
        auto& tstate = rs_topic_it->second;
        tstate.topic_scoped_work_done = true;
        erase_tstate_if_done(rs, rs_topic_it);
    }
}

void backend::erase_tstate_if_done(
  migration_reconciliation_state& mrstate, topic_map_t::iterator it) {
    auto& tstate = it->second;
    if (
      tstate.outstanding_partitions.empty()
      && (!tstate.topic_scoped_work_needed || tstate.topic_scoped_work_done)) {
        _topic_migration_map.erase(it->first);
        mrstate.outstanding_topics.erase(it);
    }
}

ss::future<> backend::send_rpc(model::node_id node_id) {
    check_ntp_states_request req;
    co_await ssx::async_for_each(
      _node_states[node_id], [this, &req](const auto& pair) {
          auto& [ntp, migration_id] = pair;
          req.sought_states.push_back(
            {.ntp = ntp,
             .migration = migration_id,
             .state = *_migration_states.find(migration_id)
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

ss::future<> backend::schedule_topic_work(model::topic_namespace nt) {
    auto it = _topic_migration_map.find(nt);
    if (it == _topic_migration_map.end()) {
        co_return;
    }
    auto migration_id = it->second;

    auto& mrstate = _migration_states.find(migration_id)->second;
    auto& tstate = mrstate.outstanding_topics.at(nt);
    if (!tstate.topic_scoped_work_needed || tstate.topic_scoped_work_done) {
        co_return;
    }
    const auto maybe_migration = _table.get_migration(migration_id);
    if (!maybe_migration) {
        vlog(dm_log.trace, "migration {} gone, ignoring", migration_id);
        co_return;
    }
    topic_work tw{
      .migration_id = migration_id,
      .sought_state = *mrstate.scope.sought_state,
      .info = get_topic_work_info(nt, maybe_migration->get())};

    ssx::spawn_with_gate(
      _gate, [this, nt = std::move(nt), tw = std::move(tw)]() mutable {
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
        it->second = tsws;
        if (ins) {
            break;
        }
        tsws->rcn().request_abort();
        // waiting for existing work to complete and delete its entry
        vlog(
          dm_log.info,
          "waiting for older topic work {} on nt={} to complete",
          tw,
          nt);
        auto old_ec = co_await tsws->future();
        vlog(
          dm_log.info,
          "older topic work {} on nt {} completed with {}",
          tw,
          nt,
          old_ec);
    }

    errc ec;
    try {
        vlog(dm_log.debug, "doing topic work {} on nt={}", tw, nt);
        ec = co_await std::visit(
          [this, &nt, &tw, tsws = std::move(tsws)](const auto& info) mutable {
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
          "exception occured during topic work {} on nt={}",
          tw,
          nt,
          std::current_exception());
        ec = errc::topic_operation_error;
    }

    auto it = _active_topic_work_states.find(nt);
    vassert(it != _active_topic_work_states.end(), "tsws {} disappeared", nt);
    it->second->set_value(ec);
    _active_topic_work_states.erase(it);

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
    case state::prepared:
        co_return co_await retry_loop(rcn, [this, &nt, &itwi, &rcn] {
            return create_topic(
              nt, itwi.source, itwi.cloud_storage_location, rcn);
        });
    case state::executed:
        co_return co_await retry_loop(
          rcn, [this, &nt, &rcn] { return prepare_mount_topic(nt, rcn); });
    case state::finished: {
        co_return co_await retry_loop(
          rcn, [this, &nt, &rcn] { return confirm_mount_topic(nt, rcn); });
    }
    case state::cancelled: {
        // attempt to unmount first
        auto unmount_res = co_await unmount_topic(nt, rcn);
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
    case state::finished: {
        if (nt == model::kafka_consumer_offsets_nt) {
            co_return errc::success;
        }
        // unmount first
        auto unmount_res = co_await unmount_topic(nt, rcn);
        if (unmount_res != errc::success) {
            vlog(
              dm_log.warn, "failed to unmount topic {}: {}", nt, unmount_res);
            co_return unmount_res;
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
        co_await _active_topic_work_states.begin()->second->future();
    }
}

ss::future<errc> backend::create_topic(
  const model::topic_namespace& local_nt,
  const std::optional<model::topic_namespace>& original_nt,
  const std::optional<cloud_storage_location>& storage_location,
  retry_chain_node& rcn) {
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
      original_nt.value_or(local_nt),
      _cloud_storage_api->get()); // checked in frontend::data_migrations_active
    // todo: is it correct to rely on it checked in frontend?
    // todo: configure timeout and backoff
    auto backoff = std::chrono::duration_cast<model::timestamp_clock::duration>(
      rcn.get_backoff());
    cloud_storage::topic_manifest tm;
    auto download_res = co_await tmd.download_manifest(
      rcn, rcn.get_deadline(), backoff, &tm);
    if (
      !download_res.has_value()
      || download_res.assume_value()
           != cloud_storage::find_topic_manifest_outcome::success) {
        vlog(
          dm_log.warn,
          "failed to download manifest for topic {} (storage_location {}): {}",
          original_nt.value_or(local_nt),
          storage_location,
          download_res);
        co_return errc::topic_operation_error;
    }
    auto maybe_cfg = tm.get_topic_config();
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
      tm.get_revision(), maybe_cfg->partition_count);
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
  const model::topic_namespace& nt, retry_chain_node& rcn) {
    auto cfg = _topic_table.get_topic_cfg(nt);
    if (!cfg) {
        co_return errc::topic_not_exists;
    }

    auto rev_id = _topic_table.get_initial_revision(nt);
    if (!rev_id) {
        co_return errc::topic_not_exists;
    }

    vlog(
      dm_log.info,
      "trying to prepare mount topic, cfg={}, rev_id={}",
      *cfg,
      *rev_id);
    auto mnt_res = co_await _topic_mount_handler->get().prepare_mount_topic(
      *cfg, *rev_id, rcn);
    if (mnt_res == cloud_storage::topic_mount_result::mount_manifest_exists) {
        co_return errc::success;
    }
    vlog(dm_log.warn, "failed to prepare mount topic {}: {}", nt, mnt_res);
    co_return errc::topic_operation_error;
}

ss::future<errc> backend::confirm_mount_topic(
  const model::topic_namespace& nt, retry_chain_node& rcn) {
    auto cfg = _topic_table.get_topic_cfg(nt);
    if (!cfg) {
        co_return errc::topic_not_exists;
    }

    auto rev_id = _topic_table.get_initial_revision(nt);
    if (!rev_id) {
        co_return errc::topic_not_exists;
    }

    vlog(
      dm_log.info,
      "trying to confirm mount topic, cfg={}, rev_id={}",
      *cfg,
      *rev_id);
    auto mnt_res = co_await _topic_mount_handler->get().confirm_mount_topic(
      *cfg, *rev_id, rcn);
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

ss::future<errc> backend::unmount_topic(
  const model::topic_namespace& nt, retry_chain_node& rcn) {
    return retry_loop(
      rcn, [this, &nt, &rcn] { return do_unmount_topic(nt, rcn); });
}

ss::future<errc> backend::do_unmount_topic(
  const model::topic_namespace& nt, retry_chain_node& rcn) {
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

void backend::to_advance_if_done(
  migration_reconciliation_states_t::const_iterator it) {
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
                  nt, tstate, id, mrstate.scope, false);
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
        auto new_it = _migration_states.emplace_hint(old_it, id, new_scope);
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
    model::topic_namespace nt{delta.ntp.ns, delta.ntp.tp.topic};
    auto it = _topic_migration_map.find(nt);
    if (it == _topic_migration_map.end()) {
        co_return;
    }
    auto migration_id = it->second;

    if (
      delta.type == topic_table_ntp_delta_type::added
      || delta.type == topic_table_ntp_delta_type::removed) {
        // it can be only ourselves, as partition changes are not allowed when
        // the topic migration is in one of the states tracked here
        co_return;
    }

    // coordination
    vassert(
      delta.type == topic_table_ntp_delta_type::replicas_updated
        || delta.type == topic_table_ntp_delta_type::disabled_flag_updated,
      "topic {} altered with topic_table_delta_type={} during "
      "migration {}",
      nt,
      delta.type,
      migration_id);
    auto& mrstate = _migration_states.find(migration_id)->second;
    if (
      !mrstate.scope.partition_work_needed(nt)
      && !mrstate.scope.topic_work_needed) {
        co_return;
    }
    auto& tstate = mrstate.outstanding_topics.at(nt);
    clear_tstate_belongings(nt, tstate);
    tstate.clear();
    // We potentially re-enqueue an already coordinated partition here.
    // The first RPC reply will clear it.
    co_await reconcile_existing_topic(
      nt, tstate, migration_id, mrstate.scope, false);

    // local partition work
    if (has_local_replica(delta.ntp)) {
        _local_work_states[nt].try_emplace(
          delta.ntp.tp.partition,
          migration_id,
          *_migration_states.find(migration_id)->second.scope.sought_state,
          migrated_replica_status::waiting_for_rpc);
    } else {
        auto topic_work_it = _local_work_states.find(nt);
        if (topic_work_it != _local_work_states.end()) {
            auto& topic_work_state = topic_work_it->second;
            auto rwstate_it = topic_work_state.find(delta.ntp.tp.partition);
            if (rwstate_it != topic_work_state.end()) {
                auto& rwstate = rwstate_it->second;
                if (rwstate.shard) {
                    stop_partition_work(delta.ntp, rwstate);
                }
                topic_work_state.erase(rwstate_it);
                if (topic_work_state.empty()) {
                    _local_work_states.erase(topic_work_it);
                }
            }
        }
    }
}

void backend::handle_shard_update(
  const model::ntp& ntp, raft::group_id, std::optional<ss::shard_id> shard) {
    if (auto maybe_rwstate = get_replica_work_state(ntp)) {
        auto& rwstate = maybe_rwstate->get();
        if (rwstate.status == migrated_replica_status::can_run) {
            update_partition_shard(ntp, rwstate, shard);
        }
    }
}

ss::future<check_ntp_states_reply>
backend::check_ntp_states_locally(check_ntp_states_request&& req) {
    vlog(dm_log.debug, "processing node request {}", req);
    check_ntp_states_reply reply;
    for (const auto& ntp_req : req.sought_states) {
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
            continue;
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
            continue;
        }

        auto& topic_work_state
          = _local_work_states[model::topic_namespace_view{ntp_req.ntp}];
        auto& rwstate
          = topic_work_state
              .try_emplace(
                ntp_req.ntp.tp.partition,
                ntp_req.migration,
                ntp_req.state,
                migrated_replica_status::waiting_for_controller_update)
              .first->second;

        if (ntp_req.migration != rwstate.migration_id) {
            vlog(
              dm_log.warn,
              "migration_id={} got RPC to move ntp {} to state {}, but current "
              "migration id is {}, ignoring",
              ntp_req.migration,
              ntp_req.ntp,
              ntp_req.state,
              rwstate.migration_id);
            continue;
        }

        if (ntp_req.state > rwstate.sought_state) {
            // RPC request indicates that partition work has already progressed
            // to a later state. Stop current work and wait for the controller
            // update.
            if (rwstate.shard) {
                stop_partition_work(ntp_req.ntp, rwstate);
            }
            rwstate = replica_work_state(
              ntp_req.migration,
              ntp_req.state,
              migrated_replica_status::waiting_for_controller_update);
        } else if (ntp_req.state < rwstate.sought_state) {
            vlog(
              dm_log.warn,
              "migration_id={} got RPC to move ntp {} to state {}, but current "
              "replica work state is {}, ignoring",
              ntp_req.migration,
              ntp_req.ntp,
              ntp_req.state,
              rwstate);
            continue;
        }

        switch (rwstate.status) {
        case migrated_replica_status::waiting_for_controller_update:
            break;
        case migrated_replica_status::waiting_for_rpc:
            // raft0 and RPC agree => time to do it!
            rwstate.status = migrated_replica_status::can_run;
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
    }

    vlog(dm_log.debug, "node request reply: {}", reply);
    return ssx::now(std::move(reply));
}

void backend::update_partition_shard(
  const model::ntp& ntp,
  replica_work_state& rwstate,
  std::optional<ss::shard_id> new_shard) {
    vlog(
      dm_log.trace,
      "for ntp {} for migration {} seeking state {} updating shard: {} => "
      "{}",
      ntp,
      rwstate.migration_id,
      rwstate.sought_state,
      rwstate.shard,
      new_shard);
    if (new_shard != rwstate.shard) {
        if (rwstate.shard) {
            stop_partition_work(ntp, rwstate);
        }
        rwstate.shard = new_shard;
        if (new_shard) {
            start_partition_work(ntp, rwstate);
        }
    }
}

void backend::clear_tstate_belongings(
  const model::topic_namespace& nt, const topic_reconciliation_state& tstate) {
    const auto& partitions = tstate.outstanding_partitions;
    for (const auto& [partition, nodes] : partitions) {
        for (const model::node_id& node : nodes) {
            auto ns_it = _node_states.find(node);
            ns_it->second.erase({nt.ns, nt.tp, partition});
            if (ns_it->second.empty()) {
                _nodes_to_retry.erase(node);
                _node_states.erase(ns_it);
            }
        }
    }
    _topic_work_to_retry.erase(nt);
}

ss::future<> backend::drop_migration_reconciliation_rstate(
  migration_reconciliation_states_t::const_iterator rs_it) {
    const auto& topics = rs_it->second.outstanding_topics;

    co_await ss::parallel_for_each(
      topics, [this](const topic_map_t::value_type& topic_map_entry) {
          return clear_tstate(topic_map_entry);
      });
    _migration_states.erase(rs_it);
}

ss::future<>
backend::clear_tstate(const topic_map_t::value_type& topic_map_entry) {
    const auto& [nt, tstate] = topic_map_entry;
    clear_tstate_belongings(nt, tstate);
    auto topic_work_it = _local_work_states.find(nt);
    if (topic_work_it != _local_work_states.end()) {
        auto& topic_work_state = topic_work_it->second;
        co_await ssx::async_for_each(
          topic_work_state, [this, &nt](auto& partition_local_work_entry) {
              auto& [partition_id, rwstate] = partition_local_work_entry;
              if (rwstate.shard) {
                  stop_partition_work(
                    model::ntp(nt.ns, nt.tp, partition_id), rwstate);
              }
          });
    }
    auto it = _active_topic_work_states.find(nt);
    if (it != _active_topic_work_states.end()) {
        it->second->rcn().request_abort();
        co_await it->second->future();
    }

    _topic_migration_map.erase(nt);
}

ss::future<> backend::reconcile_existing_topic(
  const model::topic_namespace& nt,
  topic_reconciliation_state& tstate,
  id migration,
  work_scope scope,
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
           scope,
           migration,
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
                        it->second);
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
                      auto& topic_work_state = _local_work_states[nt];
                      auto [it, _] = topic_work_state.try_emplace(
                        assignment.id,
                        migration,
                        *scope.sought_state,
                        migrated_replica_status::waiting_for_rpc);
                      auto& rwstate = it->second;
                      if (
                        rwstate.migration_id != migration
                        || rwstate.sought_state < *scope.sought_state) {
                          if (it->second.shard) {
                              stop_partition_work(ntp, rwstate);
                          }
                          rwstate = replica_work_state{
                            migration,
                            *scope.sought_state,
                            migrated_replica_status::waiting_for_rpc};
                      }
                      if (rwstate.sought_state == *scope.sought_state) {
                          switch (rwstate.status) {
                          case migrated_replica_status::
                            waiting_for_controller_update:
                              rwstate.status = migrated_replica_status::can_run;
                              [[fallthrough]];
                          case migrated_replica_status::can_run: {
                              auto new_shard = _shard_table.shard_for(ntp);
                              update_partition_shard(ntp, rwstate, new_shard);
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
        _topic_work_to_retry.insert_or_assign(nt, now);
    }
}

ss::future<> backend::reconcile_migration(
  migration_reconciliation_state& mrstate, const migration_metadata& metadata) {
    vlog(
      dm_log.debug,
      "tracking migration {} transition towards state {}",
      metadata.id,
      mrstate.scope.sought_state);

    if (!mrstate.partition_group_map) {
        mrstate.partition_group_map.emplace();
        const auto& groups = std::visit(
          [](const auto& migration) -> const chunked_vector<consumer_group>& {
              return migration.groups;
          },
          metadata.migration);

        for (const auto& group : groups) {
            auto partition = _group_proxy.partition_for(group);
            vassert(partition, "cannot get ntp for group {}", group);
            auto [it, ins] = mrstate.partition_group_map->try_emplace(
              *partition);
            it->second.push_back(group);
        }
    }

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
    _topic_migration_map.emplace(nt, migration_id);
    co_return co_await reconcile_existing_topic(
      nt, tstate, migration_id, mrstate.scope, true);
}

std::optional<std::reference_wrapper<backend::replica_work_state>>
backend::get_replica_work_state(const model::ntp& ntp) {
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
  const model::ntp& ntp, const backend::replica_work_state& rwstate) {
    vlog(
      dm_log.trace,
      "while working on migration {}, asking worker on shard "
      "{} to advance ntp {} to state {}",
      rwstate.migration_id,
      rwstate.shard,
      ntp,
      rwstate.sought_state);
    const auto maybe_migration = _table.get_migration(rwstate.migration_id);
    if (!maybe_migration) {
        vlog(dm_log.trace, "migration {} gone, ignoring", rwstate.migration_id);
        return;
    }

    partition_work work{
      .migration_id = rwstate.migration_id,
      .sought_state = rwstate.sought_state,
      .info = get_partition_work_info(ntp, maybe_migration->get())};

    ssx::spawn_with_gate(
      _gate, [this, &ntp, &rwstate, work = std::move(work)]() mutable {
          return _worker
            .invoke_on(
              *rwstate.shard,
              &worker::perform_partition_work,
              model::ntp{ntp},
              std::move(work))
            .then([this, ntp = ntp, rwstate](errc ec) mutable {
                if (ec == errc::success) {
                    vlog(
                      dm_log.trace,
                      "as part of migration {} worker on shard {} has "
                      "advanced ntp {} to state {}",
                      rwstate.migration_id,
                      rwstate.shard,
                      ntp,
                      rwstate.sought_state);
                    on_partition_work_completed(
                      std::move(ntp),
                      rwstate.migration_id,
                      rwstate.sought_state);
                } else {
                    // worker should always retry unless we instructed
                    // it to abort or it is shutting down
                    vlog(
                      dm_log.warn,
                      "while working on migration {} worker on shard "
                      "{} stopped trying to advance ntp {} to state {}",
                      rwstate.migration_id,
                      rwstate.shard,
                      std::move(ntp),
                      rwstate.sought_state);
                }
            });
      });
}

void backend::stop_partition_work(
  model::ntp ntp, const backend::replica_work_state& rwstate) {
    vlog(
      dm_log.info,
      "while working on migration {}, asking worker on shard "
      "{} to stop trying to advance ntp {} to state {}",
      rwstate.migration_id,
      rwstate.shard,
      ntp,
      rwstate.sought_state);
    ssx::spawn_with_gate(
      _gate,
      [this,
       &ntp,
       id = rwstate.migration_id,
       shard = rwstate.shard.value(),
       state = rwstate.sought_state] {
          return _worker.invoke_on(
            shard, &worker::abort_partition_work, std::move(ntp), id, state);
      });
}

void backend::on_partition_work_completed(
  model::ntp&& ntp, id migration, state state) {
    auto maybe_rwstate = get_replica_work_state(ntp);
    if (!maybe_rwstate) {
        return;
    }
    auto& rwstate = maybe_rwstate->get();
    if (rwstate.migration_id == migration && rwstate.sought_state == state) {
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
                  vassert(false, "A migration cannot be in a deleted state");
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
          .topic_work_needed = true};
    case state::executing:
        return {
          .sought_state = state::executed,
          .data_partition_work_needed = false,
          .co_partition_work_needed = false,
          .topic_work_needed = true};
    case state::cut_over:
        return {
          .sought_state = state::finished,
          .data_partition_work_needed = false,
          .co_partition_work_needed = false,
          .topic_work_needed = true};
    case state::canceling:
        return {
          .sought_state = state::cancelled,
          .data_partition_work_needed = false,
          .co_partition_work_needed = false,
          .topic_work_needed = true};
    default:
        return {
          .sought_state = {},
          .data_partition_work_needed = false,
          .co_partition_work_needed = false,
          .topic_work_needed = false};
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
          .topic_work_needed = false};
    case state::executing:
        return {
          .sought_state = state::executed,
          .data_partition_work_needed = true,
          .co_partition_work_needed = true,
          .topic_work_needed = false};
    case state::cut_over:
        return {
          .sought_state = state::finished,
          .data_partition_work_needed = false,
          .co_partition_work_needed = true,
          .topic_work_needed = true};
    case state::canceling:
        return {
          .sought_state = state::cancelled,
          .data_partition_work_needed = true,
          .co_partition_work_needed = true,
          .topic_work_needed = false};
    default:
        return {
          .sought_state = {},
          .data_partition_work_needed = false,
          .co_partition_work_needed = false,
          .topic_work_needed = false};
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
      "{{migration {}, sought_state: {}, shard: {}, status: {}}}",
      rws.migration_id,
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
