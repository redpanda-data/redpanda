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
#include "cluster/data_migration_worker.h"

#include "archival/ntp_archiver_service.h"
#include "base/vassert.h"
#include "cluster/data_migration_types.h"
#include "cluster/types.h"
#include "cluster_utils.h"
#include "container/chunked_vector.h"
#include "errc.h"
#include "kafka/protocol/types.h"
#include "logger.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "partition_leaders_table.h"
#include "partition_manager.h"
#include "rpc/connection_cache.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/all.hh>

#include <fmt/ostream.h>

#include <chrono>
#include <memory>
#include <optional>
#include <tuple>
#include <utility>

namespace cluster::data_migrations {

worker::worker(
  model::node_id self,
  partition_leaders_table& leaders_table,
  partition_manager& partition_manager,
  ss::shared_ptr<group_proxy> group_proxy,
  ss::abort_source& as)
  : _self(self)
  , _leaders_table(leaders_table)
  , _partition_manager(partition_manager)
  , _group_proxy(std::move(group_proxy))
  , _as(as)
  , _as_sub(_as.subscribe([this]() noexcept { abort_all(); }))
  , _operation_timeout(5s)
  , _cooldown_period(100ms) {}

ss::future<> worker::stop() {
    _as.request_abort();
    if (!_gate.is_closed()) {
        co_await _gate.close();
    }
    vlog(dm_log.debug, "worker stopped");
}

void worker::abort_all() noexcept {
    std::erase_if(
      _managed_ntps, [this](managed_ntps_map_t::value_type& ntp_state_pair) {
          auto& ntp_state = *ntp_state_pair.second;
          std::erase_if(
            ntp_state.migration_states,
            [](mntps_map_t::value_type& mntp_state_pair) {
                auto& mntp_state = *mntp_state_pair.second;
                if (mntp_state.running) {
                    mntp_state.running->as.request_abort();
                }
                if (mntp_state.last_requested) {
                    mntp_state.report_back(errc::shutting_down);
                    if (!mntp_state.running) {
                        // no requested and no running work, entry should go
                        return true;
                    }
                }
                return false;
            });
          if (ntp_state.migration_states.empty()) {
              _leaders_table.unregister_leadership_change_notification(
                ntp_state.leadership_subscription);
              return true;
          }
          return false;
      });
}

worker::migration_id_key
worker::make_migration_id_key(const model::ntp& ntp, id migration_id) {
    return model::topic_namespace_view(ntp) == model::kafka_consumer_offsets_nt
             ? std::make_optional(migration_id)
             : std::nullopt;
}

ss::future<errc>
worker::perform_partition_work(model::ntp&& ntp, partition_work&& work) {
    if (_as.abort_requested() || _gate.is_closed()) {
        return ssx::now(errc::shutting_down);
    }

    // calculate early as we'll be moving ntp and work
    auto migration_id_key = make_migration_id_key(ntp, work.migration_id);

    // ensure ntp is managed
    auto ntp_state_it = _managed_ntps.find(ntp);
    if (ntp_state_it == _managed_ntps.end()) {
        // not managed yet
        bool is_leader = _self == _leaders_table.get_leader(ntp);
        auto leadership_subscription
          = _leaders_table.register_leadership_change_notification(
            ntp,
            [this](
              const model::ntp& ntp, model::term_id, model::node_id leader) {
                handle_leadership_update(ntp, _self == leader);
            });
        std::tie(ntp_state_it, std::ignore) = _managed_ntps.emplace(
          std::move(ntp),
          std::make_unique<ntp_state_t>(is_leader, leadership_subscription));
    }

    auto& ntp_state = *ntp_state_it->second;
    auto& mntp_states = ntp_state.migration_states;

    auto it = mntp_states.find(migration_id_key);
    if (it == mntp_states.end()) {
        // no work yet for this migration id key
        std::tie(it, std::ignore) = mntp_states.emplace(
          migration_id_key, std::make_unique<mntp_state_t>(std::move(work)));
    } else {
        // some stale work in progress and/or enqueued, kick out both
        auto& mntp_state = *it->second;
        if (auto& r = mntp_state.running) {
            if (
              r->work->migration_id != work.migration_id
              || r->work->sought_state != work.sought_state) {
                r->as.request_abort();
            }
        }
        if (mntp_state.last_requested) {
            mntp_state.report_back(errc::invalid_data_migration_state);
        }
        mntp_state.last_requested.emplace(std::move(work));
    }

    auto f = it->second->last_requested->promise.get_future();
    spawn_work_fiber_if_needed(
      ntp_state_it->first,
      migration_id_key,
      *ntp_state_it->second,
      *it->second);

    return f;
}

void worker::abort_partition_work(
  model::ntp&& ntp, id migration_id, state sought_state) {
    auto ntp_state_it = _managed_ntps.find(ntp);
    if (ntp_state_it == _managed_ntps.end()) {
        return;
    }
    auto& mntp_states = ntp_state_it->second->migration_states;

    auto it = std::as_const(mntp_states)
                .find(make_migration_id_key(ntp, migration_id));
    if (it == mntp_states.cend()) {
        return;
    }

    auto& ntp_state = *it->second;
    if (auto& r = ntp_state.running) {
        if (
          r->work->migration_id == migration_id
          && r->work->sought_state == sought_state) {
            r->as.request_abort();
        }
    }
    if (auto& lr = ntp_state.last_requested) {
        if (
          lr->work->migration_id == migration_id
          && lr->work->sought_state == sought_state) {
            ntp_state.report_back(errc::invalid_data_migration_state);
            if (!ntp_state.running) {
                // no requested and no running work, entry should go
                unmanage_ntp(ntp_state_it, it);
            }
        }
    }
}

worker::mntp_state_t::requested_t::requested_t(partition_work&& w)
  : work(ss::make_lw_shared(std::move(w))) {}

bool worker::mntp_state_t::still_needed() const {
    vassert(running, "non running work");
    return last_requested
           && last_requested->work->sought_state == running->work->sought_state
           && last_requested->work->migration_id == running->work->migration_id
           && !running->as.abort_requested();
}

void worker::mntp_state_t::report_back(errc ec) {
    vassert(last_requested, "no requested work");
    last_requested->promise.set_value(ec);
    last_requested = std::nullopt;
}

worker::mntp_state_t::running_t::running_t(ss::lw_shared_ptr<partition_work> w)
  : work(std::move(w)) {}

worker::mntp_state_t::mntp_state_t(partition_work&& work)
  : last_requested(std::in_place, std::move(work)) {};

worker::ntp_state_t::ntp_state_t(
  bool is_leader, notification_id_type leadership_subscription)
  : is_leader(is_leader)
  , leadership_subscription(leadership_subscription) {}

void worker::handle_leadership_update(const model::ntp& ntp, bool is_leader) {
    vlog(
      dm_log.info,
      "got leadership update regarding ntp={}, is_leader={}",
      ntp,
      is_leader);
    auto& ntp_state = *_managed_ntps.at(ntp);
    ntp_state.is_leader = is_leader;
    for (auto& [midkey, mntp_state_ptr] : ntp_state.migration_states) {
        spawn_work_fiber_if_needed(ntp, midkey, ntp_state, *mntp_state_ptr);
    }
}

void worker::unmanage_ntp(const model::ntp& ntp, migration_id_key midkey) {
    auto ntp_state_it = _managed_ntps.find(ntp);
    vassert(
      ntp_state_it != _managed_ntps.end(),
      "trying to unmanage non-managed ntp {}, midkey {}",
      ntp,
      midkey);
    unmanage_ntp(
      ntp_state_it, ntp_state_it->second->migration_states.find(midkey));
}

void worker::unmanage_ntp(
  managed_ntps_map_t::iterator ntp_state_it, mntps_map_t::const_iterator it) {
    vassert(
      !it->second->running,
      "cannot unmanage NTP {} with running work",
      it->first);

    ntp_state_it->second->migration_states.erase(it);
    if (ntp_state_it->second->migration_states.empty()) {
        _leaders_table.unregister_leadership_change_notification(
          ntp_state_it->second->leadership_subscription);
        _managed_ntps.erase(ntp_state_it);
    }
}

ss::future<errc> worker::do_work(
  const model::ntp& ntp, mntp_state_t::running_t& running_work) noexcept {
    try {
        vlog(
          dm_log.trace,
          "starting work on migration {} ntp {} towards state {}",
          running_work.work->migration_id,
          ntp,
          running_work.work->sought_state);
        co_return co_await std::visit(
          [this, &ntp, &running_work](auto& info) {
              return do_work(ntp, running_work, info);
          },
          running_work.work->info);
    } catch (...) {
        vlog(
          dm_log.warn,
          "exception occurred during partition work on migration {} ntp {} "
          "towards {} state: {}",
          running_work.work->migration_id,
          ntp,
          running_work.work->sought_state,
          std::current_exception());
        co_return errc::partition_operation_failed;
    }
}

ss::future<errc> worker::do_work(
  const model::ntp& ntp,
  mntp_state_t::running_t& running_work,
  const inbound_partition_work_info&) {
    vassert(
      false,
      "inbound partition work requested on {} towards {} state",
      ntp,
      running_work.work->sought_state);
    return ssx::now(errc::success);
}

ss::future<errc> worker::do_work(
  const model::ntp& ntp,
  mntp_state_t::running_t& running_work,
  const outbound_partition_work_info& otwi) {
    auto partition = _partition_manager.get(ntp);
    if (!partition) {
        co_return errc::partition_not_exists;
    }

    switch (running_work.work->sought_state) {
    case state::prepared:
        vassert(otwi.groups.empty(), "nothing to do with groups in preparing");
        co_return co_await partition->flush_archiver();
    case state::executed:
        if (!otwi.groups.empty()) {
            auto res = co_await block_groups(
              ntp, otwi.groups, true, running_work.work->revision_id);
            co_return res.has_value() ? errc::success : res.error();
        } else {
            auto block_res = co_await block_partition(
              partition, true, running_work.work->revision_id);
            if (!block_res.has_value()) {
                co_return block_res.error();
            }
            auto block_offset = block_res.value();

            auto deadline = model::timeout_clock::now() + 5s;
            co_return co_await partition->flush(
              block_offset, deadline, running_work.as);
        }
    case state::finished: {
        vassert(
          !otwi.groups.empty(),
          "nothing to do with data partitions in cut_over, they are also being "
          "deleted by topic work");
        // todo: shift to a new "cleanup" stage?
        auto del_res = co_await _group_proxy->delete_groups(
          ntp, otwi.groups, running_work.work->revision_id);
        // invalid_data_migration_state may indicate group already deleted
        if (
          del_res != std::error_code{}
          && del_res != errc::invalid_data_migration_state) {
            co_return map_update_interruption_error_code(del_res);
        }
        auto block_res = co_await block_groups(
          ntp, otwi.groups, false, running_work.work->revision_id);
        if (!block_res.has_value()) {
            co_return block_res.error();
        }
        co_return errc::success;
    }
    case state::cancelled: {
        auto res = co_await (
          !otwi.groups.empty()
            ? block_groups(
                ntp, otwi.groups, false, running_work.work->revision_id)
            : block_partition(
                partition, false, running_work.work->revision_id));
        // invalid_data_migration_state indicates partition/group already
        // unblocked
        co_return res.has_value()
            || (res.error() == errc::invalid_data_migration_state)
          ? errc::success
          : res.error();
    }
    default:
        vassert(
          false,
          "outbound partition work requested on {} towards {} state",
          ntp,
          running_work.work->sought_state);
    }
}

ss::future<result<model::offset, errc>> worker::block_partition(
  ss::lw_shared_ptr<partition> partition,
  bool block,
  model::revision_id revision_id) {
    auto res = co_await partition->set_writes_disabled(
      partition_properties_stm::writes_disabled{block},
      model::timeout_clock::now() + 5s,
      revision_id);
    if (res.has_value()) {
        co_return res.value();
    }
    co_return map_update_interruption_error_code(res.error());
}

ss::future<result<model::offset, errc>> worker::block_groups(
  const model::ntp& ntp,
  const chunked_vector<kafka::group_id>& groups,
  bool block,
  model::revision_id revision_id) {
    auto res = co_await _group_proxy->set_blocked_for_groups(
      ntp, groups, block, revision_id);
    if (res.has_value()) {
        co_return res.value();
    }
    co_return map_update_interruption_error_code(res.error());
}

void worker::spawn_work_fiber_if_needed(
  const model::ntp& ntp,
  migration_id_key midkey,
  ntp_state_t& ntp_state,
  mntp_state_t& mntp_state) {
    if (mntp_state.running) {
        return;
    }
    ssx::spawn_with_gate(
      _gate, [this, &ntp, midkey, &ntp_state, &mntp_state]() {
          return work_fiber(ntp, midkey, ntp_state, mntp_state);
      });
}

ss::future<> worker::work_fiber(
  model::ntp ntp,
  migration_id_key midkey,
  ntp_state_t& ntp_state,
  mntp_state_t& mntp_state) {
    while (true) {
        vassert(!mntp_state.running, "work already running for {}", ntp);
        if (!mntp_state.last_requested) {
            vlog(
              dm_log.trace,
              "no requested work for ntp {}, clearing state and stopping fiber",
              ntp);
            unmanage_ntp(ntp, midkey);
            co_return;
        }
        if (_as.abort_requested() || _gate.is_closed()) {
            mntp_state.report_back(errc::shutting_down);
            unmanage_ntp(ntp, midkey);
            co_return;
        }
        if (!ntp_state.is_leader) {
            vlog(dm_log.trace, "not leader for ntp {}, stopping fiber", ntp);
            co_return;
        }

        mntp_state.running.emplace(mntp_state.last_requested->work);
        auto ec = co_await do_work(ntp, *mntp_state.running);
        bool still_needed = mntp_state.still_needed();
        vlog(
          dm_log.trace,
          "work on migration={} ntp={} towards state={} complete with errc={}, "
          "{}",
          mntp_state.running->work->migration_id,
          ntp,
          mntp_state.running->work->sought_state,
          ec,
          still_needed ? "and is still needed" : "but is not needed anymore");

        switch (ec) {
        case errc::shutting_down:
            mntp_state.running = std::nullopt;
            if (mntp_state.last_requested) {
                mntp_state.report_back(errc::shutting_down);
            }
            break;
        case errc::success:
            mntp_state.running = std::nullopt;
            if (still_needed) {
                mntp_state.report_back(errc::success);
            }
            break;
        default:
            // any other error is deemed retryable
            // carry on with requested work, whether same or new
            if (still_needed) {
                // don't hammer the system with the same work
                try {
                    co_await ss::sleep_abortable(
                      _cooldown_period, mntp_state.running->as);
                } catch (const ss::sleep_aborted&) {
                }
            }
            mntp_state.running = std::nullopt;
        }
    }
}
} // namespace cluster::data_migrations
