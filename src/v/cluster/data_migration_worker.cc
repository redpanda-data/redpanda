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
#include "cluster_utils.h"
#include "errc.h"
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

#include <fmt/ostream.h>

#include <chrono>
#include <optional>
#include <tuple>

namespace cluster::data_migrations {

worker::worker(
  model::node_id self,
  partition_leaders_table& leaders_table,
  partition_manager& partition_manager,
  ss::abort_source& as)
  : _self(self)
  , _leaders_table(leaders_table)
  , _partition_manager(partition_manager)
  , _as(as)
  , _operation_timeout(5s) {}

ss::future<> worker::stop() {
    while (!_managed_ntps.empty()) {
        unmanage_ntp(_managed_ntps.end() - 1, errc::shutting_down);
    }
    if (!_gate.is_closed()) {
        co_await _gate.close();
    }
    vlog(dm_log.debug, "worker stopped");
}

ss::future<errc>
worker::perform_partition_work(model::ntp&& ntp, partition_work&& work) {
    auto it = _managed_ntps.find(ntp);
    if (it == _managed_ntps.end()) {
        // not managed yet
        bool is_leader = _self == _leaders_table.get_leader(ntp);
        auto leadership_subscription
          = _leaders_table.register_leadership_change_notification(
            ntp,
            [this](
              const model::ntp& ntp, model::term_id, model::node_id leader) {
                handle_leadership_update(ntp, _self == leader);
            });
        std::tie(it, std::ignore) = _managed_ntps.emplace(
          std::piecewise_construct,
          std::forward_as_tuple(std::move(ntp)),
          std::forward_as_tuple(
            is_leader, std::move(work), leadership_subscription));
    } else {
        // some stale work going on, kick it out and reuse its entry
        auto& ntp_state = it->second;
        ntp_state.promise->set_value(errc::invalid_data_migration_state);
        ntp_state.promise = ss::make_lw_shared<ss::promise<errc>>();
        ntp_state.is_running = false;
        ntp_state.work = std::move(work);
        ntp_state.as->request_abort();
        ntp_state.as = ss::make_lw_shared<ss::abort_source>();
    }

    spawn_work_if_leader(it);
    return it->second.promise->get_future();
}

void worker::abort_partition_work(
  model::ntp&& ntp, id migration_id, state sought_state) {
    auto it = std::as_const(_managed_ntps).find(ntp);
    if (
      it != _managed_ntps.cend() && it->second.work.migration_id == migration_id
      && it->second.work.sought_state == sought_state) {
        unmanage_ntp(it, errc::invalid_data_migration_state);
    }
}

worker::ntp_state::ntp_state(
  bool is_leader,
  partition_work&& work,
  notification_id_type leadership_subscription)
  : is_leader(is_leader)
  , work(std::move(work))
  , leadership_subscription(leadership_subscription)
  , as(ss::make_lw_shared<ss::abort_source>()) {}

ss::future<> worker::handle_operation_result(
  model::ntp ntp, id migration_id, state sought_state, errc ec) {
    vlog(
      dm_log.trace,
      "work on migration {} ntp {} towards state {} complete with errc {}",
      migration_id,
      ntp,
      sought_state,
      ec);
    if (ec != errc::success && ec != errc::shutting_down) {
        // any other result deemed retryable. We leave is_running flag in place
        // while waiting.

        // todo: configure sleep time, make it abortable from
        // worker::abort_partition_work
        auto it = _managed_ntps.find(ntp);
        if (
          it == _managed_ntps.end()
          || it->second.work.migration_id != migration_id
          || it->second.work.sought_state != sought_state) {
            vlog(
              dm_log.debug,
              "as part of migration {}, partition work for moving ntp {} to "
              "state {} is done with result {}, but not needed anymore",
              migration_id,
              std::move(ntp),
              sought_state,
              ec);
            co_return;
        }
        co_await ss::sleep_abortable(1s, *it->second.as);
    }
    bool should_retry = ec != errc::success && ec != errc::shutting_down;
    auto it = _managed_ntps.find(ntp);
    if (
      it == _managed_ntps.end() || it->second.work.migration_id != migration_id
      || it->second.work.sought_state != sought_state) {
        vlog(
          dm_log.debug,
          "as part of migration {}, partition work for moving ntp {} to state "
          "{} was about to {}, but not needed anymore",
          migration_id,
          std::move(ntp),
          sought_state,
          should_retry ? "retry" : "complete");
        co_return;
    }
    if (should_retry) {
        it->second.is_running = false;
        vlog(
          dm_log.info,
          "as part of migration {}, partition work for moving ntp {} to state "
          "{} returned {}, retrying",
          migration_id,
          std::move(ntp),
          sought_state,
          ec);
        spawn_work_if_leader(it);
        co_return;
    }
    unmanage_ntp(it, ec);
}

void worker::handle_leadership_update(const model::ntp& ntp, bool is_leader) {
    auto it = _managed_ntps.find(ntp);
    vlog(
      dm_log.info,
      "got leadership update regarding ntp={}, is_leader={}",
      ntp,
      is_leader);
    if (it == _managed_ntps.end() || it->second.is_leader == is_leader) {
        return;
    }
    it->second.is_leader = is_leader;
    if (!it->second.is_running) {
        spawn_work_if_leader(it);
    }
}

void worker::unmanage_ntp(managed_ntp_cit it, errc result) {
    _leaders_table.unregister_leadership_change_notification(
      it->second.leadership_subscription);
    it->second.promise->set_value(result);
    it->second.as->request_abort();
    _managed_ntps.erase(it);
}

ss::future<errc> worker::do_work(managed_ntp_cit it) noexcept {
    auto migration_id = it->second.work.migration_id;
    const auto& ntp = it->first;
    auto sought_state = it->second.work.sought_state;
    try {
        vlog(
          dm_log.trace,
          "starting work on migration {} ntp {} towards state {}",
          migration_id,
          ntp,
          sought_state);
        co_return co_await std::visit(
          [this, &ntp, sought_state](auto& info) {
              return do_work(ntp, sought_state, info);
          },
          it->second.work.info);
    } catch (...) {
        vlog(
          dm_log.warn,
          "exception occured during partition work on migration {} ntp {} "
          "towards {} state: {}",
          migration_id,
          ntp,
          sought_state,
          std::current_exception());
        co_return errc::partition_operation_failed;
    }
}

ss::future<errc> worker::do_work(
  const model::ntp& ntp,
  state sought_state,
  const inbound_partition_work_info&) {
    vassert(
      false,
      "inbound partition work requested on {} towards {} state",
      ntp,
      sought_state);
    return ssx::now(errc::success);
}

ss::future<errc> worker::do_work(
  const model::ntp& ntp,
  state sought_state,
  const outbound_partition_work_info&) {
    auto partition = _partition_manager.get(ntp);
    if (!partition) {
        co_return errc::partition_not_exists;
    }

    switch (sought_state) {
    case state::prepared:
        co_return co_await flush(partition);
    case state::executed: {
        auto block_res = co_await block(partition, true);
        if (block_res != errc::success) {
            co_return block_res;
        }
        co_return co_await flush(partition);
    }
    case state::cancelled:
        co_return co_await block(partition, false);
    default:
        vassert(
          false,
          "outbound partition work requested on {} towards {} state",
          ntp,
          sought_state);
    }
}

ss::future<errc>
worker::block(ss::lw_shared_ptr<partition> partition, bool block) {
    auto res = co_await partition->set_writes_disabled(
      partition_properties_stm::writes_disabled{block},
      model::timeout_clock::now() + 5s);
    co_return map_update_interruption_error_code(res);
}

ss::future<errc> worker::flush(ss::lw_shared_ptr<partition> partition) {
    // todo: check ntp_config cloud storage writes enabled?
    auto maybe_archiver = partition->archiver();
    if (!maybe_archiver) {
        co_return errc::invalid_partition_operation;
    }
    auto& archiver = maybe_archiver->get();
    auto flush_res = archiver.flush();
    if (flush_res.response != archival::flush_response::accepted) {
        co_return errc::partition_operation_failed;
    }
    switch (co_await archiver.wait(*flush_res.offset)) {
    case archival::wait_result::not_in_progress:
        // is partition concurrently flushed/waited by smth else?
        vassert(false, "Freshly accepted flush cannot be waited for");
    case archival::wait_result::lost_leadership:
        co_return errc::leadership_changed;
    case archival::wait_result::failed:
        co_return errc::partition_operation_failed;
    case archival::wait_result::complete:
        co_return errc::success;
    }
}

void worker::spawn_work_if_leader(managed_ntp_it it) {
    vassert(!it->second.is_running, "work already running");
    vlog(
      dm_log.info,
      "attempting to spawn work for ntp={}, is_leader={}",
      it->first,
      it->second.is_leader);
    if (!it->second.is_leader) {
        return;
    }
    it->second.is_running = true;
    // this call must only tinker with `it` within the current seastar task,
    // it may be invalidated later!
    ssx::spawn_with_gate(_gate, [this, it]() {
        return do_work(it).then([ntp = it->first,
                                 migration_id = it->second.work.migration_id,
                                 sought_state = it->second.work.sought_state,
                                 this](errc ec) mutable {
            return handle_operation_result(
              std::move(ntp), migration_id, sought_state, ec);
        });
    });
}

} // namespace cluster::data_migrations
