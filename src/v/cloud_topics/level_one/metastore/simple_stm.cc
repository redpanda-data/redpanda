/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/simple_stm.h"

#include "cloud_topics/level_one/metastore/state.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "cloud_topics/logger.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "serde/async.h"
#include "ssx/future-util.h"
#include "utils/prefix_logger.h"

#include <seastar/coroutine/as_future.hh>

#include <optional>

namespace cloud_topics::l1 {

namespace {
template<typename Res>
void maybe_log_update_error(
  prefix_logger& log,
  update_key key,
  model::offset o,
  const std::expected<Res, stm_update_error>& r) {
    if (r.has_value()) {
        return;
    }
    // NOTE: inability to update the STM is not necessarily a bug! It indicates
    // that this update's construction raced with another update that broke an
    // invariant required to apply update. Expectation is that this update's
    // caller constructs a new update and tries again if needed.
    vlog(
      log.debug,
      "L1 metastore STM {} update at offset {} didn't apply: {}",
      key,
      o,
      r.error());
}
} // namespace

simple_stm::simple_stm(
  ss::logger& logger,
  raft::consensus* raft,
  config::binding<std::chrono::seconds> snapshot_delay)
  : metastore_stm_base("l1_simple_stm.snapshot", logger, raft)
  , snapshot_delay_secs_(std::move(snapshot_delay)) {
    snapshot_timer_.set_callback([this] { write_snapshot_async(); });
}

ss::future<std::expected<model::term_id, simple_stm::errc>>
simple_stm::sync(model::timeout_clock::duration timeout) {
    auto holder = _gate.hold();
    auto sync_res = co_await ss::coroutine::as_future(
      metastore_stm_base::sync(timeout));
    if (sync_res.failed()) {
        auto eptr = sync_res.get_exception();
        auto msg = fmt::format("Exception caught while syncing: {}", eptr);
        if (ssx::is_shutdown_exception(eptr)) {
            vlog(cd_log.debug, "Ignoring shutdown error: {}", msg);
            co_return std::unexpected(errc::shutting_down);
        }
        vlog(cd_log.warn, "{}", msg);
        co_return std::unexpected(errc::raft_error);
    }

    if (!sync_res.get()) {
        co_return std::unexpected(errc::not_leader);
    }
    // At this point we're guaranteed that this node is the leader and that the
    // STM has been caught up in the current term before (this doesn't mean the
    // STM is currently caught up right now though!)
    co_return _insync_term;
}

ss::future<std::expected<void, simple_stm::errc>>
simple_stm::replicate_and_wait(
  model::term_id term, model::record_batch batch, ss::abort_source& as) {
    auto holder = _gate.hold();
    // This timeout should prevent services that depend on the metastore from
    // hanging during shutdown because they are waiting on a replicate. The OCC
    // checks in the apply fiber should prevent issues with replicates that
    // succeed after the timeout pops.
    // 10s chosen because it's a long time but less than the 15s it takes for a
    // "slow shutdown" log.
    constexpr auto replicate_timeout = 10s;
    auto opts = raft::replicate_options(
      raft::consistency_level::quorum_ack,
      /*expected_term=*/term,
      replicate_timeout,
      std::ref(as));
    opts.set_force_flush();
    auto res = co_await _raft->replicate(std::move(batch), opts);
    if (res.has_error()) {
        co_return std::unexpected(errc::raft_error);
    }
    auto replicated_offset = res.value().last_offset;
    co_await wait_no_throw(
      replicated_offset, ss::lowres_clock::now() + 30s, as);
    co_return std::expected<void, errc>{};
}

ss::future<> simple_stm::do_apply(const model::record_batch& batch) {
    if (batch.header().type != model::record_batch_type::l1_stm) {
        co_return;
    }

    auto iter = model::record_batch_copy_iterator::create(batch);
    while (iter.has_next()) {
        auto r = iter.next();
        auto key_buf = r.release_key();
        if (key_buf.size_bytes() == 0) {
            continue;
        }

        iobuf_parser key_parser(std::move(key_buf));
        auto key = serde::read<update_key>(key_parser);

        iobuf value_buf = r.release_value();
        iobuf_parser value_parser(std::move(value_buf));

        auto o = batch.base_offset() + model::offset_delta{r.offset_delta()};
        switch (key) {
        case update_key::add_objects: {
            auto update = serde::read<add_objects_update>(value_parser);
            auto result = update.apply(state_);
            maybe_log_update_error(_log, key, o, result);
            break;
        }
        case update_key::replace_objects: {
            auto update = serde::read<replace_objects_update>(value_parser);
            auto result = update.apply(state_);
            maybe_log_update_error(_log, key, o, result);
            break;
        }
        case update_key::set_start_offset: {
            auto update = serde::read<set_start_offset_update>(value_parser);
            auto result = update.apply(state_);
            maybe_log_update_error(_log, key, o, result);
            break;
        }
        case update_key::remove_objects: {
            auto update = serde::read<remove_objects_update>(value_parser);
            auto result = update.apply(state_);
            maybe_log_update_error(_log, key, o, result);
            break;
        }
        case update_key::remove_topics: {
            auto update = serde::read<remove_topics_update>(value_parser);
            auto result = update.apply(state_);
            maybe_log_update_error(_log, key, o, result);
            break;
        }
        case update_key::preregister_objects: {
            auto update = serde::read<preregister_objects_update>(value_parser);
            auto result = update.apply(state_);
            maybe_log_update_error(_log, key, o, result);
            break;
        }
        case update_key::expire_preregistered_objects: {
            auto update = serde::read<expire_preregistered_objects_update>(
              value_parser);
            auto result = update.apply(state_);
            maybe_log_update_error(_log, key, o, result);
            break;
        }
        }
    }

    rearm_snapshot_timer();
}

model::offset simple_stm::max_removable_local_log_offset() { return {}; }

ss::future<raft::local_snapshot_applied> simple_stm::apply_local_snapshot(
  raft::stm_snapshot_header, iobuf&& snapshot_buf) {
    auto parser = iobuf_parser(std::move(snapshot_buf));
    auto snapshot = co_await serde::read_async<stm_snapshot>(parser);
    state_ = std::move(snapshot.state);
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
simple_stm::take_local_snapshot(ssx::semaphore_units units) {
    auto snapshot_offset = last_applied_offset();
    auto snapshot = make_snapshot();
    units.return_all();
    iobuf snapshot_buf;
    co_await serde::write_async(snapshot_buf, std::move(snapshot));
    co_return raft::stm_snapshot::create(
      0, snapshot_offset, std::move(snapshot_buf));
}

ss::future<> simple_stm::apply_raft_snapshot(const iobuf& snapshot_buf) {
    auto parser = iobuf_parser(snapshot_buf.copy());
    auto snapshot = co_await serde::read_async<stm_snapshot>(parser);
    state_ = std::move(snapshot.state);
}

ss::future<iobuf> simple_stm::take_raft_snapshot() {
    iobuf snapshot_buf;
    co_await serde::write_async(snapshot_buf, make_snapshot());
    co_return std::move(snapshot_buf);
}

stm_snapshot simple_stm::make_snapshot() const {
    return {.state = state_.copy()};
}

ss::future<> simple_stm::stop() {
    snapshot_timer_.cancel();
    co_await metastore_stm_base::stop();
}

ss::future<> simple_stm::maybe_write_snapshot() {
    if (_raft->last_snapshot_index() >= last_applied()) {
        co_return;
    }
    auto snapshot = co_await _raft->stm_manager()->take_snapshot();
    vlog(
      _log.debug,
      "creating snapshot at offset: {}",
      snapshot.last_included_offset);
    co_await _raft->write_snapshot(
      raft::write_snapshot_cfg(
        snapshot.last_included_offset, std::move(snapshot.data)));
}

void simple_stm::write_snapshot_async() {
    ssx::background = ssx::spawn_with_gate_then(
                        _gate, [this] { return maybe_write_snapshot(); })
                        .handle_exception([this](const std::exception_ptr& e) {
                            vlog(_log.warn, "failed to write snapshot: {}", e);
                        })
                        .finally([holder = _gate.hold()] {});
}

void simple_stm::rearm_snapshot_timer() {
    if (_gate.is_closed() || snapshot_timer_.armed()) {
        return;
    }
    snapshot_timer_.arm(ss::lowres_clock::now() + snapshot_delay_secs_());
}

bool stm_factory::is_applicable_for(const storage::ntp_config& ntp_cfg) const {
    const auto& ntp = ntp_cfg.ntp();
    return (ntp.ns == model::kafka_internal_namespace)
           && (ntp.tp.topic == model::l1_metastore_topic);
}

void stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto stm = builder.create_stm<simple_stm>(
      cd_log, raft, config::mock_binding(10s));
    raft->log()->stm_hookset()->add_stm(stm);
}

} // namespace cloud_topics::l1
