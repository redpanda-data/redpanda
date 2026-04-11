/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/lsm/stm.h"

#include "cloud_topics/level_one/metastore/lsm/lsm_update.h"
#include "cloud_topics/level_one/metastore/lsm/state.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
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
  lsm_update_key key,
  model::offset o,
  const std::expected<Res, lsm_update_error>& r) {
    if (r.has_value()) {
        return;
    }
    // NOTE: inability to update the STM is not necessarily a bug! It indicates
    // that this update's construction raced with another update that broke an
    // invariant required to apply update. Expectation is that this update's
    // caller constructs a new update and tries again if needed.
    vlog(
      log.debug,
      "L1 LSM STM {} update at offset {} didn't apply: {}",
      key,
      o,
      r.error());
}

std::string_view to_string_view(stm::errc e) {
    switch (e) {
    case stm::errc::not_leader:
        return "stm::errc::not_leader";
    case stm::errc::raft_error:
        return "stm::errc::raft_error";
    case stm::errc::shutting_down:
        return "stm::errc::shutting_down";
    }
}
} // namespace

stm::stm(
  ss::logger& logger,
  raft::consensus* raft,
  config::binding<std::chrono::seconds> snapshot_delay)
  : metastore_stm_base("l1_lsm_stm.snapshot", logger, raft)
  , snapshot_delay_secs_(std::move(snapshot_delay)) {
    snapshot_timer_.set_callback([this] { write_snapshot_async(); });
}

ss::future<std::expected<model::term_id, stm::errc>> stm::sync(
  model::timeout_clock::duration timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto sync_res = co_await ss::coroutine::as_future(
      metastore_stm_base::sync(timeout, as));
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
    // STM has been caught up in the term before this one (this doesn't mean
    // the STM is currently caught up right now though, e.g. if there are
    // in-flight updates from this term!)
    co_return _insync_term;
}

ss::future<std::expected<model::offset, stm::errc>> stm::replicate_and_wait(
  model::term_id term, model::record_batch batch, ss::abort_source& as) {
    auto gh = _gate.try_hold();
    if (!gh) {
        co_return std::unexpected(errc::shutting_down);
    }
    auto timeout = config::shard_local_cfg()
                     .cloud_topics_metastore_replication_timeout_ms();
    auto opts = raft::replicate_options(
      raft::consistency_level::quorum_ack, term, timeout, std::ref(as));
    opts.set_force_flush();
    const auto offsets_delta = batch.header().last_offset_delta;
    const auto res = co_await _raft->replicate(std::move(batch), opts);
    if (res.has_error()) {
        co_return std::unexpected(errc::raft_error);
    }
    auto last_offset = res.value().last_offset;
    auto base_offset = model::offset(last_offset() - offsets_delta);
    auto replicated_offset = res.value().last_offset;
    auto success = co_await wait_no_throw(
      replicated_offset, ss::lowres_clock::now() + timeout, as);
    if (!success || _raft->term() != term) {
        vlog(
          cd_log.debug,
          "Waiting for apply of batch [{}, {}] failed",
          base_offset,
          last_offset);
        co_return std::unexpected(errc::raft_error);
    }
    co_return base_offset;
}

ss::future<> stm::do_apply(const model::record_batch& batch) {
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
        auto key = serde::read<lsm_update_key>(key_parser);

        iobuf value_buf = r.release_value();
        iobuf_parser value_parser(std::move(value_buf));

        auto o = batch.base_offset() + model::offset_delta{r.offset_delta()};
        switch (key) {
        case lsm_update_key::apply_write_batch: {
            auto update = serde::read<apply_write_batch_update>(value_parser);
            auto result = update.apply(state_, o);
            maybe_log_update_error(_log, key, o, result);
            break;
        }
        case lsm_update_key::persist_manifest: {
            auto update = co_await serde::read_async<persist_manifest_update>(
              value_parser);
            auto result = update.apply(state_);
            maybe_log_update_error(_log, key, o, result);
            break;
        }
        case lsm_update_key::set_domain_uuid: {
            auto update = serde::read<set_domain_uuid_update>(value_parser);
            auto result = update.apply(state_);
            maybe_log_update_error(_log, key, o, result);
            break;
        }
        case lsm_update_key::reset_manifest: {
            auto update = co_await serde::read_async<reset_manifest_update>(
              value_parser);
            auto result = update.apply(state_, batch.term(), o);
            maybe_log_update_error(_log, key, o, result);
            break;
        }
        }
    }

    rearm_snapshot_timer();
}

model::offset stm::max_removable_local_log_offset() { return {}; }

ss::future<raft::local_snapshot_applied>
stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&& snapshot_buf) {
    auto parser = iobuf_parser(std::move(snapshot_buf));
    auto stm_snapshot = co_await serde::read_async<lsm_stm_snapshot>(parser);
    state_ = std::move(stm_snapshot.state);
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
stm::take_local_snapshot(ssx::semaphore_units units) {
    auto snapshot_offset = last_applied_offset();
    auto snapshot = co_await make_snapshot();
    units.return_all();
    iobuf snapshot_buf;
    co_await serde::write_async(snapshot_buf, std::move(snapshot));
    co_return raft::stm_snapshot::create(
      0, snapshot_offset, std::move(snapshot_buf));
}

ss::future<> stm::apply_raft_snapshot(const iobuf& snapshot_buf) {
    auto parser = iobuf_parser(snapshot_buf.copy());
    auto stm_snapshot = co_await serde::read_async<lsm_stm_snapshot>(parser);
    state_ = std::move(stm_snapshot.state);
}

ss::future<iobuf> stm::take_raft_snapshot() {
    iobuf snapshot_buf;
    co_await serde::write_async(snapshot_buf, co_await make_snapshot());
    co_return std::move(snapshot_buf);
}

ss::future<lsm_stm_snapshot> stm::make_snapshot() {
    lsm_stm_snapshot snapshot;
    snapshot.state = state_.share();
    co_return snapshot;
}

ss::future<> stm::stop() {
    snapshot_timer_.cancel();
    co_await metastore_stm_base::stop();
}

ss::future<> stm::maybe_write_snapshot() {
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

void stm::write_snapshot_async() {
    ssx::background = ssx::spawn_with_gate_then(
                        _gate, [this] { return maybe_write_snapshot(); })
                        .handle_exception([this](const std::exception_ptr& e) {
                            vlog(_log.warn, "failed to write snapshot: {}", e);
                        })
                        .finally([holder = _gate.hold()] {});
}

void stm::rearm_snapshot_timer() {
    if (_gate.is_closed() || snapshot_timer_.armed()) {
        return;
    }
    snapshot_timer_.arm(ss::lowres_clock::now() + snapshot_delay_secs_());
}

bool lsm_stm_factory::is_applicable_for(
  const storage::ntp_config& ntp_cfg) const {
    const auto& ntp = ntp_cfg.ntp();
    return (ntp.ns == model::kafka_internal_namespace)
           && (ntp.tp.topic == model::l1_metastore_topic);
}

void lsm_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto s = builder.create_stm<stm>(cd_log, raft, config::mock_binding(10s));
    raft->log()->stm_hookset()->add_stm(std::move(s));
}

std::ostream& operator<<(std::ostream& os, stm::errc e) {
    return os << to_string_view(e);
}

} // namespace cloud_topics::l1

auto format_as(cloud_topics::l1::stm::errc e) {
    return cloud_topics::l1::to_string_view(e);
}
