/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/read_replica/stm.h"

#include "cloud_topics/logger.h"
#include "model/batch_builder.h"
#include "model/record_batch_types.h"
#include "serde/async.h"
#include "serde/rw/rw.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::read_replica {

namespace {

void maybe_apply(
  model::offset o, const update_metadata_update& update, state& s) {
    auto can_apply = update.can_apply(s);
    vlog(
      cd_log.trace,
      "{} update_metadata_update at offset {}: {}",
      can_apply ? "Applying" : "Not applying",
      o,
      update);
    if (can_apply) {
        s.domain = update.domain;
        s.seqno = update.seqno;
        s.start_offset = update.start_offset;
        s.next_offset = update.next_offset;
        s.latest_term = update.latest_term;
    }
}

} // namespace

fmt::iterator state::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{domain:{}, seqno:{}, start_offset:{}, next_offset:{}, "
      "latest_term:{}}}",
      domain,
      seqno,
      start_offset,
      next_offset,
      latest_term);
}

fmt::iterator update_metadata_update::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{domain:{}, seqno:{}, start_offset:{}, next_offset:{}, "
      "latest_term:{}}}",
      domain,
      seqno,
      start_offset,
      next_offset,
      latest_term);
}

bool update_metadata_update::can_apply(const state& s) const {
    if (s.domain != l1::domain_uuid{} && s.domain != domain) {
        // Can't change the domain once set.
        return false;
    }
    if (s.seqno.has_value()) {
        // Can't regress the seqno.
        if (!seqno.has_value()) {
            return false;
        }
        if (*s.seqno > *seqno) {
            return false;
        }
    }
    if (s.seqno == seqno) {
        // If the seqnos are the same (maybe null), this implies that the LSM
        // database snapshot used to build this update is identical to the one
        // last used to update this state. Presumably that means this is a
        // no-op and we can skip this update, unless we need to set the domain
        // UUID.
        return s.domain == l1::domain_uuid{};
    }
    return true;
}

stm::stm(ss::logger& logger, raft::consensus* raft)
  : stm_base("read_replica_stm.snapshot", logger, raft) {}

ss::future<std::expected<model::term_id, stm::error>> stm::sync(
  model::timeout_clock::duration timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto sync_res = co_await ss::coroutine::as_future(
      stm_base::sync(timeout, as));
    if (sync_res.failed()) {
        auto ex = sync_res.get_exception();
        if (ssx::is_shutdown_exception(ex)) {
            co_return std::unexpected(
              error(errc::shutting_down, "Shutting down: {}", ex));
        }
        co_return std::unexpected(
          error(errc::raft_error, "Exception syncing: {}", ex));
    }

    if (!sync_res.get()) {
        co_return std::unexpected(error(errc::not_leader, "Failed to sync"));
    }
    co_return _insync_term;
}

ss::future<std::expected<model::offset, stm::error>> stm::update(
  model::term_id term, update_metadata_update update, ss::abort_source& as) {
    iobuf key_buf;
    serde::write(key_buf, update_key::update_metadata);
    iobuf value_buf;
    serde::write(value_buf, update);
    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::ct_read_replica_stm);
    builder.set_base_offset(model::offset{0});
    builder.add_record(
      {.key = std::move(key_buf), .value = std::move(value_buf)});
    auto batch = builder.build_sync();
    co_return co_await replicate_and_wait(term, std::move(batch), as);
}

ss::future<std::expected<model::offset, stm::error>> stm::replicate_and_wait(
  model::term_id term, model::record_batch batch, ss::abort_source& as) {
    auto gh = _gate.try_hold();
    if (!gh) {
        co_return std::unexpected(error(errc::shutting_down));
    }
    constexpr auto replicate_timeout = std::chrono::seconds(10);
    auto opts = raft::replicate_options(
      raft::consistency_level::quorum_ack,
      term,
      replicate_timeout,
      std::ref(as));
    opts.set_force_flush();
    const auto offsets_delta = batch.header().last_offset_delta;
    const auto res = co_await _raft->replicate(std::move(batch), opts);
    if (res.has_error()) {
        co_return std::unexpected(error(errc::raft_error));
    }
    auto last_offset = res.value().last_offset;
    auto base_offset = model::offset(last_offset() - offsets_delta);
    auto replicated_offset = res.value().last_offset;
    auto success = co_await wait_no_throw(
      replicated_offset,
      ss::lowres_clock::now() + std::chrono::seconds(30),
      as);
    if (!success || _raft->term() != term) {
        co_return std::unexpected(error(
          errc::raft_error,
          "Waiting for apply of batch [{}, {}] failed",
          base_offset,
          last_offset));
    }
    co_return base_offset;
}

ss::future<> stm::do_apply(const model::record_batch& batch) {
    if (batch.header().type != model::record_batch_type::ct_read_replica_stm) {
        co_return;
    }

    auto iter = model::record_batch_copy_iterator::create(batch);
    while (iter.has_next()) {
        auto r = iter.next();
        auto key_buf = r.release_key();
        iobuf_parser key_parser(std::move(key_buf));
        auto key = serde::read<update_key>(key_parser);

        iobuf value_buf = r.release_value();
        iobuf_parser value_parser(std::move(value_buf));

        switch (key) {
        case update_key::update_metadata: {
            auto update = serde::read<update_metadata_update>(value_parser);
            maybe_apply(batch.base_offset(), update, state_);
            break;
        }
        }
    }
}

ss::future<raft::local_snapshot_applied>
stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&& snapshot_buf) {
    auto parser = iobuf_parser(std::move(snapshot_buf));
    auto snapshot = co_await serde::read_async<stm_snapshot>(parser);
    state_ = std::move(snapshot.st);
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
stm::take_local_snapshot(ssx::semaphore_units units) {
    auto snapshot_offset = last_applied_offset();
    stm_snapshot snapshot;
    snapshot.st = state_;
    units.return_all();
    iobuf snapshot_buf;
    co_await serde::write_async(snapshot_buf, std::move(snapshot));
    co_return raft::stm_snapshot::create(
      0, snapshot_offset, std::move(snapshot_buf));
}

ss::future<> stm::apply_raft_snapshot(const iobuf& snapshot_buf) {
    auto parser = iobuf_parser(snapshot_buf.copy());
    auto snapshot = co_await serde::read_async<stm_snapshot>(parser);
    state_ = std::move(snapshot.st);
}

ss::future<iobuf> stm::take_raft_snapshot() {
    stm_snapshot snapshot;
    snapshot.st = state_;
    iobuf snapshot_buf;
    co_await serde::write_async(snapshot_buf, std::move(snapshot));
    co_return std::move(snapshot_buf);
}

bool stm_factory::is_applicable_for(const storage::ntp_config& ntp_cfg) const {
    const auto& ntp = ntp_cfg.ntp();
    return (ntp.ns == model::kafka_namespace) && ntp_cfg.cloud_topic_enabled()
           && ntp_cfg.is_read_replica_mode_enabled();
}

void stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto s = builder.create_stm<stm>(cd_log, raft);
    raft->log()->stm_hookset()->add_stm(std::move(s));
}

} // namespace cloud_topics::read_replica

fmt::appender fmt::formatter<cloud_topics::read_replica::stm::errc>::format(
  const cloud_topics::read_replica::stm::errc& e,
  fmt::format_context& ctx) const {
    using errc = cloud_topics::read_replica::stm::errc;
    switch (e) {
    case errc::not_leader:
        return formatter<string_view>::format("not_leader", ctx);
    case errc::raft_error:
        return formatter<string_view>::format("raft_error", ctx);
    case errc::shutting_down:
        return formatter<string_view>::format("shutting_down", ctx);
    }
}

fmt::appender fmt::formatter<cloud_topics::read_replica::update_key>::format(
  const cloud_topics::read_replica::update_key& k,
  fmt::format_context& ctx) const {
    using update_key = cloud_topics::read_replica::update_key;
    switch (k) {
    case update_key::update_metadata:
        return formatter<string_view>::format("update_metadata", ctx);
    }
}
