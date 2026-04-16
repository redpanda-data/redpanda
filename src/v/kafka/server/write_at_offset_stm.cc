/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/write_at_offset_stm.h"

#include "cluster/snapshot.h"
#include "kafka/server/logger.h"
#include "model/batch_utils.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

#include <algorithm>

namespace kafka {

namespace {
struct local_snapshot
  : serde::
      envelope<local_snapshot, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(last_offset); }

    kafka::offset last_offset;
};

} // namespace

const char* write_at_offset_stm::errc_category::name() const noexcept {
    return "write_at_offset_error";
}

std::string write_at_offset_stm::errc_category::message(int c) const {
    switch (static_cast<errc>(c)) {
    case errc::success:
        return "Success";
    case errc::invalid_offset:
        return "Invalid write offset";
    case errc::replicate_exception:
        return "Exception thrown during replication";
    case errc::invalid_batch_type:
        return "Invalid batch type. Offset translated batches are not "
               "supported by write at offset state machine";
    case errc::invalid_input:
        return "Invalid input provided to write at offset state machine";
    case errc::not_leader:
        return "Not leader";
    case errc::invalid_truncation_offset:
        return "Invalid truncation offset";
    }
    __builtin_unreachable();
}

const std::error_category& write_at_offset_stm::error_category() noexcept {
    static errc_category e;
    return e;
}

std::error_code write_at_offset_stm::make_error_code(errc e) noexcept {
    return {static_cast<int>(e), error_category()};
}

write_at_offset_stm::write_at_offset_stm(
  raft::consensus* raft,
  ss::logger& logger,
  storage::kvstore& kvstore,
  std::vector<model::record_batch_type> offset_translated_batches)
  : raft::persisted_stm<raft::kvstore_backed_stm_snapshot>(
      cluster::write_at_offset_stm_snapshot, logger, raft, kvstore)
  , _offset_translated_batches(std::move(offset_translated_batches))
  , _sync_lock("w-at-offset") {}

raft::replicate_stages write_at_offset_stm::replicate(
  chunked_vector<model::record_batch> batches,
  chunked_vector<kafka::offset> expected_base_offsets,
  std::optional<kafka::offset> prev_log_offset,
  model::timeout_clock::duration timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    ss::promise<> enqueued_promise;
    auto f = enqueued_promise.get_future();
    return raft::replicate_stages{
      std::move(f),
      do_replicate(
        std::move(batches),
        std::move(expected_base_offsets),
        prev_log_offset,
        timeout,
        std::move(as),
        std::move(enqueued_promise))};
}

ss::future<write_at_offset_stm::errc> write_at_offset_stm::ensure_truncatable(
  kafka::offset new_start_offset,
  model::timeout_clock::duration timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    if (new_start_offset <= kafka::offset{0}) {
        co_return errc::invalid_truncation_offset;
    }
    auto holder = _gate.hold();
    // this is a rare operation, so we hold units for the whole duration
    const auto prev_insync_term = _insync_term;
    auto u = co_await _sync_lock.get_units();
    auto sync_result = co_await sync(timeout);
    if (!sync_result) {
        co_return errc::not_leader;
    }
    const auto current_insync_term = _insync_term;
    /**
     * There was a leadership change, reset inflight last offset as the stm
     * should be caught up with so we do not need a cached value.
     */
    if (prev_insync_term != current_insync_term) {
        _inflight_last_offset.reset();
    }

    const auto stm_last_offset = expected_last_offset();
    if (new_start_offset <= stm_last_offset) {
        // already truncatable
        co_return errc::success;
    }
    vlog(
      _log.debug,
      "Ensuring truncatable up to offset {}, current stm last offset is {}",
      new_start_offset,
      stm_last_offset);
    // replicate ghost batches to fill the gap
    // term will be overriden later in the raft layer.
    auto effective_last_offset = kafka::prev_offset(new_start_offset);
    auto place_holders = model::make_compaction_placeholder_batches(
      kafka::offset_cast(kafka::next_offset(stm_last_offset)),
      kafka::offset_cast(effective_last_offset),
      model::term_id{0});
    chunked_vector<model::record_batch> to_replicate{
      std::from_range, std::move(place_holders) | std::views::as_rvalue};

    _inflight_last_offset = term_offset{
      .offset = effective_last_offset, .in_sync_term = _insync_term};

    try {
        auto result = co_await _raft->replicate(
          std::move(to_replicate),
          raft::replicate_options(
            raft::consistency_level::quorum_ack,
            /*expected_term=*/_insync_term,
            /*timeout=*/std::nullopt,
            as));
        if (result.has_error()) {
            vlog(
              _log.warn,
              "Truncation replication failed with error: {}, inflight last "
              "offset: {}",
              result.error().message(),
              _inflight_last_offset);
            co_return errc::replicate_exception;
        }
        co_return errc::success;
    } catch (...) {
        vlog(
          _log.warn,
          "Replication failed with exception - {}, inflight last offset: {}",
          std::current_exception(),
          _inflight_last_offset);
    }
    co_return errc::replicate_exception;
}

ss::future<result<raft::replicate_result>> write_at_offset_stm::do_replicate(
  chunked_vector<model::record_batch> batches,
  chunked_vector<kafka::offset> expected_base_offsets,
  std::optional<kafka::offset> prev_log_offset,
  model::timeout_clock::duration timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as,
  ss::promise<> enqueued_promise) {
    if (batches.empty() || expected_base_offsets.size() != batches.size())
      [[unlikely]] {
        enqueued_promise.set_value();
        co_return make_error_code(errc::invalid_input);
    }
    // offset translated batches are not supported by write at offset state
    // machine
    auto any_offset_translated = std::ranges::any_of(
      batches, [this](const model::record_batch& batch) {
          return is_offset_translated_batch(batch);
      });
    if (any_offset_translated) [[unlikely]] {
        enqueued_promise.set_value();
        co_return make_error_code(errc::invalid_batch_type);
    }

    auto holder = _gate.hold();

    /**
     * We use lock to serialize the sync calls, as sync is a scheduling point
     * The units are released after the request is enqueued in raft.
     */
    auto u = co_await _sync_lock.get_units();
    const auto prev_insync_term = _insync_term;
    auto sync_result = co_await sync(timeout);
    if (!sync_result) {
        _inflight_last_offset.reset();
        enqueued_promise.set_value();
        co_return raft::errc::not_leader;
    }
    const auto current_insync_term = _insync_term;
    /**
     * There was a leadership change, reset inflight last offset as the stm
     * should be caught up with so we do not need a cached value.
     */
    if (prev_insync_term != current_insync_term) {
        _inflight_last_offset.reset();
    }

    const auto stm_last_offset = expected_last_offset();
    vlog(
      _log.trace,
      "Requested replicate at offset: {} with previous log offset: {}. "
      "stm last offset: {} [inflight_last_offset: {}, last_offset: {}]",
      expected_base_offsets,
      prev_log_offset,
      stm_last_offset,
      _inflight_last_offset,
      _last_offset);
    auto expected_base_offset = expected_base_offsets.front();
    const auto effective_prev_log_offset = prev_log_offset.value_or(
      kafka::prev_offset(expected_base_offset));
    /*
     * Write can only succeed if the effective_prev_log_offset ==
     * stm_last_offset.
     */
    if (effective_prev_log_offset != stm_last_offset) {
        enqueued_promise.set_value();
        vlog(
          _log.debug,
          "Expected last log offset: {} does not match with last stm"
          " tracked offset: {}",
          effective_prev_log_offset,
          stm_last_offset);
        co_return make_error_code(errc::invalid_offset);
    }

    chunked_vector<model::record_batch> effective_batches;
    effective_batches.reserve(batches.size());
    auto effective_last_offset = effective_prev_log_offset;
    // Fill in any gaps in the input batch offset space.
    kafka::offset last_seen_offset{};
    for (auto&& [expected_offset, batch] :
         std::ranges::zip_view(expected_base_offsets, batches)) {
        if (expected_offset < last_seen_offset) [[unlikely]] {
            enqueued_promise.set_value();
            vlog(
              _log.warn,
              "Expected batch offsets are not monotonically increasing: {} "
              "followed by {}",
              last_seen_offset,
              expected_offset);
            co_return make_error_code(errc::invalid_input);
        }
        last_seen_offset = expected_offset;
        auto expected_prev_offset = kafka::prev_offset(expected_offset);
        if (effective_last_offset != expected_prev_offset) {
            // fill with ghost batch.
            // The term argument is not important here, it will be
            // overriden in Raft layer
            auto ghost_batches = model::make_ghost_batches(
              kafka::offset_cast(kafka::next_offset(effective_last_offset)),
              kafka::offset_cast(expected_prev_offset),
              model::term_id{0});
            std::ranges::move(
              ghost_batches, std::back_inserter(effective_batches));
        }
        effective_batches.push_back(std::move(batch));
        effective_last_offset = kafka::offset(
          expected_offset()
          + effective_batches.back().header().last_offset_delta);
    }

    vassert(
      effective_batches.size() >= expected_base_offsets.size(),
      "Effective batches {} are fewer than input batches {}",
      effective_batches.size(),
      expected_base_offsets.size());

    _inflight_last_offset = term_offset{
      .offset = effective_last_offset, .in_sync_term = _insync_term};
    /**
     * Write at offset state machine error handling is based on the assumption
     * that if one of the replicate calls fails all the subsequent replicate
     * calls in the same term will fail. In face of an error a new leader will
     * call sync() waiting for the state from previous terms to be applied.
     *
     * Thanks to that assumption we can simply reset all inflight state instead
     * of reverting to the previous last offset.
     */
    auto stages = try_replicate_in_stages(std::move(effective_batches), as);

    auto enq = std::move(stages.request_enqueued).finally([u = std::move(u)] {
    });

    std::move(enq).forward_to(std::move(enqueued_promise));

    auto r_fut = co_await ss::coroutine::as_future(
      std::move(stages.replicate_finished));
    const bool needs_inflight_reset = inflight_last_offset_needs_reset(
      current_insync_term);
    if (r_fut.failed()) {
        auto ex = r_fut.get_exception();
        vlog(
          _log.warn,
          "Replication failed with exception: {}, needs inflight offset reset: "
          "{}, inflight last offset: {}",
          ex,
          needs_inflight_reset,
          _inflight_last_offset);
        if (needs_inflight_reset) {
            _inflight_last_offset.reset();
        }
        // stepdown method can only throw when raft is shutting down.
        co_await _raft->step_down("write_at_offset_replication_failed");
        co_return make_error_code(errc::replicate_exception);
    }

    auto result = r_fut.get();

    if (result.has_error()) {
        vlog(
          _log.warn,
          "Replication failed with an error: {}, needs inflight offset reset: "
          "{}, inflight last offset: {}",
          result.error().message(),
          needs_inflight_reset,
          _inflight_last_offset);

        if (needs_inflight_reset) {
            _inflight_last_offset.reset();
        }

        co_await _raft->step_down("write_at_offset_replication_failed");
    }

    co_return result;
}

kafka::offset write_at_offset_stm::expected_last_offset() const {
    if (!_inflight_last_offset) {
        return _last_offset;
    }

    if (_inflight_last_offset->in_sync_term == _insync_term) {
        return std::max(_last_offset, _inflight_last_offset->offset);
    }

    return _last_offset;
}

raft::replicate_stages write_at_offset_stm::try_replicate_in_stages(
  chunked_vector<model::record_batch> batches,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    try {
        return _raft->replicate_in_stages(
          std::move(batches),
          raft::replicate_options(
            raft::consistency_level::quorum_ack,
            /*expected_term=*/_insync_term,
            /*timeout=*/std::nullopt,
            as));
    } catch (...) {
        vlog(
          _log.warn,
          "Replicate in stages failed with exception - {}",
          std::current_exception());
        return {
          ss::now(),
          ssx::now<result<raft::replicate_result>>(
            make_error_code(errc::replicate_exception))};
    }
}

bool write_at_offset_stm::is_offset_translated_batch(
  const model::record_batch& batch) const {
    return std::ranges::find(_offset_translated_batches, batch.header().type)
           != _offset_translated_batches.end();
}

ss::future<iobuf> write_at_offset_stm::take_raft_snapshot(model::offset) {
    // this state machine doesn't need snapshot as its state is based on
    // snapshot metadata i.e. start offset
    co_return iobuf{};
}

ss::future<> write_at_offset_stm::apply_raft_snapshot(const iobuf&) {
    auto start_k_offset = _raft->log()->from_log_offset(_raft->start_offset());
    _last_offset = kafka::prev_offset(model::offset_cast(start_k_offset));
    vlog(_log.trace, "Applied raft snapshot, last_offset: {}", _last_offset);
    co_return;
}

ss::future<> write_at_offset_stm::do_apply(const model::record_batch& b) {
    // offset translated batches are rare
    if (is_offset_translated_batch(b)) [[unlikely]] {
        co_return;
    }

    _last_offset = model::offset_cast(
      _raft->log()->from_log_offset(b.last_offset()));

    if (_inflight_last_offset.has_value()) {
        if (
          _last_offset >= _inflight_last_offset->offset
          || b.term() > _inflight_last_offset->in_sync_term) {
            _inflight_last_offset.reset();
        }
    }
    co_return;
}
std::ostream&
operator<<(std::ostream& o, const write_at_offset_stm::term_offset& to) {
    fmt::print(
      o, "{{offset: {}, in_sync_term: {}}}", to.offset, to.in_sync_term);
    return o;
}

ss::future<raft::local_snapshot_applied>
write_at_offset_stm::apply_local_snapshot(
  raft::stm_snapshot_header, iobuf&& data) {
    _last_offset
      = serde::from_iobuf<local_snapshot>(std::move(data)).last_offset;
    co_return raft::local_snapshot_applied::yes;
}

/**
 * Called when a local snapshot is taken. Apply fiber is stalled until
 * apply_units are alive for a consistent snapshot of the state machine.
 */
ss::future<raft::stm_snapshot>
write_at_offset_stm::take_local_snapshot(ssx::semaphore_units) {
    auto data = serde::to_iobuf(local_snapshot{.last_offset = _last_offset});
    raft::stm_snapshot_header hdr{
      .version = 0,
      .snapshot_size = static_cast<int32_t>(data.size_bytes()),
      .offset = model::prev_offset(next())};

    co_return raft::stm_snapshot{.header = hdr, .data = std::move(data)};
}

ss::future<result<kafka::offset>> write_at_offset_stm::get_expected_last_offset(
  model::timeout_clock::duration sync_timeout) {
    auto u = co_await _sync_lock.get_units();
    auto sync_result = co_await sync(sync_timeout);
    if (!sync_result) {
        vlog(
          _log.info,
          "unable to retrieve expected last offset. State machine sync failed");
        co_return raft::errc::not_leader;
    }
    co_return expected_last_offset();
}

write_at_offset_stm_factory::write_at_offset_stm_factory(
  storage::kvstore& kvstore,
  std::vector<model::record_batch_type> offset_translated_batches)
  : _kvstore(kvstore)
  , _offset_translated_batches(std::move(offset_translated_batches)) {}

bool write_at_offset_stm_factory::is_applicable_for(
  const storage::ntp_config& cfg) const {
    return model::is_shadow_link_enabled(cfg.ntp());
}

void write_at_offset_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto stm = builder.create_stm<write_at_offset_stm>(
      raft, klog, _kvstore, _offset_translated_batches);
    raft->log()->stm_hookset()->add_stm(stm);
}

} // namespace kafka

auto fmt::formatter<kafka::write_at_offset_stm::errc>::format(
  const kafka::write_at_offset_stm::errc& err, fmt::format_context& ctx) const
  -> decltype(ctx.out()) {
    std::string_view msg = "unknown";
    switch (err) {
    case kafka::write_at_offset_stm::errc::success:
        msg = "success";
        break;
        ;
    case kafka::write_at_offset_stm::errc::invalid_offset:
        msg = "invalid_offset";
        break;
    case kafka::write_at_offset_stm::errc::replicate_exception:
        msg = "replicate_exception";
        break;
    case kafka::write_at_offset_stm::errc::invalid_batch_type:
        msg = "invalid_batch_type";
        break;
    case kafka::write_at_offset_stm::errc::invalid_input:
        msg = "invalid_input";
        break;
    case kafka::write_at_offset_stm::errc::not_leader:
        msg = "not_leader";
        break;
        ;
    case kafka::write_at_offset_stm::errc::invalid_truncation_offset:
        msg = "invalid_truncation_offset";
        break;
    }
    return fmt::format_to(
      ctx.out(), "kafka::write_at_offset_stm::errc::{}", msg);
}
