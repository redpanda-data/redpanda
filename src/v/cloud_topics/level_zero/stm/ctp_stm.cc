/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/ctp_stm.h"

#include "bytes/iobuf.h"
#include "cloud_topics/level_zero/stm/ctp_stm_commands.h"
#include "cloud_topics/level_zero/stm/ctp_stm_state.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/types.h"
#include "raft/consensus.h"
#include "raft/persisted_stm.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sleep.hh>

#include <exception>
#include <stdexcept>

namespace cloud_topics {

namespace {
cluster_epoch extract_epoch(model::record_batch&& batch) {
    vassert(
      batch.header().type == model::record_batch_type::ctp_placeholder,
      "Expected batch type to be ctp_placeholder, got {}",
      batch.header().type);
    iobuf value;
    batch.for_each_record([&value](model::record&& r) {
        value = std::move(r).release_value();
        return ss::stop_iteration::yes;
    });

    auto placeholder = serde::from_iobuf<ctp_placeholder>(std::move(value));
    return placeholder.id.epoch;
}

/// Consumer used by the ctp_stm to read the minimum cluster epoch
class ctp_stm_consumer {
public:
    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        _first_epoch = extract_epoch(std::move(batch));
        co_return ss::stop_iteration::yes;
    }

    std::optional<cluster_epoch> end_of_stream() { return _first_epoch; }

private:
    std::optional<cluster_epoch> _first_epoch;
};
} // namespace

ctp_stm::ctp_stm(ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>(name, logger, raft) {}

ss::future<> ctp_stm::start() {
    ssx::spawn_with_gate(_gate, [this] { return prefix_truncate_below_lro(); });
    return raft::persisted_stm<>::start();
}

ss::future<> ctp_stm::stop() {
    _lro_advanced.broken();
    _as.request_abort();
    co_await raft::persisted_stm<>::stop();
}

ss::future<> ctp_stm::prefix_truncate_below_lro() {
    static constexpr auto retry_backoff_time = 5s;
    static constexpr auto min_truncate_period = 60s;
    while (!_gate.is_closed()) {
        vlog(
          _log.trace,
          "Waiting for LRO to advance past {}, current snapshot index: {}",
          _state.get_max_collectible_offset(),
          _raft->last_snapshot_index());
        try {
            if (
              _raft->last_snapshot_index()
              >= _state.get_max_collectible_offset()) {
                co_await _lro_advanced.wait();
            } else {
                co_await _lro_advanced.wait(retry_backoff_time);
            }
        } catch (const ss::condition_variable_timed_out& ex) {
            // Time to finish truncating
            std::ignore = ex;
        } catch (...) {
            if (ssx::is_shutdown_exception(std::current_exception())) {
                co_return;
            }
            vlog(
              _log.error,
              "error waiting for LRO to advance in ctp stm background loop: {}",
              std::current_exception());
        }
        auto lro = _state.get_max_collectible_offset();
        auto snapshot_index = _raft->last_snapshot_index();
        if (snapshot_index >= lro) {
            vlog(
              _log.trace,
              "Last snapshot index {} past LRO {}",
              _raft->last_snapshot_index(),
              _state.get_max_collectible_offset());
            continue;
        }
        auto truncate_point = _raft->log()->index_lower_bound(lro);
        if (!truncate_point) {
            vlog(
              _log.warn,
              "unable to find index lower bound for {}",
              truncate_point);
            continue;
        }
        vassert(
          truncate_point <= lro,
          "Calculated boundary {} must be <= LRO {}",
          truncate_point,
          lro);
        vlog(
          _log.trace,
          "Calulcated truncation point {} for LRO {}",
          *truncate_point,
          _state.get_last_reconciled_log_offset());
        try {
            co_await do_write_raft_snapshot(*truncate_point);
        } catch (...) {
            auto ex = std::current_exception();
            vlogl(
              _log,
              ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                             : ss::log_level::error,
              "Error occurred when attempting to write snapshot: {}",
              ex);
            continue;
        }
        // If we successfully truncated our log, then wait a bit before
        // truncating it again so if LRO is making lots of rapid but small
        // progress we aren't snapshotting too much.
        if (_raft->last_snapshot_index() > snapshot_index) {
            co_await ss::sleep_abortable(min_truncate_period, _as);
        }
    }
}

ss::future<> ctp_stm::do_write_raft_snapshot(model::offset truncation_point) {
    vlog(
      _log.trace,
      "requested to write raft snapshot (prefix_truncate) at {}",
      truncation_point);
    co_await _raft->visible_offset_monitor().wait(
      truncation_point, model::no_timeout, _as);
    co_await _raft->refresh_commit_index();
    co_await _raft->log()->stm_manager()->ensure_snapshot_exists(
      truncation_point);
    const auto max_removable_local_log_offset
      = _raft->log()->stm_manager()->max_removable_local_log_offset();
    if (truncation_point > max_removable_local_log_offset) {
        truncation_point = max_removable_local_log_offset;
        if (truncation_point <= _raft->last_snapshot_index()) {
            /// Cannot truncate, have already reached maximum allowable
            co_return;
        }
        vlog(
          _log.trace,
          "Can only evict up to offset: {}, asked to evict to: {} ",
          max_removable_local_log_offset,
          truncation_point);
    }
    if (truncation_point <= _raft->last_snapshot_index()) {
        vlog(
          _log.trace,
          "Skipping writing snapshot as Raft already progressed with the new "
          "snapshot. Current raft snapshot index: {}, requested truncation "
          "point: {}",
          _raft->last_snapshot_index(),
          truncation_point);
        co_return;
    }
    vlog(
      _log.debug,
      "Requesting raft snapshot with final offset: {}",
      truncation_point);
    auto snapshot_result = co_await _raft->stm_manager()->take_snapshot(
      truncation_point);
    // we need to check snapshot index again as it may already progressed after
    // snapshot is taken by stm_manager
    if (truncation_point <= _raft->last_snapshot_index()) {
        vlog(
          _log.trace,
          "Skipping writing snapshot as Raft already progressed with the new "
          "snapshot. Current raft snapshot index: {}, requested truncation "
          "point: {}",
          _raft->last_snapshot_index(),
          truncation_point);
        co_return;
    }
    co_await _raft->write_snapshot(
      raft::write_snapshot_cfg(
        snapshot_result.last_included_offset, std::move(snapshot_result.data)));
}

const model::ntp& ctp_stm::ntp() const noexcept { return _raft->ntp(); }

ss::future<bool> ctp_stm::sync_in_term(
  model::timeout_clock::time_point deadline, ss::abort_source& as) {
    auto sync_result = co_await sync(
      deadline - model::timeout_clock::now(), as);
    if (!sync_result) {
        // The replica is not a leader
        vlog(_log.debug, "Not a leader");
        co_return false;
    }
    // Here it's guaranteed that all commands from the previous term are
    // applied to the in-memory state. The method could be called in
    // the middle of the term.
    auto committed_offset = _raft->committed_offset();
    if (committed_offset > last_applied()) {
        // The STM is catching up.
        auto wait_res = co_await wait_no_throw(committed_offset, deadline, as);
        if (!wait_res) {
            vlog(
              _log.warn,
              "Failed to wait for committed offset {} in term {}",
              committed_offset,
              _raft->term());
            co_return false;
        }
    }
    // NOTE: there could be an in-flight replication that will be committed
    // after the wait_no_throw() and the state update will be scheduled right
    // after this method returns. This is fine because the state can tolerate
    // races.
    co_return true;
}

std::optional<cluster_epoch> ctp_stm::estimate_inactive_epoch() const noexcept {
    return _state.estimate_min_epoch().transform(prev_cluster_epoch);
}

ss::future<std::optional<cluster_epoch>> ctp_stm::get_inactive_epoch() {
    // Consume the first epoch from the partition starting from
    // start offset if nothing was reconciled yet or from the last
    // reconciled offset + 1 otherwise.
    auto so = _raft->start_offset();
    auto co = _raft->committed_offset();

    // NOTE: it's enough to read log starting from the start offset
    // for correctness. However, the local retention could be updated
    // with some arbitrary lag. In order to avoid holding data for too
    // long we're trying to read starting from the LRO.
    auto lro = _state.get_last_reconciled_log_offset().value_or(
      model::prev_offset(so));

    // NOTE: we can't make a decision to skip the log read based on offsets
    // because the LRO contains the translated LRO value. But in order for the
    // LRO to be applied the command should be replicated first. So naturally
    // this command will be the last in the log and this will make the committed
    // offset to be greater than the LRO log translated offset.

    storage::local_log_reader_config cfg(
      model::next_offset(lro),
      co,
      4_MiB,
      std::make_optional(model::record_batch_type::ctp_placeholder),
      std::nullopt,
      std::nullopt);

    auto reader = co_await _raft->make_reader(cfg);
    auto result = co_await std::move(reader).consume(
      ctp_stm_consumer{}, model::no_timeout);
    if (result.has_value()) {
        auto epoch = result.value();
        auto inactive = prev_cluster_epoch(epoch);
        vlog(
          _log.debug,
          "Minimum epoch referenced by the {} is {}, inactive epoch is {}",
          _raft->ntp(),
          epoch,
          inactive);
        // If the first epoch is the epoch zero then we can't really use
        // the inactive epoch here because cluster_epoch::min() doesn't exists
        // (no object could be created with such epoch).
        co_return inactive == cluster_epoch::min()
          ? std::nullopt
          : std::make_optional(inactive);
    } else {
        // This could naturally happen if the partition is empty because
        // everything was reconciled.
        vlog(
          _log.debug,
          "No epochs found in partition {}, max epoch {}, returning nullopt",
          _raft->ntp(),
          _state.get_max_epoch());
        co_return std::nullopt;
    }
}

ss::future<> ctp_stm::do_apply(const model::record_batch& batch) {
    if (
      batch.header().type != model::record_batch_type::ctp_placeholder
      && batch.header().type != model::record_batch_type::ctp_stm_command) {
        co_return;
    }
    vlog(_log.debug, "Applying record batch: {}", batch.header());

    switch (batch.header().type) {
    case model::record_batch_type::ctp_placeholder:
        apply_placeholder(batch);
        break;

    case model::record_batch_type::ctp_stm_command:
        // Decode the command and apply it to the state.
        batch.for_each_record([this](model::record&& r) {
            auto key = serde::from_iobuf<uint8_t>(r.release_key());
            auto cmd_key = static_cast<ctp_stm_key>(key);

            switch (cmd_key) {
            case ctp_stm_key::advance_reconciled_offset:
                apply_advance_reconciled_offset(std::move(r));
                return ss::stop_iteration::no;

            case ctp_stm_key::set_start_offset:
                apply_set_start_offset(std::move(r));
                return ss::stop_iteration::no;
            }
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Unknown ctp_stm_key({})", static_cast<int>(key)));
        });
        break;

    default:
        break;
    }
}

void ctp_stm::apply_advance_reconciled_offset(model::record record) {
    auto cmd = serde::from_iobuf<advance_reconciled_offset_cmd>(
      record.release_value());
    auto lro = cmd.last_reconciled_offset;
    vlog(_log.debug, "New LRO value is {}", lro);
    // LRO is expected to be within the translation range
    auto lro_log = _raft->log()->to_log_offset(kafka::offset_cast(lro));
    _state.advance_last_reconciled_offset(lro, lro_log);
    _lro_advanced.signal();
}

void ctp_stm::apply_set_start_offset(model::record record) {
    auto cmd = serde::from_iobuf<set_start_offset_cmd>(record.release_value());
    vlog(_log.debug, "Setting start offset {}", cmd.new_start_offset);
    _state.set_start_offset(cmd.new_start_offset);
}

void ctp_stm::apply_placeholder(const model::record_batch& batch) {
    vassert(
      batch.record_count() > 0, "Record batch must have at least one record");
    iobuf value;
    batch.for_each_record([&value](model::record&& r) {
        value = std::move(r).release_value();
        return ss::stop_iteration::yes;
    });
    auto placeholder = serde::from_iobuf<ctp_placeholder>(std::move(value));
    auto id = placeholder.id;
    // this assertion is made here rather than inside the state object itself
    // because the assertion is about the physical content of the log rather
    // than the computed state.
    vassert(
      id.epoch >= _last_seen_epoch,
      "Observed a non-monotonic epoch sequence {} < {}",
      id.epoch,
      _last_seen_epoch);
    _last_seen_epoch = id.epoch;
    _state.advance_epoch(id.epoch, batch.header().base_offset);
}

ss::future<raft::local_snapshot_applied>
ctp_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&& buf) {
    _state = serde::from_iobuf<ctp_stm_state>(std::move(buf));
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
ctp_stm::take_local_snapshot(ssx::semaphore_units) {
    auto buf = serde::to_iobuf(_state);
    co_return raft::stm_snapshot::create(
      0, this->last_applied(), std::move(buf));
}

ss::future<> ctp_stm::apply_raft_snapshot(const iobuf& buf) {
    _state = serde::from_iobuf<ctp_stm_state>(buf.copy());
    co_return;
}

ss::future<iobuf> ctp_stm::take_raft_snapshot(model::offset snapshot_at) {
    vassert(
      last_applied() >= snapshot_at,
      "The snapshot is taken at offset {} but current insync offset is {}",
      snapshot_at,
      last_applied());
    co_return serde::to_iobuf(_state);
}

ss::future<cluster_epoch_fence> ctp_stm::fence_epoch(cluster_epoch e) {
    auto term = _raft->confirmed_term();
    auto max_seen_epoch = _state.get_max_seen_epoch();
    if (max_seen_epoch.has_value() && max_seen_epoch.value() == e) {
        // Case 1. Same epoch, need to acquire read-lock.
        auto unit = co_await _lock.hold_read_lock();
        // Invariant: the max_seen_epoch is not nullopt because once
        // set the max_seen_epoch is never resets.
        if (_state.get_max_seen_epoch() == e) {
            // The max_seen_epoch didn't advance after the scheduling point
            co_return cluster_epoch_fence{std::move(unit), term};
        }
    } else {
        // Case 2. New epoch, need to acquire write-lock.
        auto unit = co_await _lock.hold_write_lock();
        auto current_epoch = _state.get_max_seen_epoch();
        if (!current_epoch.has_value() || current_epoch.value() <= e) {
            _state.advance_max_seen_epoch(e);
            // Demote to reader lock after max_seen_epoch is updated.
            unit.return_units(unit.count() - 1);
            co_return cluster_epoch_fence{std::move(unit), term};
        }
    }
    // If we reach here, it means that we need to discard the batch.
    co_return cluster_epoch_fence{};
}

model::offset ctp_stm::max_removable_local_log_offset() {
    return _state.get_max_collectible_offset();
}
}; // namespace cloud_topics
