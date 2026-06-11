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
#include "cloud_topics/level_zero/stm/types.h"
#include "cloud_topics/types.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "raft/persisted_stm.h"
#include "ssx/future-util.h"
#include "ssx/watchdog.h"
#include "storage/snapshot.h"

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
        if (_first_epoch.has_value()) {
            _first_epoch = std::min(
              _first_epoch.value(), extract_epoch(std::move(batch)));
        } else {
            _first_epoch = extract_epoch(std::move(batch));
        }
        // Since we're accepting out of order epoch we have to read all
        // placeholders. The epochs in the partition are not strictly monotonic
        // so we need to read up until the end.
        // This code is only used for testing so it doesn't make sense to
        // optimize it.
        co_return ss::stop_iteration::no;
    }

    std::optional<cluster_epoch> end_of_stream() { return _first_epoch; }

private:
    std::optional<cluster_epoch> _first_epoch;
};
} // namespace

ctp_stm::ctp_stm(ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>(name, logger, raft)
  , _lock(ss::semaphore::max_counter()) {}

ss::future<> ctp_stm::start() {
    ssx::spawn_with_gate(_gate, [this] { return prefix_truncate_bg(); });
    return raft::persisted_stm<>::start();
}

ss::future<> ctp_stm::stop() {
    _lro_advanced.broken();
    _as.request_abort();
    _epoch_update_lock.broken();
    _epoch_updated_cv.broken();
    // We can't break the lock because that could cause UAF
    // as the units are held outside of this class.
    // however lock acquisition uses the above abort_source so
    // we should not be acquiring new waiters.
    // _lock.broken();
    co_await raft::persisted_stm<>::stop();
    static constexpr auto epoch_fence_lock_timeout = 10s;
    ssx::watchdog wd(epoch_fence_lock_timeout, [this] {
        // This is basically the number of produce requests still in flight
        auto num_read_locks_held = ss::semaphore::max_counter()
                                   - _lock.available_units();
        vlog(
          _log.debug,
          "timeout waiting for epoch fencing lock units to be returned: {} "
          "units outstanding",
          num_read_locks_held);
    });
    // Wait for all the units to be returned otherwise when the units are
    // destructed we could get a UAF.
    co_await _lock.wait(ss::semaphore::max_counter());
}

ss::future<> ctp_stm::prefix_truncate_bg() {
    static constexpr auto retry_backoff_time = 5s;
    static constexpr auto min_truncate_period = 60s;
    while (!_gate.is_closed()) {
        // Compute the truncation target once per iteration. storage.mode=cloud
        // trims aggressively (the local log only holds placeholders);
        // storage.mode=tiered_cloud honors local retention plus the compaction
        // floor and so keeps more data locally.
        const bool is_tiered = _raft->log()->config().is_tiered_cloud();
        model::offset target;
        if (is_tiered) {
            target = co_await compute_local_retention_offset();
        } else {
            // storage.mode=cloud keeps only placeholders locally; the
            // reconciled data lives in the cloud, so trim as aggressively as
            // the max collectible offset and any active readers allow.
            target = max_removable_local_log_offset();
        }
        vlog(
          _log.trace,
          "Waiting for prefix-truncate target to advance past {}, current "
          "snapshot index: {}",
          target,
          _raft->last_snapshot_index());
        try {
            if (
              _raft->last_snapshot_index() >= target && _active_readers.empty()
              && !is_tiered) {
                // Only wait without a timeout if there are no active readers
                // that could be holding us back. tiered_cloud always polls
                // because its space-management offset is set without signalling
                // _lro_advanced.
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
              "error waiting for prefix-truncate target to advance in ctp stm "
              "background loop: {}",
              std::current_exception());
        }
        auto snapshot_index = _raft->last_snapshot_index();
        vlog(
          _log.trace,
          "Attempting to snapshot ctp at {}, last snapshot at {}",
          target,
          _raft->last_snapshot_index());
        try {
            co_await _raft->snapshot_and_truncate_log(target);
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
            co_await ss::sleep_abortable<ss::lowres_clock>(
              min_truncate_period, _as);
        }
    }
}

const model::ntp& ctp_stm::ntp() const noexcept { return _raft->ntp(); }

ss::future<bool> ctp_stm::sync_in_term(
  model::timeout_clock::time_point deadline, ss::abort_source& as) {
    auto holder = _gate.hold();
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
    // If there is an active reader, it holds back the inactive_epoch.
    if (!_active_readers.empty()) {
        return _active_readers.front().inactive_epoch;
    }
    return _state.estimate_inactive_epoch();
}

ss::future<std::optional<cluster_epoch>> ctp_stm::get_inactive_epoch() {
    auto holder = _gate.hold();
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
          _state.get_max_applied_epoch());
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
        batch.for_each_record(
          [this, off = batch.header().base_offset](model::record&& r) {
              auto key = serde::from_iobuf<uint8_t>(r.release_key());
              auto cmd_key = static_cast<ctp_stm_key>(key);

              switch (cmd_key) {
              case ctp_stm_key::advance_reconciled_offset:
                  apply_advance_reconciled_offset(std::move(r));
                  return ss::stop_iteration::no;

              case ctp_stm_key::set_start_offset:
                  apply_set_start_offset(std::move(r));
                  return ss::stop_iteration::no;
              case ctp_stm_key::advance_epoch:
                  apply_advance_epoch(std::move(r), off);
                  return ss::stop_iteration::no;
              case ctp_stm_key::reset_state:
                  apply_reset_state(std::move(r));
                  return ss::stop_iteration::no;
              case ctp_stm_key::set_min_allowed_local_threshold:
                  apply_set_min_allowed_local_threshold(std::move(r));
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
    auto lrlo = cmd.last_reconciled_log_offset;
    vlog(_log.debug, "New LRO value is {}, log offset {}", lro, lrlo);
    _state.advance_last_reconciled_offset(lro, lrlo);
    _lro_advanced.signal();
}

void ctp_stm::apply_set_start_offset(model::record record) {
    auto cmd = serde::from_iobuf<set_start_offset_cmd>(record.release_value());
    vlog(_log.debug, "Setting start offset {}", cmd.new_start_offset);
    _state.set_start_offset(cmd.new_start_offset);
}

void ctp_stm::apply_advance_epoch(
  model::record record, model::offset base_offset) {
    auto cmd = serde::from_iobuf<advance_epoch_cmd>(record.release_value());
    vlog(_log.debug, "Advancing epoch: {}", cmd.new_epoch);
    _epoch_checker.check_epoch(ntp(), cmd.new_epoch, base_offset);
    _state.advance_epoch(cmd.new_epoch, base_offset);
}

void ctp_stm::apply_reset_state(model::record record) {
    auto cmd = serde::from_iobuf<reset_state_cmd>(record.release_value());
    vlog(_log.info, "Resetting ctp_stm state: {}", cmd.state);
    _state = std::move(cmd.state);
}

void ctp_stm::apply_set_min_allowed_local_threshold(model::record record) {
    auto cmd = serde::from_iobuf<set_min_allowed_local_threshold_cmd>(
      record.release_value());
    vlog(_log.debug, "Applying set_min_allowed_local_threshold: {}", cmd.value);
    _state.set_min_allowed_local_threshold(cmd.value);
    _lro_advanced.signal();
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
    _epoch_checker.check_epoch(ntp(), id.epoch, batch.header().base_offset);
    _state.advance_epoch(id.epoch, batch.header().base_offset);
    _state.record_placeholder_size(
      batch.header().base_offset,
      static_cast<uint64_t>(placeholder.size_bytes));
}

struct ctp_stm_snapshot
  : serde::
      envelope<ctp_stm_snapshot, serde::version<0>, serde::compat_version<0>> {
    ctp_stm_state state;
    epoch_window_checker checker;

    auto serde_fields() { return std::tie(state, checker); }
};

ss::future<raft::local_snapshot_applied>
ctp_stm::apply_local_snapshot(raft::stm_snapshot_header header, iobuf&& buf) {
    auto snap = serde::from_iobuf<ctp_stm_snapshot>(std::move(buf));
    _state = std::move(snap.state);
    _epoch_checker = snap.checker;
    vlog(
      _log.debug,
      "applied local snapshot to state={}, checker={}",
      _state,
      _epoch_checker);
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
ctp_stm::take_local_snapshot(ssx::semaphore_units) {
    auto buf = serde::to_iobuf(
      ctp_stm_snapshot{.state = _state, .checker = _epoch_checker});
    auto snapshot_offset = last_applied();
    vlog(
      _log.debug,
      "taking local snapshot@{} with state={}, checker={}",
      last_applied(),
      _state,
      _epoch_checker);
    co_return raft::stm_snapshot::create(0, snapshot_offset, std::move(buf));
}

ss::future<> ctp_stm::apply_raft_snapshot(const iobuf& buf) {
    // An empty snapshot is applied during recovery fast-forward: the raft
    // snapshot created when bootstrapping a pre-existing partition carries no
    // per-STM data, so state_machine_manager hands each STM an empty buffer to
    // advance over. There is no ctp_stm state to restore then -- e.g. a
    // partition recovered mid tiered->cloud migration has no L1 state (its data
    // is still in tiered storage). Keep the default (empty) state and advance,
    // mirroring log_eviction_stm's empty-snapshot handling.
    if (buf.empty()) {
        co_return;
    }
    auto snap = serde::from_iobuf<ctp_stm_snapshot>(buf.copy());
    _state = std::move(snap.state);
    _epoch_checker = snap.checker;
    vlog(
      _log.debug,
      "applied raft snapshot to state={}, checker={}",
      _state,
      _epoch_checker);
    co_return;
}

ss::future<iobuf> ctp_stm::take_raft_snapshot(model::offset snapshot_at) {
    vassert(
      last_applied() >= snapshot_at,
      "The snapshot is taken at offset {} but current insync offset is {}",
      snapshot_at,
      last_applied());
    vlog(
      _log.debug,
      "taking raft snapshot @ {} with state={}, checker={}",
      last_applied(),
      _state,
      _epoch_checker);
    co_return serde::to_iobuf(
      ctp_stm_snapshot{.state = _state, .checker = _epoch_checker});
}

ss::future<std::expected<cluster_epoch_fence, stale_cluster_epoch>>
ctp_stm::fence_epoch(cluster_epoch e, model::timeout_clock::duration timeout) {
    auto holder = _gate.hold();
    if (!co_await sync(timeout, _as)) {
        // Prevent the below log spam if we are shutting down.
        _as.check();
        if (_raft->is_leader()) {
            vlog(_log.warn, "ctp_stm::fence_epoch sync timeout");
        } else {
            vlog(_log.debug, "ctp_stm::fence_epoch sync timeout (not leader)");
        }
        // The stm sync only return false on timeout or leadership transfer.
        throw ss::timed_out_error{};
    }
    auto term = _raft->confirmed_term();
    while (true) {
        if (_state.epoch_in_window(term, e)) {
            // Case 1.1. Same epoch, need to acquire read-lock.
            // Case 1.2. This epoch is out of order. We can accept it if it lies
            //           in [previous-epoch, max-seen-epoch) range. We also need
            //           to acquire a read fence as in 1.1.
            auto unit = co_await ss::get_units(_lock, 1, _as);
            if (_state.epoch_in_window(term, e)) {
                co_return cluster_epoch_fence{
                  .unit = std::move(unit), .term = term};
            }
        } else if (_state.epoch_above_window(term, e)) {
            // Case 2. New epoch, need to acquire write-lock.
            auto epoch_update_lock = _epoch_update_lock.try_get_units();
            if (!epoch_update_lock) {
                // Someone else is updating the epoch - wait for the update and
                // then re-check.
                co_await _epoch_updated_cv.wait();
                continue;
            }

            // We're the epoch updater, get a write-lock
            auto unit = co_await ss::get_units(
              _lock, ss::semaphore::max_counter(), _as);

            std::optional<cluster_epoch_fence> epoch_fence_opt;
            if (
              _state.epoch_in_window(term, e)
              || _state.epoch_above_window(term, e)) {
                vlog(_log.debug, "Bumping max seen epoch to {}", e);
                _state.advance_max_seen_epoch(term, e);
                // Demote to reader lock after max_seen_epoch is updated.
                unit.return_units(unit.count() - 1);
                epoch_fence_opt.emplace(std::move(unit), term);
            }

            // Clear units and broadcast to any waiters on success or failure to
            // update the epoch.
            epoch_update_lock.reset();
            _epoch_updated_cv.broadcast();

            if (epoch_fence_opt.has_value()) {
                co_return std::move(epoch_fence_opt).value();
            }
        }

        // If we reach here, it means that we need to discard the batch.
        co_return std::unexpected(
          stale_cluster_epoch{
            .window_min = _state.get_previous_seen_epoch(term)
                            .or_else([this] {
                                return _state.get_previous_applied_epoch();
                            })
                            .value_or(cluster_epoch{-1}),
            .window_max = _state.get_max_seen_epoch(term)
                            .or_else(
                              [this] { return _state.get_max_applied_epoch(); })
                            .value_or(cluster_epoch{-1}),
          });
    }
}

model::offset ctp_stm::max_removable_local_log_offset() {
    // If there is an active reader, it holds back prefix truncation.
    if (!_active_readers.empty()) {
        return _active_readers.front().lrlo;
    }
    return _state.get_max_collectible_offset();
}

ss::future<model::offset> ctp_stm::compute_local_retention_offset() {
    // Local retention is computed uniformly for all cloud-topic partitions,
    // compacted or not. The storage layer folds cloud_gc + strict / non-strict
    // + local-target overrides into a single retention offset, and the
    // min_allowed_local_threshold floor (advanced by L1 compaction) raises it
    // further where L1 holds the authoritative compacted view. The local log
    // is a cache: anything below the resulting floor is redundant locally.
    //   * compact topics carry no delete retention, so retention is min() and
    //     the floor is driven by the L1 compaction threshold.
    //   * non-compacted topics leave the threshold unset, so retention is
    //     driven by the storage-layer offset.
    auto cfg = build_gc_config();
    auto off = co_await _raft->log()->compute_gc_offset(cfg);
    auto retention_target = off.value_or(model::offset::min());

    // The min allowed local threshold is a kafka-offset floor set by L1
    // compaction. Translate it to a log offset; sentinels mean the floor is
    // untranslatable (stale epoch, missing translator state) and contribute
    // nothing to the floor.
    auto min_allowed_local_threshold_log = model::offset::min();
    auto allowed_start = _state.get_min_allowed_local_threshold();
    if (allowed_start != kafka::offset::min()) {
        auto so = _raft->start_offset();
        auto kafka_so = model::offset_cast(_raft->log()->from_log_offset(so));
        if (allowed_start > kafka_so) {
            try {
                min_allowed_local_threshold_log = _raft->log()->to_log_offset(
                  kafka::offset_cast(allowed_start));
            } catch (...) {
                // This is unexpected because we're checking the prerequisite
                // before attempting to translate the offset. We're logging this
                // on ERROR level for visibility.
                vlog(
                  _log.error,
                  "[{}] min_allowed_local_threshold {} translation to log "
                  "offset "
                  "threw: {}; contributing nothing to floor. Raft log SO: {}, "
                  "log start Kafka offset: {}",
                  _raft->ntp(),
                  allowed_start,
                  std::current_exception(),
                  so,
                  kafka_so);
            }
        }
    }

    auto floor = std::max(retention_target, min_allowed_local_threshold_log);
    auto cap = max_removable_local_log_offset();
    co_return std::min(cap, floor);
}

storage::gc_config ctp_stm::build_gc_config() const {
    const auto& ntp_cfg = _raft->log()->config();
    auto retention_ms = ntp_cfg.retention_duration();
    auto retention_bytes = ntp_cfg.retention_bytes();

    model::timestamp eviction_time = retention_ms.has_value()
                                       ? model::timestamp(
                                           model::timestamp::now().value()
                                           - retention_ms->count())
                                       : model::timestamp::min();
    return storage::gc_config{eviction_time, retention_bytes};
}

l0::producer_queue& ctp_stm::producer_queue() { return _producer_queue; }

void ctp_stm::register_reader(active_reader_state* state) {
    state->inactive_epoch = _state.estimate_inactive_epoch();
    state->lrlo = _state.get_max_collectible_offset();
    _active_readers.push_back(*state);
}

void epoch_window_checker::check_epoch(
  const model::ntp& ntp, cluster_epoch epoch, model::offset offset) {
    if (offset < _latest_offset) {
        return;
    }
    if (epoch > _max_epoch) {
        _min_epoch = _max_epoch;
        _max_epoch = epoch;
        if (_min_epoch == cluster_epoch::min()) {
            _min_epoch = epoch;
        }
    }
    vassert(
      _min_epoch <= epoch && epoch <= _max_epoch,
      "[{}] epoch {} at {} is outside of sliding window [{}, {}]",
      ntp,
      epoch,
      offset,
      _min_epoch,
      _max_epoch);
    _latest_offset = offset;
}

fmt::iterator epoch_window_checker::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "window=[{}, {}], offset={}", _min_epoch, _max_epoch, _latest_offset);
}

ss::future<> create_ctp_stm_bootstrap_snapshot(
  const std::filesystem::path& work_directory, ctp_stm_seed_offsets offsets) {
    // The starting CTP STM state includes start offset
    // and LRO. The LRO has two components, log offset and kafka offset.
    // The assumption here is that the partition is re-created and the
    // offset translator state is empty. Because of that log offset is
    // equal to kafka offset.
    ctp_stm_state state;
    state.set_start_offset(offsets.start_offset);
    auto k_lro = kafka::prev_offset(offsets.next_offset);
    auto l_lro = kafka::offset_cast(k_lro);
    state.advance_last_reconciled_offset(k_lro, l_lro);

    // Create empty epoch checker
    epoch_window_checker checker;

    // Create the snapshot
    ctp_stm_snapshot snap{.state = state, .checker = checker};
    auto data = serde::to_iobuf(snap);

    // The snapshot offset should be equal to l_lro since the partition is
    // empty and starting offset is equal to LRO + 1.
    auto stm_snap = raft::stm_snapshot::create(0, l_lro, std::move(data));

    // Persist using the file-backed snapshot manager
    storage::simple_snapshot_manager snapshot_mgr(
      work_directory, ss::sstring(ctp_stm::name));

    co_await raft::file_backed_stm_snapshot::persist_local_snapshot(
      snapshot_mgr, std::move(stm_snap));
}

}; // namespace cloud_topics
