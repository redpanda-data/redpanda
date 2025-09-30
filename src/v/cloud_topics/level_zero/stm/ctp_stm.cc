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

#include <seastar/core/abort_source.hh>

#include <stdexcept>

namespace cloud_topics {

namespace {
cluster_epoch extract_epoch(model::record_batch&& batch) {
    vassert(
      batch.header().type == model::record_batch_type::dl_placeholder,
      "Expected batch type to be dl_placeholder, got {}",
      batch.header().type);
    iobuf value;
    batch.for_each_record([&value](model::record&& r) {
        value = std::move(r).release_value();
        return ss::stop_iteration::yes;
    });

    auto placeholder = serde::from_iobuf<dl_placeholder>(std::move(value));
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

const model::ntp& ctp_stm::ntp() const noexcept { return _raft->ntp(); }

ss::future<bool>
ctp_stm::sync_in_term(model::timeout_clock::time_point deadline) {
    auto sync_result = co_await sync(deadline - model::timeout_clock::now());
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
        auto wait_res = co_await wait_no_throw(committed_offset, deadline);
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
      std::make_optional(model::record_batch_type::dl_placeholder),
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
      batch.header().type != model::record_batch_type::dl_placeholder
      && batch.header().type != model::record_batch_type::ctp_stm_command) {
        co_return;
    }
    vlog(_log.debug, "Applying record batch: {}", batch.header());

    switch (batch.header().type) {
    case model::record_batch_type::dl_placeholder:
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
    auto placeholder = serde::from_iobuf<dl_placeholder>(std::move(value));
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

}; // namespace cloud_topics
