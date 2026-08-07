/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/ctp_stm_state.h"

#include "model/fundamental.h"
#include "utils/to_string.h"

#include <algorithm>

namespace cloud_topics {

void ctp_stm_state::advance_max_seen_epoch(
  model::term_id term, cluster_epoch epoch) noexcept {
    if (term < _seen_window_term) {
        return;
    }
    if (term > _seen_window_term) {
        // New term: drop a bump left over from a previous term. The
        // admission window is the applied window which is always current.
        _seen_window_term = term;
        _max_seen_epoch.reset();
    }
    if (has_pending_seen_bump(term)) {
        // An unresolved bump can never be overwritten: that would allow two
        // outstanding bump batches whose landing order is not constrained,
        // and only one of them can be accounted for by the admission rules.
        // The caller (fence_epoch) serializes bumps and waits for the
        // pending one to resolve, so this is a defensive no-op.
        return;
    }
    if (epoch <= _max_applied_epoch.value_or(cluster_epoch::min())) {
        return;
    }
    // The admission window is not widened here: the bump stays pending
    // until the apply loop observes a batch with this (or a higher) epoch
    // and the applied window catches up.
    _max_seen_epoch = epoch;
}

bool ctp_stm_state::has_pending_seen_bump(model::term_id term) const noexcept {
    if (term > _seen_window_term) {
        return false;
    }
    return _max_seen_epoch.has_value()
           && *_max_seen_epoch
                > _max_applied_epoch.value_or(cluster_epoch::min());
}

std::optional<kafka::offset>
ctp_stm_state::get_last_reconciled_offset() const noexcept {
    return _last_reconciled_offset;
}

std::optional<model::offset>
ctp_stm_state::get_last_reconciled_log_offset() const noexcept {
    return _last_reconciled_log_offset;
}

std::optional<cluster_epoch>
ctp_stm_state::estimate_min_epoch() const noexcept {
    return _min_epoch_lower_bound;
}

std::optional<model::offset>
ctp_stm_state::current_epoch_window_offset() const noexcept {
    return _current_epoch_window_offset;
}

std::optional<cluster_epoch>
ctp_stm_state::get_previous_applied_epoch() const noexcept {
    return _previous_applied_epoch;
}

bool ctp_stm_state::epoch_in_window(
  model::term_id term, cluster_epoch epoch) const noexcept {
    if (has_pending_seen_bump(term)) {
        // The bump outcome is ambiguous; admit only epochs that are safe
        // whether the bump batch lands or not: the current max, and the
        // pending epoch itself (its batch resolves the ambiguity when it
        // lands, so a retry of a failed bump self-heals the window).
        if (epoch == *_max_seen_epoch) {
            return true;
        }
        return _max_applied_epoch.has_value() && epoch == *_max_applied_epoch;
    }
    // No bump is pending: every epoch admitted so far has been observed by
    // the apply loop, so the applied window is backed by positional
    // evidence in the log and every epoch in it can be replicated safely
    // in any order.
    auto end = _max_applied_epoch.value_or(cluster_epoch::min());
    auto begin = _previous_applied_epoch.value_or(end);
    return epoch >= begin && epoch <= end;
}

bool ctp_stm_state::epoch_above_window(
  model::term_id term, cluster_epoch epoch) const noexcept {
    auto end = _max_applied_epoch.value_or(cluster_epoch::min());
    if (term <= _seen_window_term && _max_seen_epoch.has_value()) {
        end = std::max(end, *_max_seen_epoch);
    }
    return epoch > end;
}

std::optional<cluster_epoch>
ctp_stm_state::estimate_inactive_epoch() const noexcept {
    return estimate_min_epoch().transform(prev_cluster_epoch);
}

void ctp_stm_state::advance_epoch(cluster_epoch epoch, model::offset offset) {
    // NOTE: a pending seen-window bump resolves implicitly here: once the
    // applied window catches up with _max_seen_epoch the bump is backed by
    // a batch in the log and the ambiguity is gone.
    // Register new epoch
    if (epoch > _max_applied_epoch.value_or(cluster_epoch::min())) {
        // A new max epoch requires the sliding window of epoch values in flight
        // to be moved.
        if (!_min_epoch_lower_bound.has_value()) {
            // First epoch applied to the STM
            _min_epoch_lower_bound = epoch;
        }
        // Move the sliding window
        _previous_applied_epoch = _max_applied_epoch.value_or(epoch);
        _max_applied_epoch = epoch;
        _current_epoch_window_offset = offset;
    }
}

void ctp_stm_state::advance_last_reconciled_offset(
  kafka::offset new_last_reconciled_offset,
  model::offset new_last_reconciled_log_offset) noexcept {
    if (
      _current_epoch_window_offset.value_or(model::offset{})
      <= new_last_reconciled_log_offset) {
        // We advanced LRO past the offset at which we saw the current
        // epoch window value so we can use previous epoch as
        // the new min_applied_epoch
        _min_epoch_lower_bound = _previous_applied_epoch;
    }
    _last_reconciled_offset = std::max(
      _last_reconciled_offset.value_or(kafka::offset{}),
      new_last_reconciled_offset);
    _last_reconciled_log_offset = std::max(
      _last_reconciled_log_offset.value_or(model::offset{}),
      new_last_reconciled_log_offset);
}

std::optional<cluster_epoch>
ctp_stm_state::get_max_applied_epoch() const noexcept {
    return _max_applied_epoch;
}

std::optional<cluster_epoch>
ctp_stm_state::get_max_seen_epoch(model::term_id term) const noexcept {
    if (term > _seen_window_term) {
        return std::nullopt;
    }
    // This value is used as the epoch floor for new L0 uploads. It includes
    // a pending bump: uploads taken at the pending epoch are admissible
    // while the bump is unresolved (uploads at an interior epoch would be
    // rejected by the fence).
    if (_max_seen_epoch.has_value()) {
        return std::max(
          *_max_seen_epoch, _max_applied_epoch.value_or(*_max_seen_epoch));
    }
    return _max_applied_epoch;
}

model::offset ctp_stm_state::get_max_collectible_offset() const noexcept {
    if (_last_reconciled_log_offset.has_value()) {
        return _last_reconciled_log_offset.value();
    }
    // Truncation is impossible without LRO
    return model::offset::min();
}

void ctp_stm_state::record_placeholder_size(
  model::offset offset, uint64_t size_bytes) {
    _size_estimator.record(offset, size_bytes);
}

uint64_t ctp_stm_state::estimated_data_size() const noexcept {
    auto lro = get_last_reconciled_log_offset().value_or(model::offset{-1});
    return _size_estimator.estimated_active_bytes(lro);
}

const size_estimator& ctp_stm_state::get_size_estimator() const noexcept {
    return _size_estimator;
}

void ctp_stm_state::set_start_offset(kafka::offset new_offset) noexcept {
    if (new_offset <= _start_offset) {
        return;
    }
    _start_offset = new_offset;
}

kafka::offset ctp_stm_state::start_offset() const noexcept {
    return _start_offset;
}

void ctp_stm_state::set_min_allowed_local_threshold(
  kafka::offset offset) noexcept {
    // The min allowed local threshold is a kafka-offset floor below which L1
    // has compacted; it is monotonic non-decreasing, so values that do not
    // advance it are ignored. The unset floor is kafka::offset::min().
    _min_allowed_local_threshold = std::max(
      _min_allowed_local_threshold, offset);
}

kafka::offset ctp_stm_state::get_min_allowed_local_threshold() const noexcept {
    return _min_allowed_local_threshold;
}

fmt::iterator ctp_stm_state::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{max_seen_epoch={}, applied_window=[{}, {}], "
      "epoch_window_offset={}, min_epoch_lower_bound={}, lro={}, lrlo={}, "
      "start_offset={}}}",
      _max_seen_epoch,
      _previous_applied_epoch,
      _max_applied_epoch,
      _current_epoch_window_offset,
      _min_epoch_lower_bound,
      _last_reconciled_offset,
      _last_reconciled_log_offset,
      _start_offset);
}

} // namespace cloud_topics
