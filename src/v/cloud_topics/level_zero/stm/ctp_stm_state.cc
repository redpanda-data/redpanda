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

namespace cloud_topics {

void ctp_stm_state::advance_max_seen_epoch(
  model::term_id term, cluster_epoch epoch) noexcept {
    if (term >= _seen_window_term && epoch > _max_seen_epoch) {
        if (term > _seen_window_term) {
            // If this is a new term, reset the window.
            _previous_seen_epoch = epoch;
            _seen_window_term = term;
        } else {
            _previous_seen_epoch = _max_seen_epoch.value_or(epoch);
        }
        _max_seen_epoch = epoch;
    }
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

std::optional<cluster_epoch>
ctp_stm_state::get_previous_seen_epoch(model::term_id term) const noexcept {
    if (term > _seen_window_term) {
        return std::nullopt;
    }
    return _previous_seen_epoch;
}

bool ctp_stm_state::epoch_in_window(
  model::term_id term, cluster_epoch epoch) const noexcept {
    // If the term is newer then treat the window as unset.
    if (term > _seen_window_term) {
        auto end = _max_applied_epoch.value_or(cluster_epoch::min());
        auto begin = _previous_applied_epoch.value_or(end);
        return epoch >= begin && epoch <= end;
    }
    // NOTE: the window should move forward with _max_seen_epoch.
    // If _max_seen_epoch is greater than _max_applied_epoch then
    // the window should be [_previous_seen_epoch, _max_seen_epoch].
    // The window reflects in-flight requests. Write fence is required
    // to move it forward.
    auto end = _max_seen_epoch.value_or(
      _max_applied_epoch.value_or(cluster_epoch::min()));
    auto begin = _previous_seen_epoch.value_or(
      _previous_applied_epoch.value_or(end));
    return epoch >= begin && epoch <= end;
}

bool ctp_stm_state::epoch_above_window(
  model::term_id term, cluster_epoch epoch) const noexcept {
    // If the term changed, treat it as unset.
    if (term > _seen_window_term) {
        auto end = _max_applied_epoch.value_or(cluster_epoch::min());
        return epoch > end;
    }
    auto end = _max_seen_epoch.value_or(
      _max_applied_epoch.value_or(cluster_epoch::min()));
    return epoch > end;
}

std::optional<cluster_epoch>
ctp_stm_state::estimate_inactive_epoch() const noexcept {
    return estimate_min_epoch().transform(prev_cluster_epoch);
}

void ctp_stm_state::advance_epoch(cluster_epoch epoch, model::offset offset) {
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
    return _max_seen_epoch;
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

fmt::iterator ctp_stm_state::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{seen_window=[{}, {}], applied_window=[{}, {}], "
      "epoch_window_offset={}, min_epoch_lower_bound={}, lro={}, lrlo={}, "
      "start_offset={}}}",
      _previous_seen_epoch,
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
