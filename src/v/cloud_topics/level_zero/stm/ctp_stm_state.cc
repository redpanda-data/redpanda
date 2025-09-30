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

namespace cloud_topics {

void ctp_stm_state::advance_max_seen_epoch(cluster_epoch epoch) noexcept {
    _max_seen_epoch = std::max(
      epoch, _max_seen_epoch.value_or(cluster_epoch{}));
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

void ctp_stm_state::advance_epoch(cluster_epoch epoch, model::offset offset) {
    // The STM works on both leader and followers, on a leader the
    // max_seen_epoch epoch is updated by the fencing mechanism.
    // On the follower the max_seen_epoch epoch has to follow the max epoch.
    _max_seen_epoch = std::max(
      _max_seen_epoch.value_or(cluster_epoch{}), epoch);
    // Register new epoch
    if (_max_applied_epoch.value_or(cluster_epoch{}) != epoch) {
        _max_applied_epoch = std::max(
          epoch, _max_applied_epoch.value_or(cluster_epoch{}));
        _max_applied_epoch_offset = offset;
        if (!_min_epoch_lower_bound.has_value()) {
            // First epoch applied to the STM
            _min_epoch_lower_bound = _max_applied_epoch;
        }
    }
}

void ctp_stm_state::advance_last_reconciled_offset(
  kafka::offset new_last_reconciled_offset,
  model::offset new_last_reconciled_log_offset) noexcept {
    if (
      _max_applied_epoch_offset.value_or(model::offset{})
      <= new_last_reconciled_log_offset) {
        // We advanced LRO past the offset at which we saw the current
        // max_applied_epoch value so we can use max_applied_epoch as
        // the new min_applied_epoch
        _min_epoch_lower_bound = _max_applied_epoch;
    }
    _last_reconciled_offset = std::max(
      _last_reconciled_offset.value_or(kafka::offset{}),
      new_last_reconciled_offset);
    _last_reconciled_log_offset = std::max(
      _last_reconciled_log_offset.value_or(model::offset{}),
      new_last_reconciled_log_offset);
}

std::optional<cluster_epoch> ctp_stm_state::get_max_epoch() const noexcept {
    return _max_applied_epoch;
}

std::optional<cluster_epoch>
ctp_stm_state::get_max_seen_epoch() const noexcept {
    return _max_seen_epoch;
}

model::offset ctp_stm_state::get_max_collectible_offset() const noexcept {
    if (_last_reconciled_log_offset.has_value()) {
        return _last_reconciled_log_offset.value();
    }
    // Truncation is impossible without LRO
    return model::offset::min();
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

} // namespace cloud_topics
