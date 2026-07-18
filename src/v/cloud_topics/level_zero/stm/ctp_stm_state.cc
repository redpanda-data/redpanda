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
    if (!_seen.has_value() || term > _seen->term) {
        // A new term always resets the window. The old window may carry a
        // stale max above the new epoch (a fenced bump whose batch never
        // landed before the leadership change); it must not survive into
        // the new term or it blocks the reset and leaves the new term's
        // in-flight epochs invisible to concurrent fences.
        _seen = seen_epochs{
          .term = term,
          .window = {.min = epoch, .max = epoch},
        };
        return;
    }
    if (term == _seen->term && epoch > _seen->window.max) {
        // The previous max becomes the window min.
        _seen->window = {.min = _seen->window.max, .max = epoch};
    }
}

std::optional<epoch_window>
ctp_stm_state::applied_epochs::window() const noexcept {
    if (!max.has_value()) {
        return std::nullopt;
    }
    return epoch_window{.min = previous.value_or(*max), .max = *max};
}

void ctp_stm_state::applied_epochs::advance(
  cluster_epoch epoch, model::offset offset) noexcept {
    if (epoch <= max.value_or(cluster_epoch::min())) {
        return;
    }
    if (!min_lower_bound.has_value()) {
        // First epoch applied to the STM
        min_lower_bound = epoch;
    }
    // Move the sliding window: the old max becomes the window min.
    previous = max.value_or(epoch);
    max = epoch;
    window_offset = offset;
}

void ctp_stm_state::applied_epochs::on_lro_advanced(
  model::offset lro_log_offset) noexcept {
    if (window_offset.value_or(model::offset{}) <= lro_log_offset) {
        // The LRO advanced past the offset at which the window transitioned
        // to the current max, so everything below the previous epoch is
        // inactive.
        min_lower_bound = previous;
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
    return _applied.min_lower_bound;
}

std::optional<model::offset>
ctp_stm_state::current_epoch_window_offset() const noexcept {
    return _applied.window_offset;
}

std::optional<cluster_epoch>
ctp_stm_state::get_previous_applied_epoch() const noexcept {
    return _applied.previous;
}

std::optional<cluster_epoch>
ctp_stm_state::get_previous_seen_epoch(model::term_id term) const noexcept {
    if (!_seen.has_value() || term > _seen->term) {
        return std::nullopt;
    }
    return _seen->window.min;
}

bool ctp_stm_state::epoch_in_window(
  model::term_id term, cluster_epoch epoch) const noexcept {
    if (!_seen.has_value() || term > _seen->term) {
        // The seen window is invisible to queries at newer terms: the
        // applied window is the only evidence.
        auto applied = _applied.window();
        return applied.has_value() && applied->contains(epoch);
    }
    const auto& seen = _seen->window;
    if (!seen.contains(epoch)) {
        return false;
    }
    if (epoch == seen.max) {
        return true;
    }
    // A below-max epoch is only admissible if some epoch batch is known to
    // precede the max-seen epoch's first batch in the log. The seen window
    // alone can't prove this: a fence-time bump whose batch never lands (a
    // failed replicate) leaves a lower bound with no counterpart in the log.
    // If nothing precedes the max epoch's first batch, the log epoch window
    // collapses to [max, max] when that batch applies, and a below-max batch
    // landing after it violates the log invariant enforced by
    // epoch_window_checker and may reference L0 objects the GC already
    // considers inactive.
    //
    // Applied state gives positional evidence, since apply follows log order:
    // - applied max < seen max: an applied batch sits at a lower log
    //   position than any batch at the max-seen epoch (applied or not).
    // - applied max == seen max: the max epoch applied; a batch preceded it
    //   iff the applied window did not collapse to [max, max].
    if (!_applied.max.has_value()) {
        return false;
    }
    if (*_applied.max < seen.max) {
        return true;
    }
    return *_applied.max == seen.max
           && _applied.previous.value_or(seen.max) < seen.max;
}

bool ctp_stm_state::epoch_above_window(
  model::term_id term, cluster_epoch epoch) const noexcept {
    if (!_seen.has_value() || term > _seen->term) {
        // The seen window is invisible to queries at newer terms.
        auto applied = _applied.window();
        return !applied.has_value() || epoch > applied->max;
    }
    return epoch > _seen->window.max;
}

std::optional<cluster_epoch>
ctp_stm_state::estimate_inactive_epoch() const noexcept {
    return estimate_min_epoch().transform(prev_cluster_epoch);
}

void ctp_stm_state::advance_epoch(cluster_epoch epoch, model::offset offset) {
    _applied.advance(epoch, offset);
}

void ctp_stm_state::advance_last_reconciled_offset(
  kafka::offset new_last_reconciled_offset,
  model::offset new_last_reconciled_log_offset) noexcept {
    _applied.on_lro_advanced(new_last_reconciled_log_offset);
    _last_reconciled_offset = std::max(
      _last_reconciled_offset.value_or(kafka::offset{}),
      new_last_reconciled_offset);
    _last_reconciled_log_offset = std::max(
      _last_reconciled_log_offset.value_or(model::offset{}),
      new_last_reconciled_log_offset);
}

std::optional<cluster_epoch>
ctp_stm_state::get_max_applied_epoch() const noexcept {
    return _applied.max;
}

std::optional<cluster_epoch>
ctp_stm_state::get_max_seen_epoch(model::term_id term) const noexcept {
    if (!_seen.has_value() || term > _seen->term) {
        return std::nullopt;
    }
    return _seen->window.max;
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
    std::optional<cluster_epoch> seen_min;
    std::optional<cluster_epoch> seen_max;
    if (_seen.has_value()) {
        seen_min = _seen->window.min;
        seen_max = _seen->window.max;
    }
    return fmt::format_to(
      it,
      "{{seen_window=[{}, {}], applied_window=[{}, {}], "
      "epoch_window_offset={}, min_epoch_lower_bound={}, lro={}, lrlo={}, "
      "start_offset={}}}",
      seen_min,
      seen_max,
      _applied.previous,
      _applied.max,
      _applied.window_offset,
      _applied.min_lower_bound,
      _last_reconciled_offset,
      _last_reconciled_log_offset,
      _start_offset);
}

} // namespace cloud_topics
