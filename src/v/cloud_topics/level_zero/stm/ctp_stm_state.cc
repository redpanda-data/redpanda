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

#include "base/vassert.h"
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

ctp_stm_state::resolved_window
ctp_stm_state::resolve_window(model::term_id term) const noexcept {
    if (!_seen.has_value() || term > _seen->term) {
        // The seen window is invisible to queries at newer terms: any epoch
        // it admitted is either already applied or died with the old
        // leadership (fence_epoch syncs before querying).
        return {
          .state = window_state::applied_only,
          .window = _applied.window(),
        };
    }
    if (_applied.max == _seen->window.max) {
        // The batch carrying the seen max has been applied: the log's epoch
        // window is frozen at the applied window until the next bump.
        return {.state = window_state::frozen, .window = _applied.window()};
    }
    return {.state = window_state::pending, .window = _seen->window};
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
    auto resolved = resolve_window(term);
    switch (resolved.state) {
    case window_state::applied_only:
    case window_state::frozen:
        // Everything in the log's own window is admissible: no in-flight
        // epoch can ratchet the window above an admitted epoch (frozen: the
        // pending max has already applied; applied_only: there is no
        // pending max).
        return resolved.window.has_value() && resolved.window->contains(epoch);
    case window_state::pending:
        // Only the window boundaries can be replicated concurrently in any
        // log order: the max is the largest epoch that can reach the log in
        // this term and can never end up below the log's epoch window; the
        // min also requires a non-empty applied state, otherwise the
        // max-seen batch may land into an empty log first and collapse the
        // log window to [max, max] above it (see
        // epoch_window_checker::check_epoch). Interior epochs have to move
        // the window min first (see epoch_moves_window).
        return epoch == resolved.window->max
               || (epoch == resolved.window->min && _applied.max.has_value());
    }
    vunreachable("invalid window_state");
}

bool ctp_stm_state::epoch_above_window(
  model::term_id term, cluster_epoch epoch) const noexcept {
    auto resolved = resolve_window(term);
    // An empty window (nothing applied, nothing seen) is below any epoch.
    return !resolved.window.has_value() || epoch > resolved.window->max;
}

bool ctp_stm_state::epoch_moves_window(
  model::term_id term, cluster_epoch epoch) const noexcept {
    if (epoch_above_window(term, epoch)) {
        // Admitted by bumping the window max.
        return true;
    }
    if (!_seen.has_value() || term != _seen->term) {
        return false;
    }
    const auto& seen = _seen->window;
    // An interior epoch is admitted by becoming the new window min. This is
    // only sound while the max-seen batch hasn't applied (a frozen window is
    // admissible via epoch_in_window and must not move) and while nothing
    // above the epoch, other than the pending max-seen epoch, has reached
    // the non-empty log.
    return seen.interior(epoch) && _applied.max.has_value()
           && *_applied.max < seen.max && epoch >= *_applied.max;
}

void ctp_stm_state::move_seen_window(
  model::term_id term, cluster_epoch epoch) noexcept {
    if (epoch_above_window(term, epoch)) {
        advance_max_seen_epoch(term, epoch);
    } else if (epoch_moves_window(term, epoch)) {
        // The interior epoch becomes the new window min: admissions below it
        // are fenced off, so its batch can't be overtaken in the log by two
        // distinct higher epochs (which would ratchet the log's epoch window
        // above it).
        _seen->window.min = epoch;
    }
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
