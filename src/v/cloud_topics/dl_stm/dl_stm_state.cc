/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/dl_stm/dl_stm_state.h"

#include "model/fundamental.h"

namespace experimental::cloud_topics {

bool dl_stm_state::register_overlay_cmd(const dl_overlay& o) noexcept {
    // Validate command
    if (o.base_offset == kafka::offset{}) {
        return false;
    }
    if (o.base_offset > o.last_offset) {
        return false;
    }
    if (o.size_bytes() == 0) {
        return false;
    }
    // Admit command
    _overlays.append(o);

    // Propagate last reconciled offset
    propagate_last_reconciled_offset(o.base_offset, o.last_offset);

    // Update terms
    for (auto kv : o.terms) {
        _term_map.insert(kv);
    }
    return true;
}

std::optional<dl_overlay>
dl_stm_state::lower_bound(kafka::offset o) const noexcept {
    return _overlays.lower_bound(o);
}

std::optional<kafka::offset>
dl_stm_state::get_term_last_offset(model::term_id t) const noexcept {
    auto it = _term_map.upper_bound(t);
    if (it == _term_map.end()) {
        return std::nullopt;
    }
    return kafka::prev_offset(it->second);
}

void dl_stm_state::propagate_last_reconciled_offset(
  kafka::offset base, kafka::offset last) noexcept {
    // If newly added offset range connects with the last reconciled offset
    // we need to propagate it forward. We also need to take into account that
    // the last object may not be added to the end of the offset range but
    // fill the gap inside the offset range.
    // So basically we have two cases:
    // case 1:
    // [           LRO][new object] <- new LRO
    // case 2:
    // [          LRO][new object][         ] <- new LRO
    // To handle the second case we need to scan the overlays.
    if (last < _last_reconciled_offset) {
        // The overlay replaces some other overlay (leveling or
        // compaction).
        return;
    }
    if (_last_reconciled_offset == kafka::offset{}) {
        // First overlay
        _last_reconciled_offset = last;
        return;
    }
    if (kafka::prev_offset(base) > _last_reconciled_offset) {
        // We're creating the gap between last reconciled offset
        // and the newly added object. The new overlay is not
        // connected to any other in the collection.
        return;
    }
    // Invariant: here prev_offset(base) <= LRO.
    // Handle case 1;
    _last_reconciled_offset = last;
    // Handle case 2;
    auto next = _overlays.lower_bound(kafka::next_offset(last));
    while (next.has_value()) {
        _last_reconciled_offset = next->last_offset;
        next = _overlays.lower_bound(kafka::next_offset(next->last_offset));
    }
    // TODO: similar mechanism should be used to track objects with unique
    // ownership and compacted objects
}
} // namespace experimental::cloud_topics
