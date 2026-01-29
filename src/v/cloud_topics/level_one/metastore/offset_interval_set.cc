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
#include "cloud_topics/level_one/metastore/offset_interval_set.h"

#include "model/fundamental.h"

namespace cloud_topics::l1 {

std::ostream&
operator<<(std::ostream& o, const offset_interval_set::interval& iv) {
    fmt::print(o, "{}", iv);
    return o;
}

bool offset_interval_set::empty() const { return iset_.empty(); }

bool offset_interval_set::insert(kafka::offset base, kafka::offset last) {
    auto len = last() - base() + 1;
    return iset_.insert(iset_t::interval{base, len}).second;
}

bool offset_interval_set::contains(kafka::offset o) const {
    return iset_.find(o()) != iset_.end();
}

bool offset_interval_set::covers(kafka::offset start, kafka::offset end) const {
    auto it = iset_.find(start());
    if (it == iset_.end()) {
        return false;
    }

    return (it->first <= start && it->second > end);
}

chunked_vector<offset_interval_set::interval>
offset_interval_set::to_vec() const {
    chunked_vector<offset_interval_set::interval> ret;
    ret.reserve(iset_.size());
    auto stream = make_stream();
    while (stream.has_next()) {
        ret.emplace_back(stream.next());
    }
    return ret;
}

void offset_interval_set::truncate_with_new_start_offset(
  kafka::offset new_start_offset) {
    // First, remove all intervals that are fully below the new start.
    while (!iset_.empty()) {
        auto begin_it = iset_.begin();
        auto begin_last_offset = kafka::offset{iset_.to_end(begin_it) - 1};
        if (begin_last_offset >= new_start_offset) {
            // This interval is partially or entirely above the new start.
            // Handle below.
            break;
        }
        // This interval is entirely below the new start.
        iset_.erase(begin_it);
    }
    if (iset_.empty()) {
        return;
    }
    auto begin_it = iset_.begin();
    auto begin_base_offset = kafka::offset{iset_.to_start(begin_it)};
    if (begin_base_offset >= new_start_offset) {
        // This interval starts above or is aligned exactly with the new start.
        return;
    }
    // This interval is partially below the new start. Replace it with an
    // interval that is aligned with the new start.
    auto begin_last_offset = kafka::offset{iset_.to_end(begin_it) - 1};
    iset_.erase(begin_it);
    insert(new_start_offset, begin_last_offset);
}

} // namespace cloud_topics::l1
