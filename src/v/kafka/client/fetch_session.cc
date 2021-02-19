// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/fetch_session.h"

#include "kafka/protocol/fetch.h"

#include <fmt/ostream.h>

#include <iostream>

namespace kafka::client {

model::offset fetch_session::offset(model::topic_partition_view tpv) const {
    auto topic_it = _offsets.find(tpv.topic);
    if (topic_it == _offsets.end()) {
        return model::offset{0};
    }
    auto part_it = topic_it->second.find(tpv.partition);
    if (part_it == topic_it->second.end()) {
        return model::offset{0};
    }
    return part_it->second;
}

bool fetch_session::apply(fetch_response& res) {
    if (_id == invalid_fetch_session_id) {
        _id = fetch_session_id{res.session_id};
    }

    if (res.session_id != _id) {
        vassert(false, "session mismatch: {}", *this);
        _id = invalid_fetch_session_id;
        return false;
    }
    ++_epoch;
    for (auto& part : res) {
        if (part.partition_response->has_error()) {
            continue;
        }
        const auto& topic = part.partition->name;
        const auto p_id = part.partition_response->id;
        auto& record_set = part.partition_response->record_set;
        if (!record_set || record_set->empty()) {
            continue;
        }

        _offsets[topic][p_id] = ++record_set->last_offset();
    }
    for (auto& topic : _offsets) {
        topic.second.rehash(topic.second.size());
    }
    return true;
}

std::ostream& operator<<(std::ostream& os, fetch_session const& fs) {
    fmt::print(os, "{id={}, epoch={}}", fs.id(), fs.epoch());
    return os;
}

} // namespace kafka::client
