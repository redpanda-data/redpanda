// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/fetch_session.h"

#include "kafka/protocol/fetch.h"
#include "kafka/protocol/schemata/offset_commit_request.h"

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
        _id = fetch_session_id{res.data.session_id};
    }
    vassert(res.data.session_id == _id, "session mismatch: {}", *this);

    ++_epoch;
    for (auto& part : res) {
        if (part.partition_response->error_code != error_code::none) {
            continue;
        }
        const auto& topic = part.partition->name;
        const auto p_id = part.partition_response->partition_index;
        auto& record_set = part.partition_response->records;
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

std::vector<offset_commit_request_topic>
fetch_session::make_offset_commit_request() const {
    std::vector<offset_commit_request_topic> res;
    if (_offsets.empty()) {
        return res;
    }
    res.push_back(offset_commit_request_topic{
      .name{_offsets.begin()->first}, .partitions{}});
    for (const auto& [t, po] : _offsets) {
        for (const auto& [p_id, o] : po) {
            if (res.back().name != t) {
                res.push_back(
                  offset_commit_request_topic{.name = t, .partitions{}});
            }
            res.back().partitions.push_back(offset_commit_request_partition{
              .partition_index = p_id,
              .committed_offset = o - model::offset(1),
              .committed_leader_epoch = invalid_leader_epoch});
        }
    }
    return res;
}

std::ostream& operator<<(std::ostream& os, const fetch_session& fs) {
    fmt::print(os, "{{id={}, epoch={}}}", fs.id(), fs.epoch());
    return os;
}

} // namespace kafka::client
