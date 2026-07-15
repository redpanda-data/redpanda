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

bool fetch_session::has_offset(model::topic_partition_view tpv) const {
    auto topic_it = _offsets.find(tpv.topic);
    if (topic_it == _offsets.end()) {
        return false;
    }
    return topic_it->second.contains(tpv.partition);
}

void fetch_session::reseed(
  model::topic_partition_view tpv, model::offset new_offset) {
    _offsets[model::topic{tpv.topic}][tpv.partition] = new_offset;
}

void fetch_session::update_session_state(const fetch_response& res) {
    if (_id == invalid_fetch_session_id) {
        _id = fetch_session_id{res.data.session_id};
    }
    vassert(res.data.session_id == _id, "session mismatch: {}", *this);
    ++_epoch;
}

void fetch_session::rehash_offsets() {
    for (auto& topic : _offsets) {
        topic.second.rehash(topic.second.size());
    }
}

void fetch_session::apply(fetch_response& res) {
    update_session_state(res);

    for (auto& part : res) {
        // offset_out_of_range partitions are reseeded by the caller (which
        // already scans every partition to build the retry decision) before
        // apply() is called, so their non-none error_code just skips them
        // here without touching the reseeded offset.
        if (part.partition_response->error_code != error_code::none) {
            continue;
        }
        auto& record_set = part.partition_response->records;
        if (!record_set || record_set->empty()) {
            continue;
        }

        const auto& topic = part.partition->topic;
        const auto p_id = part.partition_response->partition_index;
        _offsets[topic][p_id] = ++record_set->last_offset();
    }
    rehash_offsets();
}

void fetch_session::discard(fetch_response& res) {
    // The records in this response were never delivered to the caller, so
    // no partition's offset may advance here: that would make the retry
    // skip them, silently losing data. Only the session bookkeeping happens
    // -- but reseed() (called by the caller before discard()) may
    // still have grown _offsets, so compact it here too.
    update_session_state(res);
    rehash_offsets();
}

std::vector<offset_commit_request_topic>
fetch_session::make_offset_commit_request() const {
    std::vector<offset_commit_request_topic> res;
    if (_offsets.empty()) {
        return res;
    }
    res.push_back(
      offset_commit_request_topic{
        .name{_offsets.begin()->first}, .partitions{}});
    for (const auto& [t, po] : _offsets) {
        for (const auto& [p_id, o] : po) {
            if (res.back().name != t) {
                res.push_back(
                  offset_commit_request_topic{.name = t, .partitions{}});
            }
            res.back().partitions.push_back(
              offset_commit_request_partition{
                .partition_index = p_id,
                .committed_offset = o - model::offset(1),
                .committed_leader_epoch = invalid_leader_epoch});
        }
    }
    return res;
}

fmt::iterator fetch_session::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{id={}, epoch={}}}", id(), epoch());
}

} // namespace kafka::client
