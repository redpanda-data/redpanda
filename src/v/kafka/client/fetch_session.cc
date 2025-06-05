// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/fetch_session.h"

#include "container/chunked_hash_map.h"
#include "kafka/client/logger.h"
#include "kafka/client/types.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/schemata/offset_commit_request.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"

#include <fmt/ostream.h>

#include <algorithm>
#include <iostream>
#include <limits>

namespace kafka::client {

model::offset fetch_session::offset(model::topic_partition_view tpv) const {
    auto topic_it = _partitions.find(tpv.topic);
    if (topic_it == _partitions.end()) {
        return model::offset{0};
    }
    auto part_it = topic_it->second.find(tpv.partition);
    if (part_it == topic_it->second.end()) {
        return model::offset{0};
    }
    return part_it->second.recv_fetch_offset;
}

bool fetch_session::apply(fetch_response& res) {
    if (res.data.session_id == 0) {
        // TODO: consider surfacing this to the caller or maybe throw an error
        // here, to avoid fetching without fetch sessions for too long

        // No fetch session was created.
        // Reset the session; will attempt to create a new one on the next fetch
        _id = kafka::invalid_fetch_session_id;
        _epoch = kafka::initial_fetch_session_epoch;
        _next_fetch_added.clear();
        _partitions.clear();
        return false;
    }

    if (_id == invalid_fetch_session_id) {
        _id = fetch_session_id{res.data.session_id};
    }
    vassert(
      res.data.session_id == _id,
      "session mismatch: res.data.session_id={}, session={}",
      res.data.session_id,
      *this);

    // The fetch epoch is a monotonically incrementing 32-bit counter. After
    // processing request N, the broker expects to receive request N+1.
    if (std::numeric_limits<fetch_session_epoch>::max() == _epoch) {
        // The sequence number is always greater than 0.  After reaching
        // MAX_INT, it wraps around to 1.
        _epoch = fetch_session_epoch{1};
    } else {
        ++_epoch;
    }

    for (auto& [topic, partitions] : _partitions) {
        for (auto& [partition, p_state] : partitions) {
            p_state.acked_sent_fetch_offset = p_state.sent_fetch_offset;
            vlog(
              kclog.debug,
              "p_state in apply() for {}: {}",
              model::topic_partition{topic, partition},
              p_state);
        }
    }

    return true;
}

void fetch_session::fill_fetch_add_partition(
  fetch_request& req, model::topic_partition_view tpv, model::offset offset) {
    vlog(
      kclog.debug,
      "Adding partition {} at {} to next fetch request",
      model::topic_partition{tpv},
      offset());
    auto& p_state = _partitions[tpv.topic][tpv.partition];
    vlog(kclog.debug, "p_state in fill_fetch_add_partition(): {}", p_state);
    if (p_state.acked_sent_fetch_offset != offset) {
        if (
          req.data.topics.empty()
          || req.data.topics.back().topic != tpv.topic) {
            req.data.topics.push_back(fetch_request::topic{.topic{tpv.topic}});
        }
        req.data.topics.back().partitions.push_back(fetch_request::partition{
          .partition = tpv.partition,
          .fetch_offset = offset,
          // TODO: feed in _config.consumer_request_max_bytes (and maybe
          // introduce consumer_partition_max_bytes)
          .partition_max_bytes = 1048576,
        });

        p_state.sent_fetch_offset = offset;
    }
    _next_fetch_added[tpv.topic].emplace(tpv.partition);
}

void fetch_session::fill_fetch_complete(fetch_request& req) {
    for (auto& topic : _partitions) {
        for (auto& partition : topic.second) {
            if (!absl::c_contains(
                  _next_fetch_added[topic.first], partition.first)) {
                if (
                  req.data.forgotten_topics_data.empty()
                  || req.data.forgotten_topics_data.back().topic
                       != topic.first) {
                    req.data.forgotten_topics_data.push_back(
                      fetch_request::forgotten_topic{.topic{topic.first}});
                }
                req.data.forgotten_topics_data.back().partitions.push_back(
                  partition.first);
                vlog(
                  kclog.warn,
                  "Forgetting partition {} to next fetch request",
                  model::topic_partition{topic.first, partition.first});
            }
        }
    }

    for (const auto& topic : req.data.forgotten_topics_data) {
        for (const auto part_id : topic.partitions) {
            auto it = _partitions.find(topic.topic);
            vassert(
              it != _partitions.end(),
              "The topic to be forgotten must have been known beforehand");
            it->second.erase(model::partition_id{part_id});
            if (it->second.empty()) {
                _partitions.erase(it);
            }
        }
    }

    _next_fetch_added.clear();
}

std::vector<offset_commit_request_topic>
fetch_session::make_offset_commit_request() const {
    std::vector<offset_commit_request_topic> res;
    for (const auto& [t, po] : _partitions) {
        for (const auto& [p_id, o] : po) {
            if (res.empty() || res.back().name != t) {
                res.push_back(
                  offset_commit_request_topic{.name = t, .partitions{}});
            }
            res.back().partitions.push_back(offset_commit_request_partition{
              .partition_index = p_id,
              .committed_offset = o.recv_fetch_offset - model::offset(1),
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
