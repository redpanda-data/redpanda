/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "json/writer.h"
#include "kafka/protocol/offset_fetch.h"

#include <seastar/core/sstring.hh>

namespace pandaproxy::json {

inline std::vector<kafka::offset_fetch_request_topic>
partitions_request_to_offset_request(std::vector<model::topic_partition> tps) {
    std::vector<kafka::offset_fetch_request_topic> res;
    if (tps.empty()) {
        return res;
    }

    std::sort(tps.begin(), tps.end());
    res.push_back(kafka::offset_fetch_request_topic{tps.front().topic, {}});
    for (auto& tp : tps) {
        if (tp.topic != res.back().name) {
            res.push_back(
              kafka::offset_fetch_request_topic{std::move(tp.topic), {}});
        }
        res.back().partition_indexes.push_back(tp.partition);
    }
    return res;
}
} // namespace pandaproxy::json

namespace kafka {

template<typename Buffer>
void rjson_serialize(
  ::json::Writer<Buffer>& w, const kafka::offset_fetch_response_topic& v) {
    for (const auto& p : v.partitions) {
        w.StartObject();
        w.Key("topic");
        w.String(v.name());
        w.Key("partition");
        w.Int(p.partition_index);
        w.Key("offset");
        w.Int(p.committed_offset);
        w.Key("metadata");
        w.String(p.metadata.value_or(""));
        w.EndObject();
    }
}

template<typename Buffer>
void rjson_serialize(
  ::json::Writer<Buffer>& w, const kafka::offset_fetch_response& v) {
    w.StartObject();
    w.Key("offsets");
    w.StartArray();
    for (const auto& t : v.data.topics) {
        rjson_serialize(w, t);
    }
    w.EndArray();
    w.EndObject();
}

} // namespace kafka
