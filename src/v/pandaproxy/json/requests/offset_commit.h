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
#include "kafka/protocol/errors.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/schemata/offset_commit_request.h"
#include "pandaproxy/json/requests/partition_offsets.h"

#include <seastar/core/sstring.hh>

namespace pandaproxy::json {

inline std::vector<kafka::offset_commit_request_topic>
partition_offsets_request_to_offset_commit_request(
  std::vector<topic_partition_offset> tps) {
    std::vector<kafka::offset_commit_request_topic> res;
    if (tps.empty()) {
        return res;
    }

    std::sort(tps.begin(), tps.end());
    res.push_back(kafka::offset_commit_request_topic{tps.front().topic, {}});
    for (auto& tp : tps) {
        if (tp.topic != res.back().name) {
            res.push_back(
              kafka::offset_commit_request_topic{std::move(tp.topic), {}});
        }
        res.back().partitions.push_back(kafka::offset_commit_request_partition{
          .partition_index = tp.partition, .committed_offset = tp.offset});
    }
    return res;
}

} // namespace pandaproxy::json
