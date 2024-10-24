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
#include "bytes/iobuf.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/offset_for_leader_epoch_request.h"
#include "kafka/protocol/schemata/offset_for_leader_epoch_response.h"
#include "model/fundamental.h"

namespace kafka {

struct offset_for_leader_epoch_request final {
    using api_type = offset_for_leader_epoch_api;

    offset_for_leader_epoch_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const offset_for_leader_epoch_request& r) {
        return os << r.data;
    }
};

struct offset_for_leader_epoch_response final {
    using api_type = offset_for_leader_epoch_api;

    offset_for_leader_epoch_response_data data;

    offset_for_leader_epoch_response() = default;

    offset_for_leader_epoch_response(
      error_code ec,
      offset_for_leader_epoch_request current_request,
      std::vector<offset_for_leader_topic_result> unauthorized_results) {
        data.topics.reserve(
          current_request.data.topics.size() + unauthorized_results.size());

        std::transform(
          current_request.data.topics.begin(),
          current_request.data.topics.end(),
          std::back_inserter(data.topics),
          [ec](offset_for_leader_topic& o) {
              chunked_vector<epoch_end_offset> offsets;
              offsets.reserve(o.partitions.size());
              std::transform(
                o.partitions.begin(),
                o.partitions.end(),
                std::back_inserter(offsets),
                [ec](const offset_for_leader_partition& ol) {
                    return epoch_end_offset{
                      .error_code = ec, .partition = ol.partition};
                });
              return offset_for_leader_topic_result{
                .topic = std::move(o.topic), .partitions = std::move(offsets)};
          });
        std::for_each(
          unauthorized_results.begin(),
          unauthorized_results.end(),
          [ec](offset_for_leader_topic_result& r) {
              std::for_each(
                r.partitions.begin(),
                r.partitions.end(),
                [ec](epoch_end_offset& o) { o.error_code = ec; });
          });
        std::move(
          unauthorized_results.begin(),
          unauthorized_results.end(),
          std::back_inserter(data.topics));
    }

    static epoch_end_offset make_epoch_end_offset(
      model::partition_id p_id,
      kafka::error_code ec,
      model::offset end_offset,
      kafka::leader_epoch l_epoch) {
        return epoch_end_offset{
          .error_code = ec,
          .partition = p_id,
          .leader_epoch = l_epoch,
          .end_offset = end_offset,
        };
    }

    static epoch_end_offset make_epoch_end_offset(
      model::partition_id p_id,
      model::offset end_offset,
      kafka::leader_epoch l_epoch) {
        return make_epoch_end_offset(
          p_id, error_code::none, end_offset, l_epoch);
    }

    static epoch_end_offset
    make_epoch_end_offset(model::partition_id p_id, kafka::error_code ec) {
        return make_epoch_end_offset(
          p_id, ec, model::offset(-1), invalid_leader_epoch);
    }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const offset_for_leader_epoch_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
