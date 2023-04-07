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
#include "kafka/protocol/schemata/list_offset_request.h"
#include "kafka/protocol/schemata/list_offset_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

#include <absl/container/btree_set.h>

namespace kafka {

struct list_offsets_request final {
    using api_type = list_offsets_api;

    static constexpr model::timestamp earliest_timestamp{-2};
    static constexpr model::timestamp latest_timestamp{-1};

    list_offset_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    absl::btree_set<model::topic_partition> tp_dups;

    void compute_duplicate_topics();

    bool duplicate_tp(const model::topic& t, model::partition_id id) const {
        model::topic_partition tp(t, id);
        return tp_dups.find(tp) != tp_dups.end();
    }

    friend std::ostream&
    operator<<(std::ostream& os, const list_offsets_request& r) {
        return os << r.data;
    }
};

struct list_offsets_response final {
    using api_type = list_offsets_api;

    list_offset_response_data data;

    static list_offset_partition_response make_partition(
      model::partition_id id,
      error_code error,
      model::timestamp timestamp,
      model::offset offset,
      leader_epoch leader_epoch) {
        return list_offset_partition_response{
          .partition_index = id,
          .error_code = error,
          .timestamp = timestamp,
          .offset = offset,
          .leader_epoch = leader_epoch,
        };
    }

    static list_offset_partition_response make_partition(
      model::partition_id id,
      model::timestamp timestamp,
      model::offset offset,
      kafka::leader_epoch leader_epoch) {
        return make_partition(
          id, error_code::none, timestamp, offset, leader_epoch);
    }

    static list_offset_partition_response
    make_partition(model::partition_id id, error_code error) {
        return make_partition(
          id,
          error,
          model::timestamp(-1),
          model::offset(-1),
          invalid_leader_epoch);
    }

    void encode(protocol::encoder& writer, api_version version) {
        // convert to version zero in which the data model supported returning
        // multiple offsets instead of just one
        if (version == api_version(0)) {
            for (auto& topic : data.topics) {
                for (auto& partition : topic.partitions) {
                    partition.old_style_offsets.push_back(partition.offset());
                }
            }
        }
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const list_offsets_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
