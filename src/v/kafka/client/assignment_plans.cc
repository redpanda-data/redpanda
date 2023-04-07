// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/assignment_plans.h"

#include "kafka/protocol/wire.h"

namespace kafka::client {

sync_group_request_assignment
assignment_plan::encode(const assignments::value_type& m) const {
    iobuf assignments_buf;
    protocol::encoder writer(assignments_buf);
    writer.write(int32_t(m.second.size()));
    for (const auto& t : m.second) {
        writer.write(t.first);
        writer.write_array(
          t.second, [](model::partition_id id, protocol::encoder& writer) {
              writer.write(id);
          });
    }

    return sync_group_request_assignment{
      .member_id = m.first, .assignment = iobuf_to_bytes(assignments_buf)};
};

std::vector<sync_group_request_assignment>
assignment_plan::encode(const assignments& assignments) const {
    std::vector<sync_group_request_assignment> result;
    result.reserve(assignments.size());
    for (const auto& m : assignments) {
        result.push_back(encode(m));
    }
    return result;
}

assignment assignment_plan::decode(const bytes& b) const {
    if (b.empty()) {
        return {};
    }
    protocol::decoder reader(bytes_to_iobuf(b));
    auto result = reader.read_array([](protocol::decoder& reader) {
        auto topic = model::topic(reader.read_string());
        return std::make_pair(
          std::move(topic), reader.read_array([](protocol::decoder& reader) {
              return model::partition_id(reader.read_int32());
          }));
    });
    return assignment(
      std::make_move_iterator(result.begin()),
      std::make_move_iterator(result.end()));
}

assignments assignment_range::plan(
  const std::vector<member_id>& members,
  const std::vector<metadata_response::topic>& topics) {
    assignments assignments;
    for (auto const& t : topics) {
        auto [len, rem] = std::ldiv(t.partitions.size(), members.size());
        auto iterations = std::min(t.partitions.size(), members.size());
        auto p_begin = t.partitions.begin();
        auto p_end = p_begin;
        auto mem_it = members.begin();
        while (iterations--) {
            p_end += len;
            if (rem > 0) {
                ++p_end;
                --rem;
            }
            auto& rtm = assignments[*mem_it][t.name];
            rtm.reserve(std::distance(p_begin, p_end));
            std::transform(
              p_begin, p_end, std::back_inserter(rtm), [](auto& p) {
                  return p.partition_index;
              });
            p_begin = p_end;
            ++mem_it;
        }
    }
    return assignments;
}

std::unique_ptr<assignment_plan>
make_assignment_plan(const protocol_name& protocol_name) {
    if (protocol_name == assignment_range::name) {
        return std::make_unique<assignment_range>();
    } else {
        return nullptr;
    }
}

join_group_request_protocol make_join_group_request_protocol_range(
  const std::vector<model::topic>& topics) {
    iobuf metadata;
    protocol::encoder writer(metadata);
    writer.write_array(
      topics, [](const model::topic& t, protocol::encoder& writer) {
          writer.write(t);
      });
    writer.write(int32_t(-1)); // userdata length

    return join_group_request_protocol{
      .name{protocol_name{"range"}}, .metadata{iobuf_to_bytes(metadata)}};
}

std::vector<join_group_request_protocol>
make_join_group_request_protocols(const std::vector<model::topic>& topics) {
    // When this is extended, create them in order of preference
    return {make_join_group_request_protocol_range(topics)};
}

} // namespace kafka::client
