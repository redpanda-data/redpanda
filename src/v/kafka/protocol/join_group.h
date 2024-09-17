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
#include "container/fragmented_vector.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/join_group_request.h"
#include "kafka/protocol/schemata/join_group_response.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

#include <utility>

namespace kafka {

struct join_group_request final {
    using api_type = join_group_api;

    join_group_request_data data;

    join_group_request() = default;
    join_group_request(const join_group_request&) = delete;
    join_group_request& operator=(const join_group_request&) = delete;
    join_group_request(join_group_request&&) = default;
    join_group_request& operator=(join_group_request&&) = delete;

    // extra context from request header set in decode
    api_version version;
    std::optional<kafka::client_id> client_id;
    kafka::client_host client_host;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const join_group_request& r) {
        return os << r.data;
    }
};

static inline const kafka::member_id no_member("");
static inline const kafka::member_id no_leader("");
static constexpr kafka::generation_id no_generation(-1);
static inline const kafka::protocol_name no_protocol("");

struct join_group_response final {
    using api_type = join_group_api;

    join_group_response_data data;

    join_group_response() = default;

    join_group_response(kafka::member_id member_id, kafka::error_code error)
      : join_group_response(
          error, no_generation, no_protocol, no_leader, member_id) {}

    explicit join_group_response(kafka::error_code error)
      : join_group_response(no_member, error) {}

    join_group_response(const join_group_request& r, kafka::error_code error)
      : join_group_response(r.data.member_id, error) {}

    join_group_response(
      kafka::error_code error,
      kafka::generation_id generation_id,
      kafka::protocol_name protocol_name,
      kafka::member_id leader_id,
      kafka::member_id member_id,
      chunked_vector<join_group_response_member> members = {}) {
        data.throttle_time_ms = std::chrono::milliseconds(0);
        data.error_code = error;
        data.generation_id = generation_id;
        data.protocol_name = std::move(protocol_name);
        data.leader = std::move(leader_id);
        data.member_id = std::move(member_id);
        data.members = std::move(members);
    }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const join_group_response& r) {
        return os << r.data;
    }
};

inline join_group_response
make_join_error(kafka::member_id member_id, error_code error) {
    return join_group_response(
      error, no_generation, no_protocol, no_leader, std::move(member_id));
}

} // namespace kafka
