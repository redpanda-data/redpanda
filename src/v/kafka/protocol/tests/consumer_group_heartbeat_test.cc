// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "kafka/protocol/consumer_group_heartbeat.h"
#include "kafka/protocol/wire.h"

#include <gtest/gtest.h>

// For the request's SubscribedTopicNames and the response's Assignment, absent
// and present-but-empty are different instructions to the member, so the
// encoding has to keep them distinct.

namespace {

template<typename T>
iobuf encode(T& in) {
    iobuf buf;
    kafka::protocol::encoder enc(buf);
    in.encode(enc, kafka::api_version(1));
    return buf;
}

kafka::consumer_group_heartbeat_request_data decode_request(iobuf buf) {
    kafka::consumer_group_heartbeat_request_data out;
    kafka::protocol::decoder rdr(std::move(buf));
    out.decode(rdr, kafka::api_version(1));
    return out;
}

kafka::consumer_group_heartbeat_response_data decode_response(iobuf buf) {
    kafka::consumer_group_heartbeat_response_data out;
    out.decode(std::move(buf), kafka::api_version(1));
    return out;
}

} // namespace

TEST(ConsumerGroupHeartbeatWire, empty_subscription_is_not_absent) {
    kafka::consumer_group_heartbeat_request_data empty;
    empty.subscribed_topic_names = chunked_vector<model::topic>{};
    kafka::consumer_group_heartbeat_request_data absent;
    ASSERT_FALSE(absent.subscribed_topic_names.has_value());

    auto empty_out = decode_request(encode(empty));
    ASSERT_TRUE(empty_out.subscribed_topic_names.has_value());
    EXPECT_TRUE(empty_out.subscribed_topic_names->empty());
    EXPECT_FALSE(
      decode_request(encode(absent)).subscribed_topic_names.has_value());
}

TEST(ConsumerGroupHeartbeatWire, empty_assignment_is_not_absent) {
    kafka::consumer_group_heartbeat_response_data empty;
    empty.assignment = kafka::consumer_group_heartbeat_response_assignment{};
    kafka::consumer_group_heartbeat_response_data absent;
    ASSERT_FALSE(absent.assignment.has_value());

    auto empty_bytes = encode(empty);
    auto absent_bytes = encode(absent);

    // Null is a bare int8 of -1 and present is that marker plus the body, so
    // the two also differ in length.
    EXPECT_NE(empty_bytes.size_bytes(), absent_bytes.size_bytes());

    auto empty_out = decode_response(std::move(empty_bytes));
    ASSERT_TRUE(empty_out.assignment.has_value());
    EXPECT_TRUE(empty_out.assignment->topic_partitions.empty());
    EXPECT_FALSE(
      decode_response(std::move(absent_bytes)).assignment.has_value());
}
