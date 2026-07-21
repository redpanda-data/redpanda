// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "kafka/protocol/consumer_group_describe.h"
#include "kafka/protocol/consumer_group_heartbeat.h"
#include "kafka/protocol/wire.h"

#include <gtest/gtest.h>

// Concept: types whose decode() takes an iobuf (the response-data pattern).
template<typename T>
concept IobufDecodable = requires(T t, iobuf iob, kafka::api_version v) {
    { t.decode(std::move(iob), v) } -> std::same_as<void>;
};

template<typename T>
static T round_trip(T& original, kafka::api_version v = kafka::api_version(0)) {
    iobuf buf;
    {
        kafka::protocol::encoder enc(buf);
        original.encode(enc, v);
    }

    T decoded;
    if constexpr (IobufDecodable<T>) {
        decoded.decode(std::move(buf), v);
    } else {
        kafka::protocol::decoder rdr(std::move(buf));
        decoded.decode(rdr, v);
    }

    return decoded;
}

TEST(ConsumerGroupHeartbeatWire, full_join_request) {
    kafka::consumer_group_heartbeat_request_data req;
    req.group_id = kafka::group_id("g");
    req.member_id = kafka::member_id("m1");
    req.member_epoch = 0;
    req.instance_id = kafka::group_instance_id("i1");
    req.rack_id = "rack-1";
    req.rebalance_timeout_ms = std::chrono::milliseconds(60000);
    chunked_vector<model::topic> names;
    names.push_back(model::topic("t1"));
    names.push_back(model::topic("t2"));
    req.subscribed_topic_names = std::move(names);
    req.server_assignor = "uniform";

    chunked_vector<kafka::consumer_group_heartbeat_request_topic_partitions>
      owned;
    kafka::consumer_group_heartbeat_request_topic_partitions tp;
    tp.topic_id = model::topic_id::create();
    tp.partitions = {model::partition_id(0), model::partition_id(2)};
    owned.push_back(std::move(tp));
    req.topic_partitions = std::move(owned);

    EXPECT_EQ(round_trip(req), req);
    EXPECT_EQ(round_trip(req, kafka::api_version(1)), req);
}

TEST(ConsumerGroupHeartbeatWire, request_null_fields) {
    kafka::consumer_group_heartbeat_request_data req;
    req.group_id = kafka::group_id("g");
    req.member_id = kafka::member_id("m1");
    req.member_epoch = 5;

    ASSERT_FALSE(req.subscribed_topic_names.has_value());
    ASSERT_FALSE(req.topic_partitions.has_value());
    EXPECT_EQ(round_trip(req), req);
}

TEST(ConsumerGroupHeartbeatWire, request_regex_only_in_v1) {
    kafka::consumer_group_heartbeat_request_data req;
    req.group_id = kafka::group_id("g");
    req.member_id = kafka::member_id("m1");
    req.subscribed_topic_regex = "t.*";

    auto v0 = round_trip(req, kafka::api_version(0));
    EXPECT_FALSE(v0.subscribed_topic_regex.has_value());

    auto v1 = round_trip(req, kafka::api_version(1));
    EXPECT_EQ(v1.subscribed_topic_regex, "t.*");
}

TEST(ConsumerGroupHeartbeatWire, response_null_assignment) {
    kafka::consumer_group_heartbeat_response_data resp;
    resp.error_code = kafka::error_code::none;
    resp.member_id = kafka::member_id("m1");
    resp.member_epoch = 3;
    resp.heartbeat_interval_ms = std::chrono::milliseconds(5000);

    ASSERT_FALSE(resp.assignment.has_value());
    EXPECT_EQ(round_trip(resp), resp);
}

TEST(ConsumerGroupHeartbeatWire, response_with_assignment) {
    kafka::consumer_group_heartbeat_response_data resp;
    resp.error_code = kafka::error_code::none;
    resp.member_id = kafka::member_id("m1");
    resp.member_epoch = 3;
    resp.heartbeat_interval_ms = std::chrono::milliseconds(5000);

    kafka::consumer_group_heartbeat_response_assignment assignment;
    kafka::consumer_group_heartbeat_response_topic_partitions tp;
    tp.topic_id = model::topic_id::create();
    tp.partitions = {model::partition_id(1), model::partition_id(4)};
    assignment.topic_partitions.push_back(std::move(tp));
    resp.assignment = std::move(assignment);

    EXPECT_EQ(round_trip(resp), resp);
    EXPECT_EQ(round_trip(resp, kafka::api_version(1)), resp);
}

TEST(ConsumerGroupHeartbeatWire, response_empty_assignment_not_null) {
    // an empty (but present) assignment revokes everything and must be
    // distinguishable from a null (unchanged) assignment on the wire.
    kafka::consumer_group_heartbeat_response_data resp;
    resp.member_epoch = 1;
    resp.assignment = kafka::consumer_group_heartbeat_response_assignment{};

    auto decoded = round_trip(resp);
    ASSERT_TRUE(decoded.assignment.has_value());
    EXPECT_TRUE(decoded.assignment->topic_partitions.empty());
}

TEST(ConsumerGroupHeartbeatWire, assignment_null_marker_is_varint) {
    // in flexible versions the nullable assignment struct is preceded by an
    // unsigned varint null marker (0 = null, 1 = present), not the int8
    // -1/1 marker of non-flexible versions. Round trips cannot catch a
    // wrong marker encoding, so check the wire bytes directly.
    kafka::consumer_group_heartbeat_response_data resp;
    resp.member_epoch = 1;

    iobuf null_buf;
    {
        kafka::protocol::encoder enc(null_buf);
        resp.encode(enc, kafka::api_version(0));
    }
    auto null_bytes = iobuf_to_bytes(null_buf);
    // ...[assignment null marker][empty top level tagged fields]
    ASSERT_GE(null_bytes.size(), 2);
    EXPECT_EQ(null_bytes[null_bytes.size() - 2], 0x00);

    resp.assignment = kafka::consumer_group_heartbeat_response_assignment{};
    iobuf present_buf;
    {
        kafka::protocol::encoder enc(present_buf);
        resp.encode(enc, kafka::api_version(0));
    }
    auto present_bytes = iobuf_to_bytes(present_buf);
    // ...[assignment null marker][empty topic partitions array][empty
    // struct tagged fields][empty top level tagged fields]
    ASSERT_EQ(present_bytes.size(), null_bytes.size() + 2);
    EXPECT_EQ(present_bytes[present_bytes.size() - 4], 0x01);
}

TEST(ConsumerGroupDescribeWire, request_round_trip) {
    kafka::consumer_group_describe_request_data req;
    req.group_ids.push_back(kafka::group_id("g1"));
    req.group_ids.push_back(kafka::group_id("g2"));
    req.include_authorized_operations = true;

    EXPECT_EQ(round_trip(req), req);
}

TEST(ConsumerGroupDescribeWire, response_round_trip) {
    kafka::consumer_group_describe_response_data resp;

    kafka::consumer_group_described_group group;
    group.group_id = kafka::group_id("g1");
    group.group_state = "Stable";
    group.group_epoch = 7;
    group.assignment_epoch = 7;
    group.assignor_name = "uniform";

    kafka::consumer_group_describe_member member;
    member.member_id = kafka::member_id("m1");
    member.instance_id = kafka::group_instance_id("i1");
    member.member_epoch = 7;
    member.client_id = "client";
    member.client_host = "/10.0.0.1";
    member.subscribed_topic_names.push_back(model::topic("t1"));

    kafka::consumer_group_describe_assigned_topic_partitions assigned;
    assigned.topic_id = model::topic_id::create();
    assigned.topic_name = model::topic("t1");
    assigned.partitions = {model::partition_id(0)};
    member.assignment.topic_partitions.push_back(std::move(assigned));

    kafka::consumer_group_describe_target_topic_partitions target;
    target.topic_id = model::topic_id::create();
    target.topic_name = model::topic("t1");
    target.partitions = {model::partition_id(0), model::partition_id(1)};
    member.target_assignment.topic_partitions.push_back(std::move(target));

    group.members.push_back(std::move(member));
    resp.groups.push_back(std::move(group));

    EXPECT_EQ(round_trip(resp), resp);
}
