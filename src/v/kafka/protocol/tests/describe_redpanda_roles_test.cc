// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "kafka/protocol/describe_redpanda_roles.h"
#include "kafka/protocol/wire.h"

#include <gtest/gtest.h>

// Concept: types whose decode() takes an iobuf (the response-data pattern).
template<typename T>
concept IobufDecodable = requires(T t, iobuf iob, kafka::api_version v) {
    { t.decode(std::move(iob), v) } -> std::same_as<void>;
};

template<typename T>
static T round_trip(T& original) {
    iobuf buf;
    {
        kafka::protocol::encoder enc(buf);
        original.encode(enc, kafka::api_version(0));
    }

    T decoded;
    if constexpr (IobufDecodable<T>) {
        // T is a response-data type
        decoded.decode(std::move(buf), kafka::api_version(0));
    } else {
        // T is a request-data type
        kafka::protocol::decoder rdr(std::move(buf));
        decoded.decode(rdr, kafka::api_version(0));
    }

    return decoded;
}

TEST(DescribeRedpandaRolesWire, request_with_role_name_filters) {
    // Request carrying two explicit role name filters.
    kafka::describe_redpanda_roles_request_data req;

    chunked_vector<kafka::role_name_filter> filters;
    filters.push_back(kafka::role_name_filter{.name = "admin"});
    filters.push_back(kafka::role_name_filter{.name = "readonly"});
    req.role_name_filters = std::move(filters);

    EXPECT_EQ(round_trip(req), req);
}

TEST(DescribeRedpandaRolesWire, request_with_null_filters) {
    // Request with a null filter list (describes all roles).
    kafka::describe_redpanda_roles_request_data req;
    // role_name_filters defaults to std::nullopt — leave it unset.
    ASSERT_FALSE(req.role_name_filters.has_value());

    EXPECT_EQ(round_trip(req), req);
}

TEST(DescribeRedpandaRolesWire, request_with_empty_filters) {
    // Request with an explicitly empty (non-null) filter list.
    kafka::describe_redpanda_roles_request_data req;
    req.role_name_filters = chunked_vector<kafka::role_name_filter>{};

    EXPECT_EQ(round_trip(req), req);
}

TEST(DescribeRedpandaRolesWire, response_with_roles_and_members) {
    // Response containing two roles, each with members of both types
    // (user = 0, group = 1), plus throttle and no error.
    kafka::describe_redpanda_roles_response_data resp;
    resp.throttle_time_ms = std::chrono::milliseconds(42);
    resp.error_code = kafka::error_code::none;
    // error_message is std::nullopt (null) for the no-error case.

    {
        kafka::redpanda_role role;
        role.name = "admin";

        kafka::redpanda_role_member user_member;
        user_member.member_type = 0; // user
        user_member.name = "alice";
        role.members.push_back(std::move(user_member));

        kafka::redpanda_role_member group_member;
        group_member.member_type = 1; // group
        group_member.name = "ops-team";
        role.members.push_back(std::move(group_member));

        resp.roles.push_back(std::move(role));
    }

    {
        kafka::redpanda_role role;
        role.name = "readonly";

        kafka::redpanda_role_member user_member;
        user_member.member_type = 0; // user
        user_member.name = "bob";
        role.members.push_back(std::move(user_member));

        resp.roles.push_back(std::move(role));
    }

    EXPECT_EQ(round_trip(resp), resp);
}

TEST(DescribeRedpandaRolesWire, response_empty_with_error) {
    // Response with no roles, a non-zero error_code, and a non-null
    // error_message (exercises the nullable string and error paths).
    kafka::describe_redpanda_roles_response_data resp;
    resp.throttle_time_ms = std::chrono::milliseconds(0);
    resp.error_code = kafka::error_code::cluster_authorization_failed;
    resp.error_message = "not authorized";
    // roles remains empty.

    EXPECT_EQ(round_trip(resp), resp);
}
