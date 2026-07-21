// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/errors.h"
#include "kafka/server/consumer_group.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <optional>

using namespace std::chrono_literals;
using namespace kafka;

namespace {

const model::topic topic_a("topic-a");
const model::topic topic_b("topic-b");

struct consumer_group_fixture : seastar_test {
    consumer_group_fixture()
      : grp(kafka::group_id("test-group")) {
        add_topic(topic_a, 6);
    }

    void add_topic(const model::topic& t, int32_t partitions) {
        topics[t] = consumer_group_topic_metadata{
          .id = model::topic_id::create(),
          .partition_count = partitions,
        };
    }

    consumer_group_topic_resolver make_resolver() {
        return consumer_group_topic_resolver(
          [this](const model::topic& t)
            -> std::optional<consumer_group_topic_metadata> {
              auto it = topics.find(t);
              if (it == topics.end()) {
                  return std::nullopt;
              }
              return it->second;
          });
    }

    static consumer_group_settings settings() {
        return {.session_timeout = 45s, .heartbeat_interval = 5s};
    }

    consumer_group_heartbeat_request make_join(
      const ss::sstring& member,
      std::vector<model::topic> subscriptions = {topic_a},
      std::optional<ss::sstring> assignor = std::nullopt) {
        consumer_group_heartbeat_request req;
        req.data.group_id = kafka::group_id("test-group");
        req.data.member_id = kafka::member_id(member);
        req.data.member_epoch = consumer_group::join_epoch;
        req.data.rebalance_timeout_ms = 60s;
        chunked_vector<model::topic> names;
        for (auto& t : subscriptions) {
            names.push_back(t);
        }
        req.data.subscribed_topic_names = std::move(names);
        req.data.server_assignor = std::move(assignor);
        return req;
    }

    consumer_group_heartbeat_request
    make_heartbeat(const ss::sstring& member, int32_t epoch) {
        consumer_group_heartbeat_request req;
        req.data.group_id = kafka::group_id("test-group");
        req.data.member_id = kafka::member_id(member);
        req.data.member_epoch = epoch;
        return req;
    }

    static void set_owned(
      consumer_group_heartbeat_request& req,
      const consumer_group_heartbeat_response& from) {
        chunked_vector<consumer_group_heartbeat_request_topic_partitions> owned;
        if (from.data.assignment) {
            for (const auto& tp : from.data.assignment->topic_partitions) {
                consumer_group_heartbeat_request_topic_partitions o;
                o.topic_id = tp.topic_id;
                o.partitions = tp.partitions;
                owned.push_back(std::move(o));
            }
        }
        req.data.topic_partitions = std::move(owned);
    }

    static size_t partition_count(const consumer_group_heartbeat_response& r) {
        size_t count = 0;
        if (r.data.assignment) {
            for (const auto& tp : r.data.assignment->topic_partitions) {
                count += tp.partitions.size();
            }
        }
        return count;
    }

    consumer_group_heartbeat_response
    heartbeat(consumer_group_heartbeat_request req) {
        auto resolver = make_resolver();
        return grp.handle_heartbeat(std::move(req), resolver, settings());
    }

    absl::btree_map<model::topic, consumer_group_topic_metadata> topics;
    consumer_group grp;
};

TEST_F_CORO(consumer_group_fixture, join_single_member_gets_all_partitions) {
    auto resp = heartbeat(make_join("m1"));
    ASSERT_EQ_CORO(resp.data.error_code, error_code::none);
    ASSERT_EQ_CORO(resp.data.member_epoch, 1);
    ASSERT_EQ_CORO(partition_count(resp), 6);
    ASSERT_EQ_CORO(grp.state_name(), "Stable");
    ASSERT_EQ_CORO(grp.member_count(), 1);
    co_return;
}

TEST_F_CORO(consumer_group_fixture, generates_member_id_when_empty) {
    auto req = make_join("");
    auto resp = heartbeat(std::move(req));
    ASSERT_EQ_CORO(resp.data.error_code, error_code::none);
    ASSERT_TRUE_CORO(resp.data.member_id.has_value());
    ASSERT_FALSE_CORO((*resp.data.member_id)().empty());
    co_return;
}

TEST_F_CORO(consumer_group_fixture, incremental_rebalance_on_second_join) {
    auto r1 = heartbeat(make_join("m1"));
    ASSERT_EQ_CORO(partition_count(r1), 6);

    // m2 joins: it advances to the new epoch but all of its target
    // partitions are still owned by m1.
    auto r2 = heartbeat(make_join("m2"));
    ASSERT_EQ_CORO(r2.data.error_code, error_code::none);
    ASSERT_EQ_CORO(r2.data.member_epoch, 2);
    ASSERT_EQ_CORO(partition_count(r2), 0);
    ASSERT_EQ_CORO(grp.state_name(), "Reconciling");

    // m1 heartbeats reporting it still owns all six partitions: it is asked
    // to shrink to its target assignment and stays at its old epoch.
    auto hb = make_heartbeat("m1", 1);
    set_owned(hb, r1);
    auto r3 = heartbeat(std::move(hb));
    ASSERT_EQ_CORO(r3.data.error_code, error_code::none);
    ASSERT_EQ_CORO(r3.data.member_epoch, 1);
    ASSERT_EQ_CORO(partition_count(r3), 3);

    // m1 acknowledges the revocation by reporting the reduced set and
    // advances to the target epoch.
    auto hb2 = make_heartbeat("m1", 1);
    set_owned(hb2, r3);
    auto r4 = heartbeat(std::move(hb2));
    ASSERT_EQ_CORO(r4.data.error_code, error_code::none);
    ASSERT_EQ_CORO(r4.data.member_epoch, 2);

    // the revoked partitions are now free and granted to m2.
    auto r5 = heartbeat(make_heartbeat("m2", 2));
    ASSERT_EQ_CORO(r5.data.error_code, error_code::none);
    ASSERT_EQ_CORO(r5.data.member_epoch, 2);
    ASSERT_EQ_CORO(partition_count(r5), 3);
    ASSERT_EQ_CORO(grp.state_name(), "Stable");
    co_return;
}

TEST_F_CORO(consumer_group_fixture, leave_rebalances_remaining_members) {
    auto r1 = heartbeat(make_join("m1"));
    heartbeat(make_join("m2"));

    auto leave = make_heartbeat("m2", consumer_group::leave_epoch);
    auto r2 = heartbeat(std::move(leave));
    ASSERT_EQ_CORO(r2.data.error_code, error_code::none);
    ASSERT_EQ_CORO(r2.data.member_epoch, consumer_group::leave_epoch);
    ASSERT_EQ_CORO(grp.member_count(), 1);

    // m1 never revoked anything (m2 owned nothing), so it advances straight
    // to the latest epoch with the full assignment.
    auto hb = make_heartbeat("m1", r1.data.member_epoch);
    set_owned(hb, r1);
    auto r3 = heartbeat(std::move(hb));
    ASSERT_EQ_CORO(r3.data.error_code, error_code::none);
    ASSERT_EQ_CORO(r3.data.member_epoch, grp.group_epoch());
    ASSERT_EQ_CORO(partition_count(r3), 6);
    co_return;
}

TEST_F_CORO(consumer_group_fixture, static_leave_reserves_instance_id) {
    auto join = make_join("m1");
    join.data.instance_id = kafka::group_instance_id("i1");
    auto r1 = heartbeat(std::move(join));
    ASSERT_EQ_CORO(r1.data.error_code, error_code::none);
    ASSERT_EQ_CORO(partition_count(r1), 6);

    // another member cannot claim the instance id while m1 is active.
    auto join2 = make_join("m2");
    join2.data.instance_id = kafka::group_instance_id("i1");
    auto r2 = heartbeat(std::move(join2));
    ASSERT_EQ_CORO(r2.data.error_code, error_code::unreleased_instance_id);

    // a static leave keeps the member around, reserving the instance id
    // and its assignment for the rejoining incarnation.
    auto leave = make_heartbeat("m1", consumer_group::static_leave_epoch);
    leave.data.instance_id = kafka::group_instance_id("i1");
    auto r3 = heartbeat(std::move(leave));
    ASSERT_EQ_CORO(r3.data.error_code, error_code::none);
    ASSERT_EQ_CORO(r3.data.member_epoch, consumer_group::static_leave_epoch);
    ASSERT_EQ_CORO(grp.member_count(), 1);

    // the instance id is still reserved: a new incarnation with the same
    // instance id takes over the departed member under its new member id
    // and receives the reserved assignment.
    auto join3 = make_join("m3");
    join3.data.instance_id = kafka::group_instance_id("i1");
    auto r4 = heartbeat(std::move(join3));
    ASSERT_EQ_CORO(r4.data.error_code, error_code::none);
    ASSERT_EQ_CORO(r4.data.member_id, kafka::member_id("m3"));
    ASSERT_EQ_CORO(partition_count(r4), 6);
    ASSERT_EQ_CORO(grp.member_count(), 1);

    // the departed incarnation is fenced.
    auto stale = make_heartbeat("m1", 1);
    auto r5 = heartbeat(std::move(stale));
    ASSERT_EQ_CORO(r5.data.error_code, error_code::unknown_member_id);
    co_return;
}

TEST_F_CORO(consumer_group_fixture, static_leave_validates_instance_id) {
    auto join = make_join("m1");
    join.data.instance_id = kafka::group_instance_id("i1");
    heartbeat(std::move(join));

    // -2 without an instance id is invalid.
    auto leave = make_heartbeat("m1", consumer_group::static_leave_epoch);
    auto r1 = heartbeat(std::move(leave));
    ASSERT_EQ_CORO(r1.data.error_code, error_code::invalid_request);

    // -2 with an instance id the member does not own is fenced.
    auto leave2 = make_heartbeat("m1", consumer_group::static_leave_epoch);
    leave2.data.instance_id = kafka::group_instance_id("other");
    auto r2 = heartbeat(std::move(leave2));
    ASSERT_EQ_CORO(r2.data.error_code, error_code::fenced_instance_id);

    ASSERT_EQ_CORO(grp.member_count(), 1);
    co_return;
}

TEST_F_CORO(consumer_group_fixture, fenced_member_epoch) {
    heartbeat(make_join("m1"));
    auto resp = heartbeat(make_heartbeat("m1", 42));
    ASSERT_EQ_CORO(resp.data.error_code, error_code::fenced_member_epoch);
    co_return;
}

TEST_F_CORO(consumer_group_fixture, unknown_member) {
    auto resp = heartbeat(make_heartbeat("nope", 1));
    ASSERT_EQ_CORO(resp.data.error_code, error_code::unknown_member_id);
    co_return;
}

TEST_F_CORO(consumer_group_fixture, regex_subscription_rejected) {
    auto req = make_join("m1");
    req.data.subscribed_topic_regex = "topic-.*";
    auto resp = heartbeat(std::move(req));
    ASSERT_EQ_CORO(
      resp.data.error_code, error_code::invalid_regular_expression);
    co_return;
}

TEST_F_CORO(consumer_group_fixture, unsupported_assignor_rejected) {
    auto resp = heartbeat(make_join("m1", {topic_a}, "sticky"));
    ASSERT_EQ_CORO(resp.data.error_code, error_code::unsupported_assignor);
    co_return;
}

TEST_F_CORO(consumer_group_fixture, range_assignor_contiguous_split) {
    add_topic(topic_b, 5);
    auto r1 = heartbeat(make_join("a", {topic_b}, "range"));
    ASSERT_EQ_CORO(r1.data.error_code, error_code::none);
    ASSERT_EQ_CORO(partition_count(r1), 5);

    heartbeat(make_join("b", {topic_b}, "range"));

    // member "a" sorts first and keeps partitions [0, 3).
    auto hb = make_heartbeat("a", 1);
    set_owned(hb, r1);
    auto r2 = heartbeat(std::move(hb));
    ASSERT_EQ_CORO(partition_count(r2), 3);
    const auto& tp = r2.data.assignment->topic_partitions[0];
    std::vector<model::partition_id> expected{
      model::partition_id(0), model::partition_id(1), model::partition_id(2)};
    ASSERT_EQ_CORO(tp.partitions, expected);
    co_return;
}

TEST_F_CORO(consumer_group_fixture, topic_creation_triggers_rebalance) {
    auto r1 = heartbeat(make_join("m1", {topic_a, topic_b}));
    // topic_b does not exist yet.
    ASSERT_EQ_CORO(partition_count(r1), 6);
    auto epoch = grp.group_epoch();

    add_topic(topic_b, 2);
    auto r2 = heartbeat(make_heartbeat("m1", r1.data.member_epoch));
    ASSERT_EQ_CORO(r2.data.error_code, error_code::none);
    ASSERT_GT_CORO(grp.group_epoch(), epoch);
    ASSERT_EQ_CORO(partition_count(r2), 8);
    co_return;
}

TEST_F_CORO(consumer_group_fixture, offset_commit_validation) {
    auto r1 = heartbeat(make_join("m1"));
    ASSERT_EQ_CORO(
      grp.validate_offset_commit(kafka::member_id("m1"), r1.data.member_epoch),
      error_code::none);
    ASSERT_EQ_CORO(
      grp.validate_offset_commit(kafka::member_id("m1"), 42),
      error_code::stale_member_epoch);
    ASSERT_EQ_CORO(
      grp.validate_offset_commit(kafka::member_id("nope"), 1),
      error_code::unknown_member_id);
    co_return;
}

TEST_F_CORO(consumer_group_fixture, describe_group) {
    auto r1 = heartbeat(make_join("m1"));
    auto described = grp.describe();
    ASSERT_EQ_CORO(described.group_id, kafka::group_id("test-group"));
    ASSERT_EQ_CORO(described.group_state, "Stable");
    ASSERT_EQ_CORO(described.group_epoch, grp.group_epoch());
    ASSERT_EQ_CORO(described.assignment_epoch, grp.assignment_epoch());
    ASSERT_EQ_CORO(described.assignor_name, "uniform");
    ASSERT_EQ_CORO(described.members.size(), 1);
    const auto& member = described.members[0];
    ASSERT_EQ_CORO(member.member_id, kafka::member_id("m1"));
    ASSERT_EQ_CORO(member.member_epoch, r1.data.member_epoch);
    ASSERT_EQ_CORO(member.assignment.topic_partitions.size(), 1);
    ASSERT_EQ_CORO(member.assignment.topic_partitions[0].topic_name, topic_a);
    ASSERT_EQ_CORO(member.assignment.topic_partitions[0].partitions.size(), 6);
    co_return;
}

} // namespace
