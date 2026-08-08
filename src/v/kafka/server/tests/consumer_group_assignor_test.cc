/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "container/chunked_vector.h"
#include "kafka/server/uniform_assignor.h"
#include "model/fundamental.h"
#include "utils/uuid.h"

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <ranges>
#include <string>
#include <vector>

namespace kafka {
namespace {

/// Topic metadata for the topics a test registered.
class test_describer final : public topic_describer {
public:
    void add(model::topic_id topic, int32_t partitions) {
        _partitions[topic] = partitions;
    }

    std::optional<int32_t> num_partitions(model::topic_id topic) const final {
        auto it = _partitions.find(topic);
        if (it == _partitions.end()) {
            return std::nullopt;
        }
        return it->second;
    }

private:
    absl::btree_map<model::topic_id, int32_t> _partitions;
};

/// A topic id whose byte pattern orders by `n`, so a test can predict the order
/// the assignor walks topics in.
model::topic_id make_topic_id(uint16_t n) {
    std::vector<uint8_t> bytes(uuid_t::length, 0);
    bytes[uuid_t::length - 2] = static_cast<uint8_t>(n >> 8U);
    bytes[uuid_t::length - 1] = static_cast<uint8_t>(n & 0xFFU);
    return model::topic_id{uuid_t{bytes}};
}

/// An assignment of one topic, from partition ids given in any order.
partition_set partitions(std::initializer_list<int32_t> ps) {
    partition_set set;
    set.reserve(ps.size());
    for (auto p : ps) {
        set.emplace_back(p);
    }
    std::ranges::sort(set);
    return set;
}

/// An assignment written as `{{topic, {partition ids...}}, ...}`, in any order.
using assignment_spec = std::initializer_list<
  std::pair<model::topic_id, std::initializer_list<int32_t>>>;

member_assignment assigned(assignment_spec entries) {
    // `assign` expects the topics in increasing order, so we sort the spec up
    // front.
    chunked_vector<topic_partitions> sorted;
    sorted.reserve(entries.size());
    for (const auto& [topic, ps] : entries) {
        sorted.push_back({.topic = topic, .partitions = partitions(ps)});
    }
    std::ranges::sort(sorted, {}, &topic_partitions::topic);

    member_assignment assignment;
    for (auto& [topic, ps] : sorted) {
        assignment.assign(topic, std::move(ps));
    }
    return assignment;
}

/// A member built exactly as given, so a test can feed the assignor input
/// that breaks the ordering rules.
member_spec member_as_given(
  std::string_view id,
  subscribed_topic_ids subscribed,
  member_assignment assigned_to_it = {}) {
    return member_spec{
      .id = member_id{ss::sstring{id}},
      .subscribed_topics = std::move(subscribed),
      .current_assignment = std::move(assigned_to_it)};
}

member_spec member(
  std::string_view id,
  subscribed_topic_ids subscribed,
  assignment_spec assigned_to_it = {}) {
    // This sorts the subscription, which the assignor requires of its caller,
    // so that a test can list one in any order.
    std::ranges::sort(subscribed);
    return member_spec{
      .id = member_id{ss::sstring{id}},
      .subscribed_topics = std::move(subscribed),
      .current_assignment = assigned(assigned_to_it)};
}

template<typename... Ms>
group_spec spec(Ms&&... members) {
    chunked_vector<member_spec> ms;
    (ms.push_back(std::forward<Ms>(members)), ...);
    return group_spec{std::move(ms)};
}

TEST(group_spec_test, group_sharing_one_subscription_is_homogeneous) {
    auto t1 = make_topic_id(1);
    auto t2 = make_topic_id(2);

    auto group = spec(member("m1", {t1, t2}), member("m2", {t2, t1}));

    EXPECT_EQ(group.type(), subscription_type::homogeneous);
}

TEST(group_spec_test, group_with_differing_subscriptions_is_heterogeneous) {
    auto t1 = make_topic_id(1);
    auto t2 = make_topic_id(2);

    auto group = spec(member("m1", {t1, t2}), member("m2", {t1}));

    EXPECT_EQ(group.type(), subscription_type::heterogeneous);
}

TEST(group_spec_test, empty_group_is_homogeneous) {
    EXPECT_EQ(spec().type(), subscription_type::homogeneous);
}

class uniform_assignor_test : public ::testing::Test {
protected:
    /// Registers a topic and returns its id. Topics are named `t1`, `t2`, ...
    /// in registration order, and their ids sort the same way.
    model::topic_id add_topic(int32_t num_partitions) {
        auto topic = make_topic_id(++_topic_count);
        _describer.add(topic, num_partitions);
        _names[topic] = fmt::format("t{}", _topic_count);
        return topic;
    }

    /// An id for a topic that was never registered, so the metadata lookup for
    /// it fails.
    model::topic_id unknown_topic() { return make_topic_id(++_topic_count); }

    assignment_result assign(const group_spec& group) {
        // An assignment is positional, so `render` and `expect_assignment`
        // both need the spec behind it.
        chunked_vector<member_spec> members;
        members.reserve(group.members().size());
        for (const auto& member : group.members()) {
            members.push_back(
              member_spec{
                .id = member.id,
                .subscribed_topics = member.subscribed_topics.copy(),
                .current_assignment = member.current_assignment.copy()});
        }
        _assigned_group.emplace(std::move(members));
        return _assignor.assign(group, _describer);
    }

    /// Renders an assignment as `m1: t1[0,1] t2[0]; m2: t1[2]`, in the member
    /// order of the spec it was assigned from, then topic order.
    std::string render(const group_assignment& assignment) const {
        std::string out;
        for (size_t index = 0; index < assignment.size(); ++index) {
            if (!out.empty()) {
                out += "; ";
            }
            out += fmt::format("{}:", _assigned_group->members()[index].id());
            for (const auto& [topic, ps] : assignment[index]) {
                out += fmt::format(
                  " {}[{}]", topic_name(topic), fmt::join(ps, ","));
            }
        }
        return out;
    }

    /// The number of partitions assigned to each member, in spec order.
    std::vector<size_t> sizes(const group_assignment& assignment) const {
        std::vector<size_t> out;
        out.reserve(assignment.size());
        for (const auto& member : assignment) {
            out.push_back(member.num_partitions());
        }
        return out;
    }

    /// Asserts the assignment, member by member in the spec's order, each
    /// written like `assigned`. Holds both sides to `expect_valid`, so a test
    /// that writes an invalid expectation fails on the expectation too. Prints
    /// both rendered on failure.
    void expect_assignment(
      const group_assignment& actual,
      std::initializer_list<assignment_spec> members) const {
        group_assignment want{members.size()};
        size_t index = 0;
        for (const auto& member : members) {
            want[index++] = assigned(member);
        }
        EXPECT_TRUE(actual == want)
          << "assigned: " << render(actual) << "\n    want: " << render(want);
        expect_valid(*_assigned_group, actual);
        expect_valid(*_assigned_group, want);
    }

    /// Asserts the invariants of every assignment:
    ///
    /// - each partition of a subscribed topic has exactly one owner
    /// - every owner subscribes to the topic it owns a partition of
    /// - each member holds its topics in order of increasing topic id, and
    ///   each topic's partitions in order of increasing partition id
    /// - a homogeneous group must come out balanced to within one partition
    void expect_valid(
      const group_spec& group, const group_assignment& assignment) const {
        ASSERT_EQ(assignment.size(), group.members().size())
          << "an assignment has one entry per member";

        absl::btree_map<model::topic_id_partition, member_id> owners;
        absl::btree_set<model::topic_id> subscribed;
        for (const auto& member : group.members()) {
            subscribed.insert(
              member.subscribed_topics.begin(), member.subscribed_topics.end());
        }

        for (size_t index = 0; index < assignment.size(); ++index) {
            const auto& member = group.members()[index];
            EXPECT_TRUE(
              std::ranges::is_sorted(
                assignment[index], {}, &topic_partitions::topic))
              << member.id()
              << " holds its topics out of order: " << render(assignment);
            for (const auto& [topic, ps] : assignment[index]) {
                EXPECT_TRUE(std::ranges::is_sorted(ps))
                  << member.id() << " holds " << topic_name(topic)
                  << " out of partition order: " << render(assignment);
                EXPECT_TRUE(
                  std::ranges::binary_search(member.subscribed_topics, topic))
                  << member.id() << " does not subscribe to "
                  << topic_name(topic);
                for (auto partition : ps) {
                    auto [owner, inserted] = owners.try_emplace(
                      model::topic_id_partition{topic, partition}, member.id);
                    EXPECT_TRUE(inserted)
                      << topic_name(topic) << '/' << partition
                      << " assigned to both " << owner->second() << " and "
                      << member.id();
                }
            }
        }

        for (const auto& topic : subscribed) {
            auto num_partitions = _describer.num_partitions(topic);
            ASSERT_TRUE(num_partitions.has_value());
            for (auto id : std::views::iota(0, *num_partitions)) {
                EXPECT_TRUE(owners.contains(
                  model::topic_id_partition{topic, model::partition_id{id}}))
                  << topic_name(topic) << '/' << id << " was left unassigned";
            }
        }

        if (
          group.type() == subscription_type::homogeneous
          && !group.members().empty()) {
            auto [min, max] = std::ranges::minmax(sizes(assignment));
            EXPECT_LE(max - min, 1) << "unbalanced: " << render(assignment);
        }
    }

private:
    std::string topic_name(model::topic_id topic) const {
        auto it = _names.find(topic);
        return it == _names.end() ? fmt::to_string(topic) : it->second;
    }

    test_describer _describer;
    absl::btree_map<model::topic_id, std::string> _names;
    uint16_t _topic_count{0};
    uniform_assignor _assignor;
    /// The spec of the last `assign` call, which an assignment is positional
    /// against.
    std::optional<group_spec> _assigned_group;
};

TEST_F(uniform_assignor_test, is_the_assignor_named_uniform) {
    // The name a group selects this assignor by, and the one entry in
    // `group_consumer_assignors`.
    EXPECT_EQ(uniform_assignor{}.name(), "uniform");
}

TEST_F(uniform_assignor_test, empty_group_is_assigned_nothing) {
    auto topic = add_topic(3);
    std::ignore = topic;

    auto assignment = assign(spec());

    ASSERT_TRUE(assignment.has_value()) << "expected an assignment";
    EXPECT_EQ(assignment->size(), 0) << render(*assignment);
}

TEST_F(uniform_assignor_test, sole_member_takes_every_partition) {
    auto topic = add_topic(3);

    auto assignment = assign(spec(member("m1", {topic})));

    ASSERT_TRUE(assignment.has_value());
    expect_assignment(
      *assignment,
      {
        /* m1 */ {{topic, {0, 1, 2}}},
      });
}

TEST_F(uniform_assignor_test, partitions_are_split_evenly) {
    auto topic = add_topic(6);

    auto assignment = assign(spec(
      member("m1", {topic}), member("m2", {topic}), member("m3", {topic})));

    ASSERT_TRUE(assignment.has_value());
    expect_assignment(
      *assignment,
      {
        /* m1 */ {{topic, {0, 1}}},
        /* m2 */ {{topic, {2, 3}}},
        /* m3 */ {{topic, {4, 5}}},
      });
}

TEST_F(uniform_assignor_test, leftover_partitions_are_spread_one_each) {
    auto topic = add_topic(7);

    auto assignment = assign(spec(
      member("m1", {topic}), member("m2", {topic}), member("m3", {topic})));

    ASSERT_TRUE(assignment.has_value());
    EXPECT_EQ(sizes(*assignment), (std::vector<size_t>{2, 2, 3}))
      << render(*assignment);
}

TEST_F(
  uniform_assignor_test, member_subscribed_to_nothing_is_assigned_nothing) {
    add_topic(3);

    auto assignment = assign(spec(member("m1", {})));

    ASSERT_TRUE(assignment.has_value());
    // The member still gets an entry, holding nothing.
    EXPECT_EQ(sizes(*assignment), (std::vector<size_t>{0}))
      << render(*assignment);
}

TEST_F(uniform_assignor_test, an_assignment_runs_parallel_to_the_members) {
    auto topic = add_topic(4);

    auto group = spec(member("m2", {topic}), member("m1", {topic}));
    auto assignment = assign(group);

    ASSERT_TRUE(assignment.has_value());
    // One entry per member, in the spec's member order rather than by member
    // id.
    ASSERT_EQ(assignment->size(), 2);
    expect_assignment(
      *assignment,
      {
        /* m2 */ {{topic, {0, 1}}},
        /* m1 */ {{topic, {2, 3}}},
      });
}

TEST_F(uniform_assignor_test, assigned_partitions_within_target_size_are_kept) {
    auto topic = add_topic(6);

    auto assignment = assign(
      spec(member("m1", {topic}, {{topic, {0, 1, 2}}}), member("m2", {topic})));

    ASSERT_TRUE(assignment.has_value());
    expect_assignment(
      *assignment,
      {
        /* m1 */ {{topic, {0, 1, 2}}},
        /* m2 */ {{topic, {3, 4, 5}}},
      });
}

TEST_F(uniform_assignor_test, partitions_beyond_target_size_are_released) {
    auto topic = add_topic(6);

    auto assignment = assign(spec(
      member("m1", {topic}, {{topic, {0, 1, 2, 3, 4, 5}}}),
      member("m2", {topic})));

    ASSERT_TRUE(assignment.has_value());
    expect_assignment(
      *assignment,
      {
        /* m1 */ {{topic, {0, 1, 2}}},
        /* m2 */ {{topic, {3, 4, 5}}},
      });
}

TEST_F(
  uniform_assignor_test,
  a_topic_is_released_whole_when_the_target_size_runs_out) {
    auto t1 = add_topic(2);
    auto t2 = add_topic(2);

    // m1's target size is two, spent entirely on t1, so it gives up the one
    // partition of t2 it holds.
    auto group = spec(
      member("m1", {t1, t2}, {{t1, {0, 1}}, {t2, {0}}}),
      member("m2", {t1, t2}));

    auto assignment = assign(group);

    ASSERT_TRUE(assignment.has_value());
    expect_assignment(
      *assignment,
      {
        /* m1 */ {{t1, {0, 1}}},
        /* m2 */ {{t2, {0, 1}}},
      });
}

TEST_F(uniform_assignor_test, a_partition_past_the_end_of_a_topic_fails) {
    auto topic = add_topic(2);

    // The metadata has two partitions of t1 while m1 is assigned a third, so
    // the metadata is behind the group's assignment. Assigning on the metadata
    // alone would revoke that partition, so the assignor fails instead.
    auto group = spec(
      member("m1", {topic}, {{topic, {0, 5}}}), member("m2", {topic}));

    auto assignment = assign(group);

    ASSERT_FALSE(assignment.has_value()) << render(*assignment);
    EXPECT_EQ(assignment.error().errc, assignor_errc::unknown_partition);
    EXPECT_EQ(assignment.error().topic, topic);
    EXPECT_EQ(assignment.error().partition, model::partition_id{5});
}

TEST_F(uniform_assignor_test, a_negative_partition_is_malformed) {
    auto topic = add_topic(2);

    // Corrupt input: no topic numbers its partitions from below zero, so this
    // is not stale metadata. The check runs before the target-size arithmetic,
    // which would otherwise keep it and hand it back in the assignment.
    auto group = spec(
      member("m1", {topic}, {{topic, {-1, 0}}}), member("m2", {topic}));

    auto assignment = assign(group);

    ASSERT_FALSE(assignment.has_value()) << render(*assignment);
    EXPECT_EQ(assignment.error().errc, assignor_errc::malformed_assignment);
    EXPECT_EQ(assignment.error().topic, topic);
    EXPECT_EQ(assignment.error().partition, model::partition_id{-1});
}

TEST_F(
  uniform_assignor_test, a_subscription_out_of_topic_id_order_is_malformed) {
    auto t1 = add_topic(2);
    auto t2 = add_topic(2);

    // Both members share the backwards list, so the group is homogeneous.
    auto group = spec(
      member_as_given("m1", {t2, t1}), member_as_given("m2", {t2, t1}));

    auto assignment = assign(group);

    ASSERT_FALSE(assignment.has_value()) << render(*assignment);
    EXPECT_EQ(assignment.error().errc, assignor_errc::malformed_subscription);
    EXPECT_EQ(assignment.error().topic, t1);
}

TEST_F(
  uniform_assignor_test, a_subscription_with_a_repeated_topic_is_malformed) {
    auto topic = add_topic(4);

    // Without the check the repeated topic counts its partitions twice, and
    // the deal phase has exactly the room to assign every partition to two
    // members.
    auto group = spec(
      member_as_given("m1", {topic, topic}),
      member_as_given("m2", {topic, topic}));

    auto assignment = assign(group);

    ASSERT_FALSE(assignment.has_value()) << render(*assignment);
    EXPECT_EQ(assignment.error().errc, assignor_errc::malformed_subscription);
    EXPECT_EQ(assignment.error().topic, topic);
}

TEST_F(
  uniform_assignor_test, an_assignment_out_of_topic_id_order_is_malformed) {
    auto t1 = add_topic(2);
    auto t2 = add_topic(2);

    member_assignment holds;
    holds.assign(t2, model::partition_id{0});
    holds.assign(t1, model::partition_id{0});
    auto group = spec(
      member_as_given("m1", {t1, t2}, std::move(holds)),
      member("m2", {t1, t2}));

    auto assignment = assign(group);

    ASSERT_FALSE(assignment.has_value()) << render(*assignment);
    EXPECT_EQ(assignment.error().errc, assignor_errc::malformed_assignment);
    EXPECT_EQ(assignment.error().topic, t1);
}

TEST_F(uniform_assignor_test, assigned_partitions_out_of_order_are_malformed) {
    auto topic = add_topic(4);

    member_assignment holds;
    holds.assign(topic, model::partition_id{2});
    holds.assign(topic, model::partition_id{0});
    auto group = spec(
      member_as_given("m1", {topic}, std::move(holds)), member("m2", {topic}));

    auto assignment = assign(group);

    ASSERT_FALSE(assignment.has_value()) << render(*assignment);
    EXPECT_EQ(assignment.error().errc, assignor_errc::malformed_assignment);
    EXPECT_EQ(assignment.error().topic, topic);
    EXPECT_EQ(assignment.error().partition, model::partition_id{0});
}

TEST_F(uniform_assignor_test, a_repeated_assigned_partition_is_malformed) {
    auto topic = add_topic(4);

    member_assignment holds;
    holds.assign(topic, model::partition_id{1});
    holds.assign(topic, model::partition_id{1});
    auto group = spec(
      member_as_given("m1", {topic}, std::move(holds)), member("m2", {topic}));

    auto assignment = assign(group);

    ASSERT_FALSE(assignment.has_value()) << render(*assignment);
    EXPECT_EQ(assignment.error().errc, assignor_errc::malformed_assignment);
    EXPECT_EQ(assignment.error().topic, topic);
    EXPECT_EQ(assignment.error().partition, model::partition_id{1});
}

TEST_F(uniform_assignor_test, partitions_of_unsubscribed_topics_are_dropped) {
    auto subscribed = add_topic(2);
    auto dropped = add_topic(2);

    auto assignment = assign(
      spec(member("m1", {subscribed}, {{subscribed, {0}}, {dropped, {0, 1}}})));

    ASSERT_TRUE(assignment.has_value());
    expect_assignment(
      *assignment,
      {
        /* m1 */ {{subscribed, {0, 1}}},
      });
}

TEST_F(uniform_assignor_test, partitions_of_a_departed_member_are_shared_out) {
    auto topic = add_topic(6);

    // The group holds four of the topic's six partitions, so 4 and 5 have no
    // owner, which is the state a member's departure leaves behind.
    auto assignment = assign(spec(
      member("m1", {topic}, {{topic, {0, 1}}}),
      member("m2", {topic}, {{topic, {2, 3}}})));

    ASSERT_TRUE(assignment.has_value());
    expect_assignment(
      *assignment,
      {
        /* m1 */ {{topic, {0, 1, 4}}},
        /* m2 */ {{topic, {2, 3, 5}}},
      });
}

TEST_F(uniform_assignor_test, the_extra_partition_stays_with_a_full_member) {
    auto topic = add_topic(7);

    // A group of three splits 7 partitions 3/2/2. m1 already holds 3 of them,
    // so it keeps all 3 and the other two members split the rest.
    auto assignment = assign(spec(
      member("m1", {topic}, {{topic, {0, 1, 2}}}),
      member("m2", {topic}, {{topic, {3}}}),
      member("m3", {topic})));

    ASSERT_TRUE(assignment.has_value());
    expect_assignment(
      *assignment,
      {
        /* m1 */ {{topic, {0, 1, 2}}},
        /* m2 */ {{topic, {3, 4}}},
        /* m3 */ {{topic, {5, 6}}},
      });
}

TEST_F(uniform_assignor_test, the_split_counts_every_subscribed_topic) {
    auto t1 = add_topic(2);
    auto t2 = add_topic(3);

    // Five partitions across the two topics divide 2 and 3 between the
    // members, so a member's share spans both topics.
    auto assignment = assign(
      spec(member("m1", {t1, t2}), member("m2", {t1, t2})));

    ASSERT_TRUE(assignment.has_value());
    EXPECT_EQ(sizes(*assignment), (std::vector<size_t>{2, 3}))
      << render(*assignment);
}

TEST_F(uniform_assignor_test, subscription_to_a_topic_without_metadata_fails) {
    auto missing = unknown_topic();

    auto assignment = assign(spec(member("m1", {missing})));

    ASSERT_FALSE(assignment.has_value()) << render(*assignment);
    EXPECT_EQ(assignment.error().errc, assignor_errc::unknown_topic);
    EXPECT_EQ(assignment.error().topic, missing);
}

TEST_F(uniform_assignor_test, a_negative_partition_count_fails) {
    auto topic = add_topic(-1);

    auto assignment = assign(spec(member("m1", {topic})));

    ASSERT_FALSE(assignment.has_value()) << render(*assignment);
    EXPECT_EQ(assignment.error().errc, assignor_errc::invalid_partition_count);
    EXPECT_EQ(assignment.error().topic, topic);
}

TEST_F(uniform_assignor_test, a_partition_assigned_to_two_members_fails) {
    // Malformed input: the caller must supply an assignment with one owner per
    // partition. The homogeneous target-size arithmetic notices that it cannot
    // place every partition, and fails rather than build a broken assignment.
    auto topic = add_topic(4);

    auto assignment = assign(spec(
      member("m1", {topic}, {{topic, {0, 1}}}),
      member("m2", {topic}, {{topic, {1, 2}}})));

    ASSERT_FALSE(assignment.has_value()) << render(*assignment);
    EXPECT_EQ(
      assignment.error().errc, assignor_errc::partitions_left_unassigned);
}
} // namespace
} // namespace kafka
