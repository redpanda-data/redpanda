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
#include "cluster/tx_protocol_types.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "kafka/server/consumer_group.h"
#include "kafka/server/offset_writer.h"
#include "kafka/server/tx_coordinator_client.h"
#include "model/fundamental.h"
#include "raft/errc.h"
#include "test_utils/test.h"

#include <seastar/core/rwlock.hh>
#include <seastar/core/sharded.hh>

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>

namespace kafka {
namespace {

const group_id test_group{"test-group"};

/// The group's offset state is not what these tests cover, so its writer and
/// coordinator client do nothing.
class unused_writer final : public offset_writer {
public:
    model::term_id term() const final { return model::term_id{1}; }

    ss::future<result<raft::replicate_result>>
    replicate(model::record_batch, model::term_id) final {
        return ss::make_exception_future<result<raft::replicate_result>>(
          std::runtime_error("unexpected replicate"));
    }

    ss::future<result<raft::replicate_result>>
    replicate(chunked_vector<model::record_batch>, model::term_id) final {
        return ss::make_exception_future<result<raft::replicate_result>>(
          std::runtime_error("unexpected replicate"));
    }

    raft::replicate_stages replicate_in_stages(
      chunked_vector<model::record_batch>, model::term_id) final {
        return raft::replicate_stages(raft::errc::not_leader);
    }

    ss::future<> maybe_step_down(model::term_id, std::string_view) final {
        return ss::now();
    }
};

class unused_tx_coordinator final : public tx_coordinator_client {
public:
    ss::future<cluster::try_abort_reply> try_abort(
      model::partition_id, model::producer_identity, model::tx_seq) final {
        return ss::make_exception_future<cluster::try_abort_reply>(
          std::runtime_error("unexpected try_abort"));
    }
};

consumer_group_member member_at(
  const ss::sstring& id,
  member_epoch epoch,
  consumer_group_member_state state = consumer_group_member_state::stable) {
    return {
      .id = kafka::member_id(id),
      .subscription = {.client_id = kafka::client_id("c")},
      .assignment = {
        .epoch = epoch,
        .previous_epoch = epoch,
        .state = state,
      },
    };
}

struct consumer_group_test : seastar_test {
    ss::future<> SetUpAsync() override {
        co_await feature_table.start();
        co_await feature_table.invoke_on_all(
          [](features::feature_table& f) { f.testing_activate_all(); });
    }

    ss::future<> TearDownAsync() override {
        co_await group.stop();
        co_await feature_table.stop();
    }

    ss::sharded<features::feature_table> feature_table;

    ss::lw_shared_ptr<ss::rwlock> catchup_lock
      = ss::make_lw_shared<ss::rwlock>();

    consumer_group group{
      test_group,
      config::shard_local_cfg(),
      catchup_lock,
      std::make_unique<unused_writer>(),
      model::term_id(1),
      std::make_unique<unused_tx_coordinator>(),
      feature_table};
};

TEST_F(consumer_group_test, a_new_group_is_empty) {
    ASSERT_EQ(group.id(), test_group);
    ASSERT_EQ(group.epoch(), group_epoch(0));
    ASSERT_EQ(group.assignment_epoch(), assignment_epoch(0));
    ASSERT_TRUE(group.members().empty());
    ASSERT_EQ(group.state(), consumer_group_state::empty);
}

TEST_F(
  consumer_group_test, a_group_with_no_members_is_empty_whatever_the_epoch) {
    // empty wins over assigning: with nobody to assign to, the epochs say
    // nothing about the group.
    group.set_epoch(group_epoch(7));
    ASSERT_EQ(group.state(), consumer_group_state::empty);
}

TEST_F(consumer_group_test, a_group_owed_an_assignment_is_assigning) {
    group.upsert_member(member_at("m1", member_epoch(1)));
    group.set_epoch(group_epoch(2));
    group.set_assignment_epoch(assignment_epoch(1));

    ASSERT_EQ(group.state(), consumer_group_state::assigning);
}

TEST_F(consumer_group_test, a_group_whose_members_lag_the_target_reconciles) {
    group.upsert_member(member_at("m1", member_epoch(2)));
    group.upsert_member(member_at("m2", member_epoch(1)));
    group.set_epoch(group_epoch(2));
    group.set_assignment_epoch(assignment_epoch(2));

    ASSERT_EQ(group.state(), consumer_group_state::reconciling);
}

TEST_F(consumer_group_test, a_group_is_stable_once_every_member_catches_up) {
    group.upsert_member(member_at("m1", member_epoch(2)));
    group.upsert_member(member_at("m2", member_epoch(1)));
    group.set_epoch(group_epoch(2));
    group.set_assignment_epoch(assignment_epoch(2));
    ASSERT_EQ(group.state(), consumer_group_state::reconciling);

    group.upsert_member(member_at("m2", member_epoch(2)));

    ASSERT_EQ(group.state(), consumer_group_state::stable);
}

TEST_F(
  consumer_group_test,
  a_member_at_the_epoch_still_holding_partitions_is_not_reconciled) {
    // The member's epoch was bumped to the target, but it is waiting on
    // partitions their previous owner has not released yet, so the group is
    // still converging even though every epoch matches.
    group.upsert_member(member_at(
      "m1",
      member_epoch(2),
      consumer_group_member_state::unreleased_partitions));
    group.set_epoch(group_epoch(2));
    group.set_assignment_epoch(assignment_epoch(2));

    ASSERT_EQ(group.state(), consumer_group_state::reconciling);

    group.upsert_member(member_at("m1", member_epoch(2)));
    ASSERT_EQ(group.state(), consumer_group_state::stable);
}

TEST_F(consumer_group_test, a_member_owing_partitions_back_is_not_reconciled) {
    group.upsert_member(member_at(
      "m1",
      member_epoch(2),
      consumer_group_member_state::unrevoked_partitions));
    group.set_epoch(group_epoch(2));
    group.set_assignment_epoch(assignment_epoch(2));

    ASSERT_EQ(group.state(), consumer_group_state::reconciling);
}

TEST_F(consumer_group_test, a_removed_group_reports_itself_removed) {
    ASSERT_FALSE(group.removed());
    group.mark_removed();
    ASSERT_TRUE(group.removed());
}

TEST_F(consumer_group_test, upserting_a_member_replaces_it) {
    group.upsert_member(member_at("m1", member_epoch(1)));
    group.upsert_member(member_at("m1", member_epoch(4)));

    ASSERT_EQ(group.members().size(), 1);
    ASSERT_EQ(
      group.members().at(kafka::member_id("m1")).assignment.epoch,
      member_epoch(4));
}

TEST_F(consumer_group_test, erasing_the_last_member_empties_the_group) {
    group.upsert_member(member_at("m1", member_epoch(1)));
    group.upsert_member(member_at("m2", member_epoch(1)));
    group.set_epoch(group_epoch(1));
    group.set_assignment_epoch(assignment_epoch(1));
    ASSERT_EQ(group.state(), consumer_group_state::stable);

    ASSERT_TRUE(group.erase_member(kafka::member_id("m2")));
    ASSERT_EQ(group.state(), consumer_group_state::stable);

    ASSERT_TRUE(group.erase_member(kafka::member_id("m1")));
    ASSERT_EQ(group.state(), consumer_group_state::empty);

    // a member the group does not have
    ASSERT_FALSE(group.erase_member(kafka::member_id("m1")));
}

TEST_F(consumer_group_test, upserting_a_subscription_keeps_the_assignment) {
    group.upsert_member(member_at("m1", member_epoch(3)));

    group.upsert_member_subscription(
      kafka::member_id("m1"),
      consumer_group_subscription{.client_id = kafka::client_id("new")});

    const auto& m = group.members().at(kafka::member_id("m1"));
    ASSERT_EQ(m.subscription.client_id, kafka::client_id("new"));
    ASSERT_EQ(m.assignment.epoch, member_epoch(3));
}

TEST_F(consumer_group_test, upserting_an_assignment_keeps_the_subscription) {
    group.upsert_member_subscription(
      kafka::member_id("m1"),
      consumer_group_subscription{.client_id = kafka::client_id("c")});

    group.upsert_member_assignment(
      kafka::member_id("m1"),
      consumer_group_member_assignment{
        .epoch = member_epoch(2),
        .state = consumer_group_member_state::stable,
      });

    const auto& m = group.members().at(kafka::member_id("m1"));
    ASSERT_EQ(m.subscription.client_id, kafka::client_id("c"));
    ASSERT_EQ(m.assignment.epoch, member_epoch(2));
}

TEST_F(consumer_group_test, either_half_creates_a_missing_member) {
    group.upsert_member_subscription(kafka::member_id("m1"), {});
    group.upsert_member_assignment(kafka::member_id("m2"), {});

    ASSERT_EQ(group.members().size(), 2);
    ASSERT_EQ(group.members().at(kafka::member_id("m1")).id, "m1");
    ASSERT_EQ(group.members().at(kafka::member_id("m2")).id, "m2");
}

TEST_F(consumer_group_test, clearing_an_assignment_keeps_the_member) {
    group.upsert_member(member_at("m1", member_epoch(3)));

    group.clear_member_assignment(kafka::member_id("m1"));

    const auto& m = group.members().at(kafka::member_id("m1"));
    ASSERT_EQ(m.assignment.epoch, member_epoch(0));
    ASSERT_EQ(m.assignment.state, consumer_group_member_state::unknown);

    // a member the group does not have is left alone
    group.clear_member_assignment(kafka::member_id("m2"));
    ASSERT_EQ(group.members().size(), 1);
}

TEST_F(consumer_group_test, the_target_assignment_is_kept_per_member) {
    const auto topic = model::topic_id(uuid_t::create());
    member_partitions target;
    target.try_emplace(topic).first->second.emplace(model::partition_id(0));

    group.set_member_target(kafka::member_id("m1"), std::move(target));
    ASSERT_EQ(group.target_assignment().size(), 1);
    ASSERT_TRUE(group.target_assignment()
                  .at(kafka::member_id("m1"))
                  .at(topic)
                  .contains(model::partition_id(0)));

    // replaced, not merged
    group.set_member_target(kafka::member_id("m1"), {});
    ASSERT_TRUE(group.target_assignment().at(kafka::member_id("m1")).empty());

    ASSERT_TRUE(group.erase_member_target(kafka::member_id("m1")));
    ASSERT_TRUE(group.target_assignment().empty());
    ASSERT_FALSE(group.erase_member_target(kafka::member_id("m1")));
}

TEST_F(consumer_group_test, a_group_with_members_is_not_deletable) {
    ASSERT_TRUE(group.deletable());
    group.upsert_member(member_at("m1", member_epoch(1)));
    ASSERT_FALSE(group.deletable());
    group.erase_member(kafka::member_id("m1"));
    ASSERT_TRUE(group.deletable());
}

TEST_F(consumer_group_test, an_open_transaction_blocks_deletion) {
    group.offsets().apply_tx_fence(
      model::producer_identity{7, 0},
      model::tx_seq(1),
      std::chrono::seconds(30),
      model::partition_id(0),
      model::offset(100));
    ASSERT_FALSE(group.deletable());

    group.offsets().apply_tx_abort(model::producer_identity{7, 0});
    ASSERT_TRUE(group.deletable());
}

TEST_F(consumer_group_test, committed_offsets_do_not_block_deletion) {
    group.offsets().try_upsert_offset(
      model::topic_partition(model::topic("t"), model::partition_id(0)),
      offset_store::offset_metadata{
        .log_offset = model::offset(1),
        .offset = model::offset(42),
        .commit_timestamp = model::timestamp::now(),
      });
    ASSERT_TRUE(group.deletable());
}

TEST_F(consumer_group_test, state_names_match_the_kafka_names) {
    ASSERT_EQ(to_string_view(consumer_group_state::empty), "Empty");
    ASSERT_EQ(to_string_view(consumer_group_state::assigning), "Assigning");
    ASSERT_EQ(to_string_view(consumer_group_state::reconciling), "Reconciling");
    ASSERT_EQ(to_string_view(consumer_group_state::stable), "Stable");
    ASSERT_EQ(to_string_view(consumer_group_state::dead), "Dead");
}

} // namespace
} // namespace kafka
