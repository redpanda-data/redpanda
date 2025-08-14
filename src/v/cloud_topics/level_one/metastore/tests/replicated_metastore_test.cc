/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "cloud_topics/tests/cluster_fixture.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "model/fundamental.h"
#include "model/namespace.h"

using namespace experimental::cloud_topics::l1;

namespace {
using o = kafka::offset;
using ts = model::timestamp;
MATCHER_P2(MatchesRange, base, last, "") {
    return arg.base_offset == base && arg.last_offset == last;
}

} // namespace
class ReplicatedMetastoreTest
  : public experimental::cloud_topics::cluster_fixture
  , public ::testing::Test {
public:
    static constexpr size_t num_brokers = 3;
    void SetUp() override {
        for (size_t i = 0; i < num_brokers; i++) {
            add_node();
        }
        wait_for_all_members(5s).get();
    }
    model::topic_id_partition make_tp(int i) {
        return model::topic_id_partition::from(
          fmt::format("deadbeef-aaaa-0000-0000-000000000000/{}", i));
    }

    // Create an object builder with objects for the given number of
    // partitions, one per partition.
    void create_initial_objects(
      replicated_metastore& meta,
      int partitions_count,
      int last_offset,
      std::unique_ptr<metastore::object_metadata_builder>* objs) {
        auto ret = meta.object_builder();
        for (int i = 0; i < partitions_count; ++i) {
            auto tp = make_tp(i);
            auto oid = ret->get_or_create_object_for(tp);
            auto add_res = ret->add(
              oid,
              metastore::object_metadata::ntp_metadata{
                .tidp = tp,
                .base_offset = o{0},
                .last_offset = o{last_offset},
                .max_timestamp = ts{10000},
                .pos = 0,
                .size = 500,
              });
            ASSERT_TRUE(add_res.has_value()) << add_res.error();
            auto fin_res = ret->finish(oid, 500);
            ASSERT_TRUE(fin_res.has_value()) << fin_res.error();
        }
        *objs = std::move(ret);
    }

    // Add objects for the given number of partitions, one per partition.
    ss::future<> add_initial_objects(
      replicated_metastore& meta, int partitions_count, int last_offset) {
        std::unique_ptr<metastore::object_metadata_builder> objs;
        ASSERT_NO_FATAL_FAILURE_CORO(
          create_initial_objects(meta, partitions_count, last_offset, &objs));
        auto add_res = meta.add_objects(std::move(objs)).get();
        ASSERT_TRUE_CORO(add_res.has_value());
    }
};

TEST_F(ReplicatedMetastoreTest, TestAddNotFinished) {
    auto& app = get_ct_app(model::node_id{0});
    replicated_metastore meta(app.get_sharded_l1_metastore_fe()->local());
    auto tp = make_tp(0);
    auto obj_builder = meta.object_builder();
    auto oid = obj_builder->get_or_create_object_for(tp);
    auto add_res = obj_builder->add(
      oid,
      metastore::object_metadata::ntp_metadata{
        .tidp = tp,
        .base_offset = o{0},
        .last_offset = o{99},
        .max_timestamp = ts{10000},
        .pos = 0,
        .size = 500,
      });
    ASSERT_TRUE(add_res.has_value()) << add_res.error();
    auto commit_res = meta.add_objects(std::move(obj_builder)).get();
    ASSERT_FALSE(commit_res.has_value());
    ASSERT_EQ(commit_res.error(), metastore::errc::invalid_request);
}

TEST_F(ReplicatedMetastoreTest, TestBasicAdd) {
    auto& app = get_ct_app(model::node_id{0});
    replicated_metastore meta(app.get_sharded_l1_metastore_fe()->local());

    constexpr auto partitions_count = 100;
    ASSERT_NO_FATAL_FAILURE(
      add_initial_objects(meta, partitions_count, 999).get());

    // Check across the partitions of the L1 metastore that we have the right
    // number of partitions.
    size_t state_partitions_count = 0;
    size_t state_extents_count = 0;
    for (int i = 0; i < 3; ++i) {
        auto l1_stm = get_l1_stm(model::partition_id{i});
        ASSERT_TRUE(l1_stm != nullptr);
        auto& l1_state = l1_stm->state();
        ASSERT_EQ(l1_state.topic_to_state.size(), 1);

        auto& p_states = l1_state.topic_to_state.begin()->second.pid_to_state;
        state_partitions_count += p_states.size();

        // Mathematically won't be empty. This is a check that our partitioning
        // logic is working.
        EXPECT_FALSE(p_states.empty());
        for (const auto& [pid, p_state] : p_states) {
            state_extents_count += p_state.extents.size();
            EXPECT_FALSE(p_state.compaction_state.has_value());
        }
    }
    EXPECT_EQ(state_partitions_count, partitions_count);
    EXPECT_EQ(state_extents_count, partitions_count);

    // Validate the metastore get paths.
    for (int i = 0; i < partitions_count; ++i) {
        auto tp = make_tp(i);
        auto offsets = meta.get_offsets(tp).get();
        ASSERT_TRUE(offsets.has_value());
        ASSERT_EQ(offsets->next_offset, o{1000});

        // Check that our offsets seem sane.
        for (int off : {0, 999}) {
            auto offset_ge_obj = meta.get_first_ge(tp, o{off}).get();
            ASSERT_TRUE(offset_ge_obj.has_value());
        }
        // Going past the end should result in an error.
        auto offset_ge_obj = meta.get_first_ge(tp, o{1000}).get();
        ASSERT_FALSE(offset_ge_obj.has_value());
        ASSERT_EQ(offset_ge_obj.error(), metastore::errc::out_of_range);

        // Check that our timestamps seem sane.
        for (int t : {0, 10000}) {
            auto offset_ts_obj = meta.get_first_ge(tp, ts{t}).get();
            ASSERT_TRUE(offset_ts_obj.has_value());
        }
        // Going past the end should result in an error.
        auto offset_ts_obj = meta.get_first_ge(tp, ts{10001}).get();
        ASSERT_FALSE(offset_ts_obj.has_value());
        ASSERT_EQ(offset_ts_obj.error(), metastore::errc::out_of_range);

        // Sanity check the compaction offsets. Since no range is cleaned, the
        // entire log is dirty.
        auto cmp_offsets
          = meta.get_compaction_offsets(tp, model::timestamp::now()).get();
        ASSERT_TRUE(cmp_offsets.has_value());
        EXPECT_FALSE(cmp_offsets->dirty_ranges.empty());
        EXPECT_THAT(
          cmp_offsets->dirty_ranges.to_vec(),
          testing::ElementsAre(MatchesRange(o{0}, o{999})));
    }
}

TEST_F(ReplicatedMetastoreTest, TestBasicCompact) {
    auto& app = get_ct_app(model::node_id{0});
    replicated_metastore meta(app.get_sharded_l1_metastore_fe()->local());

    constexpr auto partitions_count = 100;
    ASSERT_NO_FATAL_FAILURE(
      add_initial_objects(meta, partitions_count, 999).get());

    // Compact every partition.
    std::unique_ptr<metastore::object_metadata_builder> new_objs;
    ASSERT_NO_FATAL_FAILURE(
      create_initial_objects(meta, partitions_count, 999, &new_objs));
    metastore::compaction_map_t cmap;
    for (int i = 0; i < partitions_count; ++i) {
        metastore::compaction_update update;
        update.cleaned_at = model::timestamp::now();
        update.new_cleaned_range.emplace();
        update.new_cleaned_range->base_offset = o{0};
        update.new_cleaned_range->last_offset = o{999};
        cmap[make_tp(i)] = std::move(update);
    }
    auto cmp_res = meta.compact_objects(std::move(new_objs), cmap).get();
    ASSERT_TRUE(cmp_res.has_value()) << fmt::to_string(cmp_res.error());

    // Check across the partitions of the L1 metastore that we have the right
    // number of partitions.
    size_t state_partitions_count = 0;
    size_t state_extents_count = 0;
    for (int i = 0; i < 3; ++i) {
        auto l1_stm = get_l1_stm(model::partition_id{i});
        ASSERT_TRUE(l1_stm != nullptr);
        auto& l1_state = l1_stm->state();
        ASSERT_EQ(l1_state.topic_to_state.size(), 1);

        auto& p_states = l1_state.topic_to_state.begin()->second.pid_to_state;
        state_partitions_count += p_states.size();

        // Mathematically won't be empty. This is a check that our partitioning
        // logic is working.
        EXPECT_FALSE(p_states.empty());
        for (const auto& [pid, p_state] : p_states) {
            state_extents_count += p_state.extents.size();

            // We should have compaction state in every partition.
            EXPECT_TRUE(p_state.compaction_state.has_value());
        }
    }
    EXPECT_EQ(state_partitions_count, partitions_count);
    EXPECT_EQ(state_extents_count, partitions_count);

    // Validate the metastore get paths.
    for (int i = 0; i < partitions_count; ++i) {
        auto tp = make_tp(i);
        auto offsets = meta.get_offsets(tp).get();
        ASSERT_TRUE(offsets.has_value());
        ASSERT_EQ(offsets->next_offset, o{1000});

        // Check that our offsets seem sane.
        for (int off : {0, 999}) {
            auto offset_ge_obj = meta.get_first_ge(tp, o{off}).get();
            ASSERT_TRUE(offset_ge_obj.has_value());
        }
        // Going past the end should result in an error.
        auto offset_ge_obj = meta.get_first_ge(tp, o{1000}).get();
        ASSERT_FALSE(offset_ge_obj.has_value());
        ASSERT_EQ(offset_ge_obj.error(), metastore::errc::out_of_range);

        // Check that our timestamps seem sane.
        for (int t : {0, 10000}) {
            auto offset_ts_obj = meta.get_first_ge(tp, ts{t}).get();
            ASSERT_TRUE(offset_ts_obj.has_value());
        }
        // Going past the end should result in an error.
        auto offset_ts_obj = meta.get_first_ge(tp, ts{10001}).get();
        ASSERT_FALSE(offset_ts_obj.has_value());
        ASSERT_EQ(offset_ts_obj.error(), metastore::errc::out_of_range);

        // Now check the compacted offsets. Since we've cleaned everything, we
        // should see the dirty ranges be empty.
        auto cmp_offsets
          = meta.get_compaction_offsets(tp, model::timestamp::now()).get();
        ASSERT_TRUE(cmp_offsets.has_value());
        EXPECT_TRUE(cmp_offsets->dirty_ranges.empty())
          << fmt::format("{} is not cleaned", tp);
    }
}

TEST_F(ReplicatedMetastoreTest, TestMissingNTP) {
    auto& app = get_ct_app(model::node_id{0});
    replicated_metastore meta(app.get_sharded_l1_metastore_fe()->local());
    auto offsets = meta.get_offsets(make_tp(0)).get();
    ASSERT_FALSE(offsets.has_value());
    ASSERT_EQ(offsets.error(), metastore::errc::missing_ntp);
}

TEST_F(ReplicatedMetastoreTest, TestNotLeader) {
    auto& app = get_ct_app(model::node_id{0});
    replicated_metastore meta(app.get_sharded_l1_metastore_fe()->local());
    auto tp = make_tp(0);

    auto meta_pid
      = app.get_sharded_l1_metastore_fe()->local().metastore_partition(tp);
    auto meta_ntp = model::ntp{
      model::kafka_internal_namespace,
      model::l1_metastore_topic,
      *meta_pid,
    };

    // Shuffle leadership of the metastore partition around.
    ss::gate gate;
    auto shuffle_loop = ssx::spawn_with_gate_then(gate, [this, meta_ntp] {
        return shuffle_leadership(meta_ntp).then(
          [] { return ss::sleep(300ms); });
    });

    // Run a bunch fo add operations and keep going until we see some errors
    // and successes.
    size_t error_count = 0;
    size_t success_count = 0;
    auto deadline = ss::lowres_clock::now() + 30s;
    bool timed_out = false;
    kafka::offset next_to_send{0};
    while (error_count <= 10 && success_count <= 10) {
        if (ss::lowres_clock::now() > deadline) {
            timed_out = true;
            break;
        }
        auto obj_builder = meta.object_builder();
        auto oid = obj_builder->get_or_create_object_for(tp);
        kafka::offset next_last{next_to_send() + 99};
        auto add_res = obj_builder->add(
          oid,
          metastore::object_metadata::ntp_metadata{
            .tidp = tp,
            .base_offset = next_to_send,
            .last_offset = next_last,
            .max_timestamp = ts{10000},
            .pos = 0,
            .size = 500,
          });
        ASSERT_TRUE(add_res.has_value()) << add_res.error();
        auto fin_res = obj_builder->finish(oid, 500);
        ASSERT_TRUE(fin_res.has_value()) << fin_res.error();

        auto commit_res = meta.add_objects(std::move(obj_builder)).get();
        if (!commit_res.has_value()) {
            while (true) {
                // If there's an error, reset our expected next offset with
                // whatever is actually in L1.
                auto get_res = meta.get_offsets(tp).get();
                if (!get_res.has_value()) {
                    ss::sleep(100ms).get();
                    continue;
                }
                next_to_send = get_res.value().next_offset;
            }
            ++error_count;
            continue;
        }
        next_to_send = kafka::next_offset(next_last);
        ++success_count;
    }
    gate.close().get();
    shuffle_loop.get();
    ASSERT_FALSE(timed_out);

    // Check the validity of the resulting state -- that it's contiguous with
    // no gaps or overlap.
    auto l1_stm = get_l1_stm(*meta_pid);
    ASSERT_TRUE(l1_stm != nullptr);
    auto& l1_state = l1_stm->state();
    ASSERT_EQ(l1_state.topic_to_state.size(), 1);

    auto& p_states = l1_state.topic_to_state.begin()->second.pid_to_state;
    ASSERT_EQ(1, p_states.size());
    auto& extents = p_states.begin()->second.extents;
    ASSERT_GE(extents.size(), 10);

    EXPECT_EQ(extents.begin()->base_offset, o{0});
    EXPECT_EQ(extents.begin()->last_offset, o{99});
    auto expected_next = o{0};
    for (const auto& e : extents) {
        EXPECT_EQ(expected_next, e.base_offset);
        expected_next = kafka::next_offset(e.last_offset);
    }
}
