/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_label.h"
#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "cloud_topics/level_one/metastore/manifest_io.h"
#include "cloud_topics/level_one/metastore/metastore_manifest.h"
#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "cloud_topics/level_one/metastore/tests/state_utils.h"
#include "cloud_topics/tests/cluster_fixture.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "lsm/io/cloud_persistence.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "serde/rw/rw.h"

using namespace cloud_topics::l1;

namespace {
using o = kafka::offset;
using ts = model::timestamp;
MATCHER_P2(MatchesRange, base, last, "") {
    return arg.base_offset == base && arg.last_offset == last;
}

} // namespace

enum class metastore_backend { simple, lsm };

class ReplicatedMetastoreTest
  : public cloud_topics::cluster_fixture
  , public ::testing::TestWithParam<metastore_backend> {
public:
    static constexpr size_t num_brokers = 3;

    bool is_lsm_backend() const { return GetParam() == metastore_backend::lsm; }

    cloud_topics::test_fixture_cfg fixture_cfg() const {
        return {
          .use_lsm_metastore = is_lsm_backend(),
          // Skip flushing since tests may exercise flushing.
          .skip_flush_loop = true,
        };
    }
    void SetUp() override {
        set_configuration("enable_leader_balancer", false);
        for (size_t i = 0; i < num_brokers; i++) {
            add_node(fixture_cfg());
        }
        wait_for_all_members(5s).get();
    }

    // For simple_stm, this returns the state directly.
    // For lsm_stm, this reconstructs state from the database.
    state get_stm_state(model::partition_id pid) {
        if (!is_lsm_backend()) {
            auto stm = get_l1_stm(pid);
            if (!stm) {
                return {};
            }
            return stm->state().copy();
        }
        auto lsm_stm = get_l1_lsm_stm(pid);
        if (!lsm_stm) {
            return {};
        }
        auto* remote
          = &get_node_application(model::node_id{0})->cloud_io.local();
        return lsm_state_to_state(lsm_stm->state(), remote);
    }

private:
    // Build the state by loading the cloud database and then replaying the
    // rows in the volatile buffer.
    state
    lsm_state_to_state(const lsm_state& lsm_st, cloud_io::remote* remote) {
        // Create a database using cloud metadata persistence pointing at the
        // LSM state's domain prefix in the bucket.
        auto domain_prefix = cloud_storage_clients::object_key{
          domain_cloud_prefix(lsm_st.domain_uuid)};
        auto meta_persist = lsm::io::open_cloud_metadata_persistence(
                              remote, bucket_name, domain_prefix)
                              .get();
        auto db = lsm::database::open(
                    {.database_epoch = 0},
                    lsm::io::persistence{
                      .data = lsm::io::make_memory_data_persistence(),
                      .metadata = std::move(meta_persist),
                    })
                    .get();

        // Apply volatile buffer rows to the database.
        if (!lsm_st.volatile_buffer.empty()) {
            auto wb = db.create_write_batch();
            auto max_persisted = db.max_persisted_seqno();
            for (const auto& vol_row : lsm_st.volatile_buffer) {
                if (max_persisted && vol_row.seqno <= *max_persisted) {
                    // Already applied, skip.
                    continue;
                }
                if (vol_row.row.value.empty()) {
                    wb.remove(vol_row.row.key, vol_row.seqno);
                } else {
                    wb.put(
                      vol_row.row.key, vol_row.row.value.copy(), vol_row.seqno);
                }
            }
            db.apply(std::move(wb)).get();
        }

        // Extract state from database snapshot
        auto result = snapshot_to_state(db);
        db.close().get();
        return result;
    }

public:
    model::topic_id_partition make_tp(int i) {
        return model::topic_id_partition::from(
          fmt::format("deadbeef-aaaa-0000-0000-000000000000/{}", i));
    }

    model::topic_id_partition make_topic_p0(int t) {
        return model::topic_id_partition::from(
          fmt::format("deadbeef-aaaa-0000-0000-{:012x}/0", t));
    };
    // Create an object builder with objects for the given number of
    // partitions, one per partition.
    void create_initial_objects(
      replicated_metastore& meta,
      int partitions_count,
      int last_offset,
      std::unique_ptr<metastore::object_metadata_builder>* objs) {
        auto ret = meta.object_builder().get().value();
        for (int i = 0; i < partitions_count; ++i) {
            auto tp = make_tp(i);
            auto oid = ret->get_or_create_object_for(tp).get().value();
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
            auto fin_res = ret->finish(oid, 500, 1000);
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
        metastore::term_offset_map_t terms;
        for (int i = 0; i < partitions_count; ++i) {
            terms[make_tp(i)].emplace_back(
              metastore::term_offset{
                .term = model::term_id{0}, .first_offset = o{0}});
        }
        auto add_res = meta.add_objects(*objs, terms).get();
        ASSERT_TRUE_CORO(add_res.has_value());
    }

    ss::future<> add_objects_for_topics(
      replicated_metastore& meta, int topics_count, int last_offset) {
        auto obj_builder = meta.object_builder().get().value();
        metastore::term_offset_map_t terms;
        for (int i = 0; i < topics_count; ++i) {
            auto tp = make_topic_p0(i);
            auto oid
              = (co_await obj_builder->get_or_create_object_for(tp)).value();
            auto add_res = obj_builder->add(
              oid,
              metastore::object_metadata::ntp_metadata{
                .tidp = tp,
                .base_offset = o{0},
                .last_offset = o{last_offset},
                .max_timestamp = ts{10000},
                .pos = 0,
                .size = 500,
              });
            ASSERT_TRUE_CORO(add_res.has_value()) << add_res.error();
            auto fin_res = obj_builder->finish(oid, 500, 1000);
            ASSERT_TRUE_CORO(fin_res.has_value()) << fin_res.error();

            terms[tp].emplace_back(
              metastore::term_offset{
                .term = model::term_id{0}, .first_offset = o{0}});
        }

        auto add_res = co_await meta.add_objects(*obj_builder, terms);
        ASSERT_TRUE_CORO(add_res.has_value());
    }
};

TEST_P(ReplicatedMetastoreTest, TestMissingMetastore) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();
    auto obj_builder = meta.object_builder().get().value();
    auto& tp_fe = get_node_application(model::node_id{0})
                    ->controller->get_topics_frontend()
                    .local();
    tp_fe.dispatch_delete_topics({model::l1_metastore_nt}, 10s).get();
    for (const auto& node_id : instance_ids()) {
        auto& tp_state = get_node_application(node_id)
                           ->controller->get_topics_state()
                           .local();
        RPTEST_REQUIRE_EVENTUALLY(10s, [&tp_state] {
            return !tp_state.contains(model::l1_metastore_nt);
        });
    }

    // We won't be able to find the partition of the metastore topic because it
    // doesn't exist.
    auto tp = make_tp(0);
    auto oid = obj_builder->get_or_create_object_for(tp).get();
    ASSERT_FALSE(oid.has_value());

    // Adding an object should fail immediately too because the metastore topic
    // doesn't exist and we don't know how to partition objects.
    auto add_res = obj_builder->add(create_object_id(), {});
    ASSERT_FALSE(add_res.has_value());

    // Creating an object builder should attempt to create the metastore topic
    // since it doesn't exist. After the topic is recreated, leader election may
    // not have completed yet, so retry until get_or_create_object_for succeeds.
    auto builder_res = meta.object_builder().get();
    ASSERT_TRUE(builder_res.has_value());
    auto new_builder = std::move(builder_res).value();
    RPTEST_REQUIRE_EVENTUALLY(10s, [&new_builder, &tp] {
        return new_builder->get_or_create_object_for(tp).then(
          [](auto r) { return r.has_value(); });
    });
}

TEST_P(ReplicatedMetastoreTest, TestAddNotFinished) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();
    auto tp = make_tp(0);
    auto obj_builder = meta.object_builder().get().value();
    auto oid = obj_builder->get_or_create_object_for(tp).get().value();
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
    auto commit_res = meta.add_objects(*obj_builder, {}).get();
    ASSERT_FALSE(commit_res.has_value());
    ASSERT_EQ(commit_res.error(), metastore::errc::invalid_request);
}

TEST_P(ReplicatedMetastoreTest, TestBuilderRejectsInvertedOffsets) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();
    auto tp = make_tp(0);
    auto obj_builder = meta.object_builder().get().value();
    auto oid = obj_builder->get_or_create_object_for(tp).get().value();

    // base_offset > last_offset should be rejected.
    auto add_res = obj_builder->add(
      oid,
      metastore::object_metadata::ntp_metadata{
        .tidp = tp,
        .base_offset = o{99},
        .last_offset = o{0},
        .max_timestamp = ts{10000},
        .pos = 0,
        .size = 500,
      });
    ASSERT_FALSE(add_res.has_value());

    // A valid extent should still be accepted.
    auto add_res2 = obj_builder->add(
      oid,
      metastore::object_metadata::ntp_metadata{
        .tidp = tp,
        .base_offset = o{0},
        .last_offset = o{99},
        .max_timestamp = ts{10000},
        .pos = 0,
        .size = 500,
      });
    ASSERT_TRUE(add_res2.has_value()) << add_res2.error();
}

TEST_P(ReplicatedMetastoreTest, TestBuilderRemovedObjects) {
    auto& app = get_ct_app(model::node_id{0});
    auto& m = app.get_sharded_replicated_metastore()->local();
    auto tp = make_tp(0);
    auto ob = m.object_builder().get().value();

    // pending object can be removed, but not twice
    auto oid = ob->get_or_create_object_for(tp).get().value();
    ASSERT_TRUE(ob->remove_pending_object(oid).has_value());
    ASSERT_FALSE(ob->remove_pending_object(oid).has_value());

    // after removal, object id shouldn't be reused in this builder
    auto oid2 = ob->get_or_create_object_for(tp).get().value();
    ASSERT_NE(oid, oid2);
    oid = oid2;

    // unfinished object with data can be removed
    ASSERT_TRUE(
      ob->add(oid, metastore::object_metadata::ntp_metadata{.tidp = tp})
        .has_value());
    ASSERT_TRUE(ob->remove_pending_object(oid).has_value());
    ASSERT_FALSE(ob->remove_pending_object(oid).has_value());
    ASSERT_FALSE(
      ob->add(oid, metastore::object_metadata::ntp_metadata{.tidp = tp})
        .has_value());

    oid2 = ob->get_or_create_object_for(tp).get().value();
    ASSERT_NE(oid, oid2);
    oid = oid2;

    // finished object cannot be removed
    oid = ob->get_or_create_object_for(tp).get().value();
    ASSERT_TRUE(ob->finish(oid, 0, 0).has_value());
    ASSERT_FALSE(ob->remove_pending_object(oid).has_value());
    ASSERT_FALSE(ob->finish(oid, 0, 0).has_value());
}

// Regression test, where removing an object from the builder left some
// metadata behind, which would result in a failure.
TEST_P(ReplicatedMetastoreTest, TestBuilderRemoveObjectRemovesPartition) {
    auto& app = get_ct_app(model::node_id{0});
    auto& m = app.get_sharded_replicated_metastore()->local();
    auto tp1 = make_tp(0);
    auto tp2 = make_tp(1);
    auto ob = m.object_builder().get().value();

    // Add and remove an object, setting up potential for an old bug where an
    // empty partition is left behind after removal.
    auto oid1 = ob->get_or_create_object_for(tp1).get().value();
    ASSERT_TRUE(ob->remove_pending_object(oid1).has_value());

    // Now add an object as normal.
    auto oid2 = ob->get_or_create_object_for(tp2).get().value();
    auto add_res = ob->add(
      oid2,
      metastore::object_metadata::ntp_metadata{
        .tidp = tp2,
        .base_offset = o{0},
        .last_offset = o{199},
        .max_timestamp = ts{10000},
        .pos = 0,
        .size = 500,
      });
    ASSERT_TRUE(add_res.has_value()) << add_res.error();
    auto fin_res = ob->finish(oid2, 500, 1000);
    metastore::term_offset_map_t terms;
    terms[tp2].emplace_back(
      metastore::term_offset{.term = model::term_id{1}, .first_offset = o{0}});

    // There should be no issues here. Previously Redpanda would complain that
    // it needed term information routed to the domain for tp1, despite the
    // object for tp1 being removed.
    auto commit_res = m.add_objects(*ob, terms).get();
    ASSERT_TRUE(commit_res.has_value());
}

TEST_P(ReplicatedMetastoreTest, TestBasicAdd) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();

    constexpr auto partitions_count = 100;
    ASSERT_NO_FATAL_FAILURE(
      add_initial_objects(meta, partitions_count, 999).get());

    // Check across the partitions of the L1 metastore that we have the right
    // number of partitions.
    size_t state_partitions_count = 0;
    size_t state_extents_count = 0;
    for (int i = 0; i < 3; ++i) {
        auto l1_state = get_stm_state(model::partition_id{i});
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
            auto offset_ts_obj = meta.get_first_ge(tp, o{}, ts{t}).get();
            ASSERT_TRUE(offset_ts_obj.has_value());
        }
        // Going past the end should result in an error.
        auto offset_ts_obj = meta.get_first_ge(tp, o{}, ts{10001}).get();
        ASSERT_FALSE(offset_ts_obj.has_value());
        ASSERT_EQ(offset_ts_obj.error(), metastore::errc::out_of_range);

        // Sanity check the compaction offsets. Since no range is cleaned, the
        // entire log is dirty.
        auto spec = metastore::compaction_info_spec{
          .tidp = tp,
          .tombstone_removal_upper_bound_ts = model::timestamp::now()};
        auto cmp_info = meta.get_compaction_info(spec).get();
        ASSERT_TRUE(cmp_info.has_value());
        EXPECT_FALSE(cmp_info->offsets_response.dirty_ranges.empty());
        EXPECT_THAT(
          cmp_info->offsets_response.dirty_ranges.to_vec(),
          testing::ElementsAre(MatchesRange(o{0}, o{999})));
        EXPECT_FLOAT_EQ(cmp_info->dirty_ratio, 1.0);
        EXPECT_TRUE(cmp_info->earliest_dirty_ts.has_value());
        EXPECT_EQ(cmp_info->compaction_epoch, metastore::compaction_epoch{0});
        EXPECT_EQ(cmp_info->start_offset, o{0});
    }
}

TEST_P(ReplicatedMetastoreTest, TestBasicCompact) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();

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
        update.new_cleaned_ranges.push_back(
          metastore::compaction_update::cleaned_range{
            .base_offset = o{0}, .last_offset = o{999}});
        update.expected_compaction_epoch = metastore::compaction_epoch{0};
        cmap[make_tp(i)] = std::move(update);
    }
    auto cmp_res = meta.compact_objects(*new_objs, cmap).get();
    ASSERT_TRUE(cmp_res.has_value()) << fmt::to_string(cmp_res.error());

    // Check across the partitions of the L1 metastore that we have the right
    // number of partitions.
    size_t state_partitions_count = 0;
    size_t state_extents_count = 0;
    for (int i = 0; i < 3; ++i) {
        auto l1_state = get_stm_state(model::partition_id{i});
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
            auto offset_ts_obj = meta.get_first_ge(tp, o{}, ts{t}).get();
            ASSERT_TRUE(offset_ts_obj.has_value());
        }
        // Going past the end should result in an error.
        auto offset_ts_obj = meta.get_first_ge(tp, o{}, ts{10001}).get();
        ASSERT_FALSE(offset_ts_obj.has_value());
        ASSERT_EQ(offset_ts_obj.error(), metastore::errc::out_of_range);

        // Now check the compacted offsets. Since we've cleaned everything, we
        // should see the dirty ranges be empty.
        auto spec = metastore::compaction_info_spec{
          .tidp = tp,
          .tombstone_removal_upper_bound_ts = model::timestamp::now()};
        auto cmp_info = meta.get_compaction_info(spec).get();
        ASSERT_TRUE(cmp_info.has_value());
        EXPECT_TRUE(cmp_info->offsets_response.dirty_ranges.empty())
          << fmt::format("{} is not cleaned", tp);
        EXPECT_FLOAT_EQ(cmp_info->dirty_ratio, 0.0);
        EXPECT_TRUE(!cmp_info->earliest_dirty_ts.has_value());
        EXPECT_EQ(cmp_info->compaction_epoch, metastore::compaction_epoch{1});
        EXPECT_EQ(cmp_info->start_offset, o{0});
    }
}

TEST_P(ReplicatedMetastoreTest, TestMissingNTP) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();
    auto offsets = meta.get_offsets(make_tp(0)).get();
    ASSERT_FALSE(offsets.has_value());
    ASSERT_EQ(offsets.error(), metastore::errc::missing_ntp);
}

TEST_P(ReplicatedMetastoreTest, TestNotLeader) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();
    auto tp = make_tp(0);

    auto meta_pid
      = app.get_sharded_l1_metastore_router()->local().metastore_partition(tp);
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
        auto obj_builder = meta.object_builder().get().value();
        auto oid = obj_builder->get_or_create_object_for(tp).get().value();
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
        auto fin_res = obj_builder->finish(oid, 500, 1000);
        ASSERT_TRUE(fin_res.has_value()) << fin_res.error();

        metastore::term_offset_map_t terms;
        terms[tp].emplace_back(
          metastore::term_offset{
            .term = model::term_id{0}, .first_offset = next_to_send});
        auto commit_res = meta.add_objects(*obj_builder, terms).get();
        if (!commit_res.has_value()) {
            while (true) {
                if (ss::lowres_clock::now() > deadline) {
                    timed_out = true;
                    break;
                }
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
    auto l1_state = get_stm_state(*meta_pid);
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

TEST_P(ReplicatedMetastoreTest, TestInvalidTermRequest) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();

    std::unique_ptr<metastore::object_metadata_builder> objs;
    ASSERT_NO_FATAL_FAILURE(create_initial_objects(meta, 100, o{99}, &objs));
    metastore::term_offset_map_t terms;
    for (int i = 0; i < 100; ++i) {
        if (i == 0) {
            // Omit the term of one partition.
            continue;
        }
        terms[make_tp(i)].emplace_back(
          metastore::term_offset{
            .term = model::term_id{0}, .first_offset = o{0}});
    }
    // This constitutes an incorrectly formed request, and is not expected
    // ever, hence overall failure.
    auto add_res = meta.add_objects(*objs, terms).get();
    ASSERT_FALSE(add_res.has_value());
    ASSERT_EQ(add_res.error(), metastore::errc::invalid_request);
}

TEST_P(ReplicatedMetastoreTest, TestGetTermForOffset) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();

    auto tp = make_tp(0);

    auto obj_builder = meta.object_builder().get().value();
    auto oid1 = obj_builder->get_or_create_object_for(tp).get().value();
    auto add_res1 = obj_builder->add(
      oid1,
      metastore::object_metadata::ntp_metadata{
        .tidp = tp,
        .base_offset = o{0},
        .last_offset = o{199},
        .max_timestamp = ts{10000},
        .pos = 0,
        .size = 500,
      });
    ASSERT_TRUE(add_res1.has_value()) << add_res1.error();
    auto fin_res1 = obj_builder->finish(oid1, 500, 1000);
    ASSERT_TRUE(fin_res1.has_value()) << fin_res1.error();

    // Add a couple terms.
    metastore::term_offset_map_t terms;
    terms[tp].emplace_back(
      metastore::term_offset{.term = model::term_id{1}, .first_offset = o{0}});
    terms[tp].emplace_back(
      metastore::term_offset{
        .term = model::term_id{2}, .first_offset = o{100}});

    auto add_res = meta.add_objects(*obj_builder, terms).get();
    ASSERT_TRUE(add_res.has_value());

    const auto assert_term_eq = [&](kafka::offset o, model::term_id t) {
        auto term_res = meta.get_term_for_offset(tp, o).get();
        ASSERT_TRUE(term_res.has_value());
        ASSERT_EQ(term_res.value(), t);
    };

    assert_term_eq(o{0}, model::term_id{1});
    assert_term_eq(o{50}, model::term_id{1});
    assert_term_eq(o{100}, model::term_id{2});
    assert_term_eq(o{150}, model::term_id{2});
    assert_term_eq(o{199}, model::term_id{2});
    assert_term_eq(o{200}, model::term_id{2});

    // Test offset out of range
    auto term_res_oor = meta.get_term_for_offset(tp, o{201}).get();
    ASSERT_FALSE(term_res_oor.has_value());
    ASSERT_EQ(term_res_oor.error(), metastore::errc::out_of_range);

    // Test missing ntp
    auto missing_tp = make_tp(999);
    auto term_missing = meta.get_term_for_offset(missing_tp, o{0}).get();
    ASSERT_FALSE(term_missing.has_value());
    ASSERT_EQ(term_missing.error(), metastore::errc::missing_ntp);
}

TEST_P(ReplicatedMetastoreTest, TestGetEndOffsetForTerm) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();

    auto tp = make_tp(0);

    // Set up initial objects with multiple terms
    auto obj_builder = meta.object_builder().get().value();
    auto oid1 = obj_builder->get_or_create_object_for(tp).get().value();
    auto add_res1 = obj_builder->add(
      oid1,
      metastore::object_metadata::ntp_metadata{
        .tidp = tp,
        .base_offset = o{0},
        .last_offset = o{199},
        .max_timestamp = ts{10000},
        .pos = 0,
        .size = 500,
      });
    ASSERT_TRUE(add_res1.has_value()) << add_res1.error();
    auto fin_res1 = obj_builder->finish(oid1, 500, 1000);
    ASSERT_TRUE(fin_res1.has_value()) << fin_res1.error();

    // Add a couple terms.
    metastore::term_offset_map_t terms;
    terms[tp].emplace_back(
      metastore::term_offset{.term = model::term_id{1}, .first_offset = o{0}});
    terms[tp].emplace_back(
      metastore::term_offset{
        .term = model::term_id{2}, .first_offset = o{100}});

    auto add_res = meta.add_objects(*obj_builder, terms).get();
    ASSERT_TRUE(add_res.has_value());

    auto assert_end_offset_eq = [&](
                                  model::term_id t, kafka::offset expected_o) {
        auto end_res = meta.get_end_offset_for_term(tp, t).get();
        ASSERT_TRUE(end_res.has_value());
        ASSERT_EQ(expected_o, end_res.value());
    };
    assert_end_offset_eq(model::term_id{0}, o{0});
    assert_end_offset_eq(model::term_id{1}, o{100});
    assert_end_offset_eq(model::term_id{2}, o{200});

    // Test non-existent term
    auto end_res_fail
      = meta.get_end_offset_for_term(tp, model::term_id{3}).get();
    ASSERT_FALSE(end_res_fail.has_value());
    ASSERT_EQ(end_res_fail.error(), metastore::errc::out_of_range);

    // Test missing ntp
    auto missing_tp = make_tp(999);
    auto end_missing
      = meta.get_end_offset_for_term(missing_tp, model::term_id{1}).get();
    ASSERT_FALSE(end_missing.has_value());
    ASSERT_EQ(end_missing.error(), metastore::errc::missing_ntp);
}

TEST_P(ReplicatedMetastoreTest, TestSetStartOffset) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();

    // Create initial objects for testing
    ASSERT_NO_FATAL_FAILURE(add_initial_objects(meta, 3, 99).get());

    auto tp = make_tp(0);
    auto assert_get_offsets =
      [&](kafka::offset expected_start, kafka::offset expected_next) {
          auto offsets = meta.get_offsets(tp).get();
          ASSERT_TRUE(offsets.has_value());
          ASSERT_EQ(expected_start, offsets->start_offset);
          ASSERT_EQ(expected_next, offsets->next_offset);

          // Sanity check the start offset returned by get_compaction_info() as
          // well.
          auto spec = metastore::compaction_info_spec{
            .tidp = tp,
            .tombstone_removal_upper_bound_ts = model::timestamp::now()};
          auto cmp_info = meta.get_compaction_info(spec).get();
          ASSERT_TRUE(cmp_info.has_value());
          ASSERT_EQ(cmp_info->start_offset, expected_start);
      };

    // Get initial offsets
    ASSERT_NO_FATAL_FAILURE(assert_get_offsets(o{0}, o{100}));

    // Set start offset to a value within the extent
    auto set_start_res = meta.set_start_offset(tp, o{50}).get();
    ASSERT_TRUE(set_start_res.has_value());
    ASSERT_NO_FATAL_FAILURE(assert_get_offsets(o{50}, o{100}));

    // Test setting below the start.
    auto set_start_invalid = meta.set_start_offset(tp, o{0}).get();
    ASSERT_TRUE(set_start_invalid.has_value());
    ASSERT_NO_FATAL_FAILURE(assert_get_offsets(o{50}, o{100}));

    // Test setting start offset for missing ntp
    auto missing_tp = make_tp(999);
    auto set_start_missing = meta.set_start_offset(missing_tp, o{10}).get();
    ASSERT_FALSE(set_start_missing.has_value());
    ASSERT_EQ(set_start_missing.error(), metastore::errc::invalid_request);
    ASSERT_NO_FATAL_FAILURE(assert_get_offsets(o{50}, o{100}));

    // Set start so it's totally empty
    set_start_res = meta.set_start_offset(tp, o{100}).get();
    ASSERT_TRUE(set_start_res.has_value());
    ASSERT_NO_FATAL_FAILURE(assert_get_offsets(o{100}, o{100}));

    // Sanity check that getting the term for the next offset is still valid.
    auto term_res = meta.get_term_for_offset(tp, o{100}).get();
    ASSERT_TRUE(term_res.has_value());
    ASSERT_EQ(term_res.value(), model::term_id{0});
}

TEST_P(ReplicatedMetastoreTest, TestBasicRemoveTopics) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();

    static constexpr auto topics_count = 3000;
    add_objects_for_topics(meta, topics_count, 99).get();

    // Sanity check that all topics exist.
    for (int i = 0; i < topics_count; ++i) {
        auto tp = make_topic_p0(i);
        std::expected<metastore::offsets_response, metastore::errc> offsets;
        RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
            return meta.get_offsets(tp).then([&](auto offsets_res) {
                offsets = std::move(offsets_res);
                return offsets.has_value();
            });
        });
        ASSERT_EQ(offsets->next_offset, o{100});
    }

    // Collect all topic IDs to remove.
    chunked_vector<model::topic_id> topic_ids_to_remove;
    for (int i = 0; i < topics_count; ++i) {
        auto tp = make_topic_p0(i);
        topic_ids_to_remove.push_back(tp.topic_id);
    }

    // Remove all topics, retrying as needed since batching may return
    // not_removed topics when extent counts exceed the batch limit.
    while (!topic_ids_to_remove.empty()) {
        auto remove_res = meta.remove_topics(topic_ids_to_remove).get();
        ASSERT_TRUE(remove_res.has_value());
        chunked_vector<model::topic_id> remaining;
        remaining.reserve(remove_res->not_removed.size());
        for (auto& tid : remove_res->not_removed) {
            remaining.push_back(tid);
        }
        ASSERT_LT(remaining.size(), topic_ids_to_remove.size())
          << "no progress removing topics";
        topic_ids_to_remove = std::move(remaining);
    }

    // Verify all topics are gone.
    for (int i = 0; i < topics_count; ++i) {
        auto tp = make_topic_p0(i);
        auto offsets = meta.get_offsets(tp).get();
        ASSERT_FALSE(offsets.has_value());
        ASSERT_EQ(offsets.error(), metastore::errc::missing_ntp);
    }
    for (int i = 0; i < 3; ++i) {
        auto l1_state = get_stm_state(model::partition_id{i});
        EXPECT_TRUE(l1_state.topic_to_state.empty());
    }
}

TEST_P(ReplicatedMetastoreTest, TestRemoveTopicsWithShuffleLoop) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();

    static constexpr auto topics_count = 1000;
    add_objects_for_topics(meta, topics_count, 99).get();

    chunked_vector<model::topic_id> topics_to_remove;
    for (int i = 0; i < topics_count; ++i) {
        auto tp = make_topic_p0(i);
        topics_to_remove.push_back(tp.topic_id);
    }
    // Pick a metastore partition to shuffle leadership on.
    auto meta_ntp = model::ntp{
      model::kafka_internal_namespace,
      model::l1_metastore_topic,
      model::partition_id{0},
    };
    // Start shuffle loop in the background.
    ss::gate gate;
    auto shuffle_loop = ssx::spawn_with_gate_then(gate, [this, meta_ntp] {
        return shuffle_leadership(meta_ntp).then(
          [] { return ss::sleep(300ms); });
    });

    // Retry until removal succeeds.
    auto deadline = ss::lowres_clock::now() + 30s;
    bool timed_out = false;
    bool succeeded = false;
    while (!succeeded) {
        if (ss::lowres_clock::now() > deadline) {
            timed_out = true;
            break;
        }
        auto remove_res = meta.remove_topics(topics_to_remove).get();
        if (!remove_res.has_value()) {
            ss::sleep(100ms).get();
            continue;
        }
        if (!remove_res.value().not_removed.empty()) {
            topics_to_remove.clear();
            std::copy(
              remove_res->not_removed.begin(),
              remove_res->not_removed.end(),
              std::back_inserter(topics_to_remove));
            ss::sleep(100ms).get();
            continue;
        }
        succeeded = true;
    }

    gate.close().get();
    shuffle_loop.get();

    ASSERT_FALSE(timed_out) << "Timed out waiting for remove_topics to succeed";
    ASSERT_TRUE(succeeded);

    // Verify all topics are gone.
    for (int i = 0; i < topics_count; ++i) {
        auto tp = make_topic_p0(i);
        auto offsets = meta.get_offsets(tp).get();
        ASSERT_FALSE(offsets.has_value());
        ASSERT_EQ(offsets.error(), metastore::errc::missing_ntp);
    }

    // Verify metastore state is clean across all partitions.
    for (int i = 0; i < 3; ++i) {
        auto l1_state = get_stm_state(model::partition_id{i});
        EXPECT_TRUE(l1_state.topic_to_state.empty())
          << "Partition " << i << " still has topics";
    }
}

TEST_P(ReplicatedMetastoreTest, TestGetCompactionInfos) {
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();

    constexpr auto partitions_count = 100;
    ASSERT_NO_FATAL_FAILURE(
      add_initial_objects(meta, partitions_count, 999).get());
    chunked_vector<metastore::compaction_info_spec> info_specs;
    info_specs.reserve(partitions_count);
    for (int i = 0; i < partitions_count; ++i) {
        auto tp = make_tp(i);
        info_specs.emplace_back(tp, model::timestamp::max());
    }

    {
        auto compaction_infos_res = meta.get_compaction_infos(info_specs).get();
        ASSERT_TRUE(compaction_infos_res.has_value());
        ASSERT_EQ(compaction_infos_res->size(), partitions_count);
        for (const auto& [log, info] : compaction_infos_res.value()) {
            ASSERT_TRUE(info.has_value());
            ASSERT_DOUBLE_EQ(info->dirty_ratio, 1.0);
            ASSERT_EQ(info->compaction_epoch, metastore::compaction_epoch{0});
            ASSERT_EQ(info->start_offset, o{0});
        }
    }

    // Compact every partition.
    std::unique_ptr<metastore::object_metadata_builder> new_objs;
    ASSERT_NO_FATAL_FAILURE(
      create_initial_objects(meta, partitions_count, 999, &new_objs));
    metastore::compaction_map_t cmap;
    for (int i = 0; i < partitions_count; ++i) {
        metastore::compaction_update update;
        update.cleaned_at = model::timestamp::now();
        update.new_cleaned_ranges.push_back(
          metastore::compaction_update::cleaned_range{
            .base_offset = o{0}, .last_offset = o{999}});
        update.expected_compaction_epoch = metastore::compaction_epoch{0};
        cmap[make_tp(i)] = std::move(update);
    }
    auto cmp_res = meta.compact_objects(*new_objs, cmap).get();
    ASSERT_TRUE(cmp_res.has_value()) << fmt::to_string(cmp_res.error());

    {
        auto compaction_infos_res = meta.get_compaction_infos(info_specs).get();
        ASSERT_TRUE(compaction_infos_res.has_value());
        ASSERT_EQ(compaction_infos_res->size(), partitions_count);
        for (const auto& [log, info] : compaction_infos_res.value()) {
            ASSERT_TRUE(info.has_value());
            ASSERT_DOUBLE_EQ(info->dirty_ratio, 0.0);
            ASSERT_EQ(info->compaction_epoch, metastore::compaction_epoch{1});
            ASSERT_EQ(info->start_offset, o{0});
        }
    }
}

TEST_P(ReplicatedMetastoreTest, TestBasicFlushAndRestore) {
    if (GetParam() == metastore_backend::simple) {
        GTEST_SKIP() << "Flush/restore not supported with simple backend";
    }
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();

    // Add some objects to the metastore.
    constexpr auto partitions_count = 100;
    ASSERT_NO_FATAL_FAILURE(
      add_initial_objects(meta, partitions_count, 99).get());

    // Verify the objects were added.
    for (int i = 0; i < partitions_count; ++i) {
        auto tp = make_tp(i);
        auto offsets = meta.get_offsets(tp).get();
        ASSERT_TRUE(offsets.has_value()) << "Partition " << i << " missing";
        ASSERT_EQ(offsets->next_offset, o{100});
    }

    // Flush the metastore to cloud storage.
    auto flush_res = meta.flush().get();
    ASSERT_TRUE(flush_res.has_value()) << fmt::to_string(flush_res.error());

    // Store the cluster UUID before shutting down.
    auto cluster_uuid = get_node_application(model::node_id{0})
                          ->storage.local()
                          .get_cluster_uuid();
    ASSERT_TRUE(cluster_uuid.has_value());

    // Shut down and wipe all nodes.
    for (size_t i = 0; i < num_brokers; i++) {
        auto n = model::node_id(i);
        instance(n)->shutdown();
        std::filesystem::remove_all(instance(n)->data_dir);
        remove_node_application(model::node_id(i));
    }

    // Restart all nodes.
    // NOTE: the added nodes get the same node IDs 0, 1, 2.
    for (size_t i = 0; i < num_brokers; i++) {
        add_node(fixture_cfg());
    }
    wait_for_all_members(5s).get();

    // Restore from the saved cluster UUID.
    auto& new_app = get_ct_app(model::node_id{0});
    auto& new_meta = new_app.get_sharded_replicated_metastore()->local();
    cloud_storage::remote_label rl(*cluster_uuid);
    auto restore_res = new_meta.restore(rl).get();
    ASSERT_TRUE(restore_res.has_value()) << fmt::to_string(restore_res.error());

    // Verify the objects are accessible after restore.
    for (int i = 0; i < partitions_count; ++i) {
        auto tp = make_tp(i);
        auto offsets = new_meta.get_offsets(tp).get();
        ASSERT_TRUE(offsets.has_value()) << "Partition " << i << " missing";
        ASSERT_EQ(offsets->next_offset, o{100});
    }
}

// Test that restore returns an error when the manifest doesn't exist.
TEST_P(ReplicatedMetastoreTest, TestRestoreManifestNotFound) {
    if (GetParam() == metastore_backend::simple) {
        GTEST_SKIP() << "Restore not supported with simple backend";
    }
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();

    // Attempt to restore from a non-existent cluster UUID. The manifest should
    // be not found and we should create a new topic.
    auto fake_cluster_uuid = model::cluster_uuid{uuid_t::create()};
    cloud_storage::remote_label rl(fake_cluster_uuid);
    auto restore_res = meta.restore(rl).get();
    ASSERT_TRUE(restore_res.has_value());
}

// Test that restore creates the metastore topic with the correct number of
// partitions matching the number of domains in the manifest.
TEST_P(ReplicatedMetastoreTest, TestRestoreCreatesCorrectPartitionCount) {
    if (GetParam() == metastore_backend::simple) {
        GTEST_SKIP() << "Restore not supported with simple backend";
    }
    auto& app = get_ct_app(model::node_id{0});
    auto& meta = app.get_sharded_replicated_metastore()->local();

    // Wait for the metastore topic to be created by background initialization.
    // This helps ensure that there won't be a background recreation of the
    // metastore topic after we delete it below.
    for (const auto& node_id : instance_ids()) {
        auto& tp_state = get_node_application(node_id)
                           ->controller->get_topics_state()
                           .local();
        RPTEST_REQUIRE_EVENTUALLY(10s, [&tp_state] {
            return tp_state.contains(model::l1_metastore_nt);
        });
    }

    // Delete the existing metastore topic.
    auto& tp_fe = get_node_application(model::node_id{0})
                    ->controller->get_topics_frontend()
                    .local();
    tp_fe.dispatch_delete_topics({model::l1_metastore_nt}, 10s).get();
    for (const auto& node_id : instance_ids()) {
        auto& tp_state = get_node_application(node_id)
                           ->controller->get_topics_state()
                           .local();
        RPTEST_REQUIRE_EVENTUALLY(10s, [&tp_state] {
            return !tp_state.contains(model::l1_metastore_nt);
        });
    }

    // Create a manifest with 10 domains and uplaod it.
    constexpr int expected_partitions = 10;
    metastore_manifest manifest;
    manifest.partitioning_strategy = "murmur";
    for (int i = 0; i < expected_partitions; ++i) {
        manifest.domains.push_back(domain_uuid{uuid_t::create()});
    }

    auto* remote = &get_node_application(model::node_id{0})->cloud_io.local();
    manifest_io mio(*remote, bucket_name);
    auto fake_cluster_uuid = model::cluster_uuid{uuid_t::create()};
    cloud_storage::remote_label rl(fake_cluster_uuid);
    auto upload_res
      = mio.upload_metastore_manifest(rl, std::move(manifest)).get();
    ASSERT_TRUE(upload_res.has_value());

    // Restore from the uploaded manifest. Retry until leaders are elected.
    auto deadline = ss::lowres_clock::now() + 30s;
    while (ss::lowres_clock::now() < deadline) {
        auto restore_res = meta.restore(rl).get();
        if (restore_res.has_value()) {
            break;
        }
        ss::sleep(100ms).get();
    }

    // Verify the metastore topic was created with the correct partition count.
    auto& tp_state = get_node_application(model::node_id{0})
                       ->controller->get_topics_state()
                       .local();
    auto cfg = tp_state.get_topic_cfg(model::l1_metastore_nt);
    ASSERT_TRUE(cfg.has_value());
    ASSERT_EQ(cfg->partition_count, expected_partitions);
}

INSTANTIATE_TEST_SUITE_P(
  MetastoreBackends,
  ReplicatedMetastoreTest,
  testing::Values(metastore_backend::simple, metastore_backend::lsm),
  [](const testing::TestParamInfo<metastore_backend>& info) {
      return info.param == metastore_backend::simple ? "Simple" : "LSM";
  });
