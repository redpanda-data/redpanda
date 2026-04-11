/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "cloud_topics/level_one/metastore/topic_purger.h"
#include "cluster/data_migrated_resources.h"
#include "cluster/nt_revision.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/node_config.h"
#include "gtest/gtest.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

using namespace cloud_topics::l1;

namespace {
using o = kafka::offset;
using ts = model::timestamp;
ss::abort_source never_abort;
model::topic_id make_tid(int i) {
    return model::topic_id(
      uuid_t::from_string(fmt::format("deadbeef-aaaa-0000-0000-{:012d}", i)));
}
model::topic make_topic(int i) {
    return model::topic{fmt::format("test_topic_{}", i)};
}

class unreliable_metastore : public simple_metastore {
public:
    ss::future<std::expected<topic_removal_response, metastore::errc>>
    remove_topics(const chunked_vector<model::topic_id>& topic_ids) override {
        if (fail_topics_removal_) {
            co_return std::unexpected(metastore::errc::invalid_request);
        }
        co_return co_await simple_metastore::remove_topics(topic_ids);
    }

    void fail_remove_topics(bool fail) { fail_topics_removal_ = fail; }

private:
    bool fail_topics_removal_{false};
};

} // namespace

class TopicPurgerTest : public testing::Test {
public:
    void SetUp() override {
        topic_table = std::make_unique<cluster::topic_table>(
          mr, model::node_id{1});
    }
    model::offset next_topic_table_offset() const {
        return model::offset(topic_table->last_applied_revision()() + 1);
    }
    ss::future<std::error_code> register_topic(
      const model::topic& topic,
      model::revision_id rev,
      model::topic_id tp_id) {
        auto topic_cfg = cluster::topic_configuration(
          model::kafka_namespace, topic, 1, 1, tp_id);
        topic_cfg.properties.storage_mode = model::redpanda_storage_mode::cloud;
        topic_cfg.properties.remote_delete = true;
        co_return co_await topic_table->apply(
          cluster::create_topic_cmd{
            model::topic_namespace{model::kafka_namespace, topic},
            {topic_cfg, {}}},
          model::offset{rev()});
    }
    ss::future<std::error_code>
    register_tombstone(const model::topic& topic, model::revision_id rev) {
        co_return co_await topic_table->apply(
          cluster::topic_lifecycle_transition{
            .topic = cluster::
              nt_revision{.nt = model::topic_namespace(model::kafka_namespace, topic), .initial_revision_id = model::initial_revision_id{rev()}},
            .mode = cluster::topic_lifecycle_transition_mode::pending_gc,
            .domain = cluster::topic_purge_domain::cloud_topic,
          },
          next_topic_table_offset());
    }
    ss::future<std::error_code>
    remove_tombstone(const model::topic& topic, model::revision_id rev) {
        co_return co_await topic_table->apply(
          cluster::topic_lifecycle_transition{
            .topic = cluster::
              nt_revision{.nt = model::topic_namespace(model::kafka_namespace, topic), .initial_revision_id = model::initial_revision_id{rev()}},
            .mode = cluster::topic_lifecycle_transition_mode::purged,
            .domain = cluster::topic_purge_domain::cloud_topic,
          },
          next_topic_table_offset());
    }
    ss::future<topic_purger::remove_tombstone_ret_t>
    remove_tombstone_fn(const cluster::nt_revision& ntr) {
        co_await remove_tombstone(
          ntr.nt.tp, model::revision_id{ntr.initial_revision_id()});
        co_return std::nullopt;
    }
    ss::future<> register_topics(int topics_count) {
        for (int i = 0; i < topics_count; ++i) {
            auto e = co_await register_topic(
              make_topic(i), model::revision_id{i}, make_tid(i));
            EXPECT_EQ(e, cluster::errc::success);
        }
    }
    ss::future<> register_tombstones(int tombstones_count) {
        for (int i = 0; i < tombstones_count; ++i) {
            auto e = co_await register_tombstone(
              make_topic(i), model::revision_id{i});
            EXPECT_EQ(e, cluster::errc::success);
        }
    }

    ss::future<> add_objects(int topics_count, int last_offset) {
        auto builder_res = co_await metastore.object_builder();
        ASSERT_TRUE_CORO(builder_res.has_value());
        auto& builder = builder_res.value();

        metastore::term_offset_map_t term_offsets;
        for (int i = 0; i < topics_count; ++i) {
            auto tp = model::topic_id_partition(
              make_tid(i), model::partition_id{0});

            auto oid_res = co_await builder->get_or_create_object_for(tp);
            ASSERT_TRUE_CORO(oid_res.has_value());

            auto add_res = builder->add(
              oid_res.value(),
              metastore::object_metadata::ntp_metadata{
                .tidp = tp,
                .base_offset = o{0},
                .last_offset = o{last_offset},
                .max_timestamp = ts{10000},
                .pos = 0,
                .size = 500,
              });
            ASSERT_TRUE_CORO(add_res.has_value());

            auto finish_res = builder->finish(oid_res.value(), 1000, 1500);
            ASSERT_TRUE_CORO(finish_res.has_value());

            term_offsets[tp].push_back(
              metastore::term_offset{
                .term = model::term_id{0}, .first_offset = o{0}});
        }

        auto add_res = co_await metastore.add_objects(*builder, term_offsets);
        ASSERT_TRUE_CORO(add_res.has_value());
    }

protected:
    unreliable_metastore metastore;
    cluster::data_migrations::migrated_resources mr;
    std::unique_ptr<cluster::topic_table> topic_table;
};

TEST_F(TopicPurgerTest, TestPurgeRemovesTombstones) {
    register_topics(3).get();
    ASSERT_EQ(3, topic_table->all_topics().size());
    add_objects(3, 100).get();

    // Verify the metastore has offsets for all topics.
    auto tp0 = model::topic_id_partition(make_tid(0), model::partition_id{0});
    auto tp1 = model::topic_id_partition(make_tid(1), model::partition_id{0});
    auto tp2 = model::topic_id_partition(make_tid(2), model::partition_id{0});

    auto offsets0 = metastore.get_offsets(tp0).get();
    auto offsets1 = metastore.get_offsets(tp1).get();
    auto offsets2 = metastore.get_offsets(tp2).get();
    ASSERT_TRUE(offsets0.has_value());
    ASSERT_TRUE(offsets1.has_value());
    ASSERT_TRUE(offsets2.has_value());

    // Tombstone some of the cloud topics in the topic table.
    register_tombstones(2).get();
    ASSERT_EQ(2, topic_table->get_cloud_topic_tombstones().size());
    ASSERT_EQ(1, topic_table->all_topics().size());

    auto remove_tombstones = topic_purger::remove_tombstone_fn_t{
      [this](const cluster::nt_revision& ntr) {
          return remove_tombstone_fn(ntr);
      }};

    // Reconcile the tombstones by removing state from the metastore.
    topic_purger purger(
      &metastore, topic_table.get(), std::move(remove_tombstones));
    auto result = purger.purge_tombstoned_topics(&never_abort).get();
    ASSERT_TRUE(result.has_value());

    // The tombstones should be removed, as should the state in the metastore.
    ASSERT_EQ(0, topic_table->get_cloud_topic_tombstones().size());

    // Verify the metastore no longer has offsets for the tombstoned topics.
    offsets0 = metastore.get_offsets(tp0).get();
    offsets1 = metastore.get_offsets(tp1).get();
    offsets2 = metastore.get_offsets(tp2).get();
    ASSERT_FALSE(offsets0.has_value());
    ASSERT_FALSE(offsets1.has_value());
    ASSERT_TRUE(offsets2.has_value());

    ASSERT_EQ(1, topic_table->all_topics().size());
}

TEST_F(TopicPurgerTest, TestPurgeManyTopics) {
    static constexpr auto many_topics_count = 10000;
    register_topics(many_topics_count).get();
    ASSERT_EQ(many_topics_count, topic_table->all_topics().size());
    add_objects(many_topics_count, 100).get();

    register_tombstones(many_topics_count).get();
    ASSERT_EQ(
      many_topics_count, topic_table->get_cloud_topic_tombstones().size());
    ASSERT_EQ(0, topic_table->all_topics().size());

    auto remove_tombstones = topic_purger::remove_tombstone_fn_t{
      [this](const cluster::nt_revision& ntr) {
          return remove_tombstone_fn(ntr);
      }};

    // Reconcile the tombstones by removing state from the metastore.
    topic_purger purger(
      &metastore, topic_table.get(), std::move(remove_tombstones));
    auto result = purger.purge_tombstoned_topics(&never_abort).get();
    ASSERT_TRUE(result.has_value());

    for (int i = 0; i < many_topics_count; ++i) {
        auto tp = model::topic_id_partition(
          make_tid(i), model::partition_id{0});
        auto offsets = metastore.get_offsets(tp).get();
        ASSERT_FALSE(offsets.has_value());
    }
    ASSERT_EQ(0, topic_table->get_cloud_topic_tombstones().size());
    ASSERT_EQ(0, topic_table->all_topics().size());
}

TEST_F(TopicPurgerTest, TestPurgeWithUnreliableMetastore) {
    register_topics(3).get();
    ASSERT_EQ(3, topic_table->all_topics().size());
    add_objects(3, 100).get();

    register_tombstones(3).get();
    ASSERT_EQ(3, topic_table->get_cloud_topic_tombstones().size());
    ASSERT_EQ(0, topic_table->all_topics().size());

    metastore.fail_remove_topics(true);

    auto remove_tombstones = topic_purger::remove_tombstone_fn_t{
      [this](const cluster::nt_revision& ntr) {
          return remove_tombstone_fn(ntr);
      }};
    topic_purger purger(
      &metastore, topic_table.get(), std::move(remove_tombstones));

    // Purge should fail due to metastore failure.
    auto result = purger.purge_tombstoned_topics(&never_abort).get();
    ASSERT_FALSE(result.has_value());

    // Tombstones should still be present since we failed to update the
    // metastore.
    ASSERT_EQ(3, topic_table->get_cloud_topic_tombstones().size());

    // Fix the metastore and retry purge.
    metastore.fail_remove_topics(false);

    result = purger.purge_tombstoned_topics(&never_abort).get();
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(0, topic_table->get_cloud_topic_tombstones().size());
}

TEST_F(TopicPurgerTest, TestPurgeWithUnreliableController) {
    register_topics(3).get();
    ASSERT_EQ(3, topic_table->all_topics().size());
    add_objects(3, 100).get();

    register_tombstones(3).get();
    ASSERT_EQ(3, topic_table->get_cloud_topic_tombstones().size());
    ASSERT_EQ(0, topic_table->all_topics().size());

    auto bad_remove_tombstones = topic_purger::remove_tombstone_fn_t{
      [](const cluster::nt_revision&) {
          throw std::runtime_error("Oh no!");
          return ss::make_ready_future<topic_purger::remove_tombstone_ret_t>(
            std::nullopt);
      }};
    topic_purger purger(
      &metastore, topic_table.get(), std::move(bad_remove_tombstones));

    // Purge should fail due to tombstone removal failure.
    auto result = purger.purge_tombstoned_topics(&never_abort).get();
    ASSERT_FALSE(result.has_value());
    ASSERT_EQ(3, topic_table->get_cloud_topic_tombstones().size());
    ASSERT_EQ(0, topic_table->all_topics().size());

    // The metastore removal should have succeeded though.
    for (int i = 0; i < 3; ++i) {
        auto tp = model::topic_id_partition(
          make_tid(i), model::partition_id{0});
        auto offsets = metastore.get_offsets(tp).get();
        ASSERT_FALSE(offsets.has_value());
    }

    // Now try again with a healthy removal function.
    auto remove_tombstones = topic_purger::remove_tombstone_fn_t{
      [this](const cluster::nt_revision& ntr) {
          return remove_tombstone_fn(ntr);
      }};
    topic_purger purger_retry(
      &metastore, topic_table.get(), std::move(remove_tombstones));
    result = purger_retry.purge_tombstoned_topics(&never_abort).get();
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(0, topic_table->get_cloud_topic_tombstones().size());
    ASSERT_EQ(0, topic_table->all_topics().size());
}
