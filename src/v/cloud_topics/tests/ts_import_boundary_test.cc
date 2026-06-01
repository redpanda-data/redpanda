/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/tests/cluster_fixture.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/partition.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <gtest/gtest.h>

using namespace std::chrono_literals;
using tests::kv_t;

namespace {

const model::topic test_topic{"tiered-topic"};
const model::ntp test_ntp{
  model::kafka_namespace, test_topic, model::partition_id{0}};
const model::topic_namespace test_tp_ns{model::kafka_namespace, test_topic};

} // namespace

// The TS->CT migration boundary now lives on the archival_metadata_stm (the
// "seal"), exposed via partition::ts_migration_boundary(). These tests verify
// that promoting a tiered partition records the seal, that it is stable across
// repeated promotions, and that no seal is recorded when there is no uploaded
// TS data to serve via the passthrough path.
class TsImportBoundaryTest
  : public cloud_topics::cluster_fixture
  , public ::testing::Test {
public:
    void SetUp() override {
        cfg.get("enable_leader_balancer").set_value(false);
        add_node();
        wait_for_all_members(5s).get();
    }

    ss::future<> create_tiered_topic() {
        cluster::topic_properties props;
        props.storage_mode = model::redpanda_storage_mode::tiered;
        props.shadow_indexing = model::shadow_indexing_mode::full;
        co_await create_topic(
          model::topic_namespace_view{model::kafka_namespace, test_topic},
          /*partitions=*/1,
          /*replication_factor=*/1,
          props);
    }

    // Produce records, explicitly seal the segment via force_roll(), then wait
    // until the archiver has uploaded it and recorded a non-null
    // last_kafka_offset in the archival_metadata_stm manifest. seal_ts_migration
    // requires a non-empty manifest to record the boundary, so this must happen
    // before set_storage_mode.
    ss::future<> produce_and_wait_for_ts_upload(
      ss::lw_shared_ptr<cluster::partition>& leader_p) {
        auto* producer = co_await make_producer(model::node_id{0});
        co_await producer->produce_to_partition(
          test_topic, model::partition_id{0}, kv_t::sequence(0, 1));
        co_await leader_p->log()->force_roll();

        RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [&] {
            auto stm = leader_p->archival_meta_stm();
            return stm && stm->manifest().get_last_kafka_offset().has_value();
        });
    }

    ss::future<> set_storage_mode(model::redpanda_storage_mode mode) {
        cluster::incremental_topic_updates updates;
        updates.storage_mode.op = cluster::incremental_update_operation::set;
        updates.storage_mode.value = mode;

        auto& topics_frontend
          = instance(model::node_id{0})
              ->app.controller->get_topics_frontend()
              .local();
        cluster::topic_properties_update update(test_tp_ns);
        update.properties = updates;
        cluster::topic_properties_update_vector updates_vec;
        updates_vec.push_back(std::move(update));
        auto results = co_await topics_frontend.update_topic_properties(
          std::move(updates_vec), model::no_timeout);
        RPTEST_REQUIRE_EQ_CORO(results.size(), 1);
        RPTEST_REQUIRE_EQ_CORO(results[0].ec, cluster::errc::success);
    }

    ss::future<> wait_for_leader(ss::lw_shared_ptr<cluster::partition>& out) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [&] {
            auto [leader_fx, leader_p] = get_leader(test_ntp);
            if (!leader_fx) {
                return false;
            }
            out = leader_p;
            return true;
        });
    }

    ss::future<>
    wait_for_migration_boundary(ss::lw_shared_ptr<cluster::partition>& leader_p) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(
          10s, [&] { return leader_p->ts_migration_boundary().has_value(); });
    }

    scoped_config cfg;
};

// Promoting a tiered partition to tiered_cloud records a migration seal.
TEST_F(TsImportBoundaryTest, BoundarySetOnStorageModePromotion) {
    create_tiered_topic().get();

    ss::lw_shared_ptr<cluster::partition> leader_p;
    wait_for_leader(leader_p).get();
    produce_and_wait_for_ts_upload(leader_p).get();

    set_storage_mode(model::redpanda_storage_mode::tiered_cloud).get();
    wait_for_migration_boundary(leader_p).get();

    ASSERT_TRUE(leader_p->ts_migration_boundary().has_value());
    // The boundary must match the manifest's last uploaded kafka offset.
    ASSERT_EQ(
      leader_p->ts_migration_boundary(),
      leader_p->archival_meta_stm()->manifest().get_last_kafka_offset());
}

// Promoting a tiered partition to cloud also records a migration seal.
TEST_F(TsImportBoundaryTest, BoundarySetOnStorageModePromotionToCloud) {
    create_tiered_topic().get();

    ss::lw_shared_ptr<cluster::partition> leader_p;
    wait_for_leader(leader_p).get();
    produce_and_wait_for_ts_upload(leader_p).get();

    set_storage_mode(model::redpanda_storage_mode::cloud).get();
    wait_for_migration_boundary(leader_p).get();

    ASSERT_TRUE(leader_p->ts_migration_boundary().has_value());
}

// Setting tiered_cloud a second time does not alter the boundary recorded by
// the first transition. The has_value() guard in seal_ts_migration() and the
// !cloud_topic_enabled() transition check in update_configuration both prevent
// a second seal command from being replicated.
TEST_F(TsImportBoundaryTest, BoundaryStableAfterDoublePromotion) {
    create_tiered_topic().get();

    ss::lw_shared_ptr<cluster::partition> leader_p;
    wait_for_leader(leader_p).get();
    produce_and_wait_for_ts_upload(leader_p).get();

    set_storage_mode(model::redpanda_storage_mode::tiered_cloud).get();
    wait_for_migration_boundary(leader_p).get();

    const auto first_boundary = leader_p->ts_migration_boundary();
    ASSERT_TRUE(first_boundary.has_value());

    set_storage_mode(model::redpanda_storage_mode::tiered_cloud).get();
    ss::sleep(200ms).get();

    EXPECT_EQ(leader_p->ts_migration_boundary(), first_boundary);
}

// A tiered partition with no uploaded TS data records no seal on promotion:
// there is nothing to serve via the passthrough path, so the partition is a
// native cloud topic. This exercises the skip-on-empty-manifest path in
// seal_ts_migration() (and avoids sealing on a sentinel offset).
TEST_F(TsImportBoundaryTest, NoBoundaryWithoutUploadedTsData) {
    create_tiered_topic().get();

    ss::lw_shared_ptr<cluster::partition> leader_p;
    wait_for_leader(leader_p).get();

    set_storage_mode(model::redpanda_storage_mode::tiered_cloud).get();

    // Give update_configuration and the leadership callback time to run
    // seal_ts_migration; the boundary must remain unset.
    ss::sleep(500ms).get();
    EXPECT_FALSE(leader_p->ts_migration_boundary().has_value());
}
