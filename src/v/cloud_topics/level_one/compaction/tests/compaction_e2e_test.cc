/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/async.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace cloud_topics;
using namespace std::chrono_literals;

static const cluster::topic_properties compact_topic_props = [] {
    cluster::topic_properties props;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    return props;
}();

class CompactionFixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public ::testing::Test {
public:
    CompactionFixture()
      : redpanda_thread_fixture(init_cloud_topics_tag{}, httpd_port_number()) {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
    }

    ss::future<>
    create_cloud_topic(model::ntp ntp, cluster::topic_properties props) {
        props.cloud_topic_enabled = true;
        props.shadow_indexing = model::shadow_indexing_mode::disabled;

        co_await add_topic(model::topic_namespace_view{ntp}, 1, props);
        co_await wait_for_leader(ntp);
    }
};

TEST_F(CompactionFixture, ManageAndDeleteCompactedTopic) {
    // Creating a `compact`-enabled cloud topic should make it appear as managed
    // in the scheduler.
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    create_cloud_topic(ntp, compact_topic_props).get();

    auto* ct_app = app.cloud_topics_app.get();
    auto* compaction_scheduler = ct_app->get_compaction_scheduler();
    RPTEST_REQUIRE_EVENTUALLY(
      10s, [&] { return compaction_scheduler->is_managed(ntp); });

    app.controller->get_topics_frontend()
      .local()
      .delete_topics(
        {model::topic_namespace{ntp.ns, ntp.tp.topic}}, model::no_timeout)
      .get();

    // Deleting a managed `compact`-enabled cloud topic should make it unmanaged
    // in the scheduler.
    RPTEST_REQUIRE_EVENTUALLY(
      10s, [&] { return !compaction_scheduler->is_managed(ntp); });
}

TEST_F(CompactionFixture, AlterAndManageUncompactedTopic) {
    // Enabling `compact` cleanup policy on an existing cloud topic should make
    // it managed in the scheduler.
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    create_cloud_topic(ntp, cluster::topic_properties{}).get();

    auto* ct_app = app.cloud_topics_app.get();
    auto* compaction_scheduler = ct_app->get_compaction_scheduler();
    ASSERT_FALSE(compaction_scheduler->is_managed(ntp));

    auto property_update = cluster::incremental_topic_updates{};
    property_update.cleanup_policy_bitflags.op
      = cluster::incremental_update_operation::set;
    property_update.cleanup_policy_bitflags.value
      = model::cleanup_policy_bitflags::compaction;
    auto custom_update = cluster::incremental_topic_custom_updates{};
    auto update = cluster::topic_properties_update_vector{
      cluster::topic_properties_update{
        model::topic_namespace(ntp.ns, ntp.tp.topic),
        std::move(property_update),
        std::move(custom_update)}};

    app.controller->get_topics_frontend()
      .local()
      .update_topic_properties(std::move(update), model::no_timeout)
      .get();

    RPTEST_REQUIRE_EVENTUALLY(
      10s, [&] { return compaction_scheduler->is_managed(ntp); });
}

TEST_F(CompactionFixture, ManageAndAlterCompactedTopic) {
    // Disabling `compact` cleanup policy on a managed cloud topic should make
    // it unmanaged in the scheduler.
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    create_cloud_topic(ntp, compact_topic_props).get();

    auto* ct_app = app.cloud_topics_app.get();
    auto* compaction_scheduler = ct_app->get_compaction_scheduler();
    RPTEST_REQUIRE_EVENTUALLY(
      10s, [&] { return compaction_scheduler->is_managed(ntp); });

    auto property_update = cluster::incremental_topic_updates{};
    property_update.cleanup_policy_bitflags.op
      = cluster::incremental_update_operation::set;
    property_update.cleanup_policy_bitflags.value
      = model::cleanup_policy_bitflags::deletion;
    auto custom_update = cluster::incremental_topic_custom_updates{};
    auto update = cluster::topic_properties_update_vector{
      cluster::topic_properties_update{
        model::topic_namespace(ntp.ns, ntp.tp.topic),
        std::move(property_update),
        std::move(custom_update)}};

    app.controller->get_topics_frontend()
      .local()
      .update_topic_properties(std::move(update), model::no_timeout)
      .get();

    RPTEST_REQUIRE_EVENTUALLY(
      10s, [&] { return !compaction_scheduler->is_managed(ntp); });
}
