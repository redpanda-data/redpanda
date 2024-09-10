/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_storage/remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cluster/cloud_metadata/tests/cluster_metadata_utils.h"
#include "cluster/cluster_recovery_reconciler.h"
#include "cluster/controller_snapshot.h"
#include "cluster/feature_manager.h"
#include "cluster/security_frontend.h"
#include "cluster/tests/topic_properties_generator.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "test_utils/scoped_config.h"

using namespace cluster::cloud_metadata;

namespace {
ss::logger logger("reonciliation_test");
} // anonymous namespace

class controller_snapshot_reconciliation_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    controller_snapshot_reconciliation_fixture()
      : redpanda_thread_fixture(
          redpanda_thread_fixture::init_cloud_storage_tag{},
          httpd_port_number())
      , raft0(app.partition_manager.local().get(model::controller_ntp)->raft())
      , controller_stm(app.controller->get_controller_stm().local())
      , remote(app.cloud_storage_api.local())
      , bucket(cloud_storage_clients::bucket_name("test-bucket"))
      , reconciler(
          app.controller->get_cluster_recovery_table().local(),
          app.feature_table.local(),
          app.controller->get_credential_store().local(),
          app.controller->get_topics_state().local()) {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
        RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
            return app.storage.local().get_cluster_uuid().has_value();
        });
        cluster_uuid = app.storage.local().get_cluster_uuid().value();
    }

protected:
    cluster::consensus_ptr raft0;
    cluster::controller_stm& controller_stm;
    cloud_storage::remote& remote;
    const cloud_storage_clients::bucket_name bucket;
    model::cluster_uuid cluster_uuid;

    controller_snapshot_reconciler reconciler;
};

namespace {

bool actions_contain(
  const controller_snapshot_reconciler::controller_actions& actions,
  cluster::recovery_stage stage) {
    for (const auto& s : actions.stages) {
        if (s == stage) {
            return true;
        }
    }
    return false;
}

void validate_actions(
  const controller_snapshot_reconciler::controller_actions& actions) {
    // Each recovery stage should come with a corresponding metadata with which
    // to restore.
    BOOST_REQUIRE_EQUAL(
      actions.license.has_value(),
      actions_contain(actions, cluster::recovery_stage::recovered_license));

    BOOST_REQUIRE_EQUAL(
      !actions.config.upsert.empty(),
      actions_contain(
        actions, cluster::recovery_stage::recovered_cluster_config));

    BOOST_REQUIRE_EQUAL(
      !actions.users.empty(),
      actions_contain(actions, cluster::recovery_stage::recovered_users));

    BOOST_REQUIRE_EQUAL(
      !actions.acls.empty(),
      actions_contain(actions, cluster::recovery_stage::recovered_acls));

    BOOST_REQUIRE_EQUAL(
      !actions.remote_topics.empty(),
      actions_contain(
        actions, cluster::recovery_stage::recovered_remote_topic_data));

    BOOST_REQUIRE_EQUAL(
      !actions.local_topics.empty(),
      actions_contain(actions, cluster::recovery_stage::recovered_topic_data));
}

} // anonymous namespace

FIXTURE_TEST(
  test_reconcile_license, controller_snapshot_reconciliation_fixture) {
    auto license = get_test_license();
    cluster::controller_snapshot snap;
    snap.features.snap.license = license;

    auto actions = reconciler.get_actions(snap);
    BOOST_REQUIRE(
      actions_contain(actions, cluster::recovery_stage::recovered_license));
    validate_actions(actions);

    // Once we have a license, we shouldn't need to action it anymore.
    auto err = app.controller->get_feature_manager()
                 .local()
                 .update_license(std::move(license))
                 .get();
    BOOST_REQUIRE(!err);
    actions = reconciler.get_actions(snap);
    BOOST_REQUIRE(
      !actions_contain(actions, cluster::recovery_stage::recovered_license));
    validate_actions(actions);
}

FIXTURE_TEST(
  test_reconcile_cluster_config, controller_snapshot_reconciliation_fixture) {
    auto d = ss::defer(
      [] { config::shard_local_cfg().for_each([](auto& p) { p.reset(); }); });
    cluster::controller_snapshot snap;

    // First set a config that's on the blacklist, and a config that doesn't
    // exist. These will be ignored.
    snap.config.values.emplace("cluster_id", "foo");
    snap.config.values.emplace("whats_this", "magic_in_the_air");
    auto actions = reconciler.get_actions(snap);
    BOOST_REQUIRE(actions.empty());

    // Pick some arbitrary property to change. It should result in some action.
    snap.config.values.emplace("log_segment_size_jitter_percent", "1");
    actions = reconciler.get_actions(snap);
    BOOST_REQUIRE(actions_contain(
      actions, cluster::recovery_stage::recovered_cluster_config));
    validate_actions(actions);

    // Even if the value is set, it results in an action.
    config::shard_local_cfg().log_segment_size_jitter_percent.set_value(1);
    BOOST_REQUIRE(actions_contain(
      actions, cluster::recovery_stage::recovered_cluster_config));
    validate_actions(actions);
}

FIXTURE_TEST(test_reconcile_users, controller_snapshot_reconciliation_fixture) {
    cluster::controller_snapshot snap;
    auto& security_snap = snap.security;
    security_snap.user_credentials.emplace_back(cluster::user_and_credential{
      security::credential_user{"userguy"}, security::scram_credential{}});

    auto actions = reconciler.get_actions(snap);
    BOOST_REQUIRE(
      actions_contain(actions, cluster::recovery_stage::recovered_users));
    validate_actions(actions);

    app.controller->get_security_frontend()
      .local()
      .create_user(
        security::credential_user{"userguy"},
        {},
        model::timeout_clock::now() + 30s)
      .get();

    actions = reconciler.get_actions(snap);
    BOOST_REQUIRE(actions.empty());
}

FIXTURE_TEST(test_reconcile_acls, controller_snapshot_reconciliation_fixture) {
    cluster::controller_snapshot snap;
    auto binding = binding_for_user("__pandaproxy");
    snap.security.acls.emplace_back(binding);

    // When the snapshot contains some ACLs, we should see actions.
    auto actions = reconciler.get_actions(snap);
    BOOST_REQUIRE(
      actions_contain(actions, cluster::recovery_stage::recovered_acls));
    validate_actions(actions);

    // The current implementation doesn't dedupe.
    app.controller->get_security_frontend()
      .local()
      .create_acls({binding}, 5s)
      .get();
    actions = reconciler.get_actions(snap);
    BOOST_REQUIRE(
      actions_contain(actions, cluster::recovery_stage::recovered_acls));
    validate_actions(actions);
}

FIXTURE_TEST(
  test_reconcile_remote_topics, controller_snapshot_reconciliation_fixture) {
    cluster::controller_snapshot snap;
    model::topic_namespace tp_ns{model::kafka_namespace, model::topic{"foo"}};
    auto& tps = snap.topics.topics[tp_ns];
    tps.metadata.configuration.properties = uploadable_topic_properties();

    auto actions = reconciler.get_actions(snap);
    BOOST_REQUIRE(actions_contain(
      actions, cluster::recovery_stage::recovered_remote_topic_data));
    BOOST_REQUIRE(
      !actions_contain(actions, cluster::recovery_stage::recovered_topic_data));
    validate_actions(actions);

    // Topic creation is deduped, even if the exact properties don't match.
    add_topic(tp_ns, 1, uploadable_topic_properties()).get();
    actions = reconciler.get_actions(snap);
    BOOST_REQUIRE(actions.empty());
}

FIXTURE_TEST(
  test_reconcile_non_remote_topics,
  controller_snapshot_reconciliation_fixture) {
    cluster::controller_snapshot snap;
    model::topic_namespace tp_ns{model::kafka_namespace, model::topic{"foo"}};
    auto& tps = snap.topics.topics[tp_ns];
    tps.metadata.configuration.properties = non_remote_topic_properties();

    auto actions = reconciler.get_actions(snap);
    BOOST_REQUIRE(!actions_contain(
      actions, cluster::recovery_stage::recovered_remote_topic_data));
    BOOST_REQUIRE(
      actions_contain(actions, cluster::recovery_stage::recovered_topic_data));
    validate_actions(actions);

    // Topic creation is deduped, even if the exact properties don't match.
    add_topic(tp_ns, 1, non_remote_topic_properties()).get();
    actions = reconciler.get_actions(snap);
    BOOST_REQUIRE(actions.empty());
}
