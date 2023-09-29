/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "archival/ntp_archiver_service.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "cluster/cloud_metadata/tests/cluster_metadata_utils.h"
#include "cluster/cloud_metadata/uploader.h"
#include "cluster/cluster_recovery_reconciler.h"
#include "cluster/config_frontend.h"
#include "cluster/controller_snapshot.h"
#include "cluster/feature_manager.h"
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
#include "utils/retry_chain_node.h"

#include <seastar/core/lowres_clock.hh>

using namespace cluster::cloud_metadata;
namespace {
ss::logger logger("backend_test");
static ss::abort_source never_abort;
} // anonymous namespace

class cluster_recovery_backend_test
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    cluster_recovery_backend_test()
      : redpanda_thread_fixture(
        redpanda_thread_fixture::init_cloud_storage_tag{}, httpd_port_number())
      , raft0(app.partition_manager.local().get(model::controller_ntp)->raft())
      , controller_stm(app.controller->get_controller_stm().local())
      , remote(app.cloud_storage_api.local())
      , bucket(cloud_storage_clients::bucket_name("test-bucket")) {
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
};

FIXTURE_TEST(test_recover_controller_state, cluster_recovery_backend_test) {
    // Create a license.
    auto license = get_test_license();
    auto err = app.controller->get_feature_manager()
                 .local()
                 .update_license(std::move(license))
                 .get();
    BOOST_REQUIRE(!err);

    // Update the cluster config (via the controller, rather than shard local).
    cluster::config_update_request req;
    req.upsert.emplace_back("log_segment_size_jitter_percent", "1");
    app.controller->get_config_frontend()
      .local()
      .patch(std::move(req), model::timeout_clock::now() + 30s)
      .get();

    // Create a user.
    app.controller->get_security_frontend()
      .local()
      .create_user(
        security::credential_user{"userguy"},
        {},
        model::timeout_clock::now() + 30s)
      .get();

    // Create an ACL.
    auto binding = binding_for_user("__pandaproxy");
    app.controller->get_security_frontend()
      .local()
      .create_acls({binding}, 5s)
      .get();

    // Create some topics, but disable the upload loop so we can manually flush
    // their manifests.
    scoped_config task_local_cfg;
    task_local_cfg.get("cloud_storage_disable_upload_loop_for_tests")
      .set_value(true);

    model::topic_namespace remote_tp_ns{
      model::kafka_namespace, model::topic{"remote_foo"}};
    add_topic(remote_tp_ns, 1, uploadable_topic_properties()).get();
    model::topic_namespace tp_ns{model::kafka_namespace, model::topic{"foo"}};
    add_topic(tp_ns, 1, non_remote_topic_properties()).get();

    for (const auto& [ntp, p] : app.partition_manager.local().partitions()) {
        if (ntp == model::controller_ntp) {
            continue;
        }
        if (!p->archiver().has_value()) {
            continue;
        }
        auto& archiver = p->archiver().value().get();
        archiver.sync_for_tests().get();
        archiver.upload_topic_manifest().get();
        archiver.upload_manifest("test").get();
    }

    // Write a controller snapshot and upload it.
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [this] { return controller_stm.maybe_write_snapshot(); });
    auto& uploader = app.controller->metadata_uploader();
    retry_chain_node retry_node(never_abort, 30s, 1s);
    cluster_metadata_manifest manifest;
    manifest.cluster_uuid = cluster_uuid;
    uploader.upload_next_metadata(raft0->confirmed_term(), manifest, retry_node)
      .get();
    BOOST_REQUIRE_EQUAL(manifest.metadata_id, cluster_metadata_id(0));
    BOOST_REQUIRE(!manifest.controller_snapshot_path.empty());

    // Create a new cluster.
    raft0 = nullptr;
    restart(should_wipe::yes);
    config::shard_local_cfg().for_each([](auto& p) { p.reset(); });
    RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
        return app.storage.local().get_cluster_uuid().has_value();
    });

    // Sanity check we have a different cluster.
    BOOST_REQUIRE(
      !app.controller->get_feature_table().local().get_license().has_value());
    BOOST_REQUIRE_NE(
      1, config::shard_local_cfg().log_segment_size_jitter_percent.value());
    BOOST_REQUIRE(!app.controller->get_credential_store().local().contains(
      security::credential_user{"userguy"}));
    BOOST_REQUIRE_EQUAL(
      0, app.controller->get_authorizer().local().all_bindings().get().size());
    BOOST_REQUIRE_EQUAL(
      0, app.controller->get_topics_state().local().all_topics_count());

    // Perform recovery.
    auto recover_err = app.controller->get_cluster_recovery_manager()
                         .local()
                         .initialize_recovery(bucket)
                         .get();
    BOOST_REQUIRE(recover_err.has_value());
    BOOST_REQUIRE_EQUAL(recover_err.value(), cluster::errc::success);
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return !app.controller->get_cluster_recovery_table()
                  .local()
                  .is_recovery_active();
    });

    // Validate the controller state is restored.
    auto validate_post_recovery = [&] {
        BOOST_REQUIRE(app.controller->get_feature_table()
                        .local()
                        .get_license()
                        .has_value());
        BOOST_REQUIRE_EQUAL(
          1, config::shard_local_cfg().log_segment_size_jitter_percent.value());
        BOOST_REQUIRE(app.controller->get_credential_store().local().contains(
          security::credential_user{"userguy"}));
        BOOST_REQUIRE_EQUAL(
          1,
          app.controller->get_authorizer().local().all_bindings().get().size());
        BOOST_REQUIRE_EQUAL(
          2, app.controller->get_topics_state().local().all_topics_count());
        for (const auto& [ntp, p] :
             app.partition_manager.local().partitions()) {
            if (ntp == model::controller_ntp) {
                continue;
            }
            if (p->archiver().has_value()) {
                BOOST_REQUIRE_EQUAL(p->ntp().tp.topic(), "remote_foo");
            } else {
                BOOST_REQUIRE_EQUAL(p->ntp().tp.topic(), "foo");
            }
        }
    };
    validate_post_recovery();

    // Sanity check that the above invariants still hold after restarting.
    restart(should_wipe::no);
    RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
        auto latest_recovery = app.controller->get_cluster_recovery_table()
                                 .local()
                                 .current_recovery();
        return latest_recovery.has_value()
               && latest_recovery.value().get().stage
                    == cluster::recovery_stage::complete;
    });
    validate_post_recovery();
}

FIXTURE_TEST(test_recover_failed_action, cluster_recovery_backend_test) {
    // Create a remote topic, but don't upload any of its manifests or anything.
    scoped_config task_local_cfg;
    task_local_cfg.get("cloud_storage_disable_upload_loop_for_tests")
      .set_value(true);
    model::topic_namespace remote_tp_ns{
      model::kafka_namespace, model::topic{"remote_foo"}};
    add_topic(remote_tp_ns, 1, uploadable_topic_properties()).get();

    // Upload the controller snapshot to set up a failure to create the table.
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [this] { return controller_stm.maybe_write_snapshot(); });
    auto& uploader = app.controller->metadata_uploader();
    retry_chain_node retry_node(never_abort, 30s, 1s);
    cluster_metadata_manifest manifest;
    manifest.cluster_uuid = cluster_uuid;
    uploader.upload_next_metadata(raft0->confirmed_term(), manifest, retry_node)
      .get();
    BOOST_REQUIRE_EQUAL(manifest.metadata_id, cluster_metadata_id(0));
    BOOST_REQUIRE(!manifest.controller_snapshot_path.empty());

    // Create a new cluster.
    raft0 = nullptr;
    restart(should_wipe::yes);
    RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
        return app.storage.local().get_cluster_uuid().has_value();
    });

    // Attempt a recovery.
    auto recover_err = app.controller->get_cluster_recovery_manager()
                         .local()
                         .initialize_recovery(bucket)
                         .get();
    BOOST_REQUIRE(recover_err.has_value());
    BOOST_REQUIRE_EQUAL(recover_err.value(), cluster::errc::success);
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return !app.controller->get_cluster_recovery_table()
                  .local()
                  .is_recovery_active();
    });
    auto& latest_recovery = app.controller->get_cluster_recovery_table()
                              .local()
                              .current_recovery()
                              .value()
                              .get();
    BOOST_REQUIRE_EQUAL(latest_recovery.stage, cluster::recovery_stage::failed);
    BOOST_REQUIRE(latest_recovery.error_msg.has_value());
    BOOST_REQUIRE(latest_recovery.error_msg.value().contains(
      "Failed to apply action for "
      "recovery_stage::recovered_remote_topic_data"));
}

FIXTURE_TEST(test_recover_failed_download, cluster_recovery_backend_test) {
    // Create a some topic to be able to upload a snapshot.
    model::topic_namespace remote_tp_ns{
      model::kafka_namespace, model::topic{"remote_foo"}};
    add_topic(remote_tp_ns, 1, uploadable_topic_properties()).get();

    // Upload the controller snapshot to set up a failure to create the table.
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [this] { return controller_stm.maybe_write_snapshot(); });
    auto& uploader = app.controller->metadata_uploader();
    retry_chain_node retry_node(never_abort, 60s, 1s);
    cluster_metadata_manifest manifest;
    manifest.cluster_uuid = cluster_uuid;
    uploader.upload_next_metadata(raft0->confirmed_term(), manifest, retry_node)
      .get();
    BOOST_REQUIRE_EQUAL(manifest.metadata_id, cluster_metadata_id(0));
    BOOST_REQUIRE(!manifest.controller_snapshot_path.empty());

    // Create a new cluster.
    raft0 = nullptr;
    restart(should_wipe::yes);
    RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
        return app.storage.local().get_cluster_uuid().has_value();
    });

    auto res = app.cloud_storage_api.local()
                 .delete_object(
                   bucket,
                   cloud_storage_clients::object_key{
                     manifest.controller_snapshot_path},
                   retry_node)
                 .get();
    BOOST_REQUIRE_EQUAL(res, cloud_storage::upload_result::success);

    // Attempt a recovery.
    auto recover_err = app.controller->get_cluster_recovery_manager()
                         .local()
                         .initialize_recovery(bucket)
                         .get();
    BOOST_REQUIRE(recover_err.has_value());
    BOOST_REQUIRE_EQUAL(recover_err.value(), cluster::errc::success);
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return !app.controller->get_cluster_recovery_table()
                  .local()
                  .is_recovery_active();
    });
    auto& latest_recovery = app.controller->get_cluster_recovery_table()
                              .local()
                              .current_recovery()
                              .value()
                              .get();
    BOOST_REQUIRE_EQUAL(latest_recovery.stage, cluster::recovery_stage::failed);
    BOOST_REQUIRE(latest_recovery.error_msg.has_value());
    BOOST_REQUIRE(latest_recovery.error_msg.value().contains(
      "Failed to download controller snapshot"));
}
