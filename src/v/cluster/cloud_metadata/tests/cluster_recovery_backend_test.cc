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
#include "cloud_storage/types.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/cloud_metadata/tests/cluster_metadata_utils.h"
#include "cluster/cloud_metadata/tests/manual_mixin.h"
#include "cluster/cloud_metadata/uploader.h"
#include "cluster/cluster_recovery_reconciler.h"
#include "cluster/config_frontend.h"
#include "cluster/controller_snapshot.h"
#include "cluster/feature_manager.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/partition.h"
#include "cluster/security_frontend.h"
#include "cluster/tests/topic_properties_generator.h"
#include "cluster/tests/tx_compaction_utils.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "ssx/future-util.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/util/defer.hh>

using namespace cluster::cloud_metadata;
namespace {
ss::logger logger("backend_test");
static ss::abort_source never_abort;
} // anonymous namespace

class ClusterRecoveryBackendTest
  : public seastar_test
  , public s3_imposter_fixture
  , public manual_metadata_upload_mixin
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    ClusterRecoveryBackendTest()
      : redpanda_thread_fixture(
          redpanda_thread_fixture::init_cloud_storage_tag{},
          httpd_port_number())
      , raft0(app.partition_manager.local().get(model::controller_ntp)->raft())
      , controller_stm(app.controller->get_controller_stm().local())
      , remote(app.cloud_storage_api.local())
      , bucket(cloud_storage_clients::bucket_name("test-bucket")) {}
    ss::future<> SetUpAsync() override {
        co_await ss::async([&] { set_expectations_and_listen({}); });
        co_await wait_for_controller_leadership();
        RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
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

class ClusterRecoveryBackendLeadershipParamTest
  : public ClusterRecoveryBackendTest
  , public ::testing::WithParamInterface<bool> {};

namespace {

ss::future<> leadership_change_fiber(bool& stop, cluster::partition& p) {
    int num_stepdowns = 0;
    while (!stop) {
        co_await p.raft()->step_down("test");
        RPTEST_REQUIRE_EVENTUALLY_CORO(
          10s, [&] { return stop || p.raft()->is_leader(); });
        if (stop) {
            break;
        }
        // Increase the sleep times to allow for some progress to be made.
        ++num_stepdowns;
        auto sleep_ms = random_generators::get_int(
          std::min(100, 10 * num_stepdowns));
        co_await ss::sleep(sleep_ms * 1ms);
    }
}

} // anonymous namespace

TEST_P(ClusterRecoveryBackendLeadershipParamTest, TestRecoveryControllerState) {
    auto with_leadership_changes = GetParam();

    // Create a license.
    auto license = get_test_license();
    auto err = app.controller->get_feature_manager()
                 .local()
                 .update_license(std::move(license))
                 .get();
    ASSERT_TRUE(!err);

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

    // Write some transactional data to the remote partition so we use up some
    // producer_ids.
    auto num_txns = 10;
    auto* remote_p = app.partition_manager.local()
                       .get(model::ntp{remote_tp_ns.ns, remote_tp_ns.tp, 0})
                       .get();
    cluster::tx_executor::spec spec{
      ._num_txes = num_txns,
      ._num_rolls = 3,
      ._types = cluster::tx_executor::mixed,
      ._interleave = true,
      ._compact = false};
    cluster::tx_executor{}.run_random_workload(
      spec, remote_p->raft()->term(), remote_p->rm_stm(), remote_p->log());

    auto partitions = app.partition_manager.local().partitions();
    for (const auto& [ntp, p] : partitions) {
        if (ntp == model::controller_ntp) {
            continue;
        }
        if (!p->archiver().has_value()) {
            continue;
        }
        auto& archiver = p->archiver().value().get();
        archiver.sync_for_tests().get();
        auto res = archiver.upload_next_candidates().get();
        ASSERT_GT(res.non_compacted_upload_result.num_succeeded, 0);
        archiver.upload_topic_manifest().get();
        archiver.upload_manifest("test").get();
    }
    partitions.clear();

    // Write a controller snapshot and upload it.
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [this] { return controller_stm.maybe_write_snapshot(); });
    auto& uploader = app.controller->metadata_uploader().value().get();
    retry_chain_node retry_node(never_abort, 30s, 1s);
    cluster_metadata_manifest manifest;
    manifest.cluster_uuid = cluster_uuid;
    uploader.upload_next_metadata(raft0->confirmed_term(), manifest, retry_node)
      .get();
    ASSERT_EQ(manifest.metadata_id, cluster_metadata_id(0));
    ASSERT_TRUE(!manifest.controller_snapshot_path.empty());

    // Create a new cluster.
    raft0 = nullptr;
    restart(should_wipe::yes);
    task_local_cfg.get("log_segment_size_jitter_percent").reset();
    RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
        return app.storage.local().get_cluster_uuid().has_value();
    });

    // Sanity check we have a different cluster.
    ASSERT_TRUE(
      !app.controller->get_feature_table().local().get_license().has_value());
    ASSERT_NE(
      1, config::shard_local_cfg().log_segment_size_jitter_percent.value());
    ASSERT_TRUE(!app.controller->get_credential_store().local().contains(
      security::credential_user{"userguy"}));
    ASSERT_EQ(
      0, app.controller->get_authorizer().local().all_bindings().get().size());
    ASSERT_EQ(0, app.controller->get_topics_state().local().all_topics_count());

    // Perform recovery.
    auto recover_err = app.controller->get_cluster_recovery_manager()
                         .local()
                         .initialize_recovery(bucket)
                         .get();
    ASSERT_TRUE(recover_err.has_value());
    ASSERT_EQ(recover_err.value(), cluster::errc::success);

    // If configured, start leadership transfers.
    ss::gate gate;
    bool stop_leadership_switches = false;
    if (with_leadership_changes) {
        auto controller
          = app.partition_manager.local().get(model::controller_ntp).get();
        ssx::spawn_with_gate(gate, [&] {
            return leadership_change_fiber(
              stop_leadership_switches, *controller);
        });
    }
    auto close_gate = ss::defer([&] {
        stop_leadership_switches = true;
        gate.close().get();
    });

    RPTEST_REQUIRE_EVENTUALLY(with_leadership_changes ? 120s : 10s, [&] {
        return !app.controller->get_cluster_recovery_table()
                  .local()
                  .is_recovery_active();
    });

    // Validate the controller state is restored.
    auto validate_post_recovery = [&] {
        ASSERT_TRUE(app.controller->get_feature_table()
                      .local()
                      .get_license()
                      .has_value());
        ASSERT_EQ(
          1, config::shard_local_cfg().log_segment_size_jitter_percent.value());
        ASSERT_TRUE(app.controller->get_credential_store().local().contains(
          security::credential_user{"userguy"}));
        ASSERT_EQ(
          1,
          app.controller->get_authorizer().local().all_bindings().get().size());
        // NOTE: internal topics may be created.
        auto topic_count
          = app.controller->get_topics_state().local().all_topics_count();
        ASSERT_LE(2, topic_count);
        auto partitions = app.partition_manager.local().partitions();
        for (const auto& [ntp, p] : partitions) {
            if (!model::is_user_topic(ntp)) {
                continue;
            }
            auto& tp = p->ntp().tp.topic();
            if (tp == "remote_foo") {
                RPTEST_REQUIRE_EVENTUALLY(
                  5s, [&] { return p->archiver().has_value(); });
            } else {
                ASSERT_EQ(tp, "foo");
                ASSERT_FALSE(p->archiver().has_value());
            }
        }
        auto id_reply
          = app.id_allocator_frontend.local().allocate_id(10s).get();
        ASSERT_GE(id_reply.id, num_txns);
    };
    validate_post_recovery();
    stop_leadership_switches = true;
    gate.close().get();
    close_gate.cancel();

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

    // Validate the order of events in the log.
    auto stages = read_recovery_stages(
                    *app.partition_manager.local().get(model::controller_ntp))
                    .get();
    auto expected_stages = std::vector<cluster::recovery_stage>{
      cluster::recovery_stage::initialized,
      cluster::recovery_stage::recovered_license,
      cluster::recovery_stage::recovered_cluster_config,
      cluster::recovery_stage::recovered_users,
      cluster::recovery_stage::recovered_acls,
      cluster::recovery_stage::recovered_remote_topic_data,
      cluster::recovery_stage::recovered_topic_data,
      cluster::recovery_stage::recovered_controller_snapshot,
      cluster::recovery_stage::recovered_offsets_topic,
      cluster::recovery_stage::recovered_tx_coordinator,
      cluster::recovery_stage::complete};
    if (with_leadership_changes) {
        // With leadership changes, it's possible we skip stages because the
        // action was applied in one term, leadership changed, and the next
        // leader didn't perform the action again. That said, it should still
        // progress in order.
        ASSERT_EQ(stages.front(), cluster::recovery_stage::initialized);
        ASSERT_EQ(stages.back(), cluster::recovery_stage::complete);
        auto prev_stage = stages.front();
        for (const auto& stage : stages) {
            ASSERT_GE(stage, prev_stage);
            prev_stage = stage;
        }
    } else {
        // We should expect a specific order under happy path operation.
        ASSERT_EQ(stages, expected_stages);
    }

    // Do one more validation when reading from the controller snapshot.
    RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
        return app.controller->get_controller_stm()
          .local()
          .maybe_write_snapshot();
    });
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

INSTANTIATE_TEST_SUITE_P(
  WithLeadershipChanges,
  ClusterRecoveryBackendLeadershipParamTest,
  ::testing::Bool());

TEST_F(ClusterRecoveryBackendTest, TestRecoverMissingTopicManifest) {
    // Create a remote topic, but don't upload any of its manifests or anything.
    scoped_config task_local_cfg;
    task_local_cfg.get("cloud_storage_disable_upload_loop_for_tests")
      .set_value(true);
    model::topic_namespace remote_tp_ns{
      model::kafka_namespace, model::topic{"remote_foo"}};
    auto ntp = model::ntp{remote_tp_ns.ns, remote_tp_ns.tp, 0};
    add_topic(remote_tp_ns, 1, uploadable_topic_properties()).get();
    const auto& orig_tp_meta
      = app.controller->get_topics_state().local().get_topic_metadata(
        remote_tp_ns);
    auto orig_tp_revision = orig_tp_meta.value().get_revision();

    for (int i = 0; i < 2; i++) {
        // Upload the controller snapshot to set up a recovery.
        RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
            return app.controller->get_controller_stm()
              .local()
              .maybe_write_snapshot();
        });
        auto& uploader = app.controller->metadata_uploader().value().get();
        retry_chain_node retry_node(never_abort, 30s, 1s);
        cluster_metadata_manifest manifest;
        manifest.cluster_uuid = cluster_uuid;
        uploader
          .upload_next_metadata(raft0->confirmed_term(), manifest, retry_node)
          .get();
        ASSERT_EQ(manifest.metadata_id, cluster_metadata_id(0));
        ASSERT_TRUE(!manifest.controller_snapshot_path.empty());

        // Create a new cluster.
        raft0 = nullptr;
        restart(should_wipe::yes);
        RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
            return app.storage.local().get_cluster_uuid().has_value();
        });
        raft0
          = app.partition_manager.local().get(model::controller_ntp)->raft();

        // Attempt a recovery.
        auto recover_err = app.controller->get_cluster_recovery_manager()
                             .local()
                             .initialize_recovery(bucket)
                             .get();
        ASSERT_TRUE(recover_err.has_value());
        ASSERT_EQ(recover_err.value(), cluster::errc::success);
        RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
            return !app.controller->get_cluster_recovery_table()
                      .local()
                      .is_recovery_active();
        });
        // The recovery should have completed, and our topic should be restored.
        // Even without the topic manifest, it should be restored.
        auto& latest_recovery = app.controller->get_cluster_recovery_table()
                                  .local()
                                  .current_recovery()
                                  .value()
                                  .get();
        ASSERT_EQ(latest_recovery.stage, cluster::recovery_stage::complete);
        auto stages = read_recovery_stages(*app.partition_manager.local().get(
                                             model::controller_ntp))
                        .get();
        auto expected_stages = std::vector<cluster::recovery_stage>{
          cluster::recovery_stage::initialized,
          cluster::recovery_stage::recovered_cluster_config,
          cluster::recovery_stage::recovered_remote_topic_data,
          cluster::recovery_stage::recovered_controller_snapshot,
          cluster::recovery_stage::recovered_offsets_topic,
          cluster::recovery_stage::recovered_tx_coordinator,
          cluster::recovery_stage::complete};
        ASSERT_EQ(stages, expected_stages);

        const auto& tp_meta
          = app.controller->get_topics_state().local().get_topic_metadata(
            remote_tp_ns);
        ASSERT_TRUE(tp_meta.has_value());
        const auto& tp_cfg = tp_meta->get_configuration();
        ASSERT_EQ(1, tp_cfg.partition_count);
        ASSERT_TRUE(tp_cfg.is_recovery_enabled());

        // The restored cluster has a different history recorded in its
        // controller than the original cluster; the revision id assigned to
        // the topic should be different.
        ASSERT_NE(tp_meta->get_revision(), orig_tp_revision);

        // The remove properties, however, should be the same as the original.
        ASSERT_TRUE(tp_cfg.properties.remote_topic_properties.has_value());
        ASSERT_EQ(
          orig_tp_revision,
          tp_cfg.properties.remote_topic_properties->remote_revision());
        ASSERT_EQ(
          1, tp_cfg.properties.remote_topic_properties->remote_partition_count);

        // Even though we didn't upload any partition manifests, the restored
        // partitions should exist and have appropriate remote properties.
        auto* partition = app.partition_manager.local().get(ntp).get();
        ASSERT_TRUE(partition);
        ASSERT_TRUE(partition->archival_meta_stm());
        ASSERT_EQ(
          orig_tp_revision(),
          partition->archival_meta_stm()->manifest().get_revision_id()());
    }
}

TEST_F(ClusterRecoveryBackendTest, TestRecoverFailedDownload) {
    // Create a some topic to be able to upload a snapshot.
    model::topic_namespace remote_tp_ns{
      model::kafka_namespace, model::topic{"remote_foo"}};
    add_topic(remote_tp_ns, 1, uploadable_topic_properties()).get();

    // Upload the controller snapshot to set up a failure to create the table.
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [this] { return controller_stm.maybe_write_snapshot(); });
    auto& uploader = app.controller->metadata_uploader().value().get();
    retry_chain_node retry_node(never_abort, 60s, 1s);
    cluster_metadata_manifest manifest;
    manifest.cluster_uuid = cluster_uuid;
    uploader.upload_next_metadata(raft0->confirmed_term(), manifest, retry_node)
      .get();
    ASSERT_EQ(manifest.metadata_id, cluster_metadata_id(0));
    ASSERT_TRUE(!manifest.controller_snapshot_path.empty());

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
    ASSERT_EQ(res, cloud_storage::upload_result::success);

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
    ASSERT_EQ(latest_recovery.stage, cluster::recovery_stage::failed);
    ASSERT_TRUE(latest_recovery.error_msg.has_value());
    ASSERT_TRUE(latest_recovery.error_msg.value().contains(
      "Failed to download controller snapshot"));
    auto stages = read_recovery_stages(
                    *app.partition_manager.local().get(model::controller_ntp))
                    .get();
    auto expected_stages = std::vector<cluster::recovery_stage>{
      cluster::recovery_stage::initialized, cluster::recovery_stage::failed};
    ASSERT_EQ(stages, expected_stages);
}
