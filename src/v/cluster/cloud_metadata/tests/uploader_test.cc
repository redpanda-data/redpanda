/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_file.h"
#include "cloud_storage/types.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "cluster/cloud_metadata/manifest_downloads.h"
#include "cluster/cloud_metadata/tests/manual_mixin.h"
#include "cluster/cloud_metadata/uploader.h"
#include "cluster/config_frontend.h"
#include "cluster/controller_snapshot.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "storage/snapshot.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/lowres_clock.hh>

#include <gtest/gtest.h>

using namespace cluster::cloud_metadata;

namespace {
ss::logger logger("uploader_test");
static ss::abort_source never_abort;
} // anonymous namespace

class cluster_metadata_uploader_fixture
  : public manual_metadata_upload_mixin
  , public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture
  , public ::testing::Test {
public:
    cluster_metadata_uploader_fixture()
      : redpanda_thread_fixture(
          redpanda_thread_fixture::init_cloud_storage_tag{},
          httpd_port_number())
      , raft0(app.partition_manager.local().get(model::controller_ntp)->raft())
      , controller_stm(app.controller->get_controller_stm().local())
      , remote(app.cloud_storage_api.local())
      , bucket(cloud_storage_clients::bucket_name("test-bucket")) {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
    }
    void SetUp() override {
        RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
            return app.storage.local().get_cluster_uuid().has_value();
        });
        cluster_uuid = app.storage.local().get_cluster_uuid().value();
    }

    // Returns true if the manifest downloaded has a higher metadata ID than
    // `initial_meta_id`.
    ss::future<bool> downloaded_manifest_has_higher_id(
      cluster_metadata_id initial_meta_id,
      cluster_metadata_manifest* downloaded_manifest) {
        retry_chain_node retry_node(
          never_abort, ss::lowres_clock::time_point::max(), 10ms);
        auto m_res = co_await download_highest_manifest_for_cluster(
          remote, cluster_uuid, bucket, retry_node);
        if (!m_res.has_value()) {
            vlog(
              logger.debug,
              "Current manifest has id {}, waiting for > {}",
              -1,
              initial_meta_id);
            co_return false;
        }
        if (m_res.value().metadata_id <= initial_meta_id) {
            vlog(
              logger.debug,
              "Current manifest has id {}, waiting for > {}",
              m_res.value(),
              initial_meta_id);
            co_return false;
        }
        if (m_res.value().controller_snapshot_path.empty()) {
            vlog(logger.debug, "Missing controller snapshot");
            co_return false;
        }
        *downloaded_manifest = std::move(m_res.value());
        co_return true;
    }

    ss::future<bool> list_contains_manifest_contents(
      const cluster::cloud_metadata::cluster_metadata_manifest& manifest) {
        ss::abort_source as;
        retry_chain_node retry_node(
          as, ss::lowres_clock::time_point::max(), 10ms);
        auto list_res = co_await remote.list_objects(bucket, retry_node);
        EXPECT_TRUE(!list_res.has_error());
        const auto& items = list_res.value().contents;
        if (items.empty()) {
            co_return false;
        }
        int expected_count = 0;
        vlog(
          logger.debug,
          "Looking for metadata {} and {}",
          manifest.get_manifest_path()().string(),
          manifest.controller_snapshot_path);
        for (const auto& item : items) {
            vlog(logger.debug, "Listed item: {}", item.key);
            if (
              item.key == manifest.get_manifest_path()().string()
              || item.key == manifest.controller_snapshot_path) {
                expected_count += 1;
            }
        }
        // If we were expecting the metadata, both manifest and snapshot must be
        // present.
        co_return expected_count == 2;
    }

    ss::future<cluster::config_frontend::patch_result>
    patch_config(const cluster::config_update_request& req) {
        for (int i = 0; i < 5; i++) {
            auto result
              = co_await app.controller->get_config_frontend().local().patch(
                req, model::timeout_clock::now() + 5s);
            if (
              result.errc == cluster::errc::not_leader_controller
              || result.errc == cluster::errc::no_leader_controller
              || result.errc == raft::errc::not_leader) {
                vlog(
                  logger.debug,
                  "Not leader, retrying config patch: {}",
                  result.errc);
                co_await ss::sleep(200ms);
            } else {
                co_return result;
            }
        }
        co_return cluster::config_frontend::patch_result{
          .errc = cluster::errc::timeout};
    }

protected:
    scoped_config test_local_cfg;
    cluster::consensus_ptr raft0;
    cluster::controller_stm& controller_stm;
    cloud_storage::remote& remote;
    const cloud_storage_clients::bucket_name bucket;
    model::cluster_uuid cluster_uuid;
};

TEST_F(cluster_metadata_uploader_fixture, test_download_highest_manifest) {
    auto& uploader = app.controller->metadata_uploader().value().get();
    retry_chain_node retry_node(
      never_abort, ss::lowres_clock::time_point::max(), 10ms);

    // When there are no manifests, the uploader should start out with an
    // inavlid metadata ID.
    auto down_res
      = uploader.download_highest_manifest_or_create(retry_node).get();
    ASSERT_TRUE(down_res.has_value());
    ASSERT_EQ(down_res.value().cluster_uuid, cluster_uuid);
    ASSERT_EQ(down_res.value().metadata_id, cluster_metadata_id{});

    cluster_metadata_manifest m;
    m.upload_time_since_epoch
      = std::chrono::duration_cast<std::chrono::milliseconds>(
        ss::lowres_system_clock::now().time_since_epoch());
    m.cluster_uuid = cluster_uuid;
    m.metadata_id = cluster_metadata_id(10);

    // Upload a manifest and check that we download it.
    auto up_res
      = remote.upload_manifest(bucket, m, m.get_manifest_path(), retry_node)
          .get();
    ASSERT_EQ(up_res, cloud_storage::upload_result::success);
    down_res = uploader.download_highest_manifest_or_create(retry_node).get();
    ASSERT_TRUE(down_res.has_value());
    ASSERT_EQ(down_res.value(), m);

    // If we upload a manifest with a lower metadata ID, the higher one should
    // be downloaded.
    m.metadata_id = cluster_metadata_id(9);
    up_res = remote
               .upload_manifest(bucket, m, m.get_manifest_path(), retry_node)
               .get();
    m.metadata_id = cluster_metadata_id(10);
    ASSERT_EQ(up_res, cloud_storage::upload_result::success);
    down_res = uploader.download_highest_manifest_or_create(retry_node).get();
    ASSERT_TRUE(down_res.has_value());
    ASSERT_EQ(down_res.value(), m);
}

TEST_F(
  cluster_metadata_uploader_fixture, test_download_highest_manifest_errors) {
    auto& uploader = app.controller->metadata_uploader().value().get();
    retry_chain_node retry_node(
      never_abort, ss::lowres_clock::time_point::min(), 10ms);
    auto down_res
      = uploader.download_highest_manifest_or_create(retry_node).get();
    ASSERT_TRUE(down_res.has_error());
    ASSERT_EQ(down_res.error(), error_outcome::list_failed);
}

TEST_F(cluster_metadata_uploader_fixture, test_upload_next_metadata) {
    auto& uploader = app.controller->metadata_uploader().value().get();
    retry_chain_node retry_node(
      never_abort, ss::lowres_clock::time_point::max(), 10ms);
    RPTEST_REQUIRE_EVENTUALLY(5s, [this] { return raft0->is_leader(); });
    auto down_res
      = uploader.download_highest_manifest_or_create(retry_node).get();
    ASSERT_TRUE(down_res.has_value());
    auto& manifest = down_res.value();
    ASSERT_EQ(manifest.metadata_id, cluster_metadata_id{});

    RPTEST_REQUIRE_EVENTUALLY(
      5s, [this] { return controller_stm.maybe_write_snapshot(); });
    // Uploading the first time should set the metadata ID to 0, and we should
    // increment from there.
    ss::sstring first_controller_snapshot_path;
    for (int i = 0; i < 3; i++) {
        auto err = uploader
                     .upload_next_metadata(
                       raft0->confirmed_term(), manifest, retry_node)
                     .get();
        ASSERT_EQ(err, error_outcome::success);
        ASSERT_EQ(manifest.metadata_id, cluster_metadata_id(i));
        if (first_controller_snapshot_path.empty()) {
            first_controller_snapshot_path = manifest.controller_snapshot_path;
        } else {
            // After the first upload, the subsequent controller snapshots
            // won't be changed, since no controller updates will have
            // happened.
            ASSERT_EQ(
              manifest.controller_snapshot_path,
              first_controller_snapshot_path);
        }
    }
    ASSERT_EQ(manifest.metadata_id, cluster_metadata_id(2));

    // Do a sanity check that we can read the controller.
    ASSERT_TRUE(!first_controller_snapshot_path.empty());
    cloud_storage::remote_file remote_file(
      remote,
      app.shadow_index_cache.local(),
      bucket,
      cloud_storage::remote_segment_path(first_controller_snapshot_path),
      retry_node,
      "controller");

    auto f = remote_file.hydrate_readable_file().get();
    ss::file_input_stream_options options;
    auto input = ss::make_file_input_stream(f, options);
    storage::snapshot_reader reader(
      std::move(f), std::move(input), remote_file.local_path());
    auto close = ss::defer([&reader] { reader.close().get(); });

    auto snap_metadata_buf = reader.read_metadata().get();
    auto snap_metadata_parser = iobuf_parser(std::move(snap_metadata_buf));
    auto snap_metadata = reflection::adl<raft::snapshot_metadata>{}.from(
      snap_metadata_parser);
    const size_t snap_size = reader.get_snapshot_size().get();
    auto snap_buf_parser = iobuf_parser{
      read_iobuf_exactly(reader.input(), snap_size).get()};
    auto snapshot
      = serde::read_async<cluster::controller_snapshot>(snap_buf_parser).get();
    ASSERT_EQ(snapshot.bootstrap.cluster_uuid, cluster_uuid);

    // We should see timeouts when appropriate; errors should still increment.
    retry_chain_node bad_retry_node(
      never_abort, ss::lowres_clock::time_point::min(), 10ms);
    auto err = uploader
                 .upload_next_metadata(
                   raft0->confirmed_term() + model::term_id(1),
                   manifest,
                   bad_retry_node)
                 .get();
    ASSERT_EQ(manifest.metadata_id, cluster_metadata_id(3));

    // If we attempt to upload while the term is different from expected, we
    // should see an error.
    err = uploader
            .upload_next_metadata(
              raft0->confirmed_term() - model::term_id(1), manifest, retry_node)
            .get();
    ASSERT_EQ(err, error_outcome::term_has_changed);
    ASSERT_EQ(manifest.metadata_id, cluster_metadata_id(4));
}

// Test that the upload fiber uploads monotonically increasing metadata, and
// that the fiber stop when leadership changes.
TEST_F(cluster_metadata_uploader_fixture, test_upload_in_term) {
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [this] { return controller_stm.maybe_write_snapshot(); });
    const auto get_local_snap_offset = [&] {
        auto snap = raft0->open_snapshot().get();
        EXPECT_TRUE(snap.has_value());
        auto ret = snap->metadata.last_included_index;
        snap->close().get();
        return ret;
    };
    const auto snap_offset = get_local_snap_offset();

    test_local_cfg.get("cloud_storage_cluster_metadata_upload_interval_ms")
      .set_value(1000ms);
    auto& uploader = app.controller->metadata_uploader().value().get();
    cluster::cloud_metadata::cluster_metadata_id highest_meta_id{0};

    // Checks that metadata is uploaded a new term, stepping down in between
    // calls, and ensuring that subsequent calls yield manifests with higher
    // metadata IDs and the expected snapshot offset.
    const auto check_uploads_in_term_and_stepdown =
      [&](model::offset expected_snap_offset) {
          // Wait to become leader before uploading.
          RPTEST_REQUIRE_EVENTUALLY(5s, [this] { return raft0->is_leader(); });

          // Start uploading in this term.
          auto upload_in_term = uploader.upload_until_term_change();

          // Keep checking the latest manifest for whether the metadata ID is
          // some non-zero value (indicating we've uploaded multiple manifests);
          auto initial_meta_id = highest_meta_id;
          cluster::cloud_metadata::cluster_metadata_manifest manifest;
          RPTEST_REQUIRE_EVENTUALLY(10s, [&]() -> ss::future<bool> {
              return downloaded_manifest_has_higher_id(
                initial_meta_id, &manifest);
          });
          ASSERT_GT(manifest.metadata_id, highest_meta_id);
          highest_meta_id = manifest.metadata_id;

          ASSERT_EQ(manifest.controller_snapshot_offset, expected_snap_offset);

          // Stop the upload loop and continue in a new term.
          raft0->step_down("forced stepdown").get();
          upload_in_term.get();
      };
    for (int i = 0; i < 3; ++i) {
        check_uploads_in_term_and_stepdown(snap_offset);
    }

    // Now do some action and write a new snapshot.
    RPTEST_REQUIRE_EVENTUALLY(5s, [this] { return raft0->is_leader(); });
    auto result = patch_config(cluster::config_update_request{
                                 .upsert = {{"cluster_id", "foo"}}})
                    .get();
    ASSERT_TRUE(!result.errc)
      << fmt::format("errc {} version {}", result.errc, result.version);
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [this] { return controller_stm.maybe_write_snapshot(); });
    const auto new_snap_offset = get_local_snap_offset();
    ASSERT_NE(new_snap_offset, snap_offset);
    check_uploads_in_term_and_stepdown(new_snap_offset);
}

TEST_F(cluster_metadata_uploader_fixture, test_upload_loop_deletes_orphans) {
    // Write a snapshot and begin the upload loop.
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [this] { return controller_stm.maybe_write_snapshot(); });
    test_local_cfg.get("cloud_storage_cluster_metadata_upload_interval_ms")
      .set_value(1000ms);
    auto& uploader = app.controller->metadata_uploader().value().get();
    RPTEST_REQUIRE_EVENTUALLY(5s, [this] { return raft0->is_leader(); });

    ssx::background = uploader.upload_until_term_change().handle_exception_type(
      [](const seastar::abort_requested_exception& e) { std::ignore = e; });
    // Wait for some valid metadata to show up.
    cluster::cloud_metadata::cluster_metadata_manifest manifest;
    RPTEST_REQUIRE_EVENTUALLY(5s, [this, &manifest] {
        return downloaded_manifest_has_higher_id(
          cluster::cloud_metadata::cluster_metadata_id{-1}, &manifest);
    });
    RPTEST_REQUIRE_EVENTUALLY(5s, [this, &manifest] {
        return list_contains_manifest_contents(manifest);
    });

    // Now do something to trigger another controller snapshot.
    auto result = patch_config(cluster::config_update_request{
                                 .upsert = {{"cluster_id", "foo"}}})
                    .get();

    ASSERT_TRUE(!result.errc);
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [this] { return controller_stm.maybe_write_snapshot(); });

    // The uploader should delete the stale manifest and snapshot.
    RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
        auto& s3_reqs = get_requests();
        int num_deletes = 0;
        for (const auto& r : s3_reqs) {
            if (r.method == "DELETE") {
                num_deletes++;
            }
        }
        return num_deletes >= 2;
    });
}

TEST_F(cluster_metadata_uploader_fixture, test_run_loop) {
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [this] { return controller_stm.maybe_write_snapshot(); });
    test_local_cfg.get("cloud_storage_cluster_metadata_upload_interval_ms")
      .set_value(1000ms);
    auto& uploader = app.controller->metadata_uploader().value().get();
    // Run the upload loop and make sure that new leaders continue to upload.
    uploader.start();
    cluster::cloud_metadata::cluster_metadata_id highest_meta_id{-1};
    for (int i = 0; i < 3; i++) {
        auto initial_meta_id = highest_meta_id;
        cluster::cloud_metadata::cluster_metadata_manifest manifest;
        RPTEST_REQUIRE_EVENTUALLY(10s, [&]() -> ss::future<bool> {
            return downloaded_manifest_has_higher_id(
              initial_meta_id, &manifest);
        });
        ASSERT_GT(manifest.metadata_id, highest_meta_id);
        highest_meta_id = manifest.metadata_id;

        // Stop the upload loop and continue in a new term.
        raft0->step_down("forced stepdown").get();
    }
}
