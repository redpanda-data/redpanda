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
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "cluster/cloud_metadata/manifest_downloads.h"
#include "cluster/cloud_metadata/uploader.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>

using namespace cluster::cloud_metadata;

namespace {
static ss::abort_source never_abort;
} // anonymous namespace

class cluster_metadata_uploader_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    cluster_metadata_uploader_fixture()
      : redpanda_thread_fixture(
        redpanda_thread_fixture::init_cloud_storage_tag{}, httpd_port_number())
      , raft0(app.partition_manager.local().get(model::controller_ntp)->raft())
      , remote(app.cloud_storage_api.local())
      , bucket(cloud_storage_clients::bucket_name("test-bucket")) {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
        tests::cooperative_spin_wait_with_timeout(5s, [this] {
            return app.storage.local().get_cluster_uuid().has_value();
        }).get();
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
            co_return false;
        }
        if (m_res.value().metadata_id <= initial_meta_id) {
            co_return false;
        }
        *downloaded_manifest = std::move(m_res.value());
        co_return true;
    }

protected:
    cluster::consensus_ptr raft0;
    cloud_storage::remote& remote;
    const cloud_storage_clients::bucket_name bucket;
    model::cluster_uuid cluster_uuid;
};

FIXTURE_TEST(
  test_download_highest_manifest, cluster_metadata_uploader_fixture) {
    cluster::cloud_metadata::uploader uploader(
      cluster_uuid, bucket, remote, raft0);
    retry_chain_node retry_node(
      never_abort, ss::lowres_clock::time_point::max(), 10ms);

    // When there are no manifests, the uploader should start out with an
    // inavlid metadata ID.
    auto down_res
      = uploader.download_highest_manifest_or_create(retry_node).get();
    BOOST_REQUIRE(down_res.has_value());
    BOOST_REQUIRE_EQUAL(down_res.value().cluster_uuid, cluster_uuid);
    BOOST_REQUIRE_EQUAL(down_res.value().metadata_id, cluster_metadata_id{});

    cluster_metadata_manifest m;
    m.upload_time_since_epoch
      = std::chrono::duration_cast<std::chrono::milliseconds>(
        ss::lowres_system_clock::now().time_since_epoch());
    m.cluster_uuid = cluster_uuid;
    m.metadata_id = cluster_metadata_id(10);

    // Upload a manifest and check that we download it.
    auto up_res = remote.upload_manifest(bucket, m, retry_node).get();
    BOOST_REQUIRE_EQUAL(up_res, cloud_storage::upload_result::success);
    down_res = uploader.download_highest_manifest_or_create(retry_node).get();
    BOOST_REQUIRE(down_res.has_value());
    BOOST_REQUIRE_EQUAL(down_res.value(), m);

    // If we upload a manifest with a lower metadata ID, the higher one should
    // be downloaded.
    m.metadata_id = cluster_metadata_id(9);
    up_res = remote.upload_manifest(bucket, m, retry_node).get();
    m.metadata_id = cluster_metadata_id(10);
    BOOST_REQUIRE_EQUAL(up_res, cloud_storage::upload_result::success);
    down_res = uploader.download_highest_manifest_or_create(retry_node).get();
    BOOST_REQUIRE(down_res.has_value());
    BOOST_REQUIRE_EQUAL(down_res.value(), m);
}

FIXTURE_TEST(
  test_download_highest_manifest_errors, cluster_metadata_uploader_fixture) {
    cluster::cloud_metadata::uploader uploader(
      cluster_uuid, bucket, remote, raft0);
    retry_chain_node retry_node(
      never_abort, ss::lowres_clock::time_point::min(), 10ms);
    auto down_res
      = uploader.download_highest_manifest_or_create(retry_node).get();
    BOOST_REQUIRE(down_res.has_error());
    BOOST_REQUIRE_EQUAL(down_res.error(), error_outcome::list_failed);
}

FIXTURE_TEST(test_upload_next_metadata, cluster_metadata_uploader_fixture) {
    cluster::cloud_metadata::uploader uploader(
      cluster_uuid, bucket, remote, raft0);
    retry_chain_node retry_node(
      never_abort, ss::lowres_clock::time_point::max(), 10ms);
    tests::cooperative_spin_wait_with_timeout(5s, [this] {
        return raft0->is_leader();
    }).get();
    auto down_res
      = uploader.download_highest_manifest_or_create(retry_node).get();
    BOOST_REQUIRE(down_res.has_value());
    auto& manifest = down_res.value();
    BOOST_REQUIRE_EQUAL(manifest.metadata_id, cluster_metadata_id{});

    // Uploading the first time should set the metadata ID to 0, and we should
    // increment from there.
    for (int i = 0; i < 3; i++) {
        auto err = uploader
                     .upload_next_metadata(
                       raft0->confirmed_term(), manifest, retry_node)
                     .get();
        BOOST_REQUIRE_EQUAL(err, error_outcome::success);
        BOOST_REQUIRE_EQUAL(manifest.metadata_id, cluster_metadata_id(i));
    }
    BOOST_REQUIRE_EQUAL(manifest.metadata_id, cluster_metadata_id(2));

    // We should see timeouts when appropriate; errors should still increment.
    retry_chain_node bad_retry_node(
      never_abort, ss::lowres_clock::time_point::min(), 10ms);
    auto err = uploader
                 .upload_next_metadata(
                   raft0->confirmed_term() + model::term_id(1),
                   manifest,
                   bad_retry_node)
                 .get();
    BOOST_REQUIRE_EQUAL(manifest.metadata_id, cluster_metadata_id(3));

    // If we attempt to upload while the term is different from expected, we
    // should see an error.
    err = uploader
            .upload_next_metadata(
              raft0->confirmed_term() - model::term_id(1), manifest, retry_node)
            .get();
    BOOST_REQUIRE_EQUAL(err, error_outcome::term_has_changed);
    BOOST_REQUIRE_EQUAL(manifest.metadata_id, cluster_metadata_id(4));
}

// Test that the upload fiber uploads monotonically increasing metadata, and
// that the fiber stop when leadership changes.
FIXTURE_TEST(test_upload_in_term, cluster_metadata_uploader_fixture) {
    config::shard_local_cfg()
      .cloud_storage_cluster_metadata_upload_interval_ms.set_value(1000ms);
    cluster::cloud_metadata::uploader uploader(
      cluster_uuid, bucket, remote, raft0);
    cluster::cloud_metadata::cluster_metadata_id highest_meta_id{0};

    // Checks that metadata is uploaded a new term, stepping down in between
    // calls, and ensuring that subsequent calls yield manifests with higher
    // metadata IDs and the expected snapshot offset.
    const auto check_uploads_in_term_and_stepdown = [&]() {
        // Wait to become leader before uploading.
        tests::cooperative_spin_wait_with_timeout(5s, [this] {
            return raft0->is_leader();
        }).get();

        // Start uploading in this term.
        auto upload_in_term = uploader.upload_until_term_change();
        auto defer = ss::defer([&] {
            uploader.stop_and_wait().get();
            upload_in_term.get();
        });

        // Keep checking the latest manifest for whether the metadata ID is
        // some non-zero value (indicating we've uploaded multiple manifests);
        auto initial_meta_id = highest_meta_id;
        cluster::cloud_metadata::cluster_metadata_manifest manifest;
        tests::cooperative_spin_wait_with_timeout(
          10s,
          [&]() -> ss::future<bool> {
              return downloaded_manifest_has_higher_id(
                initial_meta_id, &manifest);
          })
          .get();
        BOOST_REQUIRE_GT(manifest.metadata_id, highest_meta_id);
        highest_meta_id = manifest.metadata_id;

        // Stop the upload loop and continue in a new term.
        raft0->step_down("forced stepdown").get();
        upload_in_term.get();
        defer.cancel();
    };
    for (int i = 0; i < 3; ++i) {
        check_uploads_in_term_and_stepdown();
    }
}
