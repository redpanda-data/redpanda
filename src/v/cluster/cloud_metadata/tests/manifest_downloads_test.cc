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
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "cluster/cloud_metadata/manifest_downloads.h"
#include "cluster/cloud_metadata/tests/manual_mixin.h"
#include "model/fundamental.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/io_priority_class.hh>

namespace {
static ss::abort_source never_abort;
} // anonymous namespace

using namespace cluster::cloud_metadata;

class cluster_metadata_fixture
  : public s3_imposter_fixture
  , public manual_metadata_upload_mixin
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    cluster_metadata_fixture()
      : redpanda_thread_fixture(
          redpanda_thread_fixture::init_cloud_storage_tag{},
          httpd_port_number())
      , remote(app.cloud_storage_api.local())
      , bucket(cloud_storage_clients::bucket_name("test-bucket")) {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
        tests::cooperative_spin_wait_with_timeout(5s, [this] {
            return app.storage.local().get_cluster_uuid().has_value();
        }).get();
        cluster_uuid = app.storage.local().get_cluster_uuid().value();
    }

    cloud_storage::remote& remote;
    const cloud_storage_clients::bucket_name bucket;
    model::cluster_uuid cluster_uuid;
};

// Basic check for downloading the latest manifest. The manifest downloaded by
// the uploader should be the one with the highest metadata ID.
FIXTURE_TEST(test_download_manifest, cluster_metadata_fixture) {
    // First try download when there's nothing in the bucket, e.g. as if it's
    // the first time we're using the bucket for this cluster.
    retry_chain_node retry_node(
      never_abort, ss::lowres_clock::time_point::max(), 10ms);
    auto m_res = download_highest_manifest_for_cluster(
                   remote, cluster_uuid, bucket, retry_node)
                   .get();
    BOOST_REQUIRE(m_res.has_error());
    BOOST_REQUIRE_EQUAL(m_res.error(), error_outcome::no_matching_metadata);

    // Every manifest upload thereafter should lead a subsequent sync to
    // download the latest metadata, even if the manifests are left around.
    cluster_metadata_manifest manifest;
    manifest.cluster_uuid = cluster_uuid;
    for (int i = 0; i < 20; i += 2) {
        manifest.metadata_id = cluster_metadata_id(i);
        // As a sanity check, upload directly rather than using uploader APIs.
        remote
          .upload_manifest(
            cloud_storage_clients::bucket_name("test-bucket"),
            manifest,
            manifest.get_manifest_path(),
            retry_node)
          .get();

        // Downloading the manifest should always yield the manifest with the
        // highest metadata ID.
        auto m_res = download_highest_manifest_for_cluster(
                       remote, cluster_uuid, bucket, retry_node)
                       .get();
        BOOST_REQUIRE(m_res.has_value());
        BOOST_CHECK_EQUAL(i, m_res.value().metadata_id());
    }

    // Now set the deadline to something very low, and check that it fails.
    retry_chain_node timeout_retry_node(
      never_abort, ss::lowres_clock::time_point::min(), 10ms);
    m_res = download_highest_manifest_for_cluster(
              remote, cluster_uuid, bucket, timeout_retry_node)
              .get();
    BOOST_CHECK(m_res.has_error());
    BOOST_CHECK_EQUAL(error_outcome::list_failed, m_res.error());
}

FIXTURE_TEST(
  test_download_highest_manifest_in_bucket, cluster_metadata_fixture) {
    retry_chain_node retry_node(
      never_abort, ss::lowres_clock::time_point::max(), 10ms);
    auto m_res
      = download_highest_manifest_in_bucket(remote, bucket, retry_node).get();
    BOOST_REQUIRE(m_res.has_error());
    BOOST_REQUIRE_EQUAL(m_res.error(), error_outcome::no_matching_metadata);

    cluster_metadata_manifest manifest;
    manifest.cluster_uuid = cluster_uuid;
    manifest.metadata_id = cluster_metadata_id(10);
    remote
      .upload_manifest(
        cloud_storage_clients::bucket_name("test-bucket"),
        manifest,
        manifest.get_manifest_path(),
        retry_node)
      .get();

    m_res
      = download_highest_manifest_in_bucket(remote, bucket, retry_node).get();
    BOOST_REQUIRE(m_res.has_value());
    BOOST_CHECK_EQUAL(cluster_uuid, m_res.value().cluster_uuid);
    BOOST_CHECK_EQUAL(10, m_res.value().metadata_id());

    auto new_uuid = model::cluster_uuid(uuid_t::create());
    manifest.cluster_uuid = new_uuid;
    manifest.metadata_id = cluster_metadata_id(15);

    // Upload a new manifest with a higher metadata ID for a new cluster.
    remote
      .upload_manifest(
        cloud_storage_clients::bucket_name("test-bucket"),
        manifest,
        manifest.get_manifest_path(),
        retry_node)
      .get();
    m_res
      = download_highest_manifest_in_bucket(remote, bucket, retry_node).get();
    BOOST_REQUIRE(m_res.has_value());
    BOOST_REQUIRE_EQUAL(15, m_res.value().metadata_id());
    BOOST_REQUIRE_EQUAL(new_uuid, m_res.value().cluster_uuid);

    // Sanity check that searching by the cluster UUIDs return the expected
    // manifests.
    m_res = download_highest_manifest_for_cluster(
              remote, cluster_uuid, bucket, retry_node)
              .get();
    BOOST_REQUIRE(m_res.has_value());
    BOOST_REQUIRE_EQUAL(10, m_res.value().metadata_id());
    BOOST_REQUIRE_EQUAL(cluster_uuid, m_res.value().cluster_uuid);

    m_res = download_highest_manifest_for_cluster(
              remote, new_uuid, bucket, retry_node)
              .get();
    BOOST_REQUIRE(m_res.has_value());
    BOOST_REQUIRE_EQUAL(15, m_res.value().metadata_id());
    BOOST_REQUIRE_EQUAL(new_uuid, m_res.value().cluster_uuid);
}
