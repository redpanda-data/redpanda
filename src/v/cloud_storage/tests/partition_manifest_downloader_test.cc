// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iostream.h"
#include "bytes/streambuf.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/partition_manifest_downloader.h"
#include "cloud_storage/partition_path_utils.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage_clients/client_pool.h"
#include "model/fundamental.h"

#include <seastar/core/lowres_clock.hh>

#include <gtest/gtest.h>

using namespace cloud_storage;
using namespace std::chrono_literals;

namespace {

ss::abort_source never_abort{};

constexpr model::cloud_credentials_source config_file{
  model::cloud_credentials_source::config_file};

const ss::sstring test_uuid_str = "deadbeef-0000-0000-0000-000000000000";
const model::cluster_uuid test_uuid{uuid_t::from_string(test_uuid_str)};
const remote_label test_label{test_uuid};
const std::optional<model::topic_namespace> test_tp_ns_override = std::nullopt;
const model::ntp test_ntp{
  model::ns{"test-ns"}, model::topic{"test-topic"}, model::partition_id{42}};
const model::initial_revision_id test_rev{0};

constexpr std::string_view empty_manifest_json = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 0,
    "insync_offset": 0,
    "last_offset": 0
})json";

partition_manifest dummy_partition_manifest() {
    auto json_stream = make_iobuf_input_stream(
      iobuf::from(empty_manifest_json));
    partition_manifest pm;
    pm.update(manifest_format::json, std::move(json_stream)).get();
    return pm;
}
} // namespace

class PartitionManifestDownloaderTest
  : public ::testing::Test
  , public s3_imposter_fixture {
public:
    void SetUp() override {
        pool_.start(10, ss::sharded_parameter([this] { return conf; })).get();
        io_
          .start(
            std::ref(pool_),
            ss::sharded_parameter([this] { return conf; }),
            ss::sharded_parameter([] { return config_file; }))
          .get();
        remote_
          .start(std::ref(io_), ss::sharded_parameter([this] { return conf; }))
          .get();
        // Tests will use the remote API, no hard coded responses.
        set_expectations_and_listen({});
    }

    void TearDown() override {
        pool_.local().shutdown_connections();
        io_.local().request_stop();
        remote_.stop().get();
        io_.stop().get();
        pool_.stop().get();
    }

    void upload_labeled_bin_manifest(const partition_manifest& pm) {
        retry_chain_node retry(never_abort, 1s, 10ms);
        auto labeled_path = labeled_partition_manifest_path(
          test_label, test_ntp, test_rev);
        auto upload_res
          = remote_.local()
              .upload_manifest(
                bucket_name, pm, remote_manifest_path{labeled_path}, retry)
              .get();
        ASSERT_EQ(upload_result::success, upload_res);
    }

    void upload_prefixed_bin_manifest(const partition_manifest& pm) {
        retry_chain_node retry(never_abort, 1s, 10ms);
        auto hash_path = prefixed_partition_manifest_bin_path(
          test_ntp, test_rev);
        auto upload_res
          = remote_.local()
              .upload_manifest(
                bucket_name, pm, remote_manifest_path{hash_path}, retry)
              .get();
        ASSERT_EQ(upload_result::success, upload_res);
    }

    void upload_prefixed_json_manifest(const partition_manifest& pm) {
        retry_chain_node retry(never_abort, 1s, 10ms);
        iobuf buf;
        iobuf_ostreambuf obuf(buf);
        std::ostream os(&obuf);
        pm.serialize_json(os);
        upload_request json_req{
            .transfer_details = {
                .bucket = bucket_name,
                  .key = cloud_storage_clients::object_key{prefixed_partition_manifest_json_path(test_ntp, test_rev)},
                  .parent_rtc = retry,
            },
            .type = cloud_storage::upload_type::manifest,
            .payload = std::move(buf),
        };
        auto upload_res
          = remote_.local().upload_object(std::move(json_req)).get();
        ASSERT_EQ(upload_result::success, upload_res);
    }

protected:
    ss::sharded<cloud_storage_clients::client_pool> pool_;
    ss::sharded<cloud_io::remote> io_;
    ss::sharded<remote> remote_;
};

TEST_F(PartitionManifestDownloaderTest, TestDownloadLabeledManifest) {
    auto pm = dummy_partition_manifest();
    ASSERT_NO_FATAL_FAILURE(upload_labeled_bin_manifest(pm));
    retry_chain_node retry(never_abort, 1s, 10ms);
    {
        remote_path_provider path_provider(test_label, test_tp_ns_override);
        partition_manifest_downloader dl(
          bucket_name, path_provider, test_ntp, test_rev, remote_.local());
        partition_manifest dl_pm;
        auto dl_res = dl.download_manifest(retry, &dl_pm).get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_EQ(dl_res.value(), find_partition_manifest_outcome::success);
        ASSERT_EQ(pm, dl_pm);
    }
    {
        // The downloader can only look for what has been allowed by the path
        // provider, i.e. only those without any label.
        remote_path_provider path_provider(std::nullopt, std::nullopt);
        partition_manifest_downloader dl(
          bucket_name, path_provider, test_ntp, test_rev, remote_.local());
        partition_manifest dl_pm;
        auto dl_res = dl.download_manifest(retry, &dl_pm).get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_EQ(
          dl_res.value(),
          find_partition_manifest_outcome::no_matching_manifest);
    }
}

TEST_F(PartitionManifestDownloaderTest, TestDownloadPrefixedManifest) {
    auto pm = dummy_partition_manifest();
    ASSERT_NO_FATAL_FAILURE(upload_prefixed_bin_manifest(pm));
    retry_chain_node retry(never_abort, 1s, 10ms);
    {
        remote_path_provider path_provider(std::nullopt, std::nullopt);
        partition_manifest_downloader dl(
          bucket_name, path_provider, test_ntp, test_rev, remote_.local());
        partition_manifest dl_pm;
        auto dl_res = dl.download_manifest(retry, &dl_pm).get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_EQ(dl_res.value(), find_partition_manifest_outcome::success);
        ASSERT_EQ(pm, dl_pm);
    }
    {
        // The downloader can only look for what has been allowed by the path
        // provider, i.e. only those with the supplied remote label.
        remote_path_provider path_provider(test_label, test_tp_ns_override);
        partition_manifest_downloader dl(
          bucket_name, path_provider, test_ntp, test_rev, remote_.local());
        partition_manifest dl_pm;
        auto dl_res = dl.download_manifest(retry, &dl_pm).get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_EQ(
          dl_res.value(),
          find_partition_manifest_outcome::no_matching_manifest);
    }
}

TEST_F(PartitionManifestDownloaderTest, TestDownloadJsonManifest) {
    auto pm = dummy_partition_manifest();
    ASSERT_NO_FATAL_FAILURE(upload_prefixed_json_manifest(pm));
    retry_chain_node retry(never_abort, 1s, 10ms);

    remote_path_provider path_provider(std::nullopt, std::nullopt);
    partition_manifest_downloader dl(
      bucket_name, path_provider, test_ntp, test_rev, remote_.local());
    partition_manifest dl_pm;
    auto dl_res = dl.download_manifest(retry, &dl_pm).get();
    ASSERT_FALSE(dl_res.has_error());
    ASSERT_EQ(dl_res.value(), find_partition_manifest_outcome::success);
    ASSERT_EQ(pm, dl_pm);

    ASSERT_FALSE(get_requests().empty());
    const auto& last_req = get_requests().back();
    EXPECT_STREQ(last_req.method.c_str(), "GET");
    EXPECT_STREQ(
      last_req.url.c_str(),
      "/20000000/meta/test-ns/test-topic/42_0/manifest.json");

    // Once there is both a binary and a JSON manifest, the binary one
    // should be preferred.
    auto bin_path = prefixed_partition_manifest_bin_path(test_ntp, test_rev);
    auto bin_res = remote_.local()
                     .upload_manifest(
                       bucket_name, pm, remote_manifest_path{bin_path}, retry)
                     .get();
    ASSERT_EQ(bin_res, upload_result::success);

    dl_res = dl.download_manifest(retry, &dl_pm).get();
    ASSERT_FALSE(dl_res.has_error());
    ASSERT_EQ(dl_res.value(), find_partition_manifest_outcome::success);
    ASSERT_EQ(pm, dl_pm);

    ASSERT_FALSE(get_requests().empty());
    const auto& new_last_req = get_requests().back();
    EXPECT_STREQ(new_last_req.method.c_str(), "GET");
    EXPECT_STREQ(
      new_last_req.url.c_str(),
      "/20000000/meta/test-ns/test-topic/42_0/manifest.bin");
}

TEST_F(PartitionManifestDownloaderTest, TestLabeledManifestExists) {
    remote_path_provider path_provider(test_label, test_tp_ns_override);
    partition_manifest_downloader dl(
      bucket_name, path_provider, test_ntp, test_rev, remote_.local());

    // Upload both a binary and JSON manifest with the hash prefix scheme.
    auto pm = dummy_partition_manifest();
    ASSERT_NO_FATAL_FAILURE(upload_prefixed_bin_manifest(pm));
    ASSERT_NO_FATAL_FAILURE(upload_prefixed_json_manifest(pm));

    // Since the path provider is expecting a labeled manifest, it will not see
    // either hash-prefixed manifest.
    retry_chain_node retry(never_abort, 1s, 10ms);
    auto exists_res = dl.manifest_exists(retry).get();
    ASSERT_FALSE(exists_res.has_error());
    ASSERT_EQ(
      exists_res.value(),
      find_partition_manifest_outcome::no_matching_manifest);

    // Once a labeled manifest exists, we're good.
    ASSERT_NO_FATAL_FAILURE(upload_labeled_bin_manifest(pm));
    exists_res = dl.manifest_exists(retry).get();
    ASSERT_FALSE(exists_res.has_error());
    ASSERT_EQ(exists_res.value(), find_partition_manifest_outcome::success);
}

TEST_F(PartitionManifestDownloaderTest, TestPrefixedJSONManifestExists) {
    remote_path_provider path_provider(std::nullopt, std::nullopt);
    partition_manifest_downloader dl(
      bucket_name, path_provider, test_ntp, test_rev, remote_.local());
    auto pm = dummy_partition_manifest();
    ASSERT_NO_FATAL_FAILURE(upload_labeled_bin_manifest(pm));

    // With the hash-prefixed path provider, we won't find labeled manifests.
    retry_chain_node retry(never_abort, 1s, 10ms);
    auto exists_res = dl.manifest_exists(retry).get();
    ASSERT_FALSE(exists_res.has_error());
    ASSERT_EQ(
      exists_res.value(),
      find_partition_manifest_outcome::no_matching_manifest);

    // Once a JSON manifest exists, we're good.
    ASSERT_NO_FATAL_FAILURE(upload_prefixed_json_manifest(pm));
    exists_res = dl.manifest_exists(retry).get();
    ASSERT_FALSE(exists_res.has_error());
    ASSERT_EQ(exists_res.value(), find_partition_manifest_outcome::success);
}

TEST_F(PartitionManifestDownloaderTest, TestPrefixedBinaryManifestExists) {
    remote_path_provider path_provider(std::nullopt, std::nullopt);
    partition_manifest_downloader dl(
      bucket_name, path_provider, test_ntp, test_rev, remote_.local());
    auto pm = dummy_partition_manifest();
    ASSERT_NO_FATAL_FAILURE(upload_labeled_bin_manifest(pm));

    // With the hash-prefixed path provider, we won't find labeled manifests.
    retry_chain_node retry(never_abort, 1s, 10ms);
    auto exists_res = dl.manifest_exists(retry).get();
    ASSERT_FALSE(exists_res.has_error());
    ASSERT_EQ(
      exists_res.value(),
      find_partition_manifest_outcome::no_matching_manifest);

    // Once a hash-prefixed binary manifest exists, we're good.
    ASSERT_NO_FATAL_FAILURE(upload_prefixed_bin_manifest(pm));
    exists_res = dl.manifest_exists(retry).get();
    ASSERT_FALSE(exists_res.has_error());
    ASSERT_EQ(exists_res.value(), find_partition_manifest_outcome::success);
}
