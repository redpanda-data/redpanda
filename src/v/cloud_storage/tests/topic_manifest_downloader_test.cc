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
#include "cloud_storage/remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/topic_manifest_downloader.h"
#include "cloud_storage/topic_path_utils.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

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
const model::topic_namespace test_tp_ns{model::ns{"kafka"}, model::topic{"tp"}};
const model::initial_revision_id test_rev{21};

ss::sstring topic_manifest_json(
  const model::topic_namespace& tp_ns, model::initial_revision_id rev) {
    return fmt::format(
      R"JSON({{
      "version": 1,
      "namespace": "{}",
      "topic": "{}",
      "partition_count": 1,
      "replication_factor": 1,
      "revision_id": {}
    }})JSON",
      tp_ns.ns(),
      tp_ns.tp(),
      rev());
}

topic_manifest dummy_topic_manifest(
  std::optional<model::topic_namespace> tp = std::nullopt,
  std::optional<model::initial_revision_id> rev = std::nullopt) {
    const auto tm_json = topic_manifest_json(
      tp.value_or(test_tp_ns), rev.value_or(test_rev));
    auto json_stream = make_iobuf_input_stream(iobuf::from(tm_json));
    topic_manifest tm;
    tm.update(manifest_format::json, std::move(json_stream)).get();
    return tm;
}
} // namespace

class TopicManifestDownloaderTest
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
        remote_.stop().get();
        io_.stop().get();
        pool_.stop().get();
    }

    void upload_labeled_bin_manifest(const topic_manifest& tm) {
        retry_chain_node retry(never_abort, 1s, 10ms);
        auto labeled_path = labeled_topic_manifest_path(
          test_label, tm.get_topic_config()->tp_ns, test_rev);
        auto upload_res
          = remote_.local()
              .upload_manifest(
                bucket_name, tm, remote_manifest_path{labeled_path}, retry)
              .get();
        ASSERT_EQ(upload_result::success, upload_res);
    }

    void upload_prefixed_bin_manifest(const topic_manifest& tm) {
        retry_chain_node retry(never_abort, 1s, 10ms);
        auto hashed_path = prefixed_topic_manifest_bin_path(
          tm.get_topic_config()->tp_ns);
        auto upload_res
          = remote_.local()
              .upload_manifest(
                bucket_name, tm, remote_manifest_path{hashed_path}, retry)
              .get();
        ASSERT_EQ(upload_result::success, upload_res);
    }

    // Serializes the given topic manifest into JSON and uploads the result to
    // the appropriate path with the hash-prefixing scheme.
    void upload_json_manifest(const topic_manifest& tm) {
        retry_chain_node retry(never_abort, 1s, 10ms);
        iobuf buf;
        iobuf_ostreambuf obuf(buf);
        std::ostream os(&obuf);
        tm.serialize_v1_json(os);
        auto hashed_path = prefixed_topic_manifest_json_path(
          tm.get_topic_config()->tp_ns);
        upload_request json_req{
            .transfer_details = {
                .bucket = bucket_name,
                  .key = cloud_storage_clients::object_key{hashed_path},
                  .parent_rtc = retry,
            },
            .type = cloud_storage::upload_type::manifest,
            .payload = std::move(buf),
        };
        auto upload_res
          = remote_.local().upload_object(std::move(json_req)).get();
        ASSERT_EQ(upload_result::success, upload_res);
    }

    result<find_topic_manifest_outcome, error_outcome> find_manifests(
      retry_chain_node& parent_retry,
      ss::lowres_clock::time_point deadline,
      model::timestamp_clock::duration backoff,
      std::optional<topic_manifest_downloader::tp_ns_filter_t> tp_filter,
      chunked_vector<topic_manifest>* manifests) {
        return topic_manifest_downloader::find_manifests(
                 remote_.local(),
                 bucket_name,
                 parent_retry,
                 deadline,
                 backoff,
                 std::move(tp_filter),
                 manifests)
          .get();
    }

protected:
    ss::sharded<cloud_storage_clients::client_pool> pool_;
    ss::sharded<cloud_io::remote> io_;
    ss::sharded<remote> remote_;
};

TEST_F(TopicManifestDownloaderTest, TestDownloadLabeledManifest) {
    auto tm = dummy_topic_manifest();
    ASSERT_NO_FATAL_FAILURE(upload_labeled_bin_manifest(tm));

    topic_manifest_downloader dl(
      bucket_name, /*hint=*/std::nullopt, test_tp_ns, remote_.local());
    topic_manifest dl_tm;
    retry_chain_node retry(never_abort, 1s, 10ms);
    auto dl_res = dl.download_manifest(
                      retry, ss::lowres_clock::now() + 1s, 10ms, &dl_tm)
                    .get();
    ASSERT_FALSE(dl_res.has_error());
    ASSERT_TRUE(tm == dl_tm);
}

TEST_F(TopicManifestDownloaderTest, TestDownloadMultipleLabeledManifests) {
    auto tm = dummy_topic_manifest();
    auto labeled_path1 = labeled_topic_manifest_path(
      test_label, test_tp_ns, test_rev);
    auto labeled_path2 = labeled_topic_manifest_path(
      test_label, test_tp_ns, model::initial_revision_id{test_rev() + 1});

    // Upload two manifests of the same topic, but with different revisions.
    retry_chain_node retry(never_abort, 1s, 10ms);
    for (const auto& path : {labeled_path1, labeled_path2}) {
        auto upload_res
          = remote_.local()
              .upload_manifest(
                bucket_name, tm, remote_manifest_path{path}, retry)
              .get();
        ASSERT_EQ(upload_result::success, upload_res);
    }

    chunked_vector<std::optional<ss::sstring>> bad_hints{
      std::nullopt,
      test_uuid_str,
    };
    for (const auto& hint : bad_hints) {
        topic_manifest_downloader dl(
          bucket_name, hint, test_tp_ns, remote_.local());
        topic_manifest dl_tm;
        auto dl_res = dl.download_manifest(
                          retry, ss::lowres_clock::now() + 1s, 10ms, &dl_tm)
                        .get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_EQ(
          dl_res.value(),
          find_topic_manifest_outcome::multiple_matching_manifests);
    }
    topic_manifest_downloader dl(
      bucket_name,
      fmt::format("{}/{}", test_uuid_str, test_rev()),
      test_tp_ns,
      remote_.local());
    topic_manifest dl_tm;
    auto dl_res = dl.download_manifest(
                      retry, ss::lowres_clock::now() + 1s, 10ms, &dl_tm)
                    .get();
    ASSERT_FALSE(dl_res.has_error());
    ASSERT_EQ(tm, dl_tm);
}

TEST_F(TopicManifestDownloaderTest, TestDownloadLabeledManifestWithHint) {
    auto tm = dummy_topic_manifest();
    ASSERT_NO_FATAL_FAILURE(upload_labeled_bin_manifest(tm));

    retry_chain_node retry(never_abort, 1s, 10ms);
    for (int i = 0; i < test_uuid_str.size(); i++) {
        auto uuid_substr = test_uuid_str.substr(0, i);
        topic_manifest_downloader dl(
          bucket_name, uuid_substr, test_tp_ns, remote_.local());
        topic_manifest dl_tm;
        auto dl_res = dl.download_manifest(
                          retry, ss::lowres_clock::now() + 1s, 10ms, &dl_tm)
                        .get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_TRUE(tm == dl_tm);
    }
    const chunked_vector<ss::sstring> good_hints = {
      fmt::format("{}/", test_uuid_str, test_rev()),
      fmt::format("{}/{}", test_uuid_str, test_rev()),
      fmt::format("{}/{}/", test_uuid_str, test_rev()),
      fmt::format("{}/{}/topic_ma", test_uuid_str, test_rev()),
      fmt::format("{}/{}/topic_manifest.bin", test_uuid_str, test_rev()),
    };
    for (const auto& hint : good_hints) {
        // Adding a trailing slash and the revision.
        topic_manifest_downloader dl(
          bucket_name, hint, test_tp_ns, remote_.local());
        topic_manifest dl_tm;
        auto dl_res = dl.download_manifest(
                          retry, ss::lowres_clock::now() + 1s, 10ms, &dl_tm)
                        .get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_TRUE(tm == dl_tm);
    }
    chunked_vector<ss::sstring> bad_hints = {
      // Bad slash.
      fmt::format("{}//", test_uuid_str),
      fmt::format("/{}", test_uuid_str),

      // Bad revision.
      fmt::format("{}/{}", test_uuid_str, test_rev() + 1),

      // Missing revision.
      fmt::format("{}/topic_manifest.bin", test_uuid_str),
    };
    for (const auto& hint : bad_hints) {
        topic_manifest_downloader dl(
          bucket_name, hint, test_tp_ns, remote_.local());
        topic_manifest dl_tm;
        auto dl_res = dl.download_manifest(
                          retry, ss::lowres_clock::now() + 1s, 10ms, &dl_tm)
                        .get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_EQ(
          dl_res.value(), find_topic_manifest_outcome::no_matching_manifest);
    }
}

TEST_F(TopicManifestDownloaderTest, TestDownloadPrefixedBinaryManifest) {
    auto tm = dummy_topic_manifest();
    ASSERT_NO_FATAL_FAILURE(upload_prefixed_bin_manifest(tm));
    chunked_vector<std::optional<ss::sstring>> hints{
      std::nullopt,

      // NOTE: since there is no labeled manifest, hints are inconsequential.
      test_uuid_str,
      "foo",
    };
    retry_chain_node retry(never_abort, 1s, 10ms);
    for (const auto& hint : hints) {
        topic_manifest_downloader dl(
          bucket_name, hint, test_tp_ns, remote_.local());
        topic_manifest dl_tm;
        auto dl_res = dl.download_manifest(
                          retry, ss::lowres_clock::now() + 1s, 10ms, &dl_tm)
                        .get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_TRUE(tm == dl_tm);
    }
}

TEST_F(TopicManifestDownloaderTest, TestDownloadPrefixedJsonManifest) {
    auto tm = dummy_topic_manifest();
    ASSERT_NO_FATAL_FAILURE(upload_json_manifest(tm));

    chunked_vector<std::optional<ss::sstring>> hints{
      std::nullopt,

      // NOTE: since there is no labeled manifest, hints are inconsequential.
      test_uuid_str,
      "foo",
    };
    retry_chain_node retry(never_abort, 1s, 10ms);
    for (const auto& hint : hints) {
        topic_manifest_downloader dl(
          bucket_name, hint, test_tp_ns, remote_.local());
        topic_manifest dl_tm;
        auto dl_res = dl.download_manifest(
                          retry, ss::lowres_clock::now() + 1s, 10ms, &dl_tm)
                        .get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_TRUE(tm == dl_tm);
    }
}

TEST_F(TopicManifestDownloaderTest, TestDownloadWithRemoteError) {
    auto tm = dummy_topic_manifest();
    ASSERT_NO_FATAL_FAILURE(upload_labeled_bin_manifest(tm));

    retry_chain_node bad_retry(
      never_abort, ss::lowres_clock::time_point::min(), 10ms);
    topic_manifest_downloader dl(
      bucket_name, /*hint=*/std::nullopt, test_tp_ns, remote_.local());
    topic_manifest dl_tm;
    auto dl_res
      = dl.download_manifest(
            bad_retry, ss::lowres_clock::time_point::min(), 10ms, &dl_tm)
          .get();
    ASSERT_TRUE(dl_res.has_error());
    ASSERT_EQ(dl_res.error(), error_outcome::manifest_download_error);
}

TEST_F(TopicManifestDownloaderTest, TestDownloadPrecedence) {
    auto tm = dummy_topic_manifest();
    ASSERT_NO_FATAL_FAILURE(upload_json_manifest(tm));
    topic_manifest_downloader dl(
      bucket_name, /*hint=*/std::nullopt, test_tp_ns, remote_.local());
    topic_manifest dl_tm;

    // When there's just a JSON manifest in the bucket, it will be downloaded.
    retry_chain_node retry(never_abort, 1s, 10ms);
    {
        auto dl_res = dl.download_manifest(
                          retry, ss::lowres_clock::now() + 1s, 10ms, &dl_tm)
                        .get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_TRUE(tm == dl_tm);
        ASSERT_FALSE(get_requests().empty());
        const auto& last_req = get_requests().back();
        EXPECT_STREQ(last_req.method.c_str(), "GET");
        EXPECT_STREQ(
          last_req.url.c_str(), "/e0000000/meta/kafka/tp/topic_manifest.json");
    }

    // If there's both a JSON manifest and a binary manifest with the hash
    // prefixing scheme, the binary manifest is returned.
    ASSERT_NO_FATAL_FAILURE(upload_prefixed_bin_manifest(tm));
    {
        auto dl_res = dl.download_manifest(
                          retry, ss::lowres_clock::now() + 1s, 10ms, &dl_tm)
                        .get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_TRUE(tm == dl_tm);
        ASSERT_FALSE(get_requests().empty());
        const auto& last_req = get_requests().back();
        EXPECT_STREQ(last_req.method.c_str(), "GET");
        EXPECT_STREQ(
          last_req.url.c_str(), "/e0000000/meta/kafka/tp/topic_manifest.bin");
    }

    // If there is a labeled manifest, it is prefered over the others.
    ASSERT_NO_FATAL_FAILURE(upload_labeled_bin_manifest(tm));
    {
        auto dl_res = dl.download_manifest(
                          retry, ss::lowres_clock::now() + 1s, 10ms, &dl_tm)
                        .get();
        ASSERT_FALSE(dl_res.has_error());
        ASSERT_TRUE(tm == dl_tm);
        ASSERT_FALSE(get_requests().empty());
        const auto& last_req = get_requests().back();
        EXPECT_STREQ(last_req.method.c_str(), "GET");
        EXPECT_STREQ(
          last_req.url.c_str(),
          "/meta/kafka/tp/deadbeef-0000-0000-0000-000000000000/21/"
          "topic_manifest.bin");
    }
}

TEST_F(TopicManifestDownloaderTest, TestFindManifests) {
    for (int i = 0; i < 30; i++) {
        auto new_tp_ns = test_tp_ns;
        new_tp_ns.tp = model::topic{fmt::format("{}-{}", new_tp_ns.tp, i)};
        auto tm = dummy_topic_manifest(new_tp_ns, test_rev);
        switch (i % 3) {
        case 0:
            ASSERT_NO_FATAL_FAILURE(upload_labeled_bin_manifest(tm));
            break;
        case 1:
            ASSERT_NO_FATAL_FAILURE(upload_prefixed_bin_manifest(tm));
            break;
        case 2:
            ASSERT_NO_FATAL_FAILURE(upload_json_manifest(tm));
            break;
        }
    }
    retry_chain_node retry(never_abort, 10s, 10ms);
    chunked_vector<topic_manifest> tms;
    auto find_res = find_manifests(
      retry, ss::lowres_clock::now() + 10s, 10ms, std::nullopt, &tms);
    ASSERT_FALSE(find_res.has_error());
    ASSERT_EQ(find_res.value(), find_topic_manifest_outcome::success);
    ASSERT_EQ(30, tms.size());
    tms.clear();

    // Now check with a filter.
    const auto zero_filter = [](const model::topic_namespace& topic) {
        return topic.tp().ends_with("0");
    };
    find_res = find_manifests(
      retry, ss::lowres_clock::now() + 10s, 10ms, zero_filter, &tms);
    ASSERT_FALSE(find_res.has_error());
    ASSERT_EQ(find_res.value(), find_topic_manifest_outcome::success);
    ASSERT_EQ(3, tms.size());
    for (const auto& tm : tms) {
        ASSERT_TRUE(zero_filter(tm.get_topic_config()->tp_ns))
          << tm.get_topic_config();
    }
    tms.clear();

    const auto bogus_filter = [](const model::topic_namespace& topic) {
        return topic.tp().ends_with("foobar");
    };
    find_res = find_manifests(
      retry, ss::lowres_clock::now() + 10s, 10ms, bogus_filter, &tms);
    ASSERT_FALSE(find_res.has_error());
    ASSERT_EQ(find_res.value(), find_topic_manifest_outcome::success);
    ASSERT_EQ(0, tms.size());
}

TEST_F(TopicManifestDownloaderTest, TestFindManifestsEmpty) {
    retry_chain_node retry(never_abort, 10s, 10ms);
    chunked_vector<topic_manifest> tms;
    auto find_res = find_manifests(
      retry, ss::lowres_clock::now() + 10s, 10ms, std::nullopt, &tms);
    ASSERT_FALSE(find_res.has_error());
    ASSERT_EQ(find_res.value(), find_topic_manifest_outcome::success);
    ASSERT_EQ(0, tms.size());
}

TEST_F(TopicManifestDownloaderTest, TestFindManifestsRemoteError) {
    retry_chain_node bad_retry(
      never_abort, ss::lowres_clock::time_point::min(), 10ms);
    chunked_vector<topic_manifest> tms;
    auto find_res = find_manifests(
      bad_retry, ss::lowres_clock::time_point::min(), 10ms, std::nullopt, &tms);
    ASSERT_TRUE(find_res.has_error());
    ASSERT_EQ(find_res.error(), error_outcome::manifest_download_error);
    ASSERT_EQ(0, tms.size());
}

TEST_F(TopicManifestDownloaderTest, TestFindManifestsPrecedence) {
    auto tm = dummy_topic_manifest();
    ASSERT_NO_FATAL_FAILURE(upload_labeled_bin_manifest(tm));
    ASSERT_NO_FATAL_FAILURE(upload_prefixed_bin_manifest(tm));
    ASSERT_NO_FATAL_FAILURE(upload_json_manifest(tm));

    retry_chain_node retry(never_abort, 10s, 10ms);
    chunked_vector<topic_manifest> tms;
    auto find_res = find_manifests(
      retry, ss::lowres_clock::now() + 10s, 10ms, std::nullopt, &tms);
    ASSERT_FALSE(find_res.has_error());
    ASSERT_EQ(find_res.value(), find_topic_manifest_outcome::success);
    ASSERT_EQ(1, tms.size());

    // Even though there are multiple manifests, we the one we actually
    // download is the labeled one.
    ASSERT_TRUE(tm == tms[0]);
    ASSERT_FALSE(get_requests().empty());
    const auto& last_req = get_requests().back();
    EXPECT_STREQ(last_req.method.c_str(), "GET");
    EXPECT_STREQ(
      last_req.url.c_str(),
      "/meta/kafka/tp/deadbeef-0000-0000-0000-000000000000/21/"
      "topic_manifest.bin");
}
