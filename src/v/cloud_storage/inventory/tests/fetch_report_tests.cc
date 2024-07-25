/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/inventory/aws_ops.h"
#include "cloud_storage/inventory/inv_ops.h"
#include "cloud_storage/inventory/tests/common.h"

namespace cst = cloud_storage;
namespace csi = cst::inventory;
namespace cl = cloud_storage_clients;
namespace t = ::testing;

const auto id = csi::inventory_config_id{"redpanda-inv-weekly"};
const auto bucket = cl::bucket_name{"test-bucket"};
constexpr auto prefix = "inv-prefix";
const auto list_prefix = fmt::format("{}/{}/{}/", prefix, bucket, id);
constexpr auto latest_date = "1945-05-07T23-23Z/";
constexpr auto latest_date_which_has_report = "1757-06-23T00-02Z/";

constexpr std::string_view manifest_json_valid = R"(
{
    "sourceBucket": "example-source-bucket",
    "destinationBucket": "arn:aws:s3:::example-inventory-destination-bucket",
    "version": "2016-11-30",
    "creationTimestamp" : "1514944800000",
    "fileFormat": "CSV",
    "fileSchema": "Bucket, Key...",
    "files": [
        {
            "key": "first-key",
            "size": 2147483647,
            "MD5checksum": "f11166069f1990abeb9c97ace9cdfabc"
        }
    ]
}
)";

void setup_and_validate_list_call(
  csi::MockRemote& remote,
  retry_chain_node& parent,
  cl::client::list_bucket_result result) {
    EXPECT_CALL(
      remote,
      list_objects(
        t::Eq(bucket),
        t::Ref(parent),
        t::Eq(list_prefix),
        t::Optional('/'),
        t::Eq(std::nullopt),
        t::Eq(std::nullopt),
        t::Eq(std::nullopt)))
      .Times(1)
      .WillOnce(
        t::Return(ss::make_ready_future<cst::cloud_storage_api::list_result>(
          std::move(result))));
}

TEST(FindLatestReport, NoReportsExist) {
    ss::abort_source as;
    retry_chain_node parent{as};

    csi::MockRemote remote;
    setup_and_validate_list_call(remote, parent, {});

    csi::aws_ops ops{bucket, id, prefix};
    const auto result = ops.fetch_latest_report_metadata(remote, parent).get();

    EXPECT_TRUE(result.has_error());
    EXPECT_EQ(result.error(), csi::error_outcome::no_reports_found);
}

TEST(FindLatestReport, NonDatePathsIgnored) {
    ss::abort_source as;
    retry_chain_node parent{as};

    const auto dates = cl::client::list_bucket_result{
      .common_prefixes = {"1215-06/", "133701Z/"}};

    csi::MockRemote remote;
    setup_and_validate_list_call(remote, parent, dates);

    csi::aws_ops ops{bucket, id, prefix};
    const auto result = ops.fetch_latest_report_metadata(remote, parent).get();

    EXPECT_TRUE(result.has_error());
    EXPECT_EQ(result.error(), csi::error_outcome::no_reports_found);
}

ss::future<cst::download_result>
validate_req_and_return_manifest(cst::download_request r) {
    EXPECT_TRUE(r.payload.empty());
    EXPECT_EQ(r.type, cloud_storage::download_type::inventory_report_manifest);
    EXPECT_EQ(r.transfer_details.bucket, bucket);
    EXPECT_EQ(
      r.transfer_details.key(),
      fmt::format(
        "{}{}/manifest.json", list_prefix, latest_date_which_has_report));

    r.payload.append(manifest_json_valid.data(), manifest_json_valid.size());
    return ss::make_ready_future<cst::download_result>(
      cloud_storage::download_result::success);
}

template<typename Ops, typename... T>
void run_test(csi::MockRemote& remote, retry_chain_node& parent, T... ts) {
    // We expect the following prefixes to be checked, in order from latest to
    // earliest.
    // The scanning will stop once the first manifest checksum is found
    auto common_prefixes = std::vector<ss::sstring>{
      "1215-06-15T01-02Z/",
      latest_date_which_has_report,
      latest_date,
      "1337-05-24T02-01Z/"};
    for (auto& p : common_prefixes) {
        p = fmt::format("{}{}", list_prefix, p);
    }
    const auto dates = cl::client::list_bucket_result{
      .common_prefixes = common_prefixes};

    setup_and_validate_list_call(remote, parent, dates);

    // The latest date does not have a manifest checksum, so it will be checked
    // and then discarded
    EXPECT_CALL(
      remote,
      object_exists(
        t::Eq(bucket),
        t::Eq(fmt::format("{}{}manifest.checksum", list_prefix, latest_date)),
        t::Ref(parent),
        t::Eq(cst::existence_check_type::object)))
      .Times(1)
      .WillOnce(t::Return(ss::make_ready_future<cst::download_result>(
        cst::download_result::notfound)));

    // The next latest date does have a checksum file, so it becomes the result
    EXPECT_CALL(
      remote,
      object_exists(
        t::Eq(bucket),
        t::Eq(fmt::format(
          "{}{}manifest.checksum", list_prefix, latest_date_which_has_report)),
        t::Ref(parent),
        t::Eq(cst::existence_check_type::object)))
      .Times(1)
      .WillOnce(t::Return(ss::make_ready_future<cst::download_result>(
        cst::download_result::success)));

    // Return a valid manifest on being called
    EXPECT_CALL(remote, download_object(t::_))
      .Times(1)
      .WillOnce(t::Invoke(validate_req_and_return_manifest));

    auto ops = Ops{ts...};
    const auto result = ops.fetch_latest_report_metadata(remote, parent).get();

    EXPECT_TRUE(result.has_value());
    const auto& report_metadata = result.value();

    EXPECT_EQ(
      report_metadata.metadata_path,
      fmt::format(
        "{}{}manifest.json", list_prefix, latest_date_which_has_report));

    EXPECT_EQ(report_metadata.report_paths.size(), 1);
    EXPECT_EQ(report_metadata.report_paths[0], "first-key");

    // Test data has a trailing slash
    EXPECT_EQ(report_metadata.datetime() + "/", latest_date_which_has_report);
}

TEST(FindLatestReport, LatestReportPathReturned) {
    ss::abort_source as;
    retry_chain_node parent{as};
    csi::MockRemote remote;
    run_test<csi::aws_ops>(remote, parent, bucket, id, prefix);
}

TEST(FindLatestReport, InvOps) {
    ss::abort_source as;
    retry_chain_node parent{as};
    csi::MockRemote remote;
    run_test<csi::inv_ops>(remote, parent, csi::aws_ops{bucket, id, prefix});
}

csi::op_result<csi::report_metadata>
test_manifest_parse(std::string_view manifest) {
    ss::abort_source as;
    retry_chain_node parent{as};
    csi::MockRemote remote;

    setup_and_validate_list_call(
      remote,
      parent,
      cl::client::list_bucket_result{
        .common_prefixes = {
          fmt::format("{}{}", list_prefix, latest_date_which_has_report)}});

    EXPECT_CALL(remote, object_exists(t::_, t::_, t::_, t::_))
      .Times(1)
      .WillOnce(t::Return(ss::make_ready_future<cst::download_result>(
        cst::download_result::success)));

    auto return_manifest = [&manifest](auto r) {
        r.payload.append(manifest.data(), manifest.size());
        return ss::make_ready_future<cst::download_result>(
          cloud_storage::download_result::success);
    };
    EXPECT_CALL(remote, download_object(t::_))
      .Times(1)
      .WillOnce(return_manifest);

    auto ops = csi::inv_ops{csi::aws_ops{bucket, id, prefix}};
    return ops.fetch_latest_report_metadata(remote, parent).get();
}

TEST(FindLatestReport, ManifestWithNoFilesNode) {
    const auto result = test_manifest_parse(
      R"({"sourceBucket": "example-source-bucket"})");
    EXPECT_TRUE(result.has_error());
    EXPECT_EQ(result.error(), csi::error_outcome::manifest_files_parse_failed);
}

TEST(FindLatestReport, ManifestParseJSONError) {
    const auto result = test_manifest_parse(
      "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
      "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
      "++++++++++++++++++++++++++++++++++++++++++++++++");
    EXPECT_TRUE(result.has_error());
    EXPECT_EQ(
      result.error(), csi::error_outcome::manifest_deserialization_failed);
}

TEST(FindLatestReport, BadKeysSkipped) {
    const auto result = test_manifest_parse(
      R"({"sourceBucket": "example-source-bucket",
    "files":[{"MD5checksum":"f11166069f1990abeb9c97ace9cdfabc"},
        {"MD5checksum":"f11166069f1990abeb9c97ace9cdfabc"},
        {"key":"kx"},
        {"MD5checksum":"f11166069f1990abeb9c97ace9cdfabc"}]})");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value().report_paths, std::vector{cl::object_key{"kx"}});
}
