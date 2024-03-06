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
namespace cl = cloud_storage_clients;
namespace t = ::testing;

const auto id = cst::inventory::inventory_config_id{"redpanda-inv-weekly"};
const auto bucket = cl::bucket_name{"test-bucket"};
constexpr auto prefix = "inv-prefix";
const auto list_prefix = fmt::format("{}/{}/{}/", prefix, bucket, id);

void validate_list_objects(
  cst::inventory::MockRemote& remote,
  retry_chain_node& parent,
  cl::client::list_bucket_result result) {
    EXPECT_CALL(
      remote,
      list_objects(
        t::Eq(bucket),
        t::Ref(parent),
        t::Eq(list_prefix),
        t::Optional('/'),
        t::Eq(std::nullopt)))
      .Times(1)
      .WillOnce(
        t::Return(ss::make_ready_future<cst::cloud_storage_api::list_result>(
          std::move(result))));
}

TEST(FindLatestReport, NoReportsExist) {
    ss::abort_source as;
    retry_chain_node parent{as};

    cst::inventory::MockRemote remote;
    validate_list_objects(remote, parent, {});

    cst::inventory::aws_ops ops{bucket, id, prefix};
    const auto result = ops.latest_report_path(remote, parent).get();

    EXPECT_TRUE(result.has_error());
    EXPECT_EQ(result.error(), cl::error_outcome::key_not_found);
}

TEST(FindLatestReport, NonDatePathsIgnored) {
    ss::abort_source as;
    retry_chain_node parent{as};

    const auto dates = cl::client::list_bucket_result{
      .common_prefixes = {"1215-06/", "133701Z/"}};

    cst::inventory::MockRemote remote;
    validate_list_objects(remote, parent, dates);

    cst::inventory::aws_ops ops{bucket, id, prefix};
    const auto result = ops.latest_report_path(remote, parent).get();

    EXPECT_TRUE(result.has_error());
    EXPECT_EQ(result.error(), cl::error_outcome::key_not_found);
}

template<typename Ops, typename... T>
void run_test(
  cst::inventory::MockRemote& remote, retry_chain_node& parent, T... ts) {
    constexpr auto latest_date = "1945-05-07T23-23Z/";
    constexpr auto latest_date_which_has_report = "1757-06-23T00-02Z/";

    // We expect the following prefixes to be checked, in order from latest to
    // earliest.
    // The scanning will stop once the first manifest checksum is found
    const auto dates = cl::client::list_bucket_result{
      .common_prefixes = {
        "1215-06-15T01-02Z/",
        latest_date_which_has_report,
        latest_date,
        "1337-05-24T02-01Z/"}};

    validate_list_objects(remote, parent, dates);

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

    auto ops = Ops{ts...};
    const auto result = ops.latest_report_path(remote, parent).get();

    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(
      result.value(),
      fmt::format(
        "{}{}manifest.json", list_prefix, latest_date_which_has_report));
}

TEST(FindLatestReport, LatestReportPathReturned) {
    ss::abort_source as;
    retry_chain_node parent{as};
    cst::inventory::MockRemote remote;
    run_test<cst::inventory::aws_ops>(remote, parent, bucket, id, prefix);
}

TEST(FindLatestReport, InvOps) {
    ss::abort_source as;
    retry_chain_node parent{as};
    cst::inventory::MockRemote remote;
    run_test<cst::inventory::inv_ops>(
      remote, parent, cst::inventory::aws_ops{bucket, id, prefix});
}
