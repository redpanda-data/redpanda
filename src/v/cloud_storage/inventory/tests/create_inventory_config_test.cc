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
#include "cloud_storage/remote.h"

#include <gmock/gmock.h>

namespace cst = cloud_storage;
namespace t = ::testing;

constexpr auto id = "redpanda-inv-weekly";
constexpr auto bucket = "test-bucket";
constexpr auto prefix = "inv-prefix";
constexpr auto format = cst::inventory::report_format::csv;
constexpr auto frequency = cst::inventory::report_generation_frequency::daily;
const auto expected_key = fmt::format("?inventory&id={}", id);
const auto expected_xml_payload = fmt::format(
  R"({header}
<InventoryConfiguration {ns}><Destination><S3BucketDestination><Format>{format}</Format><Prefix>{prefix}</Prefix><Bucket>arn::aws::s3:::{bucket}</Bucket></S3BucketDestination></Destination><IsEnabled>true</IsEnabled><Id>{id}</Id><Schedule><Frequency>{schedule}</Frequency></Schedule></InventoryConfiguration>)",
  fmt::arg("header", R"(<?xml version="1.0" encoding="utf-8"?>)"),
  fmt::arg("ns", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")"),
  fmt::arg("format", format),
  fmt::arg("prefix", prefix),
  fmt::arg("bucket", bucket),
  fmt::arg("schedule", frequency),
  fmt::arg("id", id));

class MockRemote : public cst::cloud_storage_api {
public:
    MOCK_METHOD(
      ss::future<cst::upload_result>,
      upload_object,
      (cst::upload_request),
      (override));
    MOCK_METHOD(
      ss::future<cst::download_result>,
      download_object,
      (cst::download_request),
      (override));
    MOCK_METHOD(
      ss::future<cloud_storage::cloud_storage_api::list_result>,
      list_objects,
      (const cloud_storage_clients::bucket_name&,
       retry_chain_node&,
       std::optional<cloud_storage_clients::object_key>,
       std::optional<char>,
       std::optional<cloud_storage_clients::client::item_filter>),
      (override));
    MOCK_METHOD(
      ss::future<cloud_storage::download_result>,
      object_exists,
      (const cloud_storage_clients::bucket_name&,
       const cloud_storage_clients::object_key&,
       retry_chain_node&,
       cloud_storage::existence_check_type),
      (override));
};

std::string iobuf_to_xml(iobuf buf) {
    iobuf_parser p{std::move(buf)};
    return p.read_string(p.bytes_left());
}

ss::future<cst::upload_result>
validate_create_request(cst::upload_request request) {
    EXPECT_EQ(request.type, cst::upload_type::inventory_configuration);
    EXPECT_EQ(request.transfer_details.bucket(), bucket);
    EXPECT_EQ(request.transfer_details.key(), expected_key);
    EXPECT_EQ(iobuf_to_xml(std::move(request.payload)), expected_xml_payload);
    return ss::make_ready_future<cst::upload_result>(
      cst::upload_result::success);
}

template<typename T, typename... Ts>
void test_create(T t, Ts... args) {
    MockRemote remote;
    EXPECT_CALL(remote, upload_object(t::_))
      .Times(1)
      .WillOnce(t::Invoke(validate_create_request));

    ss::abort_source as;
    retry_chain_node parent{as};

    const auto result
      = t.create_inventory_configuration(remote, parent, args...).get();

    ASSERT_EQ(result, cst::upload_result::success);
}

TEST(CreateInvCfg, LowLevelApi) {
    test_create(
      cst::inventory::aws_ops{
        cloud_storage_clients::bucket_name{bucket},
        cst::inventory::inventory_config_id{id},
        prefix},
      frequency,
      format);
}

TEST(CreateInvCfg, HighLevelApi) {
    test_create(cst::inventory::inv_ops{cst::inventory::aws_ops{
      cloud_storage_clients::bucket_name{bucket},
      cst::inventory::inventory_config_id{id},
      prefix}});
}

ss::future<cst::download_result>
validate_inventory_exists_request(cst::download_request request) {
    EXPECT_EQ(request.transfer_details.bucket, bucket);
    EXPECT_EQ(request.transfer_details.key, expected_key);
    co_return cst::download_result::success;
}

TEST(InvCfgExists, HighLevelApi) {
    MockRemote remote;
    EXPECT_CALL(remote, download_object(t::_))
      .Times(1)
      .WillOnce(t::Invoke(validate_inventory_exists_request));

    ss::abort_source as;
    retry_chain_node parent{as};

    cst::inventory::inv_ops ops{cst::inventory::aws_ops{
      cloud_storage_clients::bucket_name{bucket},
      cst::inventory::inventory_config_id{id},
      prefix}};

    ASSERT_TRUE(ops.inventory_configuration_exists(remote, parent).get());
}

TEST(CreateInvCfg, IfExistsDoesNotCreate) {
    MockRemote remote;
    EXPECT_CALL(remote, download_object(t::_))
      .Times(1)
      .WillOnce(t::Invoke(validate_inventory_exists_request));

    ss::abort_source as;
    retry_chain_node parent{as};

    cst::inventory::inv_ops ops{cst::inventory::aws_ops{
      cloud_storage_clients::bucket_name{bucket},
      cst::inventory::inventory_config_id{id},
      prefix}};

    ASSERT_EQ(
      ops.maybe_create_inventory_configuration(remote, parent).get(),
      cst::inventory::inventory_creation_result::already_exists);
}

TEST(CreateInvCfg, IfDoesNotExistCreates) {
    MockRemote remote;
    EXPECT_CALL(remote, download_object(t::_))
      .Times(1)
      .WillOnce(t::Return(ss::make_ready_future<cst::download_result>(
        cst::download_result::notfound)));

    EXPECT_CALL(remote, upload_object(t::_))
      .Times(1)
      .WillOnce(t::Invoke(validate_create_request));

    ss::abort_source as;
    retry_chain_node parent{as};

    cst::inventory::inv_ops ops{cst::inventory::aws_ops{
      cloud_storage_clients::bucket_name{bucket},
      cst::inventory::inventory_config_id{id},
      prefix}};

    ASSERT_EQ(
      ops.maybe_create_inventory_configuration(remote, parent).get(),
      cst::inventory::inventory_creation_result::success);
}

TEST(CreateInvCfg, CreationRace) {
    // Simulate a creation race:
    // First, the config does not exist
    // Then, we try to create and it fails
    // Finally, the config exists
    // The outcome should be `already_exists` and not `failed`
    MockRemote remote;
    EXPECT_CALL(remote, download_object(t::_))
      .Times(2)
      .WillOnce(t::Return(ss::make_ready_future<cst::download_result>(
        cst::download_result::notfound)))
      .WillOnce(t::Return(ss::make_ready_future<cst::download_result>(
        cst::download_result::success)));

    EXPECT_CALL(remote, upload_object(t::_))
      .Times(1)
      .WillOnce(t::Return(
        ss::make_ready_future<cst::upload_result>(cst::upload_result::failed)));

    ss::abort_source as;
    retry_chain_node parent{as};

    cst::inventory::inv_ops ops{cst::inventory::aws_ops{
      cloud_storage_clients::bucket_name{bucket},
      cst::inventory::inventory_config_id{id},
      prefix}};

    ASSERT_EQ(
      ops.maybe_create_inventory_configuration(remote, parent).get(),
      cst::inventory::inventory_creation_result::already_exists);
}

TEST(CreateInvCfg, FailedToCreate) {
    MockRemote remote;
    EXPECT_CALL(remote, download_object(t::_))
      .Times(2)
      .WillOnce(t::Return(ss::make_ready_future<cst::download_result>(
        cst::download_result::notfound)))
      .WillOnce(t::Return(ss::make_ready_future<cst::download_result>(
        cst::download_result::notfound)));

    EXPECT_CALL(remote, upload_object(t::_))
      .Times(1)
      .WillOnce(t::Return(
        ss::make_ready_future<cst::upload_result>(cst::upload_result::failed)));

    ss::abort_source as;
    retry_chain_node parent{as};

    cst::inventory::inv_ops ops{cst::inventory::aws_ops{
      cloud_storage_clients::bucket_name{bucket},
      cst::inventory::inventory_config_id{id},
      prefix}};

    ASSERT_EQ(
      ops.maybe_create_inventory_configuration(remote, parent).get(),
      cst::inventory::inventory_creation_result::failed);
}
