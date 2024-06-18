/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cloud_storage/inventory/inv_ops.h"
#include "cloud_storage/inventory/tests/common.h"
#include "cloud_storage/inventory/types.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/types.h"
#include "cluster/inventory_service.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>

#include <absl/container/node_hash_set.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

namespace cst = cloud_storage;
namespace csi = cst::inventory;
namespace t = testing;

using namespace std::chrono_literals;

namespace {
struct mock_leaders_provider final : cluster::leaders_provider {
    absl::node_hash_set<model::ntp> ntp_set;

    explicit mock_leaders_provider(absl::node_hash_set<model::ntp> n)
      : ntp_set{n} {}

    ss::future<absl::node_hash_set<model::ntp>>
    ntps(ss::abort_source&) override {
        co_return absl::node_hash_set<model::ntp>{};
    };
};

struct mock_remote_provider final : cluster::remote_provider {
    csi::MockRemote& remote;
    explicit mock_remote_provider(csi::MockRemote& r)
      : remote{r} {}

    cst::cloud_storage_api& ref() override { return remote; }
};

const cloud_storage_clients::bucket_name bucket{"test-bucket"};
const csi::inventory_config_id inv_id{"test-id"};
const ss::sstring prefix{"test-prefix"};

void validate_check_inv_exists_req(cst::download_request r) {
    const auto& details = r.transfer_details;
    EXPECT_TRUE(r.payload.empty());
    EXPECT_EQ(details.bucket, bucket);
    EXPECT_EQ(
      details.key,
      cloud_storage_clients::object_key{
        fmt::format("?inventory&id={}", inv_id())});
}

void validate_create_inv_req(cst::upload_request r) {
    const auto& details = r.transfer_details;
    EXPECT_FALSE(r.payload.empty());
    EXPECT_EQ(details.bucket, bucket);
    EXPECT_EQ(
      details.key,
      cloud_storage_clients::object_key{
        fmt::format("?inventory&id={}", inv_id())});
}

ss::future<cst::download_result>
validate_check_inv_exists_success(cst::download_request r) {
    validate_check_inv_exists_req(std::move(r));
    co_return cst::download_result::success;
}

ss::future<cst::download_result>
validate_check_inv_exists_not_found(cst::download_request r) {
    validate_check_inv_exists_req(std::move(r));
    co_return cst::download_result::notfound;
}

ss::future<cst::upload_result>
validate_create_inv_req_success(cst::upload_request r) {
    validate_create_inv_req(std::move(r));
    co_return cst::upload_result::success;
}

ss::future<cst::upload_result>
validate_create_inv_req_failure(cst::upload_request r) {
    validate_create_inv_req(std::move(r));
    co_return cst::upload_result::failed;
}

ss::future<cst::cloud_storage_api::list_result> validate_list_objs(
  const cloud_storage_clients::bucket_name& b,
  retry_chain_node&,
  std::optional<cloud_storage_clients::object_key> pre,
  std::optional<char> delimiter,
  std::optional<cloud_storage_clients::client::item_filter> item_filter,
  std::optional<size_t> max_keys,
  std::optional<ss::sstring> continuation_token) {
    EXPECT_EQ(bucket, bucket);

    EXPECT_TRUE(pre.has_value());
    EXPECT_EQ(
      pre.value()(), fmt::format("{}/{}/{}/", prefix, bucket(), inv_id()));

    EXPECT_TRUE(delimiter.has_value());
    EXPECT_EQ(delimiter.value(), '/');

    EXPECT_FALSE(item_filter.has_value());
    EXPECT_FALSE(max_keys.has_value());
    EXPECT_FALSE(continuation_token.has_value());

    co_return cloud_storage_clients::client::list_bucket_result{
      .common_prefixes = {"9999-99-99T99-99Z/"}};
}

} // namespace

TEST(Service, ConfigCreationSkippedWhenExists) {
    csi::MockRemote remote;
    cluster::inventory_service svc{
      std::filesystem::path{},
      std::make_shared<mock_leaders_provider>(
        absl::node_hash_set<model::ntp>{}),
      std::make_shared<mock_remote_provider>(remote),
      csi::make_inv_ops(bucket, inv_id, prefix).get()};
    EXPECT_CALL(remote, download_object(t::_))
      .Times(1)
      .WillOnce(validate_check_inv_exists_success);
    svc.start().get();
    svc.stop().get();
}

TEST(Service, ConfigCreatedWhenMissing) {
    csi::MockRemote remote;
    cluster::inventory_service svc{
      std::filesystem::path{},
      std::make_shared<mock_leaders_provider>(
        absl::node_hash_set<model::ntp>{}),
      std::make_shared<mock_remote_provider>(remote),
      csi::make_inv_ops(bucket, inv_id, prefix).get()};
    EXPECT_CALL(remote, download_object(t::_))
      .Times(1)
      .WillOnce(validate_check_inv_exists_not_found);
    EXPECT_CALL(remote, upload_object(t::_))
      .Times(1)
      .WillOnce(validate_create_inv_req_success);
    svc.start().get();
    svc.stop().get();
}

TEST(Service, ConfigCreateRetriedOnFailure) {
    csi::MockRemote remote;
    cluster::inventory_service svc{
      std::filesystem::path{},
      std::make_shared<mock_leaders_provider>(
        absl::node_hash_set<model::ntp>{}),
      std::make_shared<mock_remote_provider>(remote),
      csi::make_inv_ops(bucket, inv_id, prefix).get(),
      100ms};
    EXPECT_CALL(remote, download_object(t::_))
      .Times(t::AtLeast(2))
      .WillRepeatedly(validate_check_inv_exists_not_found);
    EXPECT_CALL(remote, upload_object(t::_))
      .Times(t::AtLeast(2))
      .WillRepeatedly(validate_create_inv_req_failure);
    svc.start().get();
    ss::sleep(500ms).get();
    svc.stop().get();
}

TEST(Service, ReportMetadataIsFetched) {
    csi::MockRemote remote;
    cluster::inventory_service svc{
      std::filesystem::path{},
      std::make_shared<mock_leaders_provider>(
        absl::node_hash_set<model::ntp>{}),
      std::make_shared<mock_remote_provider>(remote),
      csi::make_inv_ops(bucket, inv_id, prefix).get(),
      100ms};

    EXPECT_CALL(remote, download_object(t::_))
      .Times(1)
      .WillRepeatedly(validate_check_inv_exists_success);

    EXPECT_CALL(
      remote, list_objects(bucket, t::_, t::_, t::_, t::_, t::_, t::_))
      .Times(t::AtLeast(1))
      .WillRepeatedly(validate_list_objs);

    auto request_key_pred = [](const auto& tuple) {
        const cst::download_request& req = std::get<0>(tuple);
        const auto key = req.transfer_details.key();
        return key
               == "test-prefix/test-bucket/test-id/9999-99-99T99-99Z/"
                  "manifest.json";
    };

    auto f = []() -> ss::future<cst::download_result> {
        co_return cst::download_result::success;
    };

    EXPECT_CALL(remote, download_object(t::_))
      .With(t::AllArgs(t::Truly(request_key_pred)))
      .WillRepeatedly(t::InvokeWithoutArgs(f));

    EXPECT_CALL(
      remote,
      object_exists(
        t::Eq(bucket),
        t::Eq("test-prefix/test-bucket/test-id/9999-99-99T99-99Z/"
              "manifest.checksum"),
        t::_,
        cloud_storage::existence_check_type::object))
      .Times(t::AtLeast(1))
      .WillRepeatedly(t::InvokeWithoutArgs(f));
    svc.start().get();
    ss::sleep(500ms).get();
    svc.stop().get();
}
