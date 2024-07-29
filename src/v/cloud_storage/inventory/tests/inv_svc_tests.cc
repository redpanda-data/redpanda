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
#include "gtest/gtest.h"
#include "model/fundamental.h"
#include "test_utils/tmp_dir.h"
#include "utils/base64.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>

#include <absl/container/node_hash_set.h>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <memory>
#include <tuple>

namespace cst = cloud_storage;
namespace csi = cst::inventory;
namespace t = testing;

using namespace std::chrono_literals;

namespace {
struct mock_leaders_provider final : cluster::leaders_provider {
    absl::node_hash_set<model::ntp> ntp_set;

    explicit mock_leaders_provider(absl::node_hash_set<model::ntp> n)
      : ntp_set{std::move(n)} {}

    ss::future<absl::node_hash_set<model::ntp>>
    ntps(ss::abort_source&) override {
        co_return ntp_set;
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
constexpr auto expected_date = "9999-99-99T99-99Z";
const auto manifest_path = fmt::format(
  "{}/{}/{}/{}/manifest.json", prefix, bucket(), inv_id(), expected_date);
const auto checksum_path = fmt::format(
  "{}/{}/{}/{}/manifest.checksum", prefix, bucket(), inv_id(), expected_date);

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
      .common_prefixes = {fmt::format(
        "{}/{}/{}/{}/", prefix, bucket(), inv_id(), expected_date)}};
}

constexpr auto manifest_json = R"(
{
    "sourceBucket": "example-source-bucket",
    "destinationBucket": "arn:aws:s3:::example-inventory-destination-bucket",
    "version": "2016-11-30",
    "creationTimestamp" : "1514944800000",
    "fileFormat": "CSV",
    "files": [
        {
            "key": "test-id/test-bucket/2016-11-06T21-32Z/files/test.csv.gz",
            "size": 2147483647,
            "MD5checksum": "f11166069f1990abeb9c97ace9cdfabc"
        }
    ]
}
)";

ss::future<cst::download_result>
respond_with_json(ss::sstring json_data, cst::download_request r) {
    r.payload.append(json_data.data(), json_data.size());
    co_return cst::download_result::success;
}

ss::input_stream<char> make_input_stream() {
    return csi::make_report_stream(base64_to_string(csi::compressed));
}

ss::future<cst::download_result> validate_download_inventory_file_request(
  const cloud_storage_clients::bucket_name& b,
  const cst::remote_segment_path& path,
  const csi::MockRemote::try_consume_stream& cons_str,
  retry_chain_node& parent,
  const std::string_view stream_label,
  const csi::MockRemote::download_metrics& metrics,
  std::optional<cloud_storage_clients::http_byte_range> byte_range) {
    EXPECT_EQ(b, bucket);
    EXPECT_EQ(
      path(), "test-id/test-bucket/2016-11-06T21-32Z/files/test.csv.gz");
    EXPECT_EQ(stream_label, "inventory-report");
    EXPECT_FALSE(byte_range.has_value());
    co_await cons_str(1, make_input_stream());
    co_return cst::download_result::success;
}

bool is_manifest_download_call(const std::tuple<cst::download_request>& t) {
    const auto& req = std::get<0>(t);
    const auto key = req.transfer_details.key();
    return key == manifest_path;
}

void expect_manifest_download(csi::MockRemote& remote) {
    EXPECT_CALL(remote, download_object(t::_))
      .With(t::AllArgs(t::Truly(is_manifest_download_call)))
      .WillRepeatedly(std::bind_front(respond_with_json, manifest_json));
}

void expect_checksum_exists(csi::MockRemote& remote) {
    EXPECT_CALL(
      remote,
      object_exists(
        t::Eq(bucket),
        t::Eq(checksum_path),
        t::_,
        cloud_storage::existence_check_type::object))
      .Times(t::AtLeast(1))
      .WillRepeatedly(
        t::InvokeWithoutArgs([]() -> ss::future<cst::download_result> {
            co_return cst::download_result::success;
        }));
}

std::shared_ptr<cluster::leaders_provider> make_leaders(
  std::initializer_list<std::tuple<ss::sstring, ss::sstring, uint16_t>> ntps) {
    absl::node_hash_set<model::ntp> ntp_set{};
    for (auto&& [a, b, c] : ntps) {
        ntp_set.insert(model::ntp{a, b, c});
    }
    return std::make_shared<mock_leaders_provider>(
      mock_leaders_provider(std::move(ntp_set)));
}

cluster::inventory_service make_service(
  const temporary_dir& t,
  csi::MockRemote& remote,
  std::initializer_list<std::tuple<ss::sstring, ss::sstring, uint16_t>> ntps) {
    return cluster::inventory_service{
      t.get_path(),
      make_leaders(ntps),
      std::make_shared<mock_remote_provider>(remote),
      csi::make_inv_ops(bucket, inv_id, prefix).get(),
      100ms};
}

} // namespace

TEST(Service, ConfigCreationSkippedWhenExists) {
    csi::MockRemote remote;
    cluster::inventory_service svc{
      {},
      make_leaders({}),
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
      {},
      make_leaders({}),
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
    temporary_dir t{""};
    auto svc = make_service(t, remote, {});
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

TEST(Service, ReportIsFetchedAndProcessed) {
    csi::MockRemote remote;
    temporary_dir t{"inv-svc-tests"};
    auto svc = make_service(t, remote, {{"kafka", "partagas", 0}});
    EXPECT_CALL(remote, download_object(t::_))
      .Times(1)
      .WillRepeatedly(validate_check_inv_exists_success);

    EXPECT_CALL(
      remote, list_objects(bucket, t::_, t::_, t::_, t::_, t::_, t::_))
      .Times(t::AtLeast(1))
      .WillRepeatedly(validate_list_objs);

    expect_manifest_download(remote);
    expect_checksum_exists(remote);

    // Because the service will only process a report newer than whatever it has
    // processed, the inventory will only be downloaded once. From that point on
    // the report metadata will be examined and the download skipped.
    EXPECT_CALL(
      remote, download_stream(t::_, t::_, t::_, t::_, t::_, t::_, t::_))
      .Times(1)
      .WillOnce(validate_download_inventory_file_request);

    svc.start().get();
    ss::sleep(500ms).get();
    svc.stop().get();

    const auto hash_files = csi::collect_hash_files(
      t.get_path() / std::string{model::ntp{"kafka", "partagas", 0}.path()});
    EXPECT_FALSE(hash_files.empty());
    EXPECT_TRUE(svc.can_use_inventory_data());
}

TEST(Service, HashPathCleanedUpBeforeProcessing) {
    csi::MockRemote remote;
    temporary_dir t{"inv-svc-tests"};
    auto svc = make_service(t, remote, {});
    EXPECT_CALL(remote, download_object(t::_))
      .Times(1)
      .WillRepeatedly(validate_check_inv_exists_success);

    EXPECT_CALL(
      remote, list_objects(bucket, t::_, t::_, t::_, t::_, t::_, t::_))
      .Times(t::AtLeast(1))
      .WillRepeatedly(validate_list_objs);

    expect_manifest_download(remote);
    expect_checksum_exists(remote);

    for (auto l1 : {"a", "b", "c"}) {
        for (auto l2 : {"1", "2", "3"}) {
            const auto path = t.get_path() / l1 / l2;
            std::filesystem::create_directories(path);
            std::ofstream o{path / "file.txt"};
            o << "data";
            o.close();
        }
    }
    svc.start().get();
    ss::sleep(500ms).get();
    svc.stop().get();

    for (auto l1 : {"a", "b", "c"}) {
        EXPECT_FALSE(std::filesystem::exists(t.get_path() / l1));
    }
}

TEST(Service, OlderReportIsSkipped) {
    csi::MockRemote remote;
    temporary_dir t{"inv-svc-tests"};
    auto svc = make_service(t, remote, {{"kafka", "partagas", 0}});
    EXPECT_CALL(remote, download_object(t::_))
      .Times(1)
      .WillRepeatedly(validate_check_inv_exists_success);

    auto list_return_old_date =
      []() -> ss::future<csi::MockRemote::list_result> {
        co_return cloud_storage_clients::client::list_bucket_result{
          .common_prefixes = {
            fmt::format(
              "{}/{}/{}/1111-11-11T11-19Z/", prefix, bucket(), inv_id()),
            fmt::format(
              "{}/{}/{}/1111-11-11T11-22Z/", prefix, bucket(), inv_id()),
            fmt::format(
              "{}/{}/{}/1111-11-11T11-83Z/", prefix, bucket(), inv_id()),
            fmt::format(
              "{}/{}/{}/1111-00-11T11-15Z/", prefix, bucket(), inv_id())}};
    };
    EXPECT_CALL(
      remote, list_objects(bucket, t::_, t::_, t::_, t::_, t::_, t::_))
      .Times(t::AtLeast(1))
      // The first call returns the latest date
      .WillOnce(validate_list_objs)
      // All later calls return an older dates - which will not be downloaded
      .WillRepeatedly(t::InvokeWithoutArgs(list_return_old_date));

    // General case - older manifests
    EXPECT_CALL(remote, download_object(t::_))
      .With(t::AllArgs(t::Truly([](const auto& t) {
          const auto& req = std::get<0>(t);
          const auto key = req.transfer_details.key();
          return key.filename() == "manifest.json";
      })))
      .Times(t::AtLeast(1))
      .WillRepeatedly(std::bind_front(respond_with_json, manifest_json));

    // Latest date's manifest
    EXPECT_CALL(remote, download_object(t::_))
      .With(t::AllArgs(t::Truly(is_manifest_download_call)))
      .Times(1)
      .WillOnce(std::bind_front(respond_with_json, manifest_json));

    // Existence checks for older date checksums
    EXPECT_CALL(
      remote,
      object_exists(
        t::Eq(bucket),
        t::Truly([](const auto& path) {
            return path().filename() == "manifest.checksum";
        }),
        t::_,
        cloud_storage::existence_check_type::object))
      .Times(t::AtLeast(1))
      .WillRepeatedly(
        t::InvokeWithoutArgs([]() -> ss::future<cst::download_result> {
            co_return cst::download_result::success;
        }));

    // Latest date checksum existence
    EXPECT_CALL(
      remote,
      object_exists(
        t::Eq(bucket),
        t::Eq(checksum_path),
        t::_,
        cloud_storage::existence_check_type::object))
      .Times(1)
      .WillOnce(t::InvokeWithoutArgs([]() -> ss::future<cst::download_result> {
          co_return cst::download_result::success;
      }));

    // Report is downloaded only once, for the first call
    EXPECT_CALL(
      remote, download_stream(t::_, t::_, t::_, t::_, t::_, t::_, t::_))
      .Times(1)
      .WillOnce(validate_download_inventory_file_request);

    svc.start().get();
    ss::sleep(500ms).get();
    svc.stop().get();

    const auto hash_files = csi::collect_hash_files(
      t.get_path() / std::string{model::ntp{"kafka", "partagas", 0}.path()});
    EXPECT_FALSE(hash_files.empty());
    EXPECT_TRUE(svc.can_use_inventory_data());
}

TEST(Service, DataMarkedUnsafeAfterError) {
    csi::MockRemote remote;
    temporary_dir t{"inv-svc-tests"};
    auto svc = make_service(t, remote, {{"kafka", "partagas", 0}});
    EXPECT_CALL(remote, download_object(t::_))
      .Times(1)
      .WillOnce(validate_check_inv_exists_success);
    EXPECT_CALL(
      remote, list_objects(bucket, t::_, t::_, t::_, t::_, t::_, t::_))
      .Times(t::AtLeast(1))
      .WillRepeatedly(validate_list_objs);

    expect_manifest_download(remote);
    expect_checksum_exists(remote);
    EXPECT_CALL(
      remote, download_stream(t::_, t::_, t::_, t::_, t::_, t::_, t::_))
      .Times(t::AtLeast(1))
      .WillRepeatedly([]() -> ss::future<cst::download_result> {
          co_return cst::download_result::failed;
      });

    svc.start().get();
    ss::sleep(500ms).get();
    svc.stop().get();

    EXPECT_FALSE(svc.can_use_inventory_data());
}
