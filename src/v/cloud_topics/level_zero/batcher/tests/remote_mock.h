/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "test_utils/test.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/noncopyable_function.hh>

#include <gmock/gmock.h>

#include <chrono>
#include <stdexcept>

using namespace std::chrono_literals;

class remote_mock final : public cloud_io::remote_api<ss::manual_clock> {
public:
    using reset_input_stream
      = cloud_io::remote_api<ss::manual_clock>::reset_input_stream;

    MOCK_METHOD(
      ss::future<cloud_io::download_result>,
      download_object,
      (cloud_io::basic_download_request<ss::manual_clock>),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::download_result>,
      object_exists,
      (const cloud_storage_clients::bucket_name&,
       const cloud_storage_clients::object_key&,
       basic_retry_chain_node<ss::manual_clock>&,
       std::string_view),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::upload_result>,
      upload_object,
      (cloud_io::basic_upload_request<ss::manual_clock>),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::upload_result>,
      upload_stream,
      (cloud_io::basic_transfer_details<ss::manual_clock>,
       uint64_t,
       const reset_input_stream&,
       lazy_abort_source&,
       const std::string_view,
       std::optional<size_t>),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::download_result>,
      download_stream,
      (cloud_io::basic_transfer_details<ss::manual_clock>,
       const cloud_io::try_consume_stream&,
       const std::string_view,
       bool,
       std::optional<cloud_storage_clients::http_byte_range>,
       std::function<void(size_t)>),
      (override));

    void expect_upload_object(
      cloud_io::upload_result res = cloud_io::upload_result::success) {
        EXPECT_CALL(*this, upload_object(::testing::_))
          .Times(1)
          .WillOnce(
            ::testing::Return(
              ss::make_ready_future<cloud_io::upload_result>(res)));
    }

    static std::deque<ss::sstring>
    convert_bytes_to_string(const chunked_vector<bytes>& expected) {
        std::deque<ss::sstring> result;
        for (const auto& e : expected) {
            ss::sstring s((const char*)e.data(), e.size()); // NOLINT
            result.emplace_back(std::move(s));
        }
        return result;
    }

    void expect_upload_object(
      const chunked_vector<bytes>& expected,
      cloud_io::upload_result upl_res = cloud_io::upload_result::success) {
        ON_CALL(*this, upload_object)
          .WillByDefault(
            [this, expected = convert_bytes_to_string(expected), upl_res](
              const cloud_io::basic_upload_request<ss::manual_clock>&
                req) mutable {
                auto p = iobuf_to_bytes(req.payload);
                if (!disable_request_collection) {
                    keys.push_back(req.transfer_details.key);
                    payloads.push_back(p);
                }
                ss::sstring haystack((const char*)p.data(), p.size()); // NOLINT
                // payload p should contain one expected data element
                for (auto& e : expected) {
                    if (e.empty()) {
                        continue;
                    }
                    auto res = haystack.find(e);
                    if (res != ss::sstring::npos) {
                        // Set size to zero to avoid comparing
                        e.resize(0);
                        return ss::make_ready_future<cloud_io::upload_result>(
                          upl_res);
                    }
                }
                GTEST_MESSAGE_(
                  "Unexpected payload",
                  ::testing::TestPartResult::kFatalFailure);
                __builtin_unreachable();
            });
    }

    // Disable collection of all requests data (keys and payloads)
    // for large tests.
    bool disable_request_collection{false};
    std::vector<cloud_storage_clients::object_key> keys;
    std::vector<bytes> payloads;
};

class cache_mock : public cloud_io::basic_cache_service_api<ss::manual_clock> {
public:
    MOCK_METHOD(
      ss::future<std::optional<cloud_io::cache_item_stream>>,
      get_stream,
      (std::filesystem::path key,
       size_t read_buffer_size,
       unsigned int read_ahead),
      ());

    MOCK_METHOD(
      ss::future<>,
      put,
      (std::filesystem::path key,
       ss::input_stream<char>& data,
       cloud_io::basic_space_reservation_guard<ss::manual_clock>& reservation,
       size_t write_buffer_size,
       unsigned int write_behind),
      ());

    MOCK_METHOD(
      ss::future<cloud_io::cache_element_status>,
      is_cached,
      (const std::filesystem::path&),
      ());

    MOCK_METHOD(
      ss::future<cloud_io::basic_space_reservation_guard<ss::manual_clock>>,
      reserve_space,
      (uint64_t, size_t),
      ());

    MOCK_METHOD(
      void, reserve_space_release, (uint64_t, size_t, uint64_t, size_t), ());

    void expect_put(std::exception_ptr e = nullptr) {
        ss::future<> result = ss::now();
        if (e != nullptr) {
            result = ss::make_exception_future<>(e);
        }
        EXPECT_CALL(
          *this,
          put(
            ::testing::_,
            ::testing::_,
            ::testing::_,
            ::testing::_,
            ::testing::_))
          .Times(1)
          .WillOnce(::testing::Return(std::move(result)));
    }

    void expect_reserve_space() {
        cloud_io::basic_space_reservation_guard<ss::manual_clock> guard(
          *this, 1, 1);
        auto result = ss::make_ready_future<
          cloud_io::basic_space_reservation_guard<ss::manual_clock>>(
          std::move(guard));
        EXPECT_CALL(*this, reserve_space(::testing::_, 1))
          .Times(1)
          .WillOnce(::testing::Return(std::move(result)));
    }
};
