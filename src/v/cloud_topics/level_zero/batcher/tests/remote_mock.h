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

    /// Accept any number of upload_object calls, recording keys and payloads.
    void expect_upload_object_repeatedly(
      cloud_io::upload_result res = cloud_io::upload_result::success) {
        EXPECT_CALL(*this, upload_object(::testing::_))
          .WillRepeatedly(
            [this,
             res](const cloud_io::basic_upload_request<ss::manual_clock>& req) {
                keys.push_back(req.transfer_details.key);
                payloads.push_back(iobuf_to_bytes(req.payload));
                return ss::make_ready_future<cloud_io::upload_result>(res);
            });
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
