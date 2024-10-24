// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "random/generators.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/noncopyable_function.hh>

#include <gmock/gmock.h>

#include <chrono>
#include <exception>
#include <stdexcept>

using namespace std::chrono_literals;

struct remote_mock_download {
    virtual ~remote_mock_download() = default;
    virtual std::pair<iobuf, cloud_io::download_result>
    _do_download_object(cloud_storage_clients::object_key key) = 0;
};

class remote_mock final
  : public cloud_io::remote_api<ss::lowres_clock>
  , public remote_mock_download {
public:
    using reset_input_stream
      = cloud_io::remote_api<ss::lowres_clock>::reset_input_stream;

    MOCK_METHOD(
      ss::future<cloud_io::download_result>,
      object_exists,
      (const cloud_storage_clients::bucket_name&,
       const cloud_storage_clients::object_key&,
       retry_chain_node&,
       std::string_view),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::upload_result>,
      upload_object,
      (cloud_io::basic_upload_request<ss::lowres_clock>),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::upload_result>,
      upload_stream,
      (cloud_io::basic_transfer_details<ss::lowres_clock>,
       uint64_t,
       const reset_input_stream&,
       lazy_abort_source&,
       const std::string_view,
       std::optional<size_t>),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::download_result>,
      download_stream,
      (cloud_io::basic_transfer_details<ss::lowres_clock>,
       const cloud_io::try_consume_stream&,
       const std::string_view,
       bool,
       std::optional<cloud_storage_clients::http_byte_range>,
       std::function<void(size_t)>),
      (override));

    MOCK_METHOD(
      (std::pair<iobuf, cloud_io::download_result>),
      _do_download_object,
      (cloud_storage_clients::object_key key),
      (override));

    ss::future<cloud_io::download_result> download_object(
      cloud_io::basic_download_request<ss::lowres_clock> req) override {
        auto [buf, err] = _do_download_object(req.transfer_details.key);
        req.payload = std::move(buf);
        co_return err;
    }

    void expect_download_object(
      cloud_storage_clients::object_key key,
      cloud_io::download_result res,
      iobuf body) {
        EXPECT_CALL(*this, _do_download_object(std::move(key)))
          .Times(1)
          .WillOnce(::testing::Return(std::make_pair(std::move(body), res)));
    }

    template<class Exception>
    void expect_download_object_throw(
      cloud_storage_clients::object_key key, Exception err) {
        EXPECT_CALL(*this, _do_download_object(std::move(key)))
          .Times(1)
          .WillOnce(::testing::Throw(err));
    }
};

class cache_mock : public cloud_io::basic_cache_service_api<ss::lowres_clock> {
public:
    MOCK_METHOD(
      ss::future<std::optional<cloud_io::cache_item_stream>>,
      get,
      (std::filesystem::path key,
       ss::io_priority_class io_priority,
       size_t read_buffer_size,
       unsigned int read_ahead),
      ());

    MOCK_METHOD(
      ss::future<>,
      put,
      (std::filesystem::path key,
       ss::input_stream<char>& data,
       cloud_io::basic_space_reservation_guard<ss::lowres_clock>& reservation,
       ss::io_priority_class io_priority,
       size_t write_buffer_size,
       unsigned int write_behind),
      ());

    MOCK_METHOD(
      ss::future<cloud_io::cache_element_status>,
      is_cached,
      (const std::filesystem::path&),
      ());

    MOCK_METHOD(
      ss::future<cloud_io::basic_space_reservation_guard<ss::lowres_clock>>,
      reserve_space,
      (uint64_t, size_t),
      ());

    MOCK_METHOD(
      void, reserve_space_release, (uint64_t, size_t, uint64_t, size_t), ());

    void expect_is_cached(
      std::filesystem::path p, cloud_io::cache_element_status s) {
        auto fut = ss::make_ready_future<cloud_io::cache_element_status>(s);
        EXPECT_CALL(*this, is_cached(p))
          .Times(1)
          .WillOnce(::testing::Return(std::move(fut)));
    }

    void expect_is_cached(
      std::filesystem::path p,
      std::vector<cloud_io::cache_element_status> seq) {
        auto& exp = EXPECT_CALL(*this, is_cached(p)).Times(seq.size());

        for (auto s : seq) {
            auto fut = ss::make_ready_future<cloud_io::cache_element_status>(s);
            exp.WillOnce(::testing::Return(std::move(fut)));
        }
    }

    void
    expect_is_cached_throws(std::filesystem::path p, std::exception_ptr e) {
        auto fut = ss::make_exception_future<cloud_io::cache_element_status>(e);
        EXPECT_CALL(*this, is_cached(p))
          .Times(1)
          .WillOnce(::testing::Return(std::move(fut)));
    }

    void expect_get(
      std::filesystem::path p,
      std::optional<cloud_io::cache_item_stream> item) {
        auto fut
          = ss::make_ready_future<std::optional<cloud_io::cache_item_stream>>(
            std::move(item));
        EXPECT_CALL(*this, get(p, ::testing::_, ::testing::_, ::testing::_))
          .Times(1)
          .WillOnce(::testing::Return(std::move(fut)));
    }

    void expect_get_throws(std::filesystem::path p, std::exception_ptr e) {
        auto fut = ss::make_exception_future<
          std::optional<cloud_io::cache_item_stream>>(e);
        EXPECT_CALL(*this, get(p, ::testing::_, ::testing::_, ::testing::_))
          .Times(1)
          .WillOnce(::testing::Return(std::move(fut)));
    }

    void expect_put(std::filesystem::path p, std::exception_ptr e = nullptr) {
        ss::future<> result = ss::now();
        if (e != nullptr) {
            result = ss::make_exception_future<>(e);
        }
        EXPECT_CALL(
          *this,
          put(
            p,
            ::testing::_,
            ::testing::_,
            ::testing::_,
            ::testing::_,
            ::testing::_))
          .Times(1)
          .WillOnce(::testing::Return(std::move(result)));
    }

    void expect_reserve_space(
      size_t size_bytes,
      size_t num_objects,
      cloud_io::basic_space_reservation_guard<ss::lowres_clock> r) {
        auto result = ss::make_ready_future<
          cloud_io::basic_space_reservation_guard<ss::lowres_clock>>(
          std::move(r));
        EXPECT_CALL(*this, reserve_space(size_bytes, num_objects))
          .Times(1)
          .WillOnce(::testing::Return(std::move(result)));
    }

    void expect_reserve_space_throw(std::exception_ptr e) {
        auto result = ss::make_exception_future<
          cloud_io::basic_space_reservation_guard<ss::lowres_clock>>(
          std::move(e));
        EXPECT_CALL(*this, reserve_space(::testing::_, ::testing::_))
          .Times(1)
          .WillOnce(::testing::Return(std::move(result)));
    }
};
