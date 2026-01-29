/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "absl/container/flat_hash_map.h"
#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/batch_cache/hydrated_cache_api.h"
#include "cloud_topics/types.h"
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
      get_stream,
      (std::filesystem::path key,
       size_t read_buffer_size,
       unsigned int read_ahead),
      ());

    MOCK_METHOD(
      ss::future<std::optional<cloud_io::cache_item_stream>>,
      get_stream_range,
      (std::filesystem::path key,
       uint64_t offset,
       uint64_t length,
       size_t read_buffer_size,
       unsigned int read_ahead),
      ());

    MOCK_METHOD(
      ss::future<>,
      put,
      (std::filesystem::path key,
       ss::input_stream<char>& data,
       cloud_io::basic_space_reservation_guard<ss::lowres_clock>& reservation,
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

    void expect_get_stream(
      std::filesystem::path p,
      std::optional<cloud_io::cache_item_stream> item) {
        auto fut
          = ss::make_ready_future<std::optional<cloud_io::cache_item_stream>>(
            std::move(item));
        EXPECT_CALL(*this, get_stream(p, ::testing::_, ::testing::_))
          .Times(1)
          .WillOnce(::testing::Return(std::move(fut)));
    }

    void
    expect_get_stream_throws(std::filesystem::path p, std::exception_ptr e) {
        auto fut = ss::make_exception_future<
          std::optional<cloud_io::cache_item_stream>>(e);
        EXPECT_CALL(*this, get_stream(p, ::testing::_, ::testing::_))
          .Times(1)
          .WillOnce(::testing::Return(std::move(fut)));
    }

    void expect_get_stream_range(
      std::filesystem::path p,
      uint64_t offset,
      uint64_t length,
      std::optional<cloud_io::cache_item_stream> item) {
        auto fut
          = ss::make_ready_future<std::optional<cloud_io::cache_item_stream>>(
            std::move(item));
        EXPECT_CALL(
          *this,
          get_stream_range(p, offset, length, ::testing::_, ::testing::_))
          .Times(1)
          .WillOnce(::testing::Return(std::move(fut)));
    }

    void expect_get_stream_range_throws(
      std::filesystem::path p,
      uint64_t offset,
      uint64_t length,
      std::exception_ptr e) {
        auto fut = ss::make_exception_future<
          std::optional<cloud_io::cache_item_stream>>(e);
        EXPECT_CALL(
          *this,
          get_stream_range(p, offset, length, ::testing::_, ::testing::_))
          .Times(1)
          .WillOnce(::testing::Return(std::move(fut)));
    }

    void expect_put(std::filesystem::path p, std::exception_ptr e = nullptr) {
        ss::future<> result = ss::now();
        if (e != nullptr) {
            result = ss::make_exception_future<>(e);
        }
        EXPECT_CALL(
          *this, put(p, ::testing::_, ::testing::_, ::testing::_, ::testing::_))
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

/// Key for hydrated cache mock storage
struct hydrated_cache_key {
    model::topic_id_partition tidp;
    cloud_topics::object_id id;
    cloud_topics::first_byte_offset_t byte_offset;

    bool operator==(const hydrated_cache_key&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const hydrated_cache_key& k) {
        return H::combine(std::move(h), k.tidp, k.id, k.byte_offset());
    }
};

/// Mock implementation of partition_hydrated_cache_api for testing.
/// Stores data in an in-memory map for test verification.
class hydrated_cache_mock : public cloud_topics::partition_hydrated_cache_api {
public:
    /// Injected failure mode for has() calls
    enum class has_failure {
        none,
        return_false, // Always return false
    };

    /// Injected failure mode for get() calls
    enum class get_failure {
        none,
        return_nullopt, // Return nullopt
        throw_error,    // Throw std::runtime_error
        throw_shutdown, // Throw ss::abort_requested_exception
    };

    /// Injected failure mode for put() calls
    enum class put_failure {
        none,
        throw_error,    // Throw std::runtime_error
        throw_shutdown, // Throw ss::abort_requested_exception
    };

    bool is_cached(
      const model::topic_id_partition& tidp,
      const cloud_topics::object_id& id,
      cloud_topics::first_byte_offset_t byte_offset,
      [[maybe_unused]] cloud_topics::byte_range_size_t size) const override {
        if (_has_failure == has_failure::return_false) {
            return false;
        }
        hydrated_cache_key key{
          .tidp = tidp, .id = id, .byte_offset = byte_offset};
        return _storage.contains(key);
    }

    std::optional<iobuf> get(
      const model::topic_id_partition& tidp,
      const cloud_topics::object_id& id,
      cloud_topics::first_byte_offset_t byte_offset,
      [[maybe_unused]] cloud_topics::byte_range_size_t size) override {
        ++_get_calls;
        switch (_get_failure) {
        case get_failure::return_nullopt:
            return std::nullopt;
        case get_failure::throw_error:
            throw std::runtime_error("injected get error");
        case get_failure::throw_shutdown:
            throw ss::abort_requested_exception();
        case get_failure::none:
            break;
        }

        hydrated_cache_key key{
          .tidp = tidp, .id = id, .byte_offset = byte_offset};
        auto it = _storage.find(key);
        if (it == _storage.end()) {
            return std::nullopt;
        }
        // Return a copy of the stored data
        return it->second.copy();
    }

    void put(
      const model::topic_id_partition& tidp,
      const cloud_topics::object_id& id,
      cloud_topics::first_byte_offset_t byte_offset,
      iobuf payload) override {
        ++_put_calls;
        switch (_put_failure) {
        case put_failure::throw_error:
            throw std::runtime_error("injected put error");
        case put_failure::throw_shutdown:
            throw ss::abort_requested_exception();
        case put_failure::none:
            break;
        }

        hydrated_cache_key key{
          .tidp = tidp, .id = id, .byte_offset = byte_offset};
        _storage.insert_or_assign(key, std::move(payload));
    }

    void truncate_hydrated(
      [[maybe_unused]] const model::topic_id_partition& tidp,
      [[maybe_unused]] cloud_topics::cluster_epoch invalidated_epoch) override {
        // Test mock - no-op truncation
    }

    /// Pre-populate the cache with data for testing (using default tidp)
    void add_data(
      const cloud_topics::object_id& id,
      cloud_topics::first_byte_offset_t byte_offset,
      iobuf data) {
        // Use a default tidp for backward compatibility with existing tests
        model::topic_id_partition default_tidp{
          model::topic_id::create(), model::partition_id{0}};
        hydrated_cache_key key{
          .tidp = default_tidp, .id = id, .byte_offset = byte_offset};
        _storage.insert_or_assign(key, std::move(data));
    }

    /// Pre-populate the cache with data for testing (with specific tidp)
    void add_data(
      const model::topic_id_partition& tidp,
      const cloud_topics::object_id& id,
      cloud_topics::first_byte_offset_t byte_offset,
      iobuf data) {
        hydrated_cache_key key{
          .tidp = tidp, .id = id, .byte_offset = byte_offset};
        _storage.insert_or_assign(key, std::move(data));
    }

    /// Set the failure mode for has() calls
    void set_has_failure(has_failure f) { _has_failure = f; }

    /// Set the failure mode for get() calls
    void set_get_failure(get_failure f) { _get_failure = f; }

    /// Set the failure mode for put() calls
    void set_put_failure(put_failure f) { _put_failure = f; }

    /// Get number of get() calls
    size_t get_calls() const { return _get_calls; }

    /// Get number of put() calls
    size_t put_calls() const { return _put_calls; }

    /// Check if data exists in the mock storage
    bool contains(
      const model::topic_id_partition& tidp,
      const cloud_topics::object_id& id,
      cloud_topics::first_byte_offset_t byte_offset) const {
        hydrated_cache_key key{
          .tidp = tidp, .id = id, .byte_offset = byte_offset};
        return _storage.contains(key);
    }

    /// Clear all stored data
    void clear() { _storage.clear(); }

private:
    absl::flat_hash_map<hydrated_cache_key, iobuf> _storage;
    has_failure _has_failure{has_failure::none};
    get_failure _get_failure{get_failure::none};
    put_failure _put_failure{put_failure::none};
    size_t _get_calls{0};
    size_t _put_calls{0};
};
