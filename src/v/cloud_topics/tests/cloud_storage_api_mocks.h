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
#include "cloud_io/remote.h"
#include "cloud_storage/cache_service.h"
#include "cloud_storage_clients/types.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "test_utils/tmp_dir.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>

#include <chrono>
#include <stdexcept>

using namespace std::chrono_literals;

// This header contains hand written mocks for the cloud_storage api's (cache
// and remote)

template<class Clock = seastar::manual_clock>
class remote_mock final : public cloud_io::remote_api<Clock> {
public:
    using reset_input_stream = cloud_io::remote_api<Clock>::reset_input_stream;

    ss::future<cloud_io::download_result>
    download_object(cloud_io::basic_download_request<Clock> req) override {
        auto path = req.transfer_details.key();
        // Read what was previously written into the remote_mock
        auto it = _requests.find(path);
        if (it == _requests.end()) {
            co_return cloud_io::download_result::notfound;
        }
        req.payload = it->second.copy();
        co_return cloud_io::download_result::success;
    }

    ss::future<cloud_io::download_result> object_exists(
      const cloud_storage_clients::bucket_name&,
      const cloud_storage_clients::object_key&,
      basic_retry_chain_node<Clock>&,
      std::string_view) override {
        throw std::runtime_error("Not implemented");
    }

    /// \brief Upload small objects to bucket. Suitable for uploading simple
    /// strings, does not check for leadership before upload like the segment
    /// upload function.
    ss::future<cloud_io::upload_result>
    upload_object(cloud_io::basic_upload_request<Clock>) override {
        throw std::runtime_error("Not implemented");
    }

    ss::future<cloud_io::upload_result> upload_stream(
      cloud_io::basic_transfer_details<Clock> transfer_details,
      uint64_t content_length,
      const reset_input_stream& reset_str,
      lazy_abort_source& lazy_abort_source,
      const std::string_view stream_label,
      std::optional<size_t> max_retries) override {
        auto provider = co_await reset_str();
        auto str = provider->take_stream();
        co_await provider->close();
        iobuf data;
        auto out_str = make_iobuf_ref_output_stream(data);
        co_await ss::copy(str, out_str);
        vassert(
          data.size_bytes() == content_length,
          "Expected content_length {}, actual {}",
          content_length,
          data.size_bytes());

        // add time jitter to simulate cloud storage latency
        auto jitter = random_generators::get_int(_max_time_jitter.count());
        co_await ss::sleep<ss::manual_clock>(std::chrono::milliseconds(jitter));

        if (_next_upload_result == cloud_io::upload_result::success) {
            _requests.insert(
              std::make_pair(transfer_details.key(), std::move(data)));
        } else {
            _failed_requests.insert(
              std::make_pair(transfer_details.key(), std::move(data)));
        }
        co_return _next_upload_result;
    }

    ss::future<cloud_io::download_result> download_stream(
      cloud_io::basic_transfer_details<Clock>,
      const cloud_io::try_consume_stream&,
      const std::string_view,
      bool,
      std::optional<cloud_storage_clients::http_byte_range>,
      std::function<void(size_t)>) override {
        throw std::runtime_error("Not implemented");
    }

    void set_next_upload_result(cloud_io::upload_result r) {
        _next_upload_result = r;
    }

    // check if the segment was uploaded (includes both successful and failed
    // requests)
    bool contains(const cloud_storage_clients::object_key& sp) {
        return _requests.contains(sp()) || _failed_requests.contains(sp);
    }

    // check if the segment was uploaded (includes only successful requests)
    bool contains_successful(const cloud_storage_clients::object_key& sp) {
        return _requests.contains(sp());
    }

    std::map<std::filesystem::path, iobuf>& get_successful_requests() {
        return _requests;
    }

    std::map<std::filesystem::path, iobuf>& get_failed_requests() {
        return _failed_requests;
    }

private:
    std::chrono::milliseconds _max_time_jitter{200ms};
    cloud_io::upload_result _next_upload_result{
      cloud_io::upload_result::success};
    std::map<std::filesystem::path, iobuf> _requests;
    std::map<std::filesystem::path, iobuf> _failed_requests;
};

namespace cs = cloud_storage;
struct cache_item_handle {
    iobuf data;
    cs::cache_element_status status{cs::cache_element_status::available};
};
class cache_mock : public cs::cloud_storage_cache_api {
public:
    cache_mock()
      : dir("cloud_storage_cache_mock", temporary_dir_ctor_tag{}) {}
    ss::future<> start() { co_await dir.create(); }
    ss::future<> stop() { co_await dir.remove(); }
    ss::future<std::optional<cs::cache_item>>
    get(std::filesystem::path key) override {
        auto it = objects.find(key);
        if (it == objects.end()) {
            co_return std::nullopt;
        }
        // The cache returns a file handle and not a stream or anything like
        // that. This is inconvenient and requires the following workaround in
        // unit-tests.
        auto tmp_name = dir.get_path() / key;
        auto tmp = co_await ss::open_file_dma(
          tmp_name.native(), ss::open_flags::create | ss::open_flags::rw);
        auto source = make_iobuf_input_stream(it->second.data.copy());
        auto target = co_await ss::make_file_output_stream(tmp);
        co_await ss::copy(source, target);
        co_await target.flush();
        co_await target.close();
        co_await source.close();
        // Reopen the file
        tmp = co_await ss::open_file_dma(
          tmp_name.native(), ss::open_flags::create | ss::open_flags::rw);
        co_return cs::cache_item{
          .body = std::move(tmp),
          .size = it->second.data.size_bytes(),
        };
    }

    /// Add new value to the cache, overwrite if it's already exist
    ///
    /// \param key is a cache key
    /// \param io_priority is an io priority of disk write operation
    /// \param data is an input stream containing data
    /// \param write_buffer_size is a write buffer size for disk write
    /// \param write_behind number of pages that can be written asynchronously
    /// \param reservation caller must have reserved cache space before
    /// proceeding with put
    ss::future<> put(
      std::filesystem::path key,
      ss::input_stream<char>& data,
      cs::space_reservation_guard&,
      ss::io_priority_class,
      size_t,
      unsigned int) override {
        iobuf buffer;
        auto out = make_iobuf_ref_output_stream(buffer);
        co_await ss::copy(data, out);
        objects[key] = cache_item_handle{
          .data = std::move(buffer),
        };
    }

    /// \brief Checks if the value is cached
    ///
    /// \note returned value \c cache_element_status::in_progress is
    /// shard-local, it indicates that the object is being written by this
    /// shard. If the value is being written by another shard, this function
    /// returns only committed result. The value
    /// \c cache_element_status::in_progress can be used as a hint since ntp are
    /// stored on the same shard most of the time.
    ss::future<cs::cache_element_status>
    is_cached(const std::filesystem::path& key) override {
        auto it = objects.find(key);
        if (it == objects.end()) {
            co_return cs::cache_element_status::not_available;
        }
        co_return it->second.status;
    }

    // Call this before starting a download, to trim the cache if necessary
    // and wait until enough free space is available.
    ss::future<cs::space_reservation_guard>
    reserve_space(uint64_t, size_t) override {
        // no-op
        co_return cs::space_reservation_guard(*this, 0, 0);
    }

    // Release capacity acquired via `reserve_space`.  This spawns
    // a background fiber in order to be callable from the guard destructor.
    void reserve_space_release(uint64_t, size_t, uint64_t, size_t) override {
        // no-op
    }

    cache_item_handle& insert(
      const std::filesystem::path& key,
      iobuf buffer,
      cloud_storage::cache_element_status status
      = cloud_storage::cache_element_status::available) {
        objects[key] = cache_item_handle{
          .data = std::move(buffer),
          .status = status,
        };
        return objects[key];
    }

    std::map<std::filesystem::path, cache_item_handle> objects;
    temporary_dir dir;
};
