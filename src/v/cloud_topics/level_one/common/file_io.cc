/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/file_io.h"

#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/client.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/common/object_utils.h"
#include "cloud_topics/logger.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>

#include <memory>

using namespace std::chrono_literals;

namespace experimental::cloud_topics::l1 {

namespace {

class staging_file_impl : public staging_file {
public:
    explicit staging_file_impl(std::filesystem::path path)
      : _path(std::move(path)) {}

    ss::future<size_t> size() override { return ss::file_size(_path.native()); }
    ss::future<ss::output_stream<char>> output_stream() override {
        auto file = co_await ss::open_file_dma(
          _path.native(),
          ss::open_flags::rw | ss::open_flags::truncate
            | ss::open_flags::create);
        co_return co_await ss::make_file_output_stream(std::move(file));
    }
    ss::future<> remove() override { return ss::remove_file(_path.native()); }
    ss::future<ss::input_stream<char>> input_stream() override {
        auto file = co_await ss::open_file_dma(
          _path.native(), ss::open_flags::ro);
        co_return ss::make_file_input_stream(std::move(file));
    }

private:
    std::filesystem::path _path;
};

// TODO: deduplicate, expose from cloud storage
struct one_time_stream_provider : public stream_provider {
    explicit one_time_stream_provider(ss::input_stream<char> s)
      : _st(std::move(s)) {}

    ss::input_stream<char> take_stream() override {
        auto tmp = std::exchange(_st, std::nullopt);
        return std::move(tmp.value());
    }
    ss::future<> close() override {
        if (_st.has_value()) {
            return _st->close().then([this] { _st = std::nullopt; });
        }
        return ss::now();
    }
    std::optional<ss::input_stream<char>> _st;
};

} // namespace

file_io::file_io(
  std::filesystem::path staging_dir,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage::cache* cache)
  : _remote(remote)
  , _bucket(std::move(bucket))
  , _staging_dir(std::move(staging_dir))
  , _cache(cache) {}

ss::future<std::expected<std::unique_ptr<staging_file>, io::errc>>
file_io::create_tmp_file() {
    co_return std::make_unique<staging_file_impl>(
      _staging_dir / fmt::format("{}.tmp", uuid_t::create()));
}

ss::future<std::expected<void, io::errc>>
file_io::put_object(object_id oid, staging_file* file, ss::abort_source* as) {
    auto file_size = co_await file->size();
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);
    lazy_abort_source las{[as] {
        return as->abort_requested() ? std::make_optional("abort requested")
                                     : std::nullopt;
    }};
    auto result_fut
      = co_await ss::coroutine::as_future<cloud_io::upload_result>(
        _remote->upload_stream(
          cloud_io::transfer_details{
            .bucket = _bucket,
            .key = object_path_factory::level_one_path(oid),
            .parent_rtc = root,
          },
          file_size,
          [this, file]() {
              return io::read_file(file).then(
                [](ss::input_stream<char> stream)
                  -> std::unique_ptr<stream_provider> {
                    return std::make_unique<one_time_stream_provider>(
                      std::move(stream));
                });
          },
          las,
          "l1_file_upload",
          std::nullopt));
    if (result_fut.failed()) {
        vlog(
          cd_log.warn, "Error uploading file: {}", result_fut.get_exception());
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    switch (result_fut.get()) {
    case cloud_io::upload_result::success:
        // TODO(cloud_topics): Consider preemptively putting the object in the
        // cache
        co_return std::expected<void, io::errc>{};
    case cloud_io::upload_result::timedout:
    case cloud_io::upload_result::cancelled:
        co_return std::unexpected(io::errc::cloud_op_timeout);
    case cloud_io::upload_result::failed:
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    std::unreachable();
}

ss::future<uint64_t> file_io::save_to_cache(
  ss::input_stream<char> stream,
  cloud_storage::space_reservation_guard* reservation,
  std::filesystem::path cache_key,
  uint64_t content_length) {
    co_await _cache->put(std::move(cache_key), stream, *reservation);
    co_return content_length;
}

ss::future<std::expected<ss::input_stream<char>, io::errc>>
file_io::read_object(object_extent extent, ss::abort_source* as) {
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);
    lazy_abort_source las{[as] {
        return as->abort_requested() ? std::make_optional("abort requested")
                                     : std::nullopt;
    }};
    // TODO(cloud_topics): Optimize the cache such that it understands partial
    // objects? Or we assert somehow there are no overlaps (or just live with
    // them).
    // TODO(cloud_topics): If reading just a footer, we should skip the cache.
    // Maybe we need another method for that which is iobuf based?
    std::filesystem::path cache_key = fmt::format(
      "l1_{}_position_{}_size_{}.partial",
      extent.id,
      extent.position,
      extent.size);
    while (true) {
        auto stream_fut = co_await ss::coroutine::as_future<
          std::optional<cloud_io::cache_item_stream>>(
          _cache->get_stream(cache_key));
        if (stream_fut.failed()) {
            vlog(
              cd_log.warn,
              "Error reading from cache for {}: {}",
              extent,
              stream_fut.get_exception());
            co_return std::unexpected(io::errc::file_io_error);
        }
        auto stream = stream_fut.get();
        if (stream) {
            co_return std::move(stream->body);
        }
        // TODO(cloud_topics): reserving space should also take an abort_source
        auto reservation_fut = co_await ss::coroutine::as_future<
          cloud_storage::space_reservation_guard>(
          _cache->reserve_space(extent.size, 1));
        if (reservation_fut.failed()) {
            vlog(
              cd_log.warn,
              "Error reserving cache space for download of {}: {}",
              extent,
              reservation_fut.get_exception());
            co_return std::unexpected(io::errc::file_io_error);
        }
        cloud_io::try_consume_stream consumer =
          [this, r = reservation_fut.get(), &cache_key](
            uint64_t content_length, ss::input_stream<char> stream) mutable {
              return save_to_cache(
                std::move(stream), &r, cache_key, content_length);
          };
        auto result_fut
          = co_await ss::coroutine::as_future<cloud_io::download_result>(
            _remote->download_stream(
              cloud_io::transfer_details{
                .bucket = _bucket,
                .key = object_path_factory::level_one_path(extent.id),
                .parent_rtc = root,
              },
              consumer,
              "l1_file_download",
              /*acquire_hydration_units=*/true,
              cloud_storage_clients::http_byte_range{
                extent.position, extent.position + extent.size - 1}));
        if (result_fut.failed()) {
            vlog(
              cd_log.warn,
              "Error downloading object {}: {}",
              extent,
              result_fut.get_exception());
            co_return std::unexpected(io::errc::cloud_op_error);
        }
        switch (result_fut.get()) {
        case cloud_io::download_result::success:
            continue; // Now that it's in the cache the lookup should succeed.
        case cloud_io::download_result::notfound:
            co_return std::unexpected(io::errc::cloud_missing_object);
        case cloud_io::download_result::timedout:
            co_return std::unexpected(io::errc::cloud_op_timeout);
        case cloud_io::download_result::failed:
            co_return std::unexpected(io::errc::cloud_op_error);
        }
        std::unreachable();
    }
}

ss::future<std::expected<void, io::errc>>
file_io::delete_objects(chunked_vector<object_id> ids, ss::abort_source* as) {
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);
    chunked_vector<cloud_storage_clients::object_key> keys;
    for (const auto& id : ids) {
        keys.push_back(object_path_factory::level_one_path(id));
    }
    auto result_fut
      = co_await ss::coroutine::as_future<cloud_io::upload_result>(
        _remote->delete_objects(
          _bucket, std::move(keys), root, [](size_t retry_count) {
              std::ignore = retry_count;
          }));
    if (result_fut.failed()) {
        vlog(
          cd_log.warn,
          "Error deleting objects: {}",
          result_fut.get_exception());
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    switch (result_fut.get()) {
    case cloud_io::upload_result::success:
        co_return std::expected<void, io::errc>{};
    case cloud_io::upload_result::timedout:
    case cloud_io::upload_result::cancelled:
        co_return std::unexpected(io::errc::cloud_op_timeout);
    case cloud_io::upload_result::failed:
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    std::unreachable();
}

} // namespace experimental::cloud_topics::l1
