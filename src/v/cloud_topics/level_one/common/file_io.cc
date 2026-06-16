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

#include "bytes/iostream.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/client.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/common/object_utils.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/as_future.hh>

#include <memory>
#include <optional>
#include <utility>

using namespace std::chrono_literals;

namespace cloud_topics::l1 {

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
        ss::file_output_stream_options options{};
        // The read buffer size also makes sense as the write buffer here
        // (default 128KiB).
        options.buffer_size
          = config::shard_local_cfg().storage_read_buffer_size();
        // Defaults to 1, which is reasonable for write-behind as well.
        options.write_behind
          = config::shard_local_cfg().storage_read_readahead_count();
        co_return co_await ss::make_file_output_stream(
          std::move(file), std::move(options));
    }
    ss::future<> remove() override { return ss::remove_file(_path.native()); }
    ss::future<ss::input_stream<char>> input_stream() override {
        auto file = co_await ss::open_file_dma(
          _path.native(), ss::open_flags::ro);
        ss::file_input_stream_options options{};
        options.buffer_size
          = config::shard_local_cfg().storage_read_buffer_size();
        options.read_ahead
          = config::shard_local_cfg().storage_read_readahead_count();
        co_return ss::make_file_input_stream(
          std::move(file), std::move(options));
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

// A streaming download source for L1 objects that chunks downloads per
// `chunk_size`. This may result in many GET requests per object download.
//
// Cache-bypassing ranged download of the object byte range [pos, pos+len) at
// `key`, fully buffered into an input_stream. `download_stream` (a single
// ranged GET) returns the lease to the pool as soon as the range body has been
// read off the connection, before the buffered bytes are served -- so both the
// peak in-memory bytes and the connection-hold duration are bounded by the
// range length. Backs read_object's skip_cache path without populating the
// cloud cache; the range is bounded because open_object invokes read_object
// once per chunk. A download failure throws (rethrowing the original
// exception, or a runtime_error for a non-success result), surfaced from the
// caller's read of the returned stream.
ss::future<std::expected<ss::input_stream<char>, io::errc>>
download_range_bypassing_cache(
  cloud_io::remote* remote,
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& key,
  cloud_io::group_id gid,
  size_t pos,
  size_t len,
  ss::abort_source* as) {
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);

    iobuf buf;
    cloud_io::try_consume_stream consumer =
      [&buf, as](
        this auto,
        uint64_t /*content_length*/,
        ss::input_stream<char> stream) -> ss::future<uint64_t> {
        uint64_t total = 0;
        std::exception_ptr ex;
        try {
            while (true) {
                as->check();
                auto b = co_await stream.read();
                if (b.empty()) {
                    break;
                }
                total += b.size();
                buf.append(std::move(b));
            }
        } catch (...) {
            ex = std::current_exception();
        }
        co_await stream.close();
        if (ex) {
            std::rethrow_exception(ex);
        }
        co_return total;
    };

    auto result_fut = co_await ss::coroutine::as_future(remote->download_stream(
      cloud_io::transfer_details{
        .bucket = bucket,
        .key = key,
        .parent_rtc = root,
      },
      consumer,
      "l1_stream_download",
      /*acquire_hydration_units=*/true,
      cloud_storage_clients::http_byte_range{pos, pos + len - 1},
      {},
      gid));
    if (result_fut.failed()) {
        std::rethrow_exception(result_fut.get_exception());
    }
    auto result = result_fut.get();
    if (result != cloud_io::download_result::success) {
        throw std::runtime_error(
          fmt::format("L1 streaming download failed: {}", result));
    }
    co_return make_iobuf_input_stream(std::move(buf));
}

} // namespace

file_io::file_io(
  std::filesystem::path staging_dir,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::cache* cache,
  file_io_probe* probe)
  : _remote(remote)
  , _bucket(std::move(bucket))
  , _staging_dir(std::move(staging_dir))
  , _cache(cache)
  , _probe(probe) {}

ss::future<> file_io::stop() { return _gate.close(); }

std::filesystem::path file_io::cache_key(const object_extent& extent) {
    return std::filesystem::path(
      fmt::format(
        "l1_{}_position_{}_size_{}.partial",
        extent.id,
        extent.position,
        extent.size));
}

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
        auto ex = result_fut.get_exception();
        vlog(cd_log.warn, "Error uploading file: {}", ex);
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
  cloud_io::space_reservation_guard* reservation,
  std::filesystem::path cache_key,
  uint64_t content_length) {
    co_await _cache->put(std::move(cache_key), stream, *reservation);
    co_return content_length;
}

ss::future<std::expected<void, io::errc>> file_io::do_download_to_cache(
  const object_extent& extent,
  const std::filesystem::path& cache_key,
  retry_chain_node& root,
  ss::abort_source& as,
  cloud_io::group_id gid) {
    // TODO(cloud_topics): reserving space should also take an abort_source
    auto reservation_fut
      = co_await ss::coroutine::as_future<cloud_io::space_reservation_guard>(
        _cache->reserve_space(extent.size, 1));
    if (reservation_fut.failed()) {
        auto ex = reservation_fut.get_exception();
        vlog(
          cd_log.warn,
          "Error reserving cache space for download of {}: {}",
          extent,
          ex);
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
            extent.position, extent.position + extent.size - 1},
          {},
          gid));
    if (result_fut.failed()) {
        auto ex = result_fut.get_exception();
        vlog(cd_log.warn, "Error downloading object {}: {}", extent, ex);
        // Map abort to cloud_op_timeout so a leader-abort and a
        // merger-abort produce the same errc for the same event.
        co_return std::unexpected(
          as.abort_requested() ? io::errc::cloud_op_timeout
                               : io::errc::cloud_op_error);
    }
    switch (result_fut.get()) {
    case cloud_io::download_result::success:
        co_return std::expected<void, io::errc>{};
    case cloud_io::download_result::notfound:
        co_return std::unexpected(io::errc::cloud_missing_object);
    case cloud_io::download_result::timedout:
        co_return std::unexpected(io::errc::cloud_op_timeout);
    case cloud_io::download_result::failed:
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    std::unreachable();
}

ss::future<std::expected<ss::input_stream<char>, io::errc>>
file_io::read_object(
  object_extent extent,
  ss::abort_source* as,
  cloud_io::group_id gid,
  bool skip_cache) {
    if (_gate.is_closed()) {
        co_return std::unexpected(io::errc::file_io_error);
    }
    auto holder = _gate.hold();
    if (_probe) {
        _probe->register_read();
    }
    static constexpr auto timeout = 10s;
    static constexpr auto backoff = 100ms;
    retry_chain_node root(*as, ss::lowres_clock::now() + timeout, backoff);
    // TODO(cloud_topics): Optimize the cache such that it understands partial
    // objects? Or we assert somehow there are no overlaps (or just live with
    // them).
    // TODO(cloud_topics): If reading just a footer, we should skip the cache.
    // Maybe we need another method for that which is iobuf based?
    auto cache_key = file_io::cache_key(extent);
    while (true) {
        auto stream_fut = co_await ss::coroutine::as_future<
          std::optional<cloud_io::cache_item_stream>>(_cache->get_stream(
          cache_key,
          config::shard_local_cfg().storage_read_buffer_size(),
          config::shard_local_cfg().storage_read_readahead_count()));
        if (stream_fut.failed()) {
            auto ex = stream_fut.get_exception();
            vlog(
              cd_log.warn, "Error reading from cache for {}: {}", extent, ex);
            co_return std::unexpected(io::errc::file_io_error);
        }
        auto stream = stream_fut.get();
        if (stream) {
            co_return std::move(stream->body);
        }

        if (_probe) {
            _probe->register_cache_miss();
        }

        if (skip_cache) {
            // Not in the cache: serve the requested range directly from object
            // storage as a single ranged GET, without populating the cache. Any
            // chunking that bounds peak memory is applied a layer up in
            // open_object, which invokes read_object once per chunk.
            co_return co_await download_range_bypassing_cache(
              _remote,
              _bucket,
              object_path_factory::level_one_path(extent.id),
              gid,
              extent.position,
              extent.size,
              as);
        }

        // single_flight dedups concurrent downloads for this extent.
        auto r = co_await _single_flight.run(
          cache_key,
          *as,
          [this, &extent, &cache_key, &root, as, gid]() {
              return do_download_to_cache(extent, cache_key, root, *as, gid);
          },
          &cd_log);

        if (!r.has_value()) {
            // TODO(cloud_topics): on a transient errc, a merger whose own
            // abort_source hasn't fired could re-loop as its own leader
            // instead of inheriting the leader's failure.
            co_return std::unexpected(r.error());
        }
        if (r.value()) {
            vlog(cd_log.debug, "Merged L1 read for {}", extent);
            if (_probe) {
                _probe->register_concurrent_read_merge();
            }
        }
    }
}

ss::future<std::expected<iobuf, io::errc>> file_io::fetch_native_footer(
  object_extent extent,
  ss::abort_source* as,
  cloud_io::group_id gid,
  bool skip_cache) {
    if (_probe != nullptr) {
        _probe->register_footer_read(extent.size);
    }
    return io::fetch_native_footer(extent, as, gid, skip_cache);
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
        auto ex = result_fut.get_exception();
        vlog(cd_log.warn, "Error deleting objects: {}", ex);
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

ss::future<std::expected<cloud_storage_clients::multipart_upload_ref, io::errc>>
file_io::create_multipart_upload(
  object_id oid, size_t part_size, ss::abort_source* as) {
    static constexpr auto timeout = 10s;
    auto key = object_path_factory::level_one_path(oid);
    auto result_fut = co_await ss::coroutine::as_future(
      _remote->initiate_multipart_upload(_bucket, key, part_size, timeout));
    if (result_fut.failed()) {
        auto ex = result_fut.get_exception();
        vlog(cd_log.warn, "Error initiating multipart upload: {}", ex);
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    auto result = result_fut.get();
    if (!result.has_value()) {
        vlog(
          cd_log.warn,
          "Failed to initiate multipart upload for {}: {}",
          oid,
          result.error());
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    co_return std::move(result.value());
}

} // namespace cloud_topics::l1
