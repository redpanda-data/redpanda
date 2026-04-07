/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "lsm/io/cloud_cache_persistence.h"

#include "cloud_io/io_result.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/io/cloud_data_persistence_base.h"
#include "lsm/io/file_io.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/seastar.hh>

#include <exception>

namespace lsm::io {

namespace {

static constexpr auto reservation_timeout = std::chrono::seconds(30);

class cache_staged_file_writer : public sequential_file_writer {
public:
    cache_staged_file_writer(
      cloud_io::staging_file staging,
      cloud_io::remote* remote,
      ss::abort_source* as,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key cloud_key)
      : _staging(std::move(staging))
      , _remote(remote)
      , _as(as)
      , _bucket(std::move(bucket))
      , _cloud_key(std::move(cloud_key)) {}

    ss::future<> append(iobuf b) override {
        auto deadline = ss::lowres_clock::now() + reservation_timeout;
        auto fut = co_await ss::coroutine::as_future(
          _staging.append(std::move(b), deadline));
        if (fut.failed()) {
            _failed = true;
            std::rethrow_exception(fut.get_exception());
        }
    }

    ss::future<> close() override {
        if (_failed) {
            co_await cleanup_staging_file();
            co_return;
        }

        auto flush_fut = co_await ss::coroutine::as_future(_staging.flush());
        if (flush_fut.failed()) {
            auto ex = flush_fut.get_exception();
            co_await cleanup_staging_file();
            throw_as_lsm_ex(ex, "failed to flush staging file");
        }

        auto upload_fut = co_await ss::coroutine::as_future(upload_file(
          *_remote,
          *_as,
          _bucket,
          _cloud_key,
          _staging.path(),
          _staging.written()));
        if (upload_fut.failed()) {
            auto ex = upload_fut.get_exception();
            co_await cleanup_staging_file();
            throw_as_lsm_ex(
              ex, fmt::format("failed to upload SST file {}", _cloud_key));
        }

        auto commit_fut = co_await ss::coroutine::as_future(_staging.commit());
        if (commit_fut.failed()) {
            auto ex = commit_fut.get_exception();
            co_await cleanup_staging_file();
            throw_as_lsm_ex(
              ex, "failed to commit to cache (data safe in cloud)");
        }
    }

    fmt::iterator format_to(fmt::iterator it) const override {
        return fmt::format_to(
          it,
          "{{cache_staging={}, upload={}, written={}}}",
          _staging.path(),
          _cloud_key,
          _staging.written());
    }

private:
    ss::future<> cleanup_staging_file() {
        co_await _staging.close();
        auto fut = co_await ss::coroutine::as_future(
          ss::remove_file(_staging.path().native()));
        if (fut.failed()) {
            fut.ignore_ready_future();
        }
    }

    bool _failed = false;
    cloud_io::staging_file _staging;
    cloud_io::remote* _remote;
    ss::abort_source* _as;
    cloud_storage_clients::bucket_name _bucket;
    cloud_storage_clients::object_key _cloud_key;
};

class cloud_cache_data_persistence : public cloud_data_persistence_base {
public:
    cloud_cache_data_persistence(
      cloud_io::cache* cache,
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key prefix)
      : cloud_data_persistence_base(
          remote, std::move(bucket), std::move(prefix))
      , _cache(cache) {}

    ss::future<optional_pointer<random_access_file_reader>>
    open_random_access_reader(internal::file_handle h) override {
        _as.check();
        auto _ = _gate.hold();
        auto filename = internal::sst_file_name(h);
        auto key = cache_key(filename);

        // Check the cache and download on miss, retrying if there was an
        // eviction between download and open. Bounded by the retry_chain_node.
        auto root = make_cloud_rtc(_as);
        while (true) {
            auto reader = co_await open_cached_reader(key);
            if (reader) {
                co_return reader;
            }

            auto dl_fut = co_await ss::coroutine::as_future(
              _remote->download_stream(
                {
                  .bucket = _bucket,
                  .key = cloud_key(filename),
                  .parent_rtc = root,
                },
                [this,
                 &key](uint64_t content_length, ss::input_stream<char> stream) {
                    return save_to_cache(
                      content_length, std::move(stream), key);
                },
                "SST file download",
                /*acquire_hydration_units=*/true));
            if (dl_fut.failed()) {
                throw_as_lsm_ex(
                  dl_fut.get_exception(), "error downloading file");
            }
            if (!check_cloud_result(dl_fut.get())) {
                co_return std::nullopt;
            }
        }
    }

    ss::future<std::unique_ptr<sequential_file_writer>>
    open_sequential_writer(internal::file_handle h) override {
        _as.check();
        auto _ = _gate.hold();
        auto filename = internal::sst_file_name(h);
        auto key = cache_key(filename);
        auto deadline = ss::lowres_clock::now() + reservation_timeout;
        auto staging_fut = co_await ss::coroutine::as_future(
          _cache->create_staging_file(
            key, cloud_io::staging_file_options{}, deadline));
        if (staging_fut.failed()) {
            throw_as_lsm_ex(
              staging_fut.get_exception(), "error opening file writer");
        }
        co_return std::make_unique<cache_staged_file_writer>(
          std::move(staging_fut.get()),
          _remote,
          &_as,
          _bucket,
          cloud_key(filename));
    }

    ss::future<> remove_file_locally(std::string_view filename) override {
        co_await _cache->invalidate(cache_key(filename));
    }

private:
    ss::future<optional_pointer<random_access_file_reader>>
    open_cached_reader(const std::filesystem::path& key) {
        try {
            auto item = co_await _cache->get(key);
            if (!item.has_value()) {
                co_return std::nullopt;
            }
            auto local_path = _cache->get_local_path(key);
            std::unique_ptr<random_access_file_reader> ptr;
            ptr = std::make_unique<disk_file_reader>(
              std::move(local_path), std::move(item->body));
            co_return ptr;
        } catch (const std::system_error& e) {
            if (e.code() == std::errc::no_such_file_or_directory) {
                co_return std::nullopt;
            }
            throw io_error_exception(
              e.code(), "io error opening cached reader: {}", e);
        } catch (...) {
            auto ex = std::current_exception();
            if (ssx::is_shutdown_exception(ex)) {
                throw abort_requested_exception(
                  "shutdown exception opening cached reader: {}", ex);
            }
            throw io_error_exception("io error opening cached reader: {}", ex);
        }
    }

    ss::future<uint64_t> save_to_cache(
      uint64_t content_length,
      ss::input_stream<char> input_stream,
      const std::filesystem::path& key) {
        std::exception_ptr ex;
        try {
            auto reservation = co_await _cache->reserve_space(
              content_length, 1);
            co_await _cache->put(key, input_stream, reservation);
        } catch (...) {
            ex = std::current_exception();
        }
        co_await input_stream.close();
        if (ex) {
            std::rethrow_exception(ex);
        }
        co_return content_length;
    }

    std::filesystem::path cache_key(std::string_view name) {
        return std::filesystem::path("lsm") / _prefix() / name;
    }

    cloud_io::cache* _cache;
};

} // namespace

ss::future<std::unique_ptr<data_persistence>> open_cloud_cache_data_persistence(
  cloud_io::cache* cache,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix) {
    co_return std::make_unique<cloud_cache_data_persistence>(
      cache, remote, std::move(bucket), std::move(prefix));
}

} // namespace lsm::io
