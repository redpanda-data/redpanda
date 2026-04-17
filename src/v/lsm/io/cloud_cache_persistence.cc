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
#include "cloud_io/remote.h"
#include "config/configuration.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/io/file_io.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_provider.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/as_future.hh>

#include <exception>

namespace lsm::io {

namespace {

static constexpr auto reservation_timeout = std::chrono::seconds(30);

retry_chain_node make_cloud_rtc(ss::abort_source& as) {
    constexpr auto timeout = std::chrono::seconds(10);
    auto backoff
      = config::shard_local_cfg().cloud_storage_initial_backoff_ms.value();
    return retry_chain_node{as, timeout, backoff};
}

bool check_cloud_result(cloud_io::download_result result) {
    switch (result) {
    case cloud_io::download_result::success:
        return true;
    case cloud_io::download_result::notfound:
        return false;
    case cloud_io::download_result::timedout:
        throw io_error_exception(
          std::make_error_code(std::errc::timed_out),
          "timeout reading from cloud storage");
    case cloud_io::download_result::failed:
        throw io_error_exception("failed to read from cloud storage");
    }
}

void check_cloud_result(cloud_io::upload_result result) {
    switch (result) {
    case cloud_io::upload_result::success:
        return;
    case cloud_io::upload_result::timedout:
        throw io_error_exception(
          std::make_error_code(std::errc::timed_out),
          "timeout writing to cloud storage");
    case cloud_io::upload_result::failed:
        throw io_error_exception("failed to write to cloud storage");
    case cloud_io::upload_result::cancelled:
        throw io_error_exception("cloud storage write cancelled");
    }
}

cloud_storage_clients::object_key join_cloud_key(
  const cloud_storage_clients::object_key& prefix, std::string_view suffix) {
    return cloud_storage_clients::object_key(prefix() / suffix);
}

[[noreturn]] void throw_as_lsm_ex(std::exception_ptr ex, ss::sstring msg) {
    try {
        std::rethrow_exception(ex);
    } catch (const std::system_error& e) {
        throw io_error_exception(e.code(), "{}: {}", msg, e);
    } catch (...) {
        if (ssx::is_shutdown_exception(ex)) {
            throw abort_requested_exception("{}: {}", msg, ex);
        }
        throw io_error_exception("{}: {}", msg, ex);
    }
}

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

ss::future<> upload_file(
  cloud_io::remote& remote,
  ss::abort_source& as,
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& cloud_key,
  const std::filesystem::path& local_path,
  size_t written) {
    auto root = make_cloud_rtc(as);
    lazy_abort_source las{[&as] {
        return as.abort_requested() ? std::make_optional("abort requested")
                                    : std::nullopt;
    }};
    auto result = co_await remote.upload_stream(
      {
        .bucket = bucket,
        .key = cloud_key,
        .parent_rtc = root,
      },
      written,
      [&local_path]() {
          return ss::open_file_dma(local_path.native(), ss::open_flags::ro)
            .then([](ss::file file) -> std::unique_ptr<stream_provider> {
                ss::file_input_stream_options opts{.read_ahead = 1};
                auto stream = ss::make_file_input_stream(std::move(file), opts);
                return std::make_unique<one_time_stream_provider>(
                  std::move(stream));
            });
      },
      las,
      "SST file upload",
      std::nullopt);
    check_cloud_result(result);
}

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

class cloud_cache_data_persistence : public data_persistence {
public:
    cloud_cache_data_persistence(
      cloud_io::cache* cache,
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key prefix)
      : _cache(cache)
      , _remote(remote)
      , _bucket(std::move(bucket))
      , _prefix(std::move(prefix)) {}

    ss::future<optional_pointer<random_access_file_reader>>
    open_random_access_reader(internal::file_handle h) override {
        _as.check();
        auto _ = _gate.hold();
        auto filename = internal::sst_file_name(h);
        auto key = cache_key(filename);

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

    ss::future<> remove_file(internal::file_handle h) override {
        _as.check();
        auto _ = _gate.hold();
        auto rtc = make_cloud_rtc(_as);
        auto filename = internal::sst_file_name(h);

        cloud_io::upload_result result{};
        try {
            co_await _cache->invalidate(cache_key(filename));
            result = co_await _remote->delete_object({
              .bucket = _bucket,
              .key = cloud_key(filename),
              .parent_rtc = rtc,
            });
        } catch (const std::system_error& e) {
            if (e.code() != std::errc::no_such_file_or_directory) {
                throw io_error_exception(
                  e.code(), "io error removing file: {}", e);
            }
        } catch (...) {
            auto ex = std::current_exception();
            if (ssx::is_shutdown_exception(ex)) {
                throw abort_requested_exception(
                  "shutdown exception while removing file: {}", ex);
            }
            throw io_error_exception("io error removing file: {}", ex);
        }
        check_cloud_result(result);
    }

    ss::coroutine::experimental::generator<internal::file_handle>
    list_files() override {
        _as.check();
        auto _ = _gate.hold();
        auto rtc = make_cloud_rtc(_as);
        cloud_io::list_result result
          = cloud_storage_clients::error_outcome::fail;
        try {
            result = co_await _remote->list_objects(_bucket, rtc, _prefix);
        } catch (...) {
            auto ex = std::current_exception();
            if (ssx::is_shutdown_exception(ex)) {
                throw abort_requested_exception(
                  "shutdown exception while listing files: {}", ex);
            }
            throw io_error_exception("io error listing files: {}", ex);
        }
        if (result.has_error()) {
            throw io_error_exception(
              "io error listing files: {}", result.error());
        }
        for (const auto& item : result.value().contents) {
            auto suffix = std::filesystem::path(item.key).lexically_relative(
              _prefix());
            if (suffix.has_parent_path() || !suffix.has_filename()) {
                continue;
            }
            auto file_id = internal::parse_sst_file_name(
              suffix.filename().native());
            if (!file_id) {
                continue;
            }
            co_yield *file_id;
            _as.check();
        }
    }

    ss::future<> close() override {
        _as.request_abort_ex(
          abort_requested_exception("cloud persistence layer shutdown"));
        co_await _gate.close();
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

    cloud_storage_clients::object_key cloud_key(std::string_view name) {
        return join_cloud_key(_prefix, name);
    }

    cloud_io::cache* _cache;
    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    cloud_storage_clients::object_key _prefix;
    ss::abort_source _as;
    ss::gate _gate;
};

class cloud_metadata_persistence : public metadata_persistence {
public:
    cloud_metadata_persistence(
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key prefix)
      : _remote(remote)
      , _bucket(std::move(bucket))
      , _prefix(std::move(prefix)) {}

    ss::future<std::optional<iobuf>>
    read_manifest(internal::database_epoch epoch) override {
        _as.check();
        auto _ = _gate.hold();
        auto rtc = make_cloud_rtc(_as);
        auto max_key = manifest_key(epoch);
        auto keys = co_await list_manifests();
        auto it = std::ranges::find_if(
          keys, [&max_key](const auto& key) { return key <= max_key; });
        if (it == keys.end()) {
            co_return std::nullopt;
        }
        iobuf b;
        auto result = co_await _remote->download_object({
          .transfer_details = {
            .bucket = _bucket,
            .key = std::move(*it),
            .parent_rtc = rtc,
          },
          .display_str = "LSM Manifest download",
          .payload = b,
          .expect_missing = true,
        });
        co_return check_cloud_result(result) ? std::make_optional(std::move(b))
                                             : std::nullopt;
    }

    ss::future<>
    write_manifest(internal::database_epoch epoch, iobuf b) override {
        _as.check();
        auto _ = _gate.hold();
        auto rtc = make_cloud_rtc(_as);
        auto my_key = manifest_key(epoch);
        auto result = co_await _remote->upload_object({
          .transfer_details = {
            .bucket = _bucket,
            .key = my_key,
            .parent_rtc = rtc,
          },
          .display_str = "LSM Manifest upload",
          .payload = std::move(b),
        });
        check_cloud_result(result);
        chunked_vector<cloud_storage_clients::object_key> keys_to_delete;
        for (const auto& key : co_await list_manifests()) {
            if (key >= my_key) {
                continue;
            }
            keys_to_delete.push_back(key);
        }
        if (keys_to_delete.empty()) {
            co_return;
        }
        result = co_await _remote->delete_objects(
          _bucket, std::move(keys_to_delete), rtc, [](size_t) {});
    }

    ss::future<> close() override {
        _as.request_abort_ex(abort_requested_exception(
          "cloud metadata persistence layer shutdown"));
        co_await _gate.close();
    }

private:
    ss::future<chunked_vector<cloud_storage_clients::object_key>>
    list_manifests() {
        using namespace cloud_storage_clients;
        auto rtc = make_cloud_rtc(_as);
        auto list_result = co_await _remote->list_objects(
          _bucket, rtc, manifest_prefix());
        if (list_result.has_error()) {
            switch (list_result.error()) {
            case error_outcome::fail:
                throw io_error_exception("failure listing manifest files");
            case error_outcome::retry:
            case error_outcome::key_not_found:
            case error_outcome::operation_not_supported:
            case error_outcome::authentication_failed:
                throw io_error_exception(
                  "unexpected error when listing manifest files: {}",
                  list_result.error());
            }
        }
        auto& items = list_result.value().contents;
        chunked_vector<cloud_storage_clients::object_key> manifest_keys;
        manifest_keys.reserve(items.size());
        for (const auto& item : items) {
            manifest_keys.emplace_back(item.key);
        }
        std::ranges::sort(manifest_keys, std::greater<>{});
        co_return manifest_keys;
    }

    cloud_storage_clients::object_key manifest_prefix() const {
        return cloud_storage_clients::object_key{_prefix() / "MANIFEST."};
    }

    cloud_storage_clients::object_key
    manifest_key(internal::database_epoch epoch) const {
        return cloud_storage_clients::object_key{
          _prefix() / fmt::format("MANIFEST.{:020}", epoch)};
    }

    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    cloud_storage_clients::object_key _prefix;
    ss::abort_source _as;
    ss::gate _gate;
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

ss::future<std::unique_ptr<metadata_persistence>>
open_cloud_metadata_persistence(
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix) {
    co_return std::make_unique<cloud_metadata_persistence>(
      remote, std::move(bucket), std::move(prefix));
}

} // namespace lsm::io
