/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "lsm/io/cloud_persistence.h"

#include "cloud_io/io_result.h"
#include "cloud_storage_clients/types.h"
#include "config/configuration.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/io/file_io.h"
#include "lsm/io/persistence.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"
#include "utils/uuid.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>

#include <algorithm>
#include <exception>
#include <memory>

namespace lsm::io {

namespace {

retry_chain_node make_rtc(ss::abort_source& as) {
    constexpr auto timeout = std::chrono::seconds(10);
    auto backoff
      = config::shard_local_cfg().cloud_storage_initial_backoff_ms.value();
    return retry_chain_node{
      as,
      timeout,
      backoff,
    };
}

bool check_result(cloud_io::download_result result) {
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

void check_result(cloud_io::upload_result result) {
    switch (result) {
    case cloud_io::upload_result::success:
        return;
    case cloud_io::upload_result::timedout:
        throw io_error_exception(
          std::make_error_code(std::errc::timed_out),
          "timeout reading from cloud storage");
    case cloud_io::upload_result::failed:
        throw io_error_exception("failed to write to cloud storage");
    case cloud_io::upload_result::cancelled:
        throw io_error_exception("cloud storage write cancelled");
    }
}

cloud_storage_clients::object_key
join(const cloud_storage_clients::object_key& prefix, std::string_view suffix) {
    return cloud_storage_clients::object_key(prefix() / suffix);
}

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

class staged_file_writer : public disk_seq_file_writer {
public:
    staged_file_writer(
      std::filesystem::path staging,
      ss::output_stream<char> output,
      cloud_io::remote* remote,
      ss::abort_source* as,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key key)
      : disk_seq_file_writer(std::move(staging), std::move(output))
      , _remote(remote)
      , _as(as)
      , _bucket(std::move(bucket))
      , _key(std::move(key)) {}

    ss::future<> append(iobuf b) override {
        _written += b.size_bytes();
        co_await disk_seq_file_writer::append(std::move(b));
    }

    ss::future<> close() override {
        co_await disk_seq_file_writer::close();
        try {
            co_await upload().then_wrapped([this](ss::future<> fut) {
                // If the upload fails, then we need to delete the staged file.
                if (fut.failed()) {
                    return fut.finally(
                      [this] { return ss::remove_file(path().native()); });
                }
                return fut;
            });
        } catch (const std::system_error& err) {
            throw io_error_exception(err.code(), "io error closing: {}", err);
        } catch (...) {
            auto ex = std::current_exception();
            if (ssx::is_shutdown_exception(ex)) {
                throw abort_requested_exception(
                  "shutdown exception while closing: {}", ex);
            }
            throw io_error_exception("io error closing: {}", ex);
        }
    }

    fmt::iterator format_to(fmt::iterator it) const override {
        it = fmt::format_to(it, "{{staging=");
        it = disk_seq_file_writer::format_to(it);
        return fmt::format_to(it, ", upload={}, written={}}}", _key, _written);
    }

private:
    ss::future<> upload() {
        auto root = make_rtc(*_as);
        lazy_abort_source las{[this] {
            return _as->abort_requested()
                     ? std::make_optional("abort requested")
                     : std::nullopt;
        }};
        // TODO: Ensure the file doesn't yet exist with CAS
        auto result = co_await _remote->upload_stream(
          {
            .bucket = _bucket,
            .key = _key,
            .parent_rtc = root,
          },
          _written,
          [this]() {
              return ss::open_file_dma(path().native(), ss::open_flags::ro)
                .then([](ss::file file) -> std::unique_ptr<stream_provider> {
                    ss::file_input_stream_options opts{.read_ahead = 1};
                    auto stream = ss::make_file_input_stream(
                      std::move(file), opts);
                    return std::make_unique<one_time_stream_provider>(
                      std::move(stream));
                });
          },
          las,
          "SST file upload",
          std::nullopt);
        check_result(result);
    }

    size_t _written = 0;
    cloud_io::remote* _remote;
    ss::abort_source* _as;
    cloud_storage_clients::bucket_name _bucket;
    cloud_storage_clients::object_key _key;
};

class cloud_data_persistence : public data_persistence {
public:
    cloud_data_persistence(
      std::filesystem::path staging,
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key prefix,
      ss::sstring staging_prefix)
      : _staging(std::move(staging))
      , _remote(remote)
      , _bucket(std::move(bucket))
      , _prefix(std::move(prefix))
      , _staging_prefix(std::move(staging_prefix)) {}

    ss::future<optional_pointer<random_access_file_reader>>
    open_random_access_reader(internal::file_handle h) override {
        _as.check();
        auto _ = _gate.hold();
        auto filename = internal::sst_file_name(h);
        auto filepath = staging_path(filename);
        auto reader = co_await open_local_reader(filepath);
        if (reader) {
            co_return reader;
        }
        auto root = make_rtc(_as);
        cloud_io::download_result result{};
        try {
            result = co_await _remote->download_stream(
              {
                .bucket = _bucket,
                .key = cloud_key(filename),
                .parent_rtc = root,
              },
              [this, &filepath](
                uint64_t content_length, ss::input_stream<char> stream) {
                  return save_locally(
                    content_length, std::move(stream), filepath);
              },
              "SST file download",
              /*acquire_hydration_units=*/true);
        } catch (const std::system_error& e) {
            throw io_error_exception(
              e.code(), "io error downloading file: {}", e);
        } catch (...) {
            auto ex = std::current_exception();
            if (ssx::is_shutdown_exception(ex)) {
                throw abort_requested_exception(
                  "shutdown exception while downloading file: {}", ex);
            }
            throw io_error_exception("io error downloading file: {}", ex);
        }
        if (check_result(result)) {
            co_return co_await open_local_reader(filepath);
        } else {
            co_return std::nullopt;
        }
    }

    ss::future<std::unique_ptr<sequential_file_writer>>
    open_sequential_writer(internal::file_handle h) override {
        _as.check();
        auto _ = _gate.hold();
        try {
            auto filename = internal::sst_file_name(h);
            auto filepath = staging_path(filename);
            auto file = ss::open_file_dma(
              filepath.native(),
              ss::open_flags::create | ss::open_flags::rw
                | ss::open_flags::truncate);
            auto stream = co_await ss::with_file_close_on_failure(
              std::move(file), [](ss::file f) {
                  return ss::make_file_output_stream(
                    std::move(f), ss::file_output_stream_options{});
              });
            co_return std::make_unique<staged_file_writer>(
              std::move(filepath),
              std::move(stream),
              _remote,
              &_as,
              _bucket,
              cloud_key(filename));
        } catch (const std::system_error& e) {
            throw io_error_exception(
              e.code(), "io error opening file writer: {}", e);
        } catch (...) {
            auto ex = std::current_exception();
            if (ssx::is_shutdown_exception(ex)) {
                throw abort_requested_exception(
                  "shutdown exception while opening file writer: {}", ex);
            }
            throw io_error_exception("io error opening file writer: {}", ex);
        }
    }

    ss::future<> remove_file(internal::file_handle h) override {
        _as.check();
        auto _ = _gate.hold();
        auto rtc = make_rtc(_as);
        auto filename = internal::sst_file_name(h);
        cloud_io::upload_result result{};
        try {
            co_await ss::remove_file(staging_path(filename).native());
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
        check_result(result);
    }

    ss::coroutine::experimental::generator<internal::file_handle>
    list_files() override {
        _as.check();
        auto _ = _gate.hold();
        auto rtc = make_rtc(_as);
        // TODO: Consider merging with on disk file listing.
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
    open_local_reader(std::filesystem::path filepath) {
        try {
            auto file = co_await ss::open_file_dma(
              filepath.native(), ss::open_flags::ro);
            std::unique_ptr<random_access_file_reader> ptr;
            ptr = std::make_unique<disk_file_reader>(
              std::move(filepath), std::move(file));
            co_return ptr;
        } catch (const std::system_error& e) {
            if (e.code() == std::errc::no_such_file_or_directory) {
                co_return std::nullopt;
            }
            throw io_error_exception(
              e.code(), "io error opening file reader: {}", e);
        } catch (...) {
            auto ex = std::current_exception();
            if (ssx::is_shutdown_exception(ex)) {
                throw abort_requested_exception(
                  "shutdown exception while listing files: {}", ex);
            }
            throw io_error_exception(
              "io error opening staging file reader: {}", ex);
        }
    }
    ss::future<uint64_t> save_locally(
      uint64_t content_length,
      ss::input_stream<char> input_stream,
      std::filesystem::path filepath) {
        auto temp_path = fmt::format(
          "{}.tmp.{}", filepath.native(), uuid_t::create());

        // Download to temp file so we can swap below.
        auto file = ss::open_file_dma(
          temp_path,
          ss::open_flags::create | ss::open_flags::rw
            | ss::open_flags::exclusive);
        auto output_stream
          = co_await ss::with_file_close_on_failure(
              std::move(file),
              [content_length](ss::file& f) {
                  return f.allocate(0, content_length)
                    .then([f = std::move(f)] mutable {
                        return ss::make_file_output_stream(
                          std::move(f), ss::file_output_stream_options{});
                    });
              })
              .then_wrapped(
                [&input_stream](ss::future<ss::output_stream<char>> fut) {
                    if (fut.failed()) {
                        // Ensure that we close the input stream.
                        return fut.finally(
                          [&input_stream] { return input_stream.close(); });
                    }
                    return fut;
                });

        co_await ss::copy(input_stream, output_stream)
          .finally([&output_stream] { return output_stream.close(); })
          .finally([&input_stream] { return input_stream.close(); });

        auto rename_fut = co_await ss::coroutine::as_future(
          ss::rename_file(temp_path, filepath.native()));
        if (rename_fut.failed()) {
            auto ex = rename_fut.get_exception();
            co_await ss::remove_file(temp_path).then_wrapped(
              [](ss::future<> fut) { fut.ignore_ready_future(); });
            throw io_error_exception(
              "Rename from {} to {} failed: {}",
              temp_path,
              filepath.native(),
              ex);
        }

        co_return content_length;
    }

    std::filesystem::path staging_path(std::string_view name) {
        return _staging / fmt::format("{}-{}", _staging_prefix, name);
    }

    cloud_storage_clients::object_key cloud_key(std::string_view name) {
        return join(_prefix, name);
    }

    std::filesystem::path _staging;
    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    cloud_storage_clients::object_key _prefix;
    ss::sstring _staging_prefix;
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
        auto rtc = make_rtc(_as);
        auto max_key = manifest_key(epoch);
        auto keys = co_await list_manifests();
        // list_manifests gives you biggest to smallest key, so find the first
        // key that is not greater than our max value passed in.
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
        co_return check_result(result) ? std::make_optional(std::move(b))
                                       : std::nullopt;
    }

    ss::future<>
    write_manifest(internal::database_epoch epoch, iobuf b) override {
        _as.check();
        auto _ = _gate.hold();
        auto rtc = make_rtc(_as);
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
        check_result(result);
        // Now cleanup old manifests
        chunked_vector<cloud_storage_clients::object_key> keys_to_delete;
        for (const auto& key : co_await list_manifests()) {
            if (key >= my_key) {
                continue;
            }
            keys_to_delete.push_back(key);
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
        auto rtc = make_rtc(_as);
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

ss::future<std::unique_ptr<data_persistence>> open_cloud_data_persistence(
  std::filesystem::path staging_directory,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix,
  ss::sstring staging_prefix) {
    try {
        co_await ss::recursive_touch_directory(staging_directory.native());
    } catch (const std::system_error& e) {
        throw io_error_exception(e.code(), "io error touching db dir: {}", e);
    } catch (...) {
        throw io_error_exception(
          "io error touching db dir: {}", std::current_exception());
    }
    co_return std::make_unique<cloud_data_persistence>(
      std::move(staging_directory),
      remote,
      std::move(bucket),
      std::move(prefix),
      std::move(staging_prefix));
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
