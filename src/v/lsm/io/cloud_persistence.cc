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
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/io/cloud_data_persistence_base.h"
#include "lsm/io/file_io.h"
#include "lsm/io/persistence.h"
#include "ssx/future-util.h"
#include "utils/uuid.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/coroutine/as_future.hh>

#include <algorithm>
#include <exception>
#include <memory>

namespace lsm::io {

namespace {

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
        auto upload_fut = co_await ss::coroutine::as_future(
          upload_file(*_remote, *_as, _bucket, _key, path(), _written));
        if (upload_fut.failed()) {
            auto ex = upload_fut.get_exception();
            co_await ss::remove_file(path().native())
              .then_wrapped(
                [](ss::future<> fut) { fut.ignore_ready_future(); });
            throw_as_lsm_ex(ex, "io error closing");
        }
    }

    fmt::iterator format_to(fmt::iterator it) const override {
        it = fmt::format_to(it, "{{staging=");
        it = disk_seq_file_writer::format_to(it);
        return fmt::format_to(it, ", upload={}, written={}}}", _key, _written);
    }

private:
    size_t _written = 0;
    cloud_io::remote* _remote;
    ss::abort_source* _as;
    cloud_storage_clients::bucket_name _bucket;
    cloud_storage_clients::object_key _key;
};

class cloud_data_persistence : public cloud_data_persistence_base {
public:
    cloud_data_persistence(
      std::filesystem::path staging,
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage_clients::object_key prefix,
      ss::sstring staging_prefix)
      : cloud_data_persistence_base(
          remote, std::move(bucket), std::move(prefix))
      , _staging(std::move(staging))
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
        auto root = make_cloud_rtc(_as);
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
        if (check_cloud_result(result)) {
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
                | ss::open_flags::exclusive);
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

    ss::future<> remove_file_locally(std::string_view filename) override {
        co_await ss::remove_file(staging_path(filename).native());
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

    std::filesystem::path _staging;
    ss::sstring _staging_prefix;
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
