/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "lsm/io/disk_persistence.h"

#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/io/file_io.h"
#include "lsm/io/persistence.h"
#include "utils/file_io.h"
#include "utils/uuid.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/coroutine/as_future.hh>

#include <exception>
#include <system_error>

namespace lsm::io {

namespace {

class impl
  : public data_persistence
  , public metadata_persistence {
public:
    explicit impl(std::filesystem::path root)
      : _root(std::move(root)) {}
    impl(const impl&) = delete;
    impl(impl&&) = delete;
    impl& operator=(const impl&) = delete;
    impl& operator=(impl&&) = delete;
    ~impl() override = default;

    ss::future<optional_pointer<random_access_file_reader>>
    open_random_access_reader(internal::file_handle h) override {
        try {
            auto filepath = path(internal::sst_file_name(h));
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
            throw io_error_exception(
              "io error opening file reader: {}", std::current_exception());
        }
    }

    ss::future<std::unique_ptr<sequential_file_writer>>
    open_sequential_writer(internal::file_handle h) override {
        try {
            auto filepath = path(internal::sst_file_name(h));
            auto file = ss::open_file_dma(
              filepath.native(),
              ss::open_flags::create | ss::open_flags::rw
                | ss::open_flags::truncate);
            auto stream = co_await ss::with_file_close_on_failure(
              std::move(file), [](ss::file& f) {
                  return ss::make_file_output_stream(
                    std::move(f), ss::file_output_stream_options{});
              });
            co_return std::make_unique<disk_seq_file_writer>(
              std::move(filepath), std::move(stream));
        } catch (const std::system_error& e) {
            throw io_error_exception(
              e.code(), "io error opening file writer: {}", e);
        } catch (...) {
            throw io_error_exception(
              "io error opening file writer: {}", std::current_exception());
        }
    }

    ss::future<std::optional<iobuf>>
    read_manifest(internal::database_epoch epoch) override {
        auto latest_filename = fmt::format("{:020}.MANIFEST", epoch);
        std::string filename;
        auto generator = list_all_files();
        while (auto entry_opt = co_await generator()) {
            auto& entry = entry_opt->get();
            if (!entry.name.ends_with(".MANIFEST")) {
                continue;
            }
            // Skip newer files
            if (entry.name > latest_filename) {
                continue;
            }
            if (entry.name > filename) {
                filename = entry.name;
            }
        }
        if (filename.empty()) {
            co_return std::nullopt;
        }
        try {
            co_return co_await read_fully(path(filename).native());
        } catch (const std::system_error& e) {
            if (e.code() != std::errc::no_such_file_or_directory) {
                throw io_error_exception(
                  e.code(), "io error reading manifest: {}", e);
            }
            co_return std::nullopt;
        } catch (...) {
            throw io_error_exception(
              "io error reading manifest: {}", std::current_exception());
        }
    }

    ss::future<>
    write_manifest(internal::database_epoch epoch, iobuf b) override {
        auto filename = fmt::format("{:020}.MANIFEST", epoch);
        auto staging_name = fmt::format(
          "{}.lsm-staging", filename, uuid_t::create());
        try {
            co_await write_fully(path(staging_name), std::move(b));
            co_await ss::rename_file(
              path(staging_name).native(), (_root / filename).native());
            co_await ss::sync_directory(_root.native());
        } catch (const std::system_error& e) {
            throw io_error_exception(
              e.code(),
              "io error writing manifest: {}",
              std::current_exception());
        } catch (...) {
            throw io_error_exception(
              "io error writing manifest: {}", std::current_exception());
        }
        // Clean up old manifest files
        auto generator = list_all_files();
        while (auto entry_opt = co_await generator()) {
            auto& entry = entry_opt->get();
            if (!entry.name.ends_with(".MANIFEST")) {
                continue;
            }
            // They are lexicographically ordered, so we can check
            // if it's older via string compare.
            if (entry.name < filename) {
                co_await remove_any_file(entry.name);
            }
        }
    }

    ss::future<> remove_file(internal::file_handle handle) override {
        return remove_any_file(internal::sst_file_name(handle));
    }

    ss::coroutine::experimental::generator<internal::file_handle>
    list_files() override {
        auto generator = list_all_files();
        while (auto entry = co_await generator()) {
            auto maybe_id = internal::parse_sst_file_name(entry->get().name);
            if (maybe_id) {
                co_yield *maybe_id;
            }
        }
    }

    ss::future<> close() override { co_return; }

private:
    ss::future<> remove_any_file(std::string_view filename) {
        try {
            co_await ss::remove_file(path(filename).native());
        } catch (const std::system_error& e) {
            if (e.code() != std::errc::no_such_file_or_directory) {
                throw io_error_exception(
                  e.code(), "io error removing file: {}", e);
            }
        } catch (...) {
            throw io_error_exception(
              "io error removing file: {}", std::current_exception());
        }
    }

    ss::coroutine::experimental::generator<ss::directory_entry>
    list_all_files() {
        ss::file dir;
        std::exception_ptr ep;
        try {
            dir = co_await ss::open_directory(_root.native());
            auto generator = dir.experimental_list_directory();
            while (auto entry = co_await generator()) {
                co_yield *entry;
            }
        } catch (const std::system_error& e) {
            ep = std::make_exception_ptr(
              io_error_exception(e.code(), "io error listing files: {}", e));
        } catch (...) {
            ep = std::make_exception_ptr(io_error_exception(
              "io error listing files: {}", std::current_exception()));
        }
        if (dir) {
            co_await dir.close();
        }
        if (ep) {
            std::rethrow_exception(ep);
        }
    }

    std::filesystem::path path(std::string_view name) const {
        return _root / name;
    }

    std::filesystem::path _root;
};

ss::future<> cleanup_staging_files(ss::file& dir) {
    auto generator = dir.experimental_list_directory();
    while (auto entry_opt = co_await generator()) {
        auto& entry = *entry_opt;
        if (entry.name.ends_with(".lsm-staging")) {
            co_await ss::remove_file(entry.name);
        }
    }
}

} // namespace

ss::future<std::unique_ptr<data_persistence>>
open_disk_data_persistence(std::filesystem::path directory) {
    try {
        co_await ss::recursive_touch_directory(directory.native());
    } catch (const std::system_error& e) {
        throw io_error_exception(e.code(), "io error touching db dir: {}", e);
    } catch (...) {
        throw io_error_exception(
          "io error touching db dir: {}", std::current_exception());
    }
    co_return std::make_unique<impl>(std::move(directory));
}

ss::future<std::unique_ptr<metadata_persistence>>
open_disk_metadata_persistence(std::filesystem::path directory) {
    try {
        co_await ss::recursive_touch_directory(directory.native());
    } catch (const std::system_error& e) {
        throw io_error_exception(e.code(), "io error touching db dir: {}", e);
    } catch (...) {
        throw io_error_exception(
          "io error touching db dir: {}", std::current_exception());
    }
    try {
        auto dir = co_await ss::open_directory(directory.native());
        co_await cleanup_staging_files(dir).finally(
          [&dir] { return dir.close(); });
    } catch (const std::system_error& e) {
        throw io_error_exception(e.code(), "io error listing files: {}", e);
    } catch (...) {
        throw io_error_exception(
          "io error listing files: {}", std::current_exception());
    }
    co_return std::make_unique<impl>(std::move(directory));
}

} // namespace lsm::io
