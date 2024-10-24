// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"
#include "base/units.h"
#include "bytes/iobuf.h"
#include "io/persistence.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>

#include <memory>

namespace experimental::io {
class page_cache;
class scheduler;
class pager;
} // namespace experimental::io

namespace storage::experimental::mvlog {

class file_manager;

// Wraps a pager, managing IO to a given file. This object is expected to live
// for the duration of the process, or until the underlying file is removed.
class file {
public:
    using file_t = ss::shared_ptr<::experimental::io::persistence::file>;
    file(file&) = delete;
    file(file&&) = delete;
    file& operator=(file&) = delete;
    file& operator=(file&&) = delete;
    ~file();

    // This file must outlive the returned streams.
    ss::input_stream<char> make_stream(uint64_t offset, uint64_t length);

    // Appends the given iobuf to the file.
    ss::future<> append(iobuf);

    // TODO(awong): implement.
    ss::future<> flush() { return ss::make_ready_future(); }

    // Stops remaing IO to the file and closes the handle.
    ss::future<> close();

    std::filesystem::path filepath() const { return path_; }
    size_t size() const;

private:
    friend class file_manager;
    file(
      std::filesystem::path,
      file_t,
      std::unique_ptr<::experimental::io::pager>);

    std::filesystem::path path_;
    file_t file_;
    std::unique_ptr<::experimental::io::pager> pager_;
};

// Simple wrapper around paging infrastructure, meant as a stop-gap until the
// io:: public interface is more fleshed out.
//
// TODO: This class doesn't implement any kind of concurrency control or file
// descriptor management. Callers should be wary about concurrent access to a
// single file.
class file_manager {
public:
    file_manager(file_manager&) = delete;
    file_manager(file_manager&&) = delete;
    file_manager& operator=(file_manager&) = delete;
    file_manager& operator=(file_manager&&) = delete;

    // NOTE: defaults are for tests only and are not tuned for production.
    explicit file_manager(
      size_t cache_size_bytes = 2_MiB,
      size_t small_queue_size_bytes = 1_MiB,
      size_t scheduler_num_files = 100);
    ~file_manager();

    ss::future<std::unique_ptr<file>> create_file(std::filesystem::path);
    ss::future<std::unique_ptr<file>> open_file(std::filesystem::path);
    ss::future<> remove_file(std::filesystem::path);

private:
    friend class file;

    // Persistence interface, in charge of accessing underlying files (opening,
    // appending, etc).
    std::unique_ptr<::experimental::io::persistence> storage_;

    // Page cache to buffer appends and cache recent pages.
    std::unique_ptr<::experimental::io::page_cache> cache_;

    // Scheduling policy used to dispatch IOs.
    std::unique_ptr<::experimental::io::scheduler> scheduler_;
};

} // namespace storage::experimental::mvlog
