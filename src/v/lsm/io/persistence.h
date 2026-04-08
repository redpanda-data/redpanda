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

#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "bytes/ioarray.h"
#include "bytes/iobuf.h"
#include "lsm/core/internal/files.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/generator.hh>
#include <seastar/util/optimized_optional.hh>

namespace lsm::io {

// A file abstraction for randomly reading the contents of a file.
class random_access_file_reader {
public:
    random_access_file_reader() = default;
    random_access_file_reader(const random_access_file_reader&) = delete;
    random_access_file_reader&
    operator=(const random_access_file_reader&) = delete;
    random_access_file_reader(random_access_file_reader&&) = delete;
    random_access_file_reader& operator=(random_access_file_reader&&) = delete;
    virtual ~random_access_file_reader() = default;

    // Read up to "n" bytes from the file starting at "offset".
    //
    // It's not valid to read outside the bounds of the file.
    virtual ss::future<ioarray> read(size_t offset, size_t n) = 0;

    // Closes the file, must always be called, even if `read` or `skip` return
    // an error future.
    virtual ss::future<> close() = 0;

    // Print debug information about the file
    virtual fmt::iterator format_to(fmt::iterator) const = 0;
};

// A file abstraction for sequential writing. The implementation must provide
// buffering since callers may append small fragments at a time to the file.
class sequential_file_writer {
public:
    sequential_file_writer() = default;
    sequential_file_writer(const sequential_file_writer&) = delete;
    sequential_file_writer& operator=(const sequential_file_writer&) = delete;
    sequential_file_writer(sequential_file_writer&&) = delete;
    sequential_file_writer& operator=(sequential_file_writer&&) = delete;
    virtual ~sequential_file_writer() = default;

    // Append the iobuf to the file.
    virtual ss::future<> append(iobuf) = 0;

    // Close the file, this ensures the file is properly written to persistent
    // storage (ie. flushed and fsync'd, etc).
    virtual ss::future<> close() = 0;

    // Print debug information about the file
    virtual fmt::iterator format_to(fmt::iterator) const = 0;
};

// The persistence layer for storing the manifest files describing the LSM tree
// structure.
//
// This interface requires that all operations are ATOMIC.
class metadata_persistence {
public:
    metadata_persistence() = default;
    metadata_persistence(const metadata_persistence&) = delete;
    metadata_persistence& operator=(const metadata_persistence&) = delete;
    metadata_persistence(metadata_persistence&&) = delete;
    metadata_persistence& operator=(metadata_persistence&&) = delete;
    virtual ~metadata_persistence() = default;

    // Read the latest written manifest before the given epoch if one exists.
    virtual ss::future<std::optional<iobuf>>
      read_manifest(internal::database_epoch) = 0;

    // Write the manifest atomically to durable storage at the given epoch.
    virtual ss::future<> write_manifest(internal::database_epoch, iobuf) = 0;

    // Closes the persistence layer.
    virtual ss::future<> close() = 0;
};

template<typename T>
using optional_pointer = ss::optimized_optional<std::unique_ptr<T>>;

// The persistence layer for long term data file storage. This persistence only
// is used for storing SST files.
//
// There is no concept of a "directory". All files are stored in a flat
// key/value namespace such that the `/` character is never used in a file name.
class data_persistence {
public:
    data_persistence() = default;
    data_persistence(const data_persistence&) = delete;
    data_persistence& operator=(const data_persistence&) = delete;
    data_persistence(data_persistence&&) = delete;
    data_persistence& operator=(data_persistence&&) = delete;
    virtual ~data_persistence() = default;

    // Create a reader that supports random access reads the file with the
    // specified name.
    //
    // If the file does not exist then the implementation should return
    // `std::nullopt`.
    virtual ss::future<optional_pointer<random_access_file_reader>>
      open_random_access_reader(internal::file_handle) = 0;

    // Create a writer that writes to a new file with the specified name.
    //
    // Deletes any existing file with the same name and creates a new file.
    virtual ss::future<std::unique_ptr<sequential_file_writer>>
      open_sequential_writer(internal::file_handle) = 0;

    // Remove a file. This is a noop if the file does not exist.
    virtual ss::future<> remove_file(internal::file_handle) = 0;

    // List all the files in the persistence layer.
    virtual ss::coroutine::experimental::generator<internal::file_handle>
    list_files() = 0;

    // Closes the persistence layer, this must happen after all files are
    // closed.
    virtual ss::future<> close() = 0;
};

// A holder struct for the persistence layer in an LSM database.
struct persistence {
    std::unique_ptr<data_persistence> data;
    std::unique_ptr<metadata_persistence> metadata;
};

} // namespace lsm::io
