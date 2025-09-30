/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "bytes/iobuf.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "container/chunked_vector.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>

#include <expected>

namespace cloud_topics::l1 {

// An abstraction for a local file that is used for staging uploads to object
// storage.
class staging_file {
public:
    staging_file() = default;
    staging_file(const staging_file&) = delete;
    staging_file(staging_file&&) = delete;
    staging_file& operator=(const staging_file&) = delete;
    staging_file& operator=(staging_file&&) = delete;
    virtual ~staging_file() = default;

    // Return the size of this file in bytes on disk.
    virtual ss::future<size_t> size() = 0;
    // Return an output stream for appending to this file.
    virtual ss::future<ss::output_stream<char>> output_stream() = 0;
    // Remove the file from the local filesystem.
    //
    // This should always be called (outside of unclean shutdown).
    virtual ss::future<> remove() = 0;

private:
    friend class io;
    // Return an input stream for reading from this file.
    virtual ss::future<ss::input_stream<char>> input_stream() = 0;
};

// An abstraction for IO in level one.
class io {
public:
    enum class errc : uint8_t {
        file_io_error,
        cloud_missing_object, // when reading something does not exist
        cloud_op_error,
        cloud_op_timeout,
    };
    io() = default;
    io(const io&) = delete;
    io(io&&) = delete;
    io& operator=(const io&) = delete;
    io& operator=(io&&) = delete;
    virtual ~io() = default;

    // Create a temporary file for staging uploads into object storage.
    //
    // If the operation writing to the file succeeds, then the local file should
    // be uploaded using `io::upload_file`, then deleted using
    // `local_file::remove()`. If there is an error than `local_file::remove()`
    // should still be called to clean up the temporary file.
    virtual ss::future<std::expected<std::unique_ptr<staging_file>, errc>>
    create_tmp_file() = 0;

    // Upload a local file to object storage, returning the object ID that was
    // used to identify the object in the bucket.
    virtual ss::future<std::expected<void, errc>>
    put_object(object_id, staging_file*, ss::abort_source*) = 0;

    // Read part of an object from object storage, returning an input stream
    // representing the object data.
    //
    // Behind the scenes, there may or may not be caching going on.
    virtual ss::future<std::expected<ss::input_stream<char>, errc>>
    read_object(object_extent, ss::abort_source*) = 0;

    // The same as `read_object` except that instead of returning an input
    // stream, the data is fully buffered into an `iobuf`.
    virtual ss::future<std::expected<iobuf, errc>>
    read_object_as_iobuf(object_extent, ss::abort_source*);

    // Delete the specified objects from object storage.
    virtual ss::future<std::expected<void, errc>>
    delete_objects(chunked_vector<object_id>, ss::abort_source*) = 0;

protected:
    // A helper to read a staging file.
    ss::future<ss::input_stream<char>> read_file(staging_file*);
};

} // namespace cloud_topics::l1

template<>
struct fmt::formatter<cloud_topics::l1::io::errc>
  : fmt::formatter<std::string_view> {
    auto
    format(const cloud_topics::l1::io::errc&, fmt::format_context& ctx) const
      -> decltype(ctx.out());
};
