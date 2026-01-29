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

// An abstraction for staging data before uploading to object storage.
class staging {
public:
    staging() = default;
    staging(const staging&) = delete;
    staging(staging&&) = delete;
    staging& operator=(const staging&) = delete;
    staging& operator=(staging&&) = delete;
    virtual ~staging() = default;

    // Return the size of staged data in bytes.
    virtual ss::future<size_t> size() = 0;
    // Return an output stream for appending to this staging area.
    virtual ss::future<ss::output_stream<char>> output_stream() = 0;
    // Clean up the staging area.
    //
    // This should always be called (outside of unclean shutdown).
    virtual ss::future<> remove() = 0;

private:
    friend class io;
    // Return an input stream for reading from this staging area.
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

    // Create a disk-based staging area for data to be uploaded to object
    // storage.
    //
    // If the operation writing to the staging area succeeds, then it should
    // be uploaded using `io::put_object`, then cleaned up using
    // `staging::remove()`. If there is an error then `staging::remove()`
    // should still be called to clean up resources.
    virtual ss::future<std::expected<std::unique_ptr<staging>, errc>>
    create_tmp_file() = 0;

    // Upload staged data to object storage.
    virtual ss::future<std::expected<void, errc>>
    put_object(object_id, staging*, ss::abort_source*) = 0;

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
    // A helper to read from a staging area.
    ss::future<ss::input_stream<char>> read_staging(staging*);
};

} // namespace cloud_topics::l1

template<>
struct fmt::formatter<cloud_topics::l1::io::errc>
  : fmt::formatter<std::string_view> {
    auto
    format(const cloud_topics::l1::io::errc&, fmt::format_context& ctx) const
      -> decltype(ctx.out());
};
