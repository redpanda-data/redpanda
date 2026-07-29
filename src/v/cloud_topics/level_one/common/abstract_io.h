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
#include "cloud_io/admission_control_types.h"
#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "container/chunked_vector.h"
#include "model/record.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>

#include <expected>

namespace cloud_topics::l1 {

// Forward declaration — full definition in object_handle.h.
// Do not #include object_handle.h here: object_handle.h already includes
// abstract_io.h for io::errc, so including it here creates a cycle.
class object_handle;

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
    // Behind the scenes, there may or may not be caching going on. When
    // `skip_cache` is set the cache is still consulted first, but a miss is
    // streamed directly from object storage without being written to the cache.
    // This is intended for bulk, one-shot reads (e.g. leveling and compaction)
    // that would otherwise pollute the cache with data that won't be re-read.
    virtual ss::future<std::expected<ss::input_stream<char>, errc>> read_object(
      object_extent,
      ss::abort_source*,
      cloud_io::group_id g,
      bool skip_cache = false) = 0;

    // Read a native L1 object's footer region (the passed extent) fully
    // buffered into an `iobuf`, so open_object can parse the footer index.
    // Like read_object, may be served from or populate the cache unless
    // `skip_cache` is set.
    virtual ss::future<std::expected<iobuf, errc>> fetch_native_footer(
      object_extent,
      ss::abort_source*,
      cloud_io::group_id g,
      bool skip_cache = false);

    // Fetch the raw serialized offset index (.index sidecar) of an imported
    // tiered-storage segment. `extent.imported` identifies the segment. Returns
    // cloud_missing_object when the segment has no .index (the caller falls
    // back to a full-segment scan). The bytes are returned unparsed so this
    // thin io seam stays free of the cloud_storage offset-index format;
    // open_object deserializes them. Only meaningful for imported extents.
    virtual ss::future<std::expected<iobuf, errc>>
    fetch_ts_index(object_extent, ss::abort_source*) = 0;

    // Fetch the aborted-transaction ranges of an imported tiered-storage
    // segment (parsed from its .tx manifest sidecar), in raw log-offset space.
    // `extent.imported` identifies the segment. Returns cloud_missing_object
    // when the .tx object is absent. Ranges (rather than the manifest bytes)
    // are returned so the cloud_storage manifest format stays behind the io
    // seam; open_object assembles them into the aborted set. Only meaningful
    // for imported extents.
    virtual ss::future<std::expected<chunked_vector<model::tx_range>, errc>>
    fetch_ts_tx(object_extent, ss::abort_source*) = 0;

    // Delete the specified objects from object storage. An entry with a ts_path
    // is addressed by that tiered-storage segment path; otherwise the native L1
    // object path is used. Both live in the one configured object bucket.
    virtual ss::future<std::expected<void, errc>>
    delete_objects(chunked_vector<object_location>, ss::abort_source*) = 0;

    // Create a multipart upload for streaming data directly to object storage.
    virtual ss::future<
      std::expected<cloud_storage_clients::multipart_upload_ref, errc>>
    create_multipart_upload(object_id, size_t part_size, ss::abort_source*) = 0;

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
