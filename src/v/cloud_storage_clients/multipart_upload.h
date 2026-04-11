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

#include "base/seastarx.h"
#include "base/units.h"
#include "bytes/iobuf.h"
#include "cloud_storage_clients/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include <memory>

namespace cloud_storage_clients {

// Forward declarations
class multipart_upload;
class multipart_upload_state;

using multipart_upload_ref = ss::shared_ptr<multipart_upload>;

/// Part size constants for different backends
namespace multipart_limits {
/// S3/GCS minimum part size (except last part which has no minimum)
constexpr size_t min_s3_part_size = 5_MiB;
/// S3/GCS maximum part size
constexpr size_t max_s3_part_size = 5_GiB;
/// Azure Blob Storage maximum block size (version 2019-12-12+)
constexpr size_t max_abs_block_size = 4000_MiB;
/// Maximum number of parts/blocks across all backends
constexpr size_t max_parts = 10'000;
} // namespace multipart_limits

/// Abstract interface for backend-specific multipart upload state
///
/// Each cloud provider (S3, ABS) implements this interface to handle
/// the specific API calls required for multipart uploads.
class multipart_upload_state {
public:
    virtual ~multipart_upload_state() = default;

    /// Initialize the multipart upload (lazy initialization)
    ///
    /// For S3: Calls CreateMultipartUpload to get upload_id
    /// For ABS: No-op (blocks don't require initialization)
    virtual ss::future<> initialize_multipart() = 0;

    /// Upload a single part
    ///
    /// For S3: Calls UploadPart with part number and upload_id
    /// For ABS: Calls Put Block with Base64 block ID
    ///
    /// \param part_num 1-based part number
    /// \param data Part data to upload
    virtual ss::future<> upload_part(size_t part_num, iobuf data) = 0;

    /// Complete the multipart upload (commit)
    ///
    /// For S3: Calls CompleteMultipartUpload with list of part ETags
    /// For ABS: Calls Put Block List with block IDs
    virtual ss::future<> complete_multipart_upload() = 0;

    /// Abort the multipart upload (cancel and cleanup)
    ///
    /// For S3: Calls AbortMultipartUpload
    /// For ABS: No-op (uncommitted blocks expire after 7 days)
    virtual ss::future<> abort_multipart_upload() = 0;

    /// Fallback for small files - use regular put_object instead of multipart
    ///
    /// \param data Complete file data
    virtual ss::future<> upload_as_single_object(iobuf data) = 0;

    /// Check if multipart upload was initialized
    virtual bool is_multipart_initialized() const = 0;

    /// Get upload ID for debugging (S3 only, empty for ABS)
    virtual ss::sstring upload_id() const = 0;
};

/// Multipart upload handle with put/complete/abort API
///
/// This class manages buffering and automatic part uploads for multipart
/// uploads to cloud storage. It provides both a direct API (put/complete/abort)
/// and a streaming API via as_stream().
///
/// Usage:
///   auto upload = co_await client.initiate_multipart_upload(...);
///   co_await upload->put(data1);
///   co_await upload->put(data2);
///   co_await upload->complete();  // or abort()
///
/// Or with streaming API:
///   auto stream = upload->as_stream();
///   co_await stream.write(data);
///   co_await stream.close();  // calls complete()
///   // if there is a failure you can still call `upload->abort();`
class multipart_upload : public ss::enable_shared_from_this<multipart_upload> {
public:
    // The minimum part size required by S3 (which is the known minimum size
    // across providers).
    static constexpr size_t min_part_size = 5_MiB;

    /// Construct a multipart upload
    ///
    /// \param state Backend-specific state implementation
    /// \param part_size Size of each part in bytes
    /// \param logger Logger to use for diagnostics
    explicit multipart_upload(
      ss::shared_ptr<multipart_upload_state> state,
      size_t part_size,
      ss::logger& logger);

    ~multipart_upload() override;

    // Non-copyable, non-movable
    multipart_upload(const multipart_upload&) = delete;
    multipart_upload& operator=(const multipart_upload&) = delete;
    multipart_upload(multipart_upload&&) = delete;
    multipart_upload& operator=(multipart_upload&&) = delete;

    /// Write data to the upload
    ///
    /// Data is buffered and parts are automatically uploaded when the buffer
    /// reaches part_size. This method may upload multiple parts if data size
    /// exceeds part_size.
    ///
    /// No-op if the upload has already been finalized. This can occur when an
    /// upload is aborted with pending data and its stream is subsequently
    /// closed.
    ///
    /// \param data Data to write
    [[nodiscard]] virtual ss::future<> put(iobuf data);

    /// Complete the multipart upload (commit)
    ///
    /// Uploads any remaining buffered data as the final part and commits the
    /// multipart upload.
    ///
    /// Small file optimization: If less than part_size bytes were written and
    /// no parts have been uploaded yet, uses regular put_object instead of
    /// multipart upload for efficiency.
    ///
    /// No-op if the upload has already been finalized.
    [[nodiscard]] virtual ss::future<> complete();

    /// Abort the multipart upload (cancel without committing)
    ///
    /// Cancels the multipart upload and attempts to clean up any uploaded
    /// parts. No-op if the upload has already been finalized.
    [[nodiscard]] virtual ss::future<> abort();

    /// Check if the upload has been finalized (completed or aborted)
    virtual bool is_finalized() const noexcept { return _finalized; }

    /// Get a streaming interface to this upload
    ///
    /// Returns an ss::output_stream that wraps the put() and complete()
    /// methods. Writing to the stream calls put(), and closing the stream
    /// calls complete(). abort() can still be called on the parent object
    /// even when using the stream.
    ///
    /// \return Output stream for writing data
    [[nodiscard]] ss::output_stream<char> as_stream();

private:
    /// Best-effort abort after a failure during complete()
    ss::future<> abort_on_error();

    ss::shared_ptr<multipart_upload_state> _state;
    size_t _part_size;
    ss::logger& _logger;
    iobuf _buffer;
    size_t _part_number{1};
    bool _multipart_initialized{false};
    bool _finalized{false};
};

} // namespace cloud_storage_clients
