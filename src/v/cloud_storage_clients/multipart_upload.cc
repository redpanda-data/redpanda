/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/multipart_upload.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_storage_clients {

/// Data sink implementation that delegates to multipart_upload::put()
///
/// This allows using multipart_upload via the ss::output_stream interface.
class multipart_data_sink_impl final : public ss::data_sink_impl {
public:
    explicit multipart_data_sink_impl(multipart_upload_ref upload)
      : _upload(std::move(upload)) {}

    ss::future<> put(ss::temporary_buffer<char> buf) final {
        iobuf data;
        data.append(std::move(buf));
        return _upload->put(std::move(data));
    }

    ss::future<> put(ss::net::packet data) final {
        auto buffers = data.release();
        iobuf iobuf_data;
        for (auto& buf : buffers) {
            iobuf_data.append(std::move(buf));
        }
        return _upload->put(std::move(iobuf_data));
    }

    ss::future<> put(std::vector<ss::temporary_buffer<char>> all) final {
        iobuf data;
        for (auto& buf : all) {
            data.append(std::move(buf));
        }
        return _upload->put(std::move(data));
    }

    ss::future<> flush() final {
        // Flushing is handled automatically by put() when buffer fills
        return ss::now();
    }

    ss::future<> close() final {
        // Closing the stream completes the multipart upload
        return _upload->complete();
    }

private:
    multipart_upload_ref _upload;
};

multipart_upload::multipart_upload(
  ss::shared_ptr<multipart_upload_state> state,
  size_t part_size,
  ss::logger& logger)
  : _state(std::move(state))
  , _part_size(part_size)
  , _logger(logger) {}

multipart_upload::~multipart_upload() {
    vassert(
      _finalized || !_multipart_initialized,
      "multipart_upload destroyed without calling complete() or abort(). "
      "upload_id: {}, part_number: {}",
      _state->upload_id(),
      _part_number);
}

ss::future<> multipart_upload::put(iobuf data) {
    if (_finalized) {
        // May occur if a multipart upload is aborted with pending data and then
        // its stream is closed.
        co_return;
    }

    // Append data to buffer
    if (_buffer.empty()) {
        _buffer = std::move(data);
    } else {
        _buffer.append(std::move(data));
    }

    // Upload full parts while we have enough data
    while (_buffer.size_bytes() >= _part_size) {
        // Extract one part worth of data
        auto part_data = _buffer.share(0, _part_size);
        _buffer.trim_front(_part_size);

        // Lazy initialization: only initialize on first part upload
        if (!_multipart_initialized) {
            vlog(
              _logger.debug,
              "Initializing multipart upload for first part (size: {})",
              _part_size);
            co_await _state->initialize_multipart();
            _multipart_initialized = true;
        }

        vlog(
          _logger.debug,
          "Uploading part {} (size: {})",
          _part_number,
          part_data.size_bytes());
        co_await _state->upload_part(_part_number++, std::move(part_data));
    }
}

ss::future<> multipart_upload::complete() {
    if (_finalized) {
        co_return;
    }

    _finalized = true;

    // Small file optimization: if we haven't initialized multipart yet and
    // the total data is less than part_size, use regular put_object instead
    if (!_multipart_initialized) {
        vlog(
          _logger.debug,
          "Small file optimization: using single put_object (size: {})",
          _buffer.size_bytes());
        co_await _state->upload_as_single_object(std::move(_buffer));
        co_return;
    }

    // Upload final part if any data remains
    if (_buffer.size_bytes() > 0) {
        vlog(
          _logger.debug,
          "Uploading final part {} (size: {})",
          _part_number,
          _buffer.size_bytes());
        auto fut = co_await ss::coroutine::as_future(
          _state->upload_part(_part_number++, std::move(_buffer)));
        if (fut.failed()) {
            auto ex = fut.get_exception();
            vlogl(
              _logger,
              ssx::is_shutdown_exception(ex) ? ss::log_level::warn
                                             : ss::log_level::error,
              "Multipart upload final part failed, aborting: {}",
              ex);
            co_await abort_on_error();
            std::rethrow_exception(ex);
        }
    }
    // Complete the multipart upload
    vlog(
      _logger.debug,
      "Completing multipart upload ({} parts)",
      _part_number - 1);
    auto fut = co_await ss::coroutine::as_future(
      _state->complete_multipart_upload());
    if (fut.failed()) {
        auto ex = fut.get_exception();
        vlogl(
          _logger,
          ssx::is_shutdown_exception(ex) ? ss::log_level::warn
                                         : ss::log_level::error,
          "Multipart upload completion failed, aborting: {}",
          ex);
        co_await abort_on_error();
        std::rethrow_exception(ex);
    }
}

ss::future<> multipart_upload::abort() {
    if (_finalized) {
        // Already finalized - this is idempotent
        vlog(_logger.debug, "abort() called on already finalized upload");
        co_return;
    }

    _finalized = true;

    if (!_multipart_initialized) {
        co_return;
    }

    vlog(_logger.debug, "Aborting multipart upload");
    try {
        co_await _state->abort_multipart_upload();
    } catch (...) {
        // Log but don't propagate abort failures - we're already aborting
        vlog(
          _logger.warn,
          "Abort multipart upload failed: {}",
          std::current_exception());
    }
}

ss::future<> multipart_upload::abort_on_error() {
    auto fut = co_await ss::coroutine::as_future(
      _state->abort_multipart_upload());
    if (fut.failed()) {
        vlog(
          _logger.warn,
          "Failed to abort multipart upload after completion failure: {}",
          fut.get_exception());
    }
}

ss::output_stream<char> multipart_upload::as_stream() {
    static constexpr size_t buf_size = 8192;
    auto sink = ss::data_sink(
      std::make_unique<multipart_data_sink_impl>(shared_from_this()));
    return {std::move(sink), buf_size};
}

} // namespace cloud_storage_clients
