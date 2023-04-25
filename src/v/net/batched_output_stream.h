/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"
#include "ssx/semaphore.h"

#include <seastar/core/iostream.hh>

#include <cstddef>
#include <memory>

namespace net {

class batched_output_stream_closed : std::exception {
public:
    batched_output_stream_closed(size_t ignored_bytes)
      : msg(fmt::format(
        "Output stream closed (dropped {} bytes)", ignored_bytes)) {}

    const char* what() const noexcept final { return msg.c_str(); }

private:
    std::string msg;
};

/**
 * @brief Wrap a seastar output stream to provide batching of flush calls.
 *
 * When used for zero-copy operations, a seastar output stream does not send
 * data further downstream to the sink (i.e., the kernel) until flush() is
 * called. Rather than calling flush after every write (e.g., a full rpc request
 * payload), class attempts to reduce the number of flush calls by supressing
 * flushes when multiple writes are in progress on the stream: a flush occurs
 * only when the last pending writer completes or when a configured amount of
 * unflushed bytes have accumulated.
 */
class batched_output_stream {
public:
    static constexpr size_t default_max_unflushed_bytes = 1024 * 1024;

    batched_output_stream() = default;
    explicit batched_output_stream(
      ss::output_stream<char>, size_t cache = default_max_unflushed_bytes);
    ~batched_output_stream() noexcept = default;
    // NOTE: explicitly defined for a gcc
    batched_output_stream(batched_output_stream&& o) noexcept
      : _out(std::move(o._out))
      , _cache_size(o._cache_size)
      , _write_sem(std::move(o._write_sem))
      , _unflushed_bytes(o._unflushed_bytes)
      , _closed(o._closed) {}
    batched_output_stream& operator=(batched_output_stream&& o) noexcept {
        if (this != &o) {
            this->~batched_output_stream();
            new (this) batched_output_stream(std::move(o));
        }
        return *this;
    }
    batched_output_stream(const batched_output_stream&) = delete;
    batched_output_stream& operator=(const batched_output_stream&) = delete;

    /**
     * @brief Write the given payload to the underlying stream and maybe flush.
     *
     * Writes the scattered message to the underlying output stream, flushing if
     * this is the only (last) writer trying to write to this stream or if the
     * _cache_size has been reached.
     *
     * @param msg the message to write to the underlying stream
     * @return ss::future<bool> a future which resolves when the flush, if any,
     * completes with the wrapped value indicating wheter a flush occurred on
     * this write (true) or not (false)
     */
    ss::future<bool> write(ss::scattered_message<char> msg);
    ss::future<> flush();

    /// \brief calls output_stream<char>::close()
    /// do not use `_fd.shutdown_output();` on connected_sockets
    ss::future<> stop();

    bool is_valid() const noexcept { return _cache_size != 0; }

private:
    ss::future<> do_flush();

    ss::output_stream<char> _out;
    size_t _cache_size{0};
    std::unique_ptr<ssx::semaphore> _write_sem;
    size_t _unflushed_bytes{0};
    bool _closed = false;
};
} // namespace net
