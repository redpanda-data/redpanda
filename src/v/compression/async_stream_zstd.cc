// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/async_stream_zstd.h"

#include "bytes/bytes.h"
#include "bytes/details/io_allocation_size.h"
#include "compression/logger.h"
#include "likely.h"
#include "units.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <fmt/format.h>

#include <array>
#include <zstd.h>
#include <zstd_errors.h>

namespace compression {
[[gnu::cold]] static void throw_zstd_err(size_t rc) {
    if (rc == ZSTD_error_memory_allocation) {
        ss::throw_with_backtrace<std::bad_alloc>();
    } else {
        ss::throw_with_backtrace<std::runtime_error>(
          fmt::format("ZSTD error:{}", ZSTD_getErrorName(rc)));
    }
}

static void throw_if_error(size_t rc) {
    if (unlikely(ZSTD_isError(rc))) {
        throw_zstd_err(rc);
    }
}

static constexpr size_t default_decompression_size = 2_MiB;
static constexpr int default_compression_level = 3;

static thread_local std::unique_ptr<async_stream_zstd> zstd_instance;

void initialize_async_stream_zstd(size_t decompression_size) {
    if (zstd_instance) {
        vassert(
          decompression_size <= zstd_instance->decompression_size(),
          "async_stream_zstd initialized multiple times with "
          "decompression_size > zstd_instance->decompression_size()");

        return;
    }

    zstd_instance = std::make_unique<async_stream_zstd>(decompression_size);
}

async_stream_zstd& async_stream_zstd_instance() {
    if (unlikely(!zstd_instance)) {
        initialize_async_stream_zstd(default_decompression_size);
    }

    return *zstd_instance;
}

async_stream_zstd::async_stream_zstd(size_t decompression_size) {
    _decompression_size = decompression_size;

    _d_buffer = ss::temporary_buffer<char>(ZSTD_DStreamOutSize());
    _c_buffer = ss::temporary_buffer<char>(ZSTD_CStreamOutSize());

    auto d_workspace_size = ZSTD_estimateDStreamSize(decompression_size);
    auto c_workspace_size = ZSTD_estimateCStreamSize(default_compression_level);

    // zstd requires alignment on both of the buffer below.
    _d_workspace = ss::allocate_aligned_buffer<char>(d_workspace_size, 8);
    _c_workspace = ss::allocate_aligned_buffer<char>(c_workspace_size, 8);

    vassert(
      _c_workspace,
      "Failed to allocate zstd workspace of size {}",
      c_workspace_size);
    vassert(
      _d_workspace,
      "Failed to allocate zstd workspace of size {}",
      d_workspace_size);

    _compress_ctx = ZSTD_initStaticCCtx(_c_workspace.get(), c_workspace_size);
    _decompress_ctx = ZSTD_initStaticDCtx(_d_workspace.get(), d_workspace_size);

    throw_if_error(ZSTD_CCtx_setParameter(
      _compress_ctx, ZSTD_c_compressionLevel, default_compression_level));
}

size_t async_stream_zstd::decompression_size() const {
    return _decompression_size;
}

ss::future<iobuf> async_stream_zstd::compress(iobuf i_buf) {
    auto lock = co_await _compress_mtx.get_units();

    // reset compressor ctx
    ZSTD_CCtx_reset(_compress_ctx, ZSTD_reset_session_only);

    // NOTE: always enable content size. **decompression** depends on this
    throw_if_error(
      ZSTD_CCtx_setPledgedSrcSize(_compress_ctx, i_buf.size_bytes()));

    iobuf ret_buf;

    // if the buffer is empty encode and return an empty zstd frame.
    if (i_buf.empty()) {
        size_t remaining = 0;

        do {
            ZSTD_outBuffer out = {
              .dst = _c_buffer.get_write(), .size = _c_buffer.size(), .pos = 0};
            remaining = ZSTD_endStream(_compress_ctx, &out);
            throw_if_error(remaining);
            ret_buf.append(_c_buffer.get(), out.pos);
        } while (remaining > 0);

        co_return std::move(ret_buf);
    }

    // otherwise compress and return the buffer.
    for (auto it = i_buf.cbegin(); it != i_buf.cend();) {
        ZSTD_inBuffer in = {.src = it->get(), .size = it->size(), .pos = 0};
        ++it;

        const auto mode = (it == i_buf.cend()) ? ZSTD_e_end : ZSTD_e_continue;

        int finished;
        do {
            ZSTD_outBuffer out = {
              .dst = _c_buffer.get_write(), .size = _c_buffer.size(), .pos = 0};

            const auto remaining = ZSTD_compressStream2(
              _compress_ctx, &out, &in, mode);
            throw_if_error(remaining);

            ret_buf.append(_c_buffer.get(), out.pos);
            finished = (mode == ZSTD_e_end) ? (remaining == 0)
                                            : (in.pos == in.size);

            co_await ss::coroutine::maybe_yield();
        } while (!finished);
    }

    co_return ret_buf;
}

ss::future<iobuf> async_stream_zstd::uncompress(iobuf i_buf) {
    if (unlikely(i_buf.empty())) {
        throw std::runtime_error(
          "Asked to stream_zstd::uncompress empty buffer");
    }

    auto lock = co_await _decompress_mtx.get_units();

    ZSTD_DCtx_reset(_decompress_ctx, ZSTD_reset_session_only);

    iobuf ret_buf;
    size_t last_zstd_ret = 0;

    for (auto& ibuf : i_buf) {
        ZSTD_inBuffer in = {.src = ibuf.get(), .size = ibuf.size(), .pos = 0};

        while (in.pos < in.size) {
            ZSTD_outBuffer out = {
              .dst = _d_buffer.get_write(), .size = _d_buffer.size(), .pos = 0};

            const auto zstd_ret = ZSTD_decompressStream(
              _decompress_ctx, &out, &in);
            throw_if_error(zstd_ret);

            ret_buf.append(_d_buffer.get(), out.pos);
            last_zstd_ret = zstd_ret;

            co_await ss::coroutine::maybe_yield();
        }
    }

    if (last_zstd_ret != 0) {
        throw std::runtime_error("Input truncated before reading epilog");
    }

    co_return ret_buf;
}

} // namespace compression
