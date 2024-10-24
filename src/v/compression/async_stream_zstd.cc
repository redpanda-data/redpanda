// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/async_stream_zstd.h"

#include "base/likely.h"
#include "base/units.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/details/io_allocation_size.h"

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
static constexpr int default_contexts = 2;

static thread_local std::unique_ptr<async_stream_zstd> zstd_instance;

void initialize_async_stream_zstd(size_t decompression_size) {
    if (zstd_instance) {
        vassert(
          decompression_size <= zstd_instance->decompression_size(),
          "async_stream_zstd initialized multiple times with "
          "decompression_size > zstd_instance->decompression_size()");

        return;
    }

    zstd_instance = std::make_unique<async_stream_zstd>(
      decompression_size, default_contexts);
}

async_stream_zstd& async_stream_zstd_instance() {
    if (unlikely(!zstd_instance)) {
        initialize_async_stream_zstd(default_decompression_size);
    }

    return *zstd_instance;
}

async_stream_zstd::static_cctx::static_cctx(int compression_level) {
    _c_buffer = ss::temporary_buffer<char>(ZSTD_CStreamOutSize());
    auto c_workspace_size = ZSTD_estimateCStreamSize(compression_level);
    // zstd requires alignment for the buffer below.
    _c_workspace = ss::allocate_aligned_buffer<char>(c_workspace_size, 8);

    vassert(
      _c_workspace,
      "Failed to allocate zstd workspace of size {}",
      c_workspace_size);

    _compress_ctx = ZSTD_initStaticCCtx(_c_workspace.get(), c_workspace_size);

    throw_if_error(ZSTD_CCtx_setParameter(
      _compress_ctx, ZSTD_c_compressionLevel, default_compression_level));
}

async_stream_zstd::static_cctx::static_cctx(static_cctx&& ctx) noexcept
  : _compress_ctx(ctx._compress_ctx)
  , _c_workspace(std::move(ctx._c_workspace))
  , _c_buffer(std::move(ctx._c_buffer)) {
    ctx._compress_ctx = nullptr;
}

async_stream_zstd::static_cctx&
async_stream_zstd::static_cctx::operator=(static_cctx&& x) noexcept {
    if (this != &x) {
        _compress_ctx = x._compress_ctx;
        _c_workspace = std::move(x._c_workspace);
        _c_buffer = std::move(x._c_buffer);

        x._compress_ctx = nullptr;
    }

    return *this;
}

async_stream_zstd::static_dctx::static_dctx(size_t decompression_size) {
    _d_buffer = ss::temporary_buffer<char>(ZSTD_DStreamOutSize());
    auto d_workspace_size = ZSTD_estimateDStreamSize(decompression_size);
    // zstd requires alignment for the buffer below.
    _d_workspace = ss::allocate_aligned_buffer<char>(d_workspace_size, 8);

    vassert(
      _d_workspace,
      "Failed to allocate zstd workspace of size {}",
      d_workspace_size);

    _decompress_ctx = ZSTD_initStaticDCtx(_d_workspace.get(), d_workspace_size);
}

async_stream_zstd::static_dctx::static_dctx(static_dctx&& ctx) noexcept
  : _decompress_ctx(ctx._decompress_ctx)
  , _d_workspace(std::move(ctx._d_workspace))
  , _d_buffer(std::move(ctx._d_buffer)) {
    ctx._decompress_ctx = nullptr;
}

async_stream_zstd::static_dctx&
async_stream_zstd::static_dctx::operator=(static_dctx&& x) noexcept {
    if (this != &x) {
        _decompress_ctx = x._decompress_ctx;
        _d_workspace = std::move(x._d_workspace);
        _d_buffer = std::move(x._d_buffer);

        x._decompress_ctx = nullptr;
    }

    return *this;
}

async_stream_zstd::async_stream_zstd(
  size_t decompression_size, int number_of_contexts) {
    _decompression_size = decompression_size;

    for (auto i = 0; i < number_of_contexts; i++) {
        _compression_ctx_pool.release_object(
          static_cctx(default_compression_level));
        _decompression_ctx_pool.release_object(static_dctx(decompression_size));
    }
}

size_t async_stream_zstd::decompression_size() const {
    return _decompression_size;
}

ss::future<iobuf> async_stream_zstd::compress(iobuf i_buf) {
    ss::abort_source as;
    auto ctx_scope = co_await _compression_ctx_pool.allocate_scoped_object(as);
    auto& ctx = *ctx_scope;

    // reset compressor ctx
    ZSTD_CCtx_reset(ctx._compress_ctx, ZSTD_reset_session_only);

    // NOTE: always enable content size. **decompression** depends on this
    throw_if_error(
      ZSTD_CCtx_setPledgedSrcSize(ctx._compress_ctx, i_buf.size_bytes()));

    iobuf ret_buf;

    // if the buffer is empty encode and return an empty zstd frame.
    if (i_buf.empty()) {
        size_t remaining = 0;

        do {
            ZSTD_outBuffer out = {
              .dst = ctx._c_buffer.get_write(),
              .size = ctx._c_buffer.size(),
              .pos = 0};
            remaining = ZSTD_endStream(ctx._compress_ctx, &out);
            throw_if_error(remaining);
            ret_buf.append(ctx._c_buffer.get(), out.pos);
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
              .dst = ctx._c_buffer.get_write(),
              .size = ctx._c_buffer.size(),
              .pos = 0};

            const auto remaining = ZSTD_compressStream2(
              ctx._compress_ctx, &out, &in, mode);
            throw_if_error(remaining);

            ret_buf.append(ctx._c_buffer.get(), out.pos);
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

    ss::abort_source as;
    auto ctx_scope = co_await _decompression_ctx_pool.allocate_scoped_object(
      as);
    auto& ctx = *ctx_scope;

    ZSTD_DCtx_reset(ctx._decompress_ctx, ZSTD_reset_session_only);

    iobuf ret_buf;
    size_t last_zstd_ret = 0;

    for (auto& ibuf : i_buf) {
        ZSTD_inBuffer in = {.src = ibuf.get(), .size = ibuf.size(), .pos = 0};

        while (in.pos < in.size) {
            ZSTD_outBuffer out = {
              .dst = ctx._d_buffer.get_write(),
              .size = ctx._d_buffer.size(),
              .pos = 0};

            const auto zstd_ret = ZSTD_decompressStream(
              ctx._decompress_ctx, &out, &in);
            throw_if_error(zstd_ret);

            ret_buf.append(ctx._d_buffer.get(), out.pos);
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
