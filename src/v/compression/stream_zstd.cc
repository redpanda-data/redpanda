// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/stream_zstd.h"

#include "base/likely.h"
#include "base/units.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/details/io_allocation_size.h"

#include <seastar/core/aligned_buffer.hh>

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

// workspace and decompression buffer
static thread_local size_t dctx_workspace_size = 0;
static thread_local std::unique_ptr<char[], ss::free_deleter> dctx_workspace;
static thread_local ss::temporary_buffer<char> d_buffer;

void stream_zstd::init_workspace(size_t size) {
    if (!dctx_workspace) {
        dctx_workspace_size = ZSTD_estimateDStreamSize(size);
        dctx_workspace = ss::allocate_aligned_buffer<char>(
          dctx_workspace_size, 8); // zstd requires alignment
        vassert(
          dctx_workspace,
          "Failed to allocate zstd workspace with {} bytes",
          dctx_workspace_size);
        d_buffer = ss::temporary_buffer<char>(64_KiB);
    }
}

void stream_zstd::reset_compressor() {
    _compress.reset(ZSTD_createCCtx());
    if (!_compress) {
        throw std::bad_alloc{};
    }
}
stream_zstd::zstd_compress_ctx& stream_zstd::compressor() {
    if (!_compress) {
        reset_compressor();
    }
    return _compress;
}

ZSTD_DCtx* stream_zstd::decompressor() {
    if (unlikely(!dctx_workspace)) {
        /*
         * init_workspace is normally called early during startup before
         * fragmentation is a problem and the workspace created is usually
         * larger. but we also handle it here for things like tests that don't
         * exercise that startup code path.
         */
        init_workspace(2_MiB);
    }
    auto ctx = ZSTD_initStaticDCtx(dctx_workspace.get(), dctx_workspace_size);
    vassert(ctx, "Could not initialize static decompression context");
    return ctx;
}

iobuf stream_zstd::do_compress(const iobuf& x) {
    reset_compressor();
    ZSTD_CCtx* ctx = compressor().get();
    // NOTE: always enable content size. **decompression** depends on this
    throw_if_error(ZSTD_CCtx_setPledgedSrcSize(ctx, x.size_bytes()));

    const size_t max_chunk_size = details::io_allocation_size::max_chunk_size;
    auto output_chunk_size = std::min(
      max_chunk_size, ZSTD_compressBound(x.size_bytes()));
    ss::temporary_buffer<char> obuf(output_chunk_size);
    ZSTD_outBuffer out = {
      .dst = obuf.get_write(), .size = obuf.size(), .pos = 0};

    auto frag = x.begin();
    size_t in_pos = 0;
    size_t r = 0;

    iobuf ret;

    while (frag != x.end() || r > 0) {
        if (in_pos == frag->size()) {
            frag++;
            in_pos = 0;
            if (frag == x.end()) {
                break;
            }
        }

        if (out.pos == out.size) {
            ret.append(std::move(obuf));
            obuf = ss::temporary_buffer<char>(output_chunk_size);
            out = {.dst = obuf.get_write(), .size = obuf.size(), .pos = 0};
        }

        ZSTD_inBuffer in;
        if (frag != x.end()) {
            in = {.src = frag->get(), .size = frag->size(), .pos = in_pos};
        } else {
            in = {.src = nullptr, .size = 0, .pos = 0};
        }
        auto mode = ZSTD_e_flush;
        r = ZSTD_compressStream2(ctx, &out, &in, mode);
        throw_if_error(r);
        in_pos = in.pos;
    }

    // Must happen outside of loop to encode empty-buffer sizes
    do {
        if (out.pos == out.size) {
            ret.append(std::move(obuf));
            obuf = ss::temporary_buffer<char>(output_chunk_size);
            out = {.dst = obuf.get_write(), .size = obuf.size(), .pos = 0};
        }
        r = ZSTD_endStream(ctx, &out);
        throw_if_error(r);
    } while (r > 0);

    if (out.pos) {
        obuf.trim(out.pos);
        ret.append(std::move(obuf));
    }
    return ret;
}

size_t find_zstd_size(const iobuf& x) {
    auto consumer = iobuf::iterator_consumer(x.cbegin(), x.cend());
    // defined in zstd.h ONLY under static allocation - sigh
    // our v::compression defines that public define
    std::array<char, ZSTD_FRAMEHEADERSIZE_MAX> sz_arr{};
    consumer.consume_to(std::min(sz_arr.size(), x.size_bytes()), sz_arr.data());
    auto zstd_size = ZSTD_getFrameContentSize(
      static_cast<const void*>(sz_arr.data()), x.size_bytes());
    if (zstd_size == ZSTD_CONTENTSIZE_ERROR) {
        throw std::runtime_error(fmt::format(
          "Cannot decompress. Not compressed by zstd. iobuf:{}", x));
    }
    if (
      zstd_size == ZSTD_CONTENTSIZE_UNKNOWN || (!x.empty() && zstd_size == 0)) {
        return 0;
    }
    return zstd_size;
}
size_t decompression_step(const iobuf& x) {
    size_t ret = find_zstd_size(x);
    if (ret == 0) {
        // Note that this is a similar algorithm that kafka uses. Turns out that
        // the library that kafka uses (JNI) to load up Zstd doesn't set the
        // frame content size :
        // https://github.com/luben/zstd-jni/blob/master/src/main/java/com/github/luben/zstd/ZstdOutputStream.java
        // ZSTD_CCtx_setPledgedSrcSize(ctx, x.size_bytes())
        //
        // See kafka JNI Loaders:
        // static class ZstdConstructors {
        //     static final MethodHandle INPUT = findConstructor(
        //       "com.github.luben.zstd.ZstdInputStream",
        //       MethodType.methodType(void.class, InputStream.class));
        //     static final MethodHandle OUTPUT = findConstructor(
        //       "com.github.luben.zstd.ZstdOutputStream",
        //       MethodType.methodType(void.class, OutputStream.class));
        // }
        //
        // which means that every single zstd encoded message that comes from
        // kafka regrows!
        //
        return 64_KiB;
    }
    return std::min(64_KiB, ret);
}

iobuf stream_zstd::do_uncompress(const iobuf& x) {
    if (unlikely(x.empty())) {
        throw std::runtime_error(
          "Asked to stream_zstd::uncompress empty buffer");
    }
    ZSTD_DCtx* dctx = decompressor();
    iobuf ret;
    ss::temporary_buffer<char>& obuf = d_buffer;
    ZSTD_outBuffer out = {
      .dst = obuf.get_write(), .size = obuf.size(), .pos = 0};
    for (auto& ibuf : x) {
        ZSTD_inBuffer in = {.src = ibuf.get(), .size = ibuf.size(), .pos = 0};
        while (in.pos != in.size) {
            auto err = ZSTD_decompressStream(dctx, &out, &in);
            if (in.pos != in.size && out.pos == out.size) {
                ret.append(obuf.get(), obuf.size());
                out.size = obuf.size();
                out.pos = 0;
            } else {
                throw_if_error(err);
            }
        }
    }
    ret.append(obuf.get(), out.pos);
    return ret;
}

} // namespace compression
