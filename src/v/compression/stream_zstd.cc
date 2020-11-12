// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/stream_zstd.h"

#include "bytes/bytes.h"
#include "bytes/details/io_allocation_size.h"
#include "compression/logger.h"
#include "likely.h"
#include "units.h"
#include "vlog.h"

#include <fmt/format.h>

#include <array>
#include <zstd.h>
#include <zstd_errors.h>

namespace compression {
[[gnu::cold]] static void throw_zstd_err(size_t rc) {
    throw std::runtime_error(
      fmt::format("ZSTD error:{}", ZSTD_getErrorName(rc)));
}
static void throw_if_error(size_t rc) {
    if (unlikely(ZSTD_isError(rc))) {
        throw_zstd_err(rc);
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

void stream_zstd::reset_decompressor() {
    _decompress.reset(ZSTD_createDCtx());
    if (!_decompress) {
        throw std::bad_alloc{};
    }
}

stream_zstd::zstd_decompress_ctx& stream_zstd::decompressor() {
    if (!_decompress) {
        reset_decompressor();
    }
    return _decompress;
}

iobuf stream_zstd::do_compress(const iobuf& x) {
    reset_compressor();
    ZSTD_CCtx* ctx = compressor().get();
    // NOTE: always enable content size. **decompression** depends on this
    throw_if_error(ZSTD_CCtx_setPledgedSrcSize(ctx, x.size_bytes()));
    // zstd requires linearized memory
    ss::temporary_buffer<char> obuf(ZSTD_compressBound(x.size_bytes()));
    ZSTD_outBuffer out = {
      .dst = obuf.get_write(), .size = obuf.size(), .pos = 0};

    for (auto& frag : x) {
        ZSTD_inBuffer in = {.src = frag.get(), .size = frag.size(), .pos = 0};
        auto mode = ZSTD_e_flush;
        throw_if_error(ZSTD_compressStream2(ctx, &out, &in, mode));
    }
    // Must happen outside of loop to encode empty-buffer sizes
    ZSTD_endStream(ctx, &out);
    iobuf ret;
    obuf.trim(out.pos);
    ret.append(std::move(obuf));
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
    reset_decompressor();
    ZSTD_DCtx* dctx = decompressor().get();
    iobuf ret;
    ss::temporary_buffer<char> obuf(decompression_step(x));
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
    obuf.trim(out.pos);
    ret.append(std::move(obuf));
    return ret;
}

} // namespace compression
