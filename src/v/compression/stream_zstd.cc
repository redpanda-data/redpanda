#include "compression/stream_zstd.h"

#include "bytes/details/io_allocation_size.h"
#include "likely.h"

#include <fmt/format.h>

#include <array>
#include <zstd.h>

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

iobuf stream_zstd::compress(iobuf x) {
    if (unlikely(x.empty())) {
        return iobuf();
    }
    reset_compressor();

    auto ifragment = x.begin();
    ZSTD_CCtx* ctx = compressor().get();
    // NOTE: always enable content size. **decompression** depends on this
    throw_if_error(ZSTD_CCtx_setPledgedSrcSize(ctx, x.size_bytes()));
    // zstd requires linearized memory
    ss::temporary_buffer<char> obuf(ZSTD_compressBound(x.size_bytes()));
    ZSTD_outBuffer out = {
      .dst = obuf.get_write(), .size = obuf.size(), .pos = 0};

    while (ifragment != x.end()) {
        auto& ibuf = *ifragment;
        ZSTD_inBuffer in = {.src = ibuf.get(), .size = ibuf.size(), .pos = 0};
        auto mode = ZSTD_e_flush;
        if (std::next(ifragment) == x.end()) {
            // emit a frame & mark as the end
            mode = ZSTD_e_end;
        }
        throw_if_error(ZSTD_compressStream2(ctx, &out, &in, mode));
        ifragment++;
    }
    iobuf ret;
    if (out.pos < obuf.size() / 2) {
        // we got excellent compression, copy it to new scratch buffer and
        // release the giant linearized input
        ret.append(obuf.get(), out.pos);
    } else {
        obuf.trim(out.pos);
        ret.append(std::move(obuf));
    }
    return ret;
}

static size_t find_zstd_size(const iobuf& x) {
    auto consumer = iobuf::iterator_consumer(x.cbegin(), x.cend());
    // defined in zstd.h ONLY under static allocation - sigh
    // our v::compression defines that public define
    std::array<char, ZSTD_FRAMEHEADERSIZE_MAX> sz_arr{};
    consumer.consume_to(std::min(sz_arr.size(), x.size_bytes()), sz_arr.data());
    auto zstd_size = ZSTD_getDecompressedSize(
      static_cast<const void*>(sz_arr.data()), x.size_bytes());
    if (zstd_size == ZSTD_CONTENTSIZE_ERROR) {
        throw std::runtime_error(fmt::format(
          "Cannot decompress. Not compressed by zstd. iobuf:{}", x));
    }
    if (zstd_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        throw std::runtime_error(
          fmt::format("Cannot decompress. Unknown payload size. iobuf:{}", x));
    }
    if (zstd_size == 0) {
        throw std::runtime_error(
          fmt::format("Cannot have empty size in frame: iobuf:{}", x));
    }
    return zstd_size;
}

iobuf stream_zstd::uncompress(iobuf x) {
    if (unlikely(x.empty())) {
        return iobuf();
    }
    reset_decompressor();
    const size_t decompressed_size = find_zstd_size(x);
    auto ifragment = x.begin();
    ZSTD_DCtx* dctx = decompressor().get();
    // zstd requires linearized memory
    ss::temporary_buffer<char> obuf(decompressed_size);
    ZSTD_outBuffer out = {
      .dst = obuf.get_write(), .size = obuf.size(), .pos = 0};
    while (ifragment != x.end()) {
        auto& ibuf = *ifragment;
        ZSTD_inBuffer in = {.src = ibuf.get(), .size = ibuf.size(), .pos = 0};
        throw_if_error(ZSTD_decompressStream(dctx, &out, &in));
        ifragment++;
    }
    iobuf ret;
    obuf.trim(out.pos);
    ret.append(std::move(obuf));
    return ret;
}

} // namespace compression
