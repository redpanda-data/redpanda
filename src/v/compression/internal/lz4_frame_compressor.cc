// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/internal/lz4_frame_compressor.h"

#include "bytes/bytes.h"
#include "compression/logger.h"
#include "static_deleter_fn.h"
#include "units.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/temporary_buffer.hh>

#include <lz4.h>
#include <lz4frame.h>

namespace compression::internal {
// from frameCompress.c
static constexpr size_t lz4f_header_size = 19;
static constexpr size_t lz4f_footer_size = 4;

[[noreturn]] [[gnu::cold]] static void
throw_lz4_error(const char* fmt, LZ4F_errorCode_t err) {
    throw std::runtime_error(fmt::format(fmt, LZ4F_getErrorName(err)));
}
static inline void check_lz4_error(const char* fmt, LZ4F_errorCode_t code) {
    if (unlikely(LZ4F_isError(code))) {
        throw_lz4_error(fmt, code);
    }
}

using lz4_compression_ctx = std::unique_ptr<
  LZ4F_cctx,
  // wrap lz4f C API
  static_retval_deleter_fn<
    LZ4F_cctx,
    LZ4F_errorCode_t,
    &LZ4F_freeCompressionContext>>;

static lz4_compression_ctx make_compression_context() {
    LZ4F_cctx* c = nullptr;
    LZ4F_errorCode_t code = LZ4F_createCompressionContext(&c, LZ4F_VERSION);
    check_lz4_error("LZ4F_createCompressionContext error: {}", code);
    return lz4_compression_ctx(c);
}

using lz4_decompression_ctx = std::unique_ptr<
  LZ4F_dctx,
  // wrap lz4f C API
  static_retval_deleter_fn<
    LZ4F_dctx,
    LZ4F_errorCode_t,
    &LZ4F_freeDecompressionContext>>;

static lz4_decompression_ctx make_decompression_context() {
    LZ4F_dctx* c = nullptr;
    LZ4F_errorCode_t code = LZ4F_createDecompressionContext(&c, LZ4F_VERSION);
    check_lz4_error("LZ4F_createDecompressionContext error: {}", code);
    return lz4_decompression_ctx(c);
}

iobuf lz4_frame_compressor::compress(const iobuf& b) {
    auto ctx_ptr = make_compression_context();
    LZ4F_compressionContext_t ctx = ctx_ptr.get();
    /* Required by Kafka */
    LZ4F_preferences_t prefs;
    std::memset(&prefs, 0, sizeof(prefs));
    prefs.compressionLevel = 1; // default
    prefs.frameInfo = {
      .blockMode = LZ4F_blockIndependent, .contentSize = b.size_bytes()};
    const size_t output_buffer_size = LZ4F_compressBound(b.size_bytes(), &prefs)
                                      + lz4f_footer_size + lz4f_header_size;
    check_lz4_error("lz4_compressbound erorr:{}", output_buffer_size);
    ss::temporary_buffer<char> obuf(output_buffer_size);
    char* out = obuf.get_write();
    LZ4F_errorCode_t code = LZ4F_compressBegin(
      ctx, out, output_buffer_size, &prefs);
    check_lz4_error("lz4f_compressbegin error:{}", code);

    // start after the bytes from compressBegin
    size_t consumed_bytes = code;
    for (auto& frag : b) {
        code = LZ4F_compressUpdate(
          ctx,
          // NOLINTNEXTLINE
          out + consumed_bytes,
          output_buffer_size - consumed_bytes,
          frag.get(),
          frag.size(),
          nullptr);
        check_lz4_error("lz4f_compressupdate error:{}", code);
        consumed_bytes += code;
    }
    code = LZ4F_compressEnd(
      ctx,
      // NOLINTNEXTLINE
      out + consumed_bytes,
      output_buffer_size - consumed_bytes,
      nullptr);
    check_lz4_error("lz4f_compressend:{}", code);
    consumed_bytes += code;
    obuf.trim(consumed_bytes);
    iobuf ret;
    ret.append(std::move(obuf));
    return ret;
}

inline static constexpr size_t
compute_frame_uncompressed_size(size_t frame_size, size_t original) {
    if (frame_size == 0 || frame_size > original * 255) {
        return original * 4;
    }
    return frame_size;
}

static iobuf do_uncompressed(const char* src, const size_t src_size) {
    auto ctx_ptr = make_decompression_context();
    LZ4F_decompressionContext_t ctx = ctx_ptr.get();
    LZ4F_frameInfo_t fi;
    size_t in_sz = src_size;
    LZ4F_errorCode_t code = LZ4F_getFrameInfo(ctx, &fi, src, &in_sz);
    check_lz4_error("lz4f_getframeinfo error: {}", code);
    size_t estimated_output_size = compute_frame_uncompressed_size(
      fi.contentSize, src_size);
    ss::temporary_buffer<char> obuf(estimated_output_size);
    char* out = obuf.get_write();
    /* Decompress input buffer t o output buffer until input is exhausted. */

    size_t bytes_remaining = in_sz;
    size_t consumed_bytes = 0;
    while (bytes_remaining < src_size) {
        size_t step_output_bytes = estimated_output_size - consumed_bytes;
        size_t step_remaining_bytes = src_size - bytes_remaining;
        code = LZ4F_decompress(
          ctx,
          // NOLINTNEXTLINE
          out + consumed_bytes,
          &step_output_bytes,
          // NOLINTNEXTLINE
          src + bytes_remaining,
          &step_remaining_bytes,
          nullptr);
        check_lz4_error("lz4f_decompress error: {}", code);
        vassert(
          consumed_bytes + step_output_bytes <= estimated_output_size,
          "Appended more bytes that allowed. Max:{}, consumed:{}",
          estimated_output_size,
          consumed_bytes + step_output_bytes);
        vassert(
          bytes_remaining <= src_size,
          "Consumed more bytes than input. Max:{}, consumed:{}",
          src_size,
          bytes_remaining);
        consumed_bytes += step_output_bytes;
        bytes_remaining += step_remaining_bytes;
        if (code == 0) {
            break;
        }
        /* Need to grow output buffer, this shouldn't happen if
         * contentSize was properly set. Happens all of the time with the
         * console producer 2.3.1 and below*/
        if (consumed_bytes == estimated_output_size) {
            // TODO: add probes for re-growth
            const size_t next_size = 1_KiB /*slack*/
                                     + ((estimated_output_size * 3) + 1) / 2;
            vlog(
              complog.trace,
              "Consumed bytes:{} has reached preallocated size. Growing to "
              "size:{}",
              consumed_bytes,
              next_size);
            ss::temporary_buffer<char> tmpo(next_size);
            std::copy_n(obuf.get(), consumed_bytes, tmpo.get_write());
            obuf = std::move(tmpo);
            // update the pointer back to the original position
            out = obuf.get_write();
            estimated_output_size = next_size;
        }
    }

    if (unlikely(bytes_remaining < src_size)) {
        throw std::runtime_error(fmt::format(
          "lz4 error. could not consume all input bytes in decompression. "
          "Input:{}, consumed:{}",
          src_size,
          bytes_remaining));
    }

    obuf.trim(consumed_bytes);
    iobuf ret;
    ret.append(std::move(obuf));
    return ret;
}

iobuf lz4_frame_compressor::uncompress(const iobuf& b) {
    if (std::distance(b.begin(), b.end()) == 1) {
        return do_uncompressed(b.begin()->get(), b.size_bytes());
    }
    // linearize buffer
    // TODO: optimize iobuf
    auto linearized = iobuf_to_bytes(b);
    return do_uncompressed(
      // NOLINTNEXTLINE
      reinterpret_cast<const char*>(linearized.data()),
      linearized.size());
}

} // namespace compression::internal
