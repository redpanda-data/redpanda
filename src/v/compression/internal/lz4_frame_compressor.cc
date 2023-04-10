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
#include "static_deleter_fn.h"
#include "units.h"
#include "vassert.h"

#include <seastar/core/temporary_buffer.hh>

#include <lz4.h>
#include <lz4frame.h>

namespace compression::internal {
// from frameCompress.c
static constexpr size_t lz4f_header_size = 19;
static constexpr size_t lz4f_footer_size = 4;

[[noreturn]] [[gnu::cold]] static void
throw_lz4_error(const char* fmt, LZ4F_errorCode_t err) {
    throw std::runtime_error(
      fmt::format(fmt::runtime(fmt), LZ4F_getErrorName(err)));
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

    const size_t max_chunk_size = details::io_allocation_size::max_chunk_size;

    const size_t compress_bound = LZ4F_compressBound(b.size_bytes(), &prefs);
    check_lz4_error("lz4_compressbound error:{}", compress_bound);

    const size_t estimated_output_size = compress_bound + lz4f_footer_size
                                         + lz4f_header_size;

    size_t output_chunk_size = std::min(max_chunk_size, estimated_output_size);

    ss::temporary_buffer<char> obuf(output_chunk_size);
    char* output = obuf.get_write();
    size_t output_sz = output_chunk_size;
    LZ4F_errorCode_t code = LZ4F_compressBegin(
      ctx, output, obuf.size(), &prefs);
    check_lz4_error("lz4f_compressbegin error:{}", code);
    size_t output_cursor = code;

    auto frag_i = b.begin();

    const char* input = nullptr;
    size_t input_sz = 0;
    size_t input_cursor = 0;

    // We do not consume entire input chunks at once, to avoid
    // max_chunk_size input chunks resulting in >max_chunk_size output
    // chunks.  A half-sized input chunk never results in a LZ4F_compressBound
    // that exceeds a the max output chunk.
    const size_t max_input_chunk_size = max_chunk_size / 2;

    iobuf ret;
    while (input_cursor < input_sz || frag_i != b.end()) {
        while ((input_sz - input_cursor) == 0 && frag_i != b.end()) {
            input = frag_i->get();
            input_sz = frag_i->size();
            input_cursor = 0;

            frag_i++;
        }

        size_t input_chunk_size = std::min(
          input_sz - input_cursor, max_input_chunk_size);

        code = LZ4F_compressUpdate(
          ctx,
          output + output_cursor,
          output_sz - output_cursor,
          input + input_cursor,
          input_chunk_size,
          nullptr);
        check_lz4_error("lz4f_compressupdate error:{}", code);

        // We always consume all input buffer
        input_cursor += input_chunk_size;

        // Advance by how many bytes we consumed
        output_cursor += code;

        input_chunk_size = std::min(
          input_sz - input_cursor, max_input_chunk_size);
        size_t next_output_size = LZ4F_compressBound(input_chunk_size, &prefs);

        if (output_sz - output_cursor < next_output_size) {
            obuf.trim(output_cursor);
            ret.append(std::move(obuf));
            output_chunk_size = std::min(max_chunk_size, output_chunk_size * 2);
            obuf = ss::temporary_buffer<char>(output_chunk_size);
            output = obuf.get_write();
            output_sz = obuf.size();
            output_cursor = 0;
        }
    }

    code = LZ4F_compressEnd(
      ctx, output + output_cursor, output_sz - output_cursor, nullptr);
    check_lz4_error("lz4f_compressend:{}", code);
    output_cursor += code;
    obuf.trim(output_cursor);
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

iobuf lz4_frame_compressor::uncompress(iobuf const& input) {
    size_t src_size = input.size_bytes();

    const size_t max_chunk = details::io_allocation_size::max_chunk_size;

    // Compose fragmented result from a series of `obuf` temporary buffers
    iobuf ret;

    // Read interators/offsets
    auto frag_i = input.begin();
    size_t read_this_chunk{0};
    size_t read_total{0};

    auto ctx_ptr = make_decompression_context();
    LZ4F_decompressionContext_t ctx = ctx_ptr.get();

    // Prior to main loop, optionally consume header to learn total size
    LZ4F_errorCode_t code = 0;
    size_t decompressed_size = 0;
    if (!input.empty() && input.begin()->size() >= lz4f_header_size) {
        // This is optional, because it is not guaranteed that the caller
        // passes us an iobuf with the header in a contiguous chunk.
        auto& first_chunk = *(input.begin());
        size_t sz_scratch = first_chunk.size();
        LZ4F_frameInfo_t fi;
        code = LZ4F_getFrameInfo(ctx, &fi, first_chunk.get(), &sz_scratch);
        read_this_chunk = sz_scratch;
        read_total += sz_scratch;
        check_lz4_error("lz4f_getframeinfo error: {}", code);
        decompressed_size = fi.contentSize;
    }
    size_t write_chunk_size = compute_frame_uncompressed_size(
      decompressed_size, src_size);
    write_chunk_size = std::min(write_chunk_size, max_chunk);

    // Write buffer/offsets
    size_t write_this_chunk{0};
    ss::temporary_buffer<char> obuf(write_chunk_size);
    char* out = obuf.get_write();

    while (frag_i != input.end()) {
        // These variables at as an input to LZ4F_decompress to specify
        // buffer size, and then as an output for how many bytes the
        // buffers advanced.
        size_t consumed_this_iteration = frag_i->size() - read_this_chunk;
        size_t output_this_iteration = obuf.size() - write_this_chunk;

        code = LZ4F_decompress(
          ctx,
          out + write_this_chunk,
          &output_this_iteration,
          frag_i->get() + read_this_chunk,
          &consumed_this_iteration,
          nullptr);

        write_this_chunk += output_this_iteration;
        read_this_chunk += consumed_this_iteration;
        read_total += consumed_this_iteration;

        vassert(
          write_this_chunk <= write_chunk_size,
          "Appended more bytes that allowed. Max:{}, consumed:{} ({} this "
          "iteration)",
          write_chunk_size,
          write_this_chunk,
          output_this_iteration);

        vassert(
          read_this_chunk <= frag_i->size(),
          "Consumed more bytes than in input buffer (size {}, consumed {})",
          frag_i->size(),
          read_this_chunk);

        check_lz4_error("lz4f_decompress error: {}", code);

        if (code == 0) {
            break;
        }

        while (frag_i != input.end() && read_this_chunk == frag_i->size()) {
            read_this_chunk = 0;
            frag_i++;
        }

        if (write_this_chunk == write_chunk_size && frag_i != input.end()) {
            ret.append(std::move(obuf));

            // If we were using a smaller-than-max chunk, grow it
            write_chunk_size = std::min(max_chunk, write_chunk_size * 2);

            obuf = ss::temporary_buffer<char>(write_chunk_size);
            out = obuf.get_write();
            write_this_chunk = 0;
        }
    }

    if (unlikely(read_total < src_size)) {
        throw std::runtime_error(fmt::format(
          "lz4 error. could not consume all input bytes in decompression. "
          "Input:{}, consumed:{}",
          src_size,
          read_total));
    }

    if (write_this_chunk > 0) {
        obuf.trim(write_this_chunk);
        ret.append(std::move(obuf));
    }

    return ret;
}

} // namespace compression::internal
