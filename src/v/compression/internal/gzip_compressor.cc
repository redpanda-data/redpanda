// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/internal/gzip_compressor.h"

#include "bytes/bytes.h"
#include "vassert.h"

#include <seastar/core/temporary_buffer.hh>

#include <fmt/core.h>

#include <zlib.h>

namespace compression::internal {
[[noreturn]] [[gnu::cold]] static void
throw_zstream_error(const char* fmt, int ret) {
    throw std::runtime_error(fmt::format(fmt, ret, zError(ret)));
}

inline void throw_if_zstream_error(const char* fmt, int code) {
    if (unlikely(code != Z_OK)) {
        throw_zstream_error(fmt, code);
    }
}
inline z_stream default_zstream() {
    z_stream zs;
    zs.zalloc = Z_NULL;
    zs.zfree = Z_NULL;
    zs.opaque = Z_NULL;
    zs.avail_in = 0;
    zs.next_in = Z_NULL;
    return zs;
}

class gzip_compression_codec {
public:
    gzip_compression_codec() noexcept = default;
    gzip_compression_codec(const gzip_compression_codec&) = delete;
    gzip_compression_codec& operator=(const gzip_compression_codec&) = delete;
    gzip_compression_codec(gzip_compression_codec&&) noexcept = delete;
    gzip_compression_codec&
    operator=(gzip_compression_codec&&) noexcept = delete;

    void reset() {
        vassert(!_init, "Double initialized gzip decompression codec");
        _stream = default_zstream();
        throw_if_zstream_error(
          "gzip compress deflateInit2 error: {}",
          deflateInit2(
            &_stream,
            Z_DEFAULT_COMPRESSION,
            Z_DEFLATED,
            15 + 16,
            8 /*512 byte*/,
            Z_DEFAULT_STRATEGY));
        _init = true;
    }
    z_stream& stream() { return _stream; }
    ~gzip_compression_codec() {
        if (_init) {
            _init = false;
            deflateEnd(&_stream);
        }
    }

private:
    bool _init{false};
    z_stream _stream;
};
class gzip_decompression_codec {
public:
    gzip_decompression_codec(const char* src, size_t src_size) noexcept
      : _input(src)
      , _input_size(src_size) {}
    gzip_decompression_codec(const gzip_decompression_codec&) = delete;
    gzip_decompression_codec& operator=(const gzip_decompression_codec&)
      = delete;
    gzip_decompression_codec(gzip_decompression_codec&&) noexcept = delete;
    gzip_decompression_codec&
    operator=(gzip_decompression_codec&&) noexcept = delete;

    void reset() {
        vassert(!_init, "Double initialized gzip decompression codec");
        _stream = default_zstream();
        // zlib is not const-correct
        // NOLINTNEXTLINE
        _stream.next_in = (unsigned char*)_input;
        _stream.avail_in = _input_size;
        throw_if_zstream_error(
          "gzip error with inflateInit2:{}", inflateInit2(&_stream, 15 + 32));
        // marking init must happen before gzip header
        _init = true;

        // last
        throw_if_zstream_error(
          "gzip inflateGetHeader error:{}", inflateGetHeader(&_stream, &_hdr));
    }

    void inflate_to(char* output, size_t out_size);

    ~gzip_decompression_codec() {
        if (_init) {
            _init = false;
            inflateEnd(&_stream);
        }
    }

    z_stream& stream() { return _stream; }
    gz_header& header() { return _hdr; }

private:
    bool _init{false};
    const char* _input;
    size_t _input_size;
    gz_header _hdr; // needed for gzip
    z_stream _stream;
};

iobuf gzip_compressor::compress(const iobuf& b) {
    ss::temporary_buffer<char> obuf;
    {
        gzip_compression_codec def;
        def.reset();
        z_stream& strm = def.stream();
        /* Calculate maximum compressed size and
         * allocate an output buffer accordingly, being
         * prefixed with the Message header. */
        const size_t output_size = deflateBound(&strm, b.size_bytes());
        obuf = ss::temporary_buffer<char>(output_size);

        // NOLINTNEXTLINE
        strm.next_out = (unsigned char*)obuf.get_write();
        strm.avail_out = output_size;

        /* Iterate through each segment and compress it. */
        for (auto& io : b) {
            // zlib is not const correct
            // NOLINTNEXTLINE
            strm.next_in = (unsigned char*)io.get();
            strm.avail_in = io.size();
            throw_if_zstream_error(
              "gzip error compressing chunk: {}", deflate(&strm, Z_NO_FLUSH));
        }
        /* Finish the compression */
        if (int ret = deflate(&strm, Z_FINISH); ret != Z_STREAM_END) {
            throw_if_zstream_error("gzip error finishing compression: {}", ret);
        }
        obuf.trim(def.stream().total_out);
        // trigger deflateEnd
    }
    iobuf ret;
    ret.append(std::move(obuf));
    return ret;
}

void gzip_decompression_codec::inflate_to(char* output, size_t out_size) {
    size_t consumed_bytes = 0;
    int code = 0;
    // NOLINTNEXTLINE
    auto out = reinterpret_cast<unsigned char*>(output);
    do {
        // NOLINTNEXTLINE
        _stream.next_out = out + consumed_bytes;
        _stream.avail_out = out_size - consumed_bytes;
        code = inflate(&_stream, Z_NO_FLUSH);
        switch (code) {
        case Z_STREAM_ERROR:
        case Z_NEED_DICT:
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
            throw_zstream_error("gzip uncmpress error:{}", code);
        default: /*do nothing*/;
        }
        /* Advance output pointer (in pass 2). */
        consumed_bytes = out_size - _stream.avail_out - consumed_bytes;
    } while (_stream.avail_out == 0 && code != Z_STREAM_END);
}

static ss::temporary_buffer<char>
buffer_for_input(const char* src, size_t src_size) {
    auto codec = gzip_decompression_codec(src, src_size);
    codec.reset();
    std::array<char, 512> dummy_buf{};
    // find gzip header
    codec.inflate_to(dummy_buf.data(), dummy_buf.size());
    return ss::temporary_buffer<char>(codec.stream().total_out);
}

static iobuf do_uncompress(const char* src, size_t src_size) {
    ss::temporary_buffer<char> buf;
    {
        auto codec = gzip_decompression_codec(src, src_size);
        codec.reset();
        buf = buffer_for_input(src, src_size);
        // main data decompression
        codec.inflate_to(buf.get_write(), buf.size());
    }
    iobuf ret;
    ret.append(std::move(buf));
    return ret;
}

iobuf gzip_compressor::uncompress(const iobuf& b) {
    if (std::distance(b.begin(), b.end()) == 1) {
        return do_uncompress(b.begin()->get(), b.size_bytes());
    }
    // linearize buffer
    // TODO: use streaming interface instead
    auto linearized = iobuf_to_bytes(b);
    return do_uncompress(
      // NOLINTNEXTLINE
      reinterpret_cast<const char*>(linearized.data()),
      linearized.size());
}
} // namespace compression::internal
