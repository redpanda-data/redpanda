// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/internal/gzip_compressor.h"

#include "base/vassert.h"
#include "bytes/bytes.h"
#include "thirdparty/zlib/zlib.h"

#include <seastar/core/temporary_buffer.hh>

#include <fmt/core.h>

namespace compression::internal {
[[noreturn]] [[gnu::cold]] static void
throw_zstream_error(const char* fmt, int ret) {
    throw std::runtime_error(fmt::format(fmt::runtime(fmt), ret, zError(ret)));
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
    gzip_compression_codec& operator=(gzip_compression_codec&&) noexcept
      = delete;

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
    gzip_decompression_codec(const iobuf& input) noexcept
      : _input(input)
      , _input_chunk(input.begin()) {}
    gzip_decompression_codec(const gzip_decompression_codec&) = delete;
    gzip_decompression_codec& operator=(const gzip_decompression_codec&)
      = delete;
    gzip_decompression_codec(gzip_decompression_codec&&) noexcept = delete;
    gzip_decompression_codec& operator=(gzip_decompression_codec&&) noexcept
      = delete;

    void reset() {
        vassert(!_init, "Double initialized gzip decompression codec");
        _stream = default_zstream();
        // zlib is not const-correct
        // NOLINTNEXTLINE
        _stream.next_in = (unsigned char*)(_input_chunk->get());
        _stream.avail_in = _input_chunk->size();
        throw_if_zstream_error(
          "gzip error with inflateInit2:{}", inflateInit2(&_stream, 15 + 32));
        // marking init must happen before gzip header
        _init = true;

        // last
        throw_if_zstream_error(
          "gzip inflateGetHeader error:{}", inflateGetHeader(&_stream, &_hdr));
    }

    iobuf inflate_to_iobuf();

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

    const iobuf& _input;
    iobuf::const_iterator _input_chunk;

    gz_header _hdr; // needed for gzip
    z_stream _stream;
};

iobuf gzip_compressor::compress(const iobuf& b) {
    const size_t max_chunk_size = details::io_allocation_size::max_chunk_size;

    gzip_compression_codec def;
    def.reset();
    z_stream& strm = def.stream();

    const size_t output_size_estimate = deflateBound(&strm, b.size_bytes());
    size_t output_chunk_size = std::min(max_chunk_size, output_size_estimate);

    ss::temporary_buffer<char> obuf(output_chunk_size);
    strm.next_out = reinterpret_cast<unsigned char*>(obuf.get_write());
    strm.avail_out = obuf.size();

    iobuf ret;
    auto frag_i = b.begin();
    while (true) {
        // If our input fragment is empty and we have more fragments, consume
        // advance until we find a non-empty fragment.
        while (strm.avail_in == 0 && frag_i != b.end()) {
            strm.next_in = const_cast<unsigned char*>(
              reinterpret_cast<const unsigned char*>(frag_i->get()));
            strm.avail_in = frag_i->size();
            frag_i++;
        }

        if (strm.avail_out == 0) {
            obuf.trim(output_chunk_size - strm.avail_out);
            ret.append(std::move(obuf));
            output_chunk_size = std::min(output_chunk_size * 2, max_chunk_size);
            obuf = ss::temporary_buffer<char>(output_chunk_size);
            strm.next_out = reinterpret_cast<unsigned char*>(obuf.get_write());
            strm.avail_out = obuf.size();
        }

        int flush = (strm.avail_in == 0) ? Z_FINISH : Z_NO_FLUSH;
        int deflate_r = deflate(&strm, flush);
        if (deflate_r == Z_STREAM_END) {
            break;
        }
        throw_if_zstream_error("gzip error compressing chunk: {}", deflate_r);
    }

    obuf.trim(strm.total_out - ret.size_bytes());
    ret.append(std::move(obuf));

    return ret;
}

iobuf gzip_decompression_codec::inflate_to_iobuf() {
    // Max memory allocation
    const size_t max_chunk_size = details::io_allocation_size::max_chunk_size;

    // Rough guess at compression ratio to guess initial chunk size for
    // small buffers.
    size_t chunk_size = std::min(max_chunk_size, _input.size_bytes() * 3);

    int code = 0;
    iobuf output;
    do {
        chunk_size = std::min(max_chunk_size, chunk_size * 2);

        ss::temporary_buffer<char> tmp(chunk_size);
        _stream.next_out = reinterpret_cast<unsigned char*>(tmp.get_write());
        _stream.avail_out = chunk_size;

        code = inflate(&_stream, Z_NO_FLUSH);
        switch (code) {
        case Z_STREAM_ERROR:
        case Z_NEED_DICT:
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
            throw_zstream_error("gzip uncompress error:{}", code);
        default: /*do nothing*/;
        }

        while (_stream.avail_in == 0 && _input_chunk != _input.end()) {
            _input_chunk++;
            if (_input_chunk != _input.end()) {
                _stream.next_in = const_cast<unsigned char*>(
                  reinterpret_cast<const unsigned char*>(_input_chunk->get()));
                _stream.avail_in = _input_chunk->size();
            }
        }

        size_t written_out_this_iter = chunk_size - _stream.avail_out;
        tmp.trim(written_out_this_iter);
        output.append(std::move(tmp));
    } while (code == Z_OK && _stream.avail_in > 0);

    if (code != Z_OK && code != Z_STREAM_END) {
        throw_zstream_error("gzip uncompress error:{}", code);
    }

    return output;
}

iobuf gzip_compressor::uncompress(const iobuf& b) {
    auto codec = gzip_decompression_codec(b);
    codec.reset();
    return codec.inflate_to_iobuf();
}

} // namespace compression::internal
