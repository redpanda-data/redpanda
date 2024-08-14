/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "thirdparty/zlib/zlib.h"

#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>

namespace compression {

class stream_decompression_error : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

/// Decompresses a gzip compressed stream by reading chunks from it at a time.
/// Before starting decompression, reset() should be called to initialize the
/// zlib stream. On destruction of decompressor the stream and associated
/// context are freed.
class gzip_stream_decompressor {
public:
    // The chunk size controls how much data is read in from the input stream at
    // a time.
    gzip_stream_decompressor(ss::input_stream<char> stream, size_t chunk_size);

    gzip_stream_decompressor(const gzip_stream_decompressor&) = delete;
    gzip_stream_decompressor& operator=(const gzip_stream_decompressor&)
      = delete;

    gzip_stream_decompressor(gzip_stream_decompressor&&) = delete;
    gzip_stream_decompressor& operator=(gzip_stream_decompressor&&) = delete;

    // Initializes the zlib stream. Can only be called once for this object's
    // lifetime.
    void reset();

    // Returns the next decompressed chunk of data, or std::nullopt on end of
    // stream. At most chunk_size bytes are returned. Any data over the size
    // limit is preserved in the z_stream buffer.
    ss::future<std::optional<iobuf>> next();

    bool eof() const { return _eof; }

    ss::future<> stop();

    ~gzip_stream_decompressor();

private:
    ss::future<> read_from_stream();

    ss::input_stream<char> _stream;
    bool _decompression_started{false};
    bool _eof{false};
    size_t _chunk_size;

    z_stream _zs{
      .next_in = Z_NULL,
      .avail_in = 0,
      .zalloc = Z_NULL,
      .zfree = Z_NULL,
      .opaque = Z_NULL,
    };

    ss::temporary_buffer<char> _buffer;
    ss::gate _gate;
};

} // namespace compression
