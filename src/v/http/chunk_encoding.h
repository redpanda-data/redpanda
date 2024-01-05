/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/seastarx.h"
#include "base/units.h"
#include "bytes/iobuf.h"

#include <seastar/core/temporary_buffer.hh>

namespace http {

/// Chunked transfer encoding implementation
class chunked_encoder {
public:
    enum { default_chunk_size = 128_KiB };

    /// Encoder c-tor. Parameter 'max_chunk_size' is used to set
    /// the limit on sizes of individual chunks.
    explicit chunked_encoder(
      bool bypass, size_t max_chunk_size = default_chunk_size);

    /// Transform single buffer into a series of chunks represented
    /// as an iobuf.
    iobuf encode(ss::temporary_buffer<char>&& buf) const;

    /// Transform an iobuf into a series of chunks represented
    /// as another iobuf.
    iobuf encode(iobuf&& inp);

    /// Put zero size chunk header to indicate EOF
    iobuf encode_eof() const;

    template<class BufferOrBufferSeq>
    iobuf operator()(BufferOrBufferSeq&& seq) {
        return encode(std::forward<BufferOrBufferSeq>(seq));
    }

private:
    /// Add chunk body to the output buffer sequence 'seq'. The chunk body
    /// consist of chunk header and payload. The format:
    ///     - chunk_header (size + optional extension)
    ///     - crlf
    ///     - chunk payload
    ///     - crlf
    static void
    append_chunk_body(iobuf& seq, ss::temporary_buffer<char>&& payload);

    void encode_impl(iobuf& seq, ss::temporary_buffer<char>&& buf) const;

    const size_t _max_chunk_size;
    const bool _bypass;
};

} // namespace http
