// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/iobuf.h"
#include "json/encodings.h"

namespace json {

template<
  typename OutputStream,
  typename SourceEncoding,
  typename TargetEncoding,
  unsigned writeFlags>
class generic_iobuf_writer;

namespace impl {

/**
 * \brief An in-memory output stream with non-contiguous memory allocation.
 */
template<typename Encoding>
struct generic_chunked_buffer {
    using Ch = Encoding::Ch;

    /**
     * \defgroup Implement rapidjson::Stream
     */
    /**@{*/

    void Put(Ch c) { _impl.append(&c, sizeof(Ch)); }
    void Flush() {}

    //! Get the size of string in bytes in the string buffer.
    size_t GetSize() const { return _impl.size_bytes(); }

    //! Get the length of string in Ch in the string buffer.
    size_t GetLength() const { return _impl.size_bytes() / sizeof(Ch); }

    void Reserve(size_t s) { _impl.reserve_memory(s); }

    void Clear() { _impl.clear(); }

    /**@}*/

    /**
     * Append a fragment to this chunked_buffer. This takes ownership of the
     * fragment and is a zero-copy operation.
     */
    void append(std::unique_ptr<iobuf::fragment> frag) {
        _impl.append(std::move(frag));
    }

    /**
     * Return the underlying iobuf, this is destructive and zero-copy.
     */
    iobuf as_iobuf() && { return std::move(_impl); }

private:
    template<
      typename OutputStream,
      typename SourceEncoding,
      typename TargetEncoding,
      unsigned writeFlags>
    friend class json::generic_iobuf_writer;
    iobuf _impl;
};

} // namespace impl

template<typename Encoding>
using generic_chunked_buffer = impl::generic_chunked_buffer<Encoding>;

using chunked_buffer = generic_chunked_buffer<UTF8<>>;

} // namespace json
