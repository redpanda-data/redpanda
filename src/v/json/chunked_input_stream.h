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

#include <utility>

namespace json {

namespace impl {

/**
 * \brief An in-memory input stream with non-contiguous memory allocation.
 *
 * Reads directly from the iobuf's fragments via iobuf::byte_iterator:
 * Peek()/Take() are pointer operations over the current fragment, hopping
 * to the next fragment at its boundary.
 */
template<typename Encoding = ::json::UTF8<>>
class chunked_input_stream {
public:
    using Ch = Encoding::Ch;
    static_assert(
      sizeof(Ch) == sizeof(char), "only single-byte encodings are supported");

    /// rapidjson streams have no explicit end-of-input signal; the reader
    /// detects termination by seeing a NUL (StringStream parses C strings
    /// and runs into the terminator, other streams synthesize it). A raw
    /// NUL is never legitimate JSON, so no payload byte is masked.
    static constexpr Ch eof_sentinel = '\0';

    explicit chunked_input_stream(iobuf&& buf)
      : _buf(std::move(buf))
      , _it(std::as_const(_buf).begin(), std::as_const(_buf).end())
      , _end(std::as_const(_buf).end(), std::as_const(_buf).end()) {}

    chunked_input_stream(const chunked_input_stream&) = delete;
    chunked_input_stream& operator=(const chunked_input_stream&) = delete;
    chunked_input_stream(chunked_input_stream&&) = delete;
    chunked_input_stream& operator=(chunked_input_stream&&) = delete;
    ~chunked_input_stream() = default;

    /**
     * \defgroup Implement rapidjson::Stream
     */
    /**@{*/

    Ch Peek() const { return _it != _end ? *_it : eof_sentinel; }
    Ch Take() {
        if (_it == _end) [[unlikely]] {
            return eof_sentinel;
        }
        Ch c = *_it;
        ++_it;
        ++_consumed;
        return c;
    }
    size_t Tell() const { return _consumed; }

    // Not implemented, present to satisfy in-situ parsing codepaths that are
    // instantiated but never taken.
    void Put(Ch) { RAPIDJSON_ASSERT(false); }
    Ch* PutBegin() {
        RAPIDJSON_ASSERT(false);
        return nullptr;
    }
    size_t PutEnd(Ch*) {
        RAPIDJSON_ASSERT(false);
        return 0;
    }
    void Flush() { RAPIDJSON_ASSERT(false); }

    /**@}*/

private:
    iobuf _buf;
    iobuf::byte_iterator _it;
    iobuf::byte_iterator _end;
    size_t _consumed{0};
};

} // namespace impl

template<typename Encoding>
using generic_chunked_input_stream = impl::chunked_input_stream<Encoding>;

using chunked_input_stream = generic_chunked_input_stream<UTF8<>>;

} // namespace json
