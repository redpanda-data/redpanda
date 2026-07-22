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

#include <array>
#include <optional>
#include <string_view>

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
 *
 * Single characters are staged in a small inline buffer and flushed to the
 * underlying iobuf in blocks, so that per-character Put() calls don't pay
 * for an iobuf::append() each.
 */
template<typename Encoding>
struct generic_chunked_buffer {
    using Ch = Encoding::Ch;

    /**
     * \defgroup Implement rapidjson::Stream
     */
    /**@{*/

    void Put(Ch c) {
        if (_staged == stage_capacity) [[unlikely]] {
            flush_stage();
        }
        _stage[_staged++] = c;
    }
    void Flush() { flush_stage(); }

    //! Get the size of string in bytes in the string buffer.
    size_t GetSize() const { return _impl.size_bytes() + _staged * sizeof(Ch); }

    //! Get the length of string in Ch in the string buffer.
    size_t GetLength() const { return GetSize() / sizeof(Ch); }

    void Reserve(size_t s) { _impl.reserve_memory(s); }

    void Clear() {
        _impl.clear();
        _staged = 0;
    }

    /**@}*/

    /**
     * Append a fragment to this chunked_buffer. This takes ownership of the
     * fragment and is a zero-copy operation.
     */
    void append(std::unique_ptr<iobuf::fragment> frag) {
        flush_stage();
        _impl.append(std::move(frag));
    }

    /**
     * If the buffered data is entirely contiguous in memory (i.e. it never
     * spilled out of the stage buffer), return a view over it. The view is
     * invalidated by any subsequent modification of this buffer.
     */
    std::optional<std::basic_string_view<Ch>> contiguous_view() const {
        if (!_impl.empty()) {
            return std::nullopt;
        }
        return std::basic_string_view<Ch>{_stage.data(), _staged};
    }

    /**
     * Return the underlying iobuf, this is destructive and zero-copy.
     */
    iobuf as_iobuf() && {
        flush_stage();
        return std::move(_impl);
    }

private:
    void flush_stage() {
        if (_staged > 0) {
            _impl.append(
              reinterpret_cast<const char*>(_stage.data()),
              _staged * sizeof(Ch));
            _staged = 0;
        }
    }

    static constexpr size_t stage_capacity = 1024;

    template<
      typename OutputStream,
      typename SourceEncoding,
      typename TargetEncoding,
      unsigned writeFlags>
    friend class json::generic_iobuf_writer;
    iobuf _impl;
    std::array<Ch, stage_capacity> _stage;
    size_t _staged{0};
};

} // namespace impl

template<typename Encoding>
using generic_chunked_buffer = impl::generic_chunked_buffer<Encoding>;

using chunked_buffer = generic_chunked_buffer<UTF8<>>;

} // namespace json
