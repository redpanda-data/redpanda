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
#include "config/configuration.h"
#include "seastarx.h"
#include "units.h"
#include "utils/intrusive_list_helpers.h"

#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>

#include <algorithm>
#include <cstring>
#include <ostream>

namespace storage {

using alignment = named_type<size_t, struct alignment_type>;

class segment_appender_chunk {
public:
    explicit segment_appender_chunk(size_t size, alignment alignment)
      : _chunk_size(size)
      , _alignment(alignment)
      , _buf(ss::allocate_aligned_buffer<char>(_chunk_size, alignment)) {
        // zero-out the buffer in case the alloctor gaves us a recycled buffer
        // that was from a valid previous segment.
        reset();
    }

    segment_appender_chunk(const segment_appender_chunk&) = delete;
    segment_appender_chunk& operator=(const segment_appender_chunk&) = delete;
    segment_appender_chunk(segment_appender_chunk&&) noexcept = delete;
    segment_appender_chunk&
    operator=(segment_appender_chunk&&) noexcept = delete;
    ~segment_appender_chunk() noexcept = default;

    bool is_full() const { return _pos == _chunk_size; }
    bool is_empty() const { return _pos == 0; }
    alignment alignment() const { return _alignment; }
    size_t space_left() const { return _chunk_size - _pos; }
    size_t size() const { return _pos; }

    /// \brief size() aligned to the _alignment
    size_t dma_size() const {
        // We must write in page-size multiples, example:
        //
        // Assume alignment=4096, and internal state [_flushed_offset=4094,
        // _pos=4104], i.e.: bytes_pending()=10
        //
        // We must flush 2 pages worth of bytes. The first page must be
        // flushed from 0-4096 (2 bytes worth of content) and the second
        // from 4096-8192 (8 bytes worth of content). Therefore the dma-size
        // must be 8192 bytes, starting at the bottom of the _flushed_pos
        // page, in this example, at offset 0.
        //
        const auto prev_sz = ss::align_down<size_t>(_flushed_pos, _alignment);
        const auto curr_sz = ss::align_up<size_t>(_pos, _alignment);
        return curr_sz - prev_sz;
    }

    const char* data() const { return _buf.get(); }

    const char* dma_ptr() const {
        // we must always write in hardware-aligned page multiples.
        // alignment comes from the filesystem
        const auto sz = ss::align_down<size_t>(_flushed_pos, _alignment);
        return _buf.get() + sz;
    }

    size_t bytes_pending() const { return _pos - _flushed_pos; }
    size_t flushed_pos() const { return _flushed_pos; }

    size_t append(const char* src, size_t len) {
        const size_t sz = std::min(len, space_left());
        std::copy_n(src, sz, get_current());
        _pos += sz;
        return sz;
    }

    void reset() {
        _flushed_pos = _pos = 0;
        // allow chunk reuse
        std::memset(_buf.get(), 0, _chunk_size);
    }
    void flush() { _flushed_pos = _pos; }
    char* get_current() { return _buf.get() + _pos; }
    void set_position(size_t p) { _flushed_pos = _pos = p; }

    intrusive_list_hook hook;

private:
    size_t _chunk_size{0};
    storage::alignment _alignment{0};
    size_t _pos{0};
    size_t _flushed_pos{0};
    std::unique_ptr<char[], ss::free_deleter> _buf;
    friend std::ostream&
    operator<<(std::ostream& o, const segment_appender_chunk& c) {
        return o << "{_alignment:" << c._alignment << ", _pos:" << c._pos
                 << ", _flushed_pos:" << c._flushed_pos << "}";
    }
};

} // namespace storage
