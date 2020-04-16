#include "storage/segment_appender_chunk.h"

#include <seastar/core/align.hh>

#include <algorithm>
#include <cstring>
#include <iostream>

namespace storage {

size_t segment_appender_chunk::append(const char* src, size_t len) {
    size_t left = space_left();
    const size_t sz = std::min(len, left);
    std::copy_n(src, sz, get_current());
    _pos += sz;
    left -= sz;
    // HACK: zero out the next 20 bytes, allows reuse of chunks
    // There are multiple solutions to this problem, this seems
    // to be the least accounting heavy
    // 1. Before flushing a half page we can zero out the next few bytes
    // 2. After reset() we can zero out the segment - one time cost.
    // 3. This append() where on average we'll probably append some zeroes
    //    more often, but it's very little work
    std::memset(get_current(), 0, std::min<size_t>(left, 20));
    return sz;
}
const char* segment_appender_chunk::dma_ptr() const {
    // we must always write in hardware-aligned page multiples.
    // alignment comes from the filesystem
    const auto sz = ss::align_down<size_t>(_flushed_pos, _alignment);
    return _buf.get() + sz;
}
void segment_appender_chunk::compact() {
    if (_pos < _alignment) {
        return;
    }
    const size_t copy_sz = dma_size();
    const char* copy_ptr = dma_ptr();
    const size_t final_sz = (_buf.get() + _pos) - copy_ptr;
    std::memmove(_buf.get(), copy_ptr, copy_sz);
    // must be called after flush!
    _flushed_pos = _pos = final_sz;
}
size_t segment_appender_chunk::dma_size() const {
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
std::ostream& operator<<(std::ostream& o, const segment_appender_chunk& c) {
    return o << "{_alignment:" << c._alignment << ", _pos:" << c._pos
             << ", _flushed_pos:" << c._flushed_pos << "}";
}

} // namespace storage
