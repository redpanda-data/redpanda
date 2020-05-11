#pragma once
#include "seastarx.h"
#include "units.h"
#include "utils/intrusive_list_helpers.h"

#include <seastar/core/aligned_buffer.hh>

#include <cstring>

namespace storage {
class segment_appender_chunk {
public:
    static constexpr const size_t chunk_size = 16_KiB;

    explicit segment_appender_chunk(size_t alignment)
      : _alignment(alignment)
      , _buf(ss::allocate_aligned_buffer<char>(chunk_size, alignment)) {
        // zero-out the buffer in case the alloctor gaves us a recycled buffer
        // that was from a valid previous segment.
        reset();
    }

    segment_appender_chunk(const segment_appender_chunk&) = delete;
    segment_appender_chunk& operator=(const segment_appender_chunk&) = delete;
    segment_appender_chunk(segment_appender_chunk&&) noexcept = delete;
    segment_appender_chunk& operator=(segment_appender_chunk&&) noexcept
      = delete;
    ~segment_appender_chunk() noexcept = default;

    bool is_full() const { return _pos == chunk_size; }
    bool is_empty() const { return _pos == 0; }
    size_t alignment() const { return _alignment; }
    size_t space_left() const { return chunk_size - _pos; }
    size_t size() const { return _pos; }
    /// \brief size() aligned to the _alignment
    size_t dma_size() const;
    const char* data() const { return _buf.get(); }
    const char* dma_ptr() const;
    size_t bytes_pending() const { return _pos - _flushed_pos; }
    size_t flushed_pos() const { return _flushed_pos; }

    size_t append(const char* src, size_t len);
    void reset() {
        _flushed_pos = _pos = 0;
        // allow chunk reuse
        std::memset(_buf.get(), 0, chunk_size);
    }
    void flush() { _flushed_pos = _pos; }
    char* get_current() { return _buf.get() + _pos; }
    void set_position(size_t p) { _flushed_pos = _pos = p; }

    intrusive_list_hook hook;

private:
    size_t _alignment{0};
    size_t _pos{0};
    size_t _flushed_pos{0};
    std::unique_ptr<char[], ss::free_deleter> _buf;
    friend std::ostream&
    operator<<(std::ostream&, const segment_appender_chunk&);
};

} // namespace storage
