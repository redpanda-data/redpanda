#pragma once

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "likely.h"
#include "seastarx.h"

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>

#include <cstring>
#include <deque>
#include <numeric>

namespace storage {

/// Appends data to a log segment. It can be subclassed so
/// other classes can add behavior and still be treated as
/// an appender.
/// Note: The functions in this call cannot be called concurrently.
class segment_appender {
public:
    static constexpr const size_t chunk_size = 128 * 1024; // 128KB
    static constexpr const size_t chunks_no_buffer = 8;
    struct options {
        explicit options(ss::io_priority_class p)
          : priority(p) {}
        ss::io_priority_class priority;
    };
    class chunk {
    public:
        chunk(const chunk&) = delete;
        chunk& operator=(const chunk&) = delete;
        chunk(chunk&&) noexcept = default;
        chunk& operator=(chunk&&) noexcept = default;
        explicit chunk(const size_t alignment = 4096)
          : _buf(ss::allocate_aligned_buffer<char>(chunk_size, alignment)) {}

        bool is_full() const { return _pos == chunk_size; }
        bool is_empty() const { return _pos == 0; }
        size_t space_left() const { return chunk_size - _pos; }
        size_t size() const { return _pos; }
        void reset() { _flushed_pos = _pos = 0; }
        const char* data() const { return _buf.get(); }
        size_t append(const char* src, size_t len);
        const char* dma_ptr(size_t alignment) const;
        void compact(size_t alignment);
        size_t dma_size(size_t alignment) const;
        void flush() { _flushed_pos = _pos; }
        size_t bytes_pending() const { return _pos - _flushed_pos; }
        size_t flushed_pos() const { return _flushed_pos; }
        char* get_current() { return _buf.get() + _pos; }
        void set_position(size_t p) { _flushed_pos = _pos = p; }

    private:
        friend std::ostream& operator<<(std::ostream&, const chunk&);
        std::unique_ptr<char[], ss::free_deleter> _buf;
        size_t _pos{0};
        size_t _flushed_pos{0};
    };
    using underlying_t = std::array<chunk, chunks_no_buffer>;
    using iterator = typename underlying_t::iterator;
    using const_iterator = typename underlying_t::const_iterator;

    segment_appender(ss::file f, options opts);
    ~segment_appender();
    segment_appender(segment_appender&&) noexcept = default;
    segment_appender& operator=(segment_appender&&) noexcept = default;
    segment_appender(const segment_appender&) = delete;
    segment_appender& operator=(const segment_appender&) = delete;

    uint64_t file_byte_offset() const {
        return _committed_offset + _bytes_flush_pending;
    }
    size_t dma_write_alignment() const { return _dma_write_alignment; }
    ss::io_priority_class priority_class() const { return _opts.priority; }

    ss::future<> append(const char* buf, const size_t n);
    ss::future<> append(bytes_view s) {
        return append(reinterpret_cast<const char*>(s.begin()), s.size());
    }
    ss::future<> append(const iobuf& io) {
        auto in = iobuf::iterator_consumer(io.cbegin(), io.cend());
        auto f = ss::make_ready_future<>();
        auto c = in.consume(
          io.size_bytes(), [this, &f](const char* src, size_t sz) {
              f = f.then([this, src, sz] { return append(src, sz); });
              return ss::stop_iteration::no;
          });
        if (unlikely(c != io.size_bytes())) {
            return ss::make_exception_future<>(
                     std::runtime_error("could not append data"))
              .then([f = std::move(f)]() mutable { return std::move(f); });
        }
        return f;
    }
    ss::future<> truncate(size_t n);
    ss::future<> close();
    ss::future<> flush();

protected:
    friend std::ostream& operator<<(std::ostream&, const segment_appender&);

    ss::file _out;
    options _opts;
    size_t _dma_write_alignment{0};

    uint64_t _committed_offset{0};
    uint64_t _bytes_flush_pending{0};

    iterator _current;
    std::array<chunk, chunks_no_buffer> _chunks;
};

using segment_appender_ptr = std::unique_ptr<segment_appender>;

} // namespace storage
