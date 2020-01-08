#pragma once

#include "bytes/bytes.h"
#include "bytes/iobuf.h"

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>

#include <deque>
#include <numeric>

namespace storage {

/// Appends data to a log segment. It can be subclassed so
/// other classes can add behavior and still be treated as
/// an appender.
/// Note: The functions in this call cannot be called concurrently.
class log_segment_appender {
public:
    static constexpr const size_t chunk_size = 128 * 1024; // 128KB
    static constexpr const size_t chunks_no_buffer = (1024 * 1024) / chunk_size;
    struct options {
        explicit options(seastar::io_priority_class p)
          : priority(p) {}
        seastar::io_priority_class priority;
        size_t adaptive_fallocation_size = 1024 * 1024 * 8; // 8MB
        /// when to dispatch the background fallocate
        size_t fallocation_free_space_size = 1024 * 1024 * 2; // 2MB
    };
    class chunk {
    public:
        chunk(const chunk&) = delete;
        chunk& operator=(const chunk&) = delete;
        chunk(chunk&&) noexcept = default;
        chunk& operator=(chunk&&) noexcept = default;
        explicit chunk(const size_t alignment = 4096)
          : _buf(
            seastar::allocate_aligned_buffer<char>(chunk_size, alignment)) {}

        bool is_full() const { return _pos == chunk_size; }
        bool is_empty() const { return _pos == 0; }
        size_t space_left() const { return chunk_size - _pos; }
        size_t size() const { return _pos; }
        void reset() {
            _pos = 0;
            _flushed_pos = 0;
        }
        const char* data() const { return _buf.get(); }
        size_t append(const char* src, size_t len) {
            const size_t sz = std::min(len, space_left());
            std::copy_n(src, sz, get_current());
            _pos += sz;
            return sz;
        }
        const char* dma_ptr(size_t alignment) const {
            // we must always write in hardware-aligned page multiples.
            // alignment comes from the filesystem
            const auto sz = seastar::align_down<size_t>(
              _flushed_pos, alignment);
            return _buf.get() + sz;
        }
        size_t dma_size(size_t alignment) const {
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
            const auto prev_sz = seastar::align_down<size_t>(
              _flushed_pos, alignment);
            const size_t sz = _pos - prev_sz;
            return seastar::align_up<size_t>(sz, alignment);
        }
        void compact(size_t alignment) {
            if (__builtin_expect(_flushed_pos != _pos, false)) {
                throw std::runtime_error(
                  "compact() must be called after flush()");
            }
            const char* src = dma_ptr(alignment);
            if (src == data()) {
                return;
            }
            const size_t copy_sz = dma_size(alignment);
            const size_t final_sz = size() - copy_sz;
            std::copy_n(src, copy_sz, _buf.get());
            // must be called after flush!
            _flushed_pos = _pos = final_sz;
        }
        void flush() { _flushed_pos = _pos; }
        size_t bytes_pending() const { return _pos - _flushed_pos; }
        size_t flushed_pos() const { return _flushed_pos; }

    private:
        friend std::ostream& operator<<(std::ostream&, const chunk&);
        char* get_current() { return _buf.get() + _pos; }
        std::unique_ptr<char[], free_deleter> _buf;
        size_t _pos{0};
        size_t _flushed_pos{0};
    };
    using underlying_t = std::array<chunk, chunks_no_buffer>;
    using iterator = typename underlying_t::iterator;
    using const_iterator = typename underlying_t::const_iterator;

    log_segment_appender(file f, options opts);
    ~log_segment_appender();
    log_segment_appender(log_segment_appender&&) noexcept = default;
    log_segment_appender& operator=(log_segment_appender&&) noexcept = default;
    log_segment_appender(const log_segment_appender&) = delete;
    log_segment_appender& operator=(const log_segment_appender&) = delete;

    uint64_t file_byte_offset() const {
        return _committed_offset + _bytes_flush_pending;
    }
    size_t dma_write_alignment() const { return _dma_write_alignment; }
    seastar::io_priority_class priority_class() const { return _opts.priority; }

    future<> append(const char* buf, const size_t n);
    future<> append(bytes_view s) {
        return append(reinterpret_cast<const char*>(s.begin()), s.size());
    }
    future<> append(const iobuf& io) {
        auto in = iobuf::iterator_consumer(io.cbegin(), io.cend());
        auto f = make_ready_future<>();
        auto c = in.consume(
          io.size_bytes(), [this, &f](const char* src, size_t sz) {
              f = f.then([this, src, sz] { return append(src, sz); });
              return stop_iteration::no;
          });
        if (__builtin_expect(c != io.size_bytes(), false)) {
            return make_exception_future<>(
                     std::runtime_error("could not append data"))
              .then([f = std::move(f)]() mutable { return std::move(f); });
        }
        return f;
    }
    future<> truncate(size_t n);
    future<> close();
    future<> flush();

protected:
    friend std::ostream& operator<<(std::ostream&, const log_segment_appender&);
    future<> do_adaptive_fallocate();

    file _out;
    options _opts;
    size_t _dma_write_alignment{0};

    size_t _last_fallocated_offset{0};
    uint64_t _committed_offset{0};
    uint64_t _bytes_flush_pending{0};

    iterator _current;
    std::array<chunk, chunks_no_buffer> _chunks;
};

using segment_appender_ptr = std::unique_ptr<log_segment_appender>;

} // namespace storage
