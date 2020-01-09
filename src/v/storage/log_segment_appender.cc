#include "storage/log_segment_appender.h"

#include "storage/logger.h"

#include <seastar/core/align.hh>
#include <seastar/core/print.hh>

#include <fmt/format.h>

namespace storage {
log_segment_appender::log_segment_appender(ss::file f, options opts)
  : _out(std::move(f))
  , _opts(std::move(opts))
  , _dma_write_alignment(_out.disk_write_dma_alignment()) {
    _current = _chunks.begin();
}

log_segment_appender::~log_segment_appender() {
    if (_bytes_flush_pending != 0) {
        stlog.error(
          "Must flush log segment appender before deleting. {} bytes pending",
          _bytes_flush_pending);
        std::terminate();
    }
}

ss::future<> log_segment_appender::do_adaptive_fallocate() {
    const size_t next_falloc_max = _last_fallocated_offset
                                   + _opts.adaptive_fallocation_size;
    size_t last_offset = _last_fallocated_offset;
    _last_fallocated_offset = next_falloc_max;
    return _out.allocate(last_offset, next_falloc_max);
}
ss::future<> log_segment_appender::append(const char* buf, const size_t n) {
    if (
      _last_fallocated_offset == 0
      || _committed_offset + n
           >= _last_fallocated_offset - _opts.fallocation_free_space_size) {
        // TODO(log_segment_reader, needs physical offset tracking)
        //
        // stlog.info("About to fallocate, currently: {}", *this);
        // return do_adaptive_fallocate().then(
        //   [this, buf, n] { return append(buf, n); });
    }
    size_t written = 0;
    while (__builtin_expect(_current != _chunks.end(), true)) {
        const size_t sz = _current->append(buf + written, n - written);
        written += sz;
        _bytes_flush_pending += sz;
        if (written == n) {
            break;
        }

        // must be last
        if (_current->space_left() == 0) {
            ++_current;
        }
    }
    if (written == n) {
        return ss::make_ready_future<>();
    }
    return flush().then(
      [this, next_buf = buf + written, next_sz = n - written] {
          return append(next_buf, next_sz);
      });
}

ss::future<> log_segment_appender::truncate(size_t n) {
    return flush().then([this, n] { return _out.truncate(n); }).then([this] {
        return do_adaptive_fallocate();
    });
}
ss::future<> log_segment_appender::close() {
    return flush()
      .then([this] { return _out.truncate(_committed_offset); })
      .then([this] { return _out.close(); });
}

static ss::future<> process_write_fut(size_t expected, size_t got) {
    if (__builtin_expect(expected != got, false)) {
        return ss::make_exception_future<>(fmt::format(
          "Could not flush file. Expected to write:{},but wrote:{}",
          expected,
          got));
    }
    return ss::make_ready_future<>();
}

ss::future<> log_segment_appender::flush() {
    if (_bytes_flush_pending == 0) {
        return ss::make_ready_future<>();
    }
    std::vector<ss::future<>> flushes;
    flushes.reserve(std::distance(_chunks.begin(), _current));
    for (chunk& c : _chunks) {
        if (c.bytes_pending() == 0) {
            break;
        }
        const size_t start_offset = ss::align_down<size_t>(
          _committed_offset, _dma_write_alignment);
        const size_t expected = c.dma_size(_dma_write_alignment);
        const char* src = c.dma_ptr(_dma_write_alignment);
        const size_t inside_buffer_offset = ss::align_down<size_t>(
          c.flushed_pos(), _dma_write_alignment);
        ss::future<> f
          = _out.dma_write(start_offset, src, expected, _opts.priority)
              .then(
                [&c, alignment = _dma_write_alignment, expected](size_t got) {
                    if (c.is_full()) {
                        c.reset();
                    } else {
                        c.compact(alignment);
                    }
                    return process_write_fut(expected, got);
                });
        // accounting
        _committed_offset += c.bytes_pending();
        _bytes_flush_pending -= c.bytes_pending();
        // update chunk accounting after our accounting
        c.flush();
        flushes.push_back(std::move(f));
        if (!c.is_full()) { // we've reached the end!
            break;
        }
    }
    if (__builtin_expect(_bytes_flush_pending != 0, false)) {
        throw std::runtime_error(
          fmt::format("Invalid flush, pending bytes. Details:{}", *this));
    }
    return ss::when_all_succeed(flushes.begin(), flushes.end()).then([this] {
        if (_current == _chunks.end()) {
            _current = _chunks.begin();
        }
        if (!_current->is_full()) {
            std::iter_swap(_current, _chunks.begin());
            _current = _chunks.begin();
        }
        return _out.flush();
    });
}

std::ostream&
operator<<(std::ostream& o, const log_segment_appender::chunk& c) {
    return ss::fmt_print(
      o,
      "[bytes_pending:{}, _pos:{}, _flushed_pos:{}, _ptr: {}, pos_ptr: {}]",
      c.bytes_pending(),
      c._pos,
      c._flushed_pos,
      fmt::ptr(c._buf.get()),
      fmt::ptr(c._buf.get() + c._flushed_pos));
}
std::ostream& operator<<(std::ostream& out, const log_segment_appender& o) {
    return ss::fmt_print(
      out,
      "[write_dma:{}, last_fallocated_offset:{}, "
      "adaptive_fallocation_size:{}, bytes_written:{}, "
      "committed_offset:{}, bytes_flush_pending:{}, chunk_index:{}]",
      o._dma_write_alignment,
      o._last_fallocated_offset,
      o._opts.adaptive_fallocation_size,
      o.file_byte_offset(),
      o._committed_offset,
      o._bytes_flush_pending,
      std::distance(
        o._chunks.begin(), log_segment_appender::const_iterator(o._current)));
}
} // namespace storage
