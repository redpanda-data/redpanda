#include "storage/segment_appender.h"

#include "likely.h"
#include "storage/logger.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/semaphore.hh>

#include <fmt/format.h>

namespace storage {
[[gnu::cold]] static ss::future<>
size_missmatch_error(const char* ctx, size_t expected, size_t got) {
    return ss::make_exception_future<>(fmt::format(
      "{}. Size missmatch. Expected:{}, Got:{}", ctx, expected, got));
}
[[gnu::cold]] static ss::future<>
appender_closed_error(const segment_appender& a) {
    return ss::make_exception_future<>(
      std::runtime_error(fmt::format("Appender is closed: {}", a)));
}
segment_appender::segment_appender(ss::file f, options opts)
  : _out(std::move(f))
  , _opts(std::move(opts))
  , _concurrent_flushes(_opts.number_of_chunks) {
    const auto align = _out.disk_write_dma_alignment();
    for (auto i = 0; i < _opts.number_of_chunks; ++i) {
        auto c = new chunk(align); // NOLINT
        _free_chunks.push_back(*c);
    }
    _concurrent_flushes.ensure_space_for_waiters(_opts.number_of_chunks);
}

segment_appender::~segment_appender() noexcept {
    vassert(
      _bytes_flush_pending == 0 && _closed,
      "Must flush & close before deleting {}",
      *this);
    clear();
}

segment_appender::segment_appender(segment_appender&& o) noexcept
  : _out(std::move(o._out))
  , _opts(o._opts)
  , _closed(o._closed)
  , _committed_offset(o._committed_offset)
  , _bytes_flush_pending(o._bytes_flush_pending)
  , _concurrent_flushes(std::move(o._concurrent_flushes))
  , _free_chunks(std::move(o._free_chunks))
  , _full_chunks(std::move(o._full_chunks)) {
    o._closed = true;
}

static inline void remove_chunk_list(segment_appender::underlying_t& list) {
    while (!list.empty()) {
        list.pop_back_and_dispose([](segment_appender::chunk* c) {
            delete c; // NOLINT
        });
    }
}

void segment_appender::clear() {
    remove_chunk_list(_free_chunks);
    remove_chunk_list(_full_chunks);
}
ss::future<> segment_appender::append(bytes_view s) {
    // NOLINTNEXTLINE
    return append(reinterpret_cast<const char*>(s.data()), s.size());
}

ss::future<> segment_appender::append(const iobuf& io) {
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
ss::future<> segment_appender::append(const char* buf, const size_t n) {
    if (unlikely(_closed)) {
        return appender_closed_error(*this);
    }
    size_t written = 0;
    while (likely(!_free_chunks.empty())) {
        const size_t sz = head().append(buf + written, n - written);
        written += sz;
        _bytes_flush_pending += sz;
        if (written == n) {
            break;
        }
        // must be last
        if (head().is_full()) {
            dispatch_background_head_write();
        }
    }
    if (written == n) {
        return ss::make_ready_future<>();
    }
    return ss::get_units(_concurrent_flushes, 1)
      .then([this, next_buf = buf + written, next_sz = n - written](
              ss::semaphore_units<>) {
          // do not hold the units!
          return append(next_buf, next_sz);
      });
}

ss::future<> segment_appender::truncate(size_t n) {
    return flush().then([this, n] { return _out.truncate(n); }).then([this, n] {
        _committed_offset = n;

        auto& h = head();
        const size_t align = h.alignment();
        const size_t sz = ss::align_down<size_t>(_committed_offset, align);
        char* buff = h.get_current();
        h.set_position(sz);

        return _out.dma_read(sz, buff, align, _opts.priority)
          .then([this, n, align](size_t actual) {
              const size_t expected = n % align;
              if (actual != expected) {
                  return size_missmatch_error("truncate::", expected, actual);
              }
              return ss::make_ready_future<>();
          });
    });
}
ss::future<> segment_appender::close() {
    if (_closed) {
        return ss::make_ready_future<>();
    }
    _closed = true;
    return flush()
      .then([this] { return _out.truncate(_committed_offset); })
      .then([this] { return _out.close(); });
}

void segment_appender::dispatch_background_head_write() {
    auto& h = head();
    h.hook.unlink();
    _full_chunks.push_back(h);

    const size_t start_offset = ss::align_down<size_t>(
      _committed_offset, h.alignment());
    const size_t expected = h.dma_size();
    const char* src = h.dma_ptr();
    vassert(
      expected <= chunk::chunk_size,
      "Writes can be at most a full segment. Expected {}, attempted write: {}",
      chunk::chunk_size,
      expected);
    // accounting synchronously
    _committed_offset += h.bytes_pending();
    _bytes_flush_pending -= h.bytes_pending();
    // background write
    h.flush();
    (void)ss::with_semaphore(
      _concurrent_flushes, 1, [&h, this, start_offset, expected, src] {
          return _out.dma_write(start_offset, src, expected, _opts.priority)
            .then([this, &h, expected](size_t got) {
                if (h.is_full()) {
                    h.reset();
                } else {
                    h.compact();
                }
                if (unlikely(expected != got)) {
                    return size_missmatch_error("chunk::write", expected, got);
                }
                h.hook.unlink();
                _free_chunks.push_back(h);
                return ss::make_ready_future<>();
            });
      });
}

ss::future<> segment_appender::flush() {
    if (_bytes_flush_pending == 0) {
        return ss::make_ready_future<>();
    }
    if (!_free_chunks.empty()) {
        if (!head().is_empty()) {
            dispatch_background_head_write();
        }
    }
    // steal all free fragments while we wait for a full truncation
    // to prevent concurrent appends in the middle of a truncation and
    // add them back at the end of a flush _even_ if there is an exception
    // by putting them on the finally block.
    auto suspend = std::exchange(_free_chunks, {});
    return ss::with_semaphore(
      _concurrent_flushes,
      _opts.number_of_chunks,
      [this, suspend = std::move(suspend)]() mutable {
          return _out.flush().finally(
            [this, suspend = std::move(suspend)]() mutable {
                while (!suspend.empty()) {
                    auto& f = suspend.front();
                    f.hook.unlink();
                    _free_chunks.push_back(f);
                }
            });
      });
}

std::ostream& operator<<(std::ostream& o, const segment_appender& a) {
    // NOTE: intrusivelist.size() == O(N) but often N is very small, ~8
    return o << "{no_of_chunks:" << a._opts.number_of_chunks
             << ", closed:" << a._closed
             << ", committed_offset:" << a._committed_offset
             << ", bytes_flush_pending:" << a._bytes_flush_pending
             << ", free_chunks:" << a._free_chunks.size() /*O(N)*/
             << ", full_chunks:" << a._full_chunks.size() /*O(N)*/ << "}";
}
} // namespace storage
