// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_appender.h"

#include "likely.h"
#include "storage/logger.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/align.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include <fmt/format.h>

namespace storage {
[[gnu::cold]] static ss::future<>
size_missmatch_error(const char* ctx, size_t expected, size_t got) {
    return ss::make_exception_future<>(fmt::format(
      "{}. Size missmatch. Expected:{}, Got:{}", ctx, expected, got));
}
segment_appender::segment_appender(ss::file f, options opts)
  : _out(std::move(f))
  , _opts(opts)
  , _concurrent_flushes(_opts.number_of_chunks) {
    const auto align = _out.disk_write_dma_alignment();
    for (size_t i = 0; i < _opts.number_of_chunks; ++i) {
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
  , _fallocation_offset(o._fallocation_offset)
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
    return ss::do_for_each(
      io.begin(), io.end(), [this](const iobuf::fragment& f) {
          return append(f.get(), f.size());
      });
}
ss::future<> segment_appender::append(const char* buf, const size_t n) {
    vassert(!_closed, "append() on closed segment: {}", *this);

    // committed offset isn't updated until the background write is dispatched.
    // however, we must ensure that an fallocation never occurs at an offset
    // below the committed offset. because truncation can occur at an unaligned
    // offset, its possible that a chunk offset range overlaps fallocation
    // offset. if that happens and the chunk fills up and is dispatched before
    // the next fallocation then fallocation will write zeros to a lower offset
    // than the commit index. thus, here we must compare fallocation offset to
    // the eventual committed offset taking into account pending bytes.
    auto next_committed_offset
      = _committed_offset
        + (!_free_chunks.empty() ? head().bytes_pending() : 0);

    if (next_committed_offset + n > _fallocation_offset) {
        return do_next_adaptive_fallocation().then(
          [this, buf, n] { return append(buf, n); });
    }

    size_t written = 0;
    while (likely(!_free_chunks.empty())) {
        const size_t sz = head().append(buf + written, n - written);
        written += sz;
        _bytes_flush_pending += sz;
        if (head().is_full()) {
            dispatch_background_head_write();
        }
        if (written == n) {
            break;
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

ss::future<> segment_appender::hydrate_last_half_page() {
    auto& h = head();
    vassert(
      h.flushed_pos() == 0, "can only hydrate after a flush:{} - {}", h, *this);
    /**
     * NOTE: This code has some very nuanced corner cases
     * 1. The alignment used must be the write() alignment and not
     *    the read alignment because our goal is to read half-page
     *    for the next **write()**
     *
     * 2. the file handle DMA read must be the full dma alignment even if
     *    it returns less bytes, and even if it is the last page
     */
    const size_t read_align = h.alignment();
    const size_t sz = ss::align_down<size_t>(_committed_offset, read_align);
    char* buff = h.get_current();
    std::memset(buff, 0, read_align);
    const size_t bytes_to_read = _committed_offset % read_align;
    h.set_position(bytes_to_read);
    if (bytes_to_read == 0) {
        return ss::make_ready_future<>();
    }
    return _out
      .dma_read(
        sz, buff, read_align /*must be full _write_ alignment*/, _opts.priority)
      .then([this, bytes_to_read](size_t actual) {
          vassert(
            bytes_to_read == actual && bytes_to_read == head().flushed_pos(),
            "Could not hydrate partial page bytes: expected:{}, got:{}. "
            "chunk:{} - appender:{}",
            bytes_to_read,
            actual,
            head(),
            *this);
      })
      .handle_exception([this](std::exception_ptr e) {
          vassert(
            false,
            "Could not read the last half page in dma_write_alignment: {} - {}",
            e,
            *this);
      });
}

ss::future<> segment_appender::do_truncation(size_t n) {
    return _out.truncate(n)
      .then([this] { return _out.flush(); })
      .handle_exception([n, this](std::exception_ptr e) {
          vassert(
            false,
            "Could not issue truncation:{} - to offset: {} - {}",
            e,
            n,
            *this);
      });
}

ss::future<> segment_appender::truncate(size_t n) {
    vassert(
      n <= file_byte_offset(),
      "Cannot ask to truncate at:{} which is more bytes than we have:{} - {}",
      file_byte_offset(),
      *this);
    return flush().then([this, n] { return do_truncation(n); }).then([this, n] {
        _committed_offset = n;
        _fallocation_offset = n;
        for (auto& c : _free_chunks) {
            // NOTE: Important to reset chunks for offset accounting.
            // reset any partial state, since after the truncate, it makes no
            // sense to keep any old state/pointers/sizes, etc
            c.reset();
        }
        return hydrate_last_half_page();
    });
}
ss::future<> segment_appender::close() {
    vassert(!_closed, "close() on closed segment: {}", *this);
    _closed = true;
    return flush()
      .then([this] { return do_truncation(_committed_offset); })
      .then([this] { return _out.close(); });
}

ss::future<> segment_appender::do_next_adaptive_fallocation() {
    return ss::with_semaphore(
             _concurrent_flushes,
             _opts.number_of_chunks,
             [this]() mutable {
                 // step - compute step rounded to 4096; this is needed because
                 // during a truncation the follow up fallocation might not be
                 // page aligned
                 auto step = _opts.falloc_step;
                 if (_fallocation_offset % 4096 != 0) {
                     // add left over bytes to a full page
                     step += 4096 - (_fallocation_offset % 4096);
                 }
                 vassert(
                   _fallocation_offset >= _committed_offset,
                   "Attempting to fallocate at {} below the committed offset "
                   "{}",
                   _fallocation_offset,
                   _committed_offset);
                 return _out.allocate(_fallocation_offset, step)
                   .then([this, step] { _fallocation_offset += step; });
             })
      .handle_exception([this](std::exception_ptr e) {
          vassert(
            false,
            "We failed to fallocate file. This usually means we have ran out "
            "of disk space. Please check your data partition and ensure you "
            "have enoufh space. Error: {} - {}",
            e,
            *this);
      });
}

void segment_appender::dispatch_background_head_write() {
    auto& h = head();
    vassert(
      h.bytes_pending() > 0,
      "There must be data to write to disk to advance the offset. {}",
      *this);
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
      _concurrent_flushes,
      1,
      [&h, this, start_offset, expected, src] {
          return _out.dma_write(start_offset, src, expected, _opts.priority)
            .then([this, &h, expected](size_t got) {
                if (h.is_full()) {
                    h.reset();
                }
                if (unlikely(expected != got)) {
                    return size_missmatch_error("chunk::write", expected, got);
                }
                h.hook.unlink();
                _free_chunks.push_back(h);
                return ss::make_ready_future<>();
            });
      })
      .handle_exception([this](std::exception_ptr e) {
          vassert(false, "Could not dma_write: {} - {}", e, *this);
      });
}

ss::future<> segment_appender::flush() {
    while (!_free_chunks.empty() && head().bytes_pending()) {
        dispatch_background_head_write();
    }
    return ss::with_semaphore(
             _concurrent_flushes,
             _opts.number_of_chunks,
             [this]() mutable {
                 return _out.flush().finally([this]() mutable {
                     if (auto it = std::find_if(
                           _free_chunks.begin(),
                           _free_chunks.end(),
                           [](const chunk& c) { return !c.is_empty(); });
                         it != _free_chunks.end()) {
                         it->hook.unlink();
                         _free_chunks.push_front(*it);
                     }
                 });
             })
      .handle_exception([this](std::exception_ptr e) {
          vassert(false, "Could not flush: {} - {}", e, *this);
      });
}

std::ostream& operator<<(std::ostream& o, const segment_appender& a) {
    // NOTE: intrusivelist.size() == O(N) but often N is very small, ~8
    return o << "{no_of_chunks:" << a._opts.number_of_chunks
             << ", closed:" << a._closed
             << ", fallocation_offset:" << a._fallocation_offset
             << ", committed_offset:" << a._committed_offset
             << ", bytes_flush_pending:" << a._bytes_flush_pending
             << ", free_chunks:" << a._free_chunks.size() /*O(N)*/
             << ", full_chunks:" << a._full_chunks.size() /*O(N)*/ << "}";
}
} // namespace storage
