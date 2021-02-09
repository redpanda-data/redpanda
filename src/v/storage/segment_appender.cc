// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_appender.h"

#include "config/configuration.h"
#include "likely.h"
#include "storage/chunk_cache.h"
#include "storage/logger.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/align.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include <fmt/format.h>

#include <ostream>

namespace storage {

[[gnu::cold]] static ss::future<>
size_missmatch_error(const char* ctx, size_t expected, size_t got) {
    return ss::make_exception_future<>(fmt::format(
      "{}. Size missmatch. Expected:{}, Got:{}", ctx, expected, got));
}

segment_appender::segment_appender(ss::file f, options opts)
  : _out(std::move(f))
  , _opts(opts)
  , _concurrent_flushes(ss::semaphore::max_counter())
  , _inactive_timer([this] { handle_inactive_timer(); }) {
    const auto alignment = _out.disk_write_dma_alignment();
    vassert(
      internal::chunk_cache::alignment % alignment == 0,
      "unexpected alignment {} % {} != 0",
      internal::chunk_cache::alignment,
      alignment);
}

segment_appender::~segment_appender() noexcept {
    vassert(_inflight.empty(), "not empty flights");
    vassert(
      _bytes_flush_pending == 0 && _closed,
      "Must flush & close before deleting {}",
      *this);
    if (_head) {
        internal::chunks().add(std::exchange(_head, nullptr));
    }
}

segment_appender::segment_appender(segment_appender&& o) noexcept
  : _out(std::move(o._out))
  , _opts(o._opts)
  , _closed(o._closed)
  , _committed_offset(o._committed_offset)
  , _fallocation_offset(o._fallocation_offset)
  , _bytes_flush_pending(o._bytes_flush_pending)
  , _concurrent_flushes(std::move(o._concurrent_flushes))
  , _head(std::move(o._head))
  , _inflight(std::move(o._inflight))
  , _callbacks(std::exchange(o._callbacks, nullptr))
  , _inactive_timer([this] { handle_inactive_timer(); })
  , _previously_inactive(o._previously_inactive) {
    o._closed = true;
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
    // seastar is optimized for timers that never fire. here the timer is
    // cancelled because it firing may dispatch a background write, which as
    // currently formulated, is not safe to interlave with append.
    _inactive_timer.cancel();
    return do_append(buf, n).then([this] {
        if (_head && _head->bytes_pending()) {
            _inactive_timer.arm(
              config::shard_local_cfg().segment_appender_flush_timeout_ms());
        }
    });
}

ss::future<> segment_appender::do_append(const char* buf, const size_t n) {
    vassert(!_closed, "append() on closed segment: {}", *this);

    /*
     * if at any point prior to this the inactive timer fired to flush/reclaim
     * the active chunk, then insert a barrier. this barrier is necessary
     * because the append interface assumes that all background writes are for
     * full buffers. non-full buffers, upon write completion, replace the
     * current head. if the inactive timer dispatches a background write, it may
     * clobber the head when it finishes if it is racing with append.
     */
    if (_previously_inactive) {
        _previously_inactive = false;
        return ss::get_units(_concurrent_flushes, ss::semaphore::max_counter())
          .then([this, buf, n](ss::semaphore_units<>) {
              return do_append(buf, n);
          });
    }

    /*
     * if there is no current active chunk then we need to rehydrate. this can
     * happen because of truncation or because the appender had been idle and
     * its chunk was reclaimed into the chunk cache.
     */
    if (unlikely(!_head && _committed_offset > 0)) {
        return internal::chunks()
          .get()
          .then([this](ss::lw_shared_ptr<chunk> chunk) {
              _head = std::move(chunk);
          })
          .then([this, buf, n] {
              return hydrate_last_half_page().then(
                [this, buf, n] { return do_append(buf, n); });
          });
    }

    if (next_committed_offset() + n > _fallocation_offset) {
        return do_next_adaptive_fallocation().then(
          [this, buf, n] { return do_append(buf, n); });
    }

    size_t written = 0;
    if (likely(_head)) {
        const size_t sz = _head->append(buf + written, n - written);
        written += sz;
        _bytes_flush_pending += sz;
        if (_head->is_full()) {
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
          return internal::chunks().get().then(
            [this, next_buf, next_sz](ss::lw_shared_ptr<chunk> chunk) {
                vassert(!_head, "cannot overwrite existing chunk");
                _head = std::move(chunk);
                return do_append(next_buf, next_sz);
            });
      });
}

void segment_appender::handle_inactive_timer() {
    _previously_inactive = true;

    if (_head && _head->bytes_pending()) {
        /*
         * this is the why the timer was originally set upon returning from
         * append: data was sitting in the write back buffer. the segment
         * appears to be inactive so go ahead and write that data to disk.
         */
        dispatch_background_head_write();
    }

    /*
     * inactive segment chunk reclaim
     *
     * if we can ensure there are no outstanding writes (including the one that
     * may have been dispatched above in this handler) then we can reclaim the
     * chunk and return it to the cache. but we need a retry loop because the
     * background write may take some time and it steals _head until it
     * completes. it may also return the chunk to the cache if it empty.
     */
    if (_concurrent_flushes.try_wait(ss::semaphore::max_counter())) {
        if (_head && !_head->bytes_pending()) {
            internal::chunks().add(std::exchange(_head, nullptr));
            vlog(
              stlog.debug, "reclaiming inactive chunk from appender {}", *this);
        }
        _concurrent_flushes.signal(ss::semaphore::max_counter());
    } else {
        _inactive_timer.arm(
          config::shard_local_cfg().segment_appender_flush_timeout_ms());
    }
}

ss::future<> segment_appender::hydrate_last_half_page() {
    vassert(_head, "hydrate last half page expects active chunk");
    vassert(
      _head->flushed_pos() == 0,
      "can only hydrate after a flush:{} - {}",
      *_head,
      *this);
    /**
     * NOTE: This code has some very nuanced corner cases
     * 1. The alignment used must be the write() alignment and not
     *    the read alignment because our goal is to read half-page
     *    for the next **write()**
     *
     * 2. the file handle DMA read must be the full dma alignment even if
     *    it returns less bytes, and even if it is the last page
     */
    const size_t read_align = _head->alignment();
    const size_t sz = ss::align_down<size_t>(_committed_offset, read_align);
    char* buff = _head->get_current();
    std::memset(buff, 0, read_align);
    const size_t bytes_to_read = _committed_offset % read_align;
    _head->set_position(bytes_to_read);
    if (bytes_to_read == 0) {
        return ss::make_ready_future<>();
    }
    return _out
      .dma_read(
        sz, buff, read_align /*must be full _write_ alignment*/, _opts.priority)
      .then([this, bytes_to_read](size_t actual) {
          vassert(
            bytes_to_read <= actual && bytes_to_read == _head->flushed_pos(),
            "Could not hydrate partial page bytes: expected:{}, got:{}. "
            "chunk:{} - appender:{}",
            bytes_to_read,
            actual,
            *_head,
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
        auto f = ss::now();
        if (_head) {
            // NOTE: Important to reset chunks for offset accounting.  reset any
            // partial state, since after the truncate, it makes no sense to
            // keep any old state/pointers/sizes, etc
            _head->reset();
        } else {
            // https://github.com/vectorizedio/redpanda/issues/43
            f = internal::chunks().get().then(
              [this](ss::lw_shared_ptr<chunk> chunk) {
                  _head = std::move(chunk);
              });
        }
        return f.then([this] { return hydrate_last_half_page(); });
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
             ss::semaphore::max_counter(),
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
            "have enough space. Error: {} - {}",
            e,
            *this);
      });
}

void segment_appender::maybe_advance_stable_offset(
  const ss::lw_shared_ptr<inflight_write>& write) {
    /*
     * ack the largest committed offset such that all smaller
     * offsets have been written to disk.
     */
    vassert(!_inflight.empty(), "expected non-empty inflight set");
    if (_inflight.front() == write) {
        auto committed = _inflight.front()->offset;
        _inflight.pop_front();

        while (!_inflight.empty()) {
            auto next = _inflight.front();
            if (next->done) {
                _inflight.pop_front();
                vassert(
                  committed < next->offset,
                  "invalid committed offset {} >= {}",
                  committed,
                  next->offset);
                committed = next->offset;
                continue;
            }
            break;
        }

        if (_callbacks) {
            _callbacks->committed_physical_offset(committed);
        }
    } else {
        write->done = true;
    }
}

void segment_appender::dispatch_background_head_write() {
    vassert(_head, "dispatching write requires active chunk");
    auto h = std::exchange(_head, nullptr);
    vassert(
      h->bytes_pending() > 0,
      "There must be data to write to disk to advance the offset. {}",
      *this);

    const size_t start_offset = ss::align_down<size_t>(
      _committed_offset, h->alignment());
    const size_t expected = h->dma_size();
    const char* src = h->dma_ptr();
    vassert(
      expected <= chunk::chunk_size,
      "Writes can be at most a full segment. Expected {}, attempted write: {}",
      chunk::chunk_size,
      expected);
    // accounting synchronously
    _committed_offset += h->bytes_pending();
    _bytes_flush_pending -= h->bytes_pending();
    // background write
    h->flush();
    _inflight.emplace_back(
      ss::make_lw_shared<inflight_write>(_committed_offset));
    auto w = _inflight.back();
    (void)ss::with_semaphore(
      _concurrent_flushes,
      1,
      [h, w, this, start_offset, expected, src] {
          return _out.dma_write(start_offset, src, expected, _opts.priority)
            .then([this, h, w, expected](size_t got) {
                if (h->is_full()) {
                    h->reset();
                }
                if (h->is_empty()) {
                    internal::chunks().add(h);
                } else {
                    _head = h;
                }
                if (unlikely(expected != got)) {
                    return size_missmatch_error("chunk::write", expected, got);
                }
                maybe_advance_stable_offset(w);
                return ss::make_ready_future<>();
            });
      })
      .handle_exception([this](std::exception_ptr e) {
          vassert(false, "Could not dma_write: {} - {}", e, *this);
      });
}

ss::future<> segment_appender::flush() {
    _inactive_timer.cancel();
    if (_head && _head->bytes_pending()) {
        dispatch_background_head_write();
    }
    return ss::with_semaphore(
             _concurrent_flushes,
             ss::semaphore::max_counter(),
             [this]() mutable { return _out.flush(); })
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
             << ", bytes_flush_pending:" << a._bytes_flush_pending << "}";
}

} // namespace storage
