// Copyright 2020 Redpanda Data, Inc.
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
#include "ssx/semaphore.h"
#include "storage/chunk_cache.h"
#include "storage/logger.h"
#include "storage/storage_resources.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/align.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>

#include <fmt/format.h>

#include <ostream>

namespace storage {

/*
 * Optimization ideas:
 *
 * 1. partial writes to the same physical head chunk are serialized to prevent
 * out-of-order writes clobbering previous writes. this is only relevant when
 * there are many flushes which dispatch the current head if it has any pending
 * bytes. there hasn't been any noticable performance degredation, but one
 * option for avoiding this is to do more aligned appends or add a special
 * padding batch that is read and then fully ignored by the parser.
 *
 * 2. flush operations are completed asynchronously when writes complete. there
 * is not reason to do this so aggresively. we could potentially reduce the
 * amount of flushing by using a bytes or time heuristic. we'd increase the
 * latency of each flush, but we'd dispatch less physical flush operations.
 */

[[gnu::cold]] static ss::future<>
size_missmatch_error(const char* ctx, size_t expected, size_t got) {
    return ss::make_exception_future<>(fmt::format(
      "{}. Size missmatch. Expected:{}, Got:{}", ctx, expected, got));
}

static constexpr auto head_sem_name = "s/appender-head";

segment_appender::segment_appender(ss::file f, options opts)
  : _out(std::move(f))
  , _opts(opts)
  , _concurrent_flushes(ss::semaphore::max_counter(), "s/append-flush")
  , _prev_head_write(ss::make_lw_shared<ssx::semaphore>(1, head_sem_name))
  , _inactive_timer([this] { handle_inactive_timer(); })
  , _chunk_size(internal::chunks().chunk_size()) {
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
    if (_prev_head_write) {
        vassert(
          _prev_head_write->available_units() == 1,
          "Unexpected pending head write {}",
          *this);
    }
    vassert(
      _flush_ops.empty(),
      "Active flush operations on appender destroy {}",
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
  , _prev_head_write(std::move(o._prev_head_write))
  , _flush_ops(std::move(o._flush_ops))
  , _flushed_offset(o._flushed_offset)
  , _stable_offset(o._stable_offset)
  , _inflight(std::move(o._inflight))
  , _callbacks(std::exchange(o._callbacks, nullptr))
  , _inactive_timer([this] { handle_inactive_timer(); })
  , _chunk_size(o._chunk_size) {
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
              ssx::semaphore_units) {
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
    return hard_flush()
      .then([this, n] { return do_truncation(n); })
      .then([this, n] {
          _committed_offset = n;
          _fallocation_offset = n;
          _flushed_offset = n;
          _stable_offset = n;
          auto f = ss::now();
          if (_head) {
              // NOTE: Important to reset chunks for offset accounting.  reset
              // any partial state, since after the truncate, it makes no sense
              // to keep any old state/pointers/sizes, etc
              _head->reset();
          } else {
              // https://github.com/redpanda-data/redpanda/issues/43
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
    return hard_flush()
      .then([this] { return do_truncation(_committed_offset); })
      .then([this] { return _out.close(); });
}

ss::future<> segment_appender::do_next_adaptive_fallocation() {
    auto step = _opts.resources.get_falloc_step(_opts.segment_size);
    if (step == 0) {
        // Don't fallocate.  This happens if we're low on disk, or if
        // the user has configured a 0 max falloc step.
        return ss::make_ready_future<>();
    }

    return ss::with_semaphore(
             _concurrent_flushes,
             ss::semaphore::max_counter(),
             [this, step]() mutable {
                 vassert(
                   _prev_head_write->available_units() == 1,
                   "Unexpected pending head write {}",
                   *this);
                 // step - compute step rounded to alignment(4096); this is
                 // needed because during a truncation the follow up fallocation
                 // might not be page aligned
                 if (_fallocation_offset % fallocation_alignment != 0) {
                     // add left over bytes to a full page
                     step += fallocation_alignment
                             - (_fallocation_offset % fallocation_alignment);
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

ss::future<> segment_appender::maybe_advance_stable_offset(
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
        _stable_offset = committed;
        return process_flush_ops(committed);
    } else {
        write->done = true;
        return ss::now();
    }
}

ss::future<> segment_appender::process_flush_ops(size_t committed) {
    auto flushable = std::partition(
      _flush_ops.begin(), _flush_ops.end(), [committed](const flush_op& w) {
          return w.offset > committed;
      });

    if (flushable == _flush_ops.end()) {
        return ss::now();
    }

    std::vector<flush_op> ops(
      std::make_move_iterator(flushable),
      std::make_move_iterator(_flush_ops.end()));

    _flush_ops.erase(flushable, _flush_ops.end());

    return _out.flush().then([this, committed, ops = std::move(ops)]() mutable {
        _flushed_offset = committed;
        /*
         * TODO: as an optimization, add a little house keeping to determine if
         * eligible flush operations showed up while flush() was completing.
         */
        for (auto& op : ops) {
            op.p.set_value();
        }
    });
}

void segment_appender::dispatch_background_head_write() {
    vassert(_head, "dispatching write requires active chunk");
    vassert(
      _head->bytes_pending() > 0,
      "There must be data to write to disk to advance the offset. {}",
      *this);

    const size_t start_offset = ss::align_down<size_t>(
      _committed_offset, _head->alignment());
    const size_t expected = _head->dma_size();
    const char* src = _head->dma_ptr();
    vassert(
      expected <= _chunk_size,
      "Writes can be at most a full segment. Expected {}, attempted write: {}",
      _chunk_size,
      expected);
    // accounting synchronously
    _committed_offset += _head->bytes_pending();
    _bytes_flush_pending -= _head->bytes_pending();
    // background write
    _head->flush();
    _inflight.emplace_back(
      ss::make_lw_shared<inflight_write>(_committed_offset));
    auto w = _inflight.back();

    /*
     * if _head is full then take control of it for this final write and then
     * release it back into the chunk cache. otherwise, leave it in place so
     * that new appends may accumulate. this optimization is meant to avoid
     * redhydrating the chunk on append following a flush when the head has
     * pending bytes and a write is dispatched.
     */
    const auto full = _head->is_full();
    auto h = full ? std::exchange(_head, nullptr) : _head;

    /*
     * make sure that when the write is dispatched that is sequenced in-order on
     * the correct semaphore by grabbing the units synchronously.
     */
    auto prev = _prev_head_write;
    auto units = ss::get_units(*prev, 1);

    (void)ss::with_semaphore(
      _concurrent_flushes,
      1,
      [h,
       w,
       this,
       start_offset,
       expected,
       src,
       prev,
       units = std::move(units),
       full]() mutable {
          return units
            .then([this, h, w, start_offset, expected, src, full](
                    ssx::semaphore_units u) mutable {
                return _out
                  .dma_write(start_offset, src, expected, _opts.priority)
                  .then([this, h, w, expected, full](size_t got) {
                      /*
                       * the continuation that captured full=true is the end of
                       * the dependency chain for this chunk. it can be returned
                       * to cache.
                       */
                      if (full) {
                          h->reset();
                          internal::chunks().add(h);
                      }
                      if (unlikely(expected != got)) {
                          return size_missmatch_error(
                            "chunk::write", expected, got);
                      }
                      return maybe_advance_stable_offset(w);
                  })
                  .finally([u = std::move(u)] {});
            })
            .finally([prev] {});
      })
      .handle_exception([this](std::exception_ptr e) {
          vassert(false, "Could not dma_write: {} - {}", e, *this);
      });

    /*
     * when the head becomes full it still needs to be properly sequenced (see
     * above), but no future writes to same head head are possible so the
     * dependency chain is reset for the next head.
     */
    if (full) {
        _prev_head_write = ss::make_lw_shared<ssx::semaphore>(1, head_sem_name);
    }
}

ss::future<> segment_appender::flush() {
    _inactive_timer.cancel();

    // dispatched write will drive flush completion
    if (_head && _head->bytes_pending()) {
        auto& w = _flush_ops.emplace_back(file_byte_offset());
        dispatch_background_head_write();
        return w.p.get_future();
    }

    if (file_byte_offset() <= _flushed_offset) {
        return ss::now();
    }

    /*
     * if there are inflight write _ops_ then we can be sure that flush ops will
     * be processed eventually (see: maybe advance stable offset).
     */
    if (!_inflight.empty()) {
        auto& w = _flush_ops.emplace_back(file_byte_offset());
        return w.p.get_future();
    }

    vassert(
      file_byte_offset() <= _stable_offset,
      "No inflight writes but eof {} > stable offset {}: {}",
      file_byte_offset(),
      _stable_offset,
      *this);

    return _out.flush().handle_exception([this](std::exception_ptr e) {
        vassert(false, "Could not flush: {} - {}", e, *this);
    });
}

ss::future<> segment_appender::hard_flush() {
    _inactive_timer.cancel();
    if (_head && _head->bytes_pending()) {
        dispatch_background_head_write();
    }
    return ss::with_semaphore(
             _concurrent_flushes,
             ss::semaphore::max_counter(),
             [this]() mutable {
                 vassert(
                   _prev_head_write->available_units() == 1,
                   "Unexpected pending head write {}",
                   *this);
                 vassert(
                   _flush_ops.empty(),
                   "Pending flushes after hard flush {}",
                   *this);
                 return _out.flush();
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
             << ", bytes_flush_pending:" << a._bytes_flush_pending << "}";
}

} // namespace storage
