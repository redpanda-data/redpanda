// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_appender.h"

#include "base/likely.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "config/configuration.h"
#include "reflection/adl.h"
#include "ssx/semaphore.h"
#include "storage/chunk_cache.h"
#include "storage/logger.h"
#include "storage/record_batch_utils.h"
#include "storage/storage_resources.h"

#include <seastar/core/align.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>

#include <fmt/format.h>

#include <optional>
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
    check_no_dispatched_writes();
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
  , _inflight_dispatched(std::exchange(o._inflight_dispatched, 0))
  , _dispatched_writes(std::exchange(o._dispatched_writes, 0))
  , _merged_writes(std::exchange(o._merged_writes, 0))
  , _callbacks(std::exchange(o._callbacks, nullptr))
  , _inactive_timer([this] { handle_inactive_timer(); })
  , _chunk_size(o._chunk_size) {
    o._closed = true;
}

ss::future<> segment_appender::append(const model::record_batch& batch) {
    _batch_types_to_write |= 1LU << static_cast<uint8_t>(batch.header().type);

    auto hdrbuf = std::make_unique<iobuf>(
      storage::batch_header_to_disk_iobuf(batch.header()));
    auto ptr = hdrbuf.get();
    return append(*ptr).then(
      [this, &batch, cpy = std::move(hdrbuf)] { return append(batch.data()); });
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

ss::future<> segment_appender::do_append(const char* buf, size_t n) {
    while (true) {
        vassert(!_closed, "append() on closed segment: {}", *this);

        /*
         * if there is no current active chunk then we need to rehydrate. this
         * can happen because of truncation or because the appender had been
         * idle and its chunk was reclaimed into the chunk cache.
         */
        if (unlikely(!_head && _committed_offset > 0)) {
            _head = co_await internal::chunks().get();
            co_await hydrate_last_half_page();
            continue;
        }

        if (next_committed_offset() + n > _fallocation_offset) {
            co_await do_next_adaptive_fallocation();
            continue;
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
            co_return;
        }

        // barrier. do not hold the units!
        auto units = co_await ss::get_units(_concurrent_flushes, 1);
        units.return_all();

        auto chunk = co_await internal::chunks().get();
        vassert(!_head, "cannot overwrite existing chunk");
        _head = std::move(chunk);

        buf += written;
        n -= written;
    }
}

void segment_appender::check_no_dispatched_writes() {
    vassert(
      _inflight_dispatched == 0, "Unexpected pending head write {}", *this);
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
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
      .dma_read(
        sz, buff, read_align /*must be full _write_ alignment*/, _opts.priority)
#pragma clang diagnostic pop
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
      n,
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
                 check_no_dispatched_writes();
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
                   .then([this, step] {
                       // ss::file::allocate does not adjust logical file size
                       // hence we need to do that explicitly with an extra
                       // truncate. This allows for more efficient writes.
                       // https://github.com/redpanda-data/redpanda/pull/18598.
                       return _out.truncate(_fallocation_offset + step);
                   })
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
    vassert(!_inflight.empty(), "expected non-empty inflight set");

    vassert(
      write->state == write_state::DISPATCHED,
      "write not in dispatched state: {}",
      write);

    write->set_state(write_state::DONE);

    std::optional<size_t> committed;

    /*
     * Pop off the largest possible contiguous set of DONE writes
     * (which may be zero or more as writes can finish out of order)
     * and then ack the largest committed (i.e., last) offset, and
     * process any pending flush operations.
     */
    while (!_inflight.empty()
           && _inflight.front()->state == write_state::DONE) {
        auto next_co = _inflight.front()->committed_offset;

        // check that in-flight writes have increasing offsets
        vassert(
          !committed || committed < next_co,
          "invalid committed offset {} >= {}",
          committed,
          next_co);

        committed = next_co;
        _inflight.pop_front();
    }

    if (!committed) {
        --_inflight_dispatched;
        return ss::now();
    }

    // if we advanced the committed offset, do the callbacks and
    // trigger any flush operations
    if (_callbacks) {
        _callbacks->committed_physical_offset(*committed);
    }
    _stable_offset = *committed;
    return process_flush_ops(*committed);
}

ss::future<> segment_appender::process_flush_ops(size_t committed) {
    auto flushable = std::partition(
      _flush_ops.begin(), _flush_ops.end(), [committed](const flush_op& w) {
          return w.offset > committed;
      });

    if (flushable == _flush_ops.end()) {
        --_inflight_dispatched;
        return ss::now();
    }

    flush_ops_container ops(
      std::make_move_iterator(flushable),
      std::make_move_iterator(_flush_ops.end()));

    _flush_ops.pop_back_n(std::distance(flushable, _flush_ops.end()));

    return _out.flush().then([this, committed, ops = std::move(ops)]() mutable {
        // Inflight_dispatched is incremented right before a write is dispatched
        // and then must be decremented when the write is "finished", where we
        // don't consider the write finished until any associated flush
        // operations that were triggered as part of write completion (i.e.,
        // stuff in this method) are complete.
        //
        // We also don't want to decrement this too late, i.e., in a
        // continuation attached the write completion path (which would be
        // easier), because then it might be non-zero unexpectedly as observed
        // by a client do does an append + flush and waits for the futures to
        // resolve: the flush future resolves immediately below in the set_value
        // loop, but the future returned by *this* method may resolve later,
        // after the client observes a non-zero value. So we decrement the
        // counter here, *after* the flush has completed but before we set the
        // futures which have been returned to the callers.
        //
        // Unfortunately this means we need to decrement this counter in
        // multiple places.
        --_inflight_dispatched;
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

    // accounting synchronously
    const auto prior_co = _committed_offset;
    _committed_offset += _head->bytes_pending();
    _bytes_flush_pending -= _head->bytes_pending();

    inflight_write entry{
      .full = _head->is_full(),
      .chunk = _head,
      .chunk_begin = _head->pending_aligned_begin(),
      .chunk_end = _head->pending_aligned_end(),
      .file_start_offset = start_offset,
      .committed_offset = _committed_offset};

    // background write
    _head->flush();

    auto head_sem = _prev_head_write;

    if (entry.full) {
        /*
         * If _head is full then this is the last write to this chunk, so we
         * clear out the head pointer synchronously here, then release it
         * back into the chunk cache after the write completes. Otherwise,
         * leave it in place so that new appends may accumulate. this
         * optimization is meant to avoid redhydrating the chunk on append
         * following a flush when the head has pending bytes and a write is
         * dispatched.
         */
        _head = nullptr;
        /*
         * When the head becomes full it still needs to be properly
         * sequenced with earlier writes to the same head, but no future
         * writes to same head head are possible so the dependency chain is
         * reset for the next head.
         */
        _prev_head_write = ss::make_lw_shared<ssx::semaphore>(1, head_sem_name);
    }

    if (!_inflight.empty() && _inflight.back()->try_merge(entry, prior_co)) {
        // Yay! The latest in-flight write is still queued (i.e., has not
        // been dispatched to the disk) so we just append this write
        // to that entry.
        ++_merged_writes;
        return;
    }

    _inflight.emplace_back(
      ss::make_lw_shared<inflight_write>(std::move(entry)));

    auto w = _inflight.back();

    /*
     * make sure that when the write is dispatched that is sequenced
     * in-order on the correct semaphore by grabbing the units
     * synchronously.
     */
    auto units = ss::get_units(*head_sem, 1);

    (void)ss::with_semaphore(
      _concurrent_flushes,
      1,
      [w, this, head_sem, units = std::move(units)]() mutable {
          return units
            .then([this, w](ssx::semaphore_units u) mutable {
                const auto dma_size = w->chunk_end - w->chunk_begin;

                vassert(
                  dma_size <= _chunk_size && w->chunk_end > w->chunk_begin
                    && w->chunk_end <= _chunk_size,
                  "Bad write bounds _chunk_size: {}, chunk_begin: {}, "
                  "chunk_end: {}",
                  _chunk_size,
                  w->chunk_begin,
                  w->chunk_end);

                // prevent any more writes from merging into this entry
                // as it is about to be dma_write'd.
                w->set_state(write_state::DISPATCHED);
                ++_inflight_dispatched;
                ++_dispatched_writes;

                return _out
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
                  .dma_write(
                    w->file_start_offset,
                    w->chunk->data() + w->chunk_begin,
                    dma_size,
                    _opts.priority)
#pragma clang diagnostic pop
                  .then([this, w](size_t got) {
                      /*
                       * the continuation that captured full=true is the end
                       * of the dependency chain for this chunk. it can be
                       * returned to cache.
                       */
                      if (w->full) {
                          w->chunk->reset();
                          internal::chunks().add(w->chunk);
                      }

                      // release our reference to the chunk since this
                      // structure might hang around for a while in the
                      // _inflight list but we can free this chunk to re-use
                      // now as we won't use it again
                      w->chunk = nullptr;

                      const auto expected = w->chunk_end - w->chunk_begin;
                      if (unlikely(expected != got)) {
                          return size_missmatch_error(
                            "chunk::write", expected, got);
                      }
                      return maybe_advance_stable_offset(w);
                  })
                  .finally([u = std::move(u)] {});
            })
            .finally([head_sem] {});
      })
      .handle_exception([this](std::exception_ptr e) {
          vassert(false, "Could not dma_write: {} - {}", e, *this);
      });
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
                 check_no_dispatched_writes();
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

bool segment_appender::inflight_write::try_merge(
  const inflight_write& other, size_t pco) {
    if (state == QUEUED && chunk == other.chunk) {
        // this next check is an assert rather than a check since we always
        // expect the writes to match up (i.e., right bound of prior write
        // matches the left bound of the current one), since the in-flight
        // writes should form a contiguous series of writes and we only check
        // the last write for merging.
        vassert(
          committed_offset == pco,
          "in try_merge writes didn't touch: {} {}",
          committed_offset,
          pco);

        // the lhs write cannot be full since then how could the next write
        // share its chunk: it must use a new chunk
        vassert(!full, "the lhs write cannot be full");

        // lhs chunk cannot start or end after rhs
        vassert(
          chunk_begin <= other.chunk_begin && chunk_end <= other.chunk_end,
          "lhs {}-{}, rhs: {}-{}",
          chunk_begin,
          chunk_end,
          other.chunk_begin,
          other.chunk_end);

        // we merge this write in by updating everything associated with the
        // right boundary
        committed_offset = other.committed_offset;
        chunk_end = other.chunk_end;

        // the only possible change here is false -> true, which occurs
        // when the rhs completes the chunk
        full = other.full;

        return true;
    }
    return false;
}

std::ostream& operator<<(std::ostream& o, const segment_appender& a) {
    return o << "{no_of_chunks:" << a._opts.number_of_chunks
             << ", closed:" << a._closed
             << ", fallocation_offset:" << a._fallocation_offset
             << ", stable_offset:" << a._stable_offset
             << ", flushed_offset:" << a._flushed_offset
             << ", committed_offset:" << a._committed_offset
             << ", inflight:" << a._inflight.size()
             << ", dispatched:" << a._inflight_dispatched
             << ", merged:" << a._merged_writes
             << ", bytes_flush_pending:" << a._bytes_flush_pending << "}";
}

std::ostream&
operator<<(std::ostream& s, const segment_appender::inflight_write& op) {
    fmt::print(
      s,
      "{{state: {}, committed_offest: {}}}",
      (int)op.state,
      op.committed_offset);
    return s;
}

} // namespace storage
