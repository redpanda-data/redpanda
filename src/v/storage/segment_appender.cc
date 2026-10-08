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
#include "ssx/semaphore.h"
#include "storage/chunk_cache.h"
#include "storage/logger.h"
#include "storage/record_batch_utils.h"
#include "storage/storage_resources.h"

#include <seastar/core/align.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>

#include <optional>
#include <ostream>

namespace storage {

/*
 * segment_appender implementation notes. For the public contract see
 * `segment_appender.h`.
 *
 * We write with direct I/O, bypassing the kernel's page cache. The device reads
 * the buffer by direct memory access (DMA) some time after we submit the write.
 * Two constraints follow:
 *
 * 1. The file offset and length must be multiples of the file's DMA alignment,
 * and the buffer address a multiple of the memory DMA alignment. The appender
 * writes at the chunk's fixed 4 KiB alignment, so "page" below means 4 KiB.
 *
 * 2. Memory under an in-flight DMA must not be modified [1]. The rule covers
 * the write's entire range: on a checksummed path (DIF/DIX, iSCSI, RAID 5
 * parity, a checksumming filesystem) one unstable byte can fail the whole
 * sector or stripe, as a write error or as a bad read later. Seen in
 * production [2].
 *
 * append() takes arbitrary memory addresses and lengths, so it copies the data
 * into an aligned buffer, a `segment_appender_chunk`, and resolves without
 * waiting for the data to reach disk (write-behind). That keeps disk latency
 * off the append path and lets one write cover many appends. Chunks come from a
 * per-shard cache, which bounds how much unwritten data the shard can hold in
 * memory: when no chunk is free, append() waits.
 *
 * A full chunk's last write ends on a page boundary, since append_chunk_size is
 * a multiple of 4 KiB. flush(), hard_flush() and `_inactive_timer` instead
 * write a partly filled chunk; by rule 1 that write still covers whole pages,
 * so the appender writes the last page before it is full.
 *
 *              page 0        page 1
 *            ┌─────────────┬─────────────┐
 *   chunk    │▓▓▓▓▓▓▓▓▓▓▓▓▓│▓▓▓▓▓░░░░░░░░│  memory; two of its four pages
 *            └─────────────┴─────────────┘
 *   write    ◄───────────────────────────►  the file range it covers
 *
 *              ▓ appended    ░ written, nothing appended there yet
 *
 * Unless that write ended on a page boundary, the next append() lands inside
 * the file range it covers. Whether it may land in the chunk depends on the
 * write's state. In the diagrams @ marks the bytes the next append adds.
 *
 * QUEUED - not submitted to the device, so the chunk is safe to modify. The
 * append lands in it, and try_merge() folds the write into the queued
 * one, so a single dma_write covers both appends.
 *
 *   chunk   │▓▓▓▓▓▓▓▓▓▓▓▓▓│▓▓▓▓▓@@@░░░░░│
 *   write   ◄────────── QUEUED ─────────►  extends over @@@ when dispatched
 *
 * DONE - the device is finished with the buffer, so the append lands in the
 * chunk and the following write covers page 1 again.
 *
 *   chunk   │▓▓▓▓▓▓▓▓▓▓▓▓▓│▓▓▓▓▓@@@░░░░░│
 *   write 1 ◄────────── DONE ───────────►
 *   write 2               ◄─────────────►  covers page 1 again, with @@@
 *
 * DISPATCHED - the device may be reading the buffer, so rule 2 forbids
 * modifying page 1. copy_remainder_from() moves it into a new chunk and the
 * append lands there. Waiting for the write to complete would satisfy rule 2
 * too, at the cost of disk latency on the append path.
 *
 *   old     │▓▓▓▓▓▓▓▓▓▓▓▓▓│▓▓▓▓▓░░░░░░░░│
 *   write 1 ◄──────── DISPATCHED ───────►
 *   new                   │▓▓▓▓▓@@@░░░░░│  page 1 copied, then @@@
 *   write 2               ◄─────────────►  must land after write 1
 *
 * The copy leaves two writes covering page 1, and the older one has no @@@:
 * landing second, it would erase them. `_prev_head_write` serializes writes
 * over the same file range so they land in append order.
 *
 * [1] "Semantics of racy O_DIRECT writes", linux-block:
 * https://lore.kernel.org/linux-block/CAOBGo4xx+88nZM=nqqgQU5RRiHP1QOqU4i2dDwXt7rF6K0gaUQ@mail.gmail.com/
 * [2] https://redpandadata.atlassian.net/browse/CORE-12458
 *
 * Optimization ideas:
 *
 * 1. partial writes to the same physical head chunk are serialized to prevent
 * out-of-order writes clobbering previous writes. this is only relevant when
 * there are many flushes which dispatch the current head if it has any pending
 * bytes. there hasn't been any noticeable performance degredation, but one
 * option for avoiding this is to do more aligned appends or add a special
 * padding batch that is read and then fully ignored by the parser.
 *
 * 2. flush operations are completed asynchronously when writes complete. there
 * is not reason to do this so aggressively. we could potentially reduce the
 * amount of flushing by using a bytes or time heuristic. we'd increase the
 * latency of each flush, but we'd dispatch less physical flush operations.
 */

[[gnu::cold]] static ss::future<>
size_mismatch_error(const char* ctx, size_t expected, size_t got) {
    return ss::make_exception_future<>(fmt::format(
      "{}. Size mismatch. Expected:{}, Got:{}", ctx, expected, got));
}

static constexpr auto head_sem_name = "s/appender-head";

segment_appender::segment_appender(ss::file f, options opts)
  : _out(std::move(f))
  , _opts(opts)
  , _concurrent_flushes(ss::semaphore::max_counter(), "s/append-flush")
  , _prev_head_write(ss::make_lw_shared<ssx::semaphore>(1, head_sem_name))
  , _inactive_timer([this] { handle_inactive_timer(); })
  , _chunk_size(_opts.resources.chunks().chunk_size()) {
    if (!_opts.shared_stats) {
        _opts.shared_stats = ss::make_lw_shared<stats>();
    }
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
        _opts.resources.chunks().add(std::exchange(_head, nullptr));
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
  , _committed_offset_clb(std::exchange(o._committed_offset_clb, {}))
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
    // currently formulated, is not safe to interleave with append.
    _inactive_timer.cancel();
    return do_append(buf, n).then([this] {
        if (_head && _head->bytes_pending()) {
            _inactive_timer.arm(
              config::shard_local_cfg().segment_appender_flush_timeout_ms());
        }
    });
}

ss::future<> segment_appender::do_append(const char* buf, size_t n) {
    ++_opts.shared_stats->appends;
    _opts.shared_stats->bytes_requested += n;
    while (true) {
        vassert(!_closed, "append() on closed segment: {}", *this);

        /*
         * if there is no current active chunk then we need to rehydrate. this
         * can happen because of truncation or because the appender had been
         * idle and its chunk was reclaimed into the chunk cache.
         */
        if (unlikely(!_head && _committed_offset > 0)) {
            _head = co_await _opts.resources.chunks().get();
            co_await hydrate_last_half_page();
            continue;
        }

        if (next_committed_offset() + n > _fallocation_offset) {
            co_await do_next_adaptive_fallocation();
            continue;
        }
        /*
         * A dispatched write reads the chunk up to inflight_dma_end(),
         * rounded up to a full page, so an append below that position would
         * mutate memory the kernel is reading. Copy the unflushed remainder
         * into a fresh chunk and append there. A queued write is no hazard:
         * its buffer is not handed to the kernel yet, and appending in place
         * lets the next write merge into it.
         */
        if (_head && _head->size() < _head->inflight_dma_end()) {
            // Keep the old head visible while waiting for a cache chunk. A
            // concurrent flush must be able to dispatch bytes appended after
            // the in-flight write covered by this branch.
            auto old_head = _head;
            /**
             * NOTE: Why we do not release _prev_head_write semaphore ?
             * The _prev_head_write semaphore is used to guarantee orders of
             * pending writes, it is exchanged when the head is full. This
             * guarantees that all the writes to the are of the file covered
             * by old head are dispatched in order. It is ok to release the
             * semaphore if it is guaranteed that all subsequent writes will
             * not be in the area of the prev head.
             *
             * This is not the case when we are copying the remainder from
             * the old head. In this case the new head will be written to
             * the same file offset range as the previous head this is why
             * we MUST NOT release the semaphore here.
             */

            auto new_head = co_await _opts.resources.chunks().get();

            // append() calls are serialized, so only flush() may have touched
            // the head while the chunk allocation was pending. A partial head
            // remains installed when flushed.
            vassert(
              _head == old_head,
              "Head changed while waiting for a replacement chunk: {}",
              *this);

            const auto remainder_sz = new_head->copy_remainder_from(*old_head);
            // swap in the new head with the remainder from the old one.

            _head = std::move(new_head);
            _opts.shared_stats->bytes_copied_in_chunk_remainder += remainder_sz;
            /**
             * Release the old head, or mark it for release when its last
             * write completes. A concurrent flush may have added a write for
             * old_head while the chunk request was pending -- possibly still
             * QUEUED, the dispatched one being what sent us here -- so use
             * the current last write.
             */

            if (!_inflight.empty() && _inflight.back()->chunk == old_head) {
                // chunk write isn't finished yet
                _inflight.back()->last_write_to_current_chunk = true;
            } else {
                // All writes using the old chunk are done, so it can be
                // released right away.
                old_head->reset();
                _opts.resources.chunks().add(old_head);
            }
        }

        if (likely(_head)) {
            const size_t written = _head->append(buf, n);
            _bytes_flush_pending += written;
            if (_head->is_full()) {
                dispatch_background_head_write();
            }
            buf += written;
            n -= written;
        }

        if (n == 0) {
            co_return;
        }

        ++_opts.shared_stats->split_writes;

        // barrier. do not hold the units!
        auto units = co_await ss::get_units(_concurrent_flushes, 1);
        units.return_all();

        auto chunk = co_await _opts.resources.chunks().get();
        vassert(!_head, "cannot overwrite existing chunk");
        _head = std::move(chunk);
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
         * append: data was sitting in the write-behind buffer. the segment
         * appears to be inactive so go ahead and write that data to disk.
         */
        dispatch_background_head_write();
    }

    /*
     * inactive segment chunk reclaim
     *
     * if we can ensure there are no outstanding writes (including the
     * one that may have been dispatched above in this handler) then we
     * can reclaim the chunk and return it to the cache. but we need a
     * retry loop because the background write may take some time and it
     * steals _head until it completes. it may also return the chunk to
     * the cache if it empty.
     */
    if (_concurrent_flushes.try_wait(ss::semaphore::max_counter())) {
        if (_head && !_head->bytes_pending()) {
            _opts.resources.chunks().add(std::exchange(_head, nullptr));
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
     * 2. the file handle DMA read must be the full dma alignment even
     * if it returns less bytes, and even if it is the last page
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
      .dma_read(sz, buff, read_align /*must be full _write_ alignment*/)
#pragma clang diagnostic pop
      .then([this, bytes_to_read](size_t actual) {
          ++_opts.shared_stats->last_page_hydrations;
          vassert(
            bytes_to_read <= actual && bytes_to_read == _head->flushed_pos(),
            "Could not hydrate partial page bytes: expected:{}, "
            "got:{}. "
            "chunk:{} - appender:{}",
            bytes_to_read,
            actual,
            *_head,
            *this);
      })
      .handle_exception([this](std::exception_ptr e) {
          vassert(
            false,
            "Could not read the last half page in dma_write_alignment: "
            "{} - {}",
            e,
            *this);
      });
}

ss::future<> segment_appender::do_truncation(size_t n) {
    return _out.truncate(n)
      .then([this] {
          ++_opts.shared_stats->truncates;
          return _out.flush().then([this] { ++_opts.shared_stats->fsyncs; });
      })
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
      "Cannot ask to truncate at:{} which is more bytes than we "
      "have:{} - {}",
      file_byte_offset(),
      n,
      *this);
    _inactive_timer.cancel();
    return hard_flush()
      .then([this, n] { return do_truncation(n); })
      .then([this, n] {
          _committed_offset = n;
          _fallocation_offset = n;
          _flushed_offset = n;
          _stable_offset = n;
          auto f = ss::now();
          if (_head) {
              // NOTE: Important to reset chunks for offset accounting.
              // reset any partial state, since after the truncate, it
              // makes no sense to keep any old state/pointers/sizes,
              // etc
              _head->reset();
          } else {
              // https://github.com/redpanda-data/redpanda/issues/43
              f = _opts.resources.chunks().get().then(
                [this](ss::lw_shared_ptr<chunk> chunk) {
                    _head = std::move(chunk);
                });
          }
          return f.then([this] { return hydrate_last_half_page(); });
      })
      .then([this] {
          _inactive_timer.arm(
            config::shard_local_cfg().segment_appender_flush_timeout_ms());
      });
}

ss::future<> segment_appender::close() {
    vassert(!_closed, "close() on closed segment: {}", *this);
    _closed = true;
    _inactive_timer.cancel();
    return hard_flush()
      .then([this] { return do_truncation(_committed_offset); })
      .then([this] {
          _fallocation_offset = _committed_offset;
          _flushed_offset = _committed_offset;
          _stable_offset = _committed_offset;
          return _out.close();
      });
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
                 // step - compute step rounded to alignment(4096); this
                 // is needed because during a truncation the follow up
                 // fallocation might not be page aligned
                 if (_fallocation_offset % fallocation_alignment != 0) {
                     // add left over bytes to a full page
                     step += fallocation_alignment
                             - (_fallocation_offset % fallocation_alignment);
                 }

                 vassert(
                   _fallocation_offset >= _committed_offset,
                   "Attempting to fallocate at {} below the committed "
                   "offset "
                   "{}",
                   _fallocation_offset,
                   _committed_offset);
                 return _out.allocate(_fallocation_offset, step)
                   .then([this, step] {
                       ++_opts.shared_stats->fallocations;
                       // ss::file::allocate does not adjust logical
                       // file size hence we need to do that explicitly
                       // with an extra truncate. This allows for more
                       // efficient writes.
                       // https://github.com/redpanda-data/redpanda/pull/18598.
                       return _out.truncate(_fallocation_offset + step);
                   })
                   .then([this, step] { _fallocation_offset += step; });
             })
      .handle_exception([this](std::exception_ptr e) {
          vassert(
            false,
            "We failed to fallocate file. This usually means we have "
            "ran out "
            "of disk space. Please check your data partition and "
            "ensure you "
            "have enough space. Error: {} - {}",
            e,
            *this);
      });
}

ss::future<> segment_appender::maybe_advance_stable_offset() {
    vassert(!_inflight.empty(), "expected non-empty inflight set");

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
    if (_committed_offset_clb) {
        _committed_offset_clb(*committed);
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
        // Inflight_dispatched is incremented right before a write is
        // dispatched and then must be decremented when the write is
        // "finished", where we don't consider the write finished
        // until any associated flush operations that were triggered
        // as part of write completion (i.e., stuff in this method)
        // are complete.
        //
        // We also don't want to decrement this too late, i.e., in a
        // continuation attached the write completion path (which
        // would be easier), because then it might be non-zero
        // unexpectedly as observed by a client do does an append +
        // flush and waits for the futures to resolve: the flush
        // future resolves immediately below in the set_value loop,
        // but the future returned by *this* method may resolve later,
        // after the client observes a non-zero value. So we decrement
        // the counter here, *after* the flush has completed but
        // before we set the futures which have been returned to the
        // callers.
        //
        // Unfortunately this means we need to decrement this counter
        // in multiple places.
        --_inflight_dispatched;
        _flushed_offset = committed;
        ++_opts.shared_stats->fsyncs;
        /*
         * TODO: as an optimization, add a little house keeping to
         * determine if eligible flush operations showed up while
         * flush() was completing.
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
      .last_write_to_current_chunk = _head->is_full(),
      .chunk = _head,
      .chunk_begin = _head->pending_aligned_begin(),
      .chunk_end = _head->pending_aligned_end(),
      .file_start_offset = start_offset,
      .committed_offset = _committed_offset,
      .alignment = _head->alignment(),
    };

    // background write
    _head->flush();

    auto head_sem = _prev_head_write;

    if (entry.last_write_to_current_chunk) {
        /*
         * If _head is full then this is the last write to this chunk,
         * so we clear out the head pointer synchronously here, then
         * release it back into the chunk cache after the write
         * completes. Otherwise, leave it in place so that new appends
         * may accumulate. this optimization is meant to avoid
         * rehydrating the chunk on append following a flush when the
         * head has pending bytes and a write is dispatched.
         */
        _head = nullptr;
        /*
         * When the head becomes full it still needs to be properly
         * sequenced with earlier writes to the same head, but no future
         * writes to same head head are possible so the dependency chain
         * is reset for the next head.
         */
        _prev_head_write = ss::make_lw_shared<ssx::semaphore>(1, head_sem_name);
    }

    if (!_inflight.empty() && _inflight.back()->try_merge(entry, prior_co)) {
        // Yay! The latest in-flight write is still queued (i.e., has
        // not been dispatched to the disk) so we just append this write
        // to that entry.
        ++_opts.shared_stats->merged_writes;
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

                /*
                 * At most one write per chunk is in flight: a chunk's writes
                 * all queue on one _prev_head_write - it is swapped only
                 * when the head fills, which retires the chunk - and each
                 * holds its unit until after complete().
                 */
                w->dispatch();
                ++_inflight_dispatched;
                ++_dispatched_writes;

                return _out
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
                  .dma_write(
                    w->file_start_offset,
                    w->chunk->data() + w->chunk_begin,
                    dma_size)
#pragma clang diagnostic pop
                  .then([this, w, dma_size](size_t got) {
                      _opts.shared_stats->bytes_written += dma_size;
                      ++_opts.shared_stats->writes_completed;
                      w->complete();

                      /*
                       * the continuation that captured full=true is the
                       * end of the dependency chain for this chunk. it
                       * can be returned to cache.
                       */
                      if (w->last_write_to_current_chunk) {
                          w->chunk->reset();
                          _opts.resources.chunks().add(w->chunk);
                      }

                      // release our reference to the chunk since this
                      // structure might hang around for a while in the
                      // _inflight list but we can free this chunk to
                      // re-use now as we won't use it again
                      w->chunk = nullptr;

                      const auto expected = w->chunk_end - w->chunk_begin;
                      if (unlikely(expected != got)) {
                          return size_mismatch_error(
                            "chunk::write", expected, got);
                      }
                      return maybe_advance_stable_offset();
                  })
                  .finally([u = std::move(u)] {
                      // You might be tempted to release head_sem's units in
                      // the continuation above, once dma_write completes,
                      // rather than after a potential flush (part of
                      // `maybe_advance_stable_offset`). Holding them delays
                      // the next write for the same chunk, so more appends
                      // merge into that write. Benchmarks show higher
                      // throughput and lower latency.
                  });
            })
            .finally([head_sem] {});
      })
      .handle_exception([this](std::exception_ptr e) {
          vunreachable("Could not dma_write: {} - {}", e, *this);
      });
}

ss::future<> segment_appender::flush() {
    ++_opts.shared_stats->flushes;

    // dispatched write will drive flush completion
    if (_head && _head->bytes_pending()) {
        auto& w = _flush_ops.emplace_back(file_byte_offset());
        // get future first, as the flush_op may be deleted/moved when
        // dispatching background head write.
        auto f = w.p.get_future();
        dispatch_background_head_write();
        return f;
    }

    if (file_byte_offset() <= _flushed_offset) {
        return ss::now();
    }

    /*
     * if there are inflight write _ops_ then we can be sure that flush
     * ops will be processed eventually (see: maybe advance stable
     * offset).
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

    return _out.flush()
      .then([this] { ++_opts.shared_stats->fsyncs; })
      .handle_exception([this](std::exception_ptr e) {
          vunreachable("Could not flush: {} - {}", e, *this);
      });
}

ss::future<> segment_appender::hard_flush() {
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
                 return _out.flush().then(
                   [this] { ++_opts.shared_stats->fsyncs; });
             })
      .handle_exception([this](std::exception_ptr e) {
          vunreachable("Could not flush: {} - {}", e, *this);
      });
}

bool segment_appender::inflight_write::try_merge(
  const inflight_write& other, size_t pco) {
    if (state == QUEUED && chunk == other.chunk) {
        // this next check is an assert rather than a check since we
        // always expect the writes to match up (i.e., right bound of
        // prior write matches the left bound of the current one), since
        // the in-flight writes should form a contiguous series of
        // writes and we only check the last write for merging.
        vassert(
          committed_offset == pco,
          "in try_merge writes didn't touch: {} {}",
          committed_offset,
          pco);

        // the lhs write cannot be full since then how could the next
        // write share its chunk: it must use a new chunk
        vassert(!last_write_to_current_chunk, "the lhs write cannot be full");

        // lhs chunk cannot start or end after rhs
        vassert(
          chunk_begin <= other.chunk_begin && chunk_end <= other.chunk_end,
          "lhs {}-{}, rhs: {}-{}",
          chunk_begin,
          chunk_end,
          other.chunk_begin,
          other.chunk_end);

        // we merge this write in by updating everything associated with
        // the right boundary
        committed_offset = other.committed_offset;
        chunk_end = other.chunk_end;

        // the only possible change here is false -> true, which occurs
        // when the rhs completes the chunk
        last_write_to_current_chunk = other.last_write_to_current_chunk;

        return true;
    }
    return false;
}

fmt::iterator segment_appender::format_to(fmt::iterator iterator) const {
    return fmt::format_to(
      iterator,
      "{{closed:{}, fallocation_offset:{}, stable_offset:{}, "
      "flushed_offset:{}, committed_offset:{}, inflight: {}, "
      "bytes_flush_pending:{}}}",
      _closed,
      _fallocation_offset,
      _stable_offset,
      _flushed_offset,
      _committed_offset,
      _inflight.size(),
      _bytes_flush_pending);
}

fmt::iterator segment_appender::stats::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ merged_writes: {}, bytes_requested : {}, bytes_written : {}, "
      "appends "
      ": {}, flushes: {}, fsyncs: {}, truncates: {}, fallocations: {}, "
      "last_page_hydrations: {}, split_writes: {}, writes_completed: {}}}",
      merged_writes,
      bytes_requested,
      bytes_written,
      appends,
      flushes,
      fsyncs,
      truncates,
      fallocations,
      last_page_hydrations,
      split_writes,
      writes_completed);
}

} // namespace storage
