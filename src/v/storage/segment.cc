// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment.h"

#include "compression/compression.h"
#include "config/configuration.h"
#include "ssx/future-util.h"
#include "storage/compacted_index_writer.h"
#include "storage/file_sanitizer.h"
#include "storage/fs_utils.h"
#include "storage/fwd.h"
#include "storage/logger.h"
#include "storage/parser_utils.h"
#include "storage/readers_cache.h"
#include "storage/segment_appender_utils.h"
#include "storage/segment_set.h"
#include "storage/segment_utils.h"
#include "storage/storage_resources.h"
#include "storage/types.h"
#include "storage/version.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>

#include <optional>
#include <stdexcept>
#include <utility>

namespace storage {

segment::segment(
  segment::offset_tracker tkr,
  segment_reader_ptr r,
  segment_index i,
  segment_appender_ptr a,
  std::optional<compacted_index_writer> ci,
  std::optional<batch_cache_index> c,
  storage_resources& resources,
  segment::generation_id gen) noexcept
  : _resources(resources)
  , _appender_callbacks(this)
  , _generation_id(gen)
  , _tracker(tkr)
  , _reader(std::move(r))
  , _idx(std::move(i))
  , _appender(std::move(a))
  , _compaction_index(std::move(ci))
  , _cache(std::move(c))
  , _first_write(std::nullopt) {
    if (_appender) {
        _appender->set_callbacks(&_appender_callbacks);
    }
}

void segment::check_segment_not_closed(const char* msg) {
    if (unlikely(is_closed())) {
        throw std::runtime_error(fmt::format(
          "Attempted to perform operation: '{}' on a closed segment: {}",
          msg,
          *this));
    }
}

ss::future<> segment::close() {
    check_segment_not_closed("closed()");
    set_close();
    /**
     * close() is considered a destructive operation. All future IO on this
     * segment is unsafe. write_lock() ensures that we want for any active
     * readers and writers to finish before performing a destructive operation
     *
     * the gate should be closed without the write lock because there may be a
     * pending background roll operation that requires the write lock.
     */
    vlog(stlog.trace, "closing segment: {} ", *this);
    co_await _gate.close();
    auto locked = co_await write_lock();

    auto units = co_await _resources.get_close_flush_units();

    co_await do_flush();
    co_await do_close();

    if (is_tombstone()) {
        auto size = co_await remove_persistent_state();
        vlog(
          stlog.debug,
          "Removed {} bytes for tombstone segment {}",
          size,
          *this);
    }

    clear_cached_disk_usage();
}

ss::future<usage> segment::persistent_size() {
    usage u;

    /*
     * accumulate the size of the segment file and each index. because the
     * segment appender will transparently extend the size of the segment file
     * using fallocate, always stat the on disk size for the head partition.
     */
    if (!_appender && _data_disk_usage_size.has_value()) {
        u.data = _data_disk_usage_size.value();
    } else {
        try {
            _data_disk_usage_size = co_await ss::file_size(
              _reader->path().string());
            u.data = _data_disk_usage_size.value();
        } catch (...) {
        }
    }

    u.index = co_await _idx.disk_usage();

    /*
     * lazy load and cache the compaction index size. we could track this
     * continually at relevant segments event like open, roll, and compact.
     * however, we pay for that stat() with no guarantee that the information
     * will be used.
     */
    if (_compaction_index_size.has_value()) {
        u.compaction = _compaction_index_size.value();
    } else {
        auto path = reader().path().to_compacted_index();
        try {
            _compaction_index_size = co_await ss::file_size(path.string());
            u.compaction = _compaction_index_size.value();
        } catch (...) {
        }
    }

    co_return u;
}

ss::future<size_t>
segment::remove_persistent_state(std::filesystem::path path) {
    size_t file_size = 0;
    try {
        file_size = co_await ss::file_size(path.c_str());
    } catch (...) {
        /*
         * the worst case ignoring this exception is under reporting size
         * removed. but we don't want to hold up removal below, which will
         * likely throw anyway and log a helpful error.
         */
    }

    try {
        co_await ss::remove_file(path.c_str());
        vlog(stlog.debug, "removed: {} size {}", path, file_size);
        co_return file_size;
    } catch (const std::filesystem::filesystem_error& e) {
        // be quiet about ENOENT, we want idempotent deletes
        const auto level = e.code() == std::errc::no_such_file_or_directory
                             ? ss::log_level::trace
                             : ss::log_level::info;
        vlogl(stlog, level, "error removing {}: {}", path, e);
    } catch (const std::exception& e) {
        vlog(stlog.info, "error removing {}: {}", path, e);
    }

    co_return 0;
}

void segment::clear_cached_disk_usage() {
    _idx.clear_cached_disk_usage();
    _data_disk_usage_size.reset();
    _compaction_index_size.reset();
}

ss::future<size_t> segment::remove_persistent_state() {
    vassert(is_closed(), "Cannot clear state from unclosed segment");

    clear_cached_disk_usage();

    /*
     * the compaction index is included in the removal list, even if the topic
     * isn't compactible, because compaction can be enabled or disabled at
     * runtime. if the index doesn't exist, it's silently ignored.
     */
    const auto rm = std::to_array<std::filesystem::path>({
      reader().path(),
      index().path(),
      reader().path().to_compacted_index(),
    });

    co_return co_await ss::map_reduce(
      rm,
      [this](std::filesystem::path path) {
          return remove_persistent_state(std::move(path));
      },
      size_t(0),
      std::plus<>());
}

ss::future<> segment::do_close() {
    auto f = _reader->close();
    if (_appender) {
        f = f.then([this] { return _appender->close(); });
    }
    if (_compaction_index) {
        f = f.then([this] { return _compaction_index->close(); });
    }
    // after appender flushes to make sure we make things visible
    // only after appender flush
    f = f.then([this] { return _idx.flush(); });
    return f;
}

ss::future<> segment::do_release_appender(
  segment_appender_ptr appender,
  std::optional<batch_cache_index> cache,
  std::optional<compacted_index_writer> compacted_index) {
    return ss::do_with(
      std::move(appender),
      std::move(compacted_index),
      [this, cache = std::move(cache)](
        segment_appender_ptr& appender,
        std::optional<compacted_index_writer>& compacted_index) {
          return appender->close()
            .then([this] { return _idx.flush(); })
            .then([this, &compacted_index] {
                if (compacted_index) {
                    return compacted_index->close();
                }
                clear_cached_disk_usage();
                return ss::now();
            });
      });
}

ss::future<> segment::release_appender(readers_cache* readers_cache) {
    vassert(_appender, "cannot release a null appender");
    /*
     * If we are able to get the write lock then proceed with the normal
     * appender release process.  Otherwise, schedule the destructive operations
     * that require the write lock to be run in the background in order to avoid
     * blocking segment rolling.
     *
     * An exception safe variant of try write lock is simulated since seastar
     * does not have such primitives available on the semaphore. The fast path
     * of try_write_lock is combined with immediately releasing the lock (which
     * will not also not signal any waiters--there cannot be any!) to guarantee
     * that the blocking get_units version will find the lock uncontested.
     *
     * TODO: we should upstream get_units try-variants for semaphore and rwlock.
     */
    if (_destructive_ops.try_write_lock()) {
        _destructive_ops.write_unlock();
        return write_lock().then([this](ss::rwlock::holder h) {
            return do_flush()
              .then([this] {
                  auto a = std::exchange(_appender, nullptr);
                  auto c
                    = config::shard_local_cfg().release_cache_on_segment_roll()
                        ? std::exchange(_cache, std::nullopt)
                        : std::nullopt;
                  auto i = std::exchange(_compaction_index, std::nullopt);
                  return do_release_appender(
                    std::move(a), std::move(c), std::move(i));
              })
              .finally([h = std::move(h)] {});
        });
    } else {
        return read_lock().then([this, readers_cache](ss::rwlock::holder h) {
            return do_flush()
              .then([this, readers_cache] {
                  release_appender_in_background(readers_cache);
              })
              .finally([h = std::move(h)] {});
        });
    }
}

void segment::release_appender_in_background(readers_cache* readers_cache) {
    auto a = std::exchange(_appender, nullptr);
    auto c = config::shard_local_cfg().release_cache_on_segment_roll()
               ? std::exchange(_cache, std::nullopt)
               : std::nullopt;
    auto i = std::exchange(_compaction_index, std::nullopt);
    ssx::spawn_with_gate(
      _gate,
      [this,
       readers_cache,
       a = std::move(a),
       c = std::move(c),
       i = std::move(i)]() mutable {
          return readers_cache
            ->evict_range(_tracker.base_offset, _tracker.dirty_offset)
            .then(
              [this, a = std::move(a), c = std::move(c), i = std::move(i)](
                readers_cache::range_lock_holder readers_cache_lock) mutable {
                  return ss::do_with(
                           std::move(readers_cache_lock),
                           [this](auto&) { return write_lock(); })
                    .then([this,
                           a = std::move(a),
                           c = std::move(c),
                           i = std::move(i)](ss::rwlock::holder h) mutable {
                        return do_release_appender(
                                 std::move(a), std::move(c), std::move(i))
                          .finally([h = std::move(h)] {});
                    });
              });
      });
}

ss::future<> segment::flush() {
    check_segment_not_closed("flush()");
    return read_lock().then([this](ss::rwlock::holder h) {
        return do_flush().finally([h = std::move(h)] {});
    });
}
ss::future<> segment::do_flush() {
    _generation_id++;
    if (!_appender) {
        return ss::make_ready_future<>();
    }
    auto o = _tracker.dirty_offset;
    auto fsize = _appender->file_byte_offset();
    return _appender->flush().then([this, o, fsize] {
        // never move committed offset backward, there may be multiple
        // outstanding flushes once the one executed later in terms of offset
        // finishes we guarantee that all previous flushes finished.
        _tracker.committed_offset = std::max(o, _tracker.committed_offset);
        _tracker.stable_offset = _tracker.committed_offset;
        _reader->set_file_size(std::max(fsize, _reader->file_size()));
        clear_cached_disk_usage();
    });
}

ss::future<> remove_compacted_index(const segment_full_path& reader_path) {
    auto path = reader_path.to_compacted_index();
    return ss::remove_file(path.string())
      .handle_exception([path](const std::exception_ptr& e) {
          try {
              rethrow_exception(e);
          } catch (const std::filesystem::filesystem_error& e) {
              if (e.code() == std::errc::no_such_file_or_directory) {
                  // Do not log: ENOENT on removal is success
                  return;
              }
          }
          vlog(stlog.warn, "error removing compacted index {} - {}", path, e);
      });
}

ss::future<> segment::truncate(
  model::offset prev_last_offset,
  size_t physical,
  model::timestamp new_max_timestamp) {
    check_segment_not_closed("truncate()");
    return write_lock().then(
      [this, prev_last_offset, physical, new_max_timestamp](
        ss::rwlock::holder h) {
          return do_truncate(prev_last_offset, physical, new_max_timestamp)
            .finally([this, h = std::move(h)] { clear_cached_disk_usage(); });
      });
}

ss::future<> segment::do_truncate(
  model::offset prev_last_offset,
  size_t physical,
  model::timestamp new_max_timestamp) {
    _tracker.committed_offset = prev_last_offset;
    _tracker.stable_offset = prev_last_offset;
    _tracker.dirty_offset = prev_last_offset;
    _reader->set_file_size(physical);
    vlog(
      stlog.trace,
      "truncating segment {} at {}",
      _reader->filename(),
      prev_last_offset);
    _generation_id++;
    cache_truncate(prev_last_offset + model::offset(1));
    auto f = ss::now();
    if (is_compacted_segment()) {
        // if compaction index is opened close it
        if (_compaction_index) {
            f = ss::do_with(
              std::exchange(_compaction_index, std::nullopt),
              [](std::optional<compacted_index_writer>& c) {
                  return c->close();
              });
        }
        // always remove compaction index when truncating compacted segments
        f = f.then([this] { return remove_compacted_index(_reader->path()); });
    }

    f = f.then([this, prev_last_offset, new_max_timestamp] {
        return _idx.truncate(prev_last_offset, new_max_timestamp);
    });

    // physical file only needs *one* truncation call
    if (_appender) {
        f = f.then([this, physical] { return _appender->truncate(physical); });
        // release appender to force segment roll
        if (is_compacted_segment()) {
            f = f.then([this] {
                auto appender = std::exchange(_appender, nullptr);
                auto cache = std::exchange(_cache, std::nullopt);
                auto c_idx = std::exchange(_compaction_index, std::nullopt);
                return do_release_appender(
                  std::move(appender), std::move(cache), std::move(c_idx));
            });
        }
    } else {
        f = f.then([this, physical] { return _reader->truncate(physical); });
    }

    return f;
}

ss::future<bool> segment::materialize_index() {
    vassert(
      _tracker.base_offset == model::next_offset(_tracker.dirty_offset),
      "Materializing the index must happen before tracking any data. {}",
      *this);
    return _idx.materialize_index().then([this](bool yn) {
        if (yn) {
            _tracker.committed_offset = _idx.max_offset();
            _tracker.stable_offset = _idx.max_offset();
            _tracker.dirty_offset = _idx.max_offset();
        }
        return yn;
    });
}

void segment::cache_truncate(model::offset offset) {
    check_segment_not_closed("cache_truncate()");
    if (likely(bool(_cache))) {
        _cache->truncate(offset);
    }
}
ss::future<> segment::do_compaction_index_batch(const model::record_batch& b) {
    vassert(!b.compressed(), "wrong method. Call compact_index_batch. {}", b);
    auto& w = compaction_index();
    return model::for_each_record(
      b,
      [o = b.base_offset(), batch_type = b.header().type, &w](
        const model::record& r) {
          return w.index(batch_type, r.key(), o, r.offset_delta());
      });
}
ss::future<> segment::compaction_index_batch(const model::record_batch& b) {
    if (!has_compaction_index()) {
        co_return;
    }
    // do not index not compactible batches
    if (!internal::is_compactible(b)) {
        co_return;
    }

    if (!b.compressed()) {
        co_return co_await do_compaction_index_batch(b);
    }

    // Compressed batches have to be uncompressed before we can index them
    // by key for compaction.  This is potentially _very_ expensive in memory:
    // clients can simply send us 100MiB of zeros, which will compress small
    // enough to pass batch size checks, but consume huge amounts of memory
    // in this step.
    //
    // To mitigate this, we tightly limit how many of these we will do in
    // parallel.  Users should consider _not_ using compression on their
    // compacted topics, and/or avoiding huge batches on compacted topics.
    auto units = co_await _resources.get_compaction_compression_units();

    auto decompressed = co_await internal::decompress_batch(b);

    co_return co_await do_compaction_index_batch(decompressed);
}

ss::future<append_result> segment::do_append(const model::record_batch& b) {
    check_segment_not_closed("append()");
    vassert(
      b.base_offset() >= _tracker.base_offset,
      "Invalid state. Attempted to append a batch with base_offset:{}, but "
      "would invalidate our initial state base offset of:{}. Actual batch "
      "header:{}, self:{}",
      b.base_offset(),
      _tracker.base_offset,
      b.header(),
      *this);
    vassert(
      b.header().ctx.owner_shard,
      "Shard not set when writing to: {} - header: {}",
      *this,
      b.header());
    if (unlikely(b.compressed() && !b.header().attrs.is_valid_compression())) {
        return ss::make_exception_future<
          append_result>(std::runtime_error(fmt::format(
          "record batch marked as compressed, but has no valid compression:{}",
          b.header())));
    }
    const auto start_physical_offset = _appender->file_byte_offset();
    _generation_id++;
    // proxy serialization to segment_appender
    auto write_fut = _appender->append(b).then(
      [this, &b, start_physical_offset] {
          _tracker.dirty_offset = b.last_offset();
          const auto end_physical_offset = _appender->file_byte_offset();
          const auto expected_end_physical = start_physical_offset
                                             + b.header().size_bytes;
          vassert(
            end_physical_offset == expected_end_physical,
            "size must be deterministic: end_offset:{}, expected:{}, "
            "batch.header:{} - {}",
            end_physical_offset,
            expected_end_physical,
            b.header(),
            *this);
          // inflight index. trimmed on every dma_write in appender
          _inflight.emplace(end_physical_offset, b.last_offset());
          // index the write
          _idx.maybe_track(b.header(), start_physical_offset);
          auto ret = append_result{
            .base_offset = b.base_offset(),
            .last_offset = b.last_offset(),
            .byte_size = (size_t)b.size_bytes()};
          // cache always copies the batch
          cache_put(b);
          return ret;
      });
    auto index_fut = compaction_index_batch(b);
    return ss::when_all(std::move(write_fut), std::move(index_fut))
      .then([this, batch_type = b.header().type](
              std::tuple<ss::future<append_result>, ss::future<>> p) {
          auto& [append_fut, index_fut] = p;
          const bool index_append_failed = index_fut.failed()
                                           && has_compaction_index();
          const bool has_error = append_fut.failed() || index_append_failed;
          clear_cached_disk_usage();
          if (!has_error) {
              if (
                !this->_first_write.has_value()
                && batch_type == model::record_batch_type::raft_data) {
                  // record time of first write of data batch
                  this->_first_write = ss::lowres_clock::now();
              }
              index_fut.get();
              return std::move(append_fut);
          }
          if (append_fut.failed()) {
              auto append_err = std::move(append_fut).get_exception();
              vlog(stlog.error, "segment::append failed: {}", append_err);
              if (index_fut.failed()) {
                  auto index_err = std::move(index_fut).get_exception();
                  vlog(stlog.error, "segment::append index: {}", index_err);
              }
              return ss::make_exception_future<append_result>(append_err);
          }
          auto ret = append_fut.get0();
          auto index_err = std::move(index_fut).get_exception();
          vlog(
            stlog.error,
            "segment::append index: {}. ignoring append: {}",
            index_err,
            ret);
          return ss::make_exception_future<append_result>(index_err);
      });
}

ss::future<append_result> segment::append(const model::record_batch& b) {
    if (has_compaction_index() && b.header().attrs.is_transactional()) {
        // With transactional batches, we do not know ahead of time whether the
        // batch will be committed or aborted. We may not have this information
        // during the lifetime of this segment as the batch may be aborted in
        // the next segment. We mark this index as `incomplete` and rebuild it
        // later from scratch during compaction.
        try {
            auto index = std::exchange(_compaction_index, std::nullopt);
            index->set_flag(compacted_index::footer_flags::incomplete);
            vlog(
              gclog.info,
              "Marking compaction index {} as incomplete",
              index->filename());
            co_await index->close();
        } catch (...) {
            co_return ss::coroutine::exception(std::current_exception());
        }
    }
    co_return co_await do_append(b);
}

ss::future<append_result> segment::append(model::record_batch&& b) {
    return ss::do_with(std::move(b), [this](model::record_batch& b) mutable {
        return append(b);
    });
}

ss::future<segment_reader_handle>
segment::offset_data_stream(model::offset o, ss::io_priority_class iopc) {
    check_segment_not_closed("offset_data_stream()");
    auto nearest = _idx.find_nearest(o);
    size_t position = 0;
    if (nearest) {
        position = nearest->filepos;
    }

    // This could be a corruption (bad index) or a runtime defect (bad file
    // size) (https://github.com/redpanda-data/redpanda/issues/2101)
    vassert(position < size_bytes(), "Index points beyond file size");

    return _reader->data_stream(position, iopc);
}

void segment::advance_stable_offset(size_t offset) {
    if (_inflight.empty()) {
        return;
    }

    auto it = _inflight.upper_bound(offset);
    if (it != _inflight.begin()) {
        --it;
    }

    if (it->first > offset) {
        return;
    }

    _reader->set_file_size(it->first);
    _tracker.stable_offset = it->second;
    _inflight.erase(_inflight.begin(), std::next(it));

    // after data gets flushed out of the appender recheck on disk size
    clear_cached_disk_usage();
}

std::ostream& operator<<(std::ostream& o, const segment::offset_tracker& t) {
    fmt::print(
      o,
      "{{term:{}, base_offset:{}, committed_offset:{}, dirty_offset:{}}}",
      t.term,
      t.base_offset,
      t.committed_offset,
      t.dirty_offset);
    return o;
}

std::ostream& operator<<(std::ostream& o, const segment& h) {
    o << "{offset_tracker:" << h._tracker
      << ", compacted_segment=" << h.is_compacted_segment()
      << ", finished_self_compaction=" << h.finished_self_compaction()
      << ", generation=" << h.get_generation_id() << ", reader=";
    if (h._reader) {
        o << *h._reader;
    } else {
        o << "nullptr";
    }

    o << ", writer=";
    if (h.has_appender()) {
        o << *h._appender;
    } else {
        o << "nullptr";
    }
    o << ", cache=";
    if (h._cache) {
        o << *h._cache;
    } else {
        o << "nullptr";
    }
    o << ", compaction_index:";
    if (h._compaction_index) {
        o << *h._compaction_index;
    } else {
        o << "nullopt";
    }
    return o << ", closed=" << h.is_closed()
             << ", tombstone=" << h.is_tombstone() << ", index=" << h.index()
             << "}";
}

template<typename Func>
auto with_segment(ss::lw_shared_ptr<segment> s, Func&& f) {
    return f(s).then_wrapped([s](
                               ss::future<ss::lw_shared_ptr<segment>> new_seg) {
        try {
            auto ptr = new_seg.get0();
            return ss::make_ready_future<ss::lw_shared_ptr<segment>>(ptr);
        } catch (...) {
            return s->close()
              .then_wrapped([e = std::current_exception()](ss::future<>) {
                  return ss::make_exception_future<ss::lw_shared_ptr<segment>>(
                    e);
              })
              .finally([s] {});
        }
    });
}

ss::future<ss::lw_shared_ptr<segment>> open_segment(
  segment_full_path path,
  std::optional<batch_cache_index> batch_cache,
  size_t buf_size,
  unsigned read_ahead,
  storage_resources& resources,
  ss::sharded<features::feature_table>& feature_table,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config) {
    if (path.get_version() != record_version_type::v1) {
        throw std::runtime_error(fmt::format(
          "Segment has invalid version {} != {} path {}",
          path.get_version(),
          record_version_type::v1,
          path));
    }

    auto rdr = std::make_unique<segment_reader>(
      path, buf_size, read_ahead, ntp_sanitizer_config);
    co_await rdr->load_size();

    auto idx = segment_index(
      rdr->path().to_index(),
      path.get_base_offset(),
      segment_index::default_data_buffer_step,
      feature_table,
      std::move(ntp_sanitizer_config));

    co_return ss::make_lw_shared<segment>(
      segment::offset_tracker(path.get_term(), path.get_base_offset()),
      std::move(rdr),
      std::move(idx),
      nullptr,
      std::nullopt,
      std::move(batch_cache),
      resources);
}

ss::future<ss::lw_shared_ptr<segment>> make_segment(
  const ntp_config& ntpc,
  model::offset base_offset,
  model::term_id term,
  ss::io_priority_class pc,
  record_version_type version,
  size_t buf_size,
  unsigned read_ahead,
  std::optional<batch_cache_index> batch_cache,
  storage_resources& resources,
  ss::sharded<features::feature_table>& feature_table,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config) {
    auto path = segment_full_path(ntpc, base_offset, term, version);
    vlog(stlog.info, "Creating new segment {}", path);
    return open_segment(
             path,
             std::move(batch_cache),
             buf_size,
             read_ahead,
             resources,
             feature_table,
             ntp_sanitizer_config)
      .then([path, &ntpc, pc, &resources, ntp_sanitizer_config](
              ss::lw_shared_ptr<segment> seg) mutable {
          return with_segment(
            std::move(seg),
            [path, &ntpc, pc, &resources, ntp_sanitizer_config](
              const ss::lw_shared_ptr<segment>& seg) mutable {
                return internal::make_segment_appender(
                         path,
                         internal::number_of_chunks_from_config(ntpc),
                         internal::segment_size_from_config(ntpc),
                         pc,
                         resources,
                         std::move(ntp_sanitizer_config))
                  .then([seg, &resources](segment_appender_ptr a) {
                      return ss::make_ready_future<ss::lw_shared_ptr<segment>>(
                        ss::make_lw_shared<segment>(
                          seg->offsets(),
                          seg->release_segment_reader(),
                          std::move(seg->index()),
                          std::move(a),
                          std::nullopt,
                          seg->has_cache()
                            ? std::optional(std::move(seg->cache()->get()))
                            : std::nullopt,
                          resources));
                  });
            });
      })
      .then([path, &ntpc, pc, &resources, ntp_sanitizer_config](
              ss::lw_shared_ptr<segment> seg) mutable {
          if (!ntpc.is_compacted()) {
              return ss::make_ready_future<ss::lw_shared_ptr<segment>>(seg);
          }
          return with_segment(
            seg,
            [path, pc, &resources, ntp_sanitizer_config](
              const ss::lw_shared_ptr<segment>& seg) mutable {
                auto compacted_path = path.to_compacted_index();
                return internal::make_compacted_index_writer(
                         compacted_path,
                         pc,
                         resources,
                         std::move(ntp_sanitizer_config))
                  .then([seg, &resources](compacted_index_writer compact) {
                      return ss::make_ready_future<ss::lw_shared_ptr<segment>>(
                        ss::make_lw_shared<segment>(
                          seg->offsets(),
                          seg->release_segment_reader(),
                          std::move(seg->index()),
                          seg->release_appender(),
                          std::move(compact),
                          seg->has_cache()
                            ? std::optional(std::move(seg->cache()->get()))
                            : std::nullopt,
                          resources));
                  });
            });
      });
}

ss::future<model::timestamp> segment::get_file_timestamp() const {
    auto file_path = path().string();

    auto stat = co_await ss::file_stat(file_path);
    co_return model::timestamp(
      std::chrono::duration_cast<std::chrono::milliseconds>(
        stat.time_modified.time_since_epoch())
        .count());
}

} // namespace storage
