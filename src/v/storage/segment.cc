// Copyright 2020 Vectorized, Inc.
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
#include "storage/compacted_index_writer.h"
#include "storage/fs_utils.h"
#include "storage/fwd.h"
#include "storage/logger.h"
#include "storage/parser_utils.h"
#include "storage/readers_cache.h"
#include "storage/segment_appender_utils.h"
#include "storage/segment_set.h"
#include "storage/segment_utils.h"
#include "storage/types.h"
#include "storage/version.h"
#include "utils/file_sanitizer.h"
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
  segment_reader r,
  segment_index i,
  segment_appender_ptr a,
  std::optional<compacted_index_writer> ci,
  std::optional<batch_cache_index> c) noexcept
  : _appender_callbacks(this)
  , _tracker(tkr)
  , _reader(std::move(r))
  , _idx(std::move(i))
  , _appender(std::move(a))
  , _compaction_index(std::move(ci))
  , _cache(std::move(c)) {
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
    return _gate.close().then([this] {
        return write_lock().then([this](ss::rwlock::holder h) {
            return do_flush()
              .then([this] { return do_close(); })
              .then([this] { return remove_tombstones(); })
              .finally([h = std::move(h)] {});
        });
    });
}

ss::future<> segment::remove_persistent_state() {
    vassert(is_closed(), "Cannot clear state from unclosed segment");

    std::vector<std::filesystem::path> rm;
    rm.reserve(3);
    rm.emplace_back(reader().filename().c_str());
    rm.emplace_back(index().filename().c_str());
    if (is_compacted_segment()) {
        rm.push_back(
          internal::compacted_index_path(reader().filename().c_str()));
    }
    vlog(stlog.info, "removing: {}", rm);
    return ss::do_with(
      std::move(rm), [](const std::vector<std::filesystem::path>& to_remove) {
          return ss::do_for_each(
            to_remove, [](const std::filesystem::path& name) {
                return ss::remove_file(name.c_str())
                  .handle_exception_type(
                    [name](std::filesystem::filesystem_error& e) {
                        if (e.code() == std::errc::no_such_file_or_directory) {
                            // ignore, we want to make deletes idempotent
                            return;
                        }
                        vlog(stlog.info, "error removing {}: {}", name, e);
                    })
                  .handle_exception([name](std::exception_ptr e) {
                      vlog(stlog.info, "error removing {}: {}", name, e);
                  });
            });
      });
}

ss::future<> segment::remove_tombstones() {
    if (!is_tombstone()) {
        return ss::make_ready_future<>();
    }
    return remove_persistent_state();
}

ss::future<> segment::do_close() {
    auto f = _reader.close();
    if (_appender) {
        f = f.then([this] { return _appender->close(); });
    }
    if (_compaction_index) {
        f = f.then([this] { return _compaction_index->close(); });
    }
    // after appender flushes to make sure we make things visible
    // only after appender flush
    f = f.then([this] { return _idx.close(); });
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
            .then([&compacted_index] {
                if (compacted_index) {
                    return compacted_index->close();
                }
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
     * will not also not signal any waiters--there cannot be any!) to guarnatee
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
    (void)ss::with_gate(
      _gate,
      [this,
       readers_cache,
       a = std::move(a),
       c = std::move(c),
       i = std::move(i)]() mutable {
          return readers_cache
            ->evict_range(_tracker.base_offset, _tracker.dirty_offset)
            .then([this, a = std::move(a), c = std::move(c), i = std::move(i)](
                    readers_cache::range_lock_holder) mutable {
                return write_lock().then(
                  [this, a = std::move(a), c = std::move(c), i = std::move(i)](
                    ss::rwlock::holder h) mutable {
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
        _reader.set_file_size(std::max(fsize, _reader.file_size()));
    });
}

ss::future<> remove_compacted_index(const ss::sstring& reader_path) {
    auto path = internal::compacted_index_path(reader_path.c_str());
    return ss::remove_file(path.c_str())
      .handle_exception([path](const std::exception_ptr& e) {
          vlog(stlog.warn, "error removing compacted index {} - {}", path, e);
      });
}

ss::future<>
segment::truncate(model::offset prev_last_offset, size_t physical) {
    check_segment_not_closed("truncate()");
    return write_lock().then(
      [this, prev_last_offset, physical](ss::rwlock::holder h) {
          return do_truncate(prev_last_offset, physical)
            .finally([h = std::move(h)] {});
      });
}

ss::future<>
segment::do_truncate(model::offset prev_last_offset, size_t physical) {
    _tracker.committed_offset = prev_last_offset;
    _tracker.stable_offset = prev_last_offset;
    _tracker.dirty_offset = prev_last_offset;
    _reader.set_file_size(physical);
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
        f = f.then(
          [this] { return remove_compacted_index(_reader.filename()); });
    }

    f = f.then(
      [this, prev_last_offset] { return _idx.truncate(prev_last_offset); });

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
        f = f.then([this, physical] { return _reader.truncate(physical); });
    }

    return f;
}

ss::future<bool> segment::materialize_index() {
    vassert(
      _tracker.base_offset == _tracker.dirty_offset,
      "Materializing the index must happen tracking any data. {}",
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
      b, [o = b.base_offset(), &w](const model::record& r) {
          return w.index(r.key(), o, r.offset_delta());
      });
}
ss::future<> segment::compaction_index_batch(const model::record_batch& b) {
    if (!has_compaction_index()) {
        return ss::now();
    }
    if (!b.compressed()) {
        return do_compaction_index_batch(b);
    }
    return internal::decompress_batch(b).then([this](model::record_batch&& b) {
        return ss::do_with(std::move(b), [this](model::record_batch& b) {
            return do_compaction_index_batch(b);
        });
    });
}

ss::future<append_result> segment::append(const model::record_batch& b) {
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
    // proxy serialization to segment_appender_utils
    auto write_fut
      = write(*_appender, b).then([this, &b, start_physical_offset] {
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
      .then([](std::tuple<ss::future<append_result>, ss::future<>> p) {
          auto& [append_fut, index_fut] = p;
          const bool has_error = append_fut.failed() || index_fut.failed();
          if (!has_error) {
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
            "segment::append index: {}. ignorning append: {}",
            index_err,
            ret);
          return ss::make_exception_future<append_result>(index_err);
      });
}
ss::future<append_result> segment::append(model::record_batch&& b) {
    return ss::do_with(std::move(b), [this](model::record_batch& b) mutable {
        return append(b);
    });
}

ss::input_stream<char>
segment::offset_data_stream(model::offset o, ss::io_priority_class iopc) {
    check_segment_not_closed("offset_data_stream()");
    auto nearest = _idx.find_nearest(o);
    size_t position = 0;
    if (nearest) {
        position = nearest->filepos;
    }

    // This could be a corruption (bad index) or a runtime defect (bad file
    // size) (https://github.com/vectorizedio/redpanda/issues/2101)
    vassert(position < size_bytes(), "Index points beyond file size");

    return _reader.data_stream(position, iopc);
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

    _reader.set_file_size(it->first);
    _tracker.stable_offset = it->second;
    _inflight.erase(_inflight.begin(), it);
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
      << ", reader=" << h._reader << ", writer=";
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
  const std::filesystem::path& path,
  debug_sanitize_files sanitize_fileops,
  std::optional<batch_cache_index> batch_cache,
  size_t buf_size) {
    auto const meta = segment_path::parse_segment_filename(
      path.filename().string());
    if (!meta || meta->version != record_version_type::v1) {
        return ss::make_exception_future<ss::lw_shared_ptr<segment>>(
          std::runtime_error(fmt::format(
            "Segment has invalid version {} != {} path {}",
            meta->version,
            record_version_type::v1,
            path)));
    }
    // note: this file _must_ be open in `ro` mode only. Seastar uses dma
    // files with no shared buffer cache around them. When we use a writer
    // w/ dma at the same time as the reader, we need a way to synchronize
    // filesytem metadata. In order to prevent expensive synchronization
    // primitives fsyncing both *reads* and *writes* we open this file in ro
    // mode and if raft requires truncation, we open yet-another handle w/
    // rw mode just for the truncation which gives us all the benefits of
    // preventing x-file synchronization This is fine, because truncation to
    // sealed segments are supposed to be very rare events. The hotpath of
    // truncating the appender, is optimized.
    return internal::make_reader_handle(path, sanitize_fileops)
      .then([](ss::file f) {
          return f.stat().then([f](struct stat s) {
              return ss::make_ready_future<std::tuple<uint64_t, ss::file>>(
                std::make_tuple(s.st_size, f));
          });
      })
      .then([buf_size, path](std::tuple<uint64_t, ss::file> t) {
          auto& [size, fd] = t;
          return std::make_unique<segment_reader>(
            path.string(), std::move(fd), size, buf_size);
      })
      .then([batch_cache = std::move(batch_cache), meta, sanitize_fileops](
              std::unique_ptr<segment_reader> rdr) mutable {
          auto ptr = rdr.get();
          auto index_name = std::filesystem::path(ptr->filename().c_str())
                              .replace_extension("base_index")
                              .string();

          return internal::make_handle(
                   index_name,
                   ss::open_flags::create | ss::open_flags::rw,
                   {},
                   sanitize_fileops)
            .then_wrapped([batch_cache = std::move(batch_cache),
                           ptr,
                           rdr = std::move(rdr),
                           index_name,
                           meta](ss::future<ss::file> f) mutable {
                ss::file fd;
                try {
                    fd = f.get0();
                } catch (...) {
                    return ptr->close().then(
                      [rdr = std::move(rdr), e = std::current_exception()] {
                          return ss::make_exception_future<
                            ss::lw_shared_ptr<segment>>(e);
                      });
                }
                auto idx = segment_index(
                  index_name,
                  fd,
                  meta->base_offset,
                  segment_index::default_data_buffer_step);
                return ss::make_ready_future<ss::lw_shared_ptr<segment>>(
                  ss::make_lw_shared<segment>(
                    segment::offset_tracker(meta->term, meta->base_offset),
                    std::move(*rdr),
                    std::move(idx),
                    nullptr,
                    std::nullopt,
                    std::move(batch_cache)));
            });
      });
}

ss::future<ss::lw_shared_ptr<segment>> make_segment(
  const ntp_config& ntpc,
  model::offset base_offset,
  model::term_id term,
  ss::io_priority_class pc,
  record_version_type version,
  size_t buf_size,
  debug_sanitize_files sanitize_fileops,
  std::optional<batch_cache_index> batch_cache) {
    auto path = segment_path::make_segment_path(
      ntpc, base_offset, term, version);
    vlog(stlog.info, "Creating new segment {}", path.string());
    return open_segment(
             path, sanitize_fileops, std::move(batch_cache), buf_size)
      .then([path, &ntpc, sanitize_fileops, pc](
              ss::lw_shared_ptr<segment> seg) {
          return with_segment(
            std::move(seg),
            [path, &ntpc, sanitize_fileops, pc](
              const ss::lw_shared_ptr<segment>& seg) {
                return internal::make_segment_appender(
                         path,
                         sanitize_fileops,
                         internal::number_of_chunks_from_config(ntpc),
                         pc)
                  .then([seg](segment_appender_ptr a) {
                      return ss::make_ready_future<ss::lw_shared_ptr<segment>>(
                        ss::make_lw_shared<segment>(
                          seg->offsets(),
                          std::move(seg->reader()),
                          std::move(seg->index()),
                          std::move(a),
                          std::nullopt,
                          seg->has_cache()
                            ? std::optional(std::move(seg->cache()->get()))
                            : std::nullopt));
                  });
            });
      })
      .then([path, &ntpc, sanitize_fileops, pc](
              ss::lw_shared_ptr<segment> seg) {
          if (!ntpc.is_compacted()) {
              return ss::make_ready_future<ss::lw_shared_ptr<segment>>(seg);
          }
          return with_segment(
            seg,
            [path, sanitize_fileops, pc](
              const ss::lw_shared_ptr<segment>& seg) {
                auto compacted_path = internal::compacted_index_path(path);
                return internal::make_compacted_index_writer(
                         compacted_path, sanitize_fileops, pc)
                  .then([seg](compacted_index_writer compact) {
                      return ss::make_ready_future<ss::lw_shared_ptr<segment>>(
                        ss::make_lw_shared<segment>(
                          seg->offsets(),
                          std::move(seg->reader()),
                          std::move(seg->index()),
                          seg->release_appender(),
                          std::move(compact),
                          seg->has_cache()
                            ? std::optional(std::move(seg->cache()->get()))
                            : std::nullopt));
                  });
            });
      });
}

} // namespace storage
