#include "storage/segment.h"

#include "compression/compression.h"
#include "storage/fs_utils.h"
#include "storage/logger.h"
#include "storage/parser_utils.h"
#include "storage/segment_appender_utils.h"
#include "storage/segment_set.h"
#include "storage/segment_utils.h"
#include "storage/types.h"
#include "storage/version.h"
#include "utils/file_sanitizer.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>

#include <stdexcept>

namespace storage {

segment::segment(
  segment::offset_tracker tkr,
  segment_reader r,
  segment_index i,
  std::optional<segment_appender> a,
  std::optional<compacted_index_writer> ci,
  std::optional<batch_cache_index> c) noexcept
  : _tracker(tkr)
  , _reader(std::move(r))
  , _idx(std::move(i))
  , _appender(std::move(a))
  , _compaction_index(std::move(ci))
  , _cache(std::move(c)) {}

void segment::check_segment_not_closed(const char* msg) {
    if (unlikely(_closed)) {
        throw std::runtime_error(fmt::format(
          "Attempted to perform operation: '{}' on a closed segment: {}",
          msg,
          *this));
    }
}

ss::future<> segment::close() {
    check_segment_not_closed("closed()");
    _closed = true;
    /**
     * close() is considered a destructive operation. All future IO on this
     * segment is unsafe. write_lock() ensures that we want for any active
     * readers and writers to finish before performing a destructive operation
     */
    return write_lock().then([this](ss::rwlock::holder h) {
        return do_close()
          .then([this] { return remove_thombsones(); })
          .finally([h = std::move(h)] {});
    });
}

ss::future<> segment::remove_thombsones() {
    if (!_tombstone) {
        return ss::make_ready_future<>();
    }
    std::vector<ss::sstring> rm;
    rm.push_back(reader().filename());
    rm.push_back(index().filename());
    if (_compaction_index) {
        rm.push_back(_compaction_index->filename());
    }
    vlog(stlog.info, "removing: {}", rm);
    return ss::do_with(std::move(rm), [](std::vector<ss::sstring>& to_remove) {
        return ss::do_for_each(to_remove, [](const ss::sstring& name) {
            return ss::remove_file(name).handle_exception(
              [](std::exception_ptr e) {
                  vlog(stlog.info, "error removing segment files: {}", e);
              });
        });
    });
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

ss::future<> segment::release_appender() {
    vassert(_appender, "cannot release a null appender");
    return write_lock().then([this](ss::rwlock::holder h) {
        return do_flush()
          .then([this] { return _appender->close(); })
          .then([this] { return _idx.flush(); })
          .then([this] {
              if (_compaction_index) {
                  return _compaction_index->close();
              }
              return ss::now();
          })
          .then([this] {
              _appender = std::nullopt;
              _cache = std::nullopt;
              _compaction_index = std::nullopt;
          })
          .finally([h = std::move(h)] {});
    });
}

ss::future<> segment::flush() {
    return read_lock().then([this](ss::rwlock::holder h) {
        return do_flush().finally([h = std::move(h)] {});
    });
}
ss::future<> segment::do_flush() {
    check_segment_not_closed("flush()");
    if (!_appender) {
        return ss::make_ready_future<>();
    }
    auto o = _tracker.dirty_offset;
    auto fsize = _appender->file_byte_offset();
    return _appender->flush().then([this, o, fsize] {
        _tracker.committed_offset = o;
        _reader.set_file_size(fsize);
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
    _tracker.committed_offset = _tracker.dirty_offset = prev_last_offset;
    _reader.set_file_size(physical);
    cache_truncate(prev_last_offset + model::offset(1));
    auto f = _idx.truncate(prev_last_offset);
    // physical file only needs *one* truncation call
    if (_appender) {
        f = f.then([this, physical] { return _appender->truncate(physical); });
    } else {
        f = f.then([this, physical] { return _reader.truncate(physical); });
    }
    if (_compaction_index) {
        f = f.then([this, prev_last_offset] {
            return _compaction_index->truncate(prev_last_offset);
        });
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
    return ss::do_for_each(
      b.begin(), b.end(), [o = b.base_offset(), &w](const model::record& r) {
          return w.index(r.key(), o, r.offset_delta());
      });
}
ss::future<> segment::compaction_index_batch(const model::record_batch& b) {
    if (!has_compacion_index()) {
        return ss::now();
    }
    if (!b.compressed()) {
        return do_compaction_index_batch(b);
    }
    iobuf body_buf = compression::compressor::uncompress(
      b.get_compressed_records(), b.header().attrs.compression());
    return ss::do_with(
      iobuf_parser(std::move(body_buf)),
      [this, header = b.header()](iobuf_parser& parser) {
          auto begin = boost::make_counting_iterator(uint32_t(0));
          auto end = boost::make_counting_iterator(
            uint32_t(header.record_count));
          const model::offset o = header.base_offset;
          return ss::do_for_each(begin, end, [this, o, &parser](uint32_t) {
              auto rec
                = internal::parse_one_record_from_buffer_using_kafka_format(
                  parser);
              auto k = iobuf_to_bytes(rec.key());
              return compaction_index().index(
                std::move(k), o, rec.offset_delta());
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
              "size must be deterministic: end_offset:{}, expected:{}",
              end_physical_offset,
              expected_end_physical);
            // index the write
            _idx.maybe_track(b.header(), start_physical_offset);
            auto ret = append_result{.base_offset = b.base_offset(),
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
    return _reader.data_stream(position, iopc);
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
    o << "{offset_tracker:" << h._tracker << ", reader=" << h._reader
      << ", writer=";
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
    return o << ", closed=" << h._closed << ", tombstone=" << h._tombstone
             << ", index=" << h.index() << "}";
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
              return ss::make_ready_future<uint64_t, ss::file>(s.st_size, f);
          });
      })
      .then([buf_size, path](uint64_t size, ss::file fd) {
          return std::make_unique<segment_reader>(
            path.string(), std::move(fd), size, buf_size);
      })
      .then([batch_cache = std::move(batch_cache), meta, sanitize_fileops](
              std::unique_ptr<segment_reader> rdr) mutable {
          auto ptr = rdr.get();
          auto index_name = std::filesystem::path(ptr->filename().c_str())
                              .replace_extension("base_index")
                              .string();
          return ss::open_file_dma(
                   index_name, ss::open_flags::create | ss::open_flags::rw)
            .then_wrapped([batch_cache = std::move(batch_cache),
                           ptr,
                           sanitize_fileops,
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
                if (sanitize_fileops) {
                    fd = ss::file(
                      ss::make_shared(file_io_sanitizer(std::move(fd))));
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
                    std::nullopt,
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
  const ss::sstring& base_dir,
  debug_sanitize_files sanitize_fileops,
  std::optional<batch_cache_index> batch_cache) {
    auto path = segment_path::make_segment_path(
      base_dir, ntpc.ntp(), base_offset, term, version);
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
                          std::move(*a),
                          std::nullopt,
                          seg->has_cache()
                            ? std::optional(std::move(seg->cache()))
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
                auto compacted_path = path;
                compacted_path.replace_extension(".compaction_index");
                return internal::make_compacted_index_writer(
                         compacted_path, sanitize_fileops, pc)
                  .then([seg](compacted_index_writer compact) {
                      return ss::make_ready_future<ss::lw_shared_ptr<segment>>(
                        ss::make_lw_shared<segment>(
                          seg->offsets(),
                          std::move(seg->reader()),
                          std::move(seg->index()),
                          std::move(seg->appender()),
                          std::move(compact),
                          seg->has_cache()
                            ? std::optional(std::move(seg->cache()))
                            : std::nullopt));
                  });
            });
      });
}

} // namespace storage
