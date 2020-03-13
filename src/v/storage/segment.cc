#include "storage/segment.h"

#include "storage/logger.h"
#include "storage/segment_appender_utils.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>

#include <stdexcept>

namespace storage {

segment::segment(
  segment_reader r,
  segment_index i,
  std::optional<segment_appender> a,
  std::optional<batch_cache_index> c) noexcept
  : _reader(std::move(r))
  , _idx(std::move(i))
  , _appender(std::move(a))
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
        auto fut = do_close().finally([h = std::move(h)] {});
        if (_tombstone) {
            std::vector<ss::sstring> rm;
            rm.push_back(reader().filename());
            rm.push_back(index().filename());
            fut = fut.then([rm = std::move(rm)] {
                return ss::parallel_for_each(rm, [](const ss::sstring& name) {
                    return ss::remove_file(name).handle_exception(
                      [](std::exception_ptr e) {
                          vlog(
                            stlog.info, "error removing segment files: {}", e);
                      });
                });
            });
        }
        return fut;
    });
}
ss::future<> segment::do_close() {
    auto f = _reader.close();
    if (_appender) {
        f = f.then([this] { return _appender->close(); });
    }
    // after appender flushes to make sure we make things visible
    // only after appender flush
    f = f.then([this] { return _idx.close(); });
    return f;
}

ss::future<> segment::release_appender() {
    vassert(_appender, "cannot release a null appender");
    return flush()
      .then([this] { return _appender->close(); })
      .then([this] { return _idx.flush(); })
      .then([this] {
          _appender = std::nullopt;
          _cache = std::nullopt;
      });
}

ss::future<> segment::flush() {
    check_segment_not_closed("flush()");
    if (!_appender) {
        return ss::make_ready_future<>();
    }
    auto o = dirty_offset();
    auto bytes = _appender->file_byte_offset();
    return _appender->flush().then([this, o, bytes] {
        _reader.set_last_written_offset(o);
        _reader.set_last_visible_byte_offset(bytes);
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
    _dirty_offset = prev_last_offset;
    _reader.set_last_written_offset(_dirty_offset);
    _reader.set_last_visible_byte_offset(physical);
    cache_truncate(prev_last_offset + model::offset(1));
    auto f = _idx.truncate(prev_last_offset);
    // physical file only needs *one* truncation call
    if (_appender) {
        f = f.then([this, physical] { return _appender->truncate(physical); });
    } else {
        f = f.then([this, physical] { return _reader.truncate(physical); });
    }
    return f;
}

void segment::cache_truncate(model::offset offset) {
    check_segment_not_closed("cache_truncate()");
    if (likely(bool(_cache))) {
        _cache->truncate(offset);
    }
}

ss::future<append_result> segment::append(model::record_batch b) {
    check_segment_not_closed("append()");
    return ss::do_with(std::move(b), [this](model::record_batch& b) {
        const auto start_physical_offset = _appender->file_byte_offset();
        // proxy serialization to segment_appender_utils
        return write(*_appender, b).then([this, &b, start_physical_offset] {
            _dirty_offset = b.last_offset();
            const auto end_physical_offset = _appender->file_byte_offset();
            vassert(
              end_physical_offset
                == start_physical_offset + b.header().size_bytes,
              "size must be deterministic: end_offset:{}, expected:{}",
              end_physical_offset,
              start_physical_offset + b.header().size_bytes);
            // index the write
            _idx.maybe_track(b.header(), start_physical_offset);
            auto ret = append_result{.base_offset = b.base_offset(),
                                     .last_offset = b.last_offset(),
                                     .byte_size = (size_t)b.size_bytes()};
            cache_put(std::move(b));
            return ret;
        });
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

std::ostream& operator<<(std::ostream& o, const segment& h) {
    o << "{reader=" << h._reader << ", dirty_offset:" << h.dirty_offset()
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
    return o << ", closed=" << h._closed << ", tombstone=" << h._tombstone
             << ", index=" << h.index() << "}";
}

} // namespace storage
