#include "storage/segment.h"

#include "storage/logger.h"
#include "storage/segment_appender_utils.h"
#include "vassert.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>

namespace storage {

segment::segment(
  segment_reader_ptr r,
  segment_index_ptr i,
  segment_appender_ptr a,
  batch_cache_index_ptr cache) noexcept
  : _reader(std::move(r))
  , _idx(std::move(i))
  , _appender(std::move(a))
  , _cache(std::move(cache)) {
    // TODO(agallego) - add these asserts once we migrate tests
    // vassert(_reader, "segments must have valid readers");
    // vassert(_idx, "segments must have valid offset index");
}

ss::future<> segment::close() {
    auto f = _reader->close();
    if (_appender) {
        f = f.then([this] { return _appender->close(); });
    }
    // after appender flushes to make sure we make things visible
    // only after appender flush
    f = f.then([this] { return _idx->close(); });
    return f;
}

ss::future<> segment::release_appender() {
    vassert(_appender, "cannot release a null appender");
    return flush()
      .then([this] { return _appender->close(); })
      .then([this] { return _idx->flush(); })
      .then([this] {
          _appender = nullptr;
          _cache = nullptr;
      });
}

ss::future<> segment::flush() {
    // should be outside of the appender block
    _reader->set_last_written_offset(dirty_offset());
    if (_appender) {
        return _appender->flush().then([this] {
            _reader->set_last_visible_byte_offset(
              _appender->file_byte_offset());
        });
    }
    return ss::make_ready_future<>();
}
ss::future<>
segment::truncate(model::offset prev_last_offset, size_t physical) {
    _dirty_offset = prev_last_offset;
    _reader->set_last_written_offset(_dirty_offset);
    _reader->set_last_visible_byte_offset(physical);
    cache_truncate(prev_last_offset + model::offset(1));
    auto f = _idx->truncate(prev_last_offset);
    // physical file only needs *one* truncation call
    if (_appender) {
        f = f.then([this, physical] { return _appender->truncate(physical); });
    } else {
        f = f.then([this, physical] { return _reader->truncate(physical); });
    }
    return f;
}

ss::future<append_result> segment::append(model::record_batch b) {
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
            _idx->maybe_track(b.header(), start_physical_offset);
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
    auto nearest = _idx->find_nearest(o);
    size_t position = 0;
    if (nearest) {
        position = nearest->filepos;
    }
    return _reader->data_stream(position, iopc);
}

std::ostream& operator<<(std::ostream& o, const segment& h) {
    o << "{reader=" << h.reader() << ", dirty_offset:" << h.dirty_offset()
      << ", writer=";
    if (h.has_appender()) {
        o << *h.appender();
    } else {
        o << "nullptr";
    }
    return o << ", index=" << h.index() << "}";
}
std::ostream& operator<<(std::ostream& o, const std::unique_ptr<segment>& p) {
    if (!p) {
        return o << "{nullptr}";
    }
    return o << *p;
}

} // namespace storage
