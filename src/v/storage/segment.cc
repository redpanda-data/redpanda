#include "storage/segment.h"

#include "storage/log_segment_appender_utils.h"
#include "storage/logger.h"
#include "vassert.h"

#include <seastar/core/future-util.hh>

namespace storage {

segment::segment(
  segment_reader_ptr r,
  segment_offset_index_ptr i,
  segment_appender_ptr a) noexcept
  : _reader(std::move(r))
  , _oidx(std::move(i))
  , _appender(std::move(a)) {
    // TODO(agallego) - add these asserts once we migrate tests
    // vassert(_reader, "segments must have valid readers");
    // vassert(_oidx, "segments must have valid offset index");
}

ss::future<> segment::close() {
    auto f = _reader->close();
    if (_appender) {
        f = f.then([this] { return _appender->close(); });
    }
    // after appender flushes to make sure we make things visible
    // only after appender flush
    f = f.then([this] { return _oidx->close(); });
    return f;
}

ss::future<> segment::release_appender() {
    vassert(_appender, "cannot release a null appender");
    return flush()
      .then([this] { return _appender->close(); })
      .then([this] { return _oidx->flush(); })
      .then([this] { _appender = nullptr; });
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

ss::future<> segment::truncate(model::offset) {
    return ss::make_exception_future<>(
      std::runtime_error("segment::truncate(model::offset) not implemented"));
}

ss::future<append_result> segment::append(model::record_batch b) {
    return ss::do_with(std::move(b), [this](model::record_batch& b) {
        const auto start_physical_offset = _appender->file_byte_offset();
        // proxy serialization to log_segment_appender_utils
        return write(*_appender, b).then([this, &b, start_physical_offset] {
            _dirty_offset = b.last_offset();
            const auto end_physical_offset = _appender->file_byte_offset();
            const auto byte_size = end_physical_offset - start_physical_offset;
            // index the write
            _oidx->maybe_track(
              b.base_offset(), start_physical_offset, byte_size);
            return append_result{.base_offset = b.base_offset(),
                                 .last_offset = b.last_offset(),
                                 .byte_size = byte_size};
        });
    });
}

ss::input_stream<char>
segment::offset_data_stream(model::offset o, ss::io_priority_class iopc) {
    auto nearest_location = _oidx->lower_bound(o);
    size_t position = 0;
    if (nearest_location) {
        position = *nearest_location;
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
    return o << ", offset_oidx=" << h.oindex() << "}";
}

} // namespace storage
