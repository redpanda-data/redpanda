#include "storage/segment.h"

namespace storage {

segment::segment(
  segment_reader_ptr r,
  segment_offset_index_ptr i,
  segment_appender_ptr a) noexcept
  : _reader(std::move(r))
  , _oidx(std::move(i))
  , _appender(std::move(a)) {}

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

ss::future<> segment::flush() {
    if (_appender) {
        return _appender->flush();
    }
    return ss::make_ready_future<>();
}

ss::future<> segment::truncate(model::offset) {
    return ss::make_exception_future<>(
      std::runtime_error("segment::truncate(model::offset) not implemented"));
}

std::ostream& operator<<(std::ostream& o, const segment& h) {
    o << "{reader=" << h.reader() << ", writer=";
    if (h.has_appender()) {
        o << *h.appender();
    } else {
        o << "nullptr";
    }
    return o << ", offset_index=" << h.oindex() << "}";
}

} // namespace storage
