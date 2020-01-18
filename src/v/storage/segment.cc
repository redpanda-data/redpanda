#include "storage/segment.h"

namespace storage {

segment::segment(segment_reader_ptr r, segment_appender_ptr a) noexcept
  : _reader(std::move(r))
  , _appender(std::move(a)) {}

segment::segment(segment_reader_ptr r) noexcept
  : _reader(std::move(r)) {}


ss::future<> segment::close() {
    auto f = _reader->close();
    if (_appender) {
        f = f.then([this] { return _appender->close(); });
    }
    return f;
}

ss::future<> segment::flush() {
    if (_appender) {
        return _appender->flush();
    }
    return ss::make_ready_future<>();
}

ss::future<> segment::truncate(model::offset) {
    return ss::make_ready_future<>();
}

std::ostream& operator<<(std::ostream& o, const segment& h) {
    o << "{reader=" << h.reader() << ", writer=";
    if (h.has_appender()) {
        o << *h.appender();
    } else {
        o << "nullptr";
    }
    return o << "}";
}

} // namespace storage
