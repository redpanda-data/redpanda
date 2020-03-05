#include "storage/segment_reader.h"

#include "vassert.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>

namespace storage {

segment_reader::segment_reader(
  ss::sstring filename,
  ss::file data_file,
  model::term_id term,
  model::offset base_offset,
  uint64_t file_size,
  size_t buffer_size) noexcept
  : _filename(std::move(filename))
  , _data_file(std::move(data_file))
  , _term(term)
  , _base_offset(base_offset)
  , _file_size(file_size)
  , _buffer_size(buffer_size) {}

ss::input_stream<char>
segment_reader::data_stream(uint64_t pos, const ss::io_priority_class& pc) {
    vassert(pos <= _file_size, "cannot read negative bytes");
    ss::file_input_stream_options options;
    options.buffer_size = _buffer_size;
    options.io_priority_class = pc;
    options.read_ahead = 4;
    options.dynamic_adjustments = _history;
    return make_file_input_stream(
      _data_file, pos, _file_size - pos, std::move(options));
}

ss::future<> segment_reader::truncate(size_t n) {
    _file_size = n;
    return ss::open_file_dma(_filename, ss::open_flags::rw)
      .then([n](ss::file f) {
          return f.truncate(n)
            .then([f]() mutable { return f.close(); })
            .finally([f] {});
      });
}

std::ostream& operator<<(std::ostream& os, const segment_reader& seg) {
    return os << "{log_segment:" << seg.filename() << ", " << seg.base_offset()
              << "-" << seg.max_offset() << ", filesize:" << seg.file_size()
              << "}";
}

std::ostream& operator<<(std::ostream& os, segment_reader_ptr seg) {
    if (seg) {
        return os << *seg;
    }
    return os << "{{log_segment: null}}";
}
} // namespace storage
