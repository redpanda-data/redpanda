#include "storage/log_segment.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>

namespace storage {

log_segment::log_segment(
  sstring filename,
  file data_file,
  int64_t term,
  model::offset base_offset,
  size_t buffer_size) noexcept
  : _filename(std::move(filename))
  , _data_file(std::move(data_file))
  , _term(term)
  , _base_offset(base_offset)
  , _buffer_size(buffer_size) {
}

input_stream<char>
log_segment::data_stream(uint64_t pos, const io_priority_class& pc) {
    file_input_stream_options options;
    options.buffer_size = _buffer_size;
    options.io_priority_class = pc;
    options.read_ahead = 4;
    options.dynamic_adjustments = _history;
    return make_file_input_stream(_data_file, pos, std::move(options));
}

log_segment_appender log_segment::data_appender(const io_priority_class& pc) {
    file_output_stream_options options;
    options.io_priority_class = pc;
    return log_segment_appender(_data_file, std::move(options));
}
} // namespace storage