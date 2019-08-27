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

struct base_offset_ordering {
    bool
    operator()(const log_segment_ptr& seg1, const log_segment_ptr& seg2) const {
        return seg1->base_offset() <= seg2->base_offset();
    }
    bool operator()(const log_segment_ptr& seg, model::offset value) const {
        return value <= seg->base_offset();
    }
};

log_set::log_set(std::vector<log_segment_ptr> segs) noexcept(
  log_set::is_nothrow::value)
  : _segments(std::move(segs)) {
    std::sort(_segments.begin(), _segments.end(), base_offset_ordering{});
}

log_set::generation_advancer::generation_advancer(log_set& log_set) noexcept
  : _log_set(log_set) {
}

log_set::generation_advancer::~generation_advancer() {
    ++_log_set._generation;
}

void log_set::add(log_segment_ptr seg) {
    generation_advancer ea(*this);
    _segments.push_back(std::move(seg));
}

void log_set::pop_last() {
    generation_advancer ea(*this);
    _segments.pop_back();
}


} // namespace storage