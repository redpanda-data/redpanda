#include "storage/log_segment.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>

namespace storage {

log_segment::log_segment(
  sstring filename,
  file data_file,
  model::term_id term,
  model::offset base_offset,
  size_t buffer_size) noexcept
  : _filename(std::move(filename))
  , _data_file(std::move(data_file))
  , _term(std::move(term))
  , _base_offset(base_offset)
  , _buffer_size(buffer_size) {}

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

std::ostream& operator<<(std::ostream& os, const log_segment& seg) {
    return fmt_print(
      os, "{{log_segment: {}, {}}}", seg.get_filename(), seg.base_offset());
}

std::ostream& operator<<(std::ostream& os, log_segment_ptr seg) {
    if (seg) {
        return os << *seg;
    }
    return fmt_print(os, "{{log_segment: null}}");
}

struct base_offset_ordering {
    bool
    operator()(const log_segment_ptr& seg1, const log_segment_ptr& seg2) const {
        return seg1->base_offset() <= seg2->base_offset();
    }
    bool operator()(const log_segment_ptr& seg, model::offset value) const {
        return seg->max_offset() < value;
    }
};

log_segment_selector::log_segment_selector(const log_set& set) noexcept
  : _set(set) {}

log_set::log_set(std::vector<log_segment_ptr> segs) noexcept(
  log_set::is_nothrow::value)
  : _segments(std::move(segs)) {
    std::sort(_segments.begin(), _segments.end(), base_offset_ordering{});
}

log_set::generation_advancer::generation_advancer(log_set& log_set) noexcept
  : _log_set(log_set) {}

log_set::generation_advancer::~generation_advancer() { ++_log_set._generation; }

void log_set::add(log_segment_ptr seg) {
    generation_advancer ea(*this);
    _segments.push_back(std::move(seg));
}

void log_set::pop_last() {
    generation_advancer ea(*this);
    _segments.pop_back();
}

log_segment_ptr log_segment_selector::select(model::offset offset) const {
    if (_iter_gen != _set.iter_gen()) {
        _current_segment = std::lower_bound(
          _set.begin(), _set.end(), offset, base_offset_ordering{});
        _iter_gen = _set.iter_gen();
    }
    auto seg = _current_segment;
    while (seg != _set.end()) {
        if (offset < (*seg)->max_offset()) {
            _current_segment = seg;
            return *seg;
        }
        ++seg;
    }
    return nullptr;
}

} // namespace storage
