#include "storage/log_set.h"

#include "storage/logger.h"

#include <fmt/format.h>

namespace storage {
struct base_offset_ordering {
    bool operator()(
      const segment_reader_ptr& seg1, const segment_reader_ptr& seg2) const {
        return seg1->base_offset() <= seg2->base_offset();
    }
    bool operator()(const segment_reader_ptr& seg, model::offset value) const {
        // return seg->max_offset() <= value && seg->base_offset() >= value;
        return seg->max_offset() < value;
    }
};

log_set::log_set(std::vector<segment_reader_ptr> segs) noexcept(
  log_set::is_nothrow_v)
  : _segments(std::move(segs)) {
    std::sort(_segments.begin(), _segments.end(), base_offset_ordering{});
}

void log_set::add(segment_reader_ptr seg) {
    if (
      !_segments.empty()
      && seg->base_offset() < _segments.back()->base_offset()) {
        throw std::runtime_error(fmt::format(
          "New segments must be monotonically increasing 'base_offset()' "
          "ptr->base_offset():{} must be > last:{}",
          seg->base_offset(),
          _segments.back()->base_offset()));
    }

    _segments.push_back(std::move(seg));
}

void log_set::pop_last() { _segments.pop_back(); }

log_set::const_iterator log_set::lower_bound(model::offset offset) const {
    return std::lower_bound(
      std::cbegin(_segments),
      std::cend(_segments),
      offset,
      base_offset_ordering{});
}

std::ostream& operator<<(std::ostream& o, const log_set& s) {
    o << "{size: " << s.size() << ", [";
    for (const auto& p : s) {
        o << p;
    }
    return o << "]}";
}
} // namespace storage
