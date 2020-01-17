#include "storage/log_set.h"

#include "storage/logger.h"

#include <fmt/format.h>

namespace storage {
struct base_offset_ordering {
    bool operator()(const segment& seg1, const segment& seg2) const {
        return seg1.reader->base_offset() < seg2.reader->base_offset();
    }
    bool operator()(const segment& seg, model::offset value) const {
        return seg.reader->max_offset() < value;
    }
};

log_set::log_set(log_set::underlying_t segs) noexcept(log_set::is_nothrow_v)
  : _handles(std::move(segs)) {
    std::sort(_handles.begin(), _handles.end(), base_offset_ordering{});
}

void log_set::add(segment h) {
    if (
      !_handles.empty()
      && h.reader->base_offset() < _handles.back().reader->base_offset()) {
        throw std::runtime_error(fmt::format(
          "New segments must be monotonically increasing 'base_offset()' "
          "ptr->base_offset():{} must be > last:{}",
          h.reader->base_offset(),
          _handles.back().reader->base_offset()));
    }
    _handles.push_back(std::move(h));
}

void log_set::pop_back() { _handles.pop_back(); }

log_set::const_iterator log_set::lower_bound(model::offset offset) const {
    return std::lower_bound(
      std::cbegin(_handles),
      std::cend(_handles),
      offset,
      base_offset_ordering{});
}

std::ostream& operator<<(std::ostream& o, const log_set& s) {
    o << "{size: " << s.size() << ", [";
    for (auto& p : s) {
        o << p;
    }
    return o << "]}";
}
} // namespace storage
