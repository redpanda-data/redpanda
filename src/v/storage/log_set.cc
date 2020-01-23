#include "storage/log_set.h"

#include "storage/logger.h"
#include "vassert.h"

#include <fmt/format.h>

namespace storage {
struct base_offset_ordering {
    bool operator()(const segment& seg1, const segment& seg2) const {
        return seg1.reader()->base_offset() < seg2.reader()->base_offset();
    }
    bool operator()(const segment& seg, model::offset value) const {
        return seg.reader()->max_offset() < value;
    }
};

log_set::log_set(log_set::underlying_t segs) noexcept(log_set::is_nothrow_v)
  : _handles(std::move(segs)) {
    std::sort(_handles.begin(), _handles.end(), base_offset_ordering{});
}

void log_set::add(segment&& h) {
    if (!_handles.empty()) {
        vassert(
          h.reader()->base_offset() > _handles.back().reader()->max_offset(),
          "New segments must be monotonically increasing. Got:{} - Current:{}",
          h,
          *this);
    }
    _handles.emplace_back(std::move(h));
}

void log_set::pop_back() { _handles.pop_back(); }

static inline bool is_offset_in_range(segment& s, model::offset o) {
    if (s.empty()) {
        return false;
    }
    // must use max_offset
    return o <= s.reader()->max_offset() && o >= s.reader()->base_offset();
}

log_set::iterator log_set::lower_bound(model::offset offset) {
    vassert(offset() >= 0, "cannot find negative logical offsets");
    if (_handles.empty()) {
        return _handles.end();
    }
    auto it = std::lower_bound(
      std::begin(_handles), std::end(_handles), offset, base_offset_ordering{});
    if (it == _handles.end()) {
        it = std::prev(it);
    }
    if (is_offset_in_range(*it, offset)) {
        return it;
    }
    if (std::distance(_handles.begin(), it) > 0) {
        it = std::prev(it);
    }
    if (is_offset_in_range(*it, offset)) {
        return it;
    }
    return _handles.end();
}
std::ostream& operator<<(std::ostream& o, const log_set& s) {
    o << "{size: " << s.size() << ", [";
    for (auto& p : s) {
        o << p;
    }
    return o << "]}";
}
} // namespace storage
