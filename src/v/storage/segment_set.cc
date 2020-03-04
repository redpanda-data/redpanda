#include "storage/segment_set.h"

#include "storage/logger.h"
#include "vassert.h"

#include <fmt/format.h>

namespace storage {
struct base_offset_ordering {
    using type = ss::lw_shared_ptr<segment>;
    bool operator()(const type& seg1, const type& seg2) const {
        return seg1->reader()->base_offset() < seg2->reader()->base_offset();
    }
    bool operator()(const type& seg, model::offset value) const {
        return seg->reader()->max_offset() < value;
    }
};

segment_set::segment_set(segment_set::underlying_t segs) noexcept(
  segment_set::is_nothrow_v)
  : _handles(std::move(segs)) {
    std::sort(_handles.begin(), _handles.end(), base_offset_ordering{});
}

void segment_set::add(ss::lw_shared_ptr<segment> h) {
    if (!_handles.empty()) {
        vassert(
          h->reader()->base_offset() > _handles.back()->reader()->max_offset(),
          "New segments must be monotonically increasing. Got:{} - Current:{}",
          *h,
          *this);
    }
    _handles.emplace_back(std::move(h));
}

void segment_set::pop_back() { _handles.pop_back(); }

template<typename Iterator>
static inline bool is_offset_in_range(Iterator ptr, model::offset o) {
    auto& s = **ptr;
    if (s.empty()) {
        return false;
    }
    // must use max_offset
    return o <= s.reader()->max_offset() && o >= s.reader()->base_offset();
}

template<typename Iterator>
static Iterator
segments_lower_bound(Iterator begin, Iterator end, model::offset offset) {
    vassert(offset() >= 0, "cannot find negative logical offsets");
    if (std::distance(begin, end) == 0) {
        return end;
    }
    auto it = std::lower_bound(begin, end, offset, base_offset_ordering{});
    if (it == end) {
        it = std::prev(it);
    }
    if (is_offset_in_range(it, offset)) {
        return it;
    }
    if (std::distance(begin, it) > 0) {
        it = std::prev(it);
    }
    if (is_offset_in_range(it, offset)) {
        return it;
    }
    return end;
}

/// lower_bound returns the element that is _strictly_ greater than or equal
/// to bucket->max_offset() - see comparator above because our offsets are
/// _inclusive_ we must check the previous iterator in the case that we are at
/// the end, we also check the last element.
segment_set::iterator segment_set::lower_bound(model::offset offset) {
    return segments_lower_bound(
      std::begin(_handles), std::end(_handles), offset);
}

segment_set::const_iterator
segment_set::lower_bound(model::offset offset) const {
    return segments_lower_bound(
      std::cbegin(_handles), std::cend(_handles), offset);
}

std::ostream& operator<<(std::ostream& o, const segment_set& s) {
    o << "{size: " << s.size() << ", [";
    for (auto& p : s) {
        o << p;
    }
    return o << "]}";
}
} // namespace storage
