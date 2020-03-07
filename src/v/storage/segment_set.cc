#include "storage/segment_set.h"

#include "storage/logger.h"
#include "vassert.h"

#include <fmt/format.h>

namespace storage {
struct segment_ordering {
    using type = ss::lw_shared_ptr<segment>;
    bool operator()(const type& seg1, const type& seg2) const {
        return seg1->reader().base_offset() < seg2->reader().base_offset();
    }
    bool operator()(const type& seg, model::offset value) const {
        return seg->reader().max_offset() < value;
    }
    bool operator()(const type& seg, model::timestamp value) const {
        return seg->index().base_timestamp() < value;
    }
};

segment_set::segment_set(segment_set::underlying_t segs)
  : _handles(std::move(segs)) {
    std::sort(_handles.begin(), _handles.end(), segment_ordering{});
}

void segment_set::add(ss::lw_shared_ptr<segment> h) {
    if (!_handles.empty()) {
        vassert(
          h->reader().base_offset() > _handles.back()->reader().max_offset(),
          "New segments must be monotonically increasing. Got:{} - Current:{}",
          *h,
          *this);
    }
    _handles.emplace_back(std::move(h));
}

void segment_set::pop_back() { _handles.pop_back(); }
void segment_set::pop_front() { _handles.pop_front(); }

template<typename Iterator>
struct needle_in_range {
    bool operator()(Iterator ptr, model::offset o) {
        auto& s = **ptr;
        if (s.empty()) {
            return false;
        }
        // must use max_offset
        return o <= s.reader().max_offset() && o >= s.reader().base_offset();
    }

    bool operator()(Iterator ptr, model::timestamp t) {
        auto& s = **ptr;
        if (s.empty()) {
            return false;
        }
        // must use max_offset
        return t <= s.index().max_timestamp()
               && t >= s.index().base_timestamp();
    }
};

template<typename Iterator, typename Needle>
Iterator segments_lower_bound(Iterator begin, Iterator end, Needle needle) {
    if (std::distance(begin, end) == 0) {
        return end;
    }
    auto it = std::lower_bound(begin, end, needle, segment_ordering{});
    if (it == end) {
        it = std::prev(it);
    }
    if (needle_in_range<Iterator>()(it, needle)) {
        return it;
    }
    if (std::distance(begin, it) > 0) {
        it = std::prev(it);
    }
    if (needle_in_range<Iterator>()(it, needle)) {
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

segment_set::iterator segment_set::lower_bound(model::timestamp needle) {
    return segments_lower_bound(
      std::begin(_handles), std::end(_handles), needle);
}

segment_set::const_iterator
segment_set::lower_bound(model::timestamp needle) const {
    return segments_lower_bound(
      std::cbegin(_handles), std::cend(_handles), needle);
}

std::ostream& operator<<(std::ostream& o, const segment_set& s) {
    o << "{size: " << s.size() << ", [";
    for (auto& p : s) {
        o << p;
    }
    return o << "]}";
}
} // namespace storage
