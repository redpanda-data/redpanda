#pragma once

#include "storage/log_segment_reader.h"

namespace storage {
/*
 * A container for log_segment_reader's. Usage:
 *
 * log_set l;
 * l.add(some_log_segment);
 * ...
 * l.add(another_log_segment);
 * ...
 * for (auto seg : l) {
 *   // Do something with the segment
 * }
 */
class log_set {
public:
    using underlying_t = std::vector<segment_reader_ptr>;
    using const_iterator = underlying_t::const_iterator;
    using const_reverse_iterator = underlying_t::const_reverse_iterator;
    using iterator = underlying_t::iterator;
    static constexpr bool is_nothrow_v
      = std::is_nothrow_move_constructible_v<underlying_t>;

    explicit log_set(underlying_t) noexcept(is_nothrow_v);

    size_t size() const { return _segments.size(); }

    bool empty() const { return _segments.empty(); }

    /// New segments must be monotonically increasing in base offset
    void add(segment_reader_ptr);

    void pop_last();

    segment_reader_ptr last() const { return _segments.back(); }

    const_iterator begin() const { return _segments.begin(); }

    const_iterator end() const { return _segments.end(); }
    const_reverse_iterator rbegin() const { return _segments.rbegin(); }
    const_reverse_iterator rend() const { return _segments.rend(); }

    iterator begin() { return _segments.begin(); }

    iterator end() { return _segments.end(); }

    const_iterator lower_bound(model::offset) const;

private:
    std::vector<segment_reader_ptr> _segments;
};

std::ostream& operator<<(std::ostream&, const log_set&);

} // namespace storage
