#pragma once

#include "storage/segment.h"

#include <initializer_list>

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
    using underlying_t = std::vector<segment>;
    using const_iterator = underlying_t::const_iterator;
    using const_reverse_iterator = underlying_t::const_reverse_iterator;
    using iterator = underlying_t::iterator;
    static constexpr bool is_nothrow_v
      = std::is_nothrow_move_constructible_v<underlying_t>;

    explicit log_set(underlying_t) noexcept(is_nothrow_v);
    log_set(log_set&&) noexcept = default;
    log_set& operator=(log_set&&) noexcept = default;
    log_set(const log_set&) = delete;
    log_set& operator=(const log_set&) = delete;

    size_t size() const { return _handles.size(); }

    bool empty() const { return _handles.empty(); }

    /// must be monotonically increasing in base offset
    void add(segment);

    void pop_back();

    underlying_t release() && { return std::move(_handles); }
    segment& back() { return _handles.back(); }
    const segment& back() const { return _handles.back(); }
    const segment& front() const { return _handles.front(); }

    const_iterator lower_bound(model::offset) const;

    const_iterator cbegin() const { return _handles.cbegin(); }
    const_iterator cend() const { return _handles.cend(); }
    iterator begin() { return _handles.begin(); }
    iterator end() { return _handles.end(); }
    const_iterator begin() const { return _handles.begin(); }
    const_iterator end() const { return _handles.end(); }

private:
    underlying_t _handles;
};

std::ostream& operator<<(std::ostream&, const log_set&);

} // namespace storage
