#pragma once

#include "storage/segment.h"

#include <initializer_list>

namespace storage {
/*
 * A container for log_segment_reader's. Usage:
 *
 * segment_set l;
 * l.add(some_log_segment);
 * ...
 * l.add(another_log_segment);
 * ...
 * for (auto seg : l) {
 *   // Do something with the segment
 * }
 */
class segment_set {
public:
    // type _must_ offer stable segment addresses
    // for readers and writers taking refs.
    using type = ss::lw_shared_ptr<segment>;
    using underlying_t = std::vector<type>;
    using const_iterator = underlying_t::const_iterator;
    using reverse_iterator = underlying_t::reverse_iterator;
    using const_reverse_iterator = underlying_t::const_reverse_iterator;
    using iterator = underlying_t::iterator;
    static constexpr bool is_nothrow_v
      = std::is_nothrow_move_constructible_v<underlying_t>;

    explicit segment_set(underlying_t) noexcept(is_nothrow_v);
    ~segment_set() = default;
    segment_set(segment_set&&) noexcept = default;
    segment_set& operator=(segment_set&&) noexcept = default;
    segment_set(const segment_set&) = delete;
    segment_set& operator=(const segment_set&) = delete;

    size_t size() const { return _handles.size(); }

    bool empty() const { return _handles.empty(); }

    /// must be monotonically increasing in base offset
    void add(ss::lw_shared_ptr<segment>);

    void pop_back();

    underlying_t release() && { return std::move(_handles); }
    type& back() { return _handles.back(); }
    const type& back() const { return _handles.back(); }
    const type& front() const { return _handles.front(); }

    iterator lower_bound(model::offset o);
    const_iterator lower_bound(model::offset o) const;

    reverse_iterator rbegin() { return _handles.rbegin(); }
    reverse_iterator rend() { return _handles.rend(); }
    const_reverse_iterator rbegin() const { return _handles.crbegin(); }
    const_reverse_iterator rend() const { return _handles.crend(); }

    const_iterator cbegin() const { return _handles.cbegin(); }
    const_iterator cend() const { return _handles.cend(); }
    iterator begin() { return _handles.begin(); }
    iterator end() { return _handles.end(); }
    const_iterator begin() const { return _handles.begin(); }
    const_iterator end() const { return _handles.end(); }

private:
    underlying_t _handles;
};

std::ostream& operator<<(std::ostream&, const segment_set&);

} // namespace storage
