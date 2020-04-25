#pragma once

#include "storage/segment.h"

#include <seastar/core/circular_buffer.hh>

#include <deque>

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

    // NOTE: gcc has an ABI problem and cannot make std::deque noexcept
    // so we use a circular instead of a dequeue that *is* noexcept ctor
    // and allow us to truly have an empty container at ctor time
    // We loose reverse-iterators tho
    using underlying_t = ss::circular_buffer<type>;
    using const_iterator = underlying_t::const_iterator;
    using iterator = underlying_t::iterator;

    explicit segment_set(underlying_t);
    ~segment_set() noexcept = default;
    segment_set(segment_set&&) noexcept = default;
    segment_set& operator=(segment_set&& o) noexcept = default;
    segment_set(const segment_set&) = delete;
    segment_set& operator=(const segment_set&) = delete;

    size_t size() const { return _handles.size(); }

    bool empty() const { return _handles.empty(); }

    /// must be monotonically increasing in base offset
    void add(ss::lw_shared_ptr<segment>);

    void pop_back();
    void pop_front();

    underlying_t release() && { return std::move(_handles); }
    type& back() { return _handles.back(); }
    const type& back() const { return _handles.back(); }
    const type& front() const { return _handles.front(); }
    type& operator[](size_t i) { return _handles[i]; }
    const type& operator[](size_t i) const { return _handles[i]; }

    iterator lower_bound(model::offset o);
    const_iterator lower_bound(model::offset o) const;
    iterator lower_bound(model::timestamp o);
    const_iterator lower_bound(model::timestamp o) const;

    const_iterator cbegin() const { return _handles.cbegin(); }
    const_iterator cend() const { return _handles.cend(); }
    iterator begin() { return _handles.begin(); }
    iterator end() { return _handles.end(); }
    const_iterator begin() const { return _handles.begin(); }
    const_iterator end() const { return _handles.end(); }

private:
    underlying_t _handles;

    friend std::ostream& operator<<(std::ostream&, const segment_set&);
};

std::ostream& operator<<(std::ostream&, const segment_set&);

} // namespace storage
