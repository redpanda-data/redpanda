/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include <absl/container/btree_map.h>

/**
 * A container that contains non-empty, open intervals.
 *
 * Intervals that overlap with or are exactly adjacent to one another are
 * coalesced.
 *
 * insert [50, 100)
 * insert [300, 400)
 * insert [200, 500)
 * insert [100, 150)
 *
 * Result: {[50, 150), [200, 500)}
 *
 * Similar to boost::icl::interval_set, but backed by an absl::btree_set.
 */
template<std::integral T>
class interval_set {
    // Key = interval start (inclusive)
    // Value = interval end (exclusive)
    using set_t = absl::btree_map<T, T>;

public:
    using const_iterator = set_t::const_iterator;
    using iterator = set_t::iterator;
    struct interval {
        T start;
        T length;
    };

    /**
     * Insert the interval into the set, returning the resulting iterator and
     * whether the insert was successful. Insertion will fail if the interval
     * is empty.
     *
     * If the insertion results in intervals overlapping, the intervals are
     * merged and the resulting merged interval is returned.
     */
    [[nodiscard]] std::pair<const_iterator, bool> insert(interval interval);

    /**
     * Find the interval containing \p index.
     *
     * If no such interval exists then end() is returned.
     */
    [[nodiscard]] const_iterator find(T index) const;

    /**
     * Return an iterator to the first entry in the set.
     *
     * If the set is empty then end() is returned.
     */
    [[nodiscard]] const_iterator begin() const;

    /**
     * Return an iterator to the end of the set.
     */
    [[nodiscard]] const_iterator end() const;

    /**
     * Return true if the set contains no intervals.
     */
    [[nodiscard]] bool empty() const;

    /**
     * Erase the interval pointed to by the iterator \it.
     *
     * Invalidates iterators.
     */
    void erase(const_iterator it);

    /**
     * Return the number of intervals in the set.
     */
    [[nodiscard]] size_t size() const;

    /**
     * Convenience wrappers.
     */
    static auto to_start(const_iterator it) { return it->first; }
    static auto to_end(const_iterator it) { return it->second; }

private:
    /**
     * Extend the interval being pointed at with any intervals that overlap
     * with it to its right.
     *
     * Invalidates iterators.
     */
    std::pair<const_iterator, bool> merge_right(const_iterator start_it);

    set_t set_;

    static auto& to_end(iterator it) { return it->second; }
};

template<std::integral T>
std::pair<typename interval_set<T>::const_iterator, bool>
interval_set<T>::merge_right(const_iterator start_it) {
    auto start = to_start(start_it);
    auto merged_end = to_end(start_it);
    auto next_it = std::next(start_it);
    auto merge_end_it = next_it;

    // Seek forward as long as the next interval if it overlaps with our merged
    // interval. NOTE: <= because these are open intervals.
    while (merge_end_it != set_.end() && to_start(merge_end_it) <= merged_end) {
        merged_end = std::max(to_end(merge_end_it), merged_end);
        merge_end_it = std::next(merge_end_it);
    }
    if (merge_end_it == next_it) {
        // Nothing to merge, just return.
        return {start_it, true};
    }
    // Replace our initial iterator and subsequent intervals with a merged
    // version.
    set_.erase(start_it, merge_end_it);
    return set_.emplace(start, merged_end);
}

template<std::integral T>
std::pair<typename interval_set<T>::const_iterator, bool>
interval_set<T>::insert(interval interval) {
    const auto length = interval.length;
    if (length <= 0) {
        return {set_.cend(), false};
    }

    const auto input_start = interval.start;
    const auto input_end = input_start + length;
    if (set_.empty()) {
        return set_.emplace(input_start, input_end);
    }

    auto it = set_.lower_bound(input_start);
    // We found an interval that starts at the same start point.
    //             it
    //             v
    //     [ )     [       )     <-- set_
    //             [         )       case 1
    //             [ )               case 2
    // In either case, just merge the expand the bounds of the existing
    // iterator.
    if (it != set_.end() && input_start == to_start(it)) {
        to_end(it) = std::max(input_end, to_end(it));
        return merge_right(it);
    }

    // We found an interval that starts above the start point, or there is no
    // such interval and we're pointing at the end.
    //             it (may be end())
    //             v
    //     [ )     [       )    <-- set_
    //      [   )                   case 1
    //        [      )              case 2
    // Case 1: there's overlap with the previous interval and we need to merge
    // with it.
    if (it != set_.begin()) {
        auto prev = std::prev(it);
        if (to_end(prev) >= input_start) {
            to_end(prev) = std::max(input_end, to_end(prev));
            return merge_right(prev);
        }
        // Intentional fallthrough.
    }
    // Case 2: there's no overlap to the left. Just insert and merge forward.
    auto ret = set_.emplace(input_start, input_end);
    return merge_right(ret.first);
}

template<std::integral T>
interval_set<T>::const_iterator interval_set<T>::find(T index) const {
    auto it = set_.lower_bound(index);
    if (it == set_.cend()) {
        if (set_.empty()) {
            return set_.cend();
        }
        it = std::prev(it);

    } else if (to_start(it) == index) {
        return it;
    } else if (it == set_.cbegin()) {
        // Equality condition failing before this means that the index is
        // before the first interval in the container.
        return set_.cend();
    } else {
        --it;
    }

    assert(to_start(it) < index);
    if (index < to_end(it)) {
        return it;
    }

    return set_.cend();
}

template<std::integral T>
interval_set<T>::const_iterator interval_set<T>::begin() const {
    return set_.cbegin();
}

template<std::integral T>
interval_set<T>::const_iterator interval_set<T>::end() const {
    return set_.cend();
}

template<std::integral T>
bool interval_set<T>::empty() const {
    return set_.empty();
}

template<std::integral T>
void interval_set<T>::erase(interval_set<T>::const_iterator it) {
    set_.erase(it);
}

template<std::integral T>
size_t interval_set<T>::size() const {
    return set_.size();
}
