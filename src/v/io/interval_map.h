/*
 * Copyright 2023 Redpanda Data, Inc.
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

#include <concepts>
#include <utility>

namespace experimental::io {

/**
 * A container that maps intervals to values.
 *
 * The interval_map holds non-overlapping, non-empty, open intervals, and
 * associates each interval with a given value. For example:
 *
 *     [0000, 4096) -> Page0
 *     [4096, 8192) -> Page1
 */
template<std::integral T, typename V>
class interval_map {
    using map_type = absl::btree_map<T, std::pair<T, V>>;

public:
    /**
     * Container value iterator.
     */
    using const_iterator = map_type::const_iterator;

    /**
     * Insert an interval [start, start+length) and value.
     *
     * If true is returned then the interval was inserted, and the corresponding
     * iterator points to the inserted interval.
     *
     * If false is returned then the interval was not inserted. If insertion
     * failed because the length was zero, then the returned iterator will be
     * equal to end(). Otherwise, the iterator will point at an interval that
     * overlapped with the interval being inserted.
     *
     * Invalidates iterators.
     */
    [[nodiscard]] std::pair<const_iterator, bool>
    insert(T start, T length, V value);

    /**
     * Find the interval containing \p index.
     *
     * If no such interval exists then end() is returned.
     */
    [[nodiscard]] const_iterator find(T index) const;

    /**
     * Return an iterator to the first entry in the map.
     *
     * If the map is empty then end() is returned.
     */
    [[nodiscard]] const_iterator begin() const;

    /**
     * Return an iterator to the end of the map.
     */
    [[nodiscard]] const_iterator end() const;

    /**
     * Return true if the map contains no intervals.
     */
    [[nodiscard]] bool empty() const;

    /**
     * Erase the interval pointed to by the iterator \it.
     *
     * Invalidates iterators.
     */
    void erase(const_iterator it);

private:
    map_type map_;
};

template<std::integral T, typename V>
std::pair<typename interval_map<T, V>::const_iterator, bool>
interval_map<T, V>::insert(T start, T length, V value) {
    if (length <= 0) {
        return {map_.cend(), false};
    }

    const auto end = start + length;

    auto it = map_.lower_bound(start);
    if (it == map_.end()) {
        /*
         * all intervals in the container have starting offsets that are less
         * than the starting offset of the interval being inserted.
         */
        if (map_.empty()) {
            return map_.try_emplace(start, end, value);
        }

        // checks for overlap with the interval on the left
        it = std::prev(it);
        if (it->second.first > start) {
            return {it, false};
        }

        return {map_.try_emplace(it, start, end, value), true};
    }

    // checks for overlap with the interval on the right
    if (end > it->first) {
        return {it, false};
    }

    // there are no intervals on the left
    if (it == map_.begin()) {
        return map_.try_emplace(start, end, value);
    }

    // checks for overlap with the interval on the left
    it = std::prev(it);
    if (it->second.first > start) {
        return {it, false};
    }

    return {map_.try_emplace(it, start, end, value), true};
}

template<std::integral T, typename V>
interval_map<T, V>::const_iterator interval_map<T, V>::find(T index) const {
    auto it = map_.lower_bound(index);
    if (it == map_.cend()) {
        if (map_.empty()) {
            return map_.cend();
        }
        it = std::prev(it);

    } else if (it->first == index) {
        return it;

    } else if (it == map_.cbegin()) {
        /*
         * equality condition failing before this means that the index is before
         * the first interval in the container.
         */
        return map_.cend();

    } else {
        --it;
    }

    assert(it->first < index);
    if (index < it->second.first) {
        return it;
    }

    return map_.cend();
}

template<std::integral T, typename V>
interval_map<T, V>::const_iterator interval_map<T, V>::begin() const {
    return map_.cbegin();
}

template<std::integral T, typename V>
interval_map<T, V>::const_iterator interval_map<T, V>::end() const {
    return map_.cend();
}

template<std::integral T, typename V>
bool interval_map<T, V>::empty() const {
    return map_.empty();
}

template<std::integral T, typename V>
void interval_map<T, V>::erase(interval_map<T, V>::const_iterator it) {
    map_.erase(it);
}

} // namespace experimental::io
