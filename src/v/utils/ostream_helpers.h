/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <fmt/core.h>

#include <vector>
#include <ostream>

/**
 * @brief Wraps a std::vector by reference and prints out only the
 * first N elements of the vector, to avoid very large log lines.
 *
 */
template<typename T>
struct limited_vector {
    using c_type = std::vector<T>;

    limited_vector(const c_type& container, size_t limit)
      : _container(container)
      , _limit(limit) {}

    const c_type& _container;
    const size_t _limit;
};

template<typename T>
inline std::ostream& operator<<(std::ostream& os, const limited_vector<T>& lv) {
    size_t count = 0;
    os << "{";
    for (auto&& elem : lv._container) {
        if (count) {
            os << ", ";
        }
        if (count > lv._limit) {
            os << "... " << (lv._container.size() - lv._limit) << " more elements";
            break;
        }
        os << elem;
        count++;
    }
    os << "}";
    return os;
}
