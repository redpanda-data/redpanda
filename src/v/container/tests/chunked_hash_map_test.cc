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

#include "container/chunked_hash_map.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

struct foo_with_std_hash {
    int a;
    int b;
    auto operator<=>(const foo_with_std_hash&) const = default;
};

namespace std {

template<>
struct hash<foo_with_std_hash> {
    constexpr size_t operator()(const foo_with_std_hash& x) const {
        return std::hash<int>()(x.a + x.b);
    }
};

} // namespace std

struct foo_with_absl_hash {
    int a;
    int b;

    auto operator<=>(const foo_with_absl_hash&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const foo_with_absl_hash& x) {
        return H::combine(std::move(h), x.a, x.b);
    }
};

TEST(chunked_hash_map, basic_compile_std_hash) {
    chunked_hash_map<foo_with_std_hash, int> map;
    map[{1, 2}] = 2;
    EXPECT_EQ(map.size(), 1);
}

TEST(chunked_hash_map, basic_compile_absl_hash) {
    static_assert(detail::has_absl_hash<foo_with_absl_hash>);
    chunked_hash_map<foo_with_absl_hash, int> map;
    map[{1, 2}] = 2;
    EXPECT_EQ(map.size(), 1);
}

TEST(chunked_hash_map, test_move_assignment) {
    chunked_hash_map<foo_with_absl_hash, int> map;
    chunked_hash_map<foo_with_absl_hash, int> other_map;
    other_map = std::move(map);
}
