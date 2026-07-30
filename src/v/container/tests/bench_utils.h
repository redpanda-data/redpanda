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
#include "base/seastarx.h"
#include "base/type_traits.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>

#include <string>

struct large_struct {
    size_t foo;
    size_t bar;
    std::string qux;
    std::string baz;
    std::string more;
    std::string data;
    size_t okdone;

    bool operator==(const large_struct&) const = default;
    auto operator<=>(const large_struct&) const = default;
};

template<typename ValueT>
[[gnu::noinline]]
ValueT make_value() {
    if constexpr (std::is_same_v<ValueT, int64_t>) {
        return 42;
    } else if constexpr (std::is_same_v<ValueT, ss::sstring>) {
        return ss::sstring(42, 'x'); // large than SSO size (15)
    } else if constexpr (std::is_same_v<ValueT, large_struct>) {
        constexpr size_t max_str_len = 64;
        constexpr size_t max_num_cardinality = 16;
        return large_struct{
          .foo = max_num_cardinality / 3,
          .bar = max_num_cardinality - 2,
          .qux = std::string(max_str_len / 8, 'x'),
          .baz = std::string(max_str_len / 4, 'x'),
          .more = std::string(max_str_len / 2, 'x'),
          .data = std::string(max_str_len / 1, 'x'),
          .okdone = 4242,
        };
    } else {
        static_assert(base::unsupported_type<ValueT>::value, "unsupported");
    }
}
