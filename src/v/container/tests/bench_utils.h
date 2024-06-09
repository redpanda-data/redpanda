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
#include "base/type_traits.h"
#include "random/generators.h"

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
static auto make_value() {
    if constexpr (std::is_same_v<ValueT, int64_t>) {
        return static_cast<int64_t>(random_generators::get_int<int64_t>());

    } else if constexpr (std::is_same_v<ValueT, ss::sstring>) {
        constexpr size_t max_str_len = 64;
        return random_generators::gen_alphanum_string(
          random_generators::get_int<size_t>(0, max_str_len));
    } else if constexpr (std::is_same_v<ValueT, large_struct>) {
        constexpr size_t max_str_len = 64;
        constexpr size_t max_num_cardinality = 16;
        return large_struct{
          .foo = random_generators::get_int<size_t>(0, max_num_cardinality),
          .bar = random_generators::get_int<size_t>(0, max_num_cardinality),
          .qux = random_generators::gen_alphanum_string(
            random_generators::get_int<size_t>(0, max_str_len)),
          .baz = random_generators::gen_alphanum_string(
            random_generators::get_int<size_t>(0, max_str_len)),
          .more = random_generators::gen_alphanum_string(
            random_generators::get_int<size_t>(0, max_str_len)),
          .data = random_generators::gen_alphanum_string(
            random_generators::get_int<size_t>(0, max_str_len)),
          .okdone = random_generators::get_int<size_t>(),
        };
    } else {
        static_assert(base::unsupported_type<ValueT>::value, "unsupported");
    }
}
