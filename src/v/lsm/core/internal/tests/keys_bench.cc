/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "lsm/core/internal/keys.h"
#include "random/generators.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>

#include <algorithm>
#include <iterator>
#include <ranges>
#include <vector>

using namespace lsm::internal;

namespace {

std::vector<key> generate_keys(size_t count, size_t key_length) {
    using std::views::drop;
    using std::views::iota;
    using std::views::join;
    using std::views::repeat;
    using std::views::take;

    std::vector<key> keys;
    keys.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        std::string user_key;
        user_key.reserve(key_length);
        // Emit a-z forever
        auto gen = join(repeat(iota('a', 'z'), std::unreachable_sentinel));
        for (char c : take(drop(gen, i % 26), key_length)) {
            user_key.push_back(c);
        }
        auto seqno = sequence_number(i % (count / 2));
        auto type = i % 2 == 0 ? value_type::value : value_type::tombstone;
        keys.push_back(
          key::encode({
            .key = lsm::user_key_view(user_key),
            .seqno = seqno,
            .type = type,
          }));
    }
    return keys;
}

} // namespace

PERF_TEST(key_comparison, sorting_short_keys) {
    auto keys = generate_keys(100, 8);
    perf_tests::start_measuring_time();
    std::ranges::sort(keys);
    perf_tests::stop_measuring_time();
    return keys.size();
}

PERF_TEST(key_comparison, sorting_medium_keys) {
    auto keys = generate_keys(100, 64);
    perf_tests::start_measuring_time();
    std::ranges::sort(keys);
    perf_tests::stop_measuring_time();
    return keys.size();
}

PERF_TEST(key_comparison, sorting_long_keys) {
    auto keys = generate_keys(100, 256);
    perf_tests::start_measuring_time();
    std::ranges::sort(keys);
    perf_tests::stop_measuring_time();
    return keys.size();
}
