/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "container/radix_tree.h"
#include "random/generators.h"

#include <seastar/testing/perf_tests.hh>

#include <fmt/format.h>

#include <string>
#include <vector>

namespace {

constexpr size_t key_count = 100'000;
constexpr size_t query_count = 1'000;

// Namespaced, prefix-sharing keys (~28 bytes), representative of hierarchical
// names like ACL resource patterns or object paths. Many keys share the
// "team-NNN/svc-NNN/" prefixes, exercising edge splits and deep shared paths.
std::vector<std::string> make_keys() {
    std::vector<std::string> keys;
    keys.reserve(key_count);
    for (size_t i = 0; i < key_count; ++i) {
        keys.push_back(
          fmt::format(
            "team-{:03}/svc-{:03}/topic-{:06}", i % 200, (i / 200) % 200, i));
    }
    return keys;
}

const std::vector<std::string>& shared_keys() {
    static const std::vector<std::string> keys = make_keys();
    return keys;
}

// Built once and shared by the read-only benchmarks.
const radix_tree<size_t>& shared_tree() {
    static const radix_tree<size_t> tree = [] {
        radix_tree<size_t> t;
        const auto& keys = shared_keys();
        for (size_t i = 0; i < keys.size(); ++i) {
            t.insert(keys[i], i);
        }
        return t;
    }();
    return tree;
}

// `query_count` keys drawn at random from `pool`, optionally transformed (e.g.
// to build hits, misses, or prefix queries).
template<typename Transform>
std::vector<std::string>
sample_queries(const std::vector<std::string>& pool, const Transform& xform) {
    std::vector<std::string> q;
    q.reserve(query_count);
    for (size_t i = 0; i < query_count; ++i) {
        const auto& base
          = pool[random_generators::get_int<size_t>(0, pool.size() - 1)];
        q.push_back(xform(base));
    }
    return q;
}

} // namespace

// Build the whole tree from scratch; reported time is per inserted key.
PERF_TEST(radix_tree, insert) {
    const auto& keys = shared_keys();
    radix_tree<size_t> t;
    perf_tests::start_measuring_time();
    for (size_t i = 0; i < keys.size(); ++i) {
        t.insert(keys[i], i);
    }
    perf_tests::stop_measuring_time();
    return keys.size();
}

// Exact lookup of a present key: one O(key length) descent.
PERF_TEST(radix_tree, find_hit) {
    const auto& t = shared_tree();
    const auto queries = sample_queries(
      shared_keys(), [](const std::string& k) { return k; });
    perf_tests::start_measuring_time();
    for (const auto& k : queries) {
        perf_tests::do_not_optimize(t.find(k));
    }
    perf_tests::stop_measuring_time();
    return queries.size();
}

// Exact lookup of an absent key (same namespace, unknown leaf): descent that
// diverges before reaching a terminal.
PERF_TEST(radix_tree, find_miss) {
    const auto& t = shared_tree();
    const auto queries = sample_queries(
      shared_keys(), [](const std::string& k) { return k + "-absent"; });
    perf_tests::start_measuring_time();
    for (const auto& k : queries) {
        perf_tests::do_not_optimize(t.find(k));
    }
    perf_tests::stop_measuring_time();
    return queries.size();
}

// The prefix query behind longest-prefix matching: descend collecting every
// stored key that is a prefix of the query. Here each query extends a stored
// key, so it has exactly one stored prefix (the common ACL case: 0-1 matches).
PERF_TEST(radix_tree, for_each_prefix_of_single_match) {
    const auto& t = shared_tree();
    const auto queries = sample_queries(
      shared_keys(), [](const std::string& k) { return k + "/sub/leaf"; });
    perf_tests::start_measuring_time();
    size_t matches = 0;
    for (const auto& q : queries) {
        t.for_each_prefix_of(
          q, [&matches](std::string_view, size_t) { ++matches; });
    }
    perf_tests::do_not_optimize(matches);
    perf_tests::stop_measuring_time();
    return queries.size();
}

// Worst-case prefix query: a tree of many nested prefixes of one query, so the
// descent reports many matches in a single pass.
PERF_TEST(radix_tree, for_each_prefix_of_nested) {
    // Keys "a", "aa", ..., 64 nested prefixes of the query below.
    radix_tree<size_t> t;
    std::string nested;
    for (size_t i = 0; i < 64; ++i) {
        nested.push_back('a');
        t.insert(nested, i);
    }
    const std::string query(128, 'a');
    perf_tests::start_measuring_time();
    size_t matches = 0;
    for (size_t iter = 0; iter < query_count; ++iter) {
        t.for_each_prefix_of(
          query, [&matches](std::string_view, size_t) { ++matches; });
    }
    perf_tests::do_not_optimize(matches);
    perf_tests::stop_measuring_time();
    return query_count;
}
