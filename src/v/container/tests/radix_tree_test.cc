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

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

namespace {

// Collect (key, value) prefix matches into a vector for assertions.
template<typename V>
std::vector<std::pair<std::string, V>>
collect(const radix_tree<V>& t, std::string_view query) {
    std::vector<std::pair<std::string, V>> out;
    t.for_each_prefix_of(query, [&](std::string_view k, const V& v) {
        out.emplace_back(std::string{k}, v);
    });
    return out;
}

} // namespace

TEST(radix_tree, empty) {
    radix_tree<int> t;
    EXPECT_TRUE(t.empty());
    EXPECT_EQ(t.size(), 0);
    EXPECT_EQ(t.find("anything"), nullptr);
    EXPECT_TRUE(collect(t, "anything").empty());
}

TEST(radix_tree, insert_and_find) {
    radix_tree<int> t;
    t.insert("topic", 1);
    t.insert("topic-a", 2);
    t.insert("topic-b", 3);

    EXPECT_EQ(t.size(), 3);
    ASSERT_NE(t.find("topic"), nullptr);
    EXPECT_EQ(*t.find("topic"), 1);
    EXPECT_EQ(*t.find("topic-a"), 2);
    EXPECT_EQ(*t.find("topic-b"), 3);
    EXPECT_EQ(t.find("topi"), nullptr);
    EXPECT_EQ(t.find("topic-"), nullptr);
    EXPECT_EQ(t.find("topic-c"), nullptr);
    EXPECT_EQ(t.find("topic-aa"), nullptr);
}

TEST(radix_tree, overwrite) {
    radix_tree<int> t;
    t.insert("k", 1);
    t.insert("k", 2);
    EXPECT_EQ(t.size(), 1);
    EXPECT_EQ(*t.find("k"), 2);
}

TEST(radix_tree, empty_key) {
    radix_tree<int> t;
    t.insert("", 7);
    t.insert("a", 8);
    EXPECT_EQ(t.size(), 2);
    ASSERT_NE(t.find(""), nullptr);
    EXPECT_EQ(*t.find(""), 7);

    // The empty key is a prefix of everything.
    auto m = collect(t, "abc");
    ASSERT_FALSE(m.empty());
    EXPECT_EQ(m.front().first, "");
    EXPECT_EQ(m.front().second, 7);
}

// Exercises the edge-split path: inserting "te" after "team"/"test" must split
// the shared "te" edge, and inserting "t" must split again above it.
TEST(radix_tree, edge_splitting) {
    radix_tree<int> t;
    t.insert("team", 1);
    t.insert("test", 2);
    t.insert("te", 3);
    t.insert("t", 4);
    t.insert("toast", 5);

    EXPECT_EQ(t.size(), 5);
    EXPECT_EQ(*t.find("team"), 1);
    EXPECT_EQ(*t.find("test"), 2);
    EXPECT_EQ(*t.find("te"), 3);
    EXPECT_EQ(*t.find("t"), 4);
    EXPECT_EQ(*t.find("toast"), 5);
    EXPECT_EQ(t.find("tea"), nullptr);
    EXPECT_EQ(t.find("te"), t.find("te")); // stable
}

TEST(radix_tree, for_each_prefix_of_longest_chain) {
    radix_tree<int> t;
    t.insert("t", 1);
    t.insert("tz", 2);
    t.insert("tz-", 3);
    t.insert("tz-events", 4);
    // Not a prefix of the query below.
    t.insert("tz-other", 99);
    t.insert("x", 100);

    auto m = collect(t, "tz-events-2026");
    // Reported shortest-prefix-first.
    std::vector<std::pair<std::string, int>> expected{
      {"t", 1}, {"tz", 2}, {"tz-", 3}, {"tz-events", 4}};
    EXPECT_EQ(m, expected);
}

TEST(radix_tree, for_each_prefix_of_diverges) {
    radix_tree<int> t;
    t.insert("topic-prod", 1);
    t.insert("topic-stag", 2);

    // Shares "topic-" but then diverges; neither stored key is a prefix.
    EXPECT_TRUE(collect(t, "topic-dev").empty());
    // Exact prefix match.
    auto m = collect(t, "topic-prod-1");
    ASSERT_EQ(m.size(), 1);
    EXPECT_EQ(m.front().first, "topic-prod");
}

TEST(radix_tree, erase) {
    radix_tree<int> t;
    t.insert("a", 1);
    t.insert("ab", 2);
    t.insert("abc", 3);

    EXPECT_TRUE(t.erase("ab"));
    EXPECT_FALSE(t.erase("ab"));
    EXPECT_FALSE(t.erase("zzz"));
    EXPECT_EQ(t.size(), 2);
    EXPECT_EQ(t.find("ab"), nullptr);

    // Surrounding keys and prefix traversal past the hole still work.
    EXPECT_EQ(*t.find("a"), 1);
    EXPECT_EQ(*t.find("abc"), 3);
    auto m = collect(t, "abc");
    std::vector<std::pair<std::string, int>> expected{{"a", 1}, {"abc", 3}};
    EXPECT_EQ(m, expected);

    // Re-inserting an erased key restores it as a terminal (re-splitting the
    // edge that the erase merged): size accounting and prefix collection must
    // recover.
    t.insert("ab", 20);
    EXPECT_EQ(t.size(), 3);
    ASSERT_NE(t.find("ab"), nullptr);
    EXPECT_EQ(*t.find("ab"), 20);
    auto m2 = collect(t, "abc");
    std::vector<std::pair<std::string, int>> expected2{
      {"a", 1}, {"ab", 20}, {"abc", 3}};
    EXPECT_EQ(m2, expected2);
}

// Erasing a leaf whose parent becomes a single-child non-terminal merges the
// parent into the surviving sibling and reclaims the freed nodes.
TEST(radix_tree, erase_merges_parent_and_reclaims) {
    radix_tree<int> t;
    t.insert("team", 1);
    t.insert("test", 2);
    // root + "te" + "am" + "st"
    EXPECT_EQ(t.node_count(), 4);

    EXPECT_TRUE(t.erase("test"));
    // "st" is freed; "te" now has the single child "am" and no value, so it
    // folds into "team": root + "team".
    EXPECT_EQ(t.node_count(), 2);
    EXPECT_EQ(*t.find("team"), 1);
    EXPECT_EQ(t.find("test"), nullptr);
}

// Erasing a terminal that still has one child folds it into that child.
TEST(radix_tree, erase_folds_into_child) {
    radix_tree<int> t;
    t.insert("a", 1);
    t.insert("ab", 2);
    EXPECT_EQ(t.node_count(), 3); // root + "a" + "b"

    EXPECT_TRUE(t.erase("a"));
    // "a" loses its value but keeps its one child, so it folds into "ab".
    EXPECT_EQ(t.node_count(), 2); // root + "ab"
    EXPECT_EQ(t.find("a"), nullptr);
    EXPECT_EQ(*t.find("ab"), 2);
}

// A node that still branches (>=2 children) is kept even once it is no longer
// a key.
TEST(radix_tree, erase_keeps_branch_node) {
    radix_tree<int> t;
    t.insert("a", 1);
    t.insert("ab", 2);
    t.insert("ac", 3);
    EXPECT_EQ(t.node_count(), 4); // root + "a" + "b" + "c"

    EXPECT_TRUE(t.erase("a"));
    EXPECT_EQ(t.node_count(), 4); // "a" stays as a branch point
    EXPECT_EQ(t.find("a"), nullptr);
    EXPECT_EQ(*t.find("ab"), 2);
    EXPECT_EQ(*t.find("ac"), 3);
}

// The empty key lives on the root, which is the permanent anchor: erasing it
// clears the value but the root (and the rest of the tree) stays.
TEST(radix_tree, erase_empty_key) {
    radix_tree<int> t;
    t.insert("", 1);
    t.insert("a", 2);
    EXPECT_EQ(t.node_count(), 2); // root (value) + "a"

    EXPECT_TRUE(t.erase(""));
    EXPECT_EQ(t.size(), 1);
    EXPECT_EQ(t.find(""), nullptr);
    EXPECT_EQ(*t.find("a"), 2);
    EXPECT_EQ(t.node_count(), 2); // root still anchors "a"
}

// Erasing a leaf whose parent is itself a key: the leaf is freed but the
// terminal parent is kept (folding it away would lose its value).
TEST(radix_tree, erase_leaf_under_terminal_parent) {
    radix_tree<int> t;
    t.insert("a", 1);
    t.insert("ab", 2);
    EXPECT_EQ(t.node_count(), 3); // root + "a" + "b"

    EXPECT_TRUE(t.erase("ab"));
    EXPECT_EQ(t.node_count(), 2); // "b" freed; terminal "a" kept (childless)
    EXPECT_EQ(t.find("ab"), nullptr);
    EXPECT_EQ(*t.find("a"), 1);
}

// Erasing a leaf whose non-terminal parent still has >=2 children: the leaf is
// freed but the parent stays a branch point (not folded).
TEST(radix_tree, erase_leaf_parent_still_branches) {
    radix_tree<int> t;
    t.insert("ab", 1);
    t.insert("ac", 2);
    t.insert("ad", 3);
    EXPECT_EQ(t.node_count(), 5); // root + "a" + "b" + "c" + "d"

    EXPECT_TRUE(t.erase("ab"));
    EXPECT_EQ(t.node_count(), 4); // "b" freed; "a" still branches over c,d
    EXPECT_EQ(t.find("ab"), nullptr);
    EXPECT_EQ(*t.find("ac"), 2);
    EXPECT_EQ(*t.find("ad"), 3);
}

// Folding a node into a child that is itself a branch: the merged node must
// inherit the child's children.
TEST(radix_tree, erase_folds_into_branch_child) {
    radix_tree<int> t;
    t.insert("a", 1);
    t.insert("abc", 2);
    t.insert("abd", 3);
    EXPECT_EQ(t.node_count(), 5); // root + "a" + "b" + "c" + "d"

    EXPECT_TRUE(t.erase("a"));
    // "a" loses its value and has one child ("b", a branch), so it folds into
    // it; the merged "ab" keeps both grandchildren.
    EXPECT_EQ(t.node_count(), 4); // root + "ab" + "c" + "d"
    EXPECT_EQ(t.find("a"), nullptr);
    EXPECT_EQ(*t.find("abc"), 2);
    EXPECT_EQ(*t.find("abd"), 3);
}

// Erasing every key collapses the tree back to just the root, and reclaimed
// slots are reused by subsequent inserts (the arena does not grow per churn).
TEST(radix_tree, erase_all_collapses_to_root) {
    radix_tree<int> t;
    const std::vector<std::string> keys{
      "a", "ab", "abc", "abd", "b", "ba", "team", "test", "tz", "tz-events"};
    for (size_t i = 0; i < keys.size(); ++i) {
        t.insert(keys[i], static_cast<int>(i));
    }
    const size_t peak = t.node_count();
    for (const auto& k : keys) {
        EXPECT_TRUE(t.erase(k));
    }
    EXPECT_EQ(t.size(), 0);
    EXPECT_TRUE(t.empty());
    EXPECT_EQ(t.node_count(), 1); // only the root remains

    // Re-inserting reuses reclaimed slots rather than growing the arena.
    for (size_t i = 0; i < keys.size(); ++i) {
        t.insert(keys[i], static_cast<int>(i));
    }
    EXPECT_LE(t.node_count(), peak);
}

TEST(radix_tree, clear) {
    radix_tree<int> t;
    t.insert("a", 1);
    t.insert("b", 2);
    t.clear();
    EXPECT_TRUE(t.empty());
    EXPECT_EQ(t.find("a"), nullptr);
    t.insert("c", 3);
    EXPECT_EQ(*t.find("c"), 3);
}

TEST(radix_tree, move) {
    radix_tree<int> t;
    t.insert("hello", 1);
    radix_tree<int> u = std::move(t);
    ASSERT_NE(u.find("hello"), nullptr);
    EXPECT_EQ(*u.find("hello"), 1);
}

// Differential test against std::map: random keys, then verify exact lookup
// and that for_each_prefix_of() agrees with a brute-force prefix scan.
TEST(radix_tree, randomized_against_map) {
    radix_tree<int> t;
    std::map<std::string, int> oracle;

    constexpr int n = 4000;
    for (int i = 0; i < n; ++i) {
        // Short alphabet -> lots of shared prefixes and edge splits.
        auto len = random_generators::get_int<size_t>(0, 12);
        std::string key;
        for (size_t j = 0; j < len; ++j) {
            key.push_back(
              static_cast<char>('a' + random_generators::get_int(0, 3)));
        }
        t.insert(key, i);
        oracle[key] = i;
    }

    EXPECT_EQ(t.size(), oracle.size());
    for (const auto& [k, v] : oracle) {
        ASSERT_NE(t.find(k), nullptr) << "missing key: " << k;
        EXPECT_EQ(*t.find(k), v);
    }

    // Verify prefix collection on a batch of random queries.
    for (int q = 0; q < 500; ++q) {
        auto len = random_generators::get_int<size_t>(0, 14);
        std::string query;
        for (size_t j = 0; j < len; ++j) {
            query.push_back(
              static_cast<char>('a' + random_generators::get_int(0, 3)));
        }

        std::map<std::string, int> brute;
        for (const auto& [k, v] : oracle) {
            if (query.starts_with(k)) {
                brute[k] = v;
            }
        }

        std::map<std::string, int> got;
        std::string prev;
        bool first = true;
        t.for_each_prefix_of(query, [&](std::string_view k, int v) {
            got[std::string{k}] = v;
            // Results must arrive shortest-first.
            if (!first) {
                EXPECT_LT(prev.size(), k.size());
            }
            prev = std::string{k};
            first = false;
        });
        EXPECT_EQ(got, brute) << "query: " << query;
    }
}

// Differential churn test: random interleaved inserts and erases against a
// std::map oracle. Exercises edge splitting (insert) and merging/reclamation
// (erase) on the same tree, and confirms find() agrees with the oracle the
// whole time. Erasing every key at the end must collapse the tree back to the
// root, proving merge + reclamation leaves nothing behind.
TEST(radix_tree, randomized_insert_erase_against_map) {
    radix_tree<int> t;
    std::map<std::string, int> oracle;

    auto random_key = [] {
        auto len = random_generators::get_int<size_t>(0, 10);
        std::string key;
        for (size_t j = 0; j < len; ++j) {
            // Small alphabet -> heavy sharing, frequent splits and merges.
            key.push_back(
              static_cast<char>('a' + random_generators::get_int(0, 3)));
        }
        return key;
    };

    constexpr int ops = 20000;
    for (int i = 0; i < ops; ++i) {
        auto key = random_key();
        // Bias slightly toward inserts so the tree stays populated.
        if (random_generators::get_int(0, 2) == 0) {
            const bool had = oracle.erase(key) > 0;
            EXPECT_EQ(t.erase(key), had) << "erase key='" << key << "'";
        } else {
            t.insert(key, i);
            oracle[key] = i;
        }
        ASSERT_EQ(t.size(), oracle.size());
    }

    for (const auto& [k, v] : oracle) {
        ASSERT_NE(t.find(k), nullptr) << "missing key: " << k;
        EXPECT_EQ(*t.find(k), v);
    }

    // Drain the tree; merge + reclamation must return it to just the root.
    for (const auto& [k, v] : oracle) {
        EXPECT_TRUE(t.erase(k));
    }
    EXPECT_EQ(t.size(), 0);
    EXPECT_EQ(t.node_count(), 1);
}
