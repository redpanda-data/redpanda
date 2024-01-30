// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "cluster/namespaced_cache.h"
#include "test_utils/test.h"

#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>
#include <gtest/gtest.h>

struct fixture : public seastar_test {};

struct entry {
    cluster::namespaced_cache_hook _hook;
};

TEST_F(fixture, test_basic_operations) {
    ss::sstring t_1 = "t_1";
    ss::sstring t_2 = "t_2";
    ss::sstring t_3 = "t_3";

    /**
     * Create cache with capacity of 15 and min 3 entries per namespace
     */
    cluster::namespaced_cache<entry, ss::sstring, &entry::_hook> cache(
      config::mock_binding<size_t>(15), config::mock_binding<size_t>(3));
    // verify initial state
    EXPECT_EQ(cache.get_stats().namespaces.size(), 0);
    EXPECT_EQ(cache.get_stats().total_size, 0);

    entry e0;
    entry e1;
    entry e2;
    entry e4;

    cache.insert(t_1, e0);
    cache.insert(t_2, e1);
    cache.insert(t_3, e2);

    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().namespaces[t_1], 1);
    EXPECT_EQ(cache.get_stats().namespaces[t_2], 1);
    EXPECT_EQ(cache.get_stats().namespaces[t_3], 1);
    EXPECT_EQ(cache.get_stats().total_size, 3);
    /**
     * Fill the cache up to capacity
     */
    std::vector<entry> entries{12, entry{}};
    for (int i = 0; i < 12; i += 3) {
        cache.insert(t_1, entries[i]);
        cache.insert(t_2, entries[i + 1]);
        cache.insert(t_3, entries[i + 2]);
    }

    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().namespaces[t_1], 5);
    EXPECT_EQ(cache.get_stats().namespaces[t_2], 5);
    EXPECT_EQ(cache.get_stats().namespaces[t_3], 5);
    EXPECT_EQ(cache.get_stats().total_size, 15);
    std::vector<entry> new_entries{3, entry{}};
    // now each insert will cause eviction
    cache.insert(t_1, new_entries[0]);
    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().namespaces[t_1], 5);
    EXPECT_EQ(cache.get_stats().namespaces[t_2], 5);
    EXPECT_EQ(cache.get_stats().namespaces[t_3], 5);
    EXPECT_EQ(cache.get_stats().total_size, 15);

    cache.insert(t_2, new_entries[1]);
    cache.insert(t_3, new_entries[2]);
    // when number of entries in for a namespace that requests an insert is
    // equal to the number of entries of a namespace with largest number of
    // entries an eviction should happen from the requesting namespace.
    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().namespaces[t_1], 5);
    EXPECT_EQ(cache.get_stats().namespaces[t_2], 5);
    EXPECT_EQ(cache.get_stats().namespaces[t_3], 5);
    EXPECT_EQ(cache.get_stats().total_size, 15);
    // oldest t_2 entry should be evicted
    EXPECT_FALSE(e0._hook.is_linked());
    EXPECT_FALSE(e1._hook.is_linked());
    EXPECT_FALSE(e2._hook.is_linked());
    /**
     * Test removal
     */
    for (int i = 0; i < 12; i += 3) {
        cache.remove(t_1, entries[i]);
        cache.remove(t_2, entries[i + 1]);
        cache.remove(t_3, entries[i + 2]);
    }

    for (auto& e : entries) {
        EXPECT_FALSE(e._hook.is_linked());
    }

    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().namespaces[t_1], 1);
    EXPECT_EQ(cache.get_stats().namespaces[t_2], 1);
    EXPECT_EQ(cache.get_stats().namespaces[t_3], 1);
    EXPECT_EQ(cache.get_stats().total_size, 3);

    cache.remove(t_1, new_entries[0]);
    EXPECT_EQ(cache.get_stats().namespaces.size(), 2);
    EXPECT_EQ(cache.get_stats().namespaces[t_2], 1);
    EXPECT_EQ(cache.get_stats().namespaces[t_3], 1);
    EXPECT_EQ(cache.get_stats().total_size, 2);

    cache.remove(t_1, new_entries[0]);
    EXPECT_EQ(cache.get_stats().namespaces.size(), 2);

    cache.clear();
}

TEST_F(fixture, test_asymmetric_namespaces) {
    ss::sstring t_1 = "t_1";
    ss::sstring t_2 = "t_2";
    ss::sstring t_3 = "t_3";

    /**
     * Create cache with capacity of 15 and min 3 entries per namespace
     */
    cluster::namespaced_cache<entry, ss::sstring, &entry::_hook> cache(
      config::mock_binding<size_t>(15), config::mock_binding<size_t>(3));
    // verify initial state
    EXPECT_EQ(cache.get_stats().namespaces.size(), 0);
    EXPECT_EQ(cache.get_stats().total_size, 0);
    std::vector<entry> entries{20, entry{}};
    /**
     * fill cache with 2 namespaces
     */
    for (int i = 0; i < 7; ++i) {
        cache.insert(t_1, entries[i]);
    }
    for (int i = 7; i < 15; ++i) {
        cache.insert(t_2, entries[i]);
    }
    EXPECT_EQ(cache.get_stats().namespaces.size(), 2);
    EXPECT_EQ(cache.get_stats().namespaces[t_1], 7);
    EXPECT_EQ(cache.get_stats().namespaces[t_2], 8);
    EXPECT_EQ(cache.get_stats().total_size, 15);
    // adding a new entry should cause an eviction from the namespace with the
    // most entries
    cache.insert(t_1, entries[15]);
    EXPECT_EQ(cache.get_stats().namespaces[t_1], 8);
    EXPECT_EQ(cache.get_stats().namespaces[t_2], 7);
    EXPECT_EQ(cache.get_stats().total_size, 15);

    // add another namespace
    cache.insert(t_3, entries[16]);
    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().namespaces[t_1], 7);
    EXPECT_EQ(cache.get_stats().namespaces[t_2], 7);
    EXPECT_EQ(cache.get_stats().namespaces[t_3], 1);
    EXPECT_EQ(cache.get_stats().total_size, 15);
    cache.insert(t_3, entries[17]);
    cache.insert(t_3, entries[18]);
    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().namespaces[t_1], 6);
    EXPECT_EQ(cache.get_stats().namespaces[t_2], 6);
    EXPECT_EQ(cache.get_stats().namespaces[t_3], 3);
    EXPECT_EQ(cache.get_stats().total_size, 15);

    cache.clear();
}

struct pinable_entry {
    bool pinned = false;
    cluster::namespaced_cache_hook _hook;
};
struct pinned_entry_evictor {
    bool operator()(const pinable_entry& e) const noexcept {
        return !e.pinned;
    };
};

TEST_F(fixture, test_eviction_predicate) {
    ss::sstring t_1 = "t_1";
    ss::sstring t_2 = "t_2";
    ss::sstring t_3 = "t_3";

    /**
     * Create cache with capacity of 15 and min 3 entries per namespace
     */
    cluster::namespaced_cache<
      pinable_entry,
      ss::sstring,
      &pinable_entry::_hook,
      pinned_entry_evictor>
      cache(config::mock_binding<size_t>(15), config::mock_binding<size_t>(3));

    // verify initial state
    EXPECT_EQ(cache.get_stats().namespaces.size(), 0);
    EXPECT_EQ(cache.get_stats().total_size, 0);
    std::vector<pinable_entry> entries{18, pinable_entry{}};

    // fill cache, single namespace
    for (int i = 0; i < 15; ++i) {
        cache.insert(t_1, entries[i]);
    }

    EXPECT_EQ(cache.get_stats().namespaces.size(), 1);
    EXPECT_EQ(cache.get_stats().total_size, 15);
    // pin first two entries
    entries[0].pinned = true;
    entries[1].pinned = true;

    // insert entry
    cache.insert(t_2, entries[15]);

    EXPECT_EQ(cache.get_stats().namespaces.size(), 2);
    EXPECT_EQ(cache.get_stats().total_size, 15);

    // verify if pined entries are still in cache
    EXPECT_TRUE(entries[0]._hook.is_linked());
    EXPECT_TRUE(entries[1]._hook.is_linked());
    // next not pinned entry should be evicted
    EXPECT_FALSE(entries[2]._hook.is_linked());
    // unpin entry
    entries[0].pinned = false;
    cache.insert(t_2, entries[16]);
    EXPECT_FALSE(entries[0]._hook.is_linked());
    EXPECT_EQ(cache.get_stats().namespaces.size(), 2);
    EXPECT_EQ(cache.get_stats().total_size, 15);

    cache.clear();
}

TEST_F(fixture, test_touch) {
    ss::sstring t_1 = "t_1";
    ss::sstring t_2 = "t_2";
    ss::sstring t_3 = "t_3";

    /**
     * Create cache with capacity of 15 and min 3 entries per namespace
     */
    cluster::namespaced_cache<entry, ss::sstring, &entry::_hook> cache(
      config::mock_binding<size_t>(15), config::mock_binding<size_t>(3));

    // verify initial state
    EXPECT_EQ(cache.get_stats().namespaces.size(), 0);
    EXPECT_EQ(cache.get_stats().total_size, 0);
    std::vector<entry> entries{18, entry{}};

    // fill cache
    for (int i = 0; i < 15; i += 3) {
        cache.insert(t_1, entries[i]);
        cache.insert(t_2, entries[i + 1]);
        cache.insert(t_3, entries[i + 2]);
    }

    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().total_size, 15);

    cache.touch(t_1, entries[0]);
    cache.touch(t_1, entries[3]);
    cache.insert(t_1, entries[15]);
    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    // touched items should not be evicted
    EXPECT_TRUE(entries[0]._hook.is_linked());
    EXPECT_TRUE(entries[3]._hook.is_linked());

    EXPECT_FALSE(entries[6]._hook.is_linked());

    cache.clear();
}

TEST_F(fixture, test_disposer) {
    ss::sstring t_1 = "t_1";
    ss::sstring t_2 = "t_2";
    ss::sstring t_3 = "t_3";

    struct keyed_entry {
        explicit keyed_entry(ss::sstring k)
          : key(std::move(k)) {}
        ss::sstring key;
        cluster::namespaced_cache_hook _hook;
    };

    absl::flat_hash_map<ss::sstring, std::unique_ptr<keyed_entry>> index;

    static auto index_removing_disposer =
      [&index](keyed_entry& entry) noexcept { index.erase(entry.key); };

    /**
     * Create cache with capacity of 15 and min 3 entries per namespace
     */
    cluster::namespaced_cache<
      keyed_entry,
      ss::sstring,
      &keyed_entry::_hook,
      cluster::default_cache_evictor,
      decltype(index_removing_disposer)>
      cache(
        config::mock_binding<size_t>(15),
        config::mock_binding<size_t>(3),
        cluster::default_cache_evictor{},
        index_removing_disposer);

    // verify initial state
    EXPECT_EQ(cache.get_stats().namespaces.size(), 0);
    EXPECT_EQ(cache.get_stats().total_size, 0);

    for (int i = 0; i < 15; ++i) {
        auto key = ssx::sformat("k-{}", i);
        index.emplace(key, std::make_unique<keyed_entry>(key));
    }

    // fill cache
    for (int i = 0; i < 15; i += 3) {
        cache.insert(t_1, *index[ssx::sformat("k-{}", i)]);
        cache.insert(t_2, *index[ssx::sformat("k-{}", i + 1)]);
        cache.insert(t_3, *index[ssx::sformat("k-{}", i + 2)]);
    }

    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().total_size, 15);

    // all the subsequent inserts will result in eviction
    index.emplace("new-key-t-1", std::make_unique<keyed_entry>("new-key-t-1"));
    index.emplace("new-key-t-2", std::make_unique<keyed_entry>("new-key-t-2"));
    index.emplace("new-key-t-3", std::make_unique<keyed_entry>("new-key-t-3"));

    cache.insert(t_1, *index["new-key-t-1"]);
    cache.insert(t_2, *index["new-key-t-2"]);
    cache.insert(t_3, *index["new-key-t-3"]);
    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().total_size, 15);
    EXPECT_EQ(index.size(), 15);

    // evicted entries should be removed from cache

    EXPECT_FALSE(index.contains("key-0"));
    EXPECT_FALSE(index.contains("key-1"));
    EXPECT_FALSE(index.contains("key-2"));

    cache.clear();
    index.clear();
}
