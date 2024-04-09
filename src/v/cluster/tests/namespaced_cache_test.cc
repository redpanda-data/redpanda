// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "cluster/namespaced_cache.h"
#include "config/config_store.h"
#include "config/property.h"
#include "container/intrusive_list_helpers.h"
#include "test_utils/test.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>
#include <gtest/gtest.h>

struct fixture : public seastar_test {};

struct entry {
    explicit entry(ss::sstring k)
      : key(std::move(k)) {}
    bool pinned = false;
    ss::sstring key;
    safe_intrusive_list_hook _hook;
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

    entry e0{"e-0"};
    entry e1{"e-1"};
    entry e2{"e-2"};

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
    std::vector<entry> entries{12, entry{"key"}};
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
    std::vector<entry> new_entries{3, entry{"key"}};
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
    std::vector<entry> entries{20, entry{"key"}};
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

struct pinned_entry_evictor {
    bool operator()(const entry& e) const noexcept { return !e.pinned; };
};

TEST_F(fixture, test_eviction_predicate) {
    ss::sstring t_1 = "t_1";
    ss::sstring t_2 = "t_2";
    ss::sstring t_3 = "t_3";

    /**
     * Create cache with capacity of 15 and min 3 entries per namespace
     */
    cluster::
      namespaced_cache<entry, ss::sstring, &entry::_hook, pinned_entry_evictor>
        cache(
          config::mock_binding<size_t>(15), config::mock_binding<size_t>(3));

    // verify initial state
    EXPECT_EQ(cache.get_stats().namespaces.size(), 0);
    EXPECT_EQ(cache.get_stats().total_size, 0);
    std::vector<entry> entries{18, entry{"pinnable"}};

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

TEST_F(fixture, test_insertion_when_all_entries_are_pinned) {
    ss::sstring t_1 = "t_1";
    ss::sstring t_2 = "t_2";
    ss::sstring t_3 = "t_3";

    /**
     * Create cache with capacity of 15 and min 3 entries per namespace
     */
    cluster::
      namespaced_cache<entry, ss::sstring, &entry::_hook, pinned_entry_evictor>
        cache(
          config::mock_binding<size_t>(15), config::mock_binding<size_t>(3));

    // verify initial state
    EXPECT_EQ(cache.get_stats().namespaces.size(), 0);
    EXPECT_EQ(cache.get_stats().total_size, 0);
    std::vector<entry> entries{16, entry{"pinnable"}};

    // fill cache
    for (int i = 0; i < 15; i += 3) {
        cache.insert(t_1, entries[i]);
        cache.insert(t_2, entries[i + 1]);
        cache.insert(t_3, entries[i + 2]);
    }

    EXPECT_EQ(cache.get_stats().total_size, 15);
    // pin all entries
    for (auto& e : entries) {
        e.pinned = true;
    }

    // eviction should fail as there are no entries we can evict
    EXPECT_FALSE(cache.evict());

    // insertion should still be possible even if no entries are evictable
    EXPECT_THROW(cache.insert(t_2, entries[15]), cluster::cache_full_error);
    EXPECT_EQ(cache.get_stats().total_size, 15);

    // unpin some entries
    for (int i = 3; i < 10; ++i) {
        entries[i].pinned = false;
    }

    cache.insert(t_2, entries[15]);

    ASSERT_EQ(cache.get_stats().total_size, 15);
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
    std::vector<entry> entries{18, entry{"default"}};

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

TEST_F(fixture, test_post_eviction_hook) {
    ss::sstring t_1 = "t_1";
    ss::sstring t_2 = "t_2";
    ss::sstring t_3 = "t_3";

    absl::flat_hash_map<ss::sstring, std::unique_ptr<entry>> index;

    static auto index_removing_disposer = [&index](entry& entry) noexcept {
        index.erase(entry.key);
    };

    /**
     * Create cache with capacity of 15 and min 3 entries per namespace
     */
    cluster::namespaced_cache<
      entry,
      ss::sstring,
      &entry::_hook,
      cluster::always_allow_to_evict,
      decltype(index_removing_disposer)>
      cache(
        config::mock_binding<size_t>(15),
        config::mock_binding<size_t>(3),
        cluster::always_allow_to_evict{},
        index_removing_disposer);

    // verify initial state
    EXPECT_EQ(cache.get_stats().namespaces.size(), 0);
    EXPECT_EQ(cache.get_stats().total_size, 0);

    for (int i = 0; i < 15; ++i) {
        auto key = ssx::sformat("k-{}", i);
        index.emplace(key, std::make_unique<entry>(key));
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
    index.emplace("new-key-t-1", std::make_unique<entry>("new-key-t-1"));
    index.emplace("new-key-t-2", std::make_unique<entry>("new-key-t-2"));
    index.emplace("new-key-t-3", std::make_unique<entry>("new-key-t-3"));

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

TEST_F(fixture, test_cache_full) {
    ss::sstring t_1 = "t_1";
    ss::sstring t_2 = "t_2";
    ss::sstring t_3 = "t_3";
    ss::sstring t_4 = "t_4";
    ss::sstring t_5 = "t_5";
    ss::sstring t_6 = "t_6";

    /**
     * Create cache with capacity of 15 and min 3 entries per namespace
     */
    cluster::namespaced_cache<entry, ss::sstring, &entry::_hook> cache(
      config::mock_binding<size_t>(15), config::mock_binding<size_t>(3));

    // verify initial state
    EXPECT_EQ(cache.get_stats().namespaces.size(), 0);
    EXPECT_EQ(cache.get_stats().total_size, 0);
    std::vector<entry> entries{6, entry{"default"}};

    cache.insert(t_1, entries[0]);
    cache.insert(t_2, entries[1]);
    cache.insert(t_3, entries[2]);
    cache.insert(t_4, entries[3]);
    cache.insert(t_5, entries[4]);
    EXPECT_EQ(cache.get_stats().namespaces.size(), 5);
    EXPECT_EQ(cache.get_stats().total_size, 5);

    ASSERT_THROW(cache.insert(t_6, entries[5]), cluster::cache_full_error);

    cache.clear();
}

TEST_F(fixture, test_changing_max_size) {
    ss::sstring t_1 = "t_1";
    ss::sstring t_2 = "t_2";
    ss::sstring t_3 = "t_3";
    config::config_store store;
    config::property<size_t> max_size_property(
      store,
      "max_size",
      "test_max_size",
      {.needs_restart = config::needs_restart::no,
       .visibility = config::visibility::user},
      15);
    /**
     * Create cache with capacity of 15 and min 3 entries per namespace
     */
    cluster::namespaced_cache<entry, ss::sstring, &entry::_hook> cache(
      max_size_property.bind(), config::mock_binding<size_t>(3));

    std::vector<entry> entries{15, entry{"default"}};

    // fill cache
    for (int i = 0; i < 15; i += 3) {
        cache.insert(t_1, entries[i]);
        cache.insert(t_2, entries[i + 1]);
        cache.insert(t_3, entries[i + 2]);
    }
    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().total_size, 15);

    max_size_property.set_value(10);

    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().total_size, 10);

    cache.clear();
}

struct entry_with_ts {
    using clock_type = ss::manual_clock;

    clock_type::time_point get_last_update_timestamp() const {
        return last_update_ts;
    }

    void touch() { last_update_ts = clock_type::now(); }

    clock_type::time_point last_update_ts = clock_type::now();
    safe_intrusive_list_hook _hook;
};

using namespace std::chrono_literals;

TEST_F(fixture, test_removing_old_entries) {
    ss::sstring t_1 = "t_1";
    ss::sstring t_2 = "t_2";
    ss::sstring t_3 = "t_3";
    /**
     * Create cache with capacity of 15 and min 3 entries per namespace
     */
    cluster::namespaced_cache<entry_with_ts, ss::sstring, &entry_with_ts::_hook>
      cache(config::mock_binding<size_t>(15), config::mock_binding<size_t>(3));

    std::vector<entry_with_ts> entries{15, entry_with_ts{}};
    // clock is at 0
    // fill cache
    for (int i = 0; i < 15; i += 3) {
        cache.insert(t_1, entries[i]);
        cache.insert(t_2, entries[i + 1]);
        cache.insert(t_3, entries[i + 2]);
    }
    ss::manual_clock::advance(10s);
    // clock is at 10 seconds
    /**
     * No entries should be evicted as none of them is older than 15s
     */
    cache.evict_older_than<ss::manual_clock>(ss::manual_clock::now() - 15s);
    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().total_size, 15);

    /**
     * All entries should be evicted as all of them are older than 5s
     */
    auto evicted = cache.evict_older_than<ss::manual_clock>(
      ss::manual_clock::now() - 5s);
    EXPECT_EQ(cache.get_stats().namespaces.size(), 0);
    EXPECT_EQ(cache.get_stats().total_size, 0);
    EXPECT_EQ(evicted, 15);

    ss::manual_clock::advance(10s);

    // fill cache once again
    for (int i = 0; i < 15; i += 3) {
        cache.insert(t_1, entries[i]);
        cache.insert(t_2, entries[i + 1]);
        cache.insert(t_3, entries[i + 2]);
    }
    // touch all the entries while advancing clock
    for (int i = 0; i < 15; i += 3) {
        cache.touch(t_1, entries[i]);
        ss::manual_clock::advance(1s);
        cache.touch(t_2, entries[i + 1]);
        ss::manual_clock::advance(1s);
        cache.touch(t_3, entries[i + 2]);
        ss::manual_clock::advance(1s);
    }
    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().total_size, 15);

    // no entries are older than 15 seconds
    cache.evict_older_than<ss::manual_clock>(ss::manual_clock::now() - 15s);
    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().total_size, 15);

    // some of the entries should be evicted
    cache.evict_older_than<ss::manual_clock>(ss::manual_clock::now() - 8s);
    EXPECT_EQ(cache.get_stats().namespaces.size(), 3);
    EXPECT_EQ(cache.get_stats().total_size, 8);

    cache.evict_older_than<ss::manual_clock>(ss::manual_clock::now() - 2s);
    EXPECT_EQ(cache.get_stats().namespaces.size(), 2);
    EXPECT_EQ(cache.get_stats().total_size, 2);

    cache.clear();
}
