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
#include "io/cache.h"
#include "test_utils/test.h"

#include <map>

namespace experimental::io::testing_details {
class cache_hook_accessor {
public:
    static std::optional<size_t>
    get_hook_insertion_time(const io::cache_hook& hook) {
        return hook.ghost_insertion_time_;
    }

    static void
    set_hook_insertion_time(io::cache_hook& hook, std::optional<size_t> time) {
        hook.ghost_insertion_time_ = time;
    }

    static uint8_t get_hook_freq(io::cache_hook& hook) { return hook.freq_; }
};
} // namespace experimental::io::testing_details

namespace io = experimental::io;

class CacheTest : public ::testing::Test {
public:
    static constexpr auto cache_size = 10;
    static constexpr auto small_size = 2;
    static constexpr auto main_size = cache_size - small_size;

    struct entry {
        io::cache_hook hook;
        bool may_evict{true};
        bool evicted{false};
    };

    struct evict {
        bool operator()(entry& e) noexcept {
            if (e.may_evict) {
                e.evicted = true;
                return true;
            }
            return false;
        }
    };

    struct entry_cost {
        size_t operator()(const entry& /*entry*/) noexcept { return 1; }
    };

    using cache_type = io::cache<entry, &entry::hook, evict, entry_cost>;

    void SetUp() override {
        cache = std::make_unique<cache_type>(cache_type::config{
          .cache_size = cache_size, .small_size = small_size});
    }

    void cache_insert_main(entry& e) const {
        // force entry on ghost queue
        set_hook_insertion_time(e, 0);
        cache->insert(e);
    }

    void cache_insert_small(entry& e) const {
        // force entry not on ghost queue
        set_hook_insertion_time(e, {});
        cache->insert(e);
    }

    static std::optional<size_t> get_hook_insertion_time(const entry& entry) {
        return io::testing_details::cache_hook_accessor::
          get_hook_insertion_time(entry.hook);
    }

    static void
    set_hook_insertion_time(entry& entry, std::optional<size_t> time) {
        io::testing_details::cache_hook_accessor::set_hook_insertion_time(
          entry.hook, time);
    }

    static uint8_t get_hook_freq(entry& entry) {
        return io::testing_details::cache_hook_accessor::get_hook_freq(
          entry.hook);
    }

    template<typename... Entries>
    void cleanup(Entries&... entries) {
        (cache->remove(entries), ...);
    }

    std::unique_ptr<cache_type> cache;
};

TEST_F(CacheTest, DefaultState) {
    EXPECT_EQ(cache->stat().main_queue_size, 0);
    EXPECT_EQ(cache->stat().small_queue_size, 0);
}

TEST_F(CacheTest, DefaultHookState) {
    entry e;
    EXPECT_TRUE(e.hook.evicted());
    EXPECT_FALSE(cache->ghost_queue_contains(e));

    cleanup(e);
}

TEST_F(CacheTest, HookCopy) {
    entry e0;
    e0.hook.touch();
    e0.hook.touch();
    cache->insert(e0);
    set_hook_insertion_time(e0, 3); // after insert since insert will reset
    EXPECT_EQ(get_hook_freq(e0), 2);
    EXPECT_EQ(get_hook_insertion_time(e0), 3);
    EXPECT_FALSE(e0.hook.evicted());

    entry e1{.hook = e0.hook};
    EXPECT_EQ(get_hook_freq(e1), 2);
    EXPECT_EQ(get_hook_insertion_time(e1), 3);
    EXPECT_TRUE(e1.hook.evicted());

    cleanup(e0, e1);
}

TEST_F(CacheTest, Insert) {
    entry e0;
    cache_insert_main(e0);
    EXPECT_EQ(cache->stat().main_queue_size, 1);
    EXPECT_EQ(cache->stat().small_queue_size, 0);
    EXPECT_FALSE(cache->ghost_queue_contains(e0));
    EXPECT_FALSE(e0.hook.evicted());

    entry e1;
    cache_insert_small(e1);
    EXPECT_EQ(cache->stat().small_queue_size, 1);
    EXPECT_EQ(cache->stat().main_queue_size, 1);
    EXPECT_FALSE(cache->ghost_queue_contains(e1));
    EXPECT_FALSE(e1.hook.evicted());

    cleanup(e0, e1);
}

TEST_F(CacheTest, InsertPastCapacity) {
    // fill main queue to capacity
    std::vector<entry> mains(main_size);
    for (auto& e : mains) {
        cache_insert_main(e);
    }

    // fill small queue to capacity
    std::vector<entry> smalls(small_size);
    for (auto& e : smalls) {
        cache->insert(e);
    }

    EXPECT_EQ(
      cache->stat().small_queue_size + cache->stat().main_queue_size,
      cache_size);

    // the next insertion will exceed capacity, and then
    // subsequent insertions will first evict before insert.
    std::vector<entry> more(4);
    for (auto& e : more) {
        cache->insert(e);
        EXPECT_EQ(
          cache->stat().small_queue_size + cache->stat().main_queue_size,
          cache_size + 1);
    }

    // mark everything as non-evictable
    for (auto& e : mains) {
        e.may_evict = false;
    }
    for (auto& e : smalls) {
        e.may_evict = false;
    }
    for (auto& e : more) {
        e.may_evict = false;
    }

    // if everythign is non-evictable we'll allow insertion to continue. the
    // assumption here is that this is one extremely rare and two that the
    // caller has already made a memory reservation / accounted for the entry.
    entry e0;
    cache->insert(e0);
    EXPECT_EQ(
      cache->stat().small_queue_size + cache->stat().main_queue_size,
      cache_size + 2);

    for (auto& e : mains) {
        cleanup(e);
    }
    for (auto& e : smalls) {
        cleanup(e);
    }
    for (auto& e : more) {
        cleanup(e);
    }
    cleanup(e0);
}

TEST_F(CacheTest, Remove) {
    entry e0;
    cache->remove(e0);
    EXPECT_EQ(cache->stat().main_queue_size, 0);
    EXPECT_EQ(cache->stat().small_queue_size, 0);

    entry e1;
    cache_insert_main(e0);
    cache_insert_small(e1);
    EXPECT_EQ(cache->stat().main_queue_size, 1);
    EXPECT_EQ(cache->stat().small_queue_size, 1);

    cache->remove(e0);
    EXPECT_EQ(cache->stat().main_queue_size, 0);
    EXPECT_EQ(cache->stat().small_queue_size, 1);

    cache->remove(e1);
    EXPECT_EQ(cache->stat().main_queue_size, 0);
    EXPECT_EQ(cache->stat().small_queue_size, 0);

    cleanup(e0, e1);
}

TEST_F(CacheTest, EvictMain) {
    entry e0;
    entry e1;
    cache_insert_main(e0);
    cache_insert_main(e1);
    EXPECT_EQ(cache->stat().main_queue_size, 2);
    EXPECT_EQ(cache->stat().small_queue_size, 0);

    // e0 is first evicted
    cache->evict();
    EXPECT_TRUE(e0.evicted);
    EXPECT_FALSE(e1.evicted);
    EXPECT_EQ(cache->stat().main_queue_size, 1);
    EXPECT_EQ(cache->stat().small_queue_size, 0);

    // then e1
    cache->evict();
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(e1.evicted);
    EXPECT_EQ(cache->stat().main_queue_size, 0);
    EXPECT_EQ(cache->stat().small_queue_size, 0);

    cleanup(e0, e1);
}

TEST_F(CacheTest, EvictMainPinned) {
    entry e0;
    entry e1;
    entry e2;
    cache_insert_main(e0);
    cache_insert_main(e1);
    cache_insert_main(e2);
    EXPECT_EQ(cache->stat().main_queue_size, 3);
    EXPECT_EQ(cache->stat().small_queue_size, 0);

    e0.may_evict = false;
    e1.may_evict = false;
    e2.may_evict = false;

    cache->evict();
    cache->evict();
    cache->evict();
    EXPECT_EQ(cache->stat().main_queue_size, 3);
    EXPECT_EQ(cache->stat().small_queue_size, 0);
    EXPECT_FALSE(e0.evicted);
    EXPECT_FALSE(e1.evicted);
    EXPECT_FALSE(e2.evicted);

    e1.may_evict = true;
    cache->evict();
    cache->evict();
    cache->evict();
    EXPECT_EQ(cache->stat().main_queue_size, 2);
    EXPECT_EQ(cache->stat().small_queue_size, 0);
    EXPECT_FALSE(e0.evicted);
    EXPECT_TRUE(e1.evicted);
    EXPECT_FALSE(e2.evicted);

    e0.may_evict = true;
    e2.may_evict = true;
    cache->evict();
    EXPECT_EQ(cache->stat().main_queue_size, 1);
    EXPECT_EQ(cache->stat().small_queue_size, 0);
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(e1.evicted);
    EXPECT_FALSE(e2.evicted);

    cache->evict();
    EXPECT_EQ(cache->stat().main_queue_size, 0);
    EXPECT_EQ(cache->stat().small_queue_size, 0);
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(e1.evicted);
    EXPECT_TRUE(e2.evicted);

    cleanup(e0, e1, e2);
}

TEST_F(CacheTest, EvictMainReinsert) {
    entry e0;
    entry e1;
    cache_insert_main(e0);
    cache_insert_main(e1);
    EXPECT_FALSE(e0.evicted);
    EXPECT_FALSE(e1.evicted);
    EXPECT_EQ(cache->stat().main_queue_size, 2);
    EXPECT_EQ(cache->stat().small_queue_size, 0);

    // e0 is reinserted instead of evicted
    // and e1 is actually evicted
    e0.hook.touch();
    e0.hook.touch(); // freq = 2
    cache->evict();
    EXPECT_FALSE(e0.evicted);
    EXPECT_TRUE(e1.evicted);

    // freq was 2 so two promotions allowed
    cache->evict();
    EXPECT_FALSE(e0.evicted);

    cache->evict();
    EXPECT_TRUE(e0.evicted);

    EXPECT_EQ(cache->stat().main_queue_size, 0);
    EXPECT_EQ(cache->stat().small_queue_size, 0);

    cleanup(e0, e1);
}

TEST_F(CacheTest, EvictMainSingleHotEntry) {
    entry e0;
    cache_insert_main(e0);
    EXPECT_EQ(cache->stat().main_queue_size, 1);
    EXPECT_EQ(cache->stat().small_queue_size, 0);

    // e0 is reinserted instead of evicted
    e0.hook.touch();
    cache->evict();
    EXPECT_FALSE(e0.evicted);
    EXPECT_EQ(cache->stat().main_queue_size, 1);
    EXPECT_EQ(cache->stat().small_queue_size, 0);

    // e0 stays in if it remains hot
    e0.hook.touch();
    e0.hook.touch();
    e0.hook.touch();
    e0.hook.touch();
    e0.hook.touch();
    cache->evict();
    cache->evict();
    cache->evict(); // touched 5x, freq capped at 3
    EXPECT_FALSE(e0.evicted);
    EXPECT_EQ(cache->stat().main_queue_size, 1);
    EXPECT_EQ(cache->stat().small_queue_size, 0);

    // became cold and evicted
    cache->evict();
    EXPECT_TRUE(e0.evicted);
    EXPECT_EQ(cache->stat().main_queue_size, 0);
    EXPECT_EQ(cache->stat().small_queue_size, 0);

    cleanup(e0);
}

TEST_F(CacheTest, EvictSmall) {
    entry e0;
    entry e1;
    entry e2;
    entry e3;
    cache->insert(e0);
    cache->insert(e1);
    cache->insert(e2);
    cache->insert(e3);
    EXPECT_EQ(cache->stat().small_queue_size, 4);
    EXPECT_EQ(cache->stat().main_queue_size, 0);

    EXPECT_FALSE(cache->ghost_queue_contains(e0));
    EXPECT_FALSE(cache->ghost_queue_contains(e1));
    EXPECT_FALSE(cache->ghost_queue_contains(e2));
    EXPECT_FALSE(cache->ghost_queue_contains(e3));

    // e0 is first evicted
    cache->evict();
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(cache->ghost_queue_contains(e0));
    EXPECT_FALSE(e1.evicted);
    EXPECT_FALSE(e2.evicted);
    EXPECT_FALSE(e3.evicted);
    EXPECT_EQ(cache->stat().small_queue_size, 3);
    EXPECT_EQ(cache->stat().main_queue_size, 0);

    // then e1
    cache->evict();
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(cache->ghost_queue_contains(e0));
    EXPECT_TRUE(e1.evicted);
    EXPECT_TRUE(cache->ghost_queue_contains(e1));
    EXPECT_FALSE(e2.evicted);
    EXPECT_FALSE(e3.evicted);
    EXPECT_EQ(cache->stat().small_queue_size, 2);
    EXPECT_EQ(cache->stat().main_queue_size, 0);

    cleanup(e0, e1, e2, e3);
}

TEST_F(CacheTest, EvictSmallPinned) {
    entry e0;
    entry e1;
    entry e2;
    entry e3;
    entry e4;
    cache->insert(e0);
    cache->insert(e1);
    cache->insert(e2);
    cache->insert(e3);
    cache->insert(e4);
    EXPECT_EQ(cache->stat().small_queue_size, 5);
    EXPECT_EQ(cache->stat().main_queue_size, 0);

    EXPECT_FALSE(e0.hook.evicted());
    EXPECT_FALSE(e1.hook.evicted());
    EXPECT_FALSE(e2.hook.evicted());
    EXPECT_FALSE(e3.hook.evicted());
    EXPECT_FALSE(e4.hook.evicted());

    e0.may_evict = false;
    e1.may_evict = false;
    e2.may_evict = false;
    e3.may_evict = false;
    e4.may_evict = false;

    cache->evict();
    cache->evict();
    cache->evict();
    cache->evict();
    cache->evict();
    EXPECT_EQ(cache->stat().small_queue_size, 5);
    EXPECT_EQ(cache->stat().main_queue_size, 0);
    EXPECT_FALSE(e0.evicted);
    EXPECT_FALSE(e1.evicted);
    EXPECT_FALSE(e2.evicted);
    EXPECT_FALSE(e3.evicted);
    EXPECT_FALSE(e4.evicted);

    e1.may_evict = true;
    cache->evict();
    cache->evict();
    cache->evict();
    EXPECT_EQ(cache->stat().small_queue_size, 4);
    EXPECT_EQ(cache->stat().main_queue_size, 0);
    EXPECT_FALSE(e0.evicted);
    EXPECT_TRUE(e1.evicted);
    EXPECT_TRUE(e1.hook.evicted());
    EXPECT_TRUE(cache->ghost_queue_contains(e1));
    EXPECT_FALSE(e2.evicted);
    EXPECT_FALSE(e3.evicted);
    EXPECT_FALSE(e4.evicted);

    e0.may_evict = true;
    e2.may_evict = true;
    cache->evict();
    EXPECT_EQ(cache->stat().small_queue_size, 3);
    EXPECT_EQ(cache->stat().main_queue_size, 0);
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(e0.hook.evicted());
    EXPECT_TRUE(cache->ghost_queue_contains(e0));
    EXPECT_TRUE(e1.evicted);
    EXPECT_TRUE(e1.hook.evicted());
    EXPECT_TRUE(cache->ghost_queue_contains(e1));
    EXPECT_FALSE(e2.evicted);
    EXPECT_FALSE(e3.evicted);
    EXPECT_FALSE(e4.evicted);

    cache->evict();
    EXPECT_EQ(cache->stat().small_queue_size, 2);
    EXPECT_EQ(cache->stat().main_queue_size, 0);
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(e0.hook.evicted());
    EXPECT_TRUE(cache->ghost_queue_contains(e0));
    EXPECT_TRUE(e1.evicted);
    EXPECT_TRUE(e1.hook.evicted());
    EXPECT_TRUE(cache->ghost_queue_contains(e1));
    EXPECT_TRUE(e2.evicted);
    EXPECT_TRUE(e2.hook.evicted());
    EXPECT_TRUE(cache->ghost_queue_contains(e2));
    EXPECT_FALSE(e3.evicted);
    EXPECT_FALSE(e4.evicted);

    cleanup(e0, e1, e2, e3, e4);
}

TEST_F(CacheTest, EvictSmallWarm) {
    entry e0;
    entry e1;
    entry e2;
    cache->insert(e0);
    cache->insert(e1);
    cache->insert(e2);
    EXPECT_EQ(cache->stat().small_queue_size, 3);
    EXPECT_EQ(cache->stat().main_queue_size, 0);

    // promotion from small to main queue requires more than one re-access. so
    // for a single re-access we expect the entry to be evicted.
    e0.hook.touch();
    cache->evict();
    EXPECT_TRUE(e0.evicted);
    EXPECT_TRUE(e0.hook.evicted());
    EXPECT_FALSE(e1.evicted);
    EXPECT_FALSE(e2.evicted);
    EXPECT_EQ(cache->stat().small_queue_size, 2);
    EXPECT_EQ(cache->stat().main_queue_size, 0);

    cleanup(e0, e1, e2);
}

TEST_F(CacheTest, EvictSmallHot) {
    entry e0;
    entry e1;
    entry e2;
    entry e3;
    cache->insert(e0);
    cache->insert(e1);
    cache->insert(e2);
    cache->insert(e3);
    EXPECT_EQ(cache->stat().small_queue_size, 4);
    EXPECT_EQ(cache->stat().main_queue_size, 0);

    // hot items in small queue are evicted to main
    e0.hook.touch();
    e0.hook.touch();
    cache->evict();
    EXPECT_FALSE(e0.evicted);
    EXPECT_FALSE(e0.hook.evicted());
    EXPECT_EQ(get_hook_freq(e0), 0);
    EXPECT_EQ(cache->stat().small_queue_size, 2);
    EXPECT_EQ(cache->stat().main_queue_size, 1);

    // e0 was evicted to main, but evict() will try to actually evict some data
    // so the next entry considered is e1 which is evicted
    EXPECT_TRUE(e1.evicted);
    EXPECT_TRUE(e1.hook.evicted());

    cleanup(e0, e1, e2, e3);
}

TEST_F(CacheTest, EvictSmallToFullMain) {
    // fill main queue to capacity
    std::vector<entry> mains(main_size);
    for (auto& e : mains) {
        cache_insert_main(e);
    }
    EXPECT_EQ(cache->stat().main_queue_size, main_size);

    // fill small queue right past capacity
    std::vector<entry> smalls(small_size + 1);
    for (auto& e : smalls) {
        cache->insert(e);
    }
    EXPECT_EQ(cache->stat().small_queue_size, small_size + 1);

    // the small queue is over capacity, so it will be evicted, but since it is
    // a hot item we'll evict it to the main queue.
    smalls[0].hook.touch();
    smalls[0].hook.touch();
    mains[0].hook.touch();
    cache->evict();
    EXPECT_FALSE(smalls[0].evicted);
    EXPECT_FALSE(smalls[0].hook.evicted());
    EXPECT_EQ(cache->stat().small_queue_size, small_size);

    // but since main is at capacity, this will trigger main eviction. the first
    // main entry is promoted, so the second is evicted. main queue size remains
    // unchanged--it added one and evicted one.
    EXPECT_FALSE(mains[0].evicted);
    EXPECT_FALSE(mains[0].hook.evicted());
    EXPECT_TRUE(mains[1].evicted);
    EXPECT_TRUE(mains[1].hook.evicted());
    EXPECT_EQ(cache->stat().main_queue_size, main_size);

    for (auto& e : mains) {
        cleanup(e);
    }
    for (auto& e : smalls) {
        cleanup(e);
    }
}

/*
 * same as EvictSmallToFullMain except none of the entries in main are evictable
 * so we'll fall back to evicting from the small queue.
 */
TEST_F(CacheTest, EvictSmallToFullMainPinned) {
    // fill main queue to capacity
    std::vector<entry> mains(main_size);
    for (auto& e : mains) {
        cache_insert_main(e);
        e.may_evict = false;
    }
    EXPECT_EQ(cache->stat().main_queue_size, main_size);

    // fill small queue right past capacity
    std::vector<entry> smalls(small_size + 1);
    for (auto& e : smalls) {
        cache->insert(e);
    }
    EXPECT_EQ(cache->stat().small_queue_size, small_size + 1);

    // the small queue is over capacity, so it will be evicted, but since it is
    // a hot item we'll evict it to the main queue.
    smalls[0].hook.touch();
    smalls[0].hook.touch();
    smalls[0].may_evict = false;
    cache->evict();
    EXPECT_FALSE(smalls[0].evicted);
    EXPECT_FALSE(smalls[0].hook.evicted());

    // but since main is at capacity, this will trigger main eviction. but none
    // of the entries in main are evictable (including the small that was just
    // promoted to main). so we'll fall back to small queue eviction.
    EXPECT_EQ(cache->stat().small_queue_size, small_size - 1);
    EXPECT_EQ(cache->stat().main_queue_size, main_size + 1);
    for (const auto& e : mains) {
        EXPECT_FALSE(e.evicted);
        EXPECT_FALSE(e.hook.evicted());
    }
    EXPECT_TRUE(smalls[1].evicted);
    EXPECT_TRUE(smalls[1].hook.evicted());

    for (auto& e : mains) {
        cleanup(e);
    }
    for (auto& e : smalls) {
        cleanup(e);
    }
}

TEST_F(CacheTest, NoEvictable) {
    // fill main queue to capacity
    std::vector<entry> mains(main_size);
    for (auto& e : mains) {
        cache_insert_main(e);
    }

    for (auto& e : mains) {
        e.may_evict = false;
    }

    EXPECT_FALSE(cache->evict());

    // fill small queue to capacity
    std::vector<entry> smalls(small_size + 1);
    for (auto& e : smalls) {
        cache->insert(e);
    }

    for (auto& e : smalls) {
        e.may_evict = false;
    }

    EXPECT_FALSE(cache->evict());

    EXPECT_EQ(
      cache->stat().small_queue_size + cache->stat().main_queue_size,
      cache_size + 1);

    for (auto& e : mains) {
        cleanup(e);
    }
    for (auto& e : smalls) {
        cleanup(e);
    }
}

TEST_F(CacheTest, Formattable) {
    entry e0;
    entry e1;
    entry e2;
    EXPECT_EQ(fmt::format("{}", *cache), "main 0 small 0");
    cache_insert_main(e0);
    EXPECT_EQ(fmt::format("{}", *cache), "main 1 small 0");
    cache_insert_small(e1);
    EXPECT_EQ(fmt::format("{}", *cache), "main 1 small 1");
    cache_insert_small(e2);
    EXPECT_EQ(fmt::format("{}", *cache), "main 1 small 2");
    cleanup(e0, e1, e2);
}

TEST(CacheTestCustom, CustomCost) {
    struct entry {
        io::cache_hook hook;
        std::string data;
    };

    struct entry_cost {
        size_t operator()(const entry& entry) noexcept {
            return entry.data.size();
        }
    };

    constexpr auto cache_size = 100;
    constexpr auto small_size = 5;

    using cache_type
      = io::cache<entry, &entry::hook, io::default_cache_evictor, entry_cost>;

    cache_type cache(
      cache_type::config{.cache_size = cache_size, .small_size = small_size});

    entry e1{.data = "."};
    entry e2{.data = ".."};
    entry e10{.data = ".........."};

    cache.insert(e1);
    EXPECT_EQ(cache.stat().main_queue_size + cache.stat().small_queue_size, 1);
    cache.insert(e2);
    EXPECT_EQ(cache.stat().main_queue_size + cache.stat().small_queue_size, 3);
    cache.insert(e10);
    EXPECT_EQ(cache.stat().main_queue_size + cache.stat().small_queue_size, 13);

    cache.evict();
    EXPECT_EQ(cache.stat().main_queue_size + cache.stat().small_queue_size, 12);

    cache.remove(e2);
    cache.remove(e10);
    EXPECT_EQ(cache.stat().main_queue_size + cache.stat().small_queue_size, 0);

    cache.remove(e1);
    cache.remove(e2);
    cache.remove(e10);
}
