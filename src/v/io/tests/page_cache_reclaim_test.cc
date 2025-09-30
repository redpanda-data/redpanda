
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
#include "io/page.h"
#include "io/page_cache.h"
#include "io/page_set.h"
#include "ssx/async_algorithm.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"

#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

using namespace std::chrono_literals;

namespace io = experimental::io;

static ss::logger lg{"test"};

struct reclaim_test {
    static constexpr size_t page_size = 4096;
    static constexpr double ghost_queue_ratio_limit = 0.05;

    seastar::gate gate;
    const io::page_cache::config cache_config;
    io::page_cache cache;
    io::page_set pages;
    io::page_set pages_referenced;
    uint64_t next_offset{0};
    struct io::page_cache::stats prev_stats;
    uint64_t ghost_queue_size{0};
    uint64_t evictable{0};
    double evictable_ratio{0.0};
    double ghost_queue_ratio{0.0};

    explicit reclaim_test(size_t cache_size)
      : cache_config{cache_size - 1, cache_size - 2}
      , cache(cache_config)
      , prev_stats(cache.stats()) {}

    void start() {
        std::ignore = ghost_queue_vaccum();
        std::ignore = page_churner();
    }

    seastar::future<> stop() {
        co_await gate.close();
        for (auto& page : pages) {
            cache.remove(*page);
        }
    }

    void add_new_page(
      std::optional<uint64_t> offset = std::nullopt,
      std::optional<io::page::flags> flag = std::nullopt,
      const std::optional<utils::s3_fifo::cache_hook>& hook
      = std::nullopt) noexcept {
        auto page = alloc_page(offset.value_or(next_offset), hook);
        if (!offset.has_value()) {
            next_offset += page->size();
        }
        if (flag.has_value()) {
            page->set_flag(flag.value());
        } else {
            ++evictable;
        }
        auto res = pages.insert(page);
        vassert(res.second, "could not insert page");
        cache.insert(*page);
    }

    void update_stats() {
        auto curr_stats = cache.stats();
        auto evictions_granted = curr_stats.evictions_granted()
                                 - prev_stats.evictions_granted();
        ghost_queue_size += evictions_granted;
        evictable -= evictions_granted;
        const auto live_pages = pages.size() - ghost_queue_size;
        evictable_ratio = static_cast<double>(live_pages)
                          / static_cast<double>(evictable);
        ghost_queue_ratio = static_cast<double>(ghost_queue_size)
                            / static_cast<double>(pages.size());
        prev_stats = curr_stats;
    }

    /*
     * eviction move a page into the ghost queue, but it page structure remains.
     * these needs to be vaccumed up in practice. here we just use a brute force
     * approach. in real world this work should handled more intelligently.
     */
    seastar::future<> ghost_queue_vaccum() {
        auto gh = gate.hold();
        while (!gate.is_closed()) {
            co_await seastar::yield();
            update_stats();
            if (ghost_queue_ratio <= ghost_queue_ratio_limit) {
                continue;
            }
            auto offset = random_generators::get_int<size_t>(0, pages.size());
            auto it = pages.begin();
            std::advance(it, offset);
            for (int i = 0; i < 50 && it != pages.end();) {
                if ((*it)->data().empty()) {
                    it = pages.erase(it);
                    --ghost_queue_size;
                } else {
                    ++it;
                }
            }
        }
    }

    [[nodiscard]] bool is_referenced(uint64_t offset) const {
        return pages_referenced.find(offset) != pages_referenced.end();
    }

    [[nodiscard]] static bool
    is_clean(const ss::lw_shared_ptr<io::page>& page) {
        return !page->test_flag(io::page::flags::dirty)
               && !page->test_flag(io::page::flags::faulting);
    }

    /*
     * random churn the state of pages
     */
    seastar::future<> page_churner() {
        auto gh = gate.hold();
        while (!gate.is_closed()) {
            co_await seastar::yield();
            auto offset = random_generators::get_int<size_t>(0, next_offset);
            auto it = pages.find(offset);
            if (it == pages.end()) {
                /*
                 * page was removed by ghost queue vaccum. a read to this
                 * location would allocate a new page in the faulting state.
                 */
                add_new_page(
                  seastar::align_down(offset, page_size),
                  io::page::flags::faulting);
                continue;
            }

            auto page = *it;

            /*
             * remove page from the ghost queue, and maybe fault it back
             */
            if (page->data().empty()) {
                pages.erase(it);
                --ghost_queue_size;
                if (tests::random_bool()) {
                    add_new_page(
                      page->offset(),
                      io::page::flags::faulting,
                      page->cache_hook);
                }
                continue;
            }

            bool was_dirty_or_faulting = false;

            // if faulting, then can make clean
            if (page->test_flag(io::page::flags::faulting)) {
                was_dirty_or_faulting = true;
                page->clear_flag(io::page::flags::faulting);
                if (!is_referenced(page->offset())) {
                    evictable++;
                }
            }

            // if dirty, then can read or make clean
            if (page->test_flag(io::page::flags::dirty)) {
                was_dirty_or_faulting = true;
                page->clear_flag(io::page::flags::dirty);
                if (!is_referenced(page->offset())) {
                    evictable++;
                }
            }

            // if clean, then can make dirty or read
            if (!was_dirty_or_faulting) {
                page->set_flag(io::page::flags::faulting);
                if (!is_referenced(page->offset())) {
                    evictable--;
                }
            }

            // churn the set of extra references
            if (tests::random_bool()) {
                if (!is_referenced(page->offset())) {
                    auto res = pages_referenced.insert(page);
                    vassert(res.second, "failed to insert page");
                    if (is_clean(page)) {
                        evictable--;
                    }
                }
                if (pages_referenced.size() > 10) {
                    auto offset = random_generators::get_int<size_t>(
                      0, pages_referenced.size());
                    auto it = pages_referenced.begin();
                    std::advance(it, offset);
                    if (it != pages_referenced.end()) {
                        if (is_clean(*it)) {
                            evictable++;
                        }
                        pages_referenced.erase(it);
                    }
                }
            }
        }
    }

    static seastar::lw_shared_ptr<io::page> alloc_page(
      uint64_t offset,
      std::optional<utils::s3_fifo::cache_hook> hook = std::nullopt) noexcept {
        auto buf = seastar::temporary_buffer<char>::aligned(
          page_size, page_size);
        if (hook.has_value()) {
            return seastar::make_lw_shared<io::page>(
              offset, std::move(buf), hook.value());
        }
        return seastar::make_lw_shared<io::page>(offset, std::move(buf));
    }
};

class ReclaimTest : public testing::TestWithParam<bool> {};

TEST_P(ReclaimTest, RandomOps) {
    /*
     * When seastar_reclaim is true, set the cache limit near the limit where
     * seastar will also start issuing reclaim requests. Otherwise, set the
     * cache size limit below the this limit so that it is never reached.
     */
    const auto seastar_reclaim = GetParam();
    const auto cache_size = [seastar_reclaim] {
        const auto total = seastar::memory::stats().total_memory();
        const auto min_free = seastar::memory::min_free_memory();
        if (seastar_reclaim) {
            // +2 to accomodate the adjustment made in reclaim_test constructor
            // to satisfy the s3 requirement that the main fifo queue is larger
            // than the small fifo queue
            return total - min_free + 2;
        }
#ifdef SEASTAR_DEFAULT_ALLOCATOR
        return static_cast<size_t>(200) << 20;
#else
        return (total - min_free) / 2;
#endif
    }();

    reclaim_test rt(cache_size);
    rt.start();

    const auto iters = 100 * 1000;
    for (int i = 0; i < iters; ++i) {
        rt.add_new_page();

        rt.update_stats();
        thread_local static ss::logger::rate_limit rate(250ms);
        lg.log(
          ss::log_level::warn,
          rate,
          "ss reclaims {} evictions requested {} granted {} rejected {} "
          "num_pages {} ghost queue size/ratio {}/{} evictable size/ratio "
          "{}/{}",
          ss::memory::stats().reclaims(),
          rt.cache.stats().evictions_requested(),
          rt.cache.stats().evictions_granted(),
          rt.cache.stats().evictions_rejected(),
          rt.pages.size(),
          rt.ghost_queue_size,
          rt.ghost_queue_ratio,
          rt.evictable,
          rt.evictable_ratio);

        if (i % ssx::async_algo_traits::interval == 0) {
            seastar::yield().get();
        }
    }

    rt.stop().get();

    if (seastar_reclaim) {
        EXPECT_GT(seastar::memory::stats().reclaims(), 1000);
    } else {
        EXPECT_EQ(seastar::memory::stats().reclaims(), 0);
    }

    EXPECT_GT(rt.cache.stats().evictions_requested(), 1000);
    EXPECT_GT(rt.cache.stats().evictions_granted(), 1000);
    EXPECT_GT(rt.cache.stats().evictions_rejected(), 1000);
}

INSTANTIATE_TEST_SUITE_P(
  CacheSizeLimit,
  ReclaimTest,
#ifdef SEASTAR_DEFAULT_ALLOCATOR
  ::testing::Values(false)
#else
  ::testing::Bool()
#endif
);
