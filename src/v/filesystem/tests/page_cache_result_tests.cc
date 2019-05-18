#include <seastar/core/sstring.hh>

#include <gsl/span>
#include <gtest/gtest.h>

#include <cstring>

// filesystem
#include "page_cache_result.h"

TEST(page_cache_result_lease, ref_count_ctor) {
    page_cache_result r(0, gsl::span<char>(), page_cache_result::priority::low);
    {
        page_cache_result_lease l(&r);
        ASSERT_EQ(1, r.locks);
    }
    ASSERT_TRUE(r.is_evictable());
}
TEST(page_cache_result_lease, ref_count_assignment) {
    page_cache_result r(0, gsl::span<char>(), page_cache_result::priority::low);
    {
        page_cache_result_lease l(&r);
        ASSERT_EQ(1, r.locks);
        {
            page_cache_result_lease l2;
            // assignment operator
            l2 = l;
            ASSERT_EQ(2, r.locks);
        }
        ASSERT_EQ(1, r.locks);
    }
    ASSERT_TRUE(r.is_evictable());
}

TEST(page_cache_result_lease, ref_count_move_assignment) {
    page_cache_result r(0, gsl::span<char>(), page_cache_result::priority::low);
    {
        // move ctor
        page_cache_result_lease l(&r);
        ASSERT_EQ(1, r.locks);
        auto l2 = std::move(l);
        ASSERT_EQ(1, r.locks);
    }
    ASSERT_TRUE(r.is_evictable());
}

TEST(page_cache_result, one_total_page) {
    seastar::sstring payload = "hello world";
    page_cache_result r(
      0,
      gsl::span<char>(payload.data(), payload.data()),
      page_cache_result::priority::low);
    ASSERT_TRUE(r.begin_pageno == 0);
    ASSERT_TRUE(r.end_pageno() == 1);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
