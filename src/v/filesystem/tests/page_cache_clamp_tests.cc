#include "filesystem/page_cache.generated.h"

#include <gtest/gtest.h>

#include <utility>

TEST(page_cache_clamp, page_range_exclusive) {
    // test explicitly for the second page.
    // should return (64-128] and NOT (0-64]
    auto clamp = page_cache_table_clamp_page(64);
    ASSERT_EQ(clamp.first, 64);
    ASSERT_EQ(clamp.second, 128);
}
TEST(page_cache_clamp, page_range_min) {
    // test all tuples to make sure we start at the right offset
    for (int32_t i = 0; i < 524288; i += 64) {
        auto clamp = page_cache_table_clamp_page(i);
        ASSERT_EQ(clamp.first, i);
        ASSERT_EQ(clamp.second, i + 64);
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
