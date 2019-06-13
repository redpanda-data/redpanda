#include "hashing/xx.h"

#include <gtest/gtest.h>

#include <utility>

TEST(xx, incremental_same_as_array) {
    incremental_xxhash64 inc;
    inc.update(1);
    inc.update(2);
    inc.update(42);
    ASSERT_EQ(inc.digest(), xxhash_64(std::array{1, 2, 42}));
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
