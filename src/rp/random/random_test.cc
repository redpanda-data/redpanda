#include <set>
#include <utility>

#include <gtest/gtest.h>

#include "fast_prng.h"

TEST(fast_prng, basic_gen_100_unique_rands) {
  rp::fast_prng rng;
  std::set<uint32_t> test;
  for (auto i = 0; i < 100; ++i) {
    uint32_t x = rng();
    ASSERT_FALSE(test.count(x));
    test.insert(x);
  }
}

int
main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
