#include <utility>

#include <gtest/gtest.h>
#include <smf/random.h>

#include "hashing/xx.h"

// proves that xxhash incremental and static produce the same result
TEST(xx, incremental_vs_static) {
  smf::random r;
  for (auto i = 0; i < 100; ++i) {
    auto str = r.next_str(255);
    incremental_xxhash64 hx;
    std::size_t first_half = std::max<std::size_t>(1, r.next() % str.size());
    hx.update(str.data(), first_half);
    hx.update(str.data() + first_half, str.size() - first_half);
    ASSERT_EQ(xxhash_64(str.data(), str.size()), hx.digest());
  }
}

int
main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
