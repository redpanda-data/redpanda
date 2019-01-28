#include <experimental/array>
#include <utility>

#include <gtest/gtest.h>

#include "xx.h"

using namespace v;  // NOLINT

TEST(xx, incremental_same_as_array) {
  incremental_xxhash64 inc;
  inc.update(1);
  inc.update(2);
  inc.update(42);
  ASSERT_EQ(inc.digest(), xxhash_64(std::experimental::make_array(1, 2, 42)));
}

int
main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
