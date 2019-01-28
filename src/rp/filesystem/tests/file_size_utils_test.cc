#include <gtest/gtest.h>

#include "file_size_utils.h"

TEST(file_size_utils, page_size) {
  int64_t offset = (4096 * 3) + 234;
  int64_t alignment = 4096;
  ASSERT_EQ(rp::offset_to_page(offset, alignment), 3);
}
TEST(file_size_utils, first_page) {
  int64_t offset = 4096;
  int64_t alignment = 4096;
  ASSERT_EQ(rp::offset_to_page(offset, alignment), 1);
}

TEST(file_size_utils, zero_page) {
  int64_t offset = 0;
  int64_t alignment = 4096;
  ASSERT_EQ(rp::offset_to_page(offset, alignment), 0);
}

TEST(file_size_utils, front_buffer) {
  int64_t offset = 234;  // random offset less than a page
  int64_t alignment = 4096;
  ASSERT_EQ(rp::offset_to_page(offset, alignment), 0);
}

TEST(file_size_utils, large_page_size) {
  int64_t offset = (4096 * 239487) + 234;
  int64_t alignment = 4096;
  ASSERT_EQ(rp::offset_to_page(offset, alignment), 239487);
}

int
main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
