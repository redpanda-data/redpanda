#include <utility>

#include <gtest/gtest.h>

#include "wal_name_extractor_utils.h"

namespace std {
static inline bool
operator==(const std::pair<int64_t, int64_t> &x,
           const std::pair<int64_t, int64_t> &y) {
  return x.first == y.first && x.second == y.second;
}
}  // namespace std

TEST(wal_name_extractor_utils_epoch_extractor, basic) {

  ASSERT_EQ(
    v::wal_name_extractor_utils::wal_segment_extract_epoch_term("1234.42.wal"),
    std::make_pair(1234, 42));
  ASSERT_EQ(v::wal_name_extractor_utils::wal_segment_extract_epoch_term(
              "999999999.0.wal"),
            std::make_pair(999999999, 0));
}

TEST(wal_name_extractor_utils_epoch_extractor, empty_string) {
  ASSERT_EQ(v::wal_name_extractor_utils::wal_segment_extract_epoch_term(""),
            std::make_pair(-1, -1));
}

TEST(wal_name_extractor_utils_is_wal_name, basic_false) {
  // old
  ASSERT_FALSE(v::wal_name_extractor_utils::is_wal_segment("1.wal"));
}
TEST(wal_name_extractor_utils_is_wal_name, basic_true) {
  ASSERT_TRUE(v::wal_name_extractor_utils::is_wal_segment("1.2.wal"));
}

TEST(wal_name_extractor_utils_partition_dir_extract, with_valid_path) {
  ASSERT_EQ(
    v::wal_name_extractor_utils::wal_partition_dir_extract("/foo/bar/1"),
    int32_t(1));
}

TEST(wal_name_extractor_utils_extract_topic_partition, basic) {
  ASSERT_TRUE(
    v::wal_name_extractor_utils::is_valid_ns_topic_name("alex_gallego-007"));
}
TEST(wal_name_extractor_utils_epoch_extractor, irl_bug_1) {
  ASSERT_EQ(v::wal_name_extractor_utils::wal_segment_extract_epoch_term(
              "/home/agallego/.redpanda/nemo/rickenbacker/0/703037440.22.wal"),
            std::make_pair(703037440, 22));
}

int
main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
