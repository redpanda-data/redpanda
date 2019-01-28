#include <utility>

#include <gtest/gtest.h>

#include "wal_name_extractor_utils.h"

TEST(wal_name_extractor_utils_epoch_extractor, basic) {
  ASSERT_EQ(rp::wal_name_extractor_utils::wal_segment_extract_epoch("1234.wal"),
            uint64_t(1234));
  ASSERT_EQ(
    rp::wal_name_extractor_utils::wal_segment_extract_epoch("999999999.wal"),
    uint64_t(999999999));
}

TEST(wal_name_extractor_utils_epoch_extractor, empty_string) {
  ASSERT_EQ(rp::wal_name_extractor_utils::wal_segment_extract_epoch(""), -1);
}

TEST(wal_name_extractor_utils_is_wal_name, basic) {
  ASSERT_TRUE(rp::wal_name_extractor_utils::is_wal_segment("1.wal"));
}
TEST(wal_name_extractor_utils_is_wal_name, with_valid_path) {
  ASSERT_EQ(rp::wal_name_extractor_utils::wal_segment_extract_epoch("1.wal"),
            uint64_t(1));
}
TEST(wal_name_extractor_utils_extract_topic_partition, basic) {
  ASSERT_TRUE(
    rp::wal_name_extractor_utils::is_valid_ns_topic_name("alex_gallego-007"));
}
TEST(wal_name_extractor_utils_epoch_extractor, irl_bug_1) {
  ASSERT_EQ(rp::wal_name_extractor_utils::wal_segment_extract_epoch(
              "/home/agallego/.bmtl/nemo/rickenbacker/0/703037440.wal"),
            int64_t(703037440));
}

int
main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
