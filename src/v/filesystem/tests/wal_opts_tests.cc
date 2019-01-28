#include <utility>

#include <gtest/gtest.h>

#include "wal_opts.h"

TEST(wal_opts_validator, invalid_max_log_segment_size) {
  v::wal_opts o(".", std::chrono::minutes(1), std::chrono::hours(168), -1, 4,
                1024 * 1024, 1024 * 1024 /*BAD log_segment_size 1MB*/);

  ASSERT_EQ(v::wal_opts::validate(o),
            v::wal_opts::validation_status::invalid_log_segment_size);
}
TEST(wal_opts_validator, valid_max_log_segment_size) {
  v::wal_opts o(
    ".", std::chrono::minutes(1), std::chrono::hours(168), -1, 4, 1024 * 1024,
    4096 + (100 * 1024 *
            1024) /*100MB + 4096 is good log_segment_size - default 1G*/);
  ASSERT_EQ(v::wal_opts::validate(o), v::wal_opts::validation_status::ok);
}
TEST(wal_opts_validator, invalid_flush_period) {
  v::wal_opts o(".", std::chrono::milliseconds(1) /*BAD FLUSH PERIOD*/,
                std::chrono::hours(168), -1, 4, 1024 * 1024,
                1024 * 1024 * 1024);
  ASSERT_EQ(v::wal_opts::validate(o),
            v::wal_opts::validation_status::invalid_writer_flush_period);
}
TEST(wal_opts_validator, invalid_retention_period) {
  v::wal_opts o(
    ".", std::chrono::milliseconds(1),
    std::chrono::minutes(59) /*BAD max_retention_period ! must be 1 hr*/, -1, 4,
    1024 * 1024, 1024 * 1024 * 1024);

  ASSERT_EQ(v::wal_opts::validate(o),
            v::wal_opts::validation_status::invalid_retention_period);
}
TEST(wal_opts_validator, log_size_multiples_of_4096) {
  int64_t log100mb = 100 * 1024 * 1024;
  int64_t expected_log_size = 4096 + log100mb;
  v::wal_opts o(".", std::chrono::minutes(1), std::chrono::hours(168), -1, 4,
                1024 * 1024,
                1 + log100mb /*log_segment_size - must be page_multiples*/);
  ASSERT_EQ(v::wal_opts::validate(o), v::wal_opts::validation_status::ok);
  ASSERT_EQ(expected_log_size, o.max_log_segment_size);
}

int
main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
