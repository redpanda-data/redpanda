// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/ntp_config.h"
#include "test_utils/scoped_config.h"

#include <gtest/gtest.h>

#include <memory>

struct segment_bytes_test_params {
    std::optional<bool> cleanup_compact;
    std::optional<uint64_t> override;

    std::optional<uint64_t> log_segment_size;
    std::optional<uint64_t> compacted_log_segment_size;

    std::optional<uint64_t> min_limit;
    std::optional<uint64_t> max_limit;

    std::optional<uint64_t> expected;

    friend void PrintTo(const segment_bytes_test_params& p, std::ostream* o) {
        *o << "{override: " << p.override << ", "
           << "log_segment_size: " << p.log_segment_size << ", "
           << "compacted_log_segment_size: " << p.compacted_log_segment_size
           << ", "
           << "min_limit: " << p.min_limit << ", "
           << "max_limit: " << p.max_limit << ", "
           << "expected: " << p.expected << "}";
    }
};

class segment_bytes_suite
  : public ::testing::TestWithParam<segment_bytes_test_params> {};

TEST_P(segment_bytes_suite, segment_bytes_test) {
    auto c = storage::ntp_config(
      model::ntp("test_ns", "test_tp", model::partition_id(0)),
      "/tmp/foo/bar",
      std::make_unique<storage::ntp_config::default_overrides>());

    // Setup overrides.
    c.get_overrides().segment_size = GetParam().override;
    if (GetParam().cleanup_compact) {
        c.get_overrides().cleanup_policy_bitflags
          = model::cleanup_policy_bitflags(
            model::cleanup_policy_bitflags::compaction);
    }

    // Setup cluster settings.
    scoped_config config;
    if (GetParam().log_segment_size) {
        config.get("log_segment_size").set_value(*GetParam().log_segment_size);
    }
    if (GetParam().compacted_log_segment_size) {
        config.get("compacted_log_segment_size")
          .set_value(*GetParam().compacted_log_segment_size);
    }

    if (GetParam().min_limit) {
        config.get("log_segment_size_min").set_value(GetParam().min_limit);
    }
    if (GetParam().max_limit) {
        config.get("log_segment_size_max").set_value(GetParam().max_limit);
    }

    // Verify expected segment bytes.
    ASSERT_EQ(c.segment_bytes(), GetParam().expected);
}

INSTANTIATE_TEST_SUITE_P(
  exhaustive,
  segment_bytes_suite,
  ::testing::Values(
    // Cluster config is used by default.
    segment_bytes_test_params{
      .log_segment_size = 3_MiB,
      .compacted_log_segment_size = 4_MiB,
      .expected = 3_MiB},
    segment_bytes_test_params{
      .cleanup_compact = true,
      .log_segment_size = 3_MiB,
      .compacted_log_segment_size = 4_MiB,
      .expected = 4_MiB},
    // Cluster defaults are capped by min and max limits.
    segment_bytes_test_params{
      .log_segment_size = 0_MiB,
      .compacted_log_segment_size = 4_MiB,
      .min_limit = 1_MiB,
      .max_limit = 2_MiB,
      .expected = 1_MiB},
    segment_bytes_test_params{
      .log_segment_size = 3_MiB,
      .compacted_log_segment_size = 4_MiB,
      .min_limit = 1_MiB,
      .max_limit = 2_MiB,
      .expected = 2_MiB},
    // cleanup.policy compact works the same.
    segment_bytes_test_params{
      .cleanup_compact = true,
      .log_segment_size = 3_MiB,
      .compacted_log_segment_size = 4_MiB,
      .min_limit = 1_MiB,
      .max_limit = 2_MiB,
      .expected = 2_MiB},
    // Overrides ignore log_segment_size and compacted_log_segment_size.
    segment_bytes_test_params{
      .override = 10_MiB,
      .log_segment_size = 1_MiB,
      .compacted_log_segment_size = 1_MiB,
      .expected = 10_MiB},
    segment_bytes_test_params{
      .override = 10_MiB,
      .log_segment_size = 100_MiB,
      .compacted_log_segment_size = 100_MiB,
      .expected = 10_MiB},
    // Overrides are capped by min and max limits.
    segment_bytes_test_params{
      .override = 1_MiB,
      .min_limit = 2_MiB,
      .max_limit = 3_MiB,
      .expected = 2_MiB},
    segment_bytes_test_params{
      .override = 10_MiB,
      .min_limit = 1_MiB,
      .max_limit = 2_MiB,
      .expected = 2_MiB}));
