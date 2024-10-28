// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/archival/archival_policy.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/tmp_dir.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

TEST(ArchivalPolicyTest, TestGetFileRangeSeekOffsetBelowBeginInclusive) {
    temporary_dir tmp_dir("archival_policy_test");
    auto data_path = tmp_dir.get_path();

    auto b = storage::disk_log_builder{storage::log_config{
      data_path.c_str(),
      4_KiB,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()}};
    b
      | storage::start(
        storage::ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });
    b | storage::add_segment(0);
    auto ts = model::timestamp::now();

    // Produce batches [0,9],[10,19],[30,39]
    for (auto i : {0, 10, 30}) {
        model::test::record_batch_spec spec{
          .offset = model::offset{i},
          .count = 10,
          .records = 10,
          .timestamp = ts,
        };
        b.add_random_batch(spec).get();
        ts = model::timestamp{ts.value() + 1};
    }

    auto segment = b.get_log_segments().back();
    auto upl = ss::make_lw_shared<archival::upload_candidate>(
      {.sources = {segment}});

    for (int i = 20; i < 30; ++i) {
        // The seek result found in the provided segment will be less than
        // begin_inclusive_offset, due to missing offsets [20,29]. Assert that
        // the starting offset for the upload candidate is adjusted to be equal
        // to begin_inclusive_offset.
        auto begin_inclusive_offset = model::offset{i};
        auto result = archival::get_file_range(
                        begin_inclusive_offset,
                        std::nullopt,
                        segment,
                        upl,
                        ss::default_priority_class())
                        .get();

        ASSERT_FALSE(result.has_value());
        ASSERT_EQ(upl->starting_offset, begin_inclusive_offset);
    }
}
