/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "gtest/gtest.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/compaction_reducers.h"
#include "storage/key_offset_map.h"
#include "storage/lock_manager.h"
#include "storage/log_reader.h"
#include "storage/probe.h"
#include "storage/segment_deduplication_utils.h"
#include "storage/segment_set.h"
#include "storage/segment_utils.h"
#include "storage/tests/storage_test_fixture.h"
#include "storage/types.h"
#include "test_utils/test.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/defer.hh>

class MapBuildingReducerFixtureTest
  : public storage_test_fixture
  , public seastar_test {};

TEST_F(MapBuildingReducerFixtureTest, TestMapIndexing) {
    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);

    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    storage::ntp_config ntp_cfg(
      ntp,
      mgr.config().base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(overrides));
    auto log = mgr.manage(std::move(ntp_cfg)).get();
    auto disk_log = log;

    // Append some linear kv ints
    int num_appends = 5;
    append_random_batches<linear_int_kv_batch_generator>(log, num_appends);
    log->flush().get();
    disk_log->force_roll(ss::default_priority_class()).get();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 2);

    auto& segments = disk_log->segments();
    auto& seg = segments.front();

    static constexpr int64_t max_keys = 4;
    storage::simple_key_offset_map map(max_keys);
    storage::compaction_config compact_cfg(
      model::offset::max(), std::nullopt, ss::default_priority_class(), as);
    auto pb = storage::probe{};

    auto last_indexed_offset = model::offset{-1};
    auto max_offset = seg->offsets().get_dirty_offset();
    for (int64_t i = 0; last_indexed_offset < max_offset; ++i) {
        storage::index_chunk_of_segment_for_map(
          compact_cfg, seg, map, pb, last_indexed_offset)
          .get();

        int64_t offset
          = (i + 1)
              * (max_keys * linear_int_kv_batch_generator::records_per_batch)
            - 1;
        int64_t expected_offset = std::min(offset, max_offset());
        ASSERT_EQ(map.max_offset(), model::offset{expected_offset});
        ASSERT_EQ(last_indexed_offset, model::offset{expected_offset});
    }

    ASSERT_EQ(map.max_offset(), max_offset);
}
