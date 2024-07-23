/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/retention_calculator.h"
#include "cluster/archival/tests/service_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/archival.h"
#include "test_utils/tmp_dir.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

using namespace storage;
using namespace archival;
using namespace std::chrono_literals;

inline ss::logger test_log("test"); // NOLINT

struct size_based_retention_test_spec {
    std::vector<segment_spec> remote_segments;
    tristate<size_t> retention_bytes;
    tristate<std::chrono::milliseconds> retention_duration;
    tristate<kafka::offset> desired_start_offset;
    std::optional<model::offset> next_start_offset;
};

const auto delta_10_min = model::timestamp{
  model::timestamp::now().value() - std::chrono::milliseconds{10min}.count()};

const std::vector<size_based_retention_test_spec> retention_tests{
  // retention disabled
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024}, {10, 19, 1024}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .desired_start_offset = tristate<kafka::offset>{},
    .next_start_offset = std::nullopt},

  // retention limit is already met
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024}, {10, 19, 1024}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{1024 * 5},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .desired_start_offset = tristate<kafka::offset>{},
    .next_start_offset = std::nullopt},

  // retention limit lines up with segment end
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024}, {10, 19, 1024}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{1024 * 2},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .next_start_offset = model::offset{20}},

  // retention limit does not line up with segment end
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024}, {10, 19, 1024}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{1024 * 2 - 42},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .desired_start_offset = tristate<kafka::offset>{},
    .next_start_offset = model::offset{30}},

  // only collect the first segment based on size
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024}, {10, 19, 1024}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{1024 * 3 + 42},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .desired_start_offset = tristate<kafka::offset>{},
    .next_start_offset = model::offset{10}},

  // offset in the middle of the first segment
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024}, {10, 19, 1024}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .desired_start_offset = tristate<kafka::offset>(kafka::offset(5)),
    .next_start_offset = std::nullopt},

  // offset at the end of segment
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024}, {10, 19, 1024}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .desired_start_offset = tristate<kafka::offset>(kafka::offset(9)),
    .next_start_offset = std::nullopt},

  // offset at the beginning of next of segment
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024}, {10, 19, 1024}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .desired_start_offset = tristate<kafka::offset>(kafka::offset(10)),
    .next_start_offset = model::offset(10)},

  // offset in the middle of next of segment
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024}, {10, 19, 1024}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .desired_start_offset = tristate<kafka::offset>(kafka::offset(15)),
    .next_start_offset = model::offset(10)},

  // offset past the end of the log
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024}, {10, 19, 1024}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .desired_start_offset = tristate<kafka::offset>(kafka::offset(50)),
    .next_start_offset = model::offset(40)},

  // with kafka offsets
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024, std::nullopt, 0, 1}, {10, 19, 1024, std::nullopt, 2, 2}, {20, 29, 1024, std::nullopt, 2, 3}, {30, 39, 1024, std::nullopt, 4, 5}},
    .retention_bytes = tristate<size_t>{},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .desired_start_offset = tristate<kafka::offset>(kafka::offset(1)),
    .next_start_offset = std::nullopt},

  // with kafka offsets, we should keep offsets even if the start kafka offset
  // "exists" in multiple segments
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024, std::nullopt, 0, 1}, {10, 19, 1024, std::nullopt, 2, 2}, {20, 29, 1024, std::nullopt, 2, 3}, {30, 39, 1024, std::nullopt, 4, 5}},
    .retention_bytes = tristate<size_t>{},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .desired_start_offset = tristate<kafka::offset>(kafka::offset(2)),
    .next_start_offset = model::offset(10)},

  // with kafka offsets, truncate mid-segment
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024, std::nullopt, 0, 1}, {10, 19, 1024, std::nullopt, 2, 2}, {20, 29, 1024, std::nullopt, 2, 3}, {30, 39, 1024, std::nullopt, 4, 5}},
    .retention_bytes = tristate<size_t>{},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .desired_start_offset = tristate<kafka::offset>(kafka::offset(3)),
    .next_start_offset = model::offset(20)},

  // with kafka offsets, truncate to the end
  size_based_retention_test_spec{
    .remote_segments = {{0, 9, 1024, std::nullopt, 0, 1}, {10, 19, 1024, std::nullopt, 2, 2}, {20, 29, 1024, std::nullopt, 2, 3}, {30, 39, 1024, std::nullopt, 4, 5}},
    .retention_bytes = tristate<size_t>{},
    .retention_duration = tristate<std::chrono::milliseconds>{},
    .desired_start_offset = tristate<kafka::offset>(kafka::offset(6)),
    .next_start_offset = model::offset(40)},

  // time based retention
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024, delta_10_min}, {10, 19, 1024, delta_10_min}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{},
    .retention_duration = tristate<std::chrono::milliseconds>{5min},
    .desired_start_offset = tristate<kafka::offset>{},
    .next_start_offset = model::offset{20}},

  // mixed retention
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024, delta_10_min}, {10, 19, 1024, delta_10_min}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{1024 * 2 - 42},
    .retention_duration = tristate<std::chrono::milliseconds>{5min},
    .desired_start_offset = tristate<kafka::offset>{},
    .next_start_offset = model::offset{30}},

  // mixed retention with offset-based retention, no-op
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024, delta_10_min}, {10, 19, 1024, delta_10_min}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{1024 * 2 - 42},
    .retention_duration = tristate<std::chrono::milliseconds>{5min},
    .desired_start_offset = tristate<kafka::offset>{kafka::offset(32)},
    .next_start_offset = model::offset{30}},

  // mixed retention with offset-based retention, truncate past size- or
  // time-based result
  size_based_retention_test_spec{
    .remote_segments
    = {{0, 9, 1024, delta_10_min}, {10, 19, 1024, delta_10_min}, {20, 29, 1024}, {30, 39, 1024}},
    .retention_bytes = tristate<size_t>{1024 * 2 - 42},
    .retention_duration = tristate<std::chrono::milliseconds>{5min},
    .desired_start_offset = tristate<kafka::offset>{kafka::offset(42)},
    .next_start_offset = model::offset{40}},
};

SEASTAR_THREAD_TEST_CASE(test_retention_strategies) {
    temporary_dir tmp_dir("retention_strategy_test");
    auto data_path = tmp_dir.get_path();

    for (const auto& test : retention_tests) {
        // clang-format off
        const auto& [remote_segments, retention_bytes,
                     retention_duration, desired_start_offset,
                     next_start_offset] = test;
        // clang-format on

        vlog(
          test_log.info,
          "Running test case: segments={}, retention_bytes={}, "
          "retention_duration={}, next_start_offset={}",
          remote_segments,
          retention_bytes,
          retention_duration,
          next_start_offset);

        ntp_config config{{"test_ns", "test_topic", 0}, {data_path}};
        config.set_overrides(
          {.retention_bytes = retention_bytes,
           .retention_time = retention_duration});

        cloud_storage::partition_manifest m;
        populate_manifest(m, remote_segments);
        if (desired_start_offset.has_optional_value()) {
            BOOST_REQUIRE(
              m.advance_start_kafka_offset(desired_start_offset.value()));
        }

        auto retention_calculator = retention_calculator::factory(m, config);
        if (next_start_offset.has_value()) {
            BOOST_REQUIRE(retention_calculator.has_value());
            auto next_so = retention_calculator->next_start_offset();
            BOOST_REQUIRE(next_so.has_value());
            BOOST_REQUIRE(next_so == next_start_offset);
        } else {
            if (
              retention_calculator
              && retention_calculator->next_start_offset().has_value()) {
                vlog(
                  test_log.error,
                  "next offset computed: {}",
                  retention_calculator->next_start_offset());
            }
            BOOST_REQUIRE(
              !retention_calculator
              || !retention_calculator->next_start_offset());
        }
    };
}

// Test that emulates what happens when we truncate but don't GC (e.g. if we
// apply retention but change leadership before removing the segments).
// Regression test for #9286
SEASTAR_THREAD_TEST_CASE(test_retention_after_truncation) {
    temporary_dir tmp_dir("retention_strategy_test");
    auto data_path = tmp_dir.get_path();
    cloud_storage::partition_manifest m;
    populate_manifest(m, {{0, 10, 1024}});

    // Set retention policy to truncate the first segment.
    ntp_config config{{"test_ns", "test_topic", 0}, {data_path}};
    config.set_overrides(
      {.retention_bytes = tristate<size_t>{1023},
       .retention_time = tristate<std::chrono::milliseconds>{}});

    // Simulates an iteration of the archival loop that applies retention.
    // Returns the new start_offset to truncate to.
    const auto calculate_next_truncated_offset = [&]() -> model::offset {
        auto retention_calculator = retention_calculator::factory(m, config);
        BOOST_REQUIRE(retention_calculator.has_value());
        auto next_start_offset = retention_calculator->next_start_offset();
        BOOST_REQUIRE(next_start_offset.has_value());
        BOOST_REQUIRE_NE(*next_start_offset, model::offset{});
        return *next_start_offset;
    };

    // Go through the motion of truncating.
    auto first_truncated_offset = calculate_next_truncated_offset();
    vlog(test_log.info, "Truncating to {}", first_truncated_offset);
    BOOST_REQUIRE(m.advance_start_offset(first_truncated_offset));

    // Add another segment without GCing the segments. This may happen if
    // leadership is transferred in between truncation and GC.
    populate_manifest(m, {{11, 20, 1024}});

    // Attempting to apply retention should move the start offset past the
    // one we already moved ot.
    auto second_truncated_offset = calculate_next_truncated_offset();
    BOOST_REQUIRE_GT(second_truncated_offset, first_truncated_offset);
    vlog(test_log.info, "Truncating to {}", second_truncated_offset);
    BOOST_REQUIRE(m.advance_start_offset(second_truncated_offset));
}
