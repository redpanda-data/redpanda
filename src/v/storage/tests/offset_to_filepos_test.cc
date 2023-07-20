/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "storage/offset_to_filepos.h"
#include "test_utils/archival.h"
#include "test_utils/tmp_dir.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

SEASTAR_THREAD_TEST_CASE(test_search_begin_offset_not_found) {
    temporary_dir tmp_dir("offset_to_fpos_translate");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });
    b | add_segment(0);
    auto ts = model::timestamp::now();
    auto curr_offset = model::offset{0};
    for (auto i = 0; i < 2; ++i) {
        model::test::record_batch_spec spec{
          .offset = curr_offset,
          .count = 1,
          .records = 1,
          .timestamp = ts,
        };
        b.add_random_batch(spec).get();
        curr_offset = model::next_offset(
          b.get_log_segments().back()->offsets().committed_offset);
        ts = model::timestamp{ts.value() + 1};
    }

    auto segment = b.get_log_segments().back();
    auto result = convert_begin_offset_to_file_pos(
                    curr_offset,
                    segment,
                    segment->index().base_timestamp(),
                    ss::default_priority_class())
                    .get0();
    BOOST_REQUIRE(result.error() == std::errc::invalid_seek);
}

SEASTAR_THREAD_TEST_CASE(test_search_end_offset_not_found) {
    temporary_dir tmp_dir("offset_to_fpos_translate");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });
    b | add_segment(0);
    auto ts = model::timestamp::now();
    auto curr_offset = model::offset{0};
    for (auto i = 0; i < 2; ++i) {
        model::test::record_batch_spec spec{
          .offset = curr_offset,
          .count = 1,
          .records = 1,
          .timestamp = ts,
        };
        b.add_random_batch(spec).get();
        curr_offset = model::next_offset(
          b.get_log_segments().back()->offsets().committed_offset);
        ts = model::timestamp{ts.value() + 1};
    }

    auto segment = b.get_log_segments().back();
    auto result = convert_end_offset_to_file_pos(
                    curr_offset,
                    segment,
                    segment->index().max_timestamp(),
                    ss::default_priority_class())
                    .get0();
    BOOST_REQUIRE(result.error() == std::errc::invalid_seek);
}

SEASTAR_THREAD_TEST_CASE(test_search_begin_offset_found) {
    temporary_dir tmp_dir("offset_to_fpos_translate");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });
    b | add_segment(0);
    auto ts = model::timestamp::now();
    auto curr_offset = model::offset{0};

    std::vector<size_t> positions{};
    for (auto i = 0; i < 5; ++i) {
        model::test::record_batch_spec spec{
          .offset = curr_offset,
          .count = 1,
          .records = 10,
          .timestamp = ts,
        };
        b.add_random_batch(spec).get();
        positions.push_back(b.bytes_written());
        curr_offset = model::next_offset(
          b.get_log_segments().back()->offsets().committed_offset);
        ts = model::timestamp{ts.value() + 1};
    }

    auto segment = b.get_log_segments().back();
    auto result = convert_begin_offset_to_file_pos(
                    model::offset{3},
                    segment,
                    segment->index().base_timestamp(),
                    ss::default_priority_class())
                    .get0();
    storage::offset_to_file_pos_result expected{
      model::offset{3},
      positions[2],
      model::timestamp{segment->index().base_timestamp().value() + 3}};
    BOOST_REQUIRE(result.value() == expected);
}

SEASTAR_THREAD_TEST_CASE(test_search_end_offset_found) {
    temporary_dir tmp_dir("offset_to_fpos_translate");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });
    b | add_segment(0);
    auto ts = model::timestamp::now();
    auto curr_offset = model::offset{0};

    std::vector<size_t> positions{};
    for (auto i = 0; i < 5; ++i) {
        model::test::record_batch_spec spec{
          .offset = curr_offset,
          .count = 1,
          .records = 10,
          .timestamp = ts,
        };
        b.add_random_batch(spec).get();
        positions.push_back(b.bytes_written());
        curr_offset = model::next_offset(
          b.get_log_segments().back()->offsets().committed_offset);
        ts = model::timestamp{ts.value() + 1};
    }

    auto segment = b.get_log_segments().back();
    auto result = convert_end_offset_to_file_pos(
                    model::offset{3},
                    segment,
                    segment->index().base_timestamp(),
                    ss::default_priority_class())
                    .get0();
    storage::offset_to_file_pos_result expected{
      model::offset{3},
      positions[3], // the end byte offset of the batch containing the needle
      model::timestamp{segment->index().base_timestamp().value() + 3}};
    BOOST_REQUIRE(result.value() == expected);
}

SEASTAR_THREAD_TEST_CASE(test_search_end_offset_allowed_to_be_missing) {
    temporary_dir tmp_dir("offset_to_fpos_translate");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });
    b | add_segment(0);
    auto ts = model::timestamp::now();
    auto curr_offset = model::offset{0};
    for (auto i = 0; i < 2; ++i) {
        model::test::record_batch_spec spec{
          .offset = curr_offset,
          .count = 1,
          .records = 1,
          .timestamp = ts,
        };
        b.add_random_batch(spec).get();
        curr_offset = model::next_offset(
          b.get_log_segments().back()->offsets().committed_offset);
        ts = model::timestamp{ts.value() + 1};
    }

    auto segment = b.get_log_segments().back();
    auto result = convert_end_offset_to_file_pos(
                    curr_offset,
                    segment,
                    segment->index().max_timestamp(),
                    ss::default_priority_class(),
                    should_fail_on_missing_offset::no)
                    .get0();
    storage::offset_to_file_pos_result expected{
      model::offset{1},
      segment->size_bytes(),
      model::timestamp{segment->index().max_timestamp().value()}};
    BOOST_REQUIRE(result.value() == expected);
}

inline ss::logger test_log("test");

SEASTAR_THREAD_TEST_CASE(test_search_end_offset_inside_batch) {
    temporary_dir tmp_dir("offset_to_fpos_translate");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });
    b | add_segment(0);
    auto ts = model::timestamp::now();
    auto curr_offset = model::offset{0};
    std::vector<size_t> batch_sizes{};
    std::vector<model::timestamp> timestamps{};
    for (auto i = 0; i < 2; ++i) {
        model::test::record_batch_spec spec{
          .offset = curr_offset,
          .count = 100,
          .records = 1,
          .timestamp = ts,
        };
        b.add_random_batch(spec).get();
        batch_sizes.push_back(b.bytes_written());
        // add_random_batch uses the number of records in spec -1 to produce max
        // ts per batch.
        timestamps.emplace_back(ts.value() + 99);
        curr_offset = model::next_offset(
          b.get_log_segments().back()->offsets().committed_offset);
        ts = model::timestamp{ts.value() + 1};
    }

    auto segment = b.get_log_segments().back();

    // Search for offset 190 in batches [0-99] and [100-199]
    const auto needle = curr_offset - model::offset{10};

    // End the search before the batch containing offset.
    {
        auto result = convert_end_offset_to_file_pos(
                        needle,
                        segment,
                        segment->index().max_timestamp(),
                        ss::default_priority_class(),
                        should_fail_on_missing_offset::yes,
                        accept_batch_containing_offset::no)
                        .get0();
        // When the offset is not in batch, the timestamp of the next batch is
        // used in the result as max ts.
        const auto expected_ts = model::timestamp{timestamps[0].value() + 1};
        storage::offset_to_file_pos_result expected{
          model::offset{99}, batch_sizes[0], expected_ts};
        BOOST_REQUIRE_EQUAL(result.value().offset, expected.offset);
        BOOST_REQUIRE_EQUAL(result.value().bytes, expected.bytes);
        BOOST_REQUIRE_EQUAL(result.value().ts, expected.ts);
    }

    // End the search at the batch containing offset.
    {
        auto result = convert_end_offset_to_file_pos(
                        needle,
                        segment,
                        segment->index().max_timestamp(),
                        ss::default_priority_class(),
                        should_fail_on_missing_offset::yes,
                        accept_batch_containing_offset::yes)
                        .get0();
        // When the offset is in batch, the max ts for the batch is returned
        // with the result.
        const auto expected_ts = timestamps[1];
        storage::offset_to_file_pos_result expected{
          model::offset{199}, batch_sizes[1], expected_ts};
        BOOST_REQUIRE_EQUAL(result.value().offset, expected.offset);
        BOOST_REQUIRE_EQUAL(result.value().bytes, expected.bytes);
        BOOST_REQUIRE_EQUAL(result.value().ts, expected.ts);
    }
}
