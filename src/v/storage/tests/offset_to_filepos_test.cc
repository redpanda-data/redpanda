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
    temporary_dir tmp_dir("concat_segment_read");
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
    BOOST_REQUIRE_EQUAL(
      result.error(), convert_to_file_pos_outcome::offset_not_in_segment);
}

SEASTAR_THREAD_TEST_CASE(test_search_end_offset_not_found) {
    temporary_dir tmp_dir("concat_segment_read");
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
    BOOST_REQUIRE_EQUAL(
      result.error(), convert_to_file_pos_outcome::offset_not_in_segment);
}
