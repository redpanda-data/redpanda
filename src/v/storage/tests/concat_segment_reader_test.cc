/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "model/tests/random_batch.h"
#include "storage/directories.h"
#include "storage/log_manager.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/tmp_dir.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

constexpr size_t segment_size{32_MiB};

using namespace storage;

size_t copy_stream(concat_segment_reader_view& cv) {
    auto concat_str = cv.take_stream();
    iobuf buffer{};
    auto ostream = make_iobuf_ref_output_stream(buffer);
    ss::copy(concat_str, ostream).get();

    // We do not actually need to close this, because the stream has been moved
    // out of this view.
    cv.close().get();

    // Closes the stream, the data source and the
    // concat_segment_data_source_impl
    concat_str.close().get();
    auto sz = buffer.size_bytes();
    ostream.close().get();
    return sz;
}

SEASTAR_THREAD_TEST_CASE(
  test_multiple_segments_read_with_content_verification) {
    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    disk_log_builder b{log_config{
      data_path.string(),
      segment_size,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()}};

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });
    size_t start_offset = 0;

    // Set up the first segment and start_pos
    // Add a sentinel batch after which view will start reading
    b | add_segment(start_offset)
      | add_random_batch(
        start_offset,
        1,
        maybe_compress_batches::yes,
        model::record_batch_type::acl_management_cmd);

    size_t start_pos = b.bytes_written();
    start_offset = b.get_segment(0).offsets().get_dirty_offset()
                   + model::offset{1};
    b
      | add_random_batch(
        start_offset,
        10,
        maybe_compress_batches::yes,
        model::record_batch_type::user_management_cmd);

    // Add intermediate segments
    start_offset = b.get_segment(0).offsets().get_dirty_offset()
                   + model::offset{1};
    for (auto i = 1; i < 4; ++i) {
        b | add_segment(start_offset)
          | add_random_batch(
            start_offset,
            10,
            maybe_compress_batches::yes,
            model::record_batch_type::user_management_cmd);
        start_offset = b.get_segment(i).offsets().get_dirty_offset()
                       + model::offset{1};
    }

    // Set up the last segment and end_pos
    b | add_segment(start_offset)
      | add_random_batch(
        start_offset,
        10,
        maybe_compress_batches::yes,
        model::record_batch_type::user_management_cmd);
    start_offset = b.get_segment(4).offsets().get_dirty_offset()
                   + model::offset{1};

    // Add a sentinel batch, view will read upto here
    auto final_segment = b.get_log_segments().back();
    size_t end_pos = final_segment->file_size();
    b
      | add_random_batch(
        start_offset,
        1,
        maybe_compress_batches::yes,
        model::record_batch_type::acl_management_cmd);

    const auto& log_segments = b.get_log_segments();
    auto segments = std::vector<ss::lw_shared_ptr<segment>>{
      log_segments.begin(), log_segments.end()};

    // All the batches in the view should have user_management_cmd as type. The
    // boundaries should exclude acl_management_cmd.
    concat_segment_reader_view cv{
      segments, start_pos, end_pos, ss::default_priority_class()};

    iobuf buf;
    auto result = transform_stream(
                    cv.take_stream(),
                    make_iobuf_ref_output_stream(buf),
                    [](model::record_batch_header h) {
                        BOOST_REQUIRE_EQUAL(
                          h.type,
                          model::record_batch_type::user_management_cmd);
                        return batch_consumer::consume_result::accept_batch;
                    })
                    .get();
    BOOST_REQUIRE(!result.has_error());
    BOOST_REQUIRE_EQUAL(
      result.value(),
      b.get_disk_log_impl().size_bytes()
        - (start_pos + final_segment->file_size() - end_pos));
}

SEASTAR_THREAD_TEST_CASE(test_single_segment_read_with_bounds) {
    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();

    using namespace storage;

    disk_log_builder b{log_config{
      data_path.string(),
      segment_size,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()}};

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}})
      | add_segment(0) | add_random_batch(0, 10);
    auto defer = ss::defer([&b] { b.stop().get(); });

    const auto& log_segments = b.get_log_segments();
    auto segments = std::vector<ss::lw_shared_ptr<segment>>{
      log_segments.begin(), log_segments.end()};

    size_t start_pos = 20;
    size_t end_pos = log_segments.back()->file_size() - 20;

    concat_segment_reader_view cv{
      segments, start_pos, end_pos, ss::default_priority_class()};
    BOOST_REQUIRE_EQUAL(
      b.get_disk_log_impl().size_bytes() - 40, copy_stream(cv));
}

SEASTAR_THREAD_TEST_CASE(test_single_segment_read_full) {
    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    disk_log_builder b{log_config{
      data_path.string(),
      segment_size,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()}};

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}})
      | add_segment(0) | add_random_batch(0, 10);
    auto defer = ss::defer([&b] { b.stop().get(); });

    const auto& log_segments = b.get_log_segments();
    auto segments = std::vector<ss::lw_shared_ptr<segment>>{
      log_segments.begin(), log_segments.end()};

    size_t start_pos = 0;
    size_t end_pos = log_segments.back()->file_size();
    concat_segment_reader_view cv{
      segments, start_pos, end_pos, ss::default_priority_class()};
    BOOST_REQUIRE_EQUAL(b.get_disk_log_impl().size_bytes(), copy_stream(cv));
}

SEASTAR_THREAD_TEST_CASE(test_multiple_segments_read_full) {
    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    disk_log_builder b{log_config{
      data_path.string(),
      segment_size,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()}};

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    size_t start_offset = 0;
    for (auto i = 0; i < 5; ++i) {
        b | add_segment(start_offset) | add_random_batch(start_offset, 10);
        start_offset = b.get_segment(i).offsets().get_dirty_offset()
                       + model::offset{1};
    }

    const auto& log_segments = b.get_log_segments();
    auto segments = std::vector<ss::lw_shared_ptr<segment>>{
      log_segments.begin(), log_segments.end()};

    size_t start_pos = 0;
    size_t end_pos = log_segments.back()->file_size();
    concat_segment_reader_view cv{
      segments, start_pos, end_pos, ss::default_priority_class()};
    BOOST_REQUIRE_EQUAL(b.get_disk_log_impl().size_bytes(), copy_stream(cv));
}
