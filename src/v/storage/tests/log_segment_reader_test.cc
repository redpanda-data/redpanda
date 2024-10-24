// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "storage/disk_log_appender.h"
#include "storage/file_sanitizer.h"
#include "storage/log_reader.h"
#include "storage/parser_utils.h"
#include "storage/record_batch_utils.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"
#include "storage/segment_reader.h"
#include "utils/disk_log_builder.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace storage; // NOLINT

#define check_batches(actual, expected)                                        \
    BOOST_REQUIRE_EQUAL_COLLECTIONS(                                           \
      actual.begin(), actual.end(), expected.begin(), expected.end());

namespace {
ss::circular_buffer<model::record_batch>
copy(ss::circular_buffer<model::record_batch>& input) {
    ss::circular_buffer<model::record_batch> ret;
    ret.reserve(input.size());
    for (auto& b : input) {
        ret.push_back(b.share());
    }
    return ret;
}

void write(
  ss::circular_buffer<model::record_batch> batches, disk_log_builder& builder) {
    auto seg = builder.get_log_segments().front().get();
    for (auto& b : batches) {
        b.header().header_crc = model::internal_header_only_crc(b.header());
        seg->append(std::move(b)).get();
    }
    seg->flush().get();
}
} // namespace

SEASTAR_THREAD_TEST_CASE(test_can_read_single_batch_smaller_offset) {
    disk_log_builder b;
    b | start() | add_segment(1);
    auto buf = model::test::make_random_batches(model::offset(1), 1).get();
    write(std::move(buf), b);
    // To-do Kostas Add support for pipe consume!
    auto res = b.consume().get();
    b | stop();
    BOOST_REQUIRE(res.empty());
}

SEASTAR_THREAD_TEST_CASE(test_can_read_single_batch_same_offset) {
    storage::log_reader_config reader_config(
      model::offset(1),
      model::offset(1),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(1);
    auto batches = model::test::make_random_batches(model::offset(1), 1).get();
    write(copy(batches), b);
    auto res = b.consume(reader_config).get();
    b | stop();
    check_batches(res, batches);
}

SEASTAR_THREAD_TEST_CASE(test_can_read_multiple_batches) {
    auto batches = model::test::make_random_batches(model::offset(1)).get();
    storage::log_reader_config reader_config(
      batches.front().base_offset(),
      batches.back().last_offset(),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get();
    b | stop();
    check_batches(res, batches);
}

SEASTAR_THREAD_TEST_CASE(test_does_not_read_past_committed_offset_one_segment) {
    auto batches = model::test::make_random_batches(model::offset(2)).get();
    storage::log_reader_config reader_config(
      batches.back().last_offset() + model::offset(1),
      batches.back().last_offset() + model::offset(1),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get();
    b | stop();
    BOOST_REQUIRE(res.empty());
}

SEASTAR_THREAD_TEST_CASE(
  test_does_not_read_past_committed_offset_multiple_segments) {
    auto batches = model::test::make_random_batches(model::offset(1), 2).get();
    storage::log_reader_config reader_config(
      batches.back().last_offset(),
      batches.back().last_offset(),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get();
    b | stop();
    ss::circular_buffer<model::record_batch> first;
    first.push_back(std::move(batches.back()));
    check_batches(res, first);
}

SEASTAR_THREAD_TEST_CASE(test_does_not_read_past_max_bytes) {
    auto batches = model::test::make_random_batches(model::offset(1), 2).get();
    storage::log_reader_config reader_config(
      batches.front().base_offset(),
      batches.front().last_offset(),
      0,
      static_cast<size_t>(batches.begin()->size_bytes()),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get();
    b | stop();
    ss::circular_buffer<model::record_batch> first;
    first.push_back(std::move(*batches.begin()));
    check_batches(res, first);
}

SEASTAR_THREAD_TEST_CASE(test_reads_at_least_one_batch) {
    auto batches = model::test::make_random_batches(model::offset(1), 2).get();
    storage::log_reader_config reader_config(
      batches.front().base_offset(),
      batches.front().last_offset(),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get();
    b | stop();
    ss::circular_buffer<model::record_batch> first;
    first.push_back(std::move(batches.front()));
    check_batches(res, first);
}

SEASTAR_THREAD_TEST_CASE(test_read_batch_range) {
    auto batches = model::test::make_random_batches(model::offset(0), 10).get();
    storage::log_reader_config reader_config(
      batches.front().base_offset(),
      batches.back().last_offset(),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start();
    b | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get();
    b | stop();
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      std::next(res.begin(), 2),
      std::next(res.begin(), 7),
      std::next(batches.begin(), 2),
      std::next(batches.begin(), 7));
}

SEASTAR_THREAD_TEST_CASE(test_batch_type_filter) {
    auto batches = model::test::make_random_batches(model::offset(0), 5).get();
    for (auto i = 0u; i < batches.size(); i++) {
        batches[i].header().type = model::record_batch_type(i);
    }

    storage::log_reader_config reader_config(
      batches.front().base_offset(),
      batches.back().last_offset(),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);

    // read and extract types with optional type filter
    auto read_types =
      [&b, &batches](std::optional<int> type_wanted) -> std::vector<int> {
        std::optional<model::record_batch_type> type_filter;
        if (type_wanted) {
            type_filter = model::record_batch_type(type_wanted.value());
        }

        auto config = log_reader_config(
          batches.front().base_offset(),
          batches.back().last_offset(),
          0,
          std::numeric_limits<size_t>::max(),
          ss::default_priority_class(),
          type_filter,
          std::nullopt,
          std::nullopt);

        auto res = b.consume(config).get();

        std::set<int> types;
        for (auto& batch : res) {
            types.insert(static_cast<int>(batch.header().type));
        }
        return {types.begin(), types.end()};
    };

    std::vector<int> types = read_types({});
    BOOST_CHECK_EQUAL(types, std::vector<int>({0, 1, 2, 3, 4}));

    types = read_types(1);
    BOOST_TEST(types == std::vector<int>({1}));

    types = read_types(0);
    BOOST_TEST(types == std::vector<int>({0}));

    types = read_types(2);
    BOOST_TEST(types == std::vector<int>({2}));

    types = read_types(4);
    BOOST_TEST(types == std::vector<int>({4}));

    b | stop();
}

SEASTAR_THREAD_TEST_CASE(test_does_not_read_past_max_offset) {
    auto batches = model::test::make_random_batches(model::offset(1), 3).get();
    storage::log_reader_config reader_config(
      batches.front().base_offset(),
      batches.back().last_offset(),
      0,
      std::numeric_limits<size_t>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get();
    b | stop();
    check_batches(res, batches);
}

SEASTAR_THREAD_TEST_CASE(iobuf_is_zero_test) {
    const auto a = random_generators::gen_alphanum_string(1024);
    const auto b = bytes::from_string("abc");
    std::array<char, 1024> zeros{0};
    std::array<char, 1> one{1};

    // non zero iobuf
    iobuf non_zero_1;
    non_zero_1.append(a.data(), a.size());
    non_zero_1.append(b.data(), b.size());
    BOOST_REQUIRE_EQUAL(storage::internal::is_zero(non_zero_1), false);

    iobuf non_zero_2;
    non_zero_2.append(zeros.data(), zeros.size());
    non_zero_2.append(one.data(), one.size());
    BOOST_REQUIRE_EQUAL(storage::internal::is_zero(non_zero_2), false);
    // empty iobuf is not zero
    iobuf empty;
    BOOST_REQUIRE_EQUAL(storage::internal::is_zero(empty), false);

    iobuf zero;
    zero.append(zeros.data(), zeros.size());
    zero.append(zeros.data(), zeros.size());
    BOOST_REQUIRE_EQUAL(storage::internal::is_zero(zero), true);
}
