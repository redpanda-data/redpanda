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

SEASTAR_THREAD_TEST_CASE(test_ghosts_gap) {
    long twice_i32max = static_cast<long>(std::numeric_limits<int32_t>::max())
                        * 2;
    auto ghost_batches = log_reader::make_ghost_batches(
      model::offset{0}, model::offset{twice_i32max}, model::term_id{0});
    BOOST_REQUIRE_EQUAL(3, ghost_batches.size());
    size_t num_records = 0;
    for (const auto& b : ghost_batches) {
        BOOST_REQUIRE_GT(b.record_count(), 0);
        num_records += b.record_count();
    }
    BOOST_REQUIRE_EQUAL(twice_i32max + 1, num_records);
}

SEASTAR_THREAD_TEST_CASE(test_ghost_read_with_index_overflow) {
    auto cfg = log_builder_config();
    cfg.cache = with_cache::no;
    disk_log_builder b(cfg);
    b | start() | add_segment(model::offset{0});
    auto s = b.get_log_segments().back();

    // Set a pathologically low step size so we'll add every batch into the
    // index.
    s->index().set_step_for_tests(1);
    ss::circular_buffer<model::record_batch> batches;
    auto add = [&batches](model::offset o) {
        auto b = model::test::make_random_batches(
                   o, /*count=*/1, false, std::nullopt, /*records_per_batch=*/1)
                   .get();
        batches.push_back(std::move(b.front()));
    };
    constexpr long uint32_max = std::numeric_limits<uint32_t>::max();
    add(model::offset(0));
    add(model::offset(100));
    add(model::offset(uint32_max + 1));
    add(model::offset(uint32_max + 100));
    add(model::offset(uint32_max + 200));
    write(copy(batches), b);

    // Regression test: a bug previously meant that we would seek to an
    // incorrect offset and return the wrong batch.
    storage::log_reader_config reader_config(
      model::offset{100},
      model::offset::max(),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    reader_config.fill_gaps = true;
    auto res = b.consume(reader_config).get();
    b | stop();
    BOOST_REQUIRE(!res.empty());
    auto& first = res.front();
    BOOST_CHECK_EQUAL(first.base_offset(), model::offset{100});
    BOOST_CHECK_EQUAL(first.last_offset(), model::offset{100});
    BOOST_CHECK_EQUAL(first.header().type, model::record_batch_type::raft_data);
}

SEASTAR_THREAD_TEST_CASE(test_read_with_index_overflow_base) {
    auto cfg = log_builder_config();
    cfg.cache = with_cache::no;
    disk_log_builder b(cfg);
    constexpr long uint32_max = std::numeric_limits<uint32_t>::max();
    b | start() | add_segment(model::offset{uint32_max});
    auto s = b.get_log_segments().back();

    // Set a pathologically low step size so we'll add every batch into the
    // index.
    s->index().set_step_for_tests(1);
    // Reset the index base offset to 0 before adding any entries, simulating a
    // bug in Redpanda where compaction would start indexes off with base
    // offset 0. This is important to have this test reproduce a bad seek to
    // the start of the segment.
    s->index().set_base_offset_for_tests(model::offset(0));
    ss::circular_buffer<model::record_batch> batches;
    auto add = [&batches](model::offset o) {
        auto b = model::test::make_random_batches(
                   o, /*count=*/1, false, std::nullopt, /*records_per_batch=*/1)
                   .get();
        batches.push_back(std::move(b.front()));
    };
    add(model::offset(uint32_max));
    add(model::offset(uint32_max + 100));
    add(model::offset(2 * uint32_max + 1));
    add(model::offset(2 * uint32_max + 100));
    add(model::offset(2 * uint32_max + 200));
    write(copy(batches), b);

    // Regression test: a bug previously meant that we would seek to an
    // incorrect offset and return the wrong batch even when seeking at the
    // start of the segment.
    storage::log_reader_config reader_config(
      s->offsets().get_base_offset(),
      model::offset::max(),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    auto res = b.consume(reader_config).get();
    b | stop();
    BOOST_REQUIRE(!res.empty());
    auto& first = res.front();
    BOOST_CHECK_EQUAL(first.base_offset(), model::offset{uint32_max});
    BOOST_CHECK_EQUAL(first.last_offset(), model::offset{uint32_max});
    BOOST_CHECK_EQUAL(first.header().type, model::record_batch_type::raft_data);
}
