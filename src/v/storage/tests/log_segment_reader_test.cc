// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/chunked_circular_buffer.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "random/generators.h"
#include "storage/log_reader.h"
#include "storage/parser_utils.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/test_macros.h"

#include <gtest/gtest.h>

using namespace storage; // NOLINT

template<typename l_iter_t, typename r_iter_t>
void check_iters(
  l_iter_t l_iter, l_iter_t l_end, r_iter_t r_iter, r_iter_t r_end) {
    bool l_done = false;
    bool r_done = false;
    while (true) {
        l_done = l_iter == l_end;
        r_done = r_iter == r_end;
        if (l_done || r_done) {
            break;
        }
        RPTEST_REQUIRE_EQ(*r_iter, *l_iter);
        ++r_iter;
        ++l_iter;
    }
    RPTEST_REQUIRE_EQ(l_done, r_done);
}

void check_batches(
  const chunked_circular_buffer<model::record_batch>& actual,
  const chunked_circular_buffer<model::record_batch>& expected) {
    RPTEST_REQUIRE_EQ(actual.size(), expected.size());
    check_iters(actual.begin(), actual.end(), expected.begin(), expected.end());
}

namespace {
chunked_circular_buffer<model::record_batch>
copy(chunked_circular_buffer<model::record_batch>& input) {
    chunked_circular_buffer<model::record_batch> ret;
    for (auto& b : input) {
        ret.push_back(b.share());
    }
    return ret;
}

void write(
  chunked_circular_buffer<model::record_batch> batches,
  disk_log_builder& builder) {
    auto seg = builder.get_log_segments().front().get();
    for (auto& b : batches) {
        b.header().header_crc = model::internal_header_only_crc(b.header());
        seg->append(std::move(b)).get();
    }
    seg->flush().get();
}
} // namespace

TEST(reader_test, test_can_read_single_batch_smaller_offset) {
    disk_log_builder b;
    b | start() | add_segment(1);
    auto buf = model::test::make_random_batches(model::offset(1), 1).get();
    write(std::move(buf), b);
    // To-do Kostas Add support for pipe consume!
    auto res = b.consume().get();
    b | stop();
    RPTEST_REQUIRE(res.empty());
}

TEST(reader_test, test_can_read_single_batch_same_offset) {
    storage::local_log_reader_config reader_config(
      model::offset(1),
      model::offset(1),
      model::model_limits<model::offset>::max(),
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

TEST(reader_test, test_can_read_multiple_batches) {
    auto batches = model::test::make_random_batches(model::offset(1)).get();
    storage::local_log_reader_config reader_config(
      batches.front().base_offset(),
      batches.back().last_offset(),
      model::model_limits<model::offset>::max(),
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

TEST(reader_test, test_does_not_read_past_committed_offset_one_segment) {
    auto batches = model::test::make_random_batches(model::offset(2)).get();
    storage::local_log_reader_config reader_config(
      batches.back().last_offset() + model::offset(1),
      batches.back().last_offset() + model::offset(1),
      model::model_limits<model::offset>::max(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get();
    b | stop();
    RPTEST_REQUIRE(res.empty());
}

TEST(reader_test, test_does_not_read_past_committed_offset_multiple_segments) {
    auto batches = model::test::make_random_batches(model::offset(1), 2).get();
    storage::local_log_reader_config reader_config(
      batches.back().last_offset(),
      batches.back().last_offset(),
      model::model_limits<model::offset>::max(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get();
    b | stop();
    chunked_circular_buffer<model::record_batch> first;
    first.push_back(std::move(batches.back()));
    check_batches(res, first);
}

TEST(reader_test, test_does_not_read_past_max_bytes) {
    auto batches = model::test::make_random_batches(model::offset(1), 2).get();
    storage::local_log_reader_config reader_config(
      batches.front().base_offset(),
      batches.front().last_offset(),
      static_cast<size_t>(batches.begin()->size_bytes()),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get();
    b | stop();
    chunked_circular_buffer<model::record_batch> first;
    first.push_back(std::move(*batches.begin()));
    check_batches(res, first);
}

TEST(reader_test, test_reads_at_least_one_batch) {
    auto batches = model::test::make_random_batches(model::offset(1), 2).get();
    storage::local_log_reader_config reader_config(
      batches.front().base_offset(),
      batches.front().last_offset(),
      model::model_limits<model::offset>::max(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get();
    b | stop();
    chunked_circular_buffer<model::record_batch> first;
    first.push_back(std::move(batches.front()));
    check_batches(res, first);
}

TEST(reader_test, test_read_batch_range) {
    auto batches = model::test::make_random_batches(model::offset(0), 10).get();
    storage::local_log_reader_config reader_config(
      batches.front().base_offset(),
      batches.back().last_offset(),
      model::model_limits<model::offset>::max(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start();
    b | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get();
    b | stop();
    check_iters(
      std::next(res.begin(), 2),
      std::next(res.begin(), 7),
      std::next(batches.begin(), 2),
      std::next(batches.begin(), 7));
}

TEST(reader_test, test_batch_type_filter) {
    auto batches = model::test::make_random_batches(model::offset(0), 5).get();
    for (auto i = 0u; i < batches.size(); i++) {
        batches[i].header().type = model::record_batch_type(i);
    }

    storage::local_log_reader_config reader_config(
      batches.front().base_offset(),
      batches.back().last_offset(),
      model::model_limits<model::offset>::max(),
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

        auto config = local_log_reader_config(
          batches.front().base_offset(),
          batches.back().last_offset(),
          std::numeric_limits<size_t>::max(),
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
    RPTEST_EXPECT_EQ(types, std::vector<int>({0, 1, 2, 3, 4}));

    types = read_types(1);
    RPTEST_REQUIRE(types == std::vector<int>({1}));

    types = read_types(0);
    RPTEST_REQUIRE(types == std::vector<int>({0}));

    types = read_types(2);
    RPTEST_REQUIRE(types == std::vector<int>({2}));

    types = read_types(4);
    RPTEST_REQUIRE(types == std::vector<int>({4}));

    b | stop();
}

TEST(reader_test, test_does_not_read_past_max_offset) {
    auto batches = model::test::make_random_batches(model::offset(1), 3).get();
    storage::local_log_reader_config reader_config(
      batches.front().base_offset(),
      batches.back().last_offset(),
      std::numeric_limits<size_t>::max(),
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

TEST(reader_test, iobuf_is_zero_test) {
    const auto a = random_generators::gen_alphanum_string(1024);
    const auto b = bytes::from_string("abc");
    std::array<char, 1024> zeros{0};
    std::array<char, 1> one{1};

    // non zero iobuf
    iobuf non_zero_1;
    non_zero_1.append(a.data(), a.size());
    non_zero_1.append(b.data(), b.size());
    RPTEST_REQUIRE_EQ(storage::internal::is_zero(non_zero_1), false);

    iobuf non_zero_2;
    non_zero_2.append(zeros.data(), zeros.size());
    non_zero_2.append(one.data(), one.size());
    RPTEST_REQUIRE_EQ(storage::internal::is_zero(non_zero_2), false);
    // empty iobuf is not zero
    iobuf empty;
    RPTEST_REQUIRE_EQ(storage::internal::is_zero(empty), false);

    iobuf zero;
    zero.append(zeros.data(), zeros.size());
    zero.append(zeros.data(), zeros.size());
    RPTEST_REQUIRE_EQ(storage::internal::is_zero(zero), true);
}

TEST(reader_test, test_ghosts_gap) {
    long twice_i32max = static_cast<long>(std::numeric_limits<int32_t>::max())
                        * 2;
    auto ghost_batches = log_reader::make_ghost_batches(
      model::offset{0}, model::offset{twice_i32max}, model::term_id{0});
    RPTEST_REQUIRE_EQ(3, ghost_batches.size());
    size_t num_records = 0;
    for (const auto& b : ghost_batches) {
        ASSERT_GT(b.record_count(), 0);
        num_records += b.record_count();
    }
    RPTEST_REQUIRE_EQ(twice_i32max + 1, num_records);
}

TEST(reader_test, test_ghost_read_with_index_overflow) {
    auto cfg = log_builder_config();
    cfg.cache = with_cache::no;
    disk_log_builder b(cfg);
    b | start() | add_segment(model::offset{0});
    auto s = b.get_log_segments().back();

    // Set a pathologically low step size so we'll add every batch into the
    // index.
    s->index().set_step_for_tests(1);
    chunked_circular_buffer<model::record_batch> batches;
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
    storage::local_log_reader_config reader_config(
      model::offset{100},
      model::offset::max(),
      model::model_limits<model::offset>::max(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    reader_config.fill_gaps = true;
    auto res = b.consume(reader_config).get();
    b | stop();
    RPTEST_REQUIRE(!res.empty());
    auto& first = res.front();
    RPTEST_EXPECT_EQ(first.base_offset(), model::offset{100});
    RPTEST_EXPECT_EQ(first.last_offset(), model::offset{100});
    RPTEST_EXPECT_EQ(first.header().type, model::record_batch_type::raft_data);
}

TEST(reader_test, test_read_with_index_overflow_base) {
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
    chunked_circular_buffer<model::record_batch> batches;
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
    storage::local_log_reader_config reader_config(
      s->offsets().get_base_offset(),
      model::offset::max(),
      model::model_limits<model::offset>::max(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    auto res = b.consume(reader_config).get();
    b | stop();
    RPTEST_REQUIRE(!res.empty());
    auto& first = res.front();
    RPTEST_EXPECT_EQ(first.base_offset(), model::offset{uint32_max});
    RPTEST_EXPECT_EQ(first.last_offset(), model::offset{uint32_max});
    RPTEST_EXPECT_EQ(first.header().type, model::record_batch_type::raft_data);
}
