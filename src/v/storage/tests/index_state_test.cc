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

#include "bytes/bytes.h"
#include "random/generators.h"
#include "serde/rw/envelope.h"
#include "storage/index_state.h"
#include "test_utils/gtest_exception.h"

#include <gtest/gtest.h>

using namespace redpanda::test_utils;

static storage::index_state make_random_index_state(
  storage::offset_delta_time apply_offset = storage::offset_delta_time::yes) {
    auto base_offset = model::offset(random_generators::get_int<int64_t>());
    auto st = storage::index_state::make_empty_index(base_offset, apply_offset);
    st.bitflags = random_generators::get_int<uint32_t>();
    st.max_offset = model::offset(random_generators::get_int<int64_t>());
    st.base_timestamp = model::timestamp(random_generators::get_int<int64_t>());
    st.max_timestamp = model::timestamp(random_generators::get_int<int64_t>());
    st.batch_timestamps_are_monotonic = apply_offset
                                        == storage::offset_delta_time::yes;

    if (apply_offset == storage::offset_delta_time::yes) {
        // set new field if the request is for a current-version index
        if (random_generators::get_int(0, 1) == 1) {
            st.broker_timestamp = model::timestamp(
              random_generators::get_int<int64_t>());
        }
    }

    const auto n = random_generators::get_int(1, 10000);
    for (auto i = 0; i < n; ++i) {
        st.add_entry(
          random_generators::get_int<uint32_t>(),
          storage::offset_time_index{
            model::timestamp{random_generators::get_int<int64_t>()},
            apply_offset},
          random_generators::get_int<uint64_t>());
    }

    if (apply_offset == storage::offset_delta_time::no) {
        chunked_vector<uint32_t> time_index;
        for (auto i = 0; i < n; ++i) {
            time_index.push_back(random_generators::get_int<uint32_t>());
        }

        st.index.assign_relative_time_index(std::move(time_index));
    }

    return st;
}

static void set_version(iobuf& buf, int8_t version) {
    auto tmp = iobuf_to_bytes(buf);
    buf.clear();
    buf.append((const char*)&version, sizeof(version));
    vassert(tmp.size() > 1, "unexpected buffer size: {}", tmp.size());
    buf.append(tmp.data() + 1, tmp.size() - 1);
}

// encode/decode using new serde framework
TEST(IndexState, SerdeBasic) {
    for (int i = 0; i < 100; ++i) {
        auto input = make_random_index_state();
        const auto input_copy = input.copy();
        ASSERT_EQ(input, input_copy);

        // objects are equal
        const auto buf = serde::to_iobuf(std::move(input));
        auto output = serde::from_iobuf<storage::index_state>(buf.copy());

        ASSERT_TRUE(output.batch_timestamps_are_monotonic);
        ASSERT_EQ(output.with_offset, storage::offset_delta_time::yes);

        ASSERT_EQ(output, input_copy);

        // round trip back to equal iobufs
        const auto buf2 = serde::to_iobuf(std::move(output));
        ASSERT_EQ(buf, buf2);
    }
}

TEST(IndexState, SerdeNoTimeOffsetingForExistingIndices) {
    for (int i = 0; i < 100; ++i) {
        // Create index without time offsetting
        auto input = make_random_index_state(storage::offset_delta_time::no);
        const auto input_copy = input.copy();
        auto buf = serde::to_iobuf(std::move(input));
        set_version(buf, 4);

        // Read the index and check that time offsetting was not applied
        auto output = serde::from_iobuf<storage::index_state>(buf.copy());

        ASSERT_FALSE(output.batch_timestamps_are_monotonic);
        ASSERT_EQ(output.with_offset, storage::offset_delta_time::no);

        auto output_copy = output.copy();

        ASSERT_EQ(input_copy, output);

        // Re-encode with version 5 and verify that there is still no offsetting
        const auto buf2 = serde::to_iobuf(std::move(output));
        auto output2 = serde::from_iobuf<storage::index_state>(buf2.copy());
        ASSERT_EQ(output_copy, output2);

        ASSERT_FALSE(output2.batch_timestamps_are_monotonic);
        ASSERT_EQ(output2.with_offset, storage::offset_delta_time::no);
    }
}

// accept decoding supported old version
TEST(IndexState, SerdeSupportedDeprecated) {
    for (int i = 0; i < 100; ++i) {
        auto input = make_random_index_state(storage::offset_delta_time::no);
        const auto output = serde::from_iobuf<storage::index_state>(
          storage::serde_compat::index_state_serde::encode(input));

        ASSERT_FALSE(output.batch_timestamps_are_monotonic);
        ASSERT_EQ(output.with_offset, storage::offset_delta_time::no);

        ASSERT_EQ(input, output);
    }
}

// reject decoding unsupported old versions
TEST(IndexState, SerdeUnsupportedDeprecated) {
    auto test = [](int version) {
        auto input = make_random_index_state(storage::offset_delta_time::no);
        auto buf = storage::serde_compat::index_state_serde::encode(input);
        set_version(buf, version);

        ASSERT_THROWS_WITH_PREDICATE(
          const auto output = serde::from_iobuf<storage::index_state>(
            buf.copy()),
          serde::serde_exception,
          [version](const serde::serde_exception& e) {
              auto s = fmt::format("Unsupported version: {}", version);
              return std::string_view(e.what()).find(s)
                     != std::string_view::npos;
          });
    };
    test(0);
    test(1);
    test(2);
}

// decoding should fail if all the data isn't available
TEST(IndexState, SerdeClipped) {
    auto input = make_random_index_state(storage::offset_delta_time::no);
    auto buf = serde::to_iobuf(std::move(input));

    // trim off some data from the end
    ASSERT_GT(buf.size_bytes(), 10);
    buf.trim_back(buf.size_bytes() - 10);

    ASSERT_THROWS_WITH_PREDICATE(
      serde::from_iobuf<storage::index_state>(buf.copy()),
      serde::serde_exception,
      [](const serde::serde_exception& e) {
          return std::string_view(e.what()).find("bytes_left")
                 != std::string_view::npos;
      });
}

// decoding deprecated format should fail if not all data is available
TEST(IndexState, SerdeDeprecatedClipped) {
    auto input = make_random_index_state(storage::offset_delta_time::no);
    auto buf = storage::serde_compat::index_state_serde::encode(input);

    // trim off some data from the end
    ASSERT_GT(buf.size_bytes(), 10);
    buf.trim_back(buf.size_bytes() - 10);

    ASSERT_THROWS_WITH_PREDICATE(
      serde::from_iobuf<storage::index_state>(buf.copy()),
      serde::serde_exception,
      [](const serde::serde_exception& e) {
          return std::string_view(e.what()).find(
                   "Index size does not match header size")
                 != std::string_view::npos;
      });
}

TEST(IndexState, SerdeCrc) {
    auto input = make_random_index_state();
    auto good_buf = serde::to_iobuf(std::move(input));

    auto bad_bytes = iobuf_to_bytes(good_buf);
    auto& bad_byte = bad_bytes[bad_bytes.size() / 2];
    bad_byte += 1;
    auto bad_buf = bytes_to_iobuf(bad_bytes);

    ASSERT_THROWS_WITH_PREDICATE(
      serde::from_iobuf<storage::index_state>(bad_buf.copy()),
      serde::serde_exception,
      [](const serde::serde_exception& e) {
          return std::string_view(e.what()).find("Mismatched checksum")
                 != std::string_view::npos;
      });
}

TEST(IndexState, SerdeDeprecatedCrc) {
    auto input = make_random_index_state(storage::offset_delta_time::no);
    auto good_buf = storage::serde_compat::index_state_serde::encode(input);

    auto bad_bytes = iobuf_to_bytes(good_buf);
    auto& bad_byte = bad_bytes[bad_bytes.size() / 2];
    bad_byte += 1;
    auto bad_buf = bytes_to_iobuf(bad_bytes);

    ASSERT_THROWS_WITH_PREDICATE(
      serde::from_iobuf<storage::index_state>(bad_buf.copy()),
      std::exception,
      [](const std::exception& e) {
          auto msg = std::string_view(e.what());
          auto is_crc = msg.find("Invalid checksum for index")
                        != std::string_view::npos;
          auto is_out_of_bounds = msg.find("Invalid consume_to")
                                  != std::string_view::npos;
          return is_crc || is_out_of_bounds;
      });
}

TEST(IndexState, OffsetTimeIndexTest) {
    // Before offsetting: [0, ..., 2 ^ 31 - 1, ..., 2 ^ 32 - 1]
    //                    |             |              |
    //                    |             |______________|
    //                    |             |
    // After offsetting:  [2^31, ..., 2 ^ 32 - 1]

    const uint32_t max_delta = storage::offset_time_index::delta_time_max;

    std::vector<uint32_t> deltas_before{
      0, 1, max_delta - 1, max_delta, std::numeric_limits<uint32_t>::max()};
    for (uint32_t delta_before : deltas_before) {
        auto offset_delta = storage::offset_time_index{
          model::timestamp{delta_before}, storage::offset_delta_time::yes};
        auto non_offset_delta = storage::offset_time_index{
          model::timestamp{delta_before}, storage::offset_delta_time::no};

        ASSERT_EQ(non_offset_delta(), delta_before);

        if (delta_before >= max_delta) {
            ASSERT_EQ(offset_delta(), max_delta);
        } else {
            ASSERT_EQ(offset_delta(), delta_before);
        }
    }
}

TEST(IndexState, IndexOverflow) {
    storage::index_state state;

    // Previous versions of Redpanda can have an index with offsets spanning
    // greater than uint32 offset space. In these cases, the index is not
    // reliable and querying the index should just return the first entry.
    const model::timestamp dummy_ts;
    const storage::offset_delta_time should_offset{false};
    storage::offset_time_index time_idx{dummy_ts, should_offset};
    constexpr long uint32_max = std::numeric_limits<uint32_t>::max();
    state.add_entry(0, time_idx, 1);
    state.add_entry(100, time_idx, 2);
    state.add_entry(static_cast<uint32_t>(uint32_max + 1), time_idx, 3);
    state.add_entry(static_cast<uint32_t>(uint32_max + 10), time_idx, 4);
    state.max_offset = model::offset{uint32_max + 10};
    auto res = state.find_nearest(model::offset(100));
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->offset, model::offset{0});
    EXPECT_EQ(res->filepos, 1);
}

TEST(IndexState, NonDataTimestampsWithOverflow) {
    storage::index_state state;

    // Need to ensure that non_data_timestamps in the segment_index is only set
    // iff there is an entry in the index_state. Otherwise, a call to
    // index.try_reset_relative_time_index() will trigger a vassert.
    constexpr long uint32_max = std::numeric_limits<uint32_t>::max();
    state.maybe_index(
      0,
      1,
      0,
      model::offset{uint32_max + 1},
      model::offset{uint32_max + 1},
      model::timestamp{0},
      model::timestamp{0},
      std::nullopt,
      false,
      0);
    state.maybe_index(
      1,
      1,
      1,
      model::offset{uint32_max + 2},
      model::offset{uint32_max + 2},
      model::timestamp{0},
      model::timestamp{0},
      std::nullopt,
      true,
      0);
}

TEST(IndexState, IndexOverflowTruncate) {
    storage::index_state state;

    // Previous versions of Redpanda can have an index with offsets spanning
    // greater than uint32 offset space. In these cases, the index is not
    // reliable and truncation shouldn't attempt to truncate entries based on
    // an overflown lookup. But, queries should fallback to returning the
    // beginning of the index.
    const model::timestamp dummy_ts;
    const storage::offset_delta_time should_offset{false};
    storage::offset_time_index time_idx{dummy_ts, should_offset};
    constexpr long uint32_max = std::numeric_limits<uint32_t>::max();
    state.add_entry(0, time_idx, 1);
    state.add_entry(100, time_idx, 2);
    state.add_entry(static_cast<uint32_t>(uint32_max + 1), time_idx, 3);
    state.add_entry(static_cast<uint32_t>(uint32_max + 10), time_idx, 4);
    state.max_offset = model::offset{uint32_max + 10};
    EXPECT_EQ(4, state.size());

    // The truncation shouldn't remove index entries, given the overflow.
    auto needs_flush = state.truncate(
      model::offset(uint32_max + 1), model::timestamp::now());
    EXPECT_TRUE(needs_flush);
    EXPECT_EQ(4, state.size());
    EXPECT_EQ(state.max_offset, model::offset{uint32_max + 1});

    // Queries for the offset should start from the beginning of the segment.
    auto res = state.find_nearest(model::offset(uint32_max + 1));
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->offset, model::offset{0});
    EXPECT_EQ(res->filepos, 1);
}
