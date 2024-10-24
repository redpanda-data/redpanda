// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "storage/log_reader.h"
#include "storage/mvlog/batch_collector.h"
#include "storage/mvlog/reader_outcome.h"
#include "storage/types.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/io_priority_class.hh>

#include <gtest/gtest.h>

using namespace storage::experimental::mvlog;

namespace {
model::record_batch_header make_batch_header(
  model::offset start_offset,
  model::offset last_offset,
  model::record_batch_type type = model::record_batch_type::raft_data,
  int32_t body_size = 0,
  model::timestamp first_timestamp = model::timestamp{},
  model::timestamp last_timestamp = model::timestamp{}) {
    // NOTE: explicitly not setting initializing the term.
    return model::record_batch_header{
      .size_bytes = static_cast<int32_t>(
        body_size + model::packed_record_batch_header_size),
      .base_offset = start_offset,
      .type = type,
      .crc = 0,
      .attrs = model::record_batch_attributes(0),
      .last_offset_delta = static_cast<int32_t>(last_offset() - start_offset()),
      .first_timestamp = first_timestamp,
      .max_timestamp = last_timestamp,
      .producer_id = -1,
      .producer_epoch = -1,
      .base_sequence = 0,
      .record_count = 0};
}
} // anonymous namespace

TEST(BatchCollectorTest, TestDataDecreasesOffset) {
    storage::log_reader_config cfg(
      model::offset{0}, model::offset::max(), ss::default_priority_class());
    batch_collector collector(cfg, model::term_id{0});
    auto res = collector.add_batch(
      make_batch_header(model::offset{0}, model::offset{10}), iobuf{});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), reader_outcome::success);

    // Attempting to collect something that has already been collecting will
    // result in an error.
    for (int i = 0; i <= 10; i++) {
        auto res = collector.add_batch(
          make_batch_header(model::offset{i}, model::offset{10}), iobuf{});
        ASSERT_FALSE(res.has_value());
        ASSERT_EQ(res.error(), errc::broken_data_invariant);
    }
    // Even if the next offset has uncollected data, it is unexpected for the
    // base offset to ever go down.
    res = collector.add_batch(
      make_batch_header(model::offset{10}, model::offset{11}), iobuf{});
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), errc::broken_data_invariant);

    // Net new data should succeed.
    res = collector.add_batch(
      make_batch_header(model::offset{11}, model::offset{11}), iobuf{});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), reader_outcome::success);
}

TEST(BatchCollectorTest, TestInvariantsAfterRelease) {
    storage::log_reader_config cfg(
      model::offset{0}, model::offset::max(), ss::default_priority_class());
    batch_collector collector(cfg, model::term_id{1});
    auto res = collector.add_batch(
      make_batch_header(model::offset{0}, model::offset{10}), iobuf{});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), reader_outcome::success);

    auto batches = collector.release_batches();
    ASSERT_EQ(1, batches.size());

    // Even though we've released the data, we should continue to enforce
    // invariants for prior data.
    res = collector.add_batch(
      make_batch_header(model::offset{0}, model::offset{10}), iobuf{});
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), errc::broken_data_invariant);

    res = collector.set_term(model::term_id{0});
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), errc::broken_data_invariant);
}

TEST(BatchCollectorTest, TestDataTooLow) {
    for (int i = 0; i <= 10; i++) {
        storage::log_reader_config cfg(
          model::offset{11},
          model::offset::max(),
          ss::default_priority_class());
        batch_collector collector(cfg, model::term_id{0});

        // All the data is below the reader start offset, so we should skip.
        auto res = collector.add_batch(
          make_batch_header(model::offset{0}, model::offset{i}), iobuf{});
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(res.value(), reader_outcome::skip);

        // But batches including the desired offset should collect, even if it
        // also includes records below the start.
        res = collector.add_batch(
          make_batch_header(model::offset{i + 1}, model::offset{11}), iobuf{});
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(res.value(), reader_outcome::success);
    }
}

TEST(BatchCollectorTest, TestDataTooHigh) {
    storage::log_reader_config cfg(
      model::offset{0}, model::offset{9}, ss::default_priority_class());
    {
        batch_collector collector(cfg, model::term_id{0});
        // We should be able to collect right at the upper edge of the range.
        auto res = collector.add_batch(
          make_batch_header(model::offset{9}, model::offset{10}), iobuf{});
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(res.value(), reader_outcome::success);
    }
    {
        batch_collector collector(cfg, model::term_id{0});
        // Attempting to collect past the end of the range will fail.
        auto res = collector.add_batch(
          make_batch_header(model::offset{10}, model::offset{10}), iobuf{});
        ASSERT_TRUE(res.has_value()) << res.error();
        ASSERT_EQ(res.value(), reader_outcome::stop);
    }
}

TEST(BatchCollectorTest, TestTypeFilter) {
    storage::log_reader_config cfg(
      model::offset{0}, model::offset{11}, ss::default_priority_class());
    cfg.type_filter = model::record_batch_type::raft_configuration;
    batch_collector collector(cfg, model::term_id{0});
    auto res = collector.add_batch(
      make_batch_header(
        model::offset{10},
        model::offset{10},
        model::record_batch_type::raft_data),
      iobuf{});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), reader_outcome::skip);

    // Skipping a batch shouldn't affect our ability to detect invariant
    // violations.
    res = collector.add_batch(
      make_batch_header(
        model::offset{10},
        model::offset{10},
        model::record_batch_type::raft_configuration),
      iobuf{});
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), errc::broken_data_invariant);

    // Try again but with the appropraite filter and an appropriate range.
    res = collector.add_batch(
      make_batch_header(
        model::offset{11},
        model::offset{11},
        model::record_batch_type::raft_configuration),
      iobuf{});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), reader_outcome::success);
}

TEST(BatchCollectorTest, TestSetTerm) {
    storage::log_reader_config cfg(
      model::offset{0}, model::offset::max(), ss::default_priority_class());
    batch_collector collector(cfg, model::term_id{1});
    // Moving the term backwards should fail.
    {
        auto res = collector.set_term(model::term_id{0});
        ASSERT_TRUE(res.has_error());
        ASSERT_EQ(res.error(), errc::broken_data_invariant);
    }
    // Moving the term forwards or to the same term should be fine.
    for (int i = 1; i <= 2; i++) {
        auto res = collector.set_term(model::term_id{i});
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(res.value(), reader_outcome::success);
    }

    // When we collect record batches, they should be assigned terms.
    auto res = collector.add_batch(
      make_batch_header(
        model::offset{0},
        model::offset{0},
        model::record_batch_type::raft_configuration),
      iobuf{});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), reader_outcome::success);
    auto batches = collector.release_batches();
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].term(), model::term_id{2});

    // Also check that the term is retained for new batches added to the
    // collector.
    res = collector.add_batch(
      make_batch_header(
        model::offset{1},
        model::offset{1},
        model::record_batch_type::raft_configuration),
      iobuf{});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), reader_outcome::success);

    // And that setting the term after adding some batches will only set the
    // term for new batches as appropriate.
    res = collector.set_term(model::term_id{3});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), reader_outcome::success);

    res = collector.add_batch(
      make_batch_header(
        model::offset{2},
        model::offset{2},
        model::record_batch_type::raft_configuration),
      iobuf{});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), reader_outcome::success);

    batches = collector.release_batches();
    ASSERT_EQ(batches.size(), 2);
    ASSERT_EQ(batches[0].term(), model::term_id{2});
    ASSERT_EQ(batches[1].term(), model::term_id{3});
}

TEST(BatchCollectorTest, TestFilledBuffer) {
    storage::log_reader_config cfg(
      model::offset{0}, model::offset::max(), ss::default_priority_class());

    // Create a collector that signals fullness at 2 empty batches.
    batch_collector collector(
      cfg, model::term_id{0}, model::packed_record_batch_header_size * 2);
    auto res = collector.add_batch(
      make_batch_header(model::offset{0}, model::offset{0}), iobuf{});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), reader_outcome::success);

    // Starting with the second batch, adding batches should signal the handler
    // is full.
    for (int i = 1; i <= 3; i++) {
        res = collector.add_batch(
          make_batch_header(model::offset{i}, model::offset{i}), iobuf{});
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(res.value(), reader_outcome::buffer_full);
    }
    // The handler should continue to accept these batches.
    auto batches = collector.release_batches();
    ASSERT_EQ(4, batches.size());

    // Once the old batches are released, we should be able to contribute more
    // batches.
    res = collector.add_batch(
      make_batch_header(model::offset{4}, model::offset{4}), iobuf{});
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), reader_outcome::success);
}
