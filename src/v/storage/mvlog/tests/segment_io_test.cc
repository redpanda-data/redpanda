// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/tests/random_batch.h"
#include "storage/mvlog/batch_collecting_stream_utils.h"
#include "storage/mvlog/batch_collector.h"
#include "storage/mvlog/entry.h"
#include "storage/mvlog/entry_stream.h"
#include "storage/mvlog/file.h"
#include "storage/mvlog/readable_segment.h"
#include "storage/mvlog/segment_appender.h"
#include "storage/mvlog/segment_reader.h"

#include <seastar/core/seastar.hh>

#include <gtest/gtest.h>

using namespace storage::experimental::mvlog;
using namespace experimental;

class SegmentTest : public ::testing::Test {
public:
    void SetUp() override {
        cleanup_files_.emplace_back(file_);
        paging_file_ = file_manager_.create_file(file_).get();
    }

    void TearDown() override {
        paging_file_->close().get();

        for (auto& file : cleanup_files_) {
            try {
                ss::remove_file(file.string()).get();
            } catch (...) {
            }
        }
    }

    ss::future<ss::circular_buffer<model::record_batch>>
    write_random_batches(int num_batches) {
        segment_appender appender(paging_file_.get());
        auto in_batches = co_await model::test::make_random_batches(
          model::offset{0}, num_batches, true);
        for (auto& b : in_batches) {
            co_await appender.append(b.copy());
        }
        co_return in_batches;
    }

protected:
    const std::filesystem::path file_{"segment"};
    file_manager file_manager_;
    std::unique_ptr<file> paging_file_;
    std::vector<std::filesystem::path> cleanup_files_;
};

// Basic test that we can append and read some batches.
TEST_F(SegmentTest, TestBasicRoundTrip) {
    auto in_batches = write_random_batches(100).get();
    readable_segment readable_seg(paging_file_.get());

    ASSERT_GT(paging_file_->size(), 0);
    storage::log_reader_config cfg{
      model::offset{0}, model::offset::max(), ss::default_priority_class()};
    batch_collector collector(cfg, model::term_id{0}, 128_MiB);
    auto reader = readable_seg.make_reader();

    entry_stream entries(reader->make_stream());
    auto res = collect_batches_from_stream(entries, collector).get();
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value(), collect_stream_outcome::end_of_stream);
    auto out_batches = collector.release_batches();
    ASSERT_EQ(100, out_batches.size());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(in_batches[i], out_batches[i]);
    }
}

// Test that we can append and read some batches even with a bounded size on
// the batch collector.
TEST_F(SegmentTest, TestFullCollector) {
    auto in_batches = write_random_batches(100).get();
    readable_segment readable_seg(paging_file_.get());

    ASSERT_GT(paging_file_->size(), 0);
    storage::log_reader_config cfg{
      model::offset{0}, model::offset::max(), ss::default_priority_class()};
    batch_collector collector(
      cfg, model::term_id{0}, /*max_buffer_size*/ 0_MiB);
    auto reader = readable_seg.make_reader();

    // Since our max bytes is so low, each time we read, we will see the buffer
    // as full.
    entry_stream entries(reader->make_stream());
    std::vector<model::record_batch> out_batches;
    for (int i = 0; i < 100; i++) {
        auto res = collect_batches_from_stream(entries, collector).get();
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(res.value(), collect_stream_outcome::buffer_full);
        auto collected = collector.release_batches();
        ASSERT_EQ(1, collected.size());
        out_batches.emplace_back(std::move(collected[0]));
    }
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(in_batches[i], out_batches[i]);
    }
}

// Test that we can append and read some batches within a certain range.
TEST_F(SegmentTest, TestBoundedOffsets) {
    auto in_batches = write_random_batches(5).get();
    readable_segment readable_seg(paging_file_.get());

    ASSERT_GT(paging_file_->size(), 0);
    auto reader = readable_seg.make_reader();
    auto bounded_offset = model::prev_offset(in_batches.back().base_offset());

    for (int min = 0; min <= bounded_offset(); min++) {
        for (int max = min; max <= bounded_offset(); max++) {
            storage::log_reader_config cfg{
              model::offset{min},
              model::offset{max},
              ss::default_priority_class()};
            batch_collector collector(cfg, model::term_id{0}, 128_MiB);
            entry_stream entries(reader->make_stream());
            auto res = collect_batches_from_stream(entries, collector).get();
            ASSERT_TRUE(res.has_value());
            ASSERT_EQ(res.value(), collect_stream_outcome::stop);
            auto out_batches = collector.release_batches();

            ASSERT_FALSE(out_batches.empty());
            ASSERT_LT(out_batches.size(), 10);

            ASSERT_TRUE(out_batches.begin()->contains(model::offset{min}));
            ASSERT_TRUE(out_batches.back().contains(model::offset{max}));
        }
    }
}
