// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/units.h"
#include "io/page_cache.h"
#include "io/pager.h"
#include "io/paging_data_source.h"
#include "io/persistence.h"
#include "io/scheduler.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "storage/mvlog/entry_stream_utils.h"
#include "storage/mvlog/segment_appender.h"
#include "storage/record_batch_utils.h"

#include <seastar/core/seastar.hh>

#include <gtest/gtest.h>

using namespace storage::experimental::mvlog;
using namespace ::experimental;

class SegmentAppenderTest : public ::testing::Test {
public:
    void SetUp() override {
        storage_ = std::make_unique<io::disk_persistence>();
        storage_->create(file_.string()).get()->close().get();
        cleanup_files_.emplace_back(file_);

        io::page_cache::config cache_config{
          .cache_size = 2_MiB, .small_size = 1_MiB};
        cache_ = std::make_unique<io::page_cache>(cache_config);
        scheduler_ = std::make_unique<io::scheduler>(100);
        pager_ = std::make_unique<io::pager>(
          file_, 0, storage_.get(), cache_.get(), scheduler_.get());
    }
    void TearDown() override {
        pager_->close().get();
        for (auto& file : cleanup_files_) {
            try {
                ss::remove_file(file.string()).get();
            } catch (...) {
            }
        }
    }

protected:
    const std::filesystem::path file_{"segment"};
    std::unique_ptr<io::persistence> storage_;
    std::unique_ptr<io::page_cache> cache_;
    std::unique_ptr<io::scheduler> scheduler_;
    std::unique_ptr<io::pager> pager_;
    std::vector<std::filesystem::path> cleanup_files_;
};

TEST_F(SegmentAppenderTest, TestAppendRecordBatches) {
    segment_appender appender(pager_.get());
    auto batches = model::test::make_random_batches().get();

    size_t prev_end_pos = 0;
    for (const auto& batch : batches) {
        // Construct the record body so we can compare against it.
        record_batch_entry_body entry_body;
        entry_body.term = batch.term();
        entry_body.record_batch_header.append(
          storage::batch_header_to_disk_iobuf(batch.header()));
        entry_body.records.append(batch.copy().release_data());
        auto entry_body_buf = serde::to_iobuf(std::move(entry_body));

        appender.append(batch.copy()).get();
        ASSERT_EQ(
          pager_->size(),
          prev_end_pos + entry_body_buf.size_bytes()
            + packed_entry_header_size);

        // The resulting pages should contain the header...
        auto hdr_stream = ss::input_stream<char>(
          ss::data_source(std::make_unique<io::paging_data_source>(
            pager_.get(),
            io::paging_data_source::config{
              prev_end_pos, packed_entry_header_size})));
        iobuf hdr_buf;
        hdr_buf.append(hdr_stream.read_exactly(packed_entry_header_size).get());

        auto hdr = entry_header_from_iobuf(std::move(hdr_buf));
        ASSERT_EQ(hdr.body_size, entry_body_buf.size_bytes());
        ASSERT_EQ(hdr.type, entry_type::record_batch);
        ASSERT_NE(hdr.header_crc, 0);
        ASSERT_EQ(entry_header_crc(hdr.body_size, hdr.type), hdr.header_crc);

        // ... followed by the entry body.
        auto body_stream = ss::input_stream<char>(
          ss::data_source(std::make_unique<io::paging_data_source>(
            pager_.get(),
            io::paging_data_source::config{
              prev_end_pos + packed_entry_header_size,
              entry_body_buf.size_bytes()})));
        iobuf body_buf;
        body_buf.append(
          body_stream.read_exactly(entry_body_buf.size_bytes()).get());
        ASSERT_EQ(entry_body_buf, body_buf);
        prev_end_pos = pager_->size();
    }
}
