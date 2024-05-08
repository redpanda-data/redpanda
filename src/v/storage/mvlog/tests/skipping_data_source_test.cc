// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/units.h"
#include "bytes/iostream.h"
#include "bytes/random.h"
#include "random/generators.h"
#include "storage/mvlog/file.h"
#include "storage/mvlog/skipping_data_source.h"

#include <seastar/core/seastar.hh>
#include <seastar/util/short_streams.hh>

#include <gtest/gtest.h>

using namespace storage::experimental::mvlog;
using namespace experimental;

class SkippingStreamTest : public ::testing::Test {
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

    ss::future<> write_buf(iobuf buf) {
        co_await paging_file_->append(std::move(buf));
    }
    ss::input_stream<char>
    make_skipping_stream(skipping_data_source::read_list_t read_list) {
        return ss::input_stream<char>(
          ss::data_source(std::make_unique<skipping_data_source>(
            paging_file_.get(), std::move(read_list))));
    }

    void check_equivalent(ss::input_stream<char> stream, iobuf expected_buf) {
        auto data = stream.read_exactly(expected_buf.size_bytes()).get();
        auto no_more = stream.read_exactly(1).get();
        EXPECT_TRUE(no_more.empty()) << "Stream isn't empty after reading";

        iobuf actual_stream_buf;
        actual_stream_buf.append(std::move(data));
        EXPECT_EQ(actual_stream_buf, expected_buf) << fmt::format(
          "{}\nvs\n{}",
          actual_stream_buf.hexdump(1024),
          expected_buf.hexdump(1024));
        ASSERT_EQ(actual_stream_buf.size_bytes(), expected_buf.size_bytes());
    }

protected:
    const std::filesystem::path file_{"skipping_file"};
    file_manager file_manager_;
    std::unique_ptr<file> paging_file_;
    std::vector<std::filesystem::path> cleanup_files_;
};

TEST_F(SkippingStreamTest, TestEmptyReadList) {
    auto buf = random_generators::make_iobuf();
    write_buf(std::move(buf)).get();

    // Empty read list.
    auto stream = make_skipping_stream({});
    auto data = stream.read().get();
    ASSERT_TRUE(data.empty());
}

TEST_F(SkippingStreamTest, TestEmptyInterval) {
    auto buf = random_generators::make_iobuf();
    write_buf(std::move(buf)).get();

    // Bogus list.
    skipping_data_source::read_list_t reads;
    reads.emplace_back(skipping_data_source::read_interval{0, 0});
    auto stream = make_skipping_stream(std::move(reads));
    auto data = stream.read().get();
    ASSERT_TRUE(data.empty());
}

TEST_F(SkippingStreamTest, TestOutOfBounds) {
    auto buf = random_generators::make_iobuf();
    write_buf(buf.copy()).get();

    // Bogus list.
    for (int i = 0; i < 5; i++) {
        skipping_data_source::read_list_t reads;
        reads.emplace_back(
          skipping_data_source::read_interval{buf.size_bytes() + i, 10});
        auto stream = make_skipping_stream(std::move(reads));
        auto data = stream.read().get();
        ASSERT_TRUE(data.empty());
    }
}

TEST_F(SkippingStreamTest, TestFullInterval) {
    auto buf = random_generators::make_iobuf();
    write_buf(buf.copy()).get();

    // Read exactly the right sized buffer.
    skipping_data_source::read_list_t reads;
    reads.emplace_back(
      skipping_data_source::read_interval{0, buf.size_bytes()});
    auto stream = make_skipping_stream(std::move(reads));
    ASSERT_NO_FATAL_FAILURE(
      check_equivalent(std::move(stream), std::move(buf)));
}

TEST_F(SkippingStreamTest, TestOversizedInterval) {
    const auto buf = random_generators::make_iobuf();
    write_buf(buf.copy()).get();

    for (size_t i = 0; i < buf.size_bytes(); i++) {
        // Make an expected buffer starting from offset i, reading to past the
        // end of the buffer.
        auto input_stream = make_iobuf_input_stream(buf.copy());
        auto str = ss::util::read_entire_stream_contiguous(input_stream).get();
        auto substr = str.substr(i);
        iobuf subbuf;
        subbuf.append(substr.data(), substr.size());

        // Now create a skipping stream on the original buffer that definitely
        // goes past the end.
        skipping_data_source::read_list_t reads;
        reads.emplace_back(
          skipping_data_source::read_interval{i, buf.size_bytes() + 10});
        auto stream = make_skipping_stream(std::move(reads));

        ASSERT_NO_FATAL_FAILURE(
          check_equivalent(std::move(stream), std::move(subbuf)));
    }
}

TEST_F(SkippingStreamTest, TestSkipFront) {
    auto buf = random_generators::make_iobuf();
    write_buf(buf.copy()).get();

    // Skip the beginning.
    skipping_data_source::read_list_t reads;
    reads.emplace_back(
      skipping_data_source::read_interval{10, buf.size_bytes() - 10});
    auto stream = make_skipping_stream(std::move(reads));

    // Trim the front and compare.
    buf.trim_front(10);
    ASSERT_NO_FATAL_FAILURE(
      check_equivalent(std::move(stream), std::move(buf)));
}

TEST_F(SkippingStreamTest, TestSkipBack) {
    auto buf = random_generators::make_iobuf();
    write_buf(buf.copy()).get();

    // Skip the back.
    skipping_data_source::read_list_t reads;
    reads.emplace_back(
      skipping_data_source::read_interval{0, buf.size_bytes() - 10});
    auto stream = make_skipping_stream(std::move(reads));

    // Trim the back and compare.
    buf.trim_back(10);
    ASSERT_NO_FATAL_FAILURE(
      check_equivalent(std::move(stream), std::move(buf)));
}

TEST_F(SkippingStreamTest, TestUnorderedReads) {
    auto buf = random_generators::make_iobuf();
    write_buf(buf.copy()).get();

    skipping_data_source::read_list_t reads;
    reads.emplace_back(
      skipping_data_source::read_interval{10, buf.size_bytes() - 10});
    reads.emplace_back(
      skipping_data_source::read_interval{0, buf.size_bytes() - 10});
    auto stream = make_skipping_stream(std::move(reads));

    // A read range of a suffix.
    auto front_trim_buf = buf.copy();
    front_trim_buf.trim_front(10);

    // And a read range of a prefix.
    auto back_trim_buf = buf.copy();
    back_trim_buf.trim_back(10);

    // The result should be an out-of-order set of intervals, which is fine.
    iobuf expected_buf;
    expected_buf.append(front_trim_buf.copy());
    expected_buf.append(back_trim_buf.copy());
    ASSERT_NO_FATAL_FAILURE(
      check_equivalent(std::move(stream), std::move(expected_buf)));
}

TEST_F(SkippingStreamTest, TestRandomOrderedReads) {
    skipping_data_source::read_list_t reads;
    size_t cur_size = 0;
    iobuf expected_skipping_buf;
    iobuf buf;
    for (int i = 0; i < random_generators::get_int(1, 100); i++) {
        const size_t size = random_generators::get_int(0, 10);
        auto random_buf = random_generators::make_iobuf(size);
        bool should_read = random_generators::get_int(0, 1);
        if (should_read) {
            // Build the expected output with just the intervals that are read.
            expected_skipping_buf.append(random_buf.copy());
            reads.emplace_back(
              skipping_data_source::read_interval{cur_size, size});
        }
        buf.append(random_buf.copy());
        cur_size += size;
    }
    write_buf(buf.copy()).get();
    auto stream = make_skipping_stream(std::move(reads));
    ASSERT_NO_FATAL_FAILURE(
      check_equivalent(std::move(stream), std::move(expected_skipping_buf)));
}
