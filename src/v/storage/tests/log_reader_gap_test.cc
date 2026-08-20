// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// CORE-8759: a read that cannot supply the offset range a segment says it wrote
// must fail, not report a clean end-of-stream.
//
// The parser in log_segment_batch_reader::initialize() is given a stream
// bounded by the filesize at the time of construction, which must match
// get_stable_offset(). All reads up to get_stable_offset() at construction time
// must succeed, or handle_corrupt_segment must detect the segment as corrupt.
//
// A note on why writing zeros straight through the appender is a faithful
// reproduction rather than a contrivance: for an active segment the readable
// extent (segment_reader::_file_size) is advanced only by
// segment::advance_stable_offset, from _inflight entries that are recorded at
// complete-batch boundaries. Appending raw bytes bypasses _inflight, so the
// zeros only become *readable* once a subsequent real batch extends the extent
// past them -- which is exactly the production shape: a gap with committed data
// behind it.

#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/record_utils.h"
#include "storage/exceptions.h"
#include "storage/log_reader.h"
#include "storage/parser_errc.h"
#include "storage/record_batch_builder.h"
#include "storage/record_batch_utils.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"
#include "storage/tests/utils/disk_log_builder.h"

#include <gtest/gtest.h>

#include <array>
#include <fcntl.h>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

// Comfortably larger than segment_index's 32KiB indexing step, so every batch
// gets its own index entry and a read can seek to an offset *past* an injected
// gap.
constexpr size_t big_payload = 64 * 1024;

// Comfortably smaller than the indexing step, so only the segment's first batch
// is indexed and a seek to any later offset lands back at file position 0.
constexpr size_t small_payload = 512;

model::record_batch make_batch(model::offset o, size_t payload_size) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, o);
    iobuf value;
    // Deliberately non-zero: a zero-filled *body* is fine, but keeping bodies
    // distinguishable from the injected gap makes failures easier to read.
    const std::string payload(payload_size, 'x');
    value.append(payload.data(), payload.size());
    builder.add_raw_kv(std::nullopt, std::move(value));
    return std::move(builder).build();
}

// Push raw bytes through the segment's appender, bypassing segment::append and
// therefore bypassing _inflight / the segment index. Mimics a lost write or an
// unwritten fallocated region sitting in the middle of a segment.
void inject_raw(storage::segment& seg, iobuf buf) {
    for (const auto& frag : buf) {
        seg.appender().append(frag.get(), frag.size()).get();
    }
}

void inject_zero_gap(storage::segment& seg, size_t n) {
    iobuf zeros;
    const std::vector<char> buf(n, 0);
    zeros.append(buf.data(), buf.size());
    inject_raw(seg, std::move(zeros));
}

// Overwrite bytes already on disk, in place. Used to corrupt a batch the
// segment index points *at*, which appending cannot do. Plain POSIX rather than
// seastar so the write needs no dma alignment.
[[nodiscard]] bool overwrite_in_place(
  const std::filesystem::path& path, size_t pos, size_t n, char fill) {
    const int fd = ::open(path.c_str(), O_WRONLY); // NOLINT
    if (fd < 0) {
        return false;
    }
    const std::vector<char> buf(n, fill);
    const bool ok = ::pwrite(
                      fd, buf.data(), buf.size(), static_cast<off_t>(pos))
                      == static_cast<ssize_t>(buf.size())
                    && ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}

// A well-formed, correctly-CRC'd batch header that lies about how long its body
// is. The parser accepts the header, then short-reads trying to consume
// `size_bytes - packed_record_batch_header_size` body bytes.
void inject_overlong_header(
  storage::segment& seg, model::offset base, int32_t claimed_size_bytes) {
    model::record_batch_header header{
      .size_bytes = claimed_size_bytes,
      .base_offset = base,
      .type = model::record_batch_type::raft_data,
      .crc = 0,
      .attrs = model::record_batch_attributes{},
      .last_offset_delta = 0,
      .first_timestamp = model::timestamp{1},
      .max_timestamp = model::timestamp{1},
      .producer_id = -1,
      .producer_epoch = -1,
      .base_sequence = -1,
      .record_count = 1,
    };
    // Make the header pass read_header_impl's CRC check, so the parser gets far
    // enough to try to read the (nonexistent) body.
    header.header_crc = model::internal_header_only_crc(header);
    inject_raw(seg, storage::batch_header_to_disk_iobuf(header));
}

struct log_reader_gap_fixture : public testing::Test {
    storage::disk_log_builder b;
    ~log_reader_gap_fixture() override { b.stop().get(); }

    // Reads must come off disk. skip_batch_cache only skips LRU promotion --
    // cached batches are still returned -- so the index has to be reset.
    void drop_batch_cache() {
        auto& segs = b.get_log_segments();
        for (size_t i = 0; i < segs.size(); ++i) {
            b.get_segment(i).reset_batch_cache_index().get();
        }
    }

    static storage::local_log_reader_config throwing_reader_config(
      model::offset start = model::offset(0),
      model::offset max = model::model_limits<model::offset>::max()) {
        storage::local_log_reader_config cfg{start, max};
        cfg.on_corrupt_segment
          = storage::corrupt_segment_action::throw_exception;
        return cfg;
    }

    // Assert detection fired, and that it named the first offset the read could
    // not supply.
    void expect_detected_at(
      storage::local_log_reader_config cfg, model::offset expected) {
        try {
            b.consume(cfg).get();
            FAIL() << "expected malformed_batch_stream_exception";
        } catch (const malformed_batch_stream_exception& e) {
            EXPECT_EQ(e.failed_at(), expected) << e.what();
        }
    }
};

constexpr auto stable_offset = model::offset(42);

} // namespace

// Test basic handle_corrupt_segment errc handling
TEST(corrupt_segment, classifies_every_parser_errc) {
    constexpr std::array all_errcs{
      storage::parser_errc::none,
      storage::parser_errc::end_of_stream,
      storage::parser_errc::header_only_crc_missmatch,
      storage::parser_errc::input_stream_not_enough_bytes,
      storage::parser_errc::fallocated_file_read_zero_bytes_for_header,
      storage::parser_errc::not_enough_bytes_in_parser_for_one_record,
    };
    for (const auto err : all_errcs) {
        const auto describe = fmt::format("errc: {}", to_string_view(err));
        auto detect = [err](model::offset next_offset) {
            storage::internal::handle_corrupt_segment(
              err,
              "0-0-v1.log",
              next_offset,
              stable_offset,
              storage::corrupt_segment_action::throw_exception);
        };
        auto fatal_at = [&](model::offset next_offset) {
            EXPECT_THROW(detect(next_offset), malformed_batch_stream_exception)
              << describe << " at next_offset " << next_offset;
        };
        auto benign_at = [&](model::offset next_offset) {
            EXPECT_NO_THROW(detect(next_offset))
              << describe << " at next_offset " << next_offset;
        };

        // next_offset == stable means the read terminated before successfully
        // reading stable_offset.
        const auto at_bound = stable_offset;
        const auto past_bound = model::next_offset(stable_offset);

        // Ensures any added errcs will fail the build unless also added here
        switch (err) {
        // The consumer asked to stop, valid anywhere
        case storage::parser_errc::none:
            benign_at(at_bound);
            benign_at(past_bound);
            break;
        // Parser reached end of stream.  Valid iff next_offset > stable_offset.
        case storage::parser_errc::end_of_stream:
            fatal_at(at_bound);
            benign_at(past_bound);
            break;
        // Always invalid
        case storage::parser_errc::header_only_crc_missmatch:
        case storage::parser_errc::input_stream_not_enough_bytes:
        case storage::parser_errc::fallocated_file_read_zero_bytes_for_header:
        case storage::parser_errc::not_enough_bytes_in_parser_for_one_record:
            fatal_at(at_bound);
            fatal_at(past_bound);
            break;
        }
    }
}

// Test that exception (and therefore abort message) contain segment name,
// offset at which reading stopped, stable_offset from read start, and error
TEST(corrupt_segment, message_identifies_the_damage) {
    try {
        storage::internal::handle_corrupt_segment(
          storage::parser_errc::fallocated_file_read_zero_bytes_for_header,
          "kafka/topic/0_11/1234-7-v1.log",
          model::offset(97),
          model::offset(512),
          storage::corrupt_segment_action::throw_exception);
        FAIL() << "expected malformed_batch_stream_exception";
    } catch (const malformed_batch_stream_exception& e) {
        const std::string msg{e.what()};
        EXPECT_NE(msg.find("kafka/topic/0_11/1234-7-v1.log"), std::string::npos)
          << msg;
        EXPECT_NE(msg.find("97"), std::string::npos) << msg;
        EXPECT_NE(msg.find("512"), std::string::npos) << msg;
        EXPECT_NE(
          msg.find("fallocated_file_read_zero_bytes_for_header"),
          std::string::npos)
          << msg;
    }
}

// A zeroed batch header in the middle of segment 0, with a durable batch behind
// it and a whole further segment behind that.
TEST_F(log_reader_gap_fixture, zeroed_header_mid_segment_fails_the_read) {
    using namespace storage; // NOLINT

    b | start() | add_segment(0);

    // Offset 0: complete, flushed, before the gap.
    b.add_batch(make_batch(model::offset(0), big_payload)).get();

    inject_zero_gap(b.get_segment(0), model::packed_record_batch_header_size);

    // Offset 1: complete and durable, but sitting behind the gap. Appending it
    // is what pushes the readable extent past the zeros.
    b.add_batch(make_batch(model::offset(1), big_payload)).get();

    // Offset 2: an entire second segment behind the gap.
    b.add_segment(model::offset(2)).get();
    b.add_batch(make_batch(model::offset(2), big_payload)).get();
    b.get_log()->flush().get();

    ASSERT_EQ(b.get_log_segments().size(), 2u);

    const auto log_dirty = b.get_log()->offsets().dirty_offset;
    const auto seg0_stable = b.get_segment(0).offsets().get_stable_offset();
    ASSERT_EQ(log_dirty, model::offset(2));
    ASSERT_GE(seg0_stable, model::offset(1))
      << "segment 0 must report offset 1 as written, otherwise the read below "
         "stops for a legitimate reason and the test proves nothing";

    drop_batch_cache();

    expect_detected_at(throwing_reader_config(), model::offset(1));
}

// Test behavior of read after damaged segment region.
// Depending on indexing, the actual read may or may not precede the damage.
// In this case, the read starts after the damaged region and should succeed.
//
// The distinction between this and the next test is where the index entry is
// relative to the target and damaged region -- we just want coverage of both
// behaviors.
TEST_F(log_reader_gap_fixture, data_behind_a_zeroed_header_is_still_readable) {
    using namespace storage; // NOLINT

    b | start() | add_segment(0);
    b.add_batch(make_batch(model::offset(0), big_payload)).get();
    inject_zero_gap(b.get_segment(0), model::packed_record_batch_header_size);
    b.add_batch(make_batch(model::offset(1), big_payload)).get();
    b.add_segment(model::offset(2)).get();
    b.add_batch(make_batch(model::offset(2), big_payload)).get();
    b.get_log()->flush().get();

    drop_batch_cache();

    // Start at offset 1: the segment index has an entry for it (batches are
    // larger than the 32KiB index step), so the read starts *after* the zeros.
    auto batches = b.consume(throwing_reader_config(model::offset(1))).get();

    EXPECT_EQ(batches.size(), 2u);
    ASSERT_FALSE(batches.empty());
    EXPECT_EQ(batches.front().base_offset(), model::offset(1));
    EXPECT_EQ(batches.back().base_offset(), model::offset(2));
}

// Similar to above, but index causes read to start *before* the damaged
// region.
TEST_F(log_reader_gap_fixture, data_behind_a_zeroed_header_is_not_readable) {
    using namespace storage; // NOLINT

    b | start() | add_segment(0);
    b.add_batch(make_batch(model::offset(0), small_payload)).get();
    inject_zero_gap(b.get_segment(0), model::packed_record_batch_header_size);
    b.add_batch(make_batch(model::offset(1), small_payload)).get();
    b.add_segment(model::offset(2)).get();
    b.add_batch(make_batch(model::offset(2), big_payload)).get();
    b.get_log()->flush().get();

    drop_batch_cache();

    // Start at offset 1: the segment index does not have an entry for it,
    // so the read will start at the beginning and fail
    expect_detected_at(
      throwing_reader_config(model::offset(1)), model::offset(1));
}

// Similar to above, but with gap at the start of the segment
TEST_F(log_reader_gap_fixture, data_behind_zeroed_header_at_start_readable) {
    using namespace storage; // NOLINT

    b | start() | add_segment(0);
    b.add_batch(make_batch(model::offset(0), big_payload)).get();

    // Segment 1 opens directly onto the gap, ahead of its first real batch.
    b.add_segment(model::offset(1)).get();
    inject_overlong_header(b.get_segment(1), model::offset(1), 8 * 1024 * 1024);
    b.add_batch(make_batch(model::offset(1), big_payload)).get();

    b.add_segment(model::offset(2)).get();
    b.add_batch(make_batch(model::offset(2), big_payload)).get();
    b.get_log()->flush().get();

    ASSERT_EQ(b.get_log_segments().size(), 3u);
    ASSERT_EQ(b.get_log()->offsets().dirty_offset, model::offset(2));

    drop_batch_cache();

    auto batches = b.consume(throwing_reader_config()).get();

    // Everything is returned: the index entry for offset 1 points past the gap,
    // so segment 1's parser never sees it.
    EXPECT_EQ(batches.size(), 3u);
    ASSERT_FALSE(batches.empty());
    EXPECT_EQ(batches.front().base_offset(), model::offset(0));
    EXPECT_EQ(batches.back().base_offset(), model::offset(2));
}

// Similar, but the parser stops with input_stream_not_enough_bytes rather
// than on a zeroed header. This is the variant logged in CORE-8759.
TEST_F(log_reader_gap_fixture, short_read_mid_segment_fails_the_read) {
    using namespace storage; // NOLINT

    b | start() | add_segment(0);

    b.add_batch(make_batch(model::offset(0), big_payload)).get();

    // Claim a body far longer than anything left in the segment, so
    // consume_records() is guaranteed to short-read.
    inject_overlong_header(b.get_segment(0), model::offset(1), 8 * 1024 * 1024);

    b.add_batch(make_batch(model::offset(1), big_payload)).get();
    b.add_segment(model::offset(2)).get();
    b.add_batch(make_batch(model::offset(2), big_payload)).get();
    b.get_log()->flush().get();

    const auto log_dirty = b.get_log()->offsets().dirty_offset;
    ASSERT_EQ(log_dirty, model::offset(2));

    drop_batch_cache();

    expect_detected_at(throwing_reader_config(), model::offset(1));
}

// Test read against a segment with an unflushed append, should not fail
TEST_F(log_reader_gap_fixture, read_with_unflushed_tail_should_succeed) {
    using namespace storage; // NOLINT

    b | start() | add_segment(0);
    b.add_batch(make_batch(model::offset(0), big_payload)).get();

    // Drop the cache while it is still clean, so the read below has to come off
    // disk. Appending after this leaves offset 1 cached and dirty.
    drop_batch_cache();

    b.add_batch(
       make_batch(model::offset(1), big_payload),
       log_append_config{log_append_config::fsync::no},
       disk_log_builder::should_flush_after::no)
      .get();

    // Offset 1 sits above the readable extent, so it can only be served from
    // the cache -- which is how production serves dirty batches. Asserted so
    // that the test fails rather than degrading into an ordinary read if the
    // extent ever stops lagging.
    ASSERT_LT(
      b.get_segment(0).offsets().get_stable_offset(),
      b.get_segment(0).offsets().get_dirty_offset());

    auto batches = decltype(b.consume().get()){};
    ASSERT_NO_THROW(batches = b.consume(throwing_reader_config()).get());
    ASSERT_EQ(batches.size(), 2u);
    EXPECT_EQ(batches.front().base_offset(), model::offset(0));
    EXPECT_EQ(batches.back().base_offset(), model::offset(1));

    // batch_cache_index asserts if it is destroyed still tracking dirty
    // batches.
    b.get_log()->flush().get();
}

// Log segment truncated before stable_offset.
//
// Truncating on an exact batch boundary means nothing is malformed; the bytes
// simply stop, so the parser reports end_of_stream. That is only
// distinguishable from a reader whose stream snapshot ended because it compares
// against the stable offset captured when the stream was created.
TEST_F(
  log_reader_gap_fixture, data_file_shorter_than_the_index_fails_the_read) {
    using namespace storage; // NOLINT

    b | start() | add_segment(0);
    b.add_batch(make_batch(model::offset(0), big_payload)).get();
    b.add_batch(make_batch(model::offset(1), big_payload)).get();
    b.get_log()->flush().get();

    // Boundary after offset 1, captured before offset 2 exists. Two batches
    // survive the truncation below, so detection has to name 2 rather than the
    // first offset in the log.
    const auto batch_1_end = b.get_segment(0).reader().file_size();

    b.add_batch(make_batch(model::offset(2), big_payload)).get();
    b.get_log()->flush().get();

    ASSERT_EQ(b.get_segment(0).offsets().get_stable_offset(), model::offset(2));
    ASSERT_GT(b.get_segment(0).reader().file_size(), batch_1_end);

    // Shorten the data file only. The index and the offset tracker keep
    // advertising offset 2, so the segment now claims more than it can supply.
    b.get_segment(0).reader().truncate(batch_1_end).get();
    ASSERT_EQ(b.get_segment(0).index().max_offset(), model::offset(2));

    drop_batch_cache();

    expect_detected_at(throwing_reader_config(), model::offset(2));
}

// Damage at the *first* position the parser reads. consume() reports failure
// rather than partial success when it consumed nothing, so check that
// handle_corrupt_segment still runs.
TEST_F(
  log_reader_gap_fixture, damage_at_the_first_parsed_position_is_detected) {
    using namespace storage; // NOLINT

    b | start() | add_segment(0);
    b.add_batch(make_batch(model::offset(0), big_payload)).get();
    b.add_batch(make_batch(model::offset(1), big_payload)).get();
    b.get_log()->flush().get();

    // Batches exceed the 32KiB index step, so offset 1 has its own entry and a
    // seek to it starts the parser exactly at the header corrupted below.
    auto entry = b.get_segment(0).index().find_nearest(model::offset(1));
    ASSERT_TRUE(entry.has_value());
    ASSERT_EQ(entry->offset, model::offset(1));

    ASSERT_TRUE(overwrite_in_place(
      b.get_segment(0).reader().path(),
      entry->filepos,
      model::packed_record_batch_header_size,
      '\x5a'))
      << "could not corrupt the segment file";

    drop_batch_cache();

    // Nothing precedes the damage, so bytes_consumed is zero and the parser's
    // header CRC check is the only thing that fired.
    expect_detected_at(
      throwing_reader_config(model::offset(1)), model::offset(1));
}

TEST_F(log_reader_gap_fixture, intact_log_reads_completely) {
    using namespace storage; // NOLINT

    b | start() | add_segment(0);
    b.add_batch(make_batch(model::offset(0), big_payload)).get();
    b.add_batch(make_batch(model::offset(1), big_payload)).get();
    b.add_segment(model::offset(2)).get();
    b.add_batch(make_batch(model::offset(2), big_payload)).get();
    b.get_log()->flush().get();

    ASSERT_EQ(b.get_log_segments().size(), 2u);
    drop_batch_cache();

    auto batches = b.consume(throwing_reader_config()).get();

    EXPECT_EQ(batches.size(), 3u);
    ASSERT_FALSE(batches.empty());
    EXPECT_EQ(batches.front().base_offset(), model::offset(0));
    EXPECT_EQ(batches.back().base_offset(), model::offset(2));
}
