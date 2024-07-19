// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "container/fragmented_vector.h"
#include "jumbo_log/segment/segment.h"
#include "jumbo_log/segment/sparse_index.h"
#include "jumbo_log/segment_reader/segment_reader.h"
#include "jumbo_log/segment_writer/segment_writer.h"
#include "model/fundamental.h"
#include "model/offset_interval.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "random/generators.h"
#include "serde/rw/rw.h"
#include "test_utils/test.h"
#include "utils/named_type.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>

#include <absl/container/btree_map.h>
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <ostream>
#include <stdint.h>
#include <vector>

class sparse_index_data_accessor {
public:
    bool index_equal(
      const jumbo_log::segment::sparse_index& lhs,
      const jumbo_log::segment::sparse_index& rhs) {
        if (lhs._data.entries.size() != rhs._data.entries.size()) {
            return false;
        }

        for (size_t i = 0; i < lhs._data.entries.size(); ++i) {
            if (lhs._data.entries[i].ntp != rhs._data.entries[i].ntp) {
                return false;
            }

            if (lhs._data.entries[i].offset != rhs._data.entries[i].offset) {
                return false;
            }

            if (
              lhs._data.entries[i].loc.offset_bytes
              != rhs._data.entries[i].loc.offset_bytes) {
                return false;
            }

            if (
              lhs._data.entries[i].loc.size_bytes
              != rhs._data.entries[i].loc.size_bytes) {
                return false;
            }
        }

        return lhs._data.upper_limit == rhs._data.upper_limit;
    }

    std::ostream&
    print_index(std::ostream& os, const jumbo_log::segment::sparse_index& idx) {
        os << "Sparse index:\n";
        for (const auto& entry : idx._data.entries) {
            os << fmt::format(
              "  ntp: {}; offset: {}; loc: {}-{}\n",
              entry.ntp,
              entry.offset,
              entry.loc.offset_bytes,
              entry.loc.offset_bytes + entry.loc.size_bytes);
        }
        os << fmt::format(
          "  upper limit: ntp: {}; offset: {}\n",
          idx._data.upper_limit.first,
          idx._data.upper_limit.second);
        return os;
    }
};

class jls_roundtrip_test
  : public ::seastar_test
  , public sparse_index_data_accessor {};

namespace {
ss::future<> test_roundtrip_scan(
  jls_roundtrip_test* test, size_t num_ntps, size_t batches_per_ntp) {
    std::vector<model::ntp> ntps;
    for (auto i = 0; i < num_ntps; ++i) {
        model::ntp ntp(
          model::ns("kafka"),
          model::topic(fmt::format("topic_{}", i / 3)),
          model::partition_id(i));
        ntps.push_back(ntp);
    }
    std::sort(ntps.begin(), ntps.end());

    absl::btree_map<model::ntp, fragmented_vector<model::record_batch>>
      batches_by_ntp;

    iobuf buf;
    auto out = make_iobuf_ref_output_stream(buf);

    jumbo_log::segment_writer::writer writer(
      &out,
      jumbo_log::segment_writer::options{
        // Smaller chunk size for testing.
        .chunk_size_target = 96 * 1024,
      });

    for (auto ntp : ntps) {
        model::offset last_offset{10};

        for (auto j = 0; j < batches_per_ntp; ++j) {
            // Insert a gap.
            auto base_offset = model::offset(last_offset() + 1042);

            auto rand_batches = co_await model::test::make_random_batches(
              base_offset, 1);

            for (auto& batch : rand_batches) {
                last_offset = batch.last_offset();
                batches_by_ntp[ntp].push_back(batch.copy());

                co_await writer.write(ntp, std::move(batch));
            }
        }
    }

    co_await writer.close();
    co_await out.close();

    // Determine footer size.
    auto footer_buf = buf.share(
      buf.size_bytes() - jumbo_log::segment::FOOTER_EPILOGUE_SIZE_BYTES,
      jumbo_log::segment::FOOTER_EPILOGUE_SIZE_BYTES);
    auto footer_buf_parser = iobuf_parser(std::move(footer_buf));
    auto footer_size = serde::read_nested<int64_t>(footer_buf_parser, 0);

    // Read chunk by chunk and make sure that we have read all the data.
    iobuf_parser parser(std::move(buf));
    parser.skip(jumbo_log::segment::RP2_MAGIC_SIGNATURE.size());

    auto current_ntp_it = ntps.begin();
    fragmented_vector<model::record_batch> read_batches;

    absl::
      btree_map<model::ntp, fragmented_vector<jumbo_log::segment::chunk_loc>>
        batch_chunk_locs_by_ntp;

    auto assert_invariants_and_advance = [&]() {
        SCOPED_TRACE(fmt::format(
          "assert_invariants_and_advance for ntp: {}", *current_ntp_it));

        ASSERT_EQ(read_batches, batches_by_ntp[*current_ntp_it])
          << "  for ntp: " << *current_ntp_it;

        current_ntp_it++;
        read_batches.clear();
    };

    while (parser.bytes_left() > footer_size) {
        auto chunk_size = serde::read_nested<jumbo_log::segment::chunk_size_t>(
          parser, 0);
        auto chunk_offset_bytes = parser.bytes_consumed();

        auto chunk_data = parser.share(chunk_size);

        auto this_chunk_loc = jumbo_log::segment::chunk_loc{
          .offset_bytes = chunk_offset_bytes,
          .size_bytes = chunk_size,
        };

        // Workaround until I figure out a better interface.
        auto ntp_parser = iobuf_parser(chunk_data.copy());
        auto ntp = serde::read_nested<model::ntp>(ntp_parser, 0);
        if (ntp != *current_ntp_it) {
            ASSERT_NO_FATAL_FAILURE_CORO(assert_invariants_and_advance())
              << "We have read all the batches for the current ntp. "
                 "Read the next ntp from the same chunk.";
        }

        // Read all ntps from the chunk.
        bool chunk_exhausted = false;
        while (!chunk_exhausted) {
            // Create a chunk reader for the current ntp.
            jumbo_log::segment_reader::chunk_reader_from_iobuf reader(
              chunk_data.copy(),
              *current_ntp_it,
              model::bounded_offset_interval::checked(
                model::offset(0), model::offset::max()));

            // Read all the batches from the chunk.
            while (true) {
                auto batches = co_await reader.read_some();
                if (batches.empty()) {
                    if (reader.end_of_stream()) {
                        chunk_exhausted = true;
                    } else {
                        ASSERT_NO_FATAL_FAILURE_CORO(
                          assert_invariants_and_advance())
                          << "We have read all the batches for the "
                             "current ntp. Read the next ntp from the "
                             "same chunk.";
                    }
                    break;
                } else {
                    for (auto& batch : batches) {
                        read_batches.push_back(batch.copy());
                        batch_chunk_locs_by_ntp[*current_ntp_it].push_back(
                          this_chunk_loc);
                    }
                }
            }
        }
    }

    ASSERT_NO_FATAL_FAILURE_CORO(assert_invariants_and_advance())
      << "Finished.";

    // We did go through all the ntps.
    ASSERT_TRUE_CORO(current_ntp_it == ntps.end());

    // Sparse index follows.
    auto [index, index_size] = co_await jumbo_log::segment_reader::
      sparse_index_reader_from_iobuf::read(
        parser.share_no_consume(parser.bytes_left()));

    // Skip index bytes.
    parser.skip(index_size);

    // Footer size. We parsed it already.
    parser.skip(sizeof(int64_t));

    ASSERT_EQ_CORO(
      footer_size, jumbo_log::segment::FOOTER_EPILOGUE_SIZE_BYTES + index_size);

    auto footer_signature = parser.share(
      jumbo_log::segment::RP2_MAGIC_SIGNATURE.size());
    ASSERT_EQ_CORO(footer_signature, jumbo_log::segment::RP2_MAGIC_SIGNATURE);

    // We have read all the data.
    ASSERT_EQ_CORO(parser.bytes_left(), 0);

    // Verify that sparse index points to the correct chunk for each possible
    // record.
    // test->print_index(std::cout, index);

    auto assert_index_loc =
      [&index](
        const model::ntp& ntp,
        model::offset offset,
        const jumbo_log::segment::chunk_loc& expected_chunk_loc) {
          auto loc = index.find_chunk(ntp, offset);

          ASSERT_TRUE(loc.has_value());

          ASSERT_EQ(expected_chunk_loc.offset_bytes, loc->offset_bytes);
          ASSERT_EQ(expected_chunk_loc.size_bytes, loc->size_bytes);
      };

    for (const auto& [ntp, batches] : batches_by_ntp) {
        for (auto batch_ix = 0; batch_ix < batches.size(); ++batch_ix) {
            SCOPED_TRACE(fmt::format("ntp: {}; batch_ix: {}", ntp, batch_ix));

            auto& batch = batches[batch_ix];
            auto& expected_chunk_loc = batch_chunk_locs_by_ntp[ntp][batch_ix];

            assert_index_loc(ntp, batch.base_offset(), expected_chunk_loc);
            assert_index_loc(ntp, batch.last_offset(), expected_chunk_loc);

            if (batch.record_count() > 2) {
                assert_index_loc(
                  ntp,
                  batch.base_offset()
                    + model::offset(
                      random_generators::get_int(batch.record_count() - 1)),
                  expected_chunk_loc);
            }
        }
    }
}

} // namespace

TEST_F_CORO(jls_roundtrip_test, roundtrip) {
    {
        SCOPED_TRACE("Test with a single ntp and a single batch.");
        co_await test_roundtrip_scan(this, 1, 1);
    }
    {
        SCOPED_TRACE(
          "Test with a single ntp and multiple batches that will span "
          "multiple chunks.");
        co_await test_roundtrip_scan(this, 1, 100);
    }
    {
        SCOPED_TRACE(
          "Test with multiple ntps but single butch so that they all "
          "end up in the same chunk.");
        co_await test_roundtrip_scan(this, 10, 2);
    }
    {
        SCOPED_TRACE("Mix.");
        co_await test_roundtrip_scan(this, 10, 100);
    }
}

namespace {
jumbo_log::segment::sparse_index create_test_index() {
    jumbo_log::segment_writer::sparse_index_accumulator acc;
    acc.add_entry(
      model::ntp(
        model::ns("kafka"), model::topic("topic_5"), model::partition_id(0)),
      model::offset{100},
      0,
      100);
    acc.add_entry(
      model::ntp(
        model::ns("kafka"), model::topic("topic_5"), model::partition_id(0)),
      model::offset{500},
      100,
      100);
    acc.add_entry(
      model::ntp(
        model::ns("kafka"), model::topic("topic_6"), model::partition_id(0)),
      model::offset{50},
      200,
      100);
    acc.add_upper_limit(
      model::ntp(
        model::ns("kafka"), model::topic("topic_6"), model::partition_id(0)),
      model::offset(100));

    return std::move(acc).to_index();
}

} // namespace

TEST_F_CORO(jls_roundtrip_test, test_index_roundtrip) {
    auto index = create_test_index();

    iobuf buf;
    auto out = make_iobuf_ref_output_stream(buf);

    jumbo_log::segment_writer::sparse_index_writer writer;
    jumbo_log::segment_writer::offset_tracking_output_stream otos(&out);

    auto written = co_await writer(&otos, &index);

    co_await out.close();

    auto read_index_res = co_await jumbo_log::segment_reader::
      sparse_index_reader_from_iobuf::read(std::move(buf));

    ASSERT_EQ_CORO(written, read_index_res.second);
    ASSERT_TRUE_CORO(index_equal(index, read_index_res.first));
}

TEST_F(jls_roundtrip_test, test_index) {
    auto index = create_test_index();

    {
        // Try to find a NTP that is higher than everything in the index.
        auto loc = index.find_chunk(
          model::ntp(
            model::ns("kafka"),
            model::topic("topic_1"),
            model::partition_id(0)),
          model::offset{0});

        ASSERT_FALSE(loc.has_value());
    }

    {
        // Try to find a NTP that is in the index but use a lower offset.
        auto loc = index.find_chunk(
          model::ntp(
            model::ns("kafka"),
            model::topic("topic_5"),
            model::partition_id(0)),
          model::offset{0});

        ASSERT_FALSE(loc.has_value());
    }

    {
        // Find exact match.
        auto loc = index.find_chunk(
          model::ntp(
            model::ns("kafka"),
            model::topic("topic_5"),
            model::partition_id(0)),
          model::offset{100});

        ASSERT_TRUE(loc.has_value());
        ASSERT_EQ(loc->offset_bytes, 0);
        ASSERT_EQ(loc->size_bytes, 100);
    }

    {
        // Find a slightly higher offset but still in the same chunk.
        auto loc = index.find_chunk(
          model::ntp(
            model::ns("kafka"),
            model::topic("topic_5"),
            model::partition_id(0)),
          model::offset{110});

        ASSERT_TRUE(loc.has_value());
        ASSERT_EQ(loc->offset_bytes, 0);
        ASSERT_EQ(loc->size_bytes, 100);
    }

    {
        // Find a the offset in the next chunk.
        auto loc = index.find_chunk(
          model::ntp(
            model::ns("kafka"),
            model::topic("topic_5"),
            model::partition_id(0)),
          model::offset{500});

        ASSERT_TRUE(loc.has_value());
        ASSERT_EQ(loc->offset_bytes, 100);
        ASSERT_EQ(loc->size_bytes, 100);
    }

    {
        // Find the next NTP with a lower offset.
        auto loc = index.find_chunk(
          model::ntp(
            model::ns("kafka"),
            model::topic("topic_6"),
            model::partition_id(0)),
          model::offset{0});

        ASSERT_TRUE(loc.has_value());
        ASSERT_EQ(loc->offset_bytes, 100);
        ASSERT_EQ(loc->size_bytes, 100);
    }

    {
        // Find exact match.
        auto loc = index.find_chunk(
          model::ntp(
            model::ns("kafka"),
            model::topic("topic_6"),
            model::partition_id(0)),
          model::offset{50});

        ASSERT_TRUE(loc.has_value());
        ASSERT_EQ(loc->offset_bytes, 200);
        ASSERT_EQ(loc->size_bytes, 100);
    }

    {
        // Find the next offset within the same chunk.
        auto loc = index.find_chunk(
          model::ntp(
            model::ns("kafka"),
            model::topic("topic_6"),
            model::partition_id(0)),
          model::offset{100});

        ASSERT_TRUE(loc.has_value());
        ASSERT_EQ(loc->offset_bytes, 200);
        ASSERT_EQ(loc->size_bytes, 100);
    }

    {
        // This offset certainly doesn't exist.
        auto loc = index.find_chunk(
          model::ntp(
            model::ns("kafka"),
            model::topic("topic_6"),
            model::partition_id(0)),
          model::offset{101});

        ASSERT_FALSE(loc.has_value());
    }

    {
        // This NTP certainly doesn't exist.
        auto loc = index.find_chunk(
          model::ntp(
            model::ns("kafka"),
            model::topic("topic_7"),
            model::partition_id(0)),
          model::offset{0});

        ASSERT_FALSE(loc.has_value());
    }
}

TEST_F_CORO(jls_roundtrip_test, test_chunk_roundtrip) {
    jumbo_log::segment_writer::iobuf_chunk_writer writer{};

    model::ntp ntp1(
      model::ns("kafka"), model::topic("topic_5"), model::partition_id(0));

    auto rand_batches_ntp1 = co_await model::test::make_random_batches(
      model::offset(0), 10);

    // Add some extra batches to the same NTP with a gap.
    for (auto& batch : co_await model::test::make_random_batches(
           rand_batches_ntp1.back().last_offset() + model::offset(100), 10)) {
        rand_batches_ntp1.push_back(std::move(batch));
    }

    // Drop offset 0 to facilitate a later test.
    if (rand_batches_ntp1.front().base_offset() == model::offset(0)) {
        rand_batches_ntp1.pop_front();
    }

    for (auto& batch : rand_batches_ntp1) {
        writer.write(ntp1, batch.copy());
    }

    model::ntp ntp2(
      model::ns("kafka"), model::topic("topic_6"), model::partition_id(0));
    auto rand_batches_ntp2 = co_await model::test::make_random_batches(
      model::offset(0), 10);
    for (auto& batch : rand_batches_ntp2) {
        writer.write(ntp2, std::move(batch));
    }

    auto buf = writer.advance();

    {
        SCOPED_TRACE("All batch read.");
        jumbo_log::segment_reader::chunk_reader_from_iobuf reader(
          buf.copy(),
          ntp1,
          model::bounded_offset_interval::checked(
            model::offset(0), model::offset::max()));
        auto read_batches = co_await reader.read_some();

        ASSERT_EQ_CORO(read_batches.size(), rand_batches_ntp1.size());
    }

    {
        SCOPED_TRACE("None batches read.");
        jumbo_log::segment_reader::chunk_reader_from_iobuf reader(
          buf.copy(),
          ntp1,
          model::bounded_offset_interval::checked(
            model::offset::max(), model::offset::max()));
        auto read_batches = co_await reader.read_some();

        ASSERT_EQ_CORO(read_batches.size(), 0);
    }

    {
        SCOPED_TRACE("Missing batch read.");
        jumbo_log::segment_reader::chunk_reader_from_iobuf reader(
          buf.copy(),
          ntp1,
          model::bounded_offset_interval::checked(
            model::offset(0), model::offset(0)));
        auto read_batches = co_await reader.read_some();

        ASSERT_EQ_CORO(read_batches.size(), 0);
    }

    {
        SCOPED_TRACE("Single batch read using base offset.");
        jumbo_log::segment_reader::chunk_reader_from_iobuf reader(
          buf.copy(),
          ntp1,
          model::bounded_offset_interval::checked(
            rand_batches_ntp1.front().base_offset(),
            rand_batches_ntp1.front().base_offset()));
        auto read_batches = co_await reader.read_some();

        ASSERT_EQ_CORO(read_batches[0], rand_batches_ntp1.front());
    }

    {
        SCOPED_TRACE("Single batch read using last offset.");
        jumbo_log::segment_reader::chunk_reader_from_iobuf reader(
          buf.copy(),
          ntp1,
          model::bounded_offset_interval::checked(
            rand_batches_ntp1.front().last_offset(),
            rand_batches_ntp1.front().last_offset()));
        auto read_batches = co_await reader.read_some();

        ASSERT_EQ_CORO(read_batches[0], rand_batches_ntp1.front());
    }
}
