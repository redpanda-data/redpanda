/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/object.h"
#include "model/tests/random_batch.h"

#include <seastar/util/backtrace.hh>
#include <seastar/util/defer.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iterator>

using namespace experimental::cloud_topics::l1;

namespace {
struct batch_spec {
    kafka::offset base_offset;
    kafka::offset last_offset;
    model::timestamp max_timestamp;
};

chunked_vector<model::record_batch>
make_batches(const std::vector<batch_spec>& specs) {
    chunked_vector<model::record_batch> batches;
    for (const auto& spec : specs) {
        int count = static_cast<int>(spec.last_offset - spec.base_offset) + 1;
        std::vector<size_t> record_sizes;
        std::fill_n(std::back_inserter(record_sizes), count, 100);
        batches.push_back(
          model::test::make_random_batch(model::test::record_batch_spec{
            .offset = kafka::offset_cast(spec.base_offset),
            .count = count,
            .record_sizes = record_sizes,
            .timestamp = spec.max_timestamp,
            .all_records_have_same_timestamp = true,
          }));
    }
    return batches;
}

struct batches_by_ntp {
    model::ntp ntp;
    std::vector<batch_spec> batches;
};

std::pair<object_builder::object_info, iobuf> make_object(
  const std::vector<batches_by_ntp>& specs_by_ntp,
  object_builder::options opts = {}) {
    iobuf output;
    auto builder = object_builder::create(
      make_iobuf_ref_output_stream(output), opts);
    auto _ = ss::defer([&builder] { builder->close().get(); });
    for (const auto& [ntp, specs] : specs_by_ntp) {
        builder->start_partition(ntp).get();
        auto batches = make_batches(specs);
        for (auto& batch : batches) {
            builder->add_batch(std::move(batch)).get();
        }
    }
    auto object_info = builder->finish().get();
    return std::make_pair(std::move(object_info), std::move(output));
}

std::unique_ptr<object_reader> make_reader(iobuf& buf) {
    return object_reader::create(
      make_iobuf_input_stream(buf.share(0, buf.size_bytes())));
}

object_reader::result read_one_at(iobuf& buf, size_t offset) {
    if (offset == footer::npos) {
        ss::throw_with_backtrace<std::runtime_error>(
          "Cannot read at npos offset, this is an invalid offset.");
    }
    if (offset >= buf.size_bytes()) {
        ss::throw_with_backtrace<std::out_of_range>(fmt::format(
          "Offset {} is out of range for buffer size {}",
          offset,
          buf.size_bytes()));
    }
    auto reader = object_reader::create(
      make_iobuf_input_stream(buf.share(offset, buf.size_bytes() - offset)));
    auto _ = ss::defer([&reader] { reader->close().get(); });
    return reader->read_next().get();
}

kafka::offset operator""_o(unsigned long long o) {
    return kafka::offset{static_cast<int64_t>(o)};
}

model::timestamp operator""_t(unsigned long long t) {
    return model::timestamp{static_cast<int64_t>(t)};
}

} // namespace

TEST(L1ObjectsIndex, OffsetSearch) {
    footer index;
    index.partitions.emplace(
      model::ntp{"test_ns", "test_topic", model::partition_id(0)},
      footer::partition{
        .file_position = 0,
        .indexes = {
          {.file_position = 100, .kafka_offset = 5_o},
          {.file_position = 200, .kafka_offset = 20_o},
          {.file_position = 300, .kafka_offset = 30_o},
          {.file_position = 400, .kafka_offset = 50_o},
          {.file_position = 500, .kafka_offset = 60_o},
        },
        .first_offset = 3_o,
        .last_offset = 65_o,
      });
    std::map<kafka::offset, size_t> offset_to_filepos = {
      {2_o, 0},    {3_o, 0},    {4_o, 0},    {5_o, 100},           {6_o, 100},
      {19_o, 100}, {20_o, 200}, {21_o, 200}, {29_o, 200},          {30_o, 300},
      {31_o, 300}, {49_o, 300}, {50_o, 400}, {51_o, 400},          {59_o, 400},
      {60_o, 500}, {61_o, 500}, {65_o, 500}, {66_o, footer::npos},
    };
    for (const auto& [seek, expected] : offset_to_filepos) {
        EXPECT_EQ(
          index.file_position_before_kafka_offset(
            index.partitions.begin()->first, seek),
          expected)
          << " for offset " << seek;
    }
}

TEST(L1ObjectsIndex, TimestampSearch) {
    footer index;
    index.partitions.emplace(
       model::ntp{"test_ns", "test_topic", model::partition_id(0)},
    footer::partition{
      .file_position = 0,
      .indexes = {
        {.file_position = 100, .kafka_offset = 5_o, .max_timestamp = 1000_t},
        {.file_position = 200, .kafka_offset = 20_o, .max_timestamp = 1500_t},
        {.file_position = 300, .kafka_offset = 30_o, .max_timestamp = 2000_t},
        {.file_position = 400, .kafka_offset = 50_o, .max_timestamp = 2500_t},
        {.file_position = 500, .kafka_offset = 60_o, .max_timestamp = 2500_t},
      },
      .first_offset = 3_o,
      .last_offset = 65_o,
      .max_timestamp = 3000_t,
    });
    std::map<model::timestamp, size_t> timequery_to_file_position = {
      {999_t, 0},
      {1000_t, 0},
      {1001_t, 200},
      {1499_t, 200},
      {1500_t, 200},
      {1501_t, 300},
      {1999_t, 300},
      {2000_t, 300},
      {2001_t, 400},
      {2499_t, 400},
      {2500_t, 400},
      {2501_t, 500},
      {2999_t, 500},
      {3000_t, 500},
      {3001_t, footer::npos},
    };
    for (const auto& [seek, expected] : timequery_to_file_position) {
        EXPECT_EQ(
          index.file_position_before_max_timestamp(
            index.partitions.begin()->first, seek),
          expected)
          << " for timestamp " << seek;
    }
}

TEST(L1Objects, OffsetSearch) {
    auto specs_by_ntp = std::vector<batches_by_ntp>{
      {
        .ntp = model::ntp{"test_ns", "test_topic", model::partition_id(0)},
        .batches = {
          {.base_offset = 5_o, .last_offset = 9_o},
          {.base_offset = 10_o, .last_offset = 19_o},
          {.base_offset = 20_o, .last_offset = 29_o},
        },
      },
      {
        .ntp = model::ntp{"test_ns", "test_topic", model::partition_id(0)},
        .batches = {
          {.base_offset = 30_o, .last_offset = 39_o},
          {.base_offset = 40_o, .last_offset = 49_o},
          {.base_offset = 50_o, .last_offset = 59_o},
        },
      },
      {
        .ntp = model::ntp{"test_ns", "test_topic", model::partition_id(1)},
        .batches = {
          {.base_offset = 5_o, .last_offset = 9_o},
          {.base_offset = 10_o, .last_offset = 19_o},
          {.base_offset = 20_o, .last_offset = 49_o},
          {.base_offset = 60_o, .last_offset = 69_o},
          {.base_offset = 70_o, .last_offset = 79_o},
        },
      },
      {
        .ntp = model::ntp{"test_ns", "test_topic", model::partition_id(0)},
        .batches = {
          {.base_offset = 100_o, .last_offset = 109_o},
          {.base_offset = 110_o, .last_offset = 119_o},
          {.base_offset = 1200_o, .last_offset = 1290_o},
        },
      },
    };
    // All batches end up being indexed this way.
    auto [index_one, object_one] = make_object(
      specs_by_ntp, {.indexing_frequency = 1});

    model::ntp ntp{"test_ns", "test_topic", model::partition_id(0)};

    std::unordered_map<kafka::offset, kafka::offset>
      offset_lookup_to_batch_start = {
        {1_o, 5_o},
        {90_o, 100_o},
        {1199_o, 110_o},
        {115_o, 110_o},
        {1200_o, 1200_o},
        {1201_o, 1200_o},
      };

    for (const auto& [seek, expected] : offset_lookup_to_batch_start) {
        size_t pos = index_one.index.file_position_before_kafka_offset(
          ntp, seek);
        ASSERT_NE(pos, footer::npos) << "No position found for " << seek
                                     << " in partition " << ntp.tp.partition;
        auto result = read_one_at(object_one, pos);
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(result));
        ASSERT_EQ(
          std::get<model::record_batch>(result).base_offset(),
          kafka::offset_cast(expected))
          << "for offset " << seek << " in partition " << ntp.tp.partition;
    }
    EXPECT_EQ(
      footer::npos,
      index_one.index.file_position_before_kafka_offset(ntp, 9999_o));

    // Index only the middle batches in partition 1
    auto [index_two, object_two] = make_object(
      specs_by_ntp, {.indexing_frequency = 3_KiB});

    ntp.tp.partition = model::partition_id(1);

    // 20 and 60 are indexed.
    offset_lookup_to_batch_start = {
      {4_o, 5_o},
      {5_o, 5_o},
      {19_o, 5_o},
      {20_o, 20_o},
      {25_o, 20_o},
      {59_o, 20_o},
      {60_o, 60_o},
      {61_o, 60_o},
      {78_o, 60_o},
      {79_o, 60_o},
    };

    for (const auto& [seek, expected] : offset_lookup_to_batch_start) {
        auto result = read_one_at(
          object_two,
          index_two.index.file_position_before_kafka_offset(ntp, seek));
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(result));
        ASSERT_EQ(
          std::get<model::record_batch>(result).base_offset(),
          kafka::offset_cast(expected));
    }
    EXPECT_EQ(
      footer::npos,
      index_two.index.file_position_before_kafka_offset(ntp, 80_o));
}

TEST(L1Objects, TimestampSearch) {
    std::vector<batches_by_ntp> specs_by_ntp = {
      {
        .ntp = model::ntp{"test_ns", "test_topic", model::partition_id(0)},
        .batches = {
          {.base_offset = 30_o, .last_offset = 39_o, .max_timestamp = 1500_t},
          {.base_offset = 40_o, .last_offset = 49_o, .max_timestamp = 2000_t},
          {.base_offset = 50_o, .last_offset = 59_o, .max_timestamp = 2000_t},
        },
      },
      {
        .ntp = model::ntp{"test_ns", "test_topic", model::partition_id(1)},
        .batches = {
          {.base_offset = 0_o, .last_offset = 9_o, .max_timestamp = 1000_t},
          {.base_offset = 10_o, .last_offset = 19_o, .max_timestamp = 2000_t},
          {.base_offset = 20_o, .last_offset = 29_o, .max_timestamp = 3000_t},
        },
      },
      {
        .ntp = model::ntp{"test_ns", "test_topic", model::partition_id(1)},
        .batches = {
          {.base_offset = 40_o, .last_offset = 49_o, .max_timestamp = 4000_t},
          {.base_offset = 50_o, .last_offset = 59_o, .max_timestamp = 5000_t},
        },
      },
      {
        .ntp = model::ntp{"test_ns", "test_topic", model::partition_id(0)},
        .batches = {
          {.base_offset = 0_o, .last_offset = 9_o, .max_timestamp = 1000_t},
          {.base_offset = 10_o, .last_offset = 19_o, .max_timestamp = 1500_t},
          {.base_offset = 20_o, .last_offset = 29_o, .max_timestamp = 2500_t},
        },
      },
    };
    // Every batch is indexed, except the first.
    auto [index_one, object_one] = make_object(
      specs_by_ntp, {.indexing_frequency = 1});

    model::ntp ntp{"test_ns", "test_topic", model::partition_id(0)};

    std::map<model::timestamp, kafka::offset> timequery_to_batch_start = {
      {900_t, 0_o},
      {1000_t, 0_o},
      {1001_t, 0_o},
      {1500_t, 0_o},
      {1501_t, 20_o},
      {2200_t, 20_o},
      {2500_t, 20_o},
    };

    for (const auto& [seek, expected] : timequery_to_batch_start) {
        auto pos = index_one.index.file_position_before_max_timestamp(
          ntp, seek);
        ASSERT_NE(pos, footer::npos) << "No position found for " << seek
                                     << " in partition " << ntp.tp.partition;
        auto result = read_one_at(object_one, pos);
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(result));
        ASSERT_EQ(
          std::get<model::record_batch>(result).base_offset(),
          kafka::offset_cast(expected))
          << " for timestamp " << seek << " in partition " << ntp.tp.partition;
    }
    EXPECT_EQ(
      footer::npos,
      index_one.index.file_position_before_max_timestamp(ntp, 2501_t));

    ntp.tp.partition = model::partition_id(1);

    timequery_to_batch_start = {};

    for (const auto& [seek, expected] : timequery_to_batch_start) {
        auto pos = index_one.index.file_position_before_max_timestamp(
          ntp, seek);
        ASSERT_NE(pos, footer::npos) << "No position found for " << seek
                                     << " in partition " << ntp.tp.partition;
        auto result = read_one_at(object_one, pos);
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(result));
        ASSERT_EQ(
          std::get<model::record_batch>(result).base_offset(),
          kafka::offset_cast(expected))
          << " for timestamp " << seek << " in partition " << ntp.tp.partition;
    }
    EXPECT_EQ(
      footer::npos,
      index_one.index.file_position_before_max_timestamp(ntp, 5001_t));
}

namespace {

testing::AssertionResult expect_read_results(
  std::unique_ptr<object_reader> reader,
  const std::vector<batches_by_ntp>& expected,
  std::optional<std::reference_wrapper<footer>> object_footer,
  bool expect_ntp_markers = true) {
    auto _ = ss::defer([&reader] { reader->close().get(); });
    for (const auto& [ntp, specs] : expected) {
        if (expect_ntp_markers) {
            SCOPED_TRACE(fmt::format("reading: {}", ntp));
            object_reader::result partition;
            EXPECT_NO_THROW(partition = reader->read_next().get());
            if (!std::holds_alternative<model::ntp>(partition)) {
                return testing::AssertionFailure()
                       << "Expected partition for ntp: " << ntp << ", but got "
                       << partition.index();
            }
            if (std::get<model::ntp>(partition) != ntp) {
                return testing::AssertionFailure()
                       << "Expected partition for ntp: " << ntp
                       << ", but got: " << std::get<model::ntp>(partition);
            }
        }
        for (const auto& spec : specs) {
            SCOPED_TRACE(fmt::format("reading batch @{}", spec.base_offset));
            object_reader::result result;
            EXPECT_NO_THROW(result = reader->read_next().get());
            if (!std::holds_alternative<model::record_batch>(result)) {
                return testing::AssertionFailure()
                       << "Expected batch for offset range: "
                       << spec.base_offset << "-" << spec.last_offset
                       << ", but got " << result.index();
            }
            const auto& batch = std::get<model::record_batch>(result);
            if (
              batch.base_offset() != kafka::offset_cast(spec.base_offset)
              || batch.last_offset() != kafka::offset_cast(spec.last_offset)
              || batch.header().max_timestamp != spec.max_timestamp) {
                return testing::AssertionFailure()
                       << "Batch mismatch for offset range: "
                       << spec.base_offset << "-" << spec.last_offset
                       << ", expected: " << "base_offset: " << spec.base_offset
                       << ", last_offset: " << spec.last_offset
                       << ", max_timestamp: " << spec.max_timestamp
                       << ", but got: base_offset: " << batch.base_offset()
                       << ", last_offset: " << batch.last_offset()
                       << ", max_timestamp: " << batch.header().max_timestamp;
            }
        }
    }
    if (object_footer.has_value()) {
        SCOPED_TRACE(fmt::format("reading footer"));
        object_reader::result result;
        EXPECT_NO_THROW(result = reader->read_next().get());
        if (!std::holds_alternative<footer>(result)) {
            return testing::AssertionFailure()
                   << "Expected object index at the end, but got "
                   << result.index();
        }

        if (std::get<footer>(result) != object_footer.value()) {
            return testing::AssertionFailure()
                   << "Expected matching object index: "
                   << fmt::format("{}", object_footer.value().get())
                   << ", got: " << fmt::format("{}", std::get<footer>(result));
        }
    }
    SCOPED_TRACE(fmt::format("reading eof"));
    object_reader::result result;
    EXPECT_NO_THROW(result = reader->read_next().get());
    if (!std::holds_alternative<object_reader::eof>(result)) {
        return testing::AssertionFailure()
               << "Expected eof after the end, but got " << result.index();
    }
    return testing::AssertionSuccess();
}

} // namespace

TEST(L1Objects, FullScan) {
    std::vector<batches_by_ntp> specs_by_ntp = {
      {
        .ntp = model::ntp{"test_ns", "test_topic", model::partition_id(0)},
        .batches = {
          {
            .base_offset = 0_o,
            .last_offset = 10_o,
            .max_timestamp = model::timestamp{1000},
          },
          {
            .base_offset = 11_o,
            .last_offset = 20_o,
            .max_timestamp = model::timestamp{2000},
          },
          {
            .base_offset = 21_o,
            .last_offset = 30_o,
            .max_timestamp = model::timestamp{3000},
          },
        },
      },
      {
        .ntp = model::ntp{"test_ns", "test_topic", model::partition_id(1)},
        .batches = {
          {
            .base_offset = 99_o,
            .last_offset = 100_o,
            .max_timestamp = model::timestamp{1001},
          },
          {
            .base_offset = 101_o,
            .last_offset = 110_o,
            .max_timestamp = model::timestamp{2001},
          },
          {
            .base_offset = 120_o,
            .last_offset = 130_o,
            .max_timestamp = model::timestamp{3001},
          },
        },
      },
    };
    auto [info, object] = make_object(specs_by_ntp);
    EXPECT_EQ(info.size_bytes, object.size_bytes());
    std::variant<footer, size_t> read_footer_result;
    ASSERT_NO_THROW(
      read_footer_result = footer::read(
                             object.share(
                               info.footer_offset,
                               object.size_bytes() - info.footer_offset))
                             .get());
    ASSERT_TRUE(std::holds_alternative<footer>(read_footer_result));
    EXPECT_EQ(info.index, std::get<footer>(read_footer_result));
    for (size_t missing_len : std::to_array<size_t>(
           {1,
            10,
            object.size_bytes() - info.footer_offset - sizeof(uint32_t)})) {
        size_t offset = info.footer_offset + missing_len;
        ASSERT_NO_THROW(
          read_footer_result
          = footer::read(object.share(offset, object.size_bytes() - offset))
              .get());
        EXPECT_THAT(
          read_footer_result, testing::VariantWith<size_t>(missing_len));
    }
    EXPECT_TRUE(
      expect_read_results(make_reader(object), specs_by_ntp, info.index));
}

TEST(L1Objects, PartialScan) {
    std::vector<batches_by_ntp> specs_by_ntp = {
      {
        .ntp = model::ntp{"test_ns", "test_topic", model::partition_id(0)},
        .batches = {
          {
            .base_offset = 0_o,
            .last_offset = 10_o,
            .max_timestamp = model::timestamp{1000},
          },
          {
            .base_offset = 11_o,
            .last_offset = 20_o,
            .max_timestamp = model::timestamp{2000},
          },
          {
            .base_offset = 21_o,
            .last_offset = 30_o,
            .max_timestamp = model::timestamp{3000},
          },
        },
      },
      {
        .ntp = model::ntp{"test_ns", "test_topic", model::partition_id(1)},
        .batches = {
          {
            .base_offset = 99_o,
            .last_offset = 100_o,
            .max_timestamp = model::timestamp{1001},
          },
          {
            .base_offset = 101_o,
            .last_offset = 110_o,
            .max_timestamp = model::timestamp{2001},
          },
          {
            .base_offset = 120_o,
            .last_offset = 130_o,
            .max_timestamp = model::timestamp{3001},
          },
        },
      },
    };
    auto [info, object] = make_object(specs_by_ntp);
    for (const auto& spec : specs_by_ntp) {
        auto it = info.index.partitions.find(spec.ntp);
        ASSERT_NE(it, info.index.partitions.end());
        auto reader = object_reader::create(make_iobuf_input_stream(
          object.share(it->second.file_position, it->second.length)));
        EXPECT_TRUE(expect_read_results(
          std::move(reader),
          {spec},
          std::nullopt,
          /*expect_ntp_markers=*/false));
    }
}
