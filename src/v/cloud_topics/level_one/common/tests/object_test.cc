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
#include "model/fundamental.h"
#include "model/tests/random_batch.h"

#include <seastar/util/backtrace.hh>
#include <seastar/util/defer.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iterator>

using namespace cloud_topics::l1;

namespace {
struct batch_spec {
    kafka::offset base_offset;
    kafka::offset last_offset;
    model::timestamp max_timestamp;
};

model::record_batch make_batch(const batch_spec& spec) {
    int count = static_cast<int>(spec.last_offset - spec.base_offset) + 1;
    std::vector<size_t> record_sizes;
    std::fill_n(std::back_inserter(record_sizes), count, 100);
    return model::test::make_random_batch(
      model::test::record_batch_spec{
        .offset = kafka::offset_cast(spec.base_offset),
        .count = count,
        .record_sizes = record_sizes,
        .timestamp = spec.max_timestamp,
        .all_records_have_same_timestamp = true,
      });
}

chunked_vector<model::record_batch>
make_batches(const std::vector<batch_spec>& specs) {
    chunked_vector<model::record_batch> batches;
    batches.reserve(specs.size());
    for (const auto& spec : specs) {
        batches.push_back(make_batch(spec));
    }
    return batches;
}

struct batches_by_tidp {
    model::topic_id_partition tidp;
    std::vector<batch_spec> batches;
};

std::pair<object_builder::object_info, iobuf> make_object(
  const std::vector<batches_by_tidp>& specs_by_tidp,
  object_builder::options opts = {}) {
    iobuf output;
    auto builder = object_builder::create(
      make_iobuf_ref_output_stream(output), opts);
    auto _ = ss::defer([&builder] { builder->close().get(); });
    for (const auto& [tidp, specs] : specs_by_tidp) {
        builder->start_partition(tidp).get();
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

object_reader::result read_one_at(iobuf& buf, footer::seek_result result) {
    if (result == footer::npos) {
        ss::throw_with_backtrace<std::runtime_error>(
          "Cannot read at npos offset, this is an invalid offset.");
    }
    if (result.file_position >= buf.size_bytes()) {
        ss::throw_with_backtrace<std::out_of_range>(fmt::format(
          "result {} is out of range for buffer size {}",
          result,
          buf.size_bytes()));
    }
    auto reader = object_reader::create(
      make_iobuf_input_stream(buf.share(result.file_position, result.length)));
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
      model::topic_id_partition{model::topic_id(uuid_t::create()), model::partition_id(0)},
      footer::partition{
        .file_position = 0,
        .length = 600,
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
    std::map<kafka::offset, footer::seek_result> offset_to_filepos = {
      {2_o, {.file_position = 0, .length = 600}},
      {3_o, {.file_position = 0, .length = 600}},
      {4_o, {.file_position = 0, .length = 600}},
      {5_o, {.file_position = 100, .length = 500}},
      {6_o, {.file_position = 100, .length = 500}},
      {19_o, {.file_position = 100, .length = 500}},
      {20_o, {.file_position = 200, .length = 400}},
      {21_o, {.file_position = 200, .length = 400}},
      {29_o, {.file_position = 200, .length = 400}},
      {30_o, {.file_position = 300, .length = 300}},
      {31_o, {.file_position = 300, .length = 300}},
      {49_o, {.file_position = 300, .length = 300}},
      {50_o, {.file_position = 400, .length = 200}},
      {51_o, {.file_position = 400, .length = 200}},
      {59_o, {.file_position = 400, .length = 200}},
      {60_o, {.file_position = 500, .length = 100}},
      {61_o, {.file_position = 500, .length = 100}},
      {65_o, {.file_position = 500, .length = 100}},
      {66_o, footer::npos},
    };
    for (const auto& [seek, expected] : offset_to_filepos) {
        auto seek_result = index.file_position_before_kafka_offset(
          index.partitions.begin()->first, seek);
        EXPECT_EQ(seek_result, expected) << " for offset " << seek;
    }
}

TEST(L1ObjectsIndex, TimestampSearch) {
    footer index;
    index.partitions.emplace(
      model::topic_id_partition{model::topic_id(uuid_t::create()), model::partition_id(0)},
    footer::partition{
      .file_position = 0,
      .length = 600,
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
    std::map<model::timestamp, footer::seek_result> timequery_to_file_position
      = {
        {999_t, {.file_position = 0, .length = 600}},
        {1000_t, {.file_position = 0, .length = 600}},
        {1001_t, {.file_position = 200, .length = 400}},
        {1499_t, {.file_position = 200, .length = 400}},
        {1500_t, {.file_position = 200, .length = 400}},
        {1501_t, {.file_position = 300, .length = 300}},
        {1999_t, {.file_position = 300, .length = 300}},
        {2000_t, {.file_position = 300, .length = 300}},
        {2001_t, {.file_position = 400, .length = 200}},
        {2499_t, {.file_position = 400, .length = 200}},
        {2500_t, {.file_position = 400, .length = 200}},
        {2501_t, {.file_position = 500, .length = 100}},
        {2999_t, {.file_position = 500, .length = 100}},
        {3000_t, {.file_position = 500, .length = 100}},
        {3001_t, footer::npos},
      };
    for (const auto& [seek, expected] : timequery_to_file_position) {
        auto seek_result = index.file_position_before_max_timestamp(
          index.partitions.begin()->first, seek);
        EXPECT_EQ(seek_result, expected) << " for timestamp " << seek;
    }
}

TEST(L1Objects, OffsetSearch) {
    auto test_topic_id = model::topic_id(uuid_t::create());
    auto specs_by_tidp = std::vector<batches_by_tidp>{
      {
        .tidp = model::topic_id_partition{test_topic_id, model::partition_id(0)},
        .batches = {
          {.base_offset = 5_o, .last_offset = 9_o},
          {.base_offset = 10_o, .last_offset = 19_o},
          {.base_offset = 20_o, .last_offset = 29_o},
        },
      },
      {
        .tidp = model::topic_id_partition{test_topic_id, model::partition_id(0)},
        .batches = {
          {.base_offset = 30_o, .last_offset = 39_o},
          {.base_offset = 40_o, .last_offset = 49_o},
          {.base_offset = 50_o, .last_offset = 59_o},
        },
      },
      {
        .tidp = model::topic_id_partition{test_topic_id, model::partition_id(1)},
        .batches = {
          {.base_offset = 5_o, .last_offset = 9_o},
          {.base_offset = 10_o, .last_offset = 19_o},
          {.base_offset = 20_o, .last_offset = 49_o},
          {.base_offset = 60_o, .last_offset = 69_o},
          {.base_offset = 70_o, .last_offset = 79_o},
        },
      },
      {
        .tidp = model::topic_id_partition{test_topic_id, model::partition_id(0)},
        .batches = {
          {.base_offset = 100_o, .last_offset = 109_o},
          {.base_offset = 110_o, .last_offset = 119_o},
          {.base_offset = 1200_o, .last_offset = 1290_o},
        },
      },
    };
    // All batches end up being indexed this way.
    auto [index_one, object_one] = make_object(
      specs_by_tidp, {.indexing_interval = 1});

    model::topic_id_partition tidp{test_topic_id, model::partition_id(0)};

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
        auto pos = index_one.index.file_position_before_kafka_offset(
          tidp, seek);
        ASSERT_NE(pos, footer::npos) << "No position found for " << seek
                                     << " in partition " << tidp.partition;
        auto result = read_one_at(object_one, pos);
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(result));
        ASSERT_EQ(
          std::get<model::record_batch>(result).base_offset(),
          kafka::offset_cast(expected))
          << "for offset " << seek << " in partition " << tidp.partition;
    }
    EXPECT_EQ(
      footer::npos,
      index_one.index.file_position_before_kafka_offset(tidp, 9999_o));

    // Index only the middle batches in partition 1
    auto [index_two, object_two] = make_object(
      specs_by_tidp, {.indexing_interval = 3_KiB});

    tidp.partition = model::partition_id(1);

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
          index_two.index.file_position_before_kafka_offset(tidp, seek));
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(result));
        ASSERT_EQ(
          std::get<model::record_batch>(result).base_offset(),
          kafka::offset_cast(expected));
    }
    EXPECT_EQ(
      footer::npos,
      index_two.index.file_position_before_kafka_offset(tidp, 80_o));
}

TEST(L1Objects, TimestampSearch) {
    auto test_topic_id = model::topic_id(uuid_t::create());
    std::vector<batches_by_tidp> specs_by_tidp = {
      {
        .tidp = model::topic_id_partition(test_topic_id, model::partition_id(0)),
        .batches = {
          {.base_offset = 30_o, .last_offset = 39_o, .max_timestamp = 1500_t},
          {.base_offset = 40_o, .last_offset = 49_o, .max_timestamp = 2000_t},
          {.base_offset = 50_o, .last_offset = 59_o, .max_timestamp = 2000_t},
        },
      },
      {
        .tidp = model::topic_id_partition(test_topic_id, model::partition_id(1)),
        .batches = {
          {.base_offset = 0_o, .last_offset = 9_o, .max_timestamp = 1000_t},
          {.base_offset = 10_o, .last_offset = 19_o, .max_timestamp = 2000_t},
          {.base_offset = 20_o, .last_offset = 29_o, .max_timestamp = 3000_t},
        },
      },
      {
        .tidp = model::topic_id_partition(test_topic_id, model::partition_id(1)),
        .batches = {
          {.base_offset = 40_o, .last_offset = 49_o, .max_timestamp = 4000_t},
          {.base_offset = 50_o, .last_offset = 59_o, .max_timestamp = 5000_t},
        },
      },
      {
        .tidp = model::topic_id_partition(test_topic_id, model::partition_id(0)),
        .batches = {
          {.base_offset = 0_o, .last_offset = 9_o, .max_timestamp = 1000_t},
          {.base_offset = 10_o, .last_offset = 19_o, .max_timestamp = 1500_t},
          {.base_offset = 20_o, .last_offset = 29_o, .max_timestamp = 2500_t},
        },
      },
    };
    // Every batch is indexed, except the first.
    auto [index_one, object_one] = make_object(
      specs_by_tidp, {.indexing_interval = 1});

    auto tidp = model::topic_id_partition(
      test_topic_id, model::partition_id(0));

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
          tidp, seek);
        ASSERT_NE(pos, footer::npos) << "No position found for " << seek
                                     << " in partition " << tidp.partition;
        auto result = read_one_at(object_one, pos);
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(result));
        ASSERT_EQ(
          std::get<model::record_batch>(result).base_offset(),
          kafka::offset_cast(expected))
          << " for timestamp " << seek << " in partition " << tidp.partition;
    }
    EXPECT_EQ(
      footer::npos,
      index_one.index.file_position_before_max_timestamp(tidp, 2501_t));

    tidp.partition = model::partition_id(1);

    timequery_to_batch_start = {};

    for (const auto& [seek, expected] : timequery_to_batch_start) {
        auto pos = index_one.index.file_position_before_max_timestamp(
          tidp, seek);
        ASSERT_NE(pos, footer::npos) << "No position found for " << seek
                                     << " in partition " << tidp.partition;
        auto result = read_one_at(object_one, pos);
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(result));
        ASSERT_EQ(
          std::get<model::record_batch>(result).base_offset(),
          kafka::offset_cast(expected))
          << " for timestamp " << seek << " in partition " << tidp.partition;
    }
    EXPECT_EQ(
      footer::npos,
      index_one.index.file_position_before_max_timestamp(tidp, 5001_t));
}

namespace {

testing::AssertionResult expect_read_results(
  std::unique_ptr<object_reader> reader,
  const std::vector<batches_by_tidp>& expected,
  std::optional<std::reference_wrapper<footer>> object_footer,
  bool expect_ntp_markers = true) {
    auto _ = ss::defer([&reader] { reader->close().get(); });
    for (const auto& [tidp, specs] : expected) {
        if (expect_ntp_markers) {
            SCOPED_TRACE(fmt::format("reading: {}", tidp));
            object_reader::result partition;
            EXPECT_NO_THROW(partition = reader->read_next().get());
            if (!std::holds_alternative<model::topic_id_partition>(partition)) {
                return testing::AssertionFailure()
                       << "Expected partition for tidp: " << tidp
                       << ", but got " << partition.index();
            }
            if (std::get<model::topic_id_partition>(partition) != tidp) {
                return testing::AssertionFailure()
                       << "Expected partition for tidp: " << tidp
                       << ", but got: "
                       << std::get<model::topic_id_partition>(partition);
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
    auto test_topic_id = model::topic_id(uuid_t::create());
    std::vector<batches_by_tidp> specs_by_tidp = {
      {
        .tidp = model::topic_id_partition(test_topic_id, model::partition_id(0)),
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
        .tidp = model::topic_id_partition(test_topic_id, model::partition_id(1)),
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
    auto [info, object] = make_object(specs_by_tidp);
    EXPECT_EQ(info.index.partitions.size(), 2);
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
      expect_read_results(make_reader(object), specs_by_tidp, info.index));
}

TEST(L1Objects, PartialScan) {
    auto test_topic_id = model::topic_id(uuid_t::create());
    std::vector<batches_by_tidp> specs_by_tidp = {
      {
        .tidp = model::topic_id_partition(test_topic_id, model::partition_id(0)),
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
        .tidp = model::topic_id_partition(test_topic_id, model::partition_id(1)),
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
    auto [info, object] = make_object(specs_by_tidp);
    for (const auto& spec : specs_by_tidp) {
        auto it = info.index.partitions.find(spec.tidp);
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

TEST(L1Objects, BuilderSize) {
    auto test_topic_id = model::topic_id(uuid_t::create());
    iobuf output;
    auto builder = object_builder::create(
      make_iobuf_ref_output_stream(output), {});
    auto _ = ss::defer([&builder] { builder->close().get(); });
    EXPECT_EQ(builder->file_size(), 0);
    builder->start_partition({test_topic_id, model::partition_id{0}}).get();
    auto after_partition_size = builder->file_size();
    EXPECT_GT(after_partition_size, 0);
    builder
      ->add_batch(make_batch({
        .base_offset = 10_o,
        .last_offset = 15_o,
        .max_timestamp = 100_t,
      }))
      .get();
    auto after_batch_size = builder->file_size();
    EXPECT_GT(after_batch_size, after_partition_size);
    auto finished = builder->finish().get();
    auto final_size = builder->file_size();
    EXPECT_GT(final_size, after_batch_size);
    EXPECT_EQ(final_size, finished.size_bytes);
}

TEST(L1Objects, PeekThenReadNext) {
    auto tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), model::partition_id(0));
    std::vector<batches_by_tidp> specs = {
      {
        .tidp = tidp,
        .batches = {
          {.base_offset = 0_o, .last_offset = 5_o, .max_timestamp = model::timestamp{100}},
          {.base_offset = 6_o, .last_offset = 10_o, .max_timestamp = model::timestamp{200}},
        },
      },
    };
    auto [info, object] = make_object(specs);
    auto reader = make_reader(object);
    auto _ = ss::defer([&reader] { reader->close().get(); });

    // Peek at partition marker — returns tag, not the full tidp.
    auto p1 = reader->peek().get();
    ASSERT_TRUE(std::holds_alternative<object_reader::partition_tag>(p1));

    // Peek again — idempotent.
    auto p1_again = reader->peek().get();
    ASSERT_TRUE(std::holds_alternative<object_reader::partition_tag>(p1_again));

    // read_next consumes the partition marker data from the stream.
    auto r1 = reader->read_next().get();
    ASSERT_TRUE(std::holds_alternative<model::topic_id_partition>(r1));

    // Peek at first batch header.
    auto p2 = reader->peek().get();
    ASSERT_TRUE(std::holds_alternative<model::record_batch_header>(p2));
    auto& hdr = std::get<model::record_batch_header>(p2);
    EXPECT_EQ(hdr.base_offset, kafka::offset_cast(0_o));

    // read_next consumes the body and returns full batch.
    auto r2 = reader->read_next().get();
    ASSERT_TRUE(std::holds_alternative<model::record_batch>(r2));
    auto& batch = std::get<model::record_batch>(r2);
    EXPECT_EQ(batch.base_offset(), kafka::offset_cast(0_o));
    EXPECT_EQ(batch.last_offset(), kafka::offset_cast(5_o));

    // Read the second batch without peek.
    auto r3 = reader->read_next().get();
    ASSERT_TRUE(std::holds_alternative<model::record_batch>(r3));
    EXPECT_EQ(
      std::get<model::record_batch>(r3).base_offset(), kafka::offset_cast(6_o));

    // Peek at footer — returns tag, not the full footer.
    auto p4 = reader->peek().get();
    ASSERT_TRUE(std::holds_alternative<object_reader::footer_tag>(p4));

    // read_next reads the footer data from the stream.
    auto r4 = reader->read_next().get();
    ASSERT_TRUE(std::holds_alternative<footer>(r4));

    // Peek at eof.
    auto p5 = reader->peek().get();
    ASSERT_TRUE(std::holds_alternative<object_reader::eof>(p5));
}

namespace {

std::pair<object_builder::object_info, iobuf>
combine_objects_via_reader(std::span<iobuf> objects) {
    iobuf output_iobuf;
    auto builder = object_builder::create(
      make_iobuf_ref_output_stream(output_iobuf), {});
    for (const auto& obj : objects) {
        auto reader = object_reader::create(
          make_iobuf_input_stream(obj.copy()));
        while (true) {
            auto stop = ss::visit(
              reader->read_next().get(),
              [&builder](model::topic_id_partition tidp) {
                  builder->start_partition(tidp).get();
                  return false;
              },
              [&builder](model::record_batch batch) {
                  builder->add_batch(std::move(batch)).get();
                  return false;
              },
              [](const auto&) { return true; });
            if (stop) {
                break;
            }
        }
        reader->close().get();
    }
    auto info = builder->finish().get();
    builder->close().get();
    return std::make_pair(std::move(info), std::move(output_iobuf));
}

} // namespace

TEST(L1Objects, CanCombineObjects) {
    auto test_topic_id = model::topic_id(uuid_t::create());
    std::vector<batches_by_tidp> specs_by_tidp = {
      {
        .tidp = model::topic_id_partition(test_topic_id, model::partition_id(0)),
        .batches = {
          {.base_offset = 0_o, .last_offset = 10_o, .max_timestamp = model::timestamp{1000}},
          {.base_offset = 11_o, .last_offset = 20_o, .max_timestamp = model::timestamp{2000}},
          {.base_offset = 21_o, .last_offset = 30_o, .max_timestamp = model::timestamp{3000}},
        },
      },
      {
        .tidp = model::topic_id_partition(test_topic_id, model::partition_id(1)),
        .batches = {
          {.base_offset = 0_o, .last_offset = 10_o, .max_timestamp = model::timestamp{2000}},
          {.base_offset = 15_o, .last_offset = 20_o, .max_timestamp = model::timestamp{3000}},
          {.base_offset = 21_o, .last_offset = 30_o, .max_timestamp = model::timestamp{3000}},
        },
      },
      {
        .tidp = model::topic_id_partition(test_topic_id, model::partition_id(2)),
        .batches = {
          {.base_offset = 0_o, .last_offset = 10_o, .max_timestamp = model::timestamp{2000}},
          {.base_offset = 11_o, .last_offset = 30_o, .max_timestamp = model::timestamp{5000}},
        },
      },
      {
        .tidp = model::topic_id_partition(test_topic_id, model::partition_id(3)),
        .batches = {
          {.base_offset = 0_o, .last_offset = 5_o, .max_timestamp = model::timestamp{1050}},
          {.base_offset = 6_o, .last_offset = 20_o, .max_timestamp = model::timestamp{2050}},
          {.base_offset = 21_o, .last_offset = 30_o, .max_timestamp = model::timestamp{3050}},
        },
      },
    };
    auto [info1, object1] = make_object({specs_by_tidp.front()});
    auto [info2, object2] = make_object(
      {specs_by_tidp.at(1), specs_by_tidp.at(2)});
    auto [info3, object3] = make_object({specs_by_tidp.back()});
    auto all_objects = std::to_array({
      object1.copy(),
      object2.copy(),
      object3.copy(),
    });
    auto [info_all, object_all] = combine_objects_via_reader(all_objects);
    chunked_vector<combine_objects_parameters::input_object> inputs;
    inputs.emplace_back(
      make_iobuf_input_stream(std::move(object1)), std::move(info1));
    inputs.emplace_back(
      make_iobuf_input_stream(std::move(object2)), std::move(info2));
    inputs.emplace_back(
      make_iobuf_input_stream(std::move(object3)), std::move(info3));
    iobuf output;
    auto combined_info = combine_objects(
                           {
                             .inputs = std::move(inputs),
                             .output = make_iobuf_ref_output_stream(output),
                           })
                           .get();
    EXPECT_EQ(info_all.size_bytes, combined_info.size_bytes);
    EXPECT_EQ(info_all.footer_offset, combined_info.footer_offset);
    EXPECT_EQ(output.size_bytes(), combined_info.size_bytes);
    EXPECT_EQ(info_all.index, combined_info.index);
    // Look ma, byte for byte the same!
    EXPECT_EQ(object_all, output);
}
