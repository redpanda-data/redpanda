/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

// Conformance tests for the l1::object_index contract, run against every
// implementation.
//
// The contract (see object_handle.h) is that a seek returns the highest indexed
// file position such that all previous records have lower offsets or timestamps
// than the one requested. Additionally, the implementation may return nullopt
// if the offset or timestamp is known to be out of bounds.  Only the native
// implementation does this since the TS implementation doesn't have access to
// end bounds.
//
// Each test states a layout exactly -- the batches, and which of them an index
// entry points at -- and both implementations are built from that same
// statement, so the two formats are fed identical index contents.
//
// What each format stores at an entry does differ, and the fixtures reproduce
// that faithfully: a footer entry carries the running maximum timestamp up to
// and including its own batch, while a tiered-storage index entry carries only
// its own batch's maximum.

#include "cloud_storage/offset_index.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_handle.h"
#include "cloud_topics/level_one/common/ts_object.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>

using namespace cloud_topics::l1;

namespace {

kafka::offset operator""_o(unsigned long long o) {
    return kafka::offset{static_cast<int64_t>(o)};
}

model::timestamp operator""_t(unsigned long long t) {
    return model::timestamp{static_cast<int64_t>(t)};
}

model::topic_id_partition test_tidp(int32_t partition = 0) {
    static const model::topic_id id{
      uuid_t::from_string("deadbeef-0000-0000-0000-000000000000")};
    return model::topic_id_partition{id, model::partition_id(partition)};
}

/// One batch of a partition's data, and whether an index entry points at it.
///
/// Batch 0 is never marked: neither format emits an entry before any bytes have
/// accumulated, so a target below the first entry is what reaches it.
struct batch_spec {
    kafka::offset base;
    kafka::offset last;
    model::timestamp max_timestamp;
    size_t size{1024};
    bool indexed{false};
};

/// File position of each batch, laid out end to end from zero.
std::vector<size_t> positions_of(const std::vector<batch_spec>& specs) {
    std::vector<size_t> out;
    out.reserve(specs.size());
    size_t at = 0;
    for (const auto& spec : specs) {
        out.push_back(at);
        at += spec.size;
    }
    return out;
}

size_t total_size(const std::vector<batch_spec>& specs) {
    return std::accumulate(
      specs.begin(),
      specs.end(),
      size_t{0},
      [](size_t acc, const batch_spec& s) { return acc + s.size; });
}

/// An object_index over a stated layout, exposing the ground truth the contract
/// is written in terms of.
class index_under_test {
public:
    virtual ~index_under_test() = default;

    virtual const object_index& index() const = 0;
    virtual size_t position_of(size_t batch_ix) const = 0;
    virtual size_t data_begin() const = 0;
    virtual size_t data_end() const = 0;

    /// True if implementation returns std::nullopt on out of bounds seek
    virtual bool knows_end_bounds() const = 0;
};

/// l1_footer_index over a footer describing the stated layout. Entry timestamps
/// are running maxima, as object_builder writes them.
class native_index final : public index_under_test {
public:
    explicit native_index(const std::vector<batch_spec>& specs)
      : _positions(positions_of(specs))
      , _end(total_size(specs)) {
        footer::partition partition{
          .file_position = 0,
          .length = _end,
          .indexes = {},
          .first_offset = specs.front().base,
          .last_offset = specs.back().last,
          .max_timestamp = model::timestamp::missing(),
        };
        auto running = model::timestamp::missing();
        for (size_t i = 0; i < specs.size(); ++i) {
            running = std::max(running, specs[i].max_timestamp);
            if (specs[i].indexed) {
                partition.indexes.push_back(
                  footer::partition::index_entry{
                    .file_position = _positions[i],
                    .kafka_offset = specs[i].base,
                    .max_timestamp = running,
                  });
            }
        }
        partition.max_timestamp = running;
        footer f;
        f.partitions.emplace(test_tidp(), std::move(partition));
        _index = std::make_unique<l1_footer_index>(std::move(f));
    }

    const object_index& index() const override { return *_index; }
    size_t position_of(size_t ix) const override { return _positions.at(ix); }
    size_t data_begin() const override { return 0; }
    size_t data_end() const override { return _end; }
    bool knows_end_bounds() const override { return true; }

private:
    std::vector<size_t> _positions;
    size_t _end{0};
    std::unique_ptr<l1_footer_index> _index;
};

/// ts_segment_index over an offset_index describing the same layout. Entry
/// timestamps are the batch's own maximum, as tiered storage writes them.
class imported_index final : public index_under_test {
public:
    explicit imported_index(const std::vector<batch_spec>& specs)
      : _positions(positions_of(specs))
      , _end(total_size(specs)) {
        // find_timestamp refuses to search an index whose initial time is
        // missing -- a v1 index that recorded no timestamps -- so seed it with
        // the first batch's.
        cloud_storage::offset_index oi(
          model::offset{0},
          kafka::offset{0},
          0,
          static_cast<int64_t>(specs.front().size),
          specs.front().max_timestamp);
        for (size_t i = 0; i < specs.size(); ++i) {
            if (specs[i].indexed) {
                oi.add(
                  kafka::offset_cast(specs[i].base),
                  specs[i].base,
                  static_cast<int64_t>(_positions[i]),
                  specs[i].max_timestamp);
            }
        }
        // Kafka and log offsets coincide here, so the base delta is zero.
        _index = std::make_unique<ts_segment_index>(
          std::move(oi), model::offset_delta{0}, _end);
    }

    const object_index& index() const override { return *_index; }
    size_t position_of(size_t ix) const override { return _positions.at(ix); }
    size_t data_begin() const override { return 0; }
    size_t data_end() const override { return _end; }
    bool knows_end_bounds() const override { return false; }

private:
    std::vector<size_t> _positions;
    size_t _end{0};
    std::unique_ptr<ts_segment_index> _index;
};

/// The last batch an index entry points at, which is the highest position a
/// seek can resolve to.
size_t last_indexed_batch(const std::vector<batch_spec>& specs) {
    for (size_t i = specs.size(); i > 0; --i) {
        if (specs[i - 1].indexed) {
            return i - 1;
        }
    }
    return 0;
}

/// The position an offset seek is required to return: the highest indexed
/// position whose earlier records are all below the target, falling back to the
/// start of the data. An entry carries its batch's base offset, so every record
/// before it is below that base.
size_t required_offset_position(
  const std::vector<batch_spec>& specs, kafka::offset target) {
    auto positions = positions_of(specs);
    size_t required = 0;
    for (size_t i = 0; i < specs.size(); ++i) {
        if (specs[i].indexed && specs[i].base <= target) {
            required = positions[i];
        }
    }
    return required;
}

/// The position a timestamp seek is required to return.
///
/// The predicate is over the timestamps an entry accounts for *including* its
/// own batch, not just the records before it: a timestamp entry summarises a
/// range rather than a boundary, so when the summary first reaches the target
/// the crossing record may lie anywhere in that range, and the highest position
/// the index can prove safe is the entry before it.
size_t required_timestamp_position(
  const std::vector<batch_spec>& specs, model::timestamp target) {
    auto positions = positions_of(specs);
    size_t required = 0;
    auto covered = model::timestamp::missing();
    for (size_t i = 0; i < specs.size(); ++i) {
        covered = std::max(covered, specs[i].max_timestamp);
        if (specs[i].indexed && covered < target) {
            required = positions[i];
        }
    }
    return required;
}

// 16 batches of 10 offsets each, timestamps rising by 1000, with an entry every
// fourth batch so three batches sit in each unindexed gap.
//
// Timestamps rise on purpose, and nothing here covers a segment whose batch
// timestamps do not. The two formats genuinely differ there: a footer entry's
// running maximum still accounts for an earlier spike, while a tiered-storage
// entry records only its own batch's maximum and so cannot, which makes the
// imported seek resolve past a spike sitting in an unindexed gap. That is a
// property of the tiered-storage index rather than of this interface --
// cloud_storage's own remote_segment::maybe_get_offsets has no monotonicity
// guard either, whereas local storage carries index_state's
// batch_timestamps_are_monotonic flag and scans instead of seeking when it is
// false. Covering it here would pin an upstream limitation as though it were
// this contract.
std::vector<batch_spec> rising_spec() {
    std::vector<batch_spec> specs;
    specs.reserve(16);
    for (int i = 0; i < 16; ++i) {
        auto base = kafka::offset{i * 10};
        specs.push_back(
          batch_spec{
            .base = base,
            .last = base + 9_o,
            .max_timestamp = model::timestamp{1000 * (i + 1)},
            .indexed = i > 0 && i % 4 == 0,
          });
    }
    return specs;
}

} // namespace

template<typename Impl>
class ObjectIndexTest : public ::testing::Test {
public:
    index_under_test& build(std::vector<batch_spec> specs) {
        _specs = std::move(specs);
        _index = std::make_unique<Impl>(_specs);
        return *_index;
    }

    const std::vector<batch_spec>& specs() const { return _specs; }

private:
    std::vector<batch_spec> _specs;
    std::unique_ptr<index_under_test> _index;
};

using implementations = ::testing::Types<native_index, imported_index>;
TYPED_TEST_SUITE(ObjectIndexTest, implementations);

// Seeking an offset must not start after the batch that holds it, whether the
// target is that batch's first offset, an offset inside it, or its last.
TYPED_TEST(ObjectIndexTest, OffsetSeekLandsAtOrBeforeTheTargetsBatch) {
    auto& index = this->build(rising_spec());
    const auto& specs = this->specs();
    for (size_t i = 0; i < specs.size(); ++i) {
        for (auto target :
             {specs[i].base, specs[i].base + 5_o, specs[i].last}) {
            auto seek = index.index().seek_offset_le(test_tidp(), target);
            ASSERT_TRUE(seek.has_value()) << "for offset " << target;
            EXPECT_EQ(
              seek->file_position, required_offset_position(specs, target))
              << "seek for offset " << target << " starts at "
              << seek->file_position << "; batch " << i << " holding it is at "
              << index.position_of(i);
        }
    }
}

// The same property for timestamps, probing each batch's own timestamp and one
// between it and the previous batch's: both are first matched by batch i.
TYPED_TEST(ObjectIndexTest, TimestampSeekLandsAtOrBeforeTheMatchingBatch) {
    auto& index = this->build(rising_spec());
    const auto& specs = this->specs();
    for (size_t i = 0; i < specs.size(); ++i) {
        std::vector<model::timestamp> targets{specs[i].max_timestamp};
        if (i > 0) {
            targets.push_back(model::timestamp{specs[i].max_timestamp() - 500});
        }
        for (auto target : targets) {
            auto seek = index.index().seek_timestamp_le(test_tidp(), target);
            ASSERT_TRUE(seek.has_value()) << "for timestamp " << target;
            EXPECT_EQ(
              seek->file_position, required_timestamp_position(specs, target))
              << "seek for timestamp " << target << " starts at "
              << seek->file_position << "; batch " << i << " matching it is at "
              << index.position_of(i);
        }
    }
}

// A timestamp shared by several batches is first matched by the earliest of
// them, so the seek cannot resolve past that batch even when later entries
// carry the same timestamp.
TYPED_TEST(ObjectIndexTest, RepeatedTimestampLandsBeforeItsFirstOccurrence) {
    auto specs = rising_spec();
    for (size_t i = 8; i < specs.size(); ++i) {
        specs[i].max_timestamp = 9000_t;
    }
    auto& index = this->build(std::move(specs));

    auto seek = index.index().seek_timestamp_le(test_tidp(), 9000_t);
    ASSERT_TRUE(seek.has_value());
    EXPECT_EQ(
      seek->file_position, required_timestamp_position(this->specs(), 9000_t))
      << "seek starts at " << seek->file_position
      << "; the first batch carrying the timestamp is batch 8 at "
      << index.position_of(8);
    EXPECT_LE(seek->file_position, index.position_of(8));
}

// With no entries at all -- a partition smaller than the sampling interval --
// every reachable target resolves to the start of the data.
TYPED_TEST(ObjectIndexTest, UnindexedPartitionResolvesToTheStart) {
    auto specs = rising_spec();
    for (auto& spec : specs) {
        spec.indexed = false;
    }
    auto& index = this->build(std::move(specs));

    auto by_offset = index.index().seek_offset_le(test_tidp(), 100_o);
    ASSERT_TRUE(by_offset.has_value());
    EXPECT_EQ(by_offset->file_position, index.data_begin());

    auto by_time = index.index().seek_timestamp_le(test_tidp(), 9000_t);
    ASSERT_TRUE(by_time.has_value());
    EXPECT_EQ(by_time->file_position, index.data_begin());
}

// A target below everything in the partition is matched by its first batch, so
// the seek starts at the beginning of the data rather than missing.
TYPED_TEST(ObjectIndexTest, TargetsBelowAllDataStartAtTheBeginning) {
    auto& index = this->build(rising_spec());
    const auto& specs = this->specs();

    auto by_offset = index.index().seek_offset_le(
      test_tidp(), specs.front().base);
    ASSERT_TRUE(by_offset.has_value());
    EXPECT_EQ(by_offset->file_position, index.data_begin());

    auto by_time = index.index().seek_timestamp_le(
      test_tidp(), model::timestamp{specs.front().max_timestamp() - 1});
    ASSERT_TRUE(by_time.has_value());
    EXPECT_EQ(by_time->file_position, index.data_begin());
}

// The returned length is the readable remainder, so a caller handed a seek
// result can consume to the end of the partition's data and no further.
TYPED_TEST(ObjectIndexTest, SeekLengthCoversTheRestOfTheData) {
    auto& index = this->build(rising_spec());
    const auto& specs = this->specs();

    auto by_offset = index.index().seek_offset_le(
      test_tidp(), specs.back().base);
    ASSERT_TRUE(by_offset.has_value());
    EXPECT_EQ(by_offset->file_position + by_offset->length, index.data_end());

    auto by_time = index.index().seek_timestamp_le(
      test_tidp(), specs.back().max_timestamp);
    ASSERT_TRUE(by_time.has_value());
    EXPECT_EQ(by_time->file_position + by_time->length, index.data_end());
}

// No record matches a target past the end of the partition's data. An
// implementation that knows the partition's bounds reports the miss; one that
// does not returns its last entry -- every record before it is below the
// target.
TYPED_TEST(ObjectIndexTest, OffsetAboveAllDataHasNoMatch) {
    auto& index = this->build(rising_spec());
    const auto& specs = this->specs();
    auto target = specs.back().last + 1_o;
    auto seek = index.index().seek_offset_le(test_tidp(), target);
    if (index.knows_end_bounds()) {
        EXPECT_FALSE(seek.has_value())
          << "the partition's last offset is " << specs.back().last
          << ", so a seek for " << target
          << " must report the miss rather than resolve to "
          << seek->file_position;
    } else {
        ASSERT_TRUE(seek.has_value());
        EXPECT_EQ(
          seek->file_position, index.position_of(last_indexed_batch(specs)));
    }
}

TYPED_TEST(ObjectIndexTest, TimestampAboveAllDataHasNoMatch) {
    auto& index = this->build(rising_spec());
    const auto& specs = this->specs();
    auto target = model::timestamp{specs.back().max_timestamp() + 1};
    auto seek = index.index().seek_timestamp_le(test_tidp(), target);
    if (index.knows_end_bounds()) {
        EXPECT_FALSE(seek.has_value())
          << "the partition's max timestamp is " << specs.back().max_timestamp
          << ", so a seek for " << target
          << " must report the miss rather than resolve to "
          << seek->file_position;
    } else {
        ASSERT_TRUE(seek.has_value());
        EXPECT_EQ(
          seek->file_position, index.position_of(last_indexed_batch(specs)));
    }
}
