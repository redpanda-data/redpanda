/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf_parser.h"
#include "cloud_topics/level_zero/common/object.h"
#include "model/fundamental.h"
#include "serde/rw/rw.h"
#include "test_utils/test.h"

#include <seastar/core/smp.hh>

#include <gtest/gtest.h>

using namespace cloud_topics::l0;

namespace {

model::ntp make_ntp(int partition) {
    return model::ntp(
      model::ns("kafka"),
      model::topic("test-topic"),
      model::partition_id(partition));
}

// Helper to serialize a footer and append size suffix (matching aggregator
// behavior)
iobuf serialize_footer_with_size(const footer& f) {
    iobuf result;
    iobuf footer_buf = serde::to_iobuf(f.copy());
    auto footer_size = static_cast<uint32_t>(footer_buf.size_bytes());
    result.append(std::move(footer_buf));
    auto footer_size_le = ss::cpu_to_le(footer_size);
    result.append(
      reinterpret_cast<const char*>(&footer_size_le), sizeof(footer_size_le));
    return result;
}

} // namespace

TEST(L0FooterTest, EmptyFooter) {
    footer f;
    EXPECT_TRUE(f.partitions.empty());

    // Serialize and deserialize
    iobuf buf = serde::to_iobuf(f.copy());
    auto deserialized = serde::from_iobuf<footer>(std::move(buf));

    EXPECT_TRUE(deserialized.partitions.empty());
    EXPECT_EQ(f, deserialized);
}

TEST(L0FooterTest, SinglePartition) {
    footer f;
    f.partitions[make_ntp(0)] = {
      .file_position = 0,
      .length = 1024,
    };

    // Serialize and deserialize
    iobuf buf = serde::to_iobuf(f.copy());
    auto deserialized = serde::from_iobuf<footer>(std::move(buf));

    EXPECT_EQ(deserialized.partitions.size(), 1);
    EXPECT_EQ(f, deserialized);

    auto it = deserialized.partitions.find(make_ntp(0));
    EXPECT_TRUE(it != deserialized.partitions.end());
    if (it != deserialized.partitions.end()) {
        EXPECT_EQ(it->second.file_position, 0);
        EXPECT_EQ(it->second.length, 1024);
    }
}

TEST(L0FooterTest, MultiplePartitions) {
    footer f;
    f.partitions[make_ntp(0)] = {.file_position = 0, .length = 1024};
    f.partitions[make_ntp(1)] = {.file_position = 1024, .length = 2048};
    f.partitions[make_ntp(2)] = {.file_position = 3072, .length = 512};

    // Serialize and deserialize
    iobuf buf = serde::to_iobuf(f.copy());
    auto deserialized = serde::from_iobuf<footer>(std::move(buf));

    EXPECT_EQ(deserialized.partitions.size(), 3);
    EXPECT_EQ(f, deserialized);

    // Verify each partition
    auto it0 = deserialized.partitions.find(make_ntp(0));
    EXPECT_TRUE(it0 != deserialized.partitions.end());
    if (it0 != deserialized.partitions.end()) {
        EXPECT_EQ(it0->second.file_position, 0);
        EXPECT_EQ(it0->second.length, 1024);
    }

    auto it1 = deserialized.partitions.find(make_ntp(1));
    EXPECT_TRUE(it1 != deserialized.partitions.end());
    if (it1 != deserialized.partitions.end()) {
        EXPECT_EQ(it1->second.file_position, 1024);
        EXPECT_EQ(it1->second.length, 2048);
    }

    auto it2 = deserialized.partitions.find(make_ntp(2));
    EXPECT_TRUE(it2 != deserialized.partitions.end());
    if (it2 != deserialized.partitions.end()) {
        EXPECT_EQ(it2->second.file_position, 3072);
        EXPECT_EQ(it2->second.length, 512);
    }
}

TEST(L0FooterTest, CopyMethod) {
    footer f;
    f.partitions[make_ntp(0)] = {.file_position = 100, .length = 200};
    f.partitions[make_ntp(1)] = {.file_position = 300, .length = 400};

    auto copy = f.copy();

    EXPECT_EQ(f, copy);
    EXPECT_EQ(copy.partitions.size(), 2);
}

TEST(L0FooterTest, ReadFromTail_CompleteFooter) {
    footer f;
    f.partitions[make_ntp(0)] = {.file_position = 0, .length = 1024};
    f.partitions[make_ntp(1)] = {.file_position = 1024, .length = 2048};

    // Serialize with size suffix
    auto buf = serialize_footer_with_size(f);

    // Read from tail - should succeed with complete data
    auto result = footer::read(buf.copy());

    EXPECT_TRUE(std::holds_alternative<footer>(result));
    if (std::holds_alternative<footer>(result)) {
        auto& read_footer = std::get<footer>(result);
        EXPECT_EQ(read_footer.partitions.size(), 2);
        EXPECT_EQ(f, read_footer);
    }
}

TEST(L0FooterTest, ReadFromTail_PartialFooter) {
    footer f;
    f.partitions[make_ntp(0)] = {.file_position = 0, .length = 1024};

    // Serialize with size suffix
    auto buf = serialize_footer_with_size(f);
    auto full_size = buf.size_bytes();

    // Only provide the last 4 bytes (the size field)
    auto partial = buf.share(full_size - sizeof(uint32_t), sizeof(uint32_t));

    auto result = footer::read(std::move(partial));

    // Should return the number of additional bytes needed
    EXPECT_TRUE(std::holds_alternative<size_t>(result));
    if (std::holds_alternative<size_t>(result)) {
        auto bytes_needed = std::get<size_t>(result);
        EXPECT_GT(bytes_needed, 0);
        EXPECT_EQ(bytes_needed, full_size - sizeof(uint32_t));
    }
}

TEST(L0FooterTest, ReadFromTail_BufferTooSmall) {
    // Buffer smaller than 4 bytes should throw
    iobuf tiny_buf;
    tiny_buf.append("ab", 2);

    bool threw = false;
    try {
        footer::read(std::move(tiny_buf));
    } catch (const std::runtime_error& e) {
        threw = true;
    }
    EXPECT_TRUE(threw);
}

TEST(L0FooterTest, PartitionInfoEquality) {
    footer::partition_info a{.file_position = 100, .length = 200};
    footer::partition_info b{.file_position = 100, .length = 200};
    footer::partition_info c{.file_position = 100, .length = 300};

    EXPECT_EQ(a, b);
    EXPECT_NE(a, c);
}

TEST(L0FooterTest, DifferentTopicIds) {
    footer f;

    // Create NTPs with different namespaces
    model::ntp ntp1(
      model::ns("kafka"),
      model::topic("test"),
      model::partition_id(0));
    model::ntp ntp2(
      model::ns("internal"),
      model::topic("test"),
      model::partition_id(0));

    f.partitions[ntp1] = {.file_position = 0, .length = 100};
    f.partitions[ntp2] = {.file_position = 100, .length = 200};

    // Serialize and deserialize
    iobuf buf = serde::to_iobuf(f.copy());
    auto deserialized = serde::from_iobuf<footer>(std::move(buf));

    EXPECT_EQ(deserialized.partitions.size(), 2);
    EXPECT_TRUE(
      deserialized.partitions.find(ntp1) != deserialized.partitions.end());
    EXPECT_TRUE(
      deserialized.partitions.find(ntp2) != deserialized.partitions.end());
}

TEST(L0FooterTest, LargePositionValues) {
    footer f;
    // Test with large position values that might cause issues with incorrect
    // serialization
    f.partitions[make_ntp(0)] = {
      .file_position = std::numeric_limits<size_t>::max() / 2,
      .length = std::numeric_limits<size_t>::max() / 4,
    };

    // Serialize and deserialize
    iobuf buf = serde::to_iobuf(f.copy());
    auto deserialized = serde::from_iobuf<footer>(std::move(buf));

    EXPECT_EQ(f, deserialized);
    auto it = deserialized.partitions.find(make_ntp(0));
    EXPECT_TRUE(it != deserialized.partitions.end());
    if (it != deserialized.partitions.end()) {
        EXPECT_EQ(
          it->second.file_position, std::numeric_limits<size_t>::max() / 2);
        EXPECT_EQ(it->second.length, std::numeric_limits<size_t>::max() / 4);
    }
}
