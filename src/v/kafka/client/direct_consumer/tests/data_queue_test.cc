// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/direct_consumer/data_queue.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

using namespace kafka::client;

static constexpr size_t max_bytes = 1024;
static constexpr size_t max_count = 10;

chunked_vector<fetched_topic_data>
make_data(int topic_count, int bytes_per_topic) {
    chunked_vector<fetched_topic_data> data;
    data.reserve(topic_count);
    for (auto d = 0; d < topic_count; ++d) {
        fetched_topic_data topic_data;
        topic_data.topic = model::topic(fmt::format("topic-{}", d));
        topic_data.total_bytes = bytes_per_topic;
        data.push_back(std::move(topic_data));
    }
    return data;
}

TEST(DataQueueTest, TestIfCanInsertWorks) {
    data_queue queue(max_bytes, max_count);
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);
    ASSERT_TRUE(queue.can_insert(50));
    ASSERT_TRUE(queue.can_insert(20));
    ASSERT_TRUE(queue.can_insert(300));
    // queue is empty, so we can insert a single entry that exceeds the size
    // limit
    ASSERT_TRUE(queue.can_insert(2048));
    auto f = queue.push(make_data(1, 512), 1 * 512);
    // future should be ready immediately
    ASSERT_TRUE(f.available());
    ASSERT_EQ(queue.size(), 1);
    ASSERT_EQ(queue.current_bytes(), 512);
    ASSERT_TRUE(queue.can_insert(512));
    ASSERT_FALSE(queue.can_insert(513));
    ASSERT_FALSE(queue.can_insert(2049));
}

TEST(DataQueueTest, TestPushPop) {
    data_queue queue(max_bytes, max_count);
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);

    queue.push(make_data(1, 512), 512).get();
    ASSERT_EQ(queue.size(), 1);
    ASSERT_EQ(queue.current_bytes(), 512);

    queue.push(make_data(4, 64), 256).get();
    ASSERT_EQ(queue.size(), 2);
    ASSERT_EQ(queue.current_bytes(), 768);

    auto fetches = queue.pop(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(queue.size(), 1);
    ASSERT_EQ(queue.current_bytes(), 256);

    auto fetches_2 = queue.pop(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);

    ASSERT_EQ(fetches.value().size(), 1);
    ASSERT_EQ(fetches.value()[0].total_bytes, 512);
    ASSERT_EQ(fetches_2.value().size(), 4);
}

TEST(DataQueueTest, TestPushError) {
    data_queue queue(max_bytes, max_count);
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);

    queue.push_error(kafka::error_code::broker_not_available).get();
    ASSERT_EQ(queue.size(), 1);
    ASSERT_EQ(queue.current_bytes(), 0);

    auto fetches = queue.pop(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);

    ASSERT_TRUE(fetches.has_error());
    ASSERT_EQ(fetches.error(), kafka::error_code::broker_not_available);
}

TEST(DataQueueTest, TestPopTimeout) {
    data_queue queue(max_bytes, max_count);

    ASSERT_THROW(
      queue.pop(std::chrono::milliseconds(10)).get(),
      ss::condition_variable_timed_out);
}

TEST(DataQueueTest, TestMaxCountLimit) {
    data_queue queue(max_bytes, max_count);
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);

    for (size_t i = 0; i < max_count; ++i) {
        queue.push(make_data(1, 10), 10).get();
    }
    ASSERT_EQ(queue.size(), max_count);
    ASSERT_EQ(queue.current_bytes(), max_count * 10);

    ASSERT_FALSE(queue.can_insert(10));

    auto fetches = queue.pop(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(fetches.value().size(), 1);
    ASSERT_EQ(queue.size(), max_count - 1);
    ASSERT_EQ(queue.current_bytes(), (max_count - 1) * 10);

    ASSERT_TRUE(queue.can_insert(10));
}

TEST(DataQueueTest, TestConfigurationSetters) {
    data_queue queue(max_bytes, max_count);
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);

    queue.push(make_data(1, 500), 500).get();
    ASSERT_EQ(queue.size(), 1);
    ASSERT_EQ(queue.current_bytes(), 500);
    ASSERT_TRUE(queue.can_insert(524));
    ASSERT_FALSE(queue.can_insert(525));

    queue.set_max_bytes(2048);
    ASSERT_EQ(queue.current_bytes(), 500);
    ASSERT_TRUE(queue.can_insert(1548));
    ASSERT_FALSE(queue.can_insert(1549));

    queue.set_max_count(5);
    for (size_t i = 1; i < 5; ++i) {
        queue.push(make_data(1, 10), 10).get();
    }
    ASSERT_EQ(queue.size(), 5);
    ASSERT_EQ(queue.current_bytes(), 500 + 4 * 10);
    ASSERT_FALSE(queue.can_insert(10));

    queue.set_max_count(20);
    ASSERT_EQ(queue.current_bytes(), 500 + 4 * 10);
    ASSERT_TRUE(queue.can_insert(10));
}

TEST(DataQueueTest, TestBlockingPushWhenFull) {
    data_queue queue(max_bytes, max_count);
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);

    for (size_t i = 0; i < max_count; ++i) {
        queue.push(make_data(1, 50), 50).get();
    }
    ASSERT_EQ(queue.size(), max_count);
    ASSERT_EQ(queue.current_bytes(), max_count * 50);

    ASSERT_FALSE(queue.can_insert(50));

    auto push_future = queue.push(make_data(1, 50), 50);
    ASSERT_FALSE(push_future.available());
    ASSERT_EQ(queue.size(), max_count);
    ASSERT_EQ(queue.current_bytes(), max_count * 50);

    queue.pop(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(queue.size(), max_count - 1);
    ASSERT_EQ(queue.current_bytes(), (max_count - 1) * 50);

    push_future.get();
    ASSERT_EQ(queue.size(), max_count);
    ASSERT_EQ(queue.current_bytes(), max_count * 50);
}

TEST(DataQueueTest, TestEdgeCases) {
    data_queue queue(max_bytes, max_count);
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);

    ASSERT_TRUE(queue.can_insert(2048));
    queue.push(make_data(1, 2048), 2048).get();
    ASSERT_EQ(queue.size(), 1);
    ASSERT_EQ(queue.current_bytes(), 2048);
    ASSERT_FALSE(queue.can_insert(1));

    auto fetches = queue.pop(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(fetches.value().size(), 1);
    ASSERT_EQ(fetches.value()[0].total_bytes, 2048);
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);

    ASSERT_TRUE(queue.can_insert(1024));
}

TEST(DataQueueTest, TestSizeAndCurrentBytesTracking) {
    data_queue queue(max_bytes, max_count);

    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);

    queue.push(make_data(2, 100), 200).get();
    ASSERT_EQ(queue.size(), 1);
    ASSERT_EQ(queue.current_bytes(), 200);

    queue.push(make_data(1, 300), 300).get();
    ASSERT_EQ(queue.size(), 2);
    ASSERT_EQ(queue.current_bytes(), 500);

    queue.push_error(kafka::error_code::request_timed_out).get();
    ASSERT_EQ(queue.size(), 3);
    ASSERT_EQ(queue.current_bytes(), 500);

    queue.pop(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(queue.size(), 2);
    ASSERT_EQ(queue.current_bytes(), 300);

    queue.pop(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(queue.size(), 1);
    ASSERT_EQ(queue.current_bytes(), 0);

    queue.pop(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.current_bytes(), 0);
}
