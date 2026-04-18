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
#include "cloud_topics/reconciler/reconciler_probe.h"
#include "cloud_topics/reconciler/reconciliation_consumer.h"
#include "encryption/dek_dedup.h"
#include "model/record_batch_reader.h"
#include "storage/record_batch_builder.h"
#include "test_utils/random_bytes.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

const auto build_from_reader = cloud_topics::reconciler::build_from_reader;
using namespace cloud_topics::l1;

model::record_batch_reader make_reader(
  int offset, int record_size, int num_batches, int records_per_batch) {
    ss::chunked_fifo<model::record_batch> batches;
    for (int i = 0; i < num_batches; i++) {
        storage::record_batch_builder b(
          model::record_batch_type::raft_data, model::offset(offset));
        for (int j = 0; j < records_per_batch; j++) {
            b.add_raw_kv(tests::random_iobuf(record_size), iobuf());
            offset += 1;
        }
        batches.push_back(std::move(b).build());
    }
    return model::make_chunked_memory_record_batch_reader(std::move(batches));
}

TEST(ReconciliationConsumerTest, EmptyReader) {
    cloud_topics::reconciler::reconciler_probe probe;
    // Test that reading no batches produces no metadata.
    auto reader = model::make_empty_record_batch_reader();
    iobuf output;
    auto builder = object_builder::create(
      make_iobuf_ref_output_stream(output), object_builder::options{});
    auto _ = ss::defer([&builder] { builder->close().get(); });

    model::topic_id_partition tidp{
      model::topic_id(uuid_t::create()), model::partition_id(0)};
    encryption::seen_dek_set seen_deks;
    auto metadata = build_from_reader(
                      tidp, std::move(reader), builder.get(), &probe, seen_deks)
                      .get();
    ASSERT_FALSE(metadata.has_value());
}

TEST(ReconciliationConsumerTest, BuildObject) {
    // Test building an object from multiple consumers.
    iobuf output;
    auto builder = object_builder::create(
      make_iobuf_ref_output_stream(output), object_builder::options{});
    auto _ = ss::defer([&builder] { builder->close().get(); });

    cloud_topics::reconciler::reconciler_probe probe;
    auto topic1 = model::topic_id(uuid_t::create());
    auto topic2 = model::topic_id(uuid_t::create());
    model::topic_id_partition tidp1{topic1, model::partition_id(0)};
    model::topic_id_partition tidp2{topic1, model::partition_id(1)};
    model::topic_id_partition tidp3{topic2, model::partition_id(0)};

    // Consumer 1: Partition 0 of topic1, offset range 100-109.
    {
        encryption::seen_dek_set seen_deks;
        auto reader1 = make_reader(100, 50, 2, 5);
        auto metadata1
          = build_from_reader(
              tidp1, std::move(reader1), builder.get(), &probe, seen_deks)
              .get();
        ASSERT_TRUE(metadata1.has_value());
        ASSERT_EQ(metadata1->base_offset(), 100);
        ASSERT_EQ(metadata1->last_offset(), 109);
    }

    // Consumer 2: Partition 1 of topic1, offset range 200-204.
    {
        encryption::seen_dek_set seen_deks;
        auto reader2 = make_reader(200, 75, 1, 5);
        auto metadata2
          = build_from_reader(
              tidp2, std::move(reader2), builder.get(), &probe, seen_deks)
              .get();
        ASSERT_TRUE(metadata2.has_value());
        ASSERT_EQ(metadata2->base_offset(), 200);
        ASSERT_EQ(metadata2->last_offset(), 204);
    }

    // Consumer 3: Partition 0 of topic2, offset range 300-311.
    {
        encryption::seen_dek_set seen_deks;
        auto reader3 = make_reader(300, 25, 3, 4);
        auto metadata3
          = build_from_reader(
              tidp3, std::move(reader3), builder.get(), &probe, seen_deks)
              .get();
        ASSERT_TRUE(metadata3.has_value());
        ASSERT_EQ(metadata3->base_offset(), 300);
        ASSERT_EQ(metadata3->last_offset(), 311);
    }

    auto info = builder->finish().get();
    ASSERT_EQ(info.index.partitions.size(), 3);
    ASSERT_TRUE(info.index.partitions.contains(tidp1));
    ASSERT_TRUE(info.index.partitions.contains(tidp2));
    ASSERT_TRUE(info.index.partitions.contains(tidp3));

    auto p1_it = info.index.partitions.find(tidp1);
    ASSERT_NE(p1_it, info.index.partitions.end());
    ASSERT_EQ(p1_it->second.first_offset, kafka::offset(100));
    ASSERT_EQ(p1_it->second.last_offset, kafka::offset(109));

    auto p2_it = info.index.partitions.find(tidp2);
    ASSERT_NE(p2_it, info.index.partitions.end());
    ASSERT_EQ(p2_it->second.first_offset, kafka::offset(200));
    ASSERT_EQ(p2_it->second.last_offset, kafka::offset(204));

    auto p3_it = info.index.partitions.find(tidp3);
    ASSERT_NE(p3_it, info.index.partitions.end());
    ASSERT_EQ(p3_it->second.first_offset, kafka::offset(300));
    ASSERT_EQ(p3_it->second.last_offset, kafka::offset(311));
}
