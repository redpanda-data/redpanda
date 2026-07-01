/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/prefetch/chunk_reassembler.h"
#include "model/record.h"
#include "model/tests/random_batch.h"

#include <gtest/gtest.h>

namespace cloud_topics::prefetch {
namespace {

// A non-default topic_id_partition for use in tests (object_builder refuses
// the default-constructed value as a partition sentinel).
model::topic_id_partition test_tidp() {
    static const model::topic_id_partition tidp{
      model::topic_id::create(), model::partition_id{0}};
    return tidp;
}

// Serialize a sequence of batches into one contiguous run buffer (L1 format).
// The run is the partition data section of an L1 object: a sequence of
// batch entries each encoded as [data_type byte][L1 header][body].
iobuf serialize_run(
  const chunked_circular_buffer<model::record_batch>& batches) {
    iobuf obj_buf;
    auto builder = l1::object_builder::create(
      make_iobuf_ref_output_stream(obj_buf), {});
    builder->start_partition(test_tidp()).get();
    for (const auto& b : batches) {
        builder->add_batch(b.copy()).get();
    }
    auto info = builder->finish().get();
    builder->close().get();

    const auto& part = info.index.partitions.begin()->second;
    return obj_buf.share(part.file_position, part.length);
}

} // namespace

TEST(chunk_reassembler, reconstructs_across_arbitrary_splits) {
    auto batches = model::test::make_random_batches(model::offset(0), 20).get();
    auto run = serialize_run(batches);

    for (size_t chunk = 1; chunk <= run.size_bytes(); chunk += 7) {
        chunk_reassembler r;
        chunked_vector<model::record_batch> got;
        iobuf src = run.copy();
        while (src.size_bytes() > 0) {
            auto take = std::min(chunk, src.size_bytes());
            iobuf piece;
            piece.append(src.share(0, take));
            src.trim_front(take);
            for (auto& b : r.feed(std::move(piece))) {
                got.push_back(std::move(b));
            }
        }
        ASSERT_TRUE(r.empty()) << "slack at run end with chunk=" << chunk;
        ASSERT_EQ(got.size(), batches.size()) << "chunk=" << chunk;
        for (size_t i = 0; i < got.size(); ++i) {
            ASSERT_EQ(got[i].header(), batches[i].header())
              << "chunk=" << chunk;
        }
    }
}

TEST(chunk_reassembler, slack_nonempty_mid_batch) {
    auto batches = model::test::make_random_batches(model::offset(0), 2).get();
    auto run = serialize_run(batches);
    chunk_reassembler r;
    iobuf head;
    head.append(run.share(0, run.size_bytes() - 5)); // cut 5 bytes before end
    auto first = r.feed(std::move(head));
    EXPECT_GT(r.slack_bytes(), 0u);
    iobuf tail;
    tail.append(run.share(run.size_bytes() - 5, 5));
    auto rest = r.feed(std::move(tail));
    EXPECT_TRUE(r.empty());
    EXPECT_EQ(first.size() + rest.size(), batches.size());
}

} // namespace cloud_topics::prefetch
