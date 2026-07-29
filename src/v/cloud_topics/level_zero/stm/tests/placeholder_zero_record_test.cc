/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/shard_id.hh>

#include <gtest/gtest.h>

namespace ct = cloud_topics;

// W4: a Kafka v2 batch with recordCount == 0 / lastOffsetDelta == -1 passes the
// kafka batch adapter and produce validation. Encoding a placeholder for it
// must not take the broker down.
//
// This lives in its own test binary because the current behaviour is a vassert
// abort, which kills the whole process.
TEST(placeholder_zero_record_test, zero_record_batch_does_not_abort) {
    model::record_batch_header hdr{
      .header_crc = 0,
      .size_bytes = 0,
      .base_offset = model::offset{100},
      .type = model::record_batch_type::raft_data,
      .crc = 0,
      .attrs = model::record_batch_attributes{},
      .last_offset_delta = -1,
      .first_timestamp = model::timestamp{1000},
      .max_timestamp = model::timestamp{2000},
      .producer_id = 7777,
      .producer_epoch = 3,
      .base_sequence = 100,
      .record_count = 0,
      .ctx = model::record_batch_header::context(
        model::term_id(1), ss::this_shard_id()),
    };
    iobuf empty;
    hdr.reset_size_checksum_metadata(empty);

    ct::extent_meta extent{
      .id = ct::object_id{
        .epoch = ct::cluster_epoch{1},
        .name = uuid_t::create(),
        .prefix = 42,
      },
      .first_byte_offset = ct::first_byte_offset_t{0},
      .byte_range_size = ct::byte_range_size_t{61},
      .base_offset = model::offset_cast(hdr.base_offset),
      .last_offset = model::offset_cast(hdr.last_offset()),
    };

    auto placeholder = ct::encode_placeholder_batch(hdr, extent);
    // If we get here at all, the encoder did not abort. Record what it built.
    EXPECT_EQ(placeholder.header().record_count, 0);
}
