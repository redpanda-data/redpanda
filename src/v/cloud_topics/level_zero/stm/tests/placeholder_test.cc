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
#include "cluster/rm_stm_types.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_utils.h"

#include <seastar/core/shard_id.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <optional>
#include <vector>

namespace ct = cloud_topics;

namespace {

constexpr int64_t test_producer_id = 7777;
constexpr int16_t test_producer_epoch = 3;
// Deliberately different from any offset used below so that sequence numbers
// and offsets cannot be confused in the failure output.
constexpr int32_t test_base_sequence = 500;

iobuf make_buf(std::string_view sv) {
    iobuf b;
    b.append(sv.data(), sv.size());
    return b;
}

/// Builds a raft_data batch exactly as the kafka batch adapter would hand it to
/// the produce path: header fields copied verbatim from the wire, records
/// carrying their own offset_delta. `last_offset_delta` is independent of the
/// record count on purpose - that is what compaction and the Kafka wire format
/// both allow.
model::record_batch make_client_batch(
  model::offset base_offset,
  int32_t last_offset_delta,
  const std::vector<int32_t>& record_offset_deltas,
  bool control = false,
  std::optional<iobuf> first_record_key = std::nullopt) {
    iobuf records;
    bool first = true;
    for (auto delta : record_offset_deltas) {
        std::optional<iobuf> key;
        if (first && first_record_key.has_value()) {
            key = first_record_key->copy();
        }
        first = false;
        model::record r(
          model::record_attributes{},
          /*timestamp_delta=*/0,
          delta,
          std::move(key),
          std::optional<iobuf>(make_buf("value")),
          chunked_vector<model::record_header>{});
        model::append_record_to_buffer(records, r);
    }

    model::record_batch_header hdr{
      .header_crc = 0,
      .size_bytes = 0,
      .base_offset = base_offset,
      .type = model::record_batch_type::raft_data,
      .crc = 0,
      .attrs = model::record_batch_attributes{},
      .last_offset_delta = last_offset_delta,
      .first_timestamp = model::timestamp{1000},
      .max_timestamp = model::timestamp{2000},
      .producer_id = test_producer_id,
      .producer_epoch = test_producer_epoch,
      .base_sequence = test_base_sequence,
      .record_count = static_cast<int32_t>(record_offset_deltas.size()),
      .ctx = model::record_batch_header::context(
        model::term_id(1), ss::this_shard_id()),
    };
    if (control) {
        hdr.attrs.set_control_type();
    }

    hdr.reset_size_checksum_metadata(records);
    return model::record_batch(
      hdr, std::move(records), model::record_batch::tag_ctor_ng{});
}

ct::extent_meta make_extent(const model::record_batch_header& hdr) {
    return ct::extent_meta{
      .id = ct::object_id{
        .epoch = ct::cluster_epoch{1},
        .name = uuid_t::create(),
        .prefix = 42,
      },
      .first_byte_offset = ct::first_byte_offset_t{0},
      .byte_range_size = ct::byte_range_size_t{1024},
      .base_offset = model::offset_cast(hdr.base_offset),
      .last_offset = model::offset_cast(hdr.last_offset()),
    };
}

void expect_identical_batch_identity(
  const model::record_batch_header& original,
  const model::record_batch_header& placeholder) {
    auto want = model::batch_identity::from(original);
    auto got = model::batch_identity::from(placeholder);
    EXPECT_EQ(got.pid, want.pid);
    EXPECT_EQ(got.first_seq, want.first_seq);
    EXPECT_EQ(got.last_seq, want.last_seq);
    EXPECT_EQ(got.record_count, want.record_count);
    EXPECT_EQ(got.max_timestamp, want.max_timestamp);
    EXPECT_EQ(got.is_transactional, want.is_transactional);
}

} // namespace

// W1 + W2: a batch whose record_count != last_offset_delta + 1 (what
// compaction, and the Kafka wire format in general, allow) must be represented
// by a placeholder that spans exactly the same Kafka offsets and yields exactly
// the same batch_identity, otherwise the CTP log and rm_stm both disagree with
// the batch the client actually wrote.
TEST(placeholder_test, sparse_batch_round_trips_offset_span_and_identity) {
    auto original = make_client_batch(
      model::offset{100}, /*last_offset_delta=*/9, {0, 4, 9});
    ASSERT_EQ(original.header().record_count, 3);
    ASSERT_EQ(original.header().last_offset_delta, 9);
    ASSERT_EQ(original.header().last_offset(), model::offset{109});

    auto placeholder = ct::encode_placeholder_batch(
      original.header().copy(), make_extent(original.header()));

    // W1: the placeholder must occupy the same Kafka offsets as the batch it
    // replaces.
    EXPECT_EQ(placeholder.header().last_offset_delta, 9);
    EXPECT_EQ(
      placeholder.header().last_offset(), original.header().last_offset());
    EXPECT_EQ(placeholder.header().base_offset, original.header().base_offset);

    // W2: rm_stm derives its idempotency window from whichever header it sees.
    expect_identical_batch_identity(original.header(), placeholder.header());
}

// Guard against a bogus test: the ordinary case (record_count ==
// last_offset_delta + 1) must round-trip.
TEST(placeholder_test, dense_batch_round_trips_offset_span_and_identity) {
    auto original = make_client_batch(
      model::offset{100}, /*last_offset_delta=*/2, {0, 1, 2});
    ASSERT_EQ(original.header().record_count, 3);

    auto placeholder = ct::encode_placeholder_batch(
      original.header().copy(), make_extent(original.header()));

    EXPECT_EQ(placeholder.header().last_offset_delta, 2);
    EXPECT_EQ(
      placeholder.header().last_offset(), original.header().last_offset());
    EXPECT_EQ(placeholder.header().base_offset, original.header().base_offset);
    expect_identical_batch_identity(original.header(), placeholder.header());
}

// Read side: the batch handed back to a fetching client must declare an offset
// span that covers every record its payload actually contains, otherwise the
// delivered batch is self-inconsistent (and CRC-valid, so undetectable).
TEST(placeholder_test, applied_batch_declares_span_covering_its_records) {
    auto original = make_client_batch(
      model::offset{100}, /*last_offset_delta=*/9, {0, 4, 9});
    auto placeholder = ct::encode_placeholder_batch(
      original.header().copy(), make_extent(original.header()));

    auto merged = ct::apply_placeholder_to_batch(
      placeholder.header(), original.copy());

    int32_t max_delta = -1;
    int32_t observed_records = 0;
    merged.for_each_record([&](model::record r) {
        max_delta = std::max(max_delta, r.offset_delta());
        ++observed_records;
    });

    EXPECT_EQ(observed_records, merged.header().record_count);
    EXPECT_LE(max_delta, merged.header().last_offset_delta)
      << "reconstructed batch declares [" << merged.header().base_offset << ", "
      << merged.header().last_offset()
      << "] but contains a record at offset_delta " << max_delta;
    EXPECT_EQ(merged.header().last_offset_delta, 9);
    EXPECT_EQ(merged.header().last_offset(), original.header().last_offset());
}

// W3: a client-produced batch carrying the Kafka isControl attribute. Either
// the placeholder encoder must refuse it, or the record key (which is the
// entire payload of a control record) must survive, because rm_stm parses the
// key of every control batch it applies.
TEST(placeholder_test, control_batch_key_survives_or_is_rejected) {
    // A well-formed Kafka control record key: int16 version, int16 type.
    iobuf key;
    key.append("\x00\x00\x00\x00", 4);
    auto original = make_client_batch(
      model::offset{100},
      /*last_offset_delta=*/0,
      {0},
      /*control=*/true,
      std::move(key));
    ASSERT_TRUE(original.header().attrs.is_control());
    ASSERT_EQ(original.header().record_count, 1);

    auto placeholder = ct::encode_placeholder_batch(
      original.header().copy(), make_extent(original.header()));

    if (!placeholder.header().attrs.is_control()) {
        // Acceptable: the placeholder is not a control batch, so rm_stm will
        // not try to parse its key.
        SUCCEED();
        return;
    }

    bool has_key = false;
    placeholder.for_each_record(
      [&](model::record r) { has_key = r.has_key(); });
    EXPECT_TRUE(has_key)
      << "control placeholder was emitted with a null record key";

    // The faithful downstream check: rm_stm::do_apply calls this for every
    // control batch it applies, and an exception here wedges its apply loop.
    try {
        auto crt = cluster::tx::parse_control_batch(placeholder);
        (void)crt;
    } catch (const std::exception& e) {
        ADD_FAILURE() << "parse_control_batch threw " << typeid(e).name()
                      << ": " << e.what();
    }
}
