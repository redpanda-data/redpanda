/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/bytes.h"
#include "cloud_io/admission_control_types.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/common/micro_probe.h"
#include "cloud_topics/level_zero/pipeline/serializer.h"
#include "cloud_topics/level_zero/reader/materialized_extent.h"
#include "cloud_topics/level_zero/reader/tests/materialized_extent_fixture.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "model/record_utils.h"
#include "storage/record_batch_builder.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>

#include <queue>

ss::logger test_log("materialized_extent_test_log");

TEST_F_CORO(materialized_extent_fixture, materialize_from_cache) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(true, 1);

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(res.has_value());

    chunked_vector<model::record_batch> actual;
    actual.emplace_back(make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
    ASSERT_EQ_CORO(probe.num_cache_reads, 1);
}

TEST_F_CORO(materialized_extent_fixture, cache_get_fails) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.cache_get = injected_cache_get_failure::return_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::cache_read_error);
    ASSERT_EQ_CORO(probe.num_cache_reads, 1);
}

TEST_F_CORO(materialized_extent_fixture, cache_get_throws) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.cache_get = injected_cache_get_failure::throw_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::cache_read_error);
    ASSERT_EQ_CORO(probe.num_cache_reads, 1);
}

TEST_F_CORO(materialized_extent_fixture, cache_get_shutdown) {
    // Test situation when the
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.cache_get = injected_cache_get_failure::throw_shutdown}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
    ASSERT_EQ_CORO(probe.num_cache_reads, 1);
}

TEST_F_CORO(materialized_extent_fixture, is_cached_throws) {
    // Test situation when the
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::throw_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::cache_read_error);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, is_cached_throws_shutdown) {
    // Test situation when the
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::throw_shutdown}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, is_cached_stall_then_success) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::stall_then_ok}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(res.has_value());

    chunked_vector<model::record_batch> actual;
    actual.emplace_back(
      cloud_topics::l0::make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
    ASSERT_EQ_CORO(probe.num_cache_reads, 1);
}

TEST_F_CORO(materialized_extent_fixture, is_cached_stall_then_timeout) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::noop}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 100ms, 1ms, retry_strategy::backoff);

    auto extent = make_materialized_extent(partition.front().copy());

    co_await ss::sleep(100ms);
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::timeout);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, materialize_from_cloud) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(false, 1);

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(res.has_value());

    chunked_vector<model::record_batch> actual;
    actual.emplace_back(
      cloud_topics::l0::make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
    // NOTE: the cloud_io::remote is mocked so the callbacks
    // that update cloud_* metrics are not invoked. But we can
    // at least register cache writes which happen after successful
    // object download.
    ASSERT_EQ_CORO(probe.num_cache_writes, 1);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_return_failure) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::return_failure}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::download_failure);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_throw_shutdown) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::throw_shutdown}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_return_notfound) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::return_notfound}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::download_not_found);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_return_timeout) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::return_timeout}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::timeout);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_throw_error) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::throw_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::unexpected_failure);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cache_reserve_space_throws) {
    // If we fail to reserve space the request should still succeed
    // but 'cache.put' can't be invoked.
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cache_rsv = injected_cache_rsv_failure::throw_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(res.has_value());

    chunked_vector<model::record_batch> actual;
    actual.emplace_back(
      cloud_topics::l0::make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cache_reserve_space_throws_shutdown) {
    // If we fail to reserve space because of the shutdown the result
    // should be an errc::shutdown code.
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cache_rsv = injected_cache_rsv_failure::throw_shutdown}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cache_put_throws) {
    // If we fail to put element into the cache the request should still succeed
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cache_put = injected_cache_put_failure::throw_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(res.has_value());

    chunked_vector<model::record_batch> actual;
    actual.emplace_back(
      cloud_topics::l0::make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
}

TEST_F_CORO(materialized_extent_fixture, cache_put_throws_shutdown) {
    // If we fail to put because of the shutdown the result
    // should be an errc::shutdown code.
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cache_put = injected_cache_put_failure::throw_shutdown}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    cloud_topics::l0::micro_probe probe;
    auto extent = make_materialized_extent(partition.front().copy());
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe,
      cloud_io::group_id::default_group);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
}

// --- l0-read-integrity:W1 -------------------------------------------------
//
// A Kafka v2 record batch carries a CRC over the record bytes that the
// consumer validates end to end. The L0 write path stores that produce-time
// CRC verbatim inside the L0 object (serializer.cc writes
// batch_header_to_disk_iobuf followed by the records), so the read path is
// able to verify it. These tests assert the Kafka-required behaviour: a
// corrupted L0 record payload must not be handed to the client as a batch
// whose CRC validates.

namespace {

constexpr size_t marker_size = 64;
constexpr uint8_t marker_byte = 'A';

/// Build a raft_data batch whose single record value is a long run of a
/// distinctive byte. That makes it possible to locate the record payload
/// inside the serialized L0 object and flip a byte that is certainly part of
/// a record value rather than a length prefix, so the corrupted batch still
/// decodes.
model::record_batch make_marked_batch() {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    iobuf value;
    std::string marker(marker_size, static_cast<char>(marker_byte));
    value.append(marker.data(), marker.size());
    builder.add_raw_kv(std::nullopt, std::move(value));
    // build() runs reset_size_checksum_metadata, i.e. the same crc/header_crc
    // computation the produce path performs.
    return std::move(builder).build();
}

ss::future<cloud_topics::l0::serialized_chunk>
serialize_one(model::record_batch batch) {
    chunked_vector<model::record_batch> batches;
    batches.push_back(std::move(batch));
    return cloud_topics::l0::serialize_batches(std::move(batches));
}

/// Offset of the record value inside the serialized L0 object.
size_t find_marker(const bytes& raw) {
    std::array<uint8_t, 8> needle{};
    needle.fill(marker_byte);
    auto it = std::search(raw.begin(), raw.end(), needle.begin(), needle.end());
    if (it == raw.end()) {
        return std::numeric_limits<size_t>::max();
    }
    return static_cast<size_t>(std::distance(raw.begin(), it));
}

} // namespace

// Control: an intact L0 object must materialize back to exactly the
// produce-time record crc. This establishes that every input of
// model::crc_record_batch is stored verbatim in the object, so the stored
// value is directly verifiable by the read path.
TEST_F_CORO(materialized_extent_fixture, w1_intact_object_matches_stored_crc) {
    auto source = make_marked_batch();
    const auto produce_crc = source.header().crc;

    auto chunk = co_await serialize_one(source.copy());
    cloud_topics::l0::materialized_extent ext{
      .meta = chunk.extents.front(),
      .object = std::move(chunk.payload),
    };
    auto batch = cloud_topics::l0::make_raft_data_batch(std::move(ext));
    ASSERT_EQ_CORO(batch.header().crc, produce_crc);
}

// A single bit flipped inside the record payload of an L0 object must be
// detected. Kafka's only end-to-end integrity check on the record bytes is
// the batch crc, so the batch handed to the consumer must either not exist
// or must fail crc validation.
TEST_F_CORO(materialized_extent_fixture, w1_corrupt_records_are_detected) {
    auto source = make_marked_batch();
    const auto produce_crc = source.header().crc;

    auto chunk = co_await serialize_one(source.copy());
    auto meta = chunk.extents.front();
    auto raw = iobuf_to_bytes(chunk.payload);

    auto marker_at = find_marker(raw);
    ASSERT_NE_CORO(marker_at, std::numeric_limits<size_t>::max());
    ASSERT_GE_CORO(marker_at, model::packed_record_batch_header_size);

    // Flip one bit in the middle of the record's value.
    auto flip_at = marker_at + 4;
    raw[flip_at] = static_cast<uint8_t>(raw[flip_at] ^ 0x01);
    vlog(
      test_log.info,
      "flipped byte {} of {} (records region starts at {})",
      flip_at,
      raw.size(),
      model::packed_record_batch_header_size);

    cloud_topics::l0::materialized_extent ext{
      .meta = meta,
      .object = bytes_to_iobuf(raw),
    };
    auto batch = cloud_topics::l0::make_raft_data_batch(std::move(ext));

    vlog(
      test_log.info,
      "produce-time crc: {}, crc of returned batch: {}, crc recomputed over "
      "returned bytes: {}",
      produce_crc,
      batch.header().crc,
      model::crc_record_batch(batch.header(), batch.data()));

    // The bytes really are corrupted.
    ASSERT_NE_CORO(
      model::crc_record_batch(source.header(), source.data()),
      model::crc_record_batch(batch.header(), batch.data()));

    // ... and the corrupted records still decode, i.e. a consumer sees a
    // well formed record whose value is wrong.
    auto records = batch.copy_records();
    ASSERT_EQ_CORO(records.size(), 1);
    ASSERT_NE_CORO(
      records.front().value(), source.copy_records().front().value());

    // Required: the produce-time crc stored in the object must survive to the
    // client, so the client's own crc check fails on the corrupted payload.
    EXPECT_EQ(batch.header().crc, produce_crc)
      << "read path replaced the stored produce-time record crc with a "
         "recomputation over the corrupted bytes";

    // Required: a Kafka consumer validating the batch crc must see a
    // mismatch.
    EXPECT_NE(
      model::crc_record_batch(batch.header(), batch.data()), batch.header().crc)
      << "corrupted batch is self-consistent: a Kafka client cannot detect "
         "the corruption";
    co_return;
}

// Same corruption, but observed one layer up through the full placeholder
// merge, which recomputes both crcs a second time.
TEST_F_CORO(
  materialized_extent_fixture, w1_corrupt_records_survive_placeholder_merge) {
    auto source = make_marked_batch();
    const auto produce_crc = source.header().crc;

    auto chunk = co_await serialize_one(source.copy());
    auto meta = chunk.extents.front();
    auto raw = iobuf_to_bytes(chunk.payload);
    auto marker_at = find_marker(raw);
    ASSERT_NE_CORO(marker_at, std::numeric_limits<size_t>::max());
    auto flip_at = marker_at + 4;
    raw[flip_at] = static_cast<uint8_t>(raw[flip_at] ^ 0x01);

    auto placeholder = cloud_topics::encode_placeholder_batch(
      source.header(), meta);

    cloud_topics::l0::materialized_extent ext{
      .meta = meta,
      .object = bytes_to_iobuf(raw),
    };
    auto materialized = cloud_topics::l0::make_raft_data_batch(std::move(ext));
    auto merged = cloud_topics::apply_placeholder_to_batch(
      placeholder.header(), std::move(materialized));

    vlog(
      test_log.info,
      "produce-time crc: {}, crc after placeholder merge: {}",
      produce_crc,
      merged.header().crc);

    EXPECT_EQ(merged.header().crc, produce_crc)
      << "placeholder merge recomputed the record crc over corrupted bytes";
    EXPECT_NE(
      model::crc_record_batch(merged.header(), merged.data()),
      merged.header().crc)
      << "merged batch is self-consistent: a Kafka client cannot detect the "
         "corruption";
    co_return;
}
