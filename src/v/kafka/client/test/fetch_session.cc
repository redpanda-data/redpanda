// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/fetch_session.h"

#include "kafka/client/test/utils.h"
#include "kafka/protocol/batch_consumer.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "model/fundamental.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

namespace k = kafka;
namespace kc = k::client;

std::optional<kafka::batch_reader>
make_record_set(model::offset offset, std::optional<size_t> count) {
    if (!count) {
        return std::nullopt;
    }
    iobuf record_set;
    auto writer{kafka::protocol::encoder(record_set)};
    kafka::protocol::writer_serialize_batch(writer, make_batch(offset, *count));
    return kafka::batch_reader{std::move(record_set)};
}

kafka::fetch_response make_fetch_response(
  kafka::fetch_session_id s_id,
  model::topic_partition_view tpv,
  std::optional<kafka::batch_reader> record_set) {
    kafka::fetch_response res{
      .data = {
        .throttle_time_ms = std::chrono::milliseconds{0},
        .error_code = kafka::error_code::none,
        .session_id = s_id,
        .responses{}}};
    kafka::fetch_response::partition p{.topic = tpv.topic};
    p.partitions.push_back(
      kafka::fetch_response::partition_response{
        .partition_index = tpv.partition,
        .error_code = kafka::error_code::none,
        .high_watermark = model::offset{-1},
        .last_stable_offset = model::offset{-1},
        .log_start_offset = model::offset{-1},
        .aborted_transactions = {},
        .records{std::move(record_set)}});
    res.data.responses.push_back(std::move(p));
    return res;
}

kafka::fetch_response make_out_of_range_fetch_response(
  kafka::fetch_session_id s_id,
  model::topic_partition_view tpv,
  model::offset log_start_offset) {
    kafka::fetch_response res{
      .data = {
        .throttle_time_ms = std::chrono::milliseconds{0},
        .error_code = kafka::error_code::none,
        .session_id = s_id,
        .responses{}}};
    kafka::fetch_response::partition p{.topic = tpv.topic};
    p.partitions.push_back(
      kafka::fetch_response::partition_response{
        .partition_index = tpv.partition,
        .error_code = kafka::error_code::offset_out_of_range,
        .high_watermark = model::offset{-1},
        .last_stable_offset = model::offset{-1},
        .log_start_offset = log_start_offset,
        .aborted_transactions = {},
        .records{std::nullopt}});
    res.data.responses.push_back(std::move(p));
    return res;
}

// A response for two partitions of one topic: `ok_tpv` succeeds with
// `ok_record_set`, `oor_partition` fails with offset_out_of_range.
kafka::fetch_response make_partial_out_of_range_fetch_response(
  kafka::fetch_session_id s_id,
  model::topic_partition_view ok_tpv,
  std::optional<kafka::batch_reader> ok_record_set,
  model::partition_id oor_partition,
  model::offset log_start_offset) {
    kafka::fetch_response res{
      .data = {
        .throttle_time_ms = std::chrono::milliseconds{0},
        .error_code = kafka::error_code::none,
        .session_id = s_id,
        .responses{}}};
    kafka::fetch_response::partition p{.topic = ok_tpv.topic};
    p.partitions.push_back(
      kafka::fetch_response::partition_response{
        .partition_index = ok_tpv.partition,
        .error_code = kafka::error_code::none,
        .high_watermark = model::offset{-1},
        .last_stable_offset = model::offset{-1},
        .log_start_offset = model::offset{-1},
        .aborted_transactions = {},
        .records{std::move(ok_record_set)}});
    p.partitions.push_back(
      kafka::fetch_response::partition_response{
        .partition_index = oor_partition,
        .error_code = kafka::error_code::offset_out_of_range,
        .high_watermark = model::offset{-1},
        .last_stable_offset = model::offset{-1},
        .log_start_offset = log_start_offset,
        .aborted_transactions = {},
        .records{std::nullopt}});
    res.data.responses.push_back(std::move(p));
    return res;
}

struct context {
    const kafka::fetch_session_id fetch_session_id{42};
    const model::topic_partition tp{
      model::topic{"test_topic"}, model::partition_id{2}};
    const size_t record_set_size{0};
    kafka::fetch_session_epoch expected_epoch{
      kafka::initial_fetch_session_epoch};
    model::offset expected_offset{0};

    void
    apply_fetch_response(kc::fetch_session& s, std::optional<size_t> count) {
        auto res = make_fetch_response(
          fetch_session_id, tp, make_record_set(expected_offset, count));
        expected_offset += count.value_or(0);
        ++expected_epoch;
        s.apply(res);
    }
};

SEASTAR_THREAD_TEST_CASE(test_fetch_session) {
    context ctx;
    kc::fetch_session s;

    BOOST_REQUIRE_EQUAL(s.id(), kafka::invalid_fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), kafka::initial_fetch_session_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), model::offset{0});

    // Apply some records
    ctx.apply_fetch_response(s, 8);
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);

    // Apply more records
    ctx.apply_fetch_response(s, 8);
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);
}

SEASTAR_THREAD_TEST_CASE(test_fetch_session_null_record_set) {
    context ctx;
    kc::fetch_session s;

    // Apply some records
    ctx.apply_fetch_response(s, 8);
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);

    // Apply nullopt record_set
    ctx.apply_fetch_response(s, std::nullopt);
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);
}

SEASTAR_THREAD_TEST_CASE(test_fetch_session_empty_record_set) {
    context ctx;
    kc::fetch_session s;

    // Apply some records
    ctx.apply_fetch_response(s, 8);
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);

    // Apply 0 records
    ctx.apply_fetch_response(s, 0);
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);
}

// CORE-16844: reseed() lets the caller correct the tracked offset after
// offset_out_of_range, so a fetch doesn't repeat the same stale offset.
SEASTAR_THREAD_TEST_CASE(test_fetch_session_reseed) {
    context ctx;
    kc::fetch_session s;

    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), model::offset{0});

    s.reseed(ctx.tp, model::offset{48});

    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), model::offset{48});
}

// CORE-16844: apply()/discard() do not reseed offset_out_of_range
// partitions themselves -- that's the caller's job via reseed(), done once
// while scanning every broker's response to decide whether to retry (see
// consumer::fetch()).
SEASTAR_THREAD_TEST_CASE(test_fetch_session_apply_does_not_reseed) {
    context ctx;
    kc::fetch_session s;

    auto res = make_out_of_range_fetch_response(
      ctx.fetch_session_id, ctx.tp, model::offset{48});
    s.apply(res);

    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), model::offset{0});
}

// CORE-16844: when any partition on any broker returns offset_out_of_range,
// consumer::fetch() discards the combined response and retries, calling
// discard() so that records fetched but never delivered are not
// skipped by the retry — that would be silent data loss. The epoch must
// still advance, to stay in step with the broker-side session state.
SEASTAR_THREAD_TEST_CASE(
  test_fetch_session_no_offset_advance_when_response_discarded) {
    context ctx;
    kc::fetch_session s;

    const model::topic_partition oor_tp{ctx.tp.topic, model::partition_id{3}};

    auto discarded = make_partial_out_of_range_fetch_response(
      ctx.fetch_session_id,
      ctx.tp,
      make_record_set(model::offset{0}, 8),
      oor_tp.partition,
      model::offset{48});
    s.discard(discarded);

    // The healthy partition's offset stays put; its records are re-fetched
    // by the retry.
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), model::offset{0});
    // discard() does not reseed on its own.
    BOOST_REQUIRE_EQUAL(s.offset(oor_tp), model::offset{0});
    // The epoch advances regardless: the broker processed the request.
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch + 1);

    // The retry delivers the response to the client, so offsets advance.
    auto delivered = make_partial_out_of_range_fetch_response(
      ctx.fetch_session_id,
      ctx.tp,
      make_record_set(model::offset{0}, 8),
      oor_tp.partition,
      model::offset{48});
    s.apply(delivered);

    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), model::offset{8});
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch + 2);
}

SEASTAR_THREAD_TEST_CASE(test_fetch_session_make_offset_commit_request_all) {
    context ctx;
    kc::fetch_session s;

    BOOST_REQUIRE_EQUAL(s.id(), kafka::invalid_fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), kafka::initial_fetch_session_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), model::offset{0});

    // Apply some records
    ctx.apply_fetch_response(s, 8);
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);

    auto req = s.make_offset_commit_request();
    BOOST_REQUIRE_EQUAL(req.size(), 1);
    BOOST_REQUIRE_EQUAL(req[0].name, ctx.tp.topic);
    BOOST_REQUIRE_EQUAL(req[0].partitions.size(), 1);
    const auto partition = req[0].partitions[0];
    BOOST_REQUIRE_EQUAL(partition.partition_index, ctx.tp.partition);
    BOOST_REQUIRE_EQUAL(
      partition.committed_leader_epoch, kafka::invalid_leader_epoch);
    BOOST_REQUIRE_EQUAL(partition.committed_offset, ctx.expected_offset - 1);
}
