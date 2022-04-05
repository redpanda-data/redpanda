// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/stream_zstd.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "raft/group_configuration.h"
#include "raft/types.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/randoms.h"
#include "test_utils/rpc.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <cstdint>
#include <vector>

struct checking_consumer {
    using batches_t = ss::circular_buffer<model::record_batch>;

    checking_consumer(ss::circular_buffer<model::record_batch> exp)
      : expected(std::move(exp)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        auto current_batch = std::move(expected.front());
        expected.pop_front();
        BOOST_REQUIRE_EQUAL(current_batch.base_offset(), batch.base_offset());
        BOOST_REQUIRE_EQUAL(current_batch.last_offset(), batch.last_offset());
        BOOST_REQUIRE_EQUAL(current_batch.header().crc, batch.header().crc);
        BOOST_REQUIRE_EQUAL(current_batch.compressed(), batch.compressed());
        BOOST_REQUIRE_EQUAL(current_batch.header().type, batch.header().type);
        BOOST_REQUIRE_EQUAL(current_batch.size_bytes(), batch.size_bytes());
        BOOST_REQUIRE_EQUAL(current_batch.record_count(), batch.record_count());
        BOOST_REQUIRE_EQUAL(current_batch.term(), batch.term());
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    void end_of_stream() { BOOST_REQUIRE(expected.empty()); }

    batches_t expected;
};

SEASTAR_THREAD_TEST_CASE(append_entries_requests) {
    auto batches = storage::test::make_random_batches(
      model::offset(1), 3, false);

    for (auto& b : batches) {
        b.set_term(model::term_id(123));
    }

    auto rdr = model::make_memory_record_batch_reader(std::move(batches));
    auto readers = raft::details::share_n(std::move(rdr), 2).get0();
    auto meta = raft::protocol_metadata{
      .group = raft::group_id(1),
      .commit_index = model::offset(100),
      .term = model::term_id(10),
      .prev_log_index = model::offset(99),
      .prev_log_term = model::term_id(-1),
      .last_visible_index = model::offset(200),
    };
    raft::append_entries_request req(
      raft::vnode(model::node_id(1), model::revision_id(10)),
      raft::vnode(model::node_id(10), model::revision_id(101)),
      meta,
      std::move(readers.back()));

    readers.pop_back();
    auto d = async_serialize_roundtrip_rpc(std::move(req)).get0();

    BOOST_REQUIRE_EQUAL(
      d.node_id, raft::vnode(model::node_id(1), model::revision_id(10)));
    BOOST_REQUIRE_EQUAL(d.target_node_id, req.target_node_id);
    BOOST_REQUIRE_EQUAL(d.meta.group, meta.group);
    BOOST_REQUIRE_EQUAL(d.meta.commit_index, meta.commit_index);
    BOOST_REQUIRE_EQUAL(d.meta.term, meta.term);
    BOOST_REQUIRE_EQUAL(d.meta.prev_log_index, meta.prev_log_index);
    BOOST_REQUIRE_EQUAL(d.meta.prev_log_term, meta.prev_log_term);
    BOOST_REQUIRE_EQUAL(d.meta.last_visible_index, meta.last_visible_index);

    auto batches_result = model::consume_reader_to_memory(
                            std::move(readers.back()), model::no_timeout)
                            .get0();
    d.batches
      .consume(checking_consumer(std::move(batches_result)), model::no_timeout)
      .get0();
}

model::broker create_test_broker() {
    return model::broker(
      model::node_id(random_generators::get_int(1000)), // id
      net::unresolved_address(
        "127.0.0.1",
        random_generators::get_int(10000, 20000)), // kafka api address
      net::unresolved_address(
        "127.0.0.1", random_generators::get_int(10000, 20000)), // rpc address
      model::rack_id("some_rack"),
      model::broker_properties{
        .cores = 8 // cores
      });
}

SEASTAR_THREAD_TEST_CASE(heartbeat_request_roundtrip) {
    static constexpr int64_t one_k = 1'000;
    raft::heartbeat_request req;
    req.heartbeats = std::vector<raft::heartbeat_metadata>(one_k);
    for (int64_t i = 0; i < one_k; ++i) {
        req.heartbeats[i].node_id = raft::vnode(
          model::node_id(one_k), model::revision_id(i));
        req.heartbeats[i].meta.group = raft::group_id(i);
        req.heartbeats[i].meta.commit_index = model::offset(i);
        req.heartbeats[i].meta.term = model::term_id(i);
        req.heartbeats[i].meta.prev_log_index = model::offset(i);
        req.heartbeats[i].meta.prev_log_term = model::term_id(i);
        req.heartbeats[i].meta.last_visible_index = model::offset(i);
        req.heartbeats[i].target_node_id = raft::vnode(
          model::node_id(0), model::revision_id(i));
    }
    iobuf buf;
    reflection::async_adl<raft::heartbeat_request>{}
      .to(buf, std::move(req))
      .get();
    BOOST_TEST_MESSAGE("Pre compression. Buffer size: " << buf);
    compression::stream_zstd codec;
    buf = codec.compress(std::move(buf));
    BOOST_TEST_MESSAGE("Post compression. Buffer size: " << buf);
    auto parser = iobuf_parser(codec.uncompress(std::move(buf)));
    auto res
      = reflection::async_adl<raft::heartbeat_request>{}.from(parser).get0();
    for (int64_t i = 0; i < one_k; ++i) {
        BOOST_REQUIRE_EQUAL(res.heartbeats[i].meta.group, raft::group_id(i));
        BOOST_REQUIRE_EQUAL(
          res.heartbeats[i].meta.commit_index, model::offset(i));
        BOOST_REQUIRE_EQUAL(res.heartbeats[i].meta.term, model::term_id(i));
        BOOST_REQUIRE_EQUAL(
          res.heartbeats[i].meta.prev_log_index, model::offset(i));
        BOOST_REQUIRE_EQUAL(
          res.heartbeats[i].meta.prev_log_term, model::term_id(i));
        BOOST_REQUIRE_EQUAL(
          res.heartbeats[i].meta.last_visible_index, model::offset(i));
        BOOST_REQUIRE_EQUAL(
          res.heartbeats[i].target_node_id,
          raft::vnode(model::node_id(0), model::revision_id(i)));
        BOOST_REQUIRE_EQUAL(
          res.heartbeats[i].node_id,
          raft::vnode(model::node_id(one_k), model::revision_id(i)));
    }
}
SEASTAR_THREAD_TEST_CASE(heartbeat_request_roundtrip_with_negative) {
    static constexpr int64_t one_k = 10;
    raft::heartbeat_request req;

    req.heartbeats = std::vector<raft::heartbeat_metadata>(one_k);
    for (int64_t i = 0; i < one_k; ++i) {
        req.heartbeats[i].target_node_id = raft::vnode(
          model::node_id(0), model::revision_id(-i - 100));
        req.heartbeats[i].node_id = raft::vnode(
          model::node_id(one_k), model::revision_id(-i - 100));
        req.heartbeats[i].meta.group = raft::group_id(i);
        req.heartbeats[i].meta.commit_index = model::offset(-i - 100);
        req.heartbeats[i].meta.term = model::term_id(-i - 100);
        req.heartbeats[i].meta.prev_log_index = model::offset(-i - 100);
        req.heartbeats[i].meta.prev_log_term = model::term_id(-i - 100);
        req.heartbeats[i].meta.last_visible_index = model::offset(-i - 100);
    }
    iobuf buf;
    reflection::async_adl<raft::heartbeat_request>{}
      .to(buf, std::move(req))
      .get();
    BOOST_TEST_MESSAGE("Pre compression. Buffer size: " << buf);
    compression::stream_zstd codec;
    buf = codec.compress(std::move(buf));
    BOOST_TEST_MESSAGE("Post compression. Buffer size: " << buf);
    auto parser = iobuf_parser(codec.uncompress(std::move(buf)));
    auto res
      = reflection::async_adl<raft::heartbeat_request>{}.from(parser).get0();
    for (auto& hb : res.heartbeats) {
        BOOST_REQUIRE_EQUAL(hb.meta.commit_index, model::offset{});
        BOOST_REQUIRE_EQUAL(hb.meta.term, model::term_id{-1});
        BOOST_REQUIRE_EQUAL(hb.meta.prev_log_index, model::offset{});
        BOOST_REQUIRE_EQUAL(hb.meta.prev_log_term, model::term_id{});
        BOOST_REQUIRE_EQUAL(hb.meta.last_visible_index, model::offset{});
        BOOST_REQUIRE_EQUAL(
          hb.node_id, raft::vnode(model::node_id(one_k), model::revision_id{}));
        BOOST_REQUIRE_EQUAL(
          hb.target_node_id,
          raft::vnode(model::node_id(0), model::revision_id{}));
    }
}
SEASTAR_THREAD_TEST_CASE(heartbeat_response_roundtrip) {
    static constexpr int64_t group_count = 10000;
    raft::heartbeat_reply reply;
    reply.meta.reserve(group_count);

    for (size_t i = 0; i < group_count; ++i) {
        // Negative offsets are special, so pick range starting at 0 here
        // (heartbeat_response_negative below covers the negative case)
        auto commited_idx = model::offset(
          random_generators::get_int(0, 1000000000));
        auto dirty_idx = commited_idx
                         + model::offset(random_generators::get_int(10000));

        reply.meta.push_back(raft::append_entries_reply{
          .target_node_id = raft::vnode(
            model::node_id(2), model::revision_id(i)),
          .node_id = raft::vnode(model::node_id(1), model::revision_id(i)),
          .group = raft::group_id(i),
          .term = model::term_id(random_generators::get_int(0, 1000)),
          .last_flushed_log_index = commited_idx,
          .last_dirty_log_index = dirty_idx,
          .result = raft::append_entries_reply::status::success});
    }
    absl::flat_hash_map<raft::group_id, raft::append_entries_reply> expected;
    expected.reserve(reply.meta.size());
    for (const auto& m : reply.meta) {
        expected.emplace(m.group, m);
    }
    iobuf buf;
    reflection::async_adl<raft::heartbeat_reply>{}
      .to(buf, std::move(reply))
      .get();
    BOOST_TEST_MESSAGE("Pre compression. Buffer size: " << buf);
    compression::stream_zstd codec;
    buf = codec.compress(std::move(buf));
    BOOST_TEST_MESSAGE("Post compression. Buffer size: " << buf);
    auto parser = iobuf_parser(codec.uncompress(std::move(buf)));
    auto result
      = reflection::async_adl<raft::heartbeat_reply>{}.from(parser).get0();

    for (size_t i = 0; i < result.meta.size(); ++i) {
        auto gr = result.meta[i].group;
        BOOST_REQUIRE_EQUAL(expected[gr].node_id, result.meta[i].node_id);
        BOOST_REQUIRE_EQUAL(
          expected[gr].target_node_id, result.meta[i].target_node_id);
        BOOST_REQUIRE_EQUAL(expected[gr].group, result.meta[i].group);
        BOOST_REQUIRE_EQUAL(expected[gr].term, result.meta[i].term);
        BOOST_REQUIRE_EQUAL(
          expected[gr].last_flushed_log_index,
          result.meta[i].last_flushed_log_index);
        BOOST_REQUIRE_EQUAL(
          expected[gr].last_dirty_log_index,
          result.meta[i].last_dirty_log_index);
        BOOST_REQUIRE_EQUAL(expected[gr].result, result.meta[i].result);
    }
}

/**
 * Verify that negative values get transformed to ::min values
 * during an encode/decode cycle.  These are encoded to -1 to
 * save space in varint encoding, then all negative values decode
 * to T{}
 */
SEASTAR_THREAD_TEST_CASE(heartbeat_response_negatives) {
    raft::heartbeat_reply reply;

    reply.meta.push_back(raft::append_entries_reply{
      .target_node_id = raft::vnode(model::node_id(2), model::revision_id(-1)),
      .node_id = raft::vnode(model::node_id(1), model::revision_id(-1)),
      .group = raft::group_id(1),
      .term = model::term_id(random_generators::get_int(0, 1000)),
      .last_flushed_log_index = model::offset(-1),
      .last_dirty_log_index = model::offset(-1),
      .last_term_base_offset = model::offset(-1),
      .result = raft::append_entries_reply::status::success});

    iobuf buf;
    reflection::async_adl<raft::heartbeat_reply>{}
      .to(buf, std::move(reply))
      .get();

    auto parser = iobuf_parser(std::move(buf));
    auto result
      = reflection::async_adl<raft::heartbeat_reply>{}.from(parser).get0();
    BOOST_REQUIRE_EQUAL(result.meta[0].last_flushed_log_index, model::offset{});
    BOOST_REQUIRE_EQUAL(result.meta[0].last_dirty_log_index, model::offset{});
    BOOST_REQUIRE_EQUAL(result.meta[0].last_term_base_offset, model::offset{});
    BOOST_REQUIRE_EQUAL(
      result.meta[0].node_id.revision(), model::revision_id{});
    BOOST_REQUIRE_EQUAL(
      result.meta[0].target_node_id.revision(), model::revision_id{});
}

SEASTAR_THREAD_TEST_CASE(snapshot_metadata_roundtrip) {
    auto n1 = tests::random_broker(0, 100);
    auto n2 = tests::random_broker(0, 100);
    auto n3 = tests::random_broker(0, 100);
    std::vector<model::broker> nodes{n1, n2, n3};
    raft::group_nodes current{
      .voters
      = {raft::vnode(n1.id(), model::revision_id(1)), raft::vnode(n3.id(), model::revision_id(3))},
      .learners = {raft::vnode(n2.id(), model::revision_id(1))}};

    raft::group_configuration cfg(nodes, current, model::revision_id(0));

    auto ct = ss::lowres_clock::now();
    raft::offset_translator_delta delta{
      random_generators::get_int<int64_t>(5000)};
    raft::snapshot_metadata metadata{
      .last_included_index = model::offset(123),
      .last_included_term = model::term_id(32),
      .latest_configuration = cfg,
      .cluster_time = ct,
      .log_start_delta = delta};

    auto d = serialize_roundtrip_rpc(std::move(metadata));

    BOOST_REQUIRE_EQUAL(d.last_included_index, model::offset(123));
    BOOST_REQUIRE_EQUAL(d.last_included_term, model::term_id(32));
    BOOST_REQUIRE(d.cluster_time == ct);
    BOOST_REQUIRE_EQUAL(d.latest_configuration, cfg);
    BOOST_REQUIRE_EQUAL(d.log_start_delta, delta);
}

SEASTAR_THREAD_TEST_CASE(snapshot_metadata_backward_compatibility) {
    auto n1 = tests::random_broker(0, 100);
    auto n2 = tests::random_broker(0, 100);
    auto n3 = tests::random_broker(0, 100);
    std::vector<model::broker> nodes{n1, n2, n3};
    raft::group_nodes current{
      .voters
      = {raft::vnode(n1.id(), model::revision_id(1)), raft::vnode(n3.id(), model::revision_id(3))},
      .learners = {raft::vnode(n2.id(), model::revision_id(1))}};

    raft::group_configuration cfg(nodes, current, model::revision_id(0));
    auto c = cfg;
    auto ct = ss::lowres_clock::now();
    // serialize using old format (no version included)
    iobuf serialize_buf;
    reflection::serialize(
      serialize_buf,
      model::offset(123),
      model::term_id(32),
      std::move(cfg),
      ct.time_since_epoch());

    iobuf_parser parser(std::move(serialize_buf));

    // deserialize with current format
    raft::snapshot_metadata metadata
      = reflection::adl<raft::snapshot_metadata>{}.from(parser);

    BOOST_REQUIRE_EQUAL(metadata.last_included_index, model::offset(123));
    BOOST_REQUIRE_EQUAL(metadata.last_included_term, model::term_id(32));
    BOOST_REQUIRE(metadata.cluster_time == ct);
    BOOST_REQUIRE_EQUAL(metadata.latest_configuration, c);
    BOOST_REQUIRE_EQUAL(
      metadata.log_start_delta, raft::offset_translator_delta{});
}
