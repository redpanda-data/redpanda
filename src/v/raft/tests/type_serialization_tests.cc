// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "absl/container/flat_hash_map.h"
#include "compression/stream_zstd.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "raft/group_configuration.h"
#include "raft/heartbeats.h"
#include "raft/types.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"
#include "test_utils/randoms.h"
#include "test_utils/rpc.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <chrono>
#include <cstdint>
#include <vector>

void verify_batches(
  const chunked_vector<model::record_batch>& expected,
  const chunked_vector<model::record_batch>& current) {
    BOOST_REQUIRE_EQUAL(expected.size(), current.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        auto& current_batch = current[i];
        auto& expected_batch = current[i];
        BOOST_REQUIRE_EQUAL(
          current_batch.base_offset(), expected_batch.base_offset());
        BOOST_REQUIRE_EQUAL(
          current_batch.last_offset(), expected_batch.last_offset());
        BOOST_REQUIRE_EQUAL(
          current_batch.header().crc, expected_batch.header().crc);
        BOOST_REQUIRE_EQUAL(
          current_batch.compressed(), expected_batch.compressed());
        BOOST_REQUIRE_EQUAL(
          current_batch.header().type, expected_batch.header().type);
        BOOST_REQUIRE_EQUAL(
          current_batch.size_bytes(), expected_batch.size_bytes());
        BOOST_REQUIRE_EQUAL(
          current_batch.record_count(), expected_batch.record_count());
        BOOST_REQUIRE_EQUAL(current_batch.term(), expected_batch.term());
    }
}

SEASTAR_THREAD_TEST_CASE(append_entries_requests) {
    chunked_vector<model::record_batch> batches{
      std::from_range,
      model::test::make_random_batches(model::offset(1), 3, false).get()
        | std::views::as_rvalue};

    chunked_vector<model::record_batch> reference_batches;

    for (auto& b : batches) {
        b.set_term(model::term_id(123));
        reference_batches.push_back(b.share());
    }

    auto meta = raft::protocol_metadata{
      .group = raft::group_id(1),
      .commit_index = model::offset(100),
      .term = model::term_id(10),
      .prev_log_index = model::offset(99),
      .prev_log_term = model::term_id(-1),
      .last_visible_index = model::offset(200),
      .dirty_offset = model::offset(99),
    };
    raft::append_entries_request req(
      raft::vnode(model::node_id(1), model::revision_id(10)),
      raft::vnode(model::node_id(10), model::revision_id(101)),
      meta,
      std::move(batches),
      0);

    const auto target_node_id = req.target_node();

    iobuf buf;
    serde::write_async(buf, std::move(req)).get();
    iobuf_parser p(std::move(buf));
    auto d = serde::read_async<raft::append_entries_request>(p).get();

    BOOST_REQUIRE_EQUAL(
      d.source_node(), raft::vnode(model::node_id(1), model::revision_id(10)));
    BOOST_REQUIRE_EQUAL(d.target_node(), target_node_id);
    BOOST_REQUIRE_EQUAL(d.metadata().group, meta.group);
    BOOST_REQUIRE_EQUAL(d.metadata().commit_index, meta.commit_index);
    BOOST_REQUIRE_EQUAL(d.metadata().term, meta.term);
    BOOST_REQUIRE_EQUAL(d.metadata().prev_log_index, meta.prev_log_index);
    BOOST_REQUIRE_EQUAL(d.metadata().prev_log_term, meta.prev_log_term);
    BOOST_REQUIRE_EQUAL(
      d.metadata().last_visible_index, meta.last_visible_index);
    BOOST_REQUIRE_EQUAL(d.metadata().dirty_offset, meta.dirty_offset);

    verify_batches(reference_batches, d.batches());
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

SEASTAR_THREAD_TEST_CASE(snapshot_metadata_roundtrip) {
    auto n1 = model::random_broker(0, 100);
    auto n2 = model::random_broker(0, 100);
    auto n3 = model::random_broker(0, 100);
    std::vector<model::broker> nodes{n1, n2, n3};
    raft::group_nodes current{
      .voters
      = {raft::vnode(n1.id(), model::revision_id(1)), raft::vnode(n3.id(), model::revision_id(3))},
      .learners = {raft::vnode(n2.id(), model::revision_id(1))}};

    raft::group_configuration cfg(
      nodes, current, model::revision_id(0), std::nullopt);

    auto ct = ss::lowres_clock::now();
    raft::offset_translator_delta delta{
      random_generators::get_int<int64_t>(5000)};
    raft::snapshot_metadata metadata{
      .last_included_index = model::offset(123),
      .last_included_term = model::term_id(32),
      .latest_configuration = cfg,
      .cluster_time = ct,
      .log_start_delta = delta};

    auto d = serialize_roundtrip_adl(std::move(metadata));

    BOOST_REQUIRE_EQUAL(d.last_included_index, model::offset(123));
    BOOST_REQUIRE_EQUAL(d.last_included_term, model::term_id(32));
    BOOST_REQUIRE(
      std::chrono::time_point_cast<std::chrono::milliseconds>(d.cluster_time)
      == std::chrono::time_point_cast<std::chrono::milliseconds>(ct));
    BOOST_REQUIRE_EQUAL(d.latest_configuration.all_nodes(), cfg.all_nodes());
    BOOST_REQUIRE_EQUAL(d.log_start_delta, delta);
}

SEASTAR_THREAD_TEST_CASE(snapshot_metadata_backward_compatibility) {
    auto bp1 = model::random_broker_properties();
    // Zero out the available_memory_bytes field which isn't supported
    // by adl since the field was added after serde serialization became
    // the default.
    bp1.available_memory_bytes = 0;
    auto n1 = model::random_broker(0, 100, bp1);

    auto bp2 = model::random_broker_properties();
    bp2.available_memory_bytes = 0;
    auto n2 = model::random_broker(0, 100, bp2);

    auto bp3 = model::random_broker_properties();
    bp3.available_memory_bytes = 0;
    auto n3 = model::random_broker(0, 100, bp3);

    std::vector<model::broker> nodes{n1, n2, n3};
    raft::group_nodes current{
      .voters
      = {raft::vnode(n1.id(), model::revision_id(1)), raft::vnode(n3.id(), model::revision_id(3))},
      .learners = {raft::vnode(n2.id(), model::revision_id(1))}};

    raft::group_configuration cfg(
      nodes, current, model::revision_id(0), std::nullopt);
    auto c = cfg;
    auto ct = ss::lowres_clock::now();
    // serialize using old format (no version included)
    iobuf serialize_buf;
    reflection::serialize(
      serialize_buf,
      model::offset(123),
      model::term_id(32),
      std::move(cfg),
      std::chrono::duration_cast<std::chrono::milliseconds>(
        ct.time_since_epoch()));

    iobuf_parser parser(std::move(serialize_buf));

    // deserialize with current format
    raft::snapshot_metadata metadata
      = reflection::adl<raft::snapshot_metadata>{}.from(parser);

    BOOST_REQUIRE_EQUAL(metadata.last_included_index, model::offset(123));
    BOOST_REQUIRE_EQUAL(metadata.last_included_term, model::term_id(32));
    BOOST_REQUIRE(
      std::chrono::time_point_cast<std::chrono::milliseconds>(
        metadata.cluster_time)
      == std::chrono::time_point_cast<std::chrono::milliseconds>(ct));
    BOOST_REQUIRE_EQUAL(metadata.latest_configuration, c);
    BOOST_REQUIRE_EQUAL(
      metadata.log_start_delta, raft::offset_translator_delta{});
}

SEASTAR_THREAD_TEST_CASE(append_entries_request_serde_wrapper_serde) {
    chunked_vector<model::record_batch> batches{
      std::from_range,
      model::test::make_random_batches(model::offset(1), 3, false).get()
        | std::views::as_rvalue};
    chunked_vector<model::record_batch> reference_batches;
    for (auto& b : batches) {
        b.set_term(model::term_id(123));
        reference_batches.push_back(b.share());
    }

    auto meta = raft::protocol_metadata{
      .group = raft::group_id(1),
      .commit_index = model::offset(100),
      .term = model::term_id(10),
      .prev_log_index = model::offset(99),
      .prev_log_term = model::term_id(-1),
      .last_visible_index = model::offset(200),
      .dirty_offset = model::offset(99),
    };
    raft::append_entries_request req(
      raft::vnode(model::node_id(1), model::revision_id(10)),
      raft::vnode(model::node_id(10), model::revision_id(101)),
      meta,
      std::move(batches),
      0);

    const auto src_node = req.source_node();
    const auto target_node = req.target_node();

    raft::append_entries_request_serde_wrapper wrapper(std::move(req));

    iobuf buf;
    serde::write_async(buf, std::move(wrapper)).get();
    iobuf_parser parser(std::move(buf));
    auto decoded_wrapper
      = serde::read_async<raft::append_entries_request_serde_wrapper>(parser)
          .get();
    auto decoded_req = std::move(decoded_wrapper).release();

    BOOST_REQUIRE_EQUAL(decoded_req.source_node(), src_node);
    BOOST_REQUIRE_EQUAL(decoded_req.target_node(), target_node);
    BOOST_REQUIRE_EQUAL(decoded_req.metadata().group, meta.group);
    BOOST_REQUIRE_EQUAL(decoded_req.metadata().commit_index, meta.commit_index);
    BOOST_REQUIRE_EQUAL(decoded_req.metadata().term, meta.term);
    BOOST_REQUIRE_EQUAL(
      decoded_req.metadata().prev_log_index, meta.prev_log_index);
    BOOST_REQUIRE_EQUAL(
      decoded_req.metadata().prev_log_term, meta.prev_log_term);
    BOOST_REQUIRE_EQUAL(
      decoded_req.metadata().last_visible_index, meta.last_visible_index);
    BOOST_REQUIRE_EQUAL(decoded_req.metadata().dirty_offset, meta.dirty_offset);

    verify_batches(reference_batches, decoded_req.batches());
}
