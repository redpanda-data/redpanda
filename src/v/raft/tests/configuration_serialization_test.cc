// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/metadata.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "raft/consensus_utils.h"
#include "raft/group_configuration.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "test_utils/randoms.h"

#include <seastar/testing/thread_test_case.hh>

#include <bits/stdint-uintn.h>
#include <boost/test/tools/old/interface.hpp>

#include <iterator>
#include <optional>
#include <vector>

std::vector<model::broker> random_brokers() {
    std::vector<model::broker> ret;
    for (auto i = 0; i < random_generators::get_int(5, 10); ++i) {
        ret.push_back(model::random_broker(i, i));
    }
    return ret;
}

raft::group_configuration random_configuration() {
    auto brokers = random_brokers();
    raft::group_nodes current;
    for (auto& b : brokers) {
        if (random_generators::get_int(0, 100) > 50) {
            current.voters.emplace_back(
              b.id(), model::revision_id(random_generators::get_int(100)));
        } else {
            current.learners.emplace_back(
              b.id(), model::revision_id(random_generators::get_int(100)));
        }
    }
    std::optional<raft::group_nodes> old_cfg;

    std::optional<raft::configuration_update> update;

    if (tests::random_bool()) {
        raft::group_nodes old;
        for (auto& b : brokers) {
            if (random_generators::get_int(0, 100) > 50) {
                old.voters.emplace_back(
                  b.id(), model::revision_id(random_generators::get_int(100)));
            } else {
                old.learners.emplace_back(
                  b.id(), model::revision_id(random_generators::get_int(100)));
            }
        }
    }
    if (tests::random_bool()) {
        update = raft::configuration_update{};

        std::generate_n(
          std::back_inserter(update->replicas_to_add),
          random_generators::get_int(1, 10),
          []() {
              return raft::vnode(
                tests::random_named_int<model::node_id>(),
                tests::random_named_int<model::revision_id>());
          });
        std::generate_n(
          std::back_inserter(update->replicas_to_remove),
          random_generators::get_int(1, 10),
          []() {
              return raft::vnode(
                tests::random_named_int<model::node_id>(),
                tests::random_named_int<model::revision_id>());
          });
    }
    return {
      std::move(brokers),
      std::move(current),
      tests::random_named_int<model::revision_id>(),
      std::move(update),
      std::move(old_cfg)};
}

SEASTAR_THREAD_TEST_CASE(roundtrip_raft_configuration_entry) {
    auto cfg = random_configuration();
    // serialize to entry
    auto batches = raft::details::serialize_configuration_as_batches(cfg);
    // extract from entry
    auto new_cfg = reflection::from_iobuf<raft::group_configuration>(
      batches.begin()->copy_records().begin()->release_value());

    BOOST_REQUIRE_EQUAL(new_cfg, cfg);
}

struct test_consumer {
    explicit test_consumer(model::offset base_offset)
      : _next_offset(base_offset + model::offset(1)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch& b) {
        if (
          !b.compressed()
          && b.header().type == model::record_batch_type::raft_configuration) {
            _config_offsets.push_back(_next_offset);
        }
        _next_offset += model::offset(b.header().last_offset_delta)
                        + model::offset(1);
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    std::vector<model::offset> end_of_stream() { return _config_offsets; }

    model::offset _next_offset;
    std::vector<model::offset> _config_offsets;
};

SEASTAR_THREAD_TEST_CASE(test_config_extracting_reader) {
    auto cfg_1 = random_configuration();
    auto cfg_2 = random_configuration();
    using batches_t = ss::circular_buffer<model::record_batch>;
    ss::circular_buffer<model::record_batch> all_batches;

    // serialize to batches
    auto cfg_batch_1 = raft::details::serialize_configuration_as_batches(cfg_1);
    auto cfg_batch_2 = raft::details::serialize_configuration_as_batches(cfg_2);
    auto batches = model::test::make_random_batches(model::offset(0), 10, true);

    std::vector<batches_t> ranges;
    ranges.reserve(4);
    // interleave config batches with data batches
    ranges.push_back(std::move(cfg_batch_1));
    ranges.push_back(
      model::test::make_random_batches(model::offset(0), 10, true));
    ranges.push_back(std::move(cfg_batch_2));
    ranges.push_back(
      model::test::make_random_batches(model::offset(0), 10, true));

    for (auto& r : ranges) {
        std::move(r.begin(), r.end(), std::back_inserter(all_batches));
    }

    raft::details::for_each_ref_extract_configuration(
      model::offset(100),
      model::make_memory_record_batch_reader(std::move(all_batches)),
      test_consumer(model::offset(100)),
      model::no_timeout)
      .then([&cfg_1, &cfg_2](auto res) {
          auto& [offsets, configurations] = res;
          BOOST_REQUIRE_EQUAL(offsets[0], model::offset(101));
          BOOST_REQUIRE_EQUAL(configurations[0].offset, model::offset(101));
          BOOST_REQUIRE_EQUAL(configurations[0].cfg, cfg_1);

          BOOST_REQUIRE_EQUAL(configurations[1].offset, offsets[1]);
          BOOST_REQUIRE_EQUAL(configurations[1].cfg, cfg_2);
      })
      .get0();
}

/**
 * Simple configuration with one broker, one learner and one voter in old
 * configuration
 */

model::internal::broker_v0 node_0{
  model::node_id(0),           // id
  tests::random_net_address(), // kafka api address
  tests::random_net_address(), // rpc address
  std::nullopt,
  model::broker_properties{.cores = random_generators::get_int<uint32_t>(96)}};

model::internal::broker_v0 node_1{
  model::node_id(1),           // id
  tests::random_net_address(), // kafka api address
  tests::random_net_address(), // rpc address
  std::nullopt,
  model::broker_properties{.cores = random_generators::get_int<uint32_t>(96)}};

model::internal::broker_v0 node_2{
  model::node_id(2),           // id
  tests::random_net_address(), // kafka api address
  tests::random_net_address(), // rpc address
  std::nullopt,
  model::broker_properties{.cores = random_generators::get_int<uint32_t>(96)}};

struct group_nodes_v0 {
    std::vector<model::node_id> voters;
    std::vector<model::node_id> learners;
};
iobuf serialize_v0() {
    iobuf buffer;

    group_nodes_v0 current;
    current.learners.push_back(node_0.id);
    current.voters.push_back(node_1.id);

    std::optional<group_nodes_v0> old;
    old.emplace();
    old->voters.push_back(node_2.id);

    reflection::serialize(
      buffer,
      (uint8_t)0, // version
      std::vector<model::internal::broker_v0>{node_0, node_1, node_2},
      std::move(current),
      std::move(old));

    return buffer;
}

iobuf serialize_v1() {
    iobuf buffer;

    group_nodes_v0 current;
    current.learners.push_back(node_0.id);
    current.voters.push_back(node_1.id);

    std::optional<group_nodes_v0> old;
    old.emplace();
    old->voters.push_back(node_2.id);

    reflection::serialize(
      buffer,
      (uint8_t)1, // version
      std::vector<model::internal::broker_v0>{node_0, node_1, node_2},
      std::move(current),
      std::move(old),
      model::revision_id(15));

    return buffer;
}

iobuf serialize_v2() {
    iobuf buffer;

    raft::group_nodes current;
    current.learners.emplace_back(node_0.id, model::revision_id(15));
    current.voters.emplace_back(node_1.id, model::revision_id(10));

    std::optional<raft::group_nodes> old = raft::group_nodes{};
    old->voters.emplace_back(node_2.id, model::revision_id(5));

    reflection::serialize(
      buffer,
      (uint8_t)2, // version
      std::vector<model::internal::broker_v0>{node_0, node_1, node_2},
      std::move(current),
      std::move(old),
      model::revision_id(15));
    return buffer;
}

iobuf serialize_v3() {
    iobuf buffer;

    raft::group_nodes current;
    current.learners.emplace_back(node_0.id, model::revision_id(15));
    current.voters.emplace_back(node_1.id, model::revision_id(10));

    std::optional<raft::group_nodes> old = raft::group_nodes{};
    old->voters.emplace_back(node_2.id, model::revision_id(5));

    reflection::serialize(
      buffer,
      (uint8_t)3, // version
      std::vector<model::broker>{
        node_0.to_v3(), node_1.to_v3(), node_2.to_v3()},
      std::move(current),
      std::move(old),
      model::revision_id(15));

    return buffer;
}

iobuf serialize_v4() {
    iobuf buffer;

    raft::group_nodes current;
    current.learners.emplace_back(node_0.id, model::revision_id(15));
    current.voters.emplace_back(node_1.id, model::revision_id(10));

    std::optional<raft::group_nodes> old = raft::group_nodes{};
    old->voters.emplace_back(node_2.id, model::revision_id(5));

    raft::configuration_update update{};

    update.replicas_to_add.emplace_back(
      model::node_id(10), model::revision_id(12));
    update.replicas_to_remove.emplace_back(
      model::node_id(12), model::revision_id(15));

    reflection::serialize(
      buffer,
      raft::group_configuration(
        std::vector<model::broker>{
          node_0.to_v3(), node_1.to_v3(), node_2.to_v3()},
        std::move(current),
        model::revision_id(15),
        std::move(update),
        std::move(old)));

    return buffer;
}

iobuf serialize_v5() {
    iobuf buffer;

    raft::group_nodes current;
    current.learners.emplace_back(node_0.id, model::revision_id(15));
    current.voters.emplace_back(node_1.id, model::revision_id(10));

    std::optional<raft::group_nodes> old = raft::group_nodes{};
    old->voters.emplace_back(node_2.id, model::revision_id(5));

    raft::configuration_update update{};

    update.replicas_to_add.emplace_back(
      model::node_id(10), model::revision_id(12));
    update.replicas_to_remove.emplace_back(
      model::node_id(12), model::revision_id(15));

    reflection::serialize(
      buffer,
      raft::group_configuration(
        std::move(current),
        model::revision_id(15),
        std::move(update),
        std::move(old)));

    return buffer;
}
SEASTAR_THREAD_TEST_CASE(configuration_backward_compatibility_test) {
    iobuf v0 = serialize_v0();
    iobuf v1 = serialize_v1();
    iobuf v2 = serialize_v2();
    iobuf v3 = serialize_v3();
    iobuf v4 = serialize_v4();
    iobuf v5 = serialize_v5();

    auto cfg_v0 = reflection::from_iobuf<raft::group_configuration>(
      std::move(v0));

    auto cfg_v1 = reflection::from_iobuf<raft::group_configuration>(
      std::move(v1));

    auto cfg_v2 = reflection::from_iobuf<raft::group_configuration>(
      std::move(v2));

    auto cfg_v3 = reflection::from_iobuf<raft::group_configuration>(
      std::move(v3));

    auto cfg_v4 = reflection::from_iobuf<raft::group_configuration>(
      std::move(v4));

    auto cfg_v5 = reflection::from_iobuf<raft::group_configuration>(
      std::move(v5));

    BOOST_REQUIRE_EQUAL(cfg_v0.brokers(), cfg_v1.brokers());
    BOOST_REQUIRE_EQUAL(cfg_v1.brokers(), cfg_v2.brokers());
    BOOST_REQUIRE_EQUAL(cfg_v2.brokers(), cfg_v3.brokers());
    BOOST_REQUIRE_EQUAL(cfg_v3.brokers(), cfg_v4.brokers());

    BOOST_REQUIRE_EQUAL(
      cfg_v0.current_config().learners.size(),
      cfg_v1.current_config().learners.size());
    BOOST_REQUIRE_EQUAL(
      cfg_v1.current_config().learners.size(),
      cfg_v2.current_config().learners.size());
    BOOST_REQUIRE_EQUAL(
      cfg_v2.current_config().learners.size(),
      cfg_v3.current_config().learners.size());
    BOOST_REQUIRE_EQUAL(
      cfg_v3.current_config().learners.size(),
      cfg_v4.current_config().learners.size());
    BOOST_REQUIRE_EQUAL(
      cfg_v4.current_config().learners.size(),
      cfg_v5.current_config().learners.size());

    BOOST_REQUIRE_EQUAL(
      cfg_v0.current_config().voters.size(),
      cfg_v1.current_config().voters.size());
    BOOST_REQUIRE_EQUAL(
      cfg_v1.current_config().voters.size(),
      cfg_v2.current_config().voters.size());
    BOOST_REQUIRE_EQUAL(
      cfg_v2.current_config().voters.size(),
      cfg_v3.current_config().voters.size());
    BOOST_REQUIRE_EQUAL(
      cfg_v3.current_config().voters.size(),
      cfg_v4.current_config().voters.size());
    BOOST_REQUIRE_EQUAL(
      cfg_v4.current_config().voters.size(),
      cfg_v5.current_config().voters.size());

    BOOST_REQUIRE_EQUAL(
      cfg_v0.old_config()->voters.size(), cfg_v1.old_config()->voters.size());
    BOOST_REQUIRE_EQUAL(
      cfg_v1.old_config()->voters.size(), cfg_v2.old_config()->voters.size());
    BOOST_REQUIRE_EQUAL(
      cfg_v2.old_config()->voters.size(), cfg_v3.old_config()->voters.size());
    BOOST_REQUIRE_EQUAL(
      cfg_v3.old_config()->voters.size(), cfg_v4.old_config()->voters.size());
    BOOST_REQUIRE_EQUAL(
      cfg_v4.old_config()->voters.size(), cfg_v5.old_config()->voters.size());

    BOOST_REQUIRE_EQUAL(
      cfg_v0.old_config()->voters[0].id(), cfg_v1.old_config()->voters[0].id());

    BOOST_REQUIRE_EQUAL(
      cfg_v0.current_config().learners[0].id(),
      cfg_v1.current_config().learners[0].id());
    BOOST_REQUIRE_EQUAL(
      cfg_v1.current_config().learners[0].id(),
      cfg_v2.current_config().learners[0].id());
    BOOST_REQUIRE_EQUAL(
      cfg_v2.current_config().learners[0].id(),
      cfg_v3.current_config().learners[0].id());
    BOOST_REQUIRE_EQUAL(
      cfg_v3.current_config().learners[0].id(),
      cfg_v4.current_config().learners[0].id());
    BOOST_REQUIRE_EQUAL(
      cfg_v4.current_config().learners[0].id(),
      cfg_v5.current_config().learners[0].id());

    BOOST_REQUIRE_EQUAL(
      cfg_v0.current_config().voters[0].id(),
      cfg_v1.current_config().voters[0].id());
    BOOST_REQUIRE_EQUAL(
      cfg_v1.current_config().voters[0].id(),
      cfg_v2.current_config().voters[0].id());
    BOOST_REQUIRE_EQUAL(
      cfg_v2.current_config().voters[0].id(),
      cfg_v3.current_config().voters[0].id());
    BOOST_REQUIRE_EQUAL(
      cfg_v3.current_config().voters[0].id(),
      cfg_v4.current_config().voters[0].id());
    BOOST_REQUIRE_EQUAL(
      cfg_v4.current_config().voters[0].id(),
      cfg_v5.current_config().voters[0].id());

    BOOST_REQUIRE_EQUAL(cfg_v0.revision_id(), raft::no_revision);
    BOOST_REQUIRE_EQUAL(cfg_v1.revision_id(), model::revision_id(15));
    BOOST_REQUIRE_EQUAL(cfg_v2.revision_id(), model::revision_id(15));
    BOOST_REQUIRE_EQUAL(cfg_v3.revision_id(), model::revision_id(15));
    BOOST_REQUIRE_EQUAL(cfg_v4.revision_id(), model::revision_id(15));
    BOOST_REQUIRE_EQUAL(cfg_v5.revision_id(), model::revision_id(15));
}

SEASTAR_THREAD_TEST_CASE(configuration_broker_many_endpoints) {
    model::broker node_0(
      model::node_id(2),
      {
        model::broker_endpoint(tests::random_net_address()),
        model::broker_endpoint("foobar", tests::random_net_address()),
      },
      tests::random_net_address(),
      std::nullopt,
      model::broker_properties{
        .cores = random_generators::get_int<uint32_t>(96)});

    model::broker node_1(
      model::node_id(2),
      {
        model::broker_endpoint("foobar2", tests::random_net_address()),
        model::broker_endpoint(tests::random_net_address()),
        model::broker_endpoint("foobar3", tests::random_net_address()),
      },
      tests::random_net_address(),
      std::nullopt,
      model::broker_properties{
        .cores = random_generators::get_int<uint32_t>(96)});

    iobuf buffer;

    raft::group_nodes current;
    current.learners.emplace_back(node_0.id(), model::revision_id(15));
    current.voters.emplace_back(node_1.id(), model::revision_id(10));

    raft::group_nodes old;
    old.voters.emplace_back(node_1.id(), model::revision_id(5));

    raft::group_configuration cfg_in(
      std::vector<model::broker>{node_0, node_1},
      std::move(current),
      model::revision_id(15),
      std::nullopt,
      std::move(old));
    reflection::serialize(buffer, cfg_in);

    auto cfg = reflection::from_iobuf<raft::group_configuration>(
      std::move(buffer));

    BOOST_REQUIRE_EQUAL(cfg.brokers().size(), 2);
    BOOST_REQUIRE_EQUAL(node_0.kafka_advertised_listeners().size(), 2);
    BOOST_REQUIRE_EQUAL(node_1.kafka_advertised_listeners().size(), 3);
    BOOST_REQUIRE_EQUAL(
      cfg.brokers()[0].kafka_advertised_listeners(),
      node_0.kafka_advertised_listeners());
    BOOST_REQUIRE_EQUAL(
      cfg.brokers()[1].kafka_advertised_listeners(),
      node_1.kafka_advertised_listeners());
}
