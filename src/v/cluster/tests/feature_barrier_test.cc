/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/feature_manager.h"
#include "cluster/members_table.h"
#include "test_utils/fixture.h"
#include "vlog.h"

#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace std::chrono_literals;
using namespace cluster;

struct node_state {
    ss::abort_source as;
    ss::gate gate;
    feature_barrier_state barrier_state;

    node_state(
      members_table& members,
      model::node_id id,
      feature_barrier_state::rpc_fn fn)
      : barrier_state(id, members, as, gate, std::move(fn)) {}

    ~node_state() {
        as.request_abort();
        gate.close().get();
    }
};

struct barrier_fixture {
    /**
     * Populate members_table with `n` brokers
     */
    void create_brokers(int n) {
        std::vector<model::broker> brokers;
        for (int i = 0; i < n; ++i) {
            brokers.push_back(model::broker(
              model::node_id{i},
              net::unresolved_address{},
              net::unresolved_address{},
              std::nullopt,
              model::broker_properties{}));
        }
        members.update_brokers(model::offset{0}, brokers);
    }

    void create_node_state(model::node_id id) {
        using namespace std::placeholders;

        states[id] = ss::make_lw_shared<node_state>(
          members,
          id,
          std::bind(&barrier_fixture::rpc_hook, this, _1, _2, _3, _4));
    }

    void kill(model::node_id id) { states.erase(id); }
    void restart(model::node_id id) {
        kill(id);
        create_node_state(id);
    }

    feature_barrier_state::rpc_fn_ret rpc_hook(
      model::node_id src,
      model::node_id dst,
      feature_barrier_tag tag,
      bool src_entered) {
        if (rpc_rx_errors.contains(dst)) {
            co_return rpc_rx_errors[dst];
        }
        if (rpc_tx_errors.contains(src)) {
            co_return rpc_tx_errors[src];
        }

        BOOST_REQUIRE(states.contains(dst));
        BOOST_REQUIRE(states.contains(src));

        auto update_result = states[dst]->barrier_state.update_barrier(
          tag, src, src_entered);

        auto client_context = rpc::client_context<feature_barrier_response>(
          rpc::header{},
          {.entered = update_result.entered,
           .complete = update_result.complete});
        co_return result<rpc::client_context<feature_barrier_response>>(
          std::move(client_context));
    }

    ss::lw_shared_ptr<node_state> get_node_state(model::node_id id) {
        auto r = states[id];
        assert(r);
        return r;
    }

    feature_barrier_state& get_barrier_state(model::node_id id) {
        return get_node_state(id)->barrier_state;
    }

    std::map<model::node_id, ss::lw_shared_ptr<node_state>> states;

    std::map<model::node_id, std::error_code> rpc_rx_errors;
    std::map<model::node_id, std::error_code> rpc_tx_errors;

    members_table members;
};

/**
 * The no-op case for a single node cluster
 */
FIXTURE_TEST(test_barrier_single_node, barrier_fixture) {
    create_brokers(1);
    create_node_state(model::node_id{0});

    // Should proceed immediately as it is the only node.
    auto f = get_barrier_state(model::node_id{0})
               .barrier(feature_barrier_tag{"test"});

    BOOST_REQUIRE(f.available() && !f.failed());
};

/**
 * The simple case where everyone enters the barrier and proceeds
 * cleanly to completion.
 */
FIXTURE_TEST(test_barrier_simple, barrier_fixture) {
    create_brokers(3);
    create_node_state(model::node_id{0});
    create_node_state(model::node_id{1});
    create_node_state(model::node_id{2});

    auto f0 = get_barrier_state(model::node_id{0})
                .barrier(feature_barrier_tag{"test"});
    auto f1 = get_barrier_state(model::node_id{1})
                .barrier(feature_barrier_tag{"test"});

    BOOST_REQUIRE(!f0.available());
    BOOST_REQUIRE(!f1.available());

    auto f2 = get_barrier_state(model::node_id{2})
                .barrier(feature_barrier_tag{"test"});

    ss::sleep(10ms).get();

    BOOST_REQUIRE(f0.available());
    BOOST_REQUIRE(f1.available());
    BOOST_REQUIRE(f2.available());

    // Now that all three have started running, all should complete
    f0.get();
    f1.get();
    f2.get();
}

FIXTURE_TEST(test_barrier_node_restart, barrier_fixture) {
    create_brokers(3);
    create_node_state(model::node_id{0});
    create_node_state(model::node_id{1});
    create_node_state(model::node_id{2});

    // Nodes 1+2 enter the barrier
    auto f0 = get_barrier_state(model::node_id{0})
                .barrier(feature_barrier_tag{"test"});
    auto f1 = get_barrier_state(model::node_id{1})
                .barrier(feature_barrier_tag{"test"});

    BOOST_REQUIRE(!f0.available());
    BOOST_REQUIRE(!f1.available());

    // Node 3 enters the barrier
    auto f2 = get_barrier_state(model::node_id{2})
                .barrier(feature_barrier_tag{"test"});

    // Prompt reactor to process outstanding futures
    ss::sleep(10ms).get();

    BOOST_REQUIRE(f0.available());
    BOOST_REQUIRE(f1.available());

    // Prompt reactor to process outstanding futures
    ss::sleep(10ms).get();

    restart(model::node_id{2});
    f2 = get_barrier_state(model::node_id{2})
           .barrier(feature_barrier_tag{"test"});

    // Prompt reactor to process outstanding futures
    ss::sleep(10ms).get();

    BOOST_REQUIRE(f2.available());

    // All futures are ready.
    f0.get();
    f1.get();
    f2.get();
}

FIXTURE_TEST(test_barrier_node_isolated, barrier_fixture) {
    create_brokers(3);
    create_node_state(model::node_id{0});
    create_node_state(model::node_id{1});
    create_node_state(model::node_id{2});

    rpc_rx_errors[model::node_id{2}] = cluster::make_error_code(
      cluster::errc::timeout);
    rpc_tx_errors[model::node_id{2}] = cluster::make_error_code(
      cluster::errc::timeout);

    auto f0 = get_barrier_state(model::node_id{0})
                .barrier(feature_barrier_tag{"test"});
    auto f1 = get_barrier_state(model::node_id{1})
                .barrier(feature_barrier_tag{"test"});
    auto f2 = get_barrier_state(model::node_id{2})
                .barrier(feature_barrier_tag{"test"});

    // Without comms to node 2, this barrier should block to complete
    ss::sleep(1000ms).get();

    rpc_rx_errors.clear();
    rpc_tx_errors.clear();

    // After comms are restored the barrier should succeed (waiting for
    // the retry period in barrier())
    ss::sleep(1000ms).get();

    BOOST_REQUIRE(f0.available());
    BOOST_REQUIRE(f1.available());
    BOOST_REQUIRE(f2.available());

    f0.get();
    f1.get();
    f2.get();
}

SEASTAR_THREAD_TEST_CASE(test_barrier_encoding) {
    feature_barrier_request req{
      .tag = feature_barrier_tag{"ohai"},
      .peer = model::node_id{2},
      .entered = true};
    auto req2 = req;

    iobuf req_io = reflection::to_iobuf(std::move(req2));
    iobuf_parser req_parser(std::move(req_io));
    auto req_decoded = reflection::adl<feature_barrier_request>{}.from(
      req_parser);
    BOOST_REQUIRE_EQUAL(req.tag, req_decoded.tag);
    BOOST_REQUIRE_EQUAL(req.peer, req_decoded.peer);
    BOOST_REQUIRE_EQUAL(req.entered, req_decoded.entered);

    feature_barrier_response resp{.entered = true, .complete = true};

    iobuf resp_io = reflection::to_iobuf(std::move(resp));
    iobuf_parser resp_parser(std::move(resp_io));
    auto resp_decoded = reflection::adl<feature_barrier_response>{}.from(
      resp_parser);
    BOOST_REQUIRE_EQUAL(resp.entered, resp_decoded.entered);
    BOOST_REQUIRE_EQUAL(resp.complete, resp_decoded.complete);
}