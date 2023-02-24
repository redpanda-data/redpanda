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

#include "cluster/commands.h"
#include "cluster/feature_manager.h"
#include "cluster/members_table.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "vlog.h"

#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace std::chrono_literals;
using namespace cluster;

// Variant for unit testing, using manual_clock
using feature_barrier_state_test = feature_barrier_state<ss::manual_clock>;

static ss::logger logger("test");

struct node_state {
    ss::abort_source as;
    ss::gate gate;
    feature_barrier_state_test barrier_state;

    node_state(
      members_table& members,
      model::node_id id,
      feature_barrier_state_test::rpc_fn fn)
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
        for (auto& br : brokers) {
            members.apply(model::offset(0), cluster::add_node_cmd(br.id(), br));
        }
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

    feature_barrier_state_test::rpc_fn_ret rpc_hook(
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

    feature_barrier_state_test& get_barrier_state(model::node_id id) {
        return get_node_state(id)->barrier_state;
    }

    std::map<model::node_id, ss::lw_shared_ptr<node_state>> states;

    std::map<model::node_id, std::error_code> rpc_rx_errors;
    std::map<model::node_id, std::error_code> rpc_tx_errors;

    members_table members;
};

/**
 * Barrier on a node that hasn't joined yet, shouldn't
 * proceed until node sees self in members table.
 */
FIXTURE_TEST(test_barrier_joining_node, barrier_fixture) {
    create_node_state(model::node_id{0});

    auto f = get_barrier_state(model::node_id{0})
               .barrier(feature_barrier_tag{"test"});

    // Barrier should block until members_table includes the node
    ss::sleep(10ms).get();
    BOOST_REQUIRE(!f.available());

    // Populate members_table
    create_brokers(1);
    tests::flush_tasks();

    BOOST_REQUIRE(f.available() && !f.failed());
    f.get();
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
    auto f2 = get_barrier_state(model::node_id{2})
                .barrier(feature_barrier_tag{"test"});

    // The nodes that have entered should not give up or error out,
    // even if it is a long time until the last node participates.
    ss::manual_clock::advance(10s);
    tests::flush_tasks();
    BOOST_REQUIRE(!f0.available());
    BOOST_REQUIRE(!f2.available());

    auto f1 = get_barrier_state(model::node_id{1})
                .barrier(feature_barrier_tag{"test"});
    tests::flush_tasks();

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
    tests::flush_tasks();

    BOOST_REQUIRE(f0.available());
    BOOST_REQUIRE(f1.available());

    // Prompt reactor to process outstanding futures
    tests::flush_tasks();

    restart(model::node_id{2});
    f2 = get_barrier_state(model::node_id{2})
           .barrier(feature_barrier_tag{"test"});

    // Prompt reactor to process outstanding futures
    tests::flush_tasks();

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
    tests::flush_tasks();

    // Advance clock long enough for them to retry and still see an error
    vlog(logger.debug, "Should have just tried first time and failed");
    ss::manual_clock::advance(1000ms);
    tests::flush_tasks();
    vlog(logger.debug, "Should have just retried and failed again");

    rpc_rx_errors.clear();
    rpc_tx_errors.clear();

    // Advance clock far enough for the nodes to all retry (and succeed) their
    // RPCs
    ss::manual_clock::advance(1000ms);
    tests::flush_tasks();
    vlog(logger.debug, "Should have just retried and succeeded");

    BOOST_REQUIRE(f0.available());
    BOOST_REQUIRE(f1.available());
    BOOST_REQUIRE(f2.available());

    f0.get();
    f1.get();
    f2.get();
}

/**
 * Exit during the early part of the barrier, when it is sending
 * RPCs to peers.
 */
FIXTURE_TEST(test_barrier_exit_early, barrier_fixture) {
    create_brokers(3);
    create_node_state(model::node_id{0});
    create_node_state(model::node_id{1});
    create_node_state(model::node_id{2});

    // Block RPCs to node 2, this will prevent node 1 from getting
    // past its RPC-sending phase.
    rpc_rx_errors[model::node_id{2}] = cluster::make_error_code(
      cluster::errc::timeout);
    rpc_tx_errors[model::node_id{2}] = cluster::make_error_code(
      cluster::errc::timeout);

    // Try to barrier on node 0, find that it does not advance
    auto f0 = get_barrier_state(model::node_id{0})
                .barrier(feature_barrier_tag{"test"});
    ss::manual_clock::advance(1000ms);
    tests::flush_tasks();
    BOOST_REQUIRE(!f0.available());

    // Exit on node 0, its barrier future should complete with an error
    kill(model::node_id{0});
    tests::flush_tasks();
    BOOST_REQUIRE(f0.available() && f0.failed());
    try {
        f0.get();
    } catch (ss::sleep_aborted&) {
        // Expected exception
    } catch (...) {
        throw;
    }
}

/**
 * Exit during the early part of the barrier, when it is sending
 * RPCs to peers.
 */
FIXTURE_TEST(test_barrier_exit_late, barrier_fixture) {
    create_brokers(3);
    create_node_state(model::node_id{0});
    create_node_state(model::node_id{1});
    create_node_state(model::node_id{2});

    // Try to barrier on node 0, find that it does not advance
    auto f0 = get_barrier_state(model::node_id{0})
                .barrier(feature_barrier_tag{"test"});
    ss::manual_clock::advance(1000ms);
    tests::flush_tasks();
    BOOST_REQUIRE(!f0.available());

    // Exit on node 0, its barrier future should complete with an error
    kill(model::node_id{0});
    tests::flush_tasks();
    BOOST_REQUIRE(f0.available() && f0.failed());
    try {
        f0.get();
    } catch (ss::broken_condition_variable&) {
        // Expected exception
    } catch (...) {
        throw;
    }
}

SEASTAR_THREAD_TEST_CASE(test_barrier_encoding) {
    feature_barrier_request req{
      .tag = feature_barrier_tag{"ohai"},
      .peer = model::node_id{2},
      .entered = true};
    auto req2 = req;

    iobuf req_io = serde::to_iobuf(std::move(req2));
    auto req_decoded = serde::from_iobuf<feature_barrier_request>(
      std::move(req_io));
    BOOST_REQUIRE_EQUAL(req.tag, req_decoded.tag);
    BOOST_REQUIRE_EQUAL(req.peer, req_decoded.peer);
    BOOST_REQUIRE_EQUAL(req.entered, req_decoded.entered);

    const feature_barrier_response resp{.entered = true, .complete = true};
    auto orig_resp = resp;

    iobuf resp_io = serde::to_iobuf(std::move(orig_resp));
    auto resp_decoded = serde::from_iobuf<feature_barrier_response>(
      std::move(resp_io));
    BOOST_REQUIRE_EQUAL(resp.entered, resp_decoded.entered);
    BOOST_REQUIRE_EQUAL(resp.complete, resp_decoded.complete);
}
