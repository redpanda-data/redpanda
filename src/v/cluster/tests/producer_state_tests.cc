/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/producer_state.h"
#include "cluster/producer_state_manager.h"
#include "config/mock_property.h"
#include "model/tests/randoms.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <seastar/testing/thread_test_case.hh>

#include <optional>
#include <vector>

using namespace std::chrono_literals;

ss::logger logger{"producer_state_test"};

struct test_fixture {
    static constexpr uint64_t default_max_producers = 10;

    cluster::producer_state_manager& manager() { return _psm.local(); }

    void check_producers(size_t registered) {
        BOOST_REQUIRE_EQUAL(
          manager()._cache.get_stats().total_size, registered);
    }

    test_fixture() {
        ss::smp::invoke_on_all([]() {
            config::shard_local_cfg().disable_metrics.set_value(true);
        }).get();
        config::shard_local_cfg().disable_metrics.set_value(true);
        _max_producers.start(default_max_producers).get();
        _psm
          .start(ss::sharded_parameter(
            [this] { return _max_producers.local().bind(); }))
          .get();
        _psm.invoke_on_all(&cluster::producer_state_manager::start).get();
        check_producers(0);
    }

    ~test_fixture() {
        _max_producers.stop().get();
        _psm.stop().get();
    }

    cluster::producer_ptr new_producer(ss::noncopyable_function<void()> f = [] {
    }) {
        auto p = ss::make_lw_shared<cluster::producer_state>(
          model::random_producer_identity(),
          raft::group_id{_counter++},
          std::move(f));
        manager().register_producer(*p, std::nullopt);
        return p;
    }

    void clean(std::vector<cluster::producer_ptr>& producers) {
        for (auto& producer : producers) {
            manager().deregister_producer(*producer, std::nullopt);
            producer->shutdown_input();
        }
        producers.clear();
    }

    long _counter = 0;
    ss::sharded<config::mock_property<uint64_t>> _max_producers;
    ss::sharded<cluster::producer_state_manager> _psm;
};

FIXTURE_TEST(test_locked_producer_is_not_evicted, test_fixture) {
    const size_t num_producers = 10;
    std::vector<cluster::producer_ptr> producers;
    producers.reserve(num_producers);
    for (int i = 0; i < num_producers; i++) {
        producers.push_back(new_producer());
    }
    // Ensure all producers are registered and linked up
    check_producers(num_producers);

    // run an active operation on producer, should temporarily
    // unlink itself.
    ss::promise<> wait;
    ss::condition_variable wait_for_func_begin;
    auto f = producers[0]->run_with_lock([&](auto units) {
        wait_for_func_begin.signal();
        return wait.get_future().finally([u = std::move(units)] {});
    });

    wait_for_func_begin.wait().get();
    check_producers(num_producers);
    // create one producer more and to trigger eviction
    auto new_p = new_producer();
    check_producers(num_producers);
    // validate that first producer is not evicted
    BOOST_REQUIRE(producers[0]->is_evicted() == false);
    BOOST_REQUIRE(producers[0]->can_evict() == false);
    // unblock the function so producer can link itself back.
    wait.set_value();

    f.get();
    producers.push_back(new_p);
    check_producers(num_producers);

    clean(producers);
    check_producers(0);
}

FIXTURE_TEST(test_lru_maintenance, test_fixture) {
    const size_t num_producers = 5;
    std::vector<cluster::producer_ptr> producers;
    producers.reserve(num_producers);
    for (int i = 0; i < num_producers; i++) {
        auto prod = new_producer();
        producers.push_back(prod);
    }
    check_producers(num_producers);

    // run a function on each producer and ensure that is the
    // moved to the end of LRU list
    for (auto& producer : producers) {
        producer->run_with_lock([](auto units) {}).get();
    }

    clean(producers);
    check_producers(0);
}

FIXTURE_TEST(test_eviction_max_pids, test_fixture) {
    int evicted_so_far = 0;
    std::vector<cluster::producer_ptr> producers;
    producers.reserve(default_max_producers);
    for (int i = 0; i < default_max_producers; i++) {
        producers.push_back(new_producer([&] { evicted_so_far++; }));
    }
    BOOST_REQUIRE_EQUAL(evicted_so_far, 0);

    // we are already at the limit, add a few more producers
    size_t extra_producers = 5;
    for (int i = 0; i < extra_producers; i++) {
        producers.push_back(new_producer([&] { evicted_so_far++; }));
    }

    check_producers(default_max_producers);

    RPTEST_REQUIRE_EVENTUALLY(
      10s, [&] { return evicted_so_far == extra_producers; });

    check_producers(default_max_producers);

    // producers are evicted on an lru basis, so the prefix
    // set of producers should be evicted first.
    for (int i = 0; i < producers.size(); i++) {
        BOOST_REQUIRE_EQUAL(i < extra_producers, producers[i]->is_evicted());
    }

    clean(producers);
}
