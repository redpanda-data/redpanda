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
#include "cluster/types.h"
#include "config/mock_property.h"
#include "config/property.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/testing/thread_test_case.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/test/tools/old/interface.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

using namespace std::chrono_literals;
using namespace cluster::tx;

ss::logger logger{"producer_state_test"};
prefix_logger ctx_logger{logger, ""};

struct test_fixture {
    using psm_ptr = std::unique_ptr<producer_state_manager>;

    static constexpr uint64_t default_max_producers = 10;

    void validate_producer_count(size_t registered) {
        BOOST_REQUIRE_EQUAL(
          manager()._cache.get_stats().total_size, registered);
    }

    void validate_namespace_count(size_t registered) {
        BOOST_REQUIRE_EQUAL(
          manager()._cache.get_stats().namespaces.size(), registered);
    }

    void create_producer_state_manager(
      size_t max_producers, size_t min_producers_per_vcluster) {
        _psm = std::make_unique<producer_state_manager>(
          config::mock_binding<size_t>(max_producers),
          config::mock_binding(std::chrono::milliseconds::max()),
          config::mock_binding<size_t>(min_producers_per_vcluster));
        _psm->start().get();
        validate_producer_count(0);
        validate_namespace_count(0);
    }

    ~test_fixture() {
        if (_psm) {
            _psm->stop().get();
        }
    }

    producer_state_manager& manager() { return *_psm; }

    producer_ptr new_producer(
      ss::noncopyable_function<void()> f = [] {},
      std::optional<model::vcluster_id> vcluster = std::nullopt) {
        auto p = ss::make_lw_shared<producer_state>(
          ctx_logger,
          model::random_producer_identity(),
          raft::group_id{_counter++},
          std::move(f));
        manager().register_producer(*p, vcluster);
        return p;
    }

    void clean(std::vector<producer_ptr>& producers) {
        for (auto& producer : producers) {
            manager().deregister_producer(*producer, std::nullopt);
            producer->shutdown_input();
        }
        producers.clear();
    }

    int64_t _counter{0};
    psm_ptr _psm;
};

FIXTURE_TEST(test_locked_producer_is_not_evicted, test_fixture) {
    create_producer_state_manager(10, 10);
    const size_t num_producers = 10;
    std::vector<producer_ptr> producers;
    producers.reserve(num_producers);
    for (int i = 0; i < num_producers; i++) {
        producers.push_back(new_producer());
    }
    // Ensure all producers are registered and linked up
    validate_producer_count(num_producers);

    // run an active operation on producer, should temporarily
    // unlink itself.
    ss::promise<> wait;
    ss::condition_variable wait_for_func_begin;
    auto f = producers[0]->run_with_lock([&](auto units) {
        wait_for_func_begin.signal();
        return wait.get_future().finally([u = std::move(units)] {});
    });

    wait_for_func_begin.wait().get();
    validate_producer_count(num_producers);
    // create one producer more and to trigger eviction
    auto new_p = new_producer();
    validate_producer_count(num_producers);
    // validate that first producer is not evicted
    BOOST_REQUIRE(producers[0]->is_evicted() == false);
    BOOST_REQUIRE(producers[0]->can_evict() == false);
    // unblock the function so producer can link itself back.
    wait.set_value();

    f.get();
    producers.push_back(new_p);
    validate_producer_count(num_producers);

    clean(producers);
    validate_producer_count(0);
}

FIXTURE_TEST(test_inflight_idem_producer_is_not_evicted, test_fixture) {
    create_producer_state_manager(1, 1);
    auto producer = new_producer();
    auto defer = ss::defer(
      [&] { manager().deregister_producer(*producer, std::nullopt); });
    validate_producer_count(1);

    model::test::record_batch_spec spec{
      .offset = model::offset{10},
      .allow_compression = true,
      .count = 7,
      .bt = model::record_batch_type::raft_data,
      .enable_idempotence = true,
      .producer_id = producer->id().id,
      .producer_epoch = producer->id().epoch};
    auto batch = model::test::make_random_batch(spec);
    auto bid = model::batch_identity::from(batch.header());
    auto request = producer->try_emplace_request(bid, model::term_id{1}, true);
    BOOST_REQUIRE(!request.has_error());
    // producer has an inflight request
    BOOST_REQUIRE(!producer->can_evict());
    producer->apply_data(batch.header(), kafka::offset{10});
    BOOST_REQUIRE(producer->can_evict());
}

FIXTURE_TEST(test_inflight_tx_producer_is_not_evicted, test_fixture) {
    create_producer_state_manager(1, 1);
    auto producer = new_producer();
    auto defer = ss::defer(
      [&] { manager().deregister_producer(*producer, std::nullopt); });
    validate_producer_count(1);

    // begin a transaction on the producer
    auto batch = make_fence_batch(
      producer->id(),
      model::tx_seq{0},
      std::chrono::milliseconds{10000},
      model::partition_id{0});

    auto begin_header = batch.header();
    producer->apply_transaction_begin(
      begin_header, read_fence_batch(std::move(batch)));
    BOOST_REQUIRE(producer->has_transaction_in_progress());
    BOOST_REQUIRE(!producer->can_evict());

    // Add some data to the partition.
    model::test::record_batch_spec spec{
      .offset = model::offset{10},
      .allow_compression = true,
      .count = 7,
      .bt = model::record_batch_type::raft_data,
      .enable_idempotence = true,
      .producer_id = producer->id().id,
      .producer_epoch = producer->id().epoch,
      .is_transactional = true};
    batch = model::test::make_random_batch(spec);
    auto bid = model::batch_identity::from(batch.header());
    auto request = producer->try_emplace_request(bid, model::term_id{1}, true);
    BOOST_REQUIRE(!request.has_error());
    // producer has an inflight request
    BOOST_REQUIRE(!producer->can_evict());
    producer->apply_data(batch.header(), kafka::offset{10});
    // transaction is still open, cannot evict.
    BOOST_REQUIRE(!producer->can_evict());
    // commit the transaction.
    producer->apply_transaction_end(model::control_record_type::tx_commit);
    BOOST_REQUIRE(producer->can_evict());
}

FIXTURE_TEST(test_lru_maintenance, test_fixture) {
    create_producer_state_manager(10, 10);
    const size_t num_producers = 5;
    std::vector<producer_ptr> producers;
    producers.reserve(num_producers);
    for (int i = 0; i < num_producers; i++) {
        auto prod = new_producer();
        producers.push_back(prod);
    }
    validate_producer_count(num_producers);

    // run a function on each producer and ensure that is the
    // moved to the end of LRU list
    for (auto& producer : producers) {
        producer->run_with_lock([](auto units) {}).get();
    }

    clean(producers);
    validate_producer_count(0);
}

FIXTURE_TEST(test_eviction_max_pids, test_fixture) {
    create_producer_state_manager(10, 10);
    int evicted_so_far = 0;
    std::vector<producer_ptr> producers;
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

    validate_producer_count(default_max_producers);

    RPTEST_REQUIRE_EVENTUALLY(
      10s, [&] { return evicted_so_far == extra_producers; });

    validate_producer_count(default_max_producers);

    // producers are evicted on an lru basis, so the prefix
    // set of producers should be evicted first.
    for (int i = 0; i < producers.size(); i++) {
        BOOST_REQUIRE_EQUAL(i < extra_producers, producers[i]->is_evicted());
    }

    clean(producers);
}

FIXTURE_TEST(test_state_management_with_multiple_namespaces, test_fixture) {
    size_t total_producers = 20;
    model::vcluster_id vcluster_1 = model::vcluster_id(
      xid::from_string("00000000000000000100"));
    model::vcluster_id vcluster_2 = model::vcluster_id(
      xid::from_string("00000000000000000200"));
    model::vcluster_id vcluster_3 = model::vcluster_id(
      xid::from_string("00000000000000000300"));
    model::vcluster_id vcluster_4 = model::vcluster_id(
      xid::from_string("00000000000000000400"));
    model::vcluster_id vcluster_5 = model::vcluster_id(
      xid::from_string("00000000000000000500"));

    absl::flat_hash_map<model::vcluster_id, size_t> evicted_producers;
    create_producer_state_manager(total_producers, 5);
    struct vcluster_producer {
        model::vcluster_id vcluster;
        producer_ptr producer;
    };
    std::vector<vcluster_producer> producers;
    producers.reserve(default_max_producers);

    auto new_vcluster_producer = [&](model::vcluster_id& vcluster) {
        auto p = new_producer([&] { evicted_producers[vcluster]++; }, vcluster);
        producers.push_back(
          vcluster_producer{.vcluster = vcluster, .producer = p});
    };
    /**
     * Fill producer state manager with producers from one vcluster
     */
    for (int i = 0; i < total_producers; ++i) {
        new_vcluster_producer(vcluster_1);
    }
    validate_producer_count(20);
    validate_namespace_count(1);
    /**
     * Add 3 producers in another vcluster
     */
    for (int i = 0; i < 3; ++i) {
        new_vcluster_producer(vcluster_2);
    }
    validate_producer_count(20);
    // namespace count should increase
    validate_namespace_count(2);
    // 3 producers should be evicted from vcluster_1
    BOOST_REQUIRE_EQUAL(evicted_producers[vcluster_1], 3);

    for (int i = 0; i < 10; ++i) {
        new_vcluster_producer(vcluster_2);
    }

    validate_producer_count(20);
    // namespace count should increase
    validate_namespace_count(2);
    // 10 producers should be evicted from vcluster_1
    BOOST_REQUIRE_EQUAL(evicted_producers[vcluster_1], 10);
    // 3 producers should be evicted from vcluster_2
    BOOST_REQUIRE_EQUAL(evicted_producers[vcluster_2], 3);

    for (int i = 0; i < 5; ++i) {
        new_vcluster_producer(vcluster_3);
        new_vcluster_producer(vcluster_4);
    }
    /**
     * Another five elements should be evicted from each existing vcluster
     */
    BOOST_REQUIRE_EQUAL(evicted_producers[vcluster_1], 15);
    BOOST_REQUIRE_EQUAL(evicted_producers[vcluster_2], 8);

    validate_producer_count(20);
    // namespace count should increase
    validate_namespace_count(4);

    /**
     * No more space in the manager for another vcluster.
     */
    BOOST_REQUIRE_EXCEPTION(
      new_vcluster_producer(vcluster_5),
      cluster::cache_full_error,
      [](const auto& ex) { return true; });

    for (auto vp : producers) {
        manager().deregister_producer(*vp.producer, vp.vcluster);
        vp.producer->shutdown_input();
    }
}
