/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/client_quota_serde.h"
#include "cluster/client_quota_store.h"
#include "config/configuration.h"
#include "kafka/server/quota_manager.h"

#include <seastar/core/sleep.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>

#include <fmt/format.h>

#include <iostream>
#include <limits>
#include <optional>
#include <vector>

static const auto fixed_client_id = "shared-client-id";
static const size_t total_requests{10 << 20};
static const size_t unique_client_id_count = 1000;

std::vector<ss::sstring> initialize_client_ids() {
    std::vector<ss::sstring> client_ids;
    client_ids.reserve(unique_client_id_count);
    for (int i = 0; i < unique_client_id_count; ++i) {
        client_ids.push_back("client-id-" + std::to_string(i));
    }
    return client_ids;
}

std::vector<ss::sstring> unique_client_ids = initialize_client_ids();

ss::future<>
send_requests(kafka::quota_manager& qm, size_t count, bool use_unique) {
    auto offset = ss::this_shard_id() * count;
    for (size_t i = 0; i < count; ++i) {
        auto cid_idx = (offset + i) % unique_client_id_count;
        auto client_id = use_unique ? unique_client_ids[cid_idx]
                                    : fixed_client_id;

        // Have a mixed workload of produce and fetch to highlight any cache
        // contention on produce/fetch token buckets for the same client id
        if (ss::this_shard_id() % 2 == 0) {
            co_await qm.record_fetch_tp(client_id, 1);
            auto delay = co_await qm.throttle_fetch_tp(client_id);
            perf_tests::do_not_optimize(delay);
        } else {
            auto delay = co_await qm.record_produce_tp_and_throttle(
              client_id, 1);
            perf_tests::do_not_optimize(delay);
        }
        co_await maybe_yield();
    }
    co_return;
}

ss::future<> test_quota_manager(size_t count, bool use_unique) {
    ss::sharded<cluster::client_quota::store> quota_store;
    ss::sharded<kafka::quota_manager> sqm;
    co_await quota_store.start();
    co_await sqm.start(std::ref(quota_store));
    co_await sqm.invoke_on_all(&kafka::quota_manager::start);

    perf_tests::start_measuring_time();
    co_await sqm.invoke_on_all([count, use_unique](kafka::quota_manager& qm) {
        return send_requests(qm, count, use_unique);
    });
    perf_tests::stop_measuring_time();
    co_await sqm.stop();
    co_await quota_store.stop();
}

struct throughput_test_case {
    std::optional<uint32_t> fetch_tp;
    bool use_unique;
};

future<size_t> run_tc(throughput_test_case tc) {
    co_await ss::smp::invoke_on_all([fetch_tp{tc.fetch_tp}]() {
        config::shard_local_cfg().target_fetch_quota_byte_rate.set_value(
          fetch_tp);
    });
    co_await test_quota_manager(total_requests / ss::smp::count, tc.use_unique);
    co_return total_requests;
}

struct throughput_group {};

PERF_TEST_CN(throughput_group, test_quota_manager_on_unlimited_shared) {
    return run_tc(throughput_test_case{
      .fetch_tp = std::numeric_limits<uint32_t>::max(),
      .use_unique = false,
    });
}

PERF_TEST_CN(throughput_group, test_quota_manager_on_unlimited_unique) {
    return run_tc(throughput_test_case{
      .fetch_tp = std::numeric_limits<uint32_t>::max(),
      .use_unique = true,
    });
}

PERF_TEST_CN(throughput_group, test_quota_manager_on_limited_shared) {
    return run_tc(throughput_test_case{
      .fetch_tp = 1000,
      .use_unique = false,
    });
}

PERF_TEST_CN(throughput_group, test_quota_manager_on_limited_unique) {
    return run_tc(throughput_test_case{
      .fetch_tp = 1000,
      .use_unique = true,
    });
}

PERF_TEST_CN(throughput_group, test_quota_manager_off_shared) {
    return run_tc(throughput_test_case{
      .fetch_tp = std::nullopt,
      .use_unique = false,
    });
}

PERF_TEST_CN(throughput_group, test_quota_manager_off_unique) {
    return run_tc(throughput_test_case{
      .fetch_tp = std::nullopt,
      .use_unique = true,
    });
}

struct latency_test_case {
    bool is_new_client;
    bool is_produce_not_fetch;
    int n_other_clients;
    bool on_shard_0;
    bool no_quotas;
};

static const size_t n_repeats = 100;

future<size_t> run_latency_test(latency_test_case tc) {
    ss::sharded<cluster::client_quota::store> quota_store;
    ss::sharded<kafka::quota_manager> sqm;
    co_await quota_store.start();
    co_await sqm.start(std::ref(quota_store));
    co_await sqm.invoke_on_all(&kafka::quota_manager::start);

    auto shard = tc.on_shard_0 ? 0 : 1;
    BOOST_ASSERT_MSG(
      shard < ss::smp::count, "Not enough cores available for the benchmark");

    co_await ss::smp::submit_to(
      shard,
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      seastar::coroutine::lambda([&sqm, &quota_store, tc]() -> ss::future<> {
          auto& qm = sqm.local();

          // Try to create a realistic setup
          if (!tc.no_quotas) {
              using cluster::client_quota::entity_key;
              using cluster::client_quota::entity_value;
              auto key = entity_key{entity_key::client_id_default_match{}};
              auto value = entity_value{
                .producer_byte_rate = 1 << 30, .consumer_byte_rate = 1 << 30};
              co_await quota_store.invoke_on_all(
                [&key, &value](cluster::client_quota::store& qs) {
                    qs.set_quota(key, value);
                });
          }

          auto now = kafka::quota_manager::clock::now();

          // Have a non-trivial number of existing clients in the map
          for (int i = 0; i < tc.n_other_clients; i++) {
              co_await qm.record_produce_tp_and_throttle(
                fmt::format("client-{}", i), 1, now);
          }

          // Pre-generate the client-id's used during the benchmark
          auto client_ids = std::vector<ss::sstring>{};
          client_ids.reserve(n_repeats);
          if (tc.is_new_client) {
              for (int i = 0; i < n_repeats; i++) {
                  client_ids.emplace_back(fmt::format("new-client-{}", i));
              }
          } else {
              for (int i = 0; i < n_repeats; i++) {
                  client_ids.emplace_back(fixed_client_id);
              }
              // Ensure that the client id used is already "known"
              co_await qm.record_produce_tp_and_throttle(
                fixed_client_id, 1, now);
          }

          perf_tests::start_measuring_time();

          // Run repeatedly to reduce the overhead of start/stop_measuring_time
          for (int i = 0; i < n_repeats; i++) {
              if (tc.is_produce_not_fetch) {
                  // Produce
                  auto res = co_await qm.record_produce_tp_and_throttle(
                    client_ids[i], 1, now);
                  perf_tests::do_not_optimize(res);
              } else {
                  // Fetch
                  auto res = co_await qm.throttle_fetch_tp(client_ids[i], now);
                  perf_tests::do_not_optimize(res);
                  co_await qm.record_fetch_tp(client_ids[i], 1, now);
              }
          }

          perf_tests::stop_measuring_time();
      }));

    co_await sqm.stop();
    co_await quota_store.stop();

    co_return n_repeats;
}

struct latency_group {};

PERF_TEST_CN(latency_group, existing_client_produce_100_others) {
    return run_latency_test(latency_test_case{
      .is_new_client = false,
      .is_produce_not_fetch = true,
      .n_other_clients = 100,
      .on_shard_0 = true,
    });
}

PERF_TEST_CN(latency_group, existing_client_fetch_100_others) {
    return run_latency_test(latency_test_case{
      .is_new_client = false,
      .is_produce_not_fetch = false,
      .n_other_clients = 100,
      .on_shard_0 = true,
    });
}

PERF_TEST_CN(latency_group, new_client_produce_100_others) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = true,
      .n_other_clients = 100,
      .on_shard_0 = true,
    });
}

PERF_TEST_CN(latency_group, new_client_fetch_100_others) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = false,
      .n_other_clients = 100,
      .on_shard_0 = true,
    });
}

PERF_TEST_CN(latency_group, existing_client_produce_1000_others) {
    return run_latency_test(latency_test_case{
      .is_new_client = false,
      .is_produce_not_fetch = true,
      .n_other_clients = 1000,
      .on_shard_0 = true,
    });
}

PERF_TEST_CN(latency_group, existing_client_fetch_1000_others) {
    return run_latency_test(latency_test_case{
      .is_new_client = false,
      .is_produce_not_fetch = false,
      .n_other_clients = 1000,
      .on_shard_0 = true,
    });
}

PERF_TEST_CN(latency_group, new_client_produce_1000_others) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = true,
      .n_other_clients = 1000,
      .on_shard_0 = true,
    });
}

PERF_TEST_CN(latency_group, new_client_fetch_1000_others) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = false,
      .n_other_clients = 1000,
      .on_shard_0 = true,
    });
}

PERF_TEST_CN(latency_group, existing_client_produce_10000_others) {
    return run_latency_test(latency_test_case{
      .is_new_client = false,
      .is_produce_not_fetch = true,
      .n_other_clients = 10000,
      .on_shard_0 = true,
    });
}

PERF_TEST_CN(latency_group, existing_client_fetch_10000_others) {
    return run_latency_test(latency_test_case{
      .is_new_client = false,
      .is_produce_not_fetch = false,
      .n_other_clients = 10000,
      .on_shard_0 = true,
    });
}

PERF_TEST_CN(latency_group, new_client_produce_10000_others) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = true,
      .n_other_clients = 10000,
      .on_shard_0 = true,
    });
}

PERF_TEST_CN(latency_group, new_client_fetch_10000_others) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = false,
      .n_other_clients = 10000,
      .on_shard_0 = true,
    });
}

PERF_TEST_CN(latency_group, existing_client_produce_100_others_not_shard_0) {
    return run_latency_test(latency_test_case{
      .is_new_client = false,
      .is_produce_not_fetch = true,
      .n_other_clients = 100,
      .on_shard_0 = false,
    });
}

PERF_TEST_CN(latency_group, existing_client_fetch_100_others_not_shard_0) {
    return run_latency_test(latency_test_case{
      .is_new_client = false,
      .is_produce_not_fetch = false,
      .n_other_clients = 100,
      .on_shard_0 = false,
    });
}

PERF_TEST_CN(latency_group, new_client_produce_100_others_not_shard_0) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = true,
      .n_other_clients = 100,
      .on_shard_0 = false,
    });
}

PERF_TEST_CN(latency_group, new_client_fetch_100_others_not_shard_0) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = false,
      .n_other_clients = 100,
      .on_shard_0 = false,
    });
}

PERF_TEST_CN(latency_group, existing_client_produce_1000_others_not_shard_0) {
    return run_latency_test(latency_test_case{
      .is_new_client = false,
      .is_produce_not_fetch = true,
      .n_other_clients = 1000,
      .on_shard_0 = false,
    });
}

PERF_TEST_CN(latency_group, existing_client_fetch_1000_others_not_shard_0) {
    return run_latency_test(latency_test_case{
      .is_new_client = false,
      .is_produce_not_fetch = false,
      .n_other_clients = 1000,
      .on_shard_0 = false,
    });
}

PERF_TEST_CN(latency_group, new_client_produce_1000_others_not_shard_0) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = true,
      .n_other_clients = 1000,
      .on_shard_0 = false,
    });
}

PERF_TEST_CN(latency_group, new_client_fetch_1000_others_not_shard_0) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = false,
      .n_other_clients = 1000,
      .on_shard_0 = false,
    });
}

PERF_TEST_CN(latency_group, existing_client_produce_10000_others_not_shard_0) {
    return run_latency_test(latency_test_case{
      .is_new_client = false,
      .is_produce_not_fetch = true,
      .n_other_clients = 10000,
      .on_shard_0 = false,
    });
}

PERF_TEST_CN(latency_group, existing_client_fetch_10000_others_not_shard_0) {
    return run_latency_test(latency_test_case{
      .is_new_client = false,
      .is_produce_not_fetch = false,
      .n_other_clients = 10000,
      .on_shard_0 = false,
    });
}

PERF_TEST_CN(latency_group, new_client_produce_10000_others_not_shard_0) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = true,
      .n_other_clients = 10000,
      .on_shard_0 = false,
    });
}

PERF_TEST_CN(latency_group, new_client_fetch_10000_others_not_shard_0) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = false,
      .n_other_clients = 10000,
      .on_shard_0 = false,
    });
}

PERF_TEST_CN(latency_group, default_configs_produce_worst) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = true,
      .n_other_clients = 0,
      .on_shard_0 = false,
      .no_quotas = true,
    });
}

PERF_TEST_CN(latency_group, default_configs_fetch_worst) {
    return run_latency_test(latency_test_case{
      .is_new_client = true,
      .is_produce_not_fetch = false,
      .n_other_clients = 0,
      .on_shard_0 = false,
      .no_quotas = true,
    });
}
