// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/client_quota_serde.h"
#include "cluster/client_quota_store.h"
#include "config/configuration.h"
#include "kafka/server/client_quota_translator.h"
#include "kafka/server/quota_manager.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <array>
#include <chrono>

using namespace std::chrono_literals;
using cluster::client_quota::entity_key;
using cluster::client_quota::entity_value;

static const auto default_key = entity_key(
  entity_key::client_id_default_match{});
static const auto user = "alice";
static const auto cid = "franz-go";
static const auto franz_go_key = entity_key{
  entity_key::client_id_prefix_match{"franz-go"}};
static const auto not_franz_go_key = entity_key{
  entity_key::client_id_prefix_match{"not-franz-go"}};

namespace kafka {

struct fixture {
    ss::sharded<cluster::client_quota::store> quota_store;
    ss::sharded<quota_manager> sqm;

    fixture() {
        quota_store.start().get();
        sqm.start(std::ref(quota_store)).get();
        sqm.invoke_on_all(&quota_manager::start).get();
    }

    ~fixture() {
        sqm.stop().get();
        quota_store.stop().get();
    }

    void set_basic_quotas() {
        auto default_values = entity_value{
          .producer_byte_rate = 1024,
          .consumer_byte_rate = 1025,
          .controller_mutation_rate = 1026,
        };

        auto franz_go_values = entity_value{
          .producer_byte_rate = 4096,
          .consumer_byte_rate = 4097,
        };

        auto not_franz_go_values = entity_value{
          .producer_byte_rate = 2048,
          .consumer_byte_rate = 2049,
        };

        quota_store.local().set_quota(default_key, default_values);
        quota_store.local().set_quota(franz_go_key, franz_go_values);
        quota_store.local().set_quota(not_franz_go_key, not_franz_go_values);
    }
};

template<typename F>
ss::future<> set_config(F update) {
    co_await ss::smp::invoke_on_all(
      [update{std::move(update)}]() { update(config::shard_local_cfg()); });
    co_await ss::sleep(std::chrono::milliseconds(1));
}

SEASTAR_THREAD_TEST_CASE(quota_manager_fetch_no_throttling) {
    fixture f;

    auto& qm = f.sqm.local();
    const auto now = quota_manager::clock::now();

    // Test that if fetch throttling is disabled, we don't throttle
    qm.record_fetch_tp(user, cid, 10000000000000, now).get();
    auto delay = qm.throttle_fetch_tp(user, cid, now).get();

    BOOST_CHECK_EQUAL(0ms, delay);
}

SEASTAR_THREAD_TEST_CASE(quota_manager_fetch_throttling) {
    fixture f;

    auto default_values = entity_value{
      .consumer_byte_rate = 100,
    };
    f.quota_store.local().set_quota(default_key, default_values);

    auto& qm = f.sqm.local();

    auto now = quota_manager::clock::now();

    // Test that below the fetch quota we don't throttle
    qm.record_fetch_tp(user, cid, 99, now).get();
    auto delay = qm.throttle_fetch_tp(user, cid, now).get();

    BOOST_CHECK_EQUAL(delay, 0ms);

    // Test that above the fetch quota we throttle
    qm.record_fetch_tp(user, cid, 10, now).get();
    delay = qm.throttle_fetch_tp(user, cid, now).get();

    BOOST_CHECK_GT(delay, 0ms);

    // Test that once we wait out the throttling delay, we don't
    // throttle again (as long as we stay under the limit)
    now += 1s;
    qm.record_fetch_tp(user, cid, 10, now).get();
    delay = qm.throttle_fetch_tp(user, cid, now).get();

    BOOST_CHECK_EQUAL(delay, 0ms);
}

SEASTAR_THREAD_TEST_CASE(quota_manager_fetch_stress_test) {
    fixture f;

    set_config([](config::configuration& conf) {
        conf.max_kafka_throttle_delay_ms.set_value(
          std::chrono::milliseconds::max());
    }).get();

    auto default_values = entity_value{
      .consumer_byte_rate = 100,
    };
    f.quota_store
      .invoke_on_all([&default_values](cluster::client_quota::store& store) {
          store.set_quota(default_key, default_values);
      })
      .get();

    // Exercise the quota manager from multiple cores to attempt to
    // discover segfaults caused by data races/use-after-free
    f.sqm
      .invoke_on_all(
        ss::coroutine::lambda([](quota_manager& qm) -> ss::future<> {
            for (size_t i = 0; i < 1000; ++i) {
                co_await qm.record_fetch_tp(
                  user, cid, 1, quota_manager::clock::now());
                auto delay [[maybe_unused]] = co_await qm.throttle_fetch_tp(
                  user, cid, quota_manager::clock::now());
                co_await ss::maybe_yield();
            }
        }))
      .get();
}

SEASTAR_THREAD_TEST_CASE(static_config_test) {
    fixture f;

    f.set_basic_quotas();

    auto& buckets_map = f.sqm.local().get_global_map_for_testing();
    const auto now = quota_manager::clock::now();

    BOOST_REQUIRE_EQUAL(buckets_map->size(), 0);

    {
        ss::sstring client_id = "franz-go";
        f.sqm.local().record_fetch_tp(user, client_id, 1, now).get();
        f.sqm.local()
          .record_produce_tp_and_throttle(user, client_id, 1, now)
          .get();
        f.sqm.local().record_partition_mutations(user, client_id, 1, now).get();
        auto it = buckets_map->find(k_group_name{client_id});
        BOOST_REQUIRE(it != buckets_map->end());
        BOOST_REQUIRE(it->second->tp_produce_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_produce_rate->rate(), 4096);
        BOOST_REQUIRE(it->second->tp_fetch_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_fetch_rate->rate(), 4097);
        BOOST_REQUIRE(!it->second->pm_rate.has_value());
    }
    {
        ss::sstring client_id = "not-franz-go";
        f.sqm.local().record_fetch_tp(user, client_id, 1, now).get();
        f.sqm.local()
          .record_produce_tp_and_throttle(user, client_id, 1, now)
          .get();
        f.sqm.local().record_partition_mutations(user, client_id, 1, now).get();
        auto it = buckets_map->find(k_group_name{client_id});
        BOOST_REQUIRE(it != buckets_map->end());
        BOOST_REQUIRE(it->second->tp_produce_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_produce_rate->rate(), 2048);
        BOOST_REQUIRE(it->second->tp_fetch_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_fetch_rate->rate(), 2049);
        BOOST_REQUIRE(!it->second->pm_rate.has_value());
    }
    {
        ss::sstring client_id = "unconfigured";
        f.sqm.local().record_fetch_tp(user, client_id, 1, now).get();
        f.sqm.local()
          .record_produce_tp_and_throttle(user, client_id, 1, now)
          .get();
        f.sqm.local().record_partition_mutations(user, client_id, 1, now).get();
        auto it = buckets_map->find(k_client_id{client_id});
        BOOST_REQUIRE(it != buckets_map->end());
        BOOST_REQUIRE(it->second->tp_produce_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_produce_rate->rate(), 1024);
        BOOST_REQUIRE(it->second->tp_fetch_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_fetch_rate->rate(), 1025);
        BOOST_REQUIRE(it->second->pm_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->pm_rate->rate(), 1026);
    }
}

SEASTAR_THREAD_TEST_CASE(update_test) {
    using clock = quota_manager::clock;
    fixture f;

    f.set_basic_quotas();

    auto& buckets_map = f.sqm.local().get_global_map_for_testing();

    auto now = clock::now();
    {
        // Update fetch config
        ss::sstring client_id = "franz-go";
        f.sqm.local().record_fetch_tp(user, client_id, 8194, now).get();
        f.sqm.local()
          .record_produce_tp_and_throttle(user, client_id, 8192, now)
          .get();

        // Increment the franz-go and not-franz-go group fetch quotas
        auto franz_go_values = f.quota_store.local().get_quota(franz_go_key);
        auto not_franz_go_values = f.quota_store.local().get_quota(
          not_franz_go_key);

        // Sanity check that the fetch quotas are present
        auto has_fetch_quota = [](const auto& qv) {
            return qv.has_value() && qv->consumer_byte_rate.has_value();
        };
        BOOST_REQUIRE(has_fetch_quota(franz_go_values));
        BOOST_REQUIRE(has_fetch_quota(not_franz_go_values));

        *franz_go_values->consumer_byte_rate += 1;
        *not_franz_go_values->consumer_byte_rate += 1;

        f.quota_store.local().set_quota(franz_go_key, *franz_go_values);
        f.quota_store.local().set_quota(not_franz_go_key, *not_franz_go_values);

        // Wait for the quota update to propagate
        ss::sleep(std::chrono::milliseconds(1)).get();

        // Check the rate has been updated
        auto it = buckets_map->find(k_group_name{client_id});
        BOOST_REQUIRE(it != buckets_map->end());
        BOOST_REQUIRE(it->second->tp_fetch_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_fetch_rate->rate(), 4098);

        // Check produce is the same bucket
        BOOST_REQUIRE(it->second->tp_produce_rate.has_value());
        auto delay = f.sqm.local()
                       .record_produce_tp_and_throttle(user, client_id, 1, now)
                       .get();
        BOOST_CHECK_EQUAL(delay / 1ms, 1000);
    }

    {
        // Remove produce config
        ss::sstring client_id = "franz-go";
        f.sqm.local().record_fetch_tp(user, client_id, 8196, now).get();
        f.sqm.local()
          .record_produce_tp_and_throttle(user, client_id, 8192, now)
          .get();

        auto franz_go_values = f.quota_store.local().get_quota(franz_go_key);
        BOOST_REQUIRE(
          franz_go_values.has_value()
          && franz_go_values->producer_byte_rate.has_value());
        franz_go_values->producer_byte_rate = std::nullopt;
        f.quota_store.local().set_quota(franz_go_key, *franz_go_values);

        // Wait for the quota update to propagate
        ss::sleep(std::chrono::milliseconds(1)).get();

        // Check the produce rate has been updated on the group
        auto it = buckets_map->find(k_group_name{client_id});
        BOOST_REQUIRE(it != buckets_map->end());
        BOOST_CHECK(!it->second->tp_produce_rate.has_value());

        // Check fetch is the same bucket
        BOOST_REQUIRE(it->second->tp_fetch_rate.has_value());
        auto delay
          = f.sqm.local().throttle_fetch_tp(user, client_id, now).get();
        BOOST_CHECK_EQUAL(delay / 1ms, 1000);

        // Check the new produce rate now applies
        f.sqm.local()
          .record_produce_tp_and_throttle(user, client_id, 8192, now)
          .get();
        auto client_it = buckets_map->find(k_client_id{client_id});
        BOOST_REQUIRE(client_it != buckets_map->end());
        BOOST_REQUIRE(client_it->second->tp_produce_rate.has_value());
        BOOST_CHECK_EQUAL(client_it->second->tp_produce_rate->rate(), 1024);
    }

    {
        // Update fetch config again using the quota store
        ss::sstring client_id = "franz-go";
        auto key = entity_key{entity_key::client_id_match{client_id}};
        auto value = entity_value{.consumer_byte_rate = 16384};
        f.quota_store.local().set_quota(key, value);

        // Wait for the quota update to propagate
        ss::sleep(std::chrono::milliseconds(1)).get();

        // Check the rate has been updated
        auto it = buckets_map->find(k_client_id{client_id});
        BOOST_REQUIRE(it != buckets_map->end());
        BOOST_REQUIRE(it->second->tp_fetch_rate.has_value());
        BOOST_CHECK_EQUAL(it->second->tp_fetch_rate->rate(), 16384);
    }
}

SEASTAR_THREAD_TEST_CASE(test_increasing_specificity) {
    fixture f;

    using clock = quota_manager::clock;

    auto& buckets_map = f.sqm.local().get_global_map_for_testing();
    auto now = clock::now();

    struct request_size {
        uint64_t produce_bytes = 1;
        uint64_t consume_bytes = 1;
        uint32_t n_mutations = 1;
    };

    struct delays {
        clock::duration produce_delay;
        clock::duration consume_delay;
        std::chrono::milliseconds pm_delay;
    };

    const auto make_size = [](uint32_t i) {
        return request_size{
          .produce_bytes = i + 1,
          .consume_bytes = i + 2,
          .n_mutations = i + 3,
        };
    };

    const auto make_records = [&f] [[nodiscard]] (
                                const quota_manager::clock::time_point& now,
                                request_size r = {}) {
        auto produce_delay = f.sqm.local()
                               .record_produce_tp_and_throttle(
                                 user, cid, r.produce_bytes, now)
                               .get();

        f.sqm.local().record_fetch_tp(user, cid, r.consume_bytes, now).get();
        auto consume_delay
          = f.sqm.local().throttle_fetch_tp(user, cid, now).get();

        // partition mutations throttles after we have exceeded the capacity.
        // Add a dummy second call to simplify testing code below
        f.sqm.local()
          .record_partition_mutations(user, cid, r.n_mutations, now)
          .get();
        auto pm_delay
          = f.sqm.local().record_partition_mutations(user, cid, 0, now).get();
        return delays{produce_delay, consume_delay, pm_delay};
    };

    // Sanity: no quotas
    const delays d = make_records(now);
    BOOST_REQUIRE(buckets_map->empty());
    BOOST_REQUIRE_EQUAL(d.produce_delay, 0ms);
    BOOST_REQUIRE_EQUAL(d.consume_delay, 0ms);
    BOOST_REQUIRE_EQUAL(d.pm_delay, 0ms);

    auto kcid = k_client_id{cid};
    auto kgroup = k_group_name{cid};
    auto kuser = k_user{user};

    auto default_cid_match = entity_key::client_id_default_match{};
    auto prefix_cid_match = entity_key::client_id_prefix_match{cid};
    auto exact_cid_match = entity_key::client_id_match{cid};

    auto default_user_match = entity_key::user_default_match{};
    auto exact_user_match = entity_key::user_match{user};

    struct test_case {
        entity_key ekey;
        tracker_key tkey;
    };

    const auto test_cases = std::to_array<test_case>({
      {
        .ekey{default_cid_match},
        .tkey{kcid},
      },
      {
        .ekey{prefix_cid_match},
        .tkey{kgroup},
      },
      {
        .ekey{exact_cid_match},
        .tkey{kcid},
      },
      {
        .ekey{default_user_match},
        .tkey{kuser},
      },
      {
        .ekey{default_user_match, default_cid_match},
        .tkey{std::make_pair(kuser, kcid)},
      },
      {
        .ekey{default_user_match, prefix_cid_match},
        .tkey{std::make_pair(kuser, kgroup)},
      },
      {
        .ekey{default_user_match, exact_cid_match},
        .tkey{std::make_pair(kuser, kcid)},
      },
      {
        .ekey{exact_user_match},
        .tkey{kuser},
      },
      {
        .ekey{exact_user_match, default_cid_match},
        .tkey{std::make_pair(kuser, kcid)},
      },
      {
        .ekey{exact_user_match, prefix_cid_match},
        .tkey{std::make_pair(kuser, kgroup)},
      },
      {
        .ekey{exact_user_match, exact_cid_match},
        .tkey{std::make_pair(kuser, kcid)},
      },
    });

    for (size_t i = 0; i < test_cases.size(); ++i) {
        const auto& tc = test_cases[i];
        request_size max_rate = make_size(10 * i);

        // Refresh buckets: The operations bellow use 2x the quota for each key.
        // By going 5 seconds in the future, buckets are bound to be full again
        now += 5s;
        BOOST_TEST_CONTEXT(i, tc.ekey, tc.tkey) {
            auto quota = entity_value{
              .producer_byte_rate = max_rate.produce_bytes,
              .consumer_byte_rate = max_rate.consume_bytes,
              .controller_mutation_rate = max_rate.n_mutations,
            };
            f.quota_store.local().set_quota(tc.ekey, quota);

            // Record zero bytes to update the global map to new rates
            const delays d = make_records(now, {0, 0, 0});
            // Sanity check: Zero byte-requests should not be throttled
            BOOST_REQUIRE_EQUAL(d.produce_delay, 0ms);
            BOOST_REQUIRE_EQUAL(d.consume_delay, 0ms);
            BOOST_REQUIRE_EQUAL(d.pm_delay, 0ms);

            // Verify that all rates have been updated
            auto it = buckets_map->find(tc.tkey);
            BOOST_REQUIRE(it != buckets_map->end());

            auto& produce_rate = it->second->tp_produce_rate;
            BOOST_REQUIRE(produce_rate.has_value());
            BOOST_REQUIRE_EQUAL(produce_rate->rate(), max_rate.produce_bytes);

            auto& fetch_rate = it->second->tp_fetch_rate;
            BOOST_REQUIRE(fetch_rate.has_value());
            BOOST_REQUIRE_EQUAL(fetch_rate->rate(), max_rate.consume_bytes);

            auto& pm_rate = it->second->pm_rate;
            BOOST_REQUIRE(pm_rate.has_value());
            BOOST_REQUIRE_EQUAL(pm_rate->rate(), max_rate.n_mutations);

            // Requesting double the quota should end up with 1s throttling time
            delays throttle = make_records(
              now,
              {2 * max_rate.produce_bytes,
               2 * max_rate.consume_bytes,
               2 * max_rate.n_mutations});

            BOOST_REQUIRE_EQUAL(throttle.produce_delay, 1s);
            BOOST_REQUIRE_EQUAL(throttle.consume_delay, 1s);
            BOOST_REQUIRE_EQUAL(throttle.pm_delay, 1s);
        }
    }
}
} // namespace kafka
