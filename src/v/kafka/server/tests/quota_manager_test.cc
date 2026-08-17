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
#include "test_utils/async.h"
#include "test_utils/metrics.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <array>
#include <chrono>
#include <string_view>

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

    // Spin until the correct throttling is returned.
    {
        auto wait_until_throttle = [now, &qm, &delay] {
            return tests::cooperative_spin_wait_with_timeout(
              5s, [now, &qm, &delay](this auto) -> ss::future<bool> {
                  delay = co_await qm.throttle_fetch_tp(user, cid, now);
                  co_return delay > 0ms;
              });
        };
        wait_until_throttle().get();
    }

    // Test that once we wait out the throttling delay, we don't
    // throttle again (as long as we stay under the limit)
    now += 1s;
    qm.record_fetch_tp(user, cid, 10, now).get();
    delay = qm.throttle_fetch_tp(user, cid, now).get();

    BOOST_CHECK_EQUAL(delay, 0ms);
}

SEASTAR_THREAD_TEST_CASE(quota_manager_fetch_stress_test) {
    fixture f;

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
        // Spin until the correct throttling is returned.
        {
            auto wait_until_throttle = [now, &f, &delay, client_id] {
                return tests::cooperative_spin_wait_with_timeout(
                  5s,
                  [now, &f, &delay, client_id](this auto) -> ss::future<bool> {
                      delay
                        = co_await f.sqm.local().record_produce_tp_and_throttle(
                          user, client_id, 0, now);
                      co_return delay > 0ms;
                  });
            };
            wait_until_throttle().get();
        }
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
        // Spin until the correct throttling is returned.
        {
            auto wait_until_throttle = [now, &f, &delay, client_id] {
                return tests::cooperative_spin_wait_with_timeout(
                  5s,
                  [now, &f, &delay, client_id](this auto) -> ss::future<bool> {
                      delay = co_await f.sqm.local().throttle_fetch_tp(
                        user, client_id, now);
                      co_return delay > 0ms;
                  });
            };
            wait_until_throttle().get();
        }
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

struct delays {
    quota_manager::clock::duration produce_delay;
    quota_manager::clock::duration consume_delay;
    std::chrono::milliseconds pm_delay;
    friend bool operator==(const delays&, const delays&) = default;
};

std::ostream& operator<<(std::ostream& os, const delays& d) {
    os << ss::format(
      "produce delay: {}, consume_delay: {}, pm_delay: {}",
      d.produce_delay,
      d.consume_delay,
      d.pm_delay);
    return os;
}

constexpr delays no_delay{0ms, 0ms, 0ms};
constexpr delays one_sec{1s, 1s, 1s};

struct request_size {
    uint64_t produce_bytes = 1;
    uint64_t consume_bytes = 1;
    uint32_t n_mutations = 1;
};

constexpr request_size zero_bytes{0, 0, 0};

SEASTAR_THREAD_TEST_CASE(test_increasing_specificity) {
    fixture f;

    using clock = quota_manager::clock;

    auto& buckets_map = f.sqm.local().get_global_map_for_testing();
    auto now = clock::now();

    const auto make_size = [](uint32_t i) {
        return request_size{
          .produce_bytes = i + 1,
          .consume_bytes = i + 2,
          .n_mutations = i + 3,
        };
    };

    const auto make_records = [&f] [[nodiscard]] (
                                this auto,
                                quota_manager::clock::time_point now,
                                request_size r = {}) -> ss::future<delays> {
        auto produce_delay
          = co_await f.sqm.local().record_produce_tp_and_throttle(
            user, cid, r.produce_bytes, now);

        co_await f.sqm.local().record_fetch_tp(user, cid, r.consume_bytes, now);
        auto consume_delay = co_await f.sqm.local().throttle_fetch_tp(
          user, cid, now);

        // partition mutations throttles after we have exceeded the capacity.
        // Add a dummy second call to simplify testing code below
        co_await f.sqm.local().record_partition_mutations(
          user, cid, r.n_mutations, now);
        auto pm_delay = co_await f.sqm.local().record_partition_mutations(
          user, cid, 0, now);
        co_return delays{produce_delay, consume_delay, pm_delay};
    };

    // Sanity: no quotas
    const delays d = make_records(now).get();
    BOOST_REQUIRE(buckets_map->empty());
    BOOST_REQUIRE_EQUAL(d, no_delay);

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
            delays d = make_records(now, zero_bytes).get();

            // Spin until the correct throttling is returned. The token buckets
            // are updated through a relaxed memory model. There is some
            // uncertainty involved here
            {
                auto wait_until_throttle = [now, &make_records, &d] {
                    return tests::cooperative_spin_wait_with_timeout(
                      5s,
                      [now, &make_records, &d](this auto) -> ss::future<bool> {
                          d = co_await make_records(now, zero_bytes);
                          co_return d == no_delay;
                      });
                };
                wait_until_throttle().get();
            }

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
                                 2 * max_rate.n_mutations})
                                .get();

            // Spin until the correct throttling is returned.
            {
                auto wait_until_throttle = [now, &make_records, &throttle] {
                    return tests::cooperative_spin_wait_with_timeout(
                      5s,
                      [now, &make_records, &throttle](
                        this auto) -> ss::future<bool> {
                          throttle = co_await make_records(now, zero_bytes);
                          co_return throttle.produce_delay > 0s
                            && throttle.consume_delay > 0s
                            && throttle.pm_delay > 0s;
                      });
                };
                wait_until_throttle().get();
            }

            BOOST_REQUIRE_EQUAL(throttle, one_sec);
        }
    }
}

namespace {

/// Maps a single-component key type to the component label that carries
/// its value (e.g. "client_id" -> "redpanda_quota_client_id").
ss::sstring component_label_for(std::string_view key_type) {
    if (key_type == "user") {
        return "redpanda_quota_user";
    }
    if (key_type == "client_id") {
        return "redpanda_quota_client_id";
    }
    if (key_type == "group_name") {
        return "redpanda_quota_group_name";
    }
    return {};
}

/// Value of a per-entity counter (`metric_name`) for a single-component
/// entity (user/client_id/group_name) and quota_type, or nullopt if no such
/// series exists. The shard label is added automatically by find_metric_value.
std::optional<uint64_t> entity_metric_value(
  std::string_view metric_name,
  std::string_view key_type,
  std::string_view key_value,
  std::string_view quota_type) {
    return test_utils::find_metric_value<uint64_t>(
      metric_name,
      ss::metrics::default_handle(),
      {{"redpanda_quota_type", ss::sstring(quota_type)},
       {component_label_for(key_type), ss::sstring(key_value)}});
}

std::optional<uint64_t> entity_throughput(
  std::string_view key_type,
  std::string_view key_value,
  std::string_view quota_type) {
    return entity_metric_value(
      "kafka_quotas_client_quota_throughput_by_entity",
      key_type,
      key_value,
      quota_type);
}

std::optional<uint64_t> entity_throttle_time_ms(
  std::string_view key_type,
  std::string_view key_value,
  std::string_view quota_type) {
    return entity_metric_value(
      "kafka_quotas_client_quota_throttle_time_ms_by_entity",
      key_type,
      key_value,
      quota_type);
}

/// True if a per-entity throttle-time series exists for the given entity and
/// quota_type.
bool has_entity_throttle_metric(
  std::string_view key_type,
  std::string_view key_value,
  std::string_view quota_type) {
    return entity_throttle_time_ms(key_type, key_value, quota_type).has_value();
}

/// Issues produce requests for (user, cid) at a fixed timestamp until one is
/// throttled, which is when the per-entity probe is created. Fails the test
/// if no throttle occurs within the timeout.
void produce_until_throttled(
  quota_manager& qm, quota_manager::clock::time_point now) {
    tests::cooperative_spin_wait_with_timeout(
      5s,
      [now, &qm](this auto) -> ss::future<bool> {
          auto delay = co_await qm.record_produce_tp_and_throttle(
            user, cid, 10000, now);
          co_return delay > quota_manager::clock::duration::zero();
      })
      .get();
}

/// Issues partition-mutation requests for (user, cid) at a fixed timestamp
/// until one is throttled, which is when the per-entity probe is created.
/// Fails the test if no throttle occurs within the timeout.
void mutate_until_throttled(
  quota_manager& qm, quota_manager::clock::time_point now) {
    tests::cooperative_spin_wait_with_timeout(
      5s,
      [now, &qm](this auto) -> ss::future<bool> {
          auto delay = co_await qm.record_partition_mutations(
            user, cid, 1000, now);
          co_return delay > quota_manager::clock::duration::zero();
      })
      .get();
}

} // namespace

// When kafka_per_entity_quota_metrics is off (the default), no per-entity
// metric series are registered even when a entity is actively throttled.
SEASTAR_THREAD_TEST_CASE(per_entity_metrics_not_registered_when_disabled) {
    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(false);
    }).get();

    fixture f;

    auto default_values = entity_value{.producer_byte_rate = 100};
    f.quota_store.local().set_quota(default_key, default_values);

    auto& qm = f.sqm.local();
    const auto now = quota_manager::clock::now();

    quota_manager::clock::duration delay;
    tests::cooperative_spin_wait_with_timeout(
      5s,
      [now, &qm, &delay](this auto) -> ss::future<bool> {
          delay = co_await qm.record_produce_tp_and_throttle(
            user, cid, 10000, now);
          co_return delay > quota_manager::clock::duration::zero();
      })
      .get();

    BOOST_CHECK(
      !has_entity_throttle_metric("client_id", "franz-go", "produce_quota"));
}

// With the flag enabled, no metric series is registered until a throttle
// actually fires.
SEASTAR_THREAD_TEST_CASE(per_entity_metrics_not_registered_before_throttle) {
    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(true);
    }).get();

    fixture f;

    // A quota large enough that a single 1-byte record won't exhaust it
    auto default_values = entity_value{.producer_byte_rate = 1000000};
    f.quota_store.local().set_quota(default_key, default_values);

    auto& qm = f.sqm.local();
    const auto now = quota_manager::clock::now();

    const auto delay
      = qm.record_produce_tp_and_throttle(user, cid, 1, now).get();

    BOOST_CHECK_EQUAL(delay, quota_manager::clock::duration::zero());
    BOOST_CHECK(
      !has_entity_throttle_metric("client_id", "franz-go", "produce_quota"));

    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(false);
    }).get();
}

// Calling record_fetch_tp without a subsequent throttle_fetch_tp never
// creates a per-entity metric series, even when the bucket is exhausted.
SEASTAR_THREAD_TEST_CASE(record_fetch_tp_alone_does_not_register_probe) {
    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(true);
    }).get();

    fixture f;

    auto default_values = entity_value{.consumer_byte_rate = 100};
    f.quota_store.local().set_quota(default_key, default_values);

    auto& qm = f.sqm.local();
    const auto now = quota_manager::clock::now();

    for (int i = 0; i < 10; ++i) {
        qm.record_fetch_tp(user, cid, 10000, now).get();
    }

    BOOST_CHECK(
      !has_entity_throttle_metric("client_id", "franz-go", "produce_quota"));

    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(false);
    }).get();
}

// After GC expires the local map entry the per-entity probe is destroyed
// and the metric series is deregistered. A stale timestamp is passed so
// last_seen_ms is already past the expire threshold (now - 10 * full_window)
// the first time GC fires.
SEASTAR_THREAD_TEST_CASE(per_entity_probe_deregistered_on_gc) {
    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(true);
        conf.quota_manager_gc_sec.set_value(std::chrono::milliseconds{10});
        conf.default_window_sec.set_value(std::chrono::milliseconds{1});
        conf.default_num_windows.set_value(static_cast<int16_t>(1));
    }).get();

    fixture f;

    auto default_values = entity_value{.producer_byte_rate = 100};
    f.quota_store.local().set_quota(default_key, default_values);

    auto& qm = f.sqm.local();
    // 1 s in the past puts last_seen_ms well beyond expire_threshold
    // (clock::now() - 10 * 1ms = clock::now() - 10ms).
    const auto stale_now = quota_manager::clock::now() - 1s;

    quota_manager::clock::duration delay;
    tests::cooperative_spin_wait_with_timeout(
      5s,
      [stale_now, &qm, &delay](this auto) -> ss::future<bool> {
          delay = co_await qm.record_produce_tp_and_throttle(
            user, cid, 10000, stale_now);
          co_return delay > quota_manager::clock::duration::zero();
      })
      .get();

    BOOST_REQUIRE(
      has_entity_throttle_metric("client_id", "franz-go", "produce_quota"));

    // No more quota calls - last_seen_ms stays stale. GC fires at ~10ms
    // intervals; the entry is immediately eligible and the probe is destroyed.
    tests::cooperative_spin_wait_with_timeout(
      2s,
      [](this auto) -> ss::future<bool> {
          co_return !has_entity_throttle_metric(
            "client_id", "franz-go", "produce_quota");
      })
      .get();

    BOOST_CHECK(
      !has_entity_throttle_metric("client_id", "franz-go", "produce_quota"));

    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(false);
    }).get();
}

// Toggling kafka_per_entity_quota_metrics off immediately clears all
// per-entity probes without waiting for the GC cycle.
SEASTAR_THREAD_TEST_CASE(per_entity_probe_cleared_on_config_toggle_off) {
    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(true);
    }).get();

    fixture f;

    auto default_values = entity_value{.producer_byte_rate = 100};
    f.quota_store.local().set_quota(default_key, default_values);

    auto& qm = f.sqm.local();
    const auto now = quota_manager::clock::now();

    quota_manager::clock::duration delay;
    tests::cooperative_spin_wait_with_timeout(
      5s,
      [now, &qm, &delay](this auto) -> ss::future<bool> {
          delay = co_await qm.record_produce_tp_and_throttle(
            user, cid, 10000, now);
          co_return delay > quota_manager::clock::duration::zero();
      })
      .get();

    BOOST_REQUIRE(
      has_entity_throttle_metric("client_id", "franz-go", "produce_quota"));

    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(false);
    }).get();

    BOOST_CHECK(
      !has_entity_throttle_metric("client_id", "franz-go", "produce_quota"));
}

// A produce throttle registers the per-entity series and records the
// entity's throughput; a later produce that does not throttle (probe already
// exists) keeps accumulating it.
SEASTAR_THREAD_TEST_CASE(per_entity_produce_records_throughput) {
    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(true);
    }).get();

    fixture f;

    auto default_values = entity_value{.producer_byte_rate = 100};
    f.quota_store.local().set_quota(default_key, default_values);

    auto& qm = f.sqm.local();
    const auto now = quota_manager::clock::now();

    produce_until_throttled(qm, now);

    BOOST_CHECK(
      has_entity_throttle_metric("client_id", "franz-go", "produce_quota"));

    auto throttle_ms = entity_throttle_time_ms(
      "client_id", "franz-go", "produce_quota");
    BOOST_REQUIRE(throttle_ms.has_value());
    BOOST_CHECK(*throttle_ms > 0);

    auto throttled = entity_throughput(
      "client_id", "franz-go", "produce_quota");
    BOOST_REQUIRE(throttled.has_value());
    BOOST_CHECK(*throttled > 0);

    // Far enough in the future that the rate window has fully replenished, so
    // this produce is not throttled but still records throughput because the
    // probe already exists (the else-if branch).
    const auto later = now + 1h;
    auto later_delay
      = qm.record_produce_tp_and_throttle(user, cid, 50, later).get();
    BOOST_CHECK_EQUAL(later_delay, quota_manager::clock::duration::zero());

    auto after = entity_throughput("client_id", "franz-go", "produce_quota");
    BOOST_REQUIRE(after.has_value());
    BOOST_CHECK_EQUAL(*after - *throttled, 50);

    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(false);
    }).get();
}

// Once a fetch throttle has created the probe, record_fetch_tp accumulates the
// entity's fetch throughput.
SEASTAR_THREAD_TEST_CASE(per_entity_fetch_records_throughput) {
    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(true);
    }).get();

    fixture f;

    auto default_values = entity_value{.consumer_byte_rate = 100};
    f.quota_store.local().set_quota(default_key, default_values);

    auto& qm = f.sqm.local();
    const auto now = quota_manager::clock::now();

    qm.record_fetch_tp(user, cid, 10000, now).get();
    quota_manager::clock::duration delay;
    tests::cooperative_spin_wait_with_timeout(
      5s,
      [now, &qm, &delay](this auto) -> ss::future<bool> {
          delay = co_await qm.throttle_fetch_tp(user, cid, now);
          co_return delay > quota_manager::clock::duration::zero();
      })
      .get();

    BOOST_REQUIRE(
      has_entity_throttle_metric("client_id", "franz-go", "fetch_quota"));
    // Bytes recorded before the probe existed are dropped, so the counter
    // starts at 0 here.
    auto before
      = entity_throughput("client_id", "franz-go", "fetch_quota").value_or(0);

    qm.record_fetch_tp(user, cid, 1234, now).get();

    auto after = entity_throughput("client_id", "franz-go", "fetch_quota");
    BOOST_REQUIRE(after.has_value());
    BOOST_CHECK_EQUAL(*after - before, 1234);

    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(false);
    }).get();
}

// A throttled partition-mutation request registers the per-entity series for
// the partition_mutation_quota type and records the entity's throttle time and
// mutation count (throughput is in mutations, not bytes, for this quota type).
SEASTAR_THREAD_TEST_CASE(per_entity_partition_mutations_record_throughput) {
    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(true);
    }).get();

    fixture f;

    f.quota_store.local().set_quota(
      default_key, entity_value{.controller_mutation_rate = 100});

    auto& qm = f.sqm.local();
    const auto now = quota_manager::clock::now();
    mutate_until_throttled(qm, now);

    BOOST_CHECK(has_entity_throttle_metric(
      "client_id", "franz-go", "partition_mutation_quota"));

    auto throttle_ms = entity_throttle_time_ms(
      "client_id", "franz-go", "partition_mutation_quota");
    BOOST_REQUIRE(throttle_ms.has_value());
    BOOST_CHECK(*throttle_ms > 0);

    auto throttled = entity_throughput(
      "client_id", "franz-go", "partition_mutation_quota");
    BOOST_REQUIRE(throttled.has_value());
    BOOST_CHECK(*throttled > 0);

    // Far enough in the future that the rate window has fully replenished, so
    // this mutation is not throttled but still records throughput because the
    // probe already exists (the else branch).
    const auto later = now + 1h;
    auto later_delay = qm.record_partition_mutations(user, cid, 5, later).get();
    BOOST_CHECK_EQUAL(later_delay, 0ms);

    auto after = entity_throughput(
      "client_id", "franz-go", "partition_mutation_quota");
    BOOST_REQUIRE(after.has_value());
    BOOST_CHECK_EQUAL(*after - *throttled, 5);

    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(false);
    }).get();
}

// A user-scoped quota produces a k_user tracker key, labelled with the user in
// quota_user and quota_type=produce_quota.
SEASTAR_THREAD_TEST_CASE(per_entity_user_quota_labels) {
    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(true);
    }).get();

    fixture f;

    f.quota_store.local().set_quota(
      entity_key{entity_key::user_match{user}},
      entity_value{.producer_byte_rate = 100});

    auto& qm = f.sqm.local();
    produce_until_throttled(qm, quota_manager::clock::now());

    BOOST_CHECK(has_entity_throttle_metric("user", user, "produce_quota"));

    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(false);
    }).get();
}

// A client-id-prefix (group) quota produces a k_group_name tracker key,
// labelled with the prefix in quota_group_name and quota_type=produce_quota.
SEASTAR_THREAD_TEST_CASE(per_entity_group_quota_labels) {
    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(true);
    }).get();

    fixture f;

    f.quota_store.local().set_quota(
      franz_go_key, entity_value{.producer_byte_rate = 100});

    auto& qm = f.sqm.local();
    produce_until_throttled(qm, quota_manager::clock::now());

    BOOST_CHECK(
      has_entity_throttle_metric("group_name", "franz-go", "produce_quota"));

    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(false);
    }).get();
}

// A (user, client-id) quota produces a pair tracker key carrying both the
// quota_user and quota_client_id labels - the case the old single-value label
// format could collide on. find_metric_value matches the labels exactly.
SEASTAR_THREAD_TEST_CASE(per_entity_user_client_id_quota_labels) {
    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(true);
    }).get();

    fixture f;

    f.quota_store.local().set_quota(
      entity_key{
        entity_key::user_match{user}, entity_key::client_id_match{cid}},
      entity_value{.producer_byte_rate = 100});

    auto& qm = f.sqm.local();
    produce_until_throttled(qm, quota_manager::clock::now());

    auto series = test_utils::find_metric_value<uint64_t>(
      "kafka_quotas_client_quota_throughput_by_entity",
      ss::metrics::default_handle(),
      {{"redpanda_quota_user", user},
       {"redpanda_quota_client_id", cid},
       {"redpanda_quota_type", "produce_quota"}});
    BOOST_CHECK(series.has_value());

    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(false);
    }).get();
}

// A (user, client-id-prefix) quota produces a pair tracker key carrying both
// the quota_user and quota_group_name labels.
SEASTAR_THREAD_TEST_CASE(per_entity_user_group_quota_labels) {
    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(true);
    }).get();

    fixture f;

    f.quota_store.local().set_quota(
      entity_key{
        entity_key::user_match{user},
        entity_key::client_id_prefix_match{"franz-go"}},
      entity_value{.producer_byte_rate = 100});

    auto& qm = f.sqm.local();
    produce_until_throttled(qm, quota_manager::clock::now());

    auto series = test_utils::find_metric_value<uint64_t>(
      "kafka_quotas_client_quota_throughput_by_entity",
      ss::metrics::default_handle(),
      {{"redpanda_quota_user", user},
       {"redpanda_quota_group_name", "franz-go"},
       {"redpanda_quota_type", "produce_quota"}});
    BOOST_CHECK(series.has_value());

    set_config([](config::configuration& conf) {
        conf.kafka_per_entity_quota_metrics.set_value(false);
    }).get();
}

} // namespace kafka
