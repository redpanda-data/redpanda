/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/model/types.h"
#include "cluster_link/shadow_link_report_cache.h"
#include "config/mock_property.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace cluster_link;
using namespace std::chrono_literals;

namespace {

// Mock fetcher for testing that simulates delays
class mock_report_fetcher : public shadow_link_report_fetcher {
public:
    // Track how many times fetch was called
    int fetch_count{0};

    // Control whether fetches succeed or fail
    bool should_fail{false};

    // Configurable delay to simulate slow fetches
    std::chrono::milliseconds fetch_delay{0ms};

    // Last link_name that was fetched
    cluster_link::model::name_t last_fetched_link_name{""};

    ss::future<cluster_link::model::status_report_ret_t> fetch_link_report(
      cluster_link::model::name_t link_name, ss::abort_source& as) override {
        fetch_count++;
        last_fetched_link_name = link_name;

        // Simulate fetch delay
        if (fetch_delay > 0ms) {
            co_await ss::sleep_abortable(fetch_delay, as);
        }

        if (should_fail) {
            co_return std::unexpected(
              cluster_link::errc::failed_to_connect_to_remote_cluster);
        }

        // Create a fake report with some data
        cluster_link::model::shadow_link_status_report response;
        // Note: link_id is still id_t in the report structure, but we receive
        // name_t For testing purposes, use a numeric ID derived from the name
        response.link_id = cluster_link::model::id_t{
          static_cast<int64_t>(std::hash<ss::sstring>{}(link_name()))};

        // Add a fake topic with partition reports
        ::model::topic test_topic{ssx::sformat("test_topic_{}", link_name())};
        cluster_link::rpc::shadow_link_status_topic_response topic_response;
        topic_response.status
          = cluster_link::model::mirror_topic_status::active;

        // Add a fake partition report
        cluster_link::rpc::shadow_topic_partition_leader_report
          partition_report;
        partition_report.partition = ::model::partition_id{0};
        partition_report.source_partition_start_offset = kafka::offset{0};
        partition_report.source_partition_high_watermark = kafka::offset{100};
        partition_report.source_partition_last_stable_offset = kafka::offset{
          100};
        partition_report.shadow_partition_high_watermark = kafka::offset{90};
        partition_report.last_update_time = std::chrono::milliseconds{
          std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count()};

        topic_response.partition_reports[::model::partition_id{0}]
          = partition_report;

        response.topic_responses.emplace(test_topic, std::move(topic_response));

        co_return response;
    }
};

// Test fixture for shadow_link_report_cache tests
class shadow_topic_report_cache_test : public seastar_test {
protected:
    ss::future<> SetUpAsync() override {
        vassert(ss::smp::count > 1, "Tests require multiple shards");
        // Create mock fetcher with configurable delay
        // Create cache with 1 hour TTL (long enough to avoid expiration during
        // tests). Pass the fetcher only on shard 0, other shards get nullptr.
        co_await ttl_config.start(std::chrono::milliseconds(1h));
        co_await cache.start(
          ss::sharded_parameter([this] { return ttl_config.local().bind(); }),
          ss::sharded_parameter(
            [this]() -> std::unique_ptr<shadow_link_report_fetcher> {
                // Only shard 0 gets the fetcher, other shards get nullptr
                if (ss::this_shard_id() == 0) {
                    auto fetcher = std::make_unique<mock_report_fetcher>();
                    fetcher_ptr = fetcher.get();
                    return fetcher;
                }
                return std::unique_ptr<shadow_link_report_fetcher>{};
            }));
        // Start cache on all shards
        co_await cache.invoke_on_all(&shadow_link_report_cache::start);
    }

    ss::future<> TearDownAsync() override {
        co_await cache.stop();
        co_await ttl_config.stop();
    }

    ss::sharded<config::mock_property<std::chrono::milliseconds>> ttl_config;

    ss::sharded<shadow_link_report_cache> cache;
    std::unique_ptr<shadow_link_report_fetcher> fetcher;
    mock_report_fetcher* fetcher_ptr{nullptr};
};

} // anonymous namespace

TEST_F_CORO(shadow_topic_report_cache_test, single_shard_fetch) {
    // Configure fetcher with a small delay to simulate fetching
    fetcher_ptr->fetch_delay = 50ms;

    cluster_link::model::name_t link_name{"test_link_42"};

    // Measure time before fetch
    auto start_time = std::chrono::steady_clock::now();

    // First get should trigger a fetch
    auto report_result = co_await cache.local().get_report(
      link_name, 5s, false);

    auto elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start_time);

    // Verify fetch was called
    EXPECT_EQ(fetcher_ptr->fetch_count, 1);
    EXPECT_EQ(fetcher_ptr->last_fetched_link_name, link_name);

    // Verify report is valid
    ASSERT_TRUE_CORO(report_result.has_value());
    auto report = report_result.value();
    EXPECT_TRUE(report);
    if (report) {
        EXPECT_FALSE(report->topic_responses.empty());
    }

    // Verify fetch delay was respected (should be at least 50ms)
    EXPECT_GE(elapsed_time.count(), 50);

    // Second get should use cache (no additional delay)
    start_time = std::chrono::steady_clock::now();
    auto report2_result = co_await cache.local().get_report(
      link_name, 5s, false);
    elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start_time);

    // Verify no additional fetch
    EXPECT_EQ(fetcher_ptr->fetch_count, 1);

    // Verify cached response is fast (should be much less than 50ms)
    EXPECT_LT(elapsed_time.count(), 20);

    // Verify we got the same cached report
    ASSERT_TRUE_CORO(report2_result.has_value())
      << "Second report fetch failed";
    auto report2 = report2_result.value();
    ASSERT_TRUE_CORO(report2);
    EXPECT_EQ(report.get(), report2.get()); // Same pointer = COW working
}

TEST_F_CORO(shadow_topic_report_cache_test, concurrent_fetches) {
    // Configure fetcher with delay to simulate fetching
    fetcher_ptr->fetch_delay = 100ms;

    // Create multiple link names to fetch concurrently
    cluster_link::model::name_t link1{"link_1"};
    cluster_link::model::name_t link2{"link_2"};
    cluster_link::model::name_t link3{"link_3"};

    auto start_time = std::chrono::steady_clock::now();

    // Issue concurrent fetches for different links
    auto f1 = cache.local().get_report(link1, 5s, false);
    auto f2 = cache.local().get_report(link2, 5s, false);
    auto f3 = cache.local().get_report(link3, 5s, false);

    auto [result1, result2, result3] = co_await ss::when_all_succeed(
      std::move(f1), std::move(f2), std::move(f3));

    auto elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start_time);

    // Verify all fetches completed
    EXPECT_EQ(fetcher_ptr->fetch_count, 3);

    // Verify all reports are valid
    ASSERT_TRUE_CORO(result1.has_value());
    ASSERT_TRUE_CORO(result2.has_value());
    ASSERT_TRUE_CORO(result3.has_value());

    auto report1 = result1.value();
    auto report2 = result2.value();
    auto report3 = result3.value();

    ASSERT_TRUE_CORO(report1);
    ASSERT_TRUE_CORO(report2);
    ASSERT_TRUE_CORO(report3);

    // Test concurrent requests for the SAME link (deduplication)
    fetcher_ptr->fetch_count = 0;
    cluster_link::model::name_t link4{"link_4"};

    start_time = std::chrono::steady_clock::now();

    // Issue multiple concurrent requests for the same link
    auto f4_1 = cache.local().get_report(link4, 5s, false);
    auto f4_2 = cache.local().get_report(link4, 5s, false);
    auto f4_3 = cache.local().get_report(link4, 5s, false);

    auto [result4_1, result4_2, result4_3] = co_await ss::when_all_succeed(
      std::move(f4_1), std::move(f4_2), std::move(f4_3));

    elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start_time);

    // All should get the same report
    ASSERT_TRUE_CORO(result4_1.has_value()) << "report4_1 failed";
    ASSERT_TRUE_CORO(result4_2.has_value()) << "report4_2 failed";
    ASSERT_TRUE_CORO(result4_3.has_value()) << "report4_3 failed";

    auto report4_1 = result4_1.value();
    auto report4_2 = result4_2.value();
    auto report4_3 = result4_3.value();

    ASSERT_TRUE_CORO(report4_1);
    ASSERT_TRUE_CORO(report4_2);
    ASSERT_TRUE_CORO(report4_3);

    // Should only fetch once due to deduplication
    EXPECT_EQ(fetcher_ptr->fetch_count, 1);

    // Time should be ~100ms (single fetch), not 300ms
    EXPECT_LT(elapsed_time.count(), 150);
}

TEST_F_CORO(shadow_topic_report_cache_test, force_refresh) {
    fetcher_ptr->fetch_delay = 30ms;

    cluster_link::model::name_t link_name{"test_link_force_refresh"};

    // First get
    auto result1 = co_await cache.local().get_report(link_name, 5s, false);
    EXPECT_EQ(fetcher_ptr->fetch_count, 1);
    ASSERT_TRUE_CORO(result1.has_value());
    auto report1 = result1.value();
    ASSERT_TRUE_CORO(report1);

    // Force refresh should trigger a new fetch
    auto result2 = co_await cache.local().get_report(link_name, 5s, true);
    EXPECT_EQ(fetcher_ptr->fetch_count, 2);
    ASSERT_TRUE_CORO(result2.has_value());
    auto report2 = result2.value();
    ASSERT_TRUE_CORO(report2);

    // Reports should be different instances (new fetch)
    EXPECT_NE(report1.get(), report2.get());
}

TEST_F_CORO(shadow_topic_report_cache_test, fetch_timeout) {
    // Configure a very long delay that will exceed timeout
    fetcher_ptr->fetch_delay = 10s;

    cluster_link::model::name_t link_name{"test_link_timeout"};

    // Try to get with a short timeout - should return error
    auto result = co_await cache.local().get_report(link_name, 500ms, false);
    EXPECT_FALSE(result.has_value());
    if (!result.has_value()) {
        EXPECT_EQ(
          result.error(), cluster_link::errc::report_generation_timed_out);
    }
}

TEST_F_CORO(shadow_topic_report_cache_test, fetch_error) {
    fetcher_ptr->should_fail = true;
    fetcher_ptr->fetch_delay = 50ms;

    cluster_link::model::name_t link_name{"test_link_error"};

    // First attempt should fail and start retries
    auto result = co_await cache.local().get_report(link_name, 200ms, false);

    // Should return error since fetch failed
    EXPECT_FALSE(result.has_value());

    // Verify fetch was attempted (may retry during timeout window)
    EXPECT_GT(fetcher_ptr->fetch_count, 0);
}

TEST_F_CORO(shadow_topic_report_cache_test, cached_response_performance) {
    fetcher_ptr->fetch_delay = 100ms;

    cluster_link::model::name_t link_name{"test_link_perf"};

    // First fetch - should take ~100ms
    auto start = std::chrono::steady_clock::now();
    auto result1 = co_await cache.local().get_report(link_name, 5s, false);
    auto duration1 = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);

    EXPECT_GE(duration1.count(), 100);
    EXPECT_EQ(fetcher_ptr->fetch_count, 1);
    ASSERT_TRUE_CORO(result1.has_value());
    auto report1 = result1.value();

    // Second fetch - should be cached and fast
    start = std::chrono::steady_clock::now();
    auto result2 = co_await cache.local().get_report(link_name, 5s, false);
    auto duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);

    EXPECT_LT(duration2.count(), 20);
    EXPECT_EQ(fetcher_ptr->fetch_count, 1); // No additional fetch
    ASSERT_TRUE_CORO(result2.has_value());
    auto report2 = result2.value();
    EXPECT_EQ(report1.get(), report2.get()); // Same cached object
}

TEST_F_CORO(shadow_topic_report_cache_test, multiple_links_independent_caches) {
    fetcher_ptr->fetch_delay = 50ms;

    cluster_link::model::name_t link1{"link_alpha"};
    cluster_link::model::name_t link2{"link_beta"};

    // Fetch for link1
    auto result1 = co_await cache.local().get_report(link1, 5s, false);
    EXPECT_EQ(fetcher_ptr->fetch_count, 1);
    ASSERT_TRUE_CORO(result1.has_value());
    auto report1 = result1.value();
    ASSERT_TRUE_CORO(report1);

    // Fetch for link2 - should trigger new fetch
    auto result2 = co_await cache.local().get_report(link2, 5s, false);
    EXPECT_EQ(fetcher_ptr->fetch_count, 2);
    ASSERT_TRUE_CORO(result2.has_value());
    auto report2 = result2.value();
    ASSERT_TRUE_CORO(report2);

    // Fetch link1 again - should use cache
    auto result1_cached = co_await cache.local().get_report(link1, 5s, false);
    EXPECT_EQ(fetcher_ptr->fetch_count, 2); // No new fetch
    ASSERT_TRUE_CORO(result1_cached.has_value());
    auto report1_cached = result1_cached.value();
    EXPECT_EQ(report1.get(), report1_cached.get());

    // Fetch link2 again - should use cache
    auto result2_cached = co_await cache.local().get_report(link2, 5s, false);
    EXPECT_EQ(fetcher_ptr->fetch_count, 2); // No new fetch
    ASSERT_TRUE_CORO(result2_cached.has_value());
    auto report2_cached = result2_cached.value();
    EXPECT_EQ(report2.get(), report2_cached.get());
}

TEST_F_CORO(shadow_topic_report_cache_test, multi_shard_fetch) {
    fetcher_ptr->fetch_delay = 50ms;
    cluster_link::model::name_t link_name{"test_link_multishard"};

    static constexpr auto other_shard = ss::shard_id{1};
    // issue a fetch on shard 1, should be routed to shard0
    co_await cache.invoke_on(other_shard, [link_name](auto& other) {
        return other.get_report(link_name, 5s, false).then([](auto result) {
            // Verify report is valid
            EXPECT_TRUE(result.has_value());
            auto& report = result.value();

            EXPECT_TRUE(report);
            if (report) {
                EXPECT_FALSE(report->topic_responses.empty());
            }
            return ss::now();
        });
    });

    // Verify fetch was called
    EXPECT_EQ(fetcher_ptr->fetch_count, 1);
    EXPECT_EQ(fetcher_ptr->last_fetched_link_name, link_name);
    // Second get on shard 0 / 1 should use cache (no additional delay)
    auto result2 = co_await cache.local().get_report(link_name, 5s, false);
    // Verify no additional fetch
    EXPECT_EQ(fetcher_ptr->fetch_count, 1);
    // Verify we got the same cached report
    ASSERT_TRUE_CORO(result2.has_value()) << "Second report fetch failed";
    auto report2 = result2.value();
    ASSERT_TRUE_CORO(report2);
}

TEST_F_CORO(shadow_topic_report_cache_test, multi_shard_fuzz) {
    // Configure fetcher with random delays
    fetcher_ptr->fetch_delay = 10ms;

    // Test parameters
    static constexpr auto test_duration = 5s;
    static constexpr auto num_links = 5;
    constexpr auto timeout_ms = 2s;

    // Random number generator
    auto start_time = std::chrono::steady_clock::now();
    // Track statistics
    size_t successful_gets{0};
    size_t failed_gets{0};
    size_t total_operations{0};

    ss::gate g;

    // Run fuzz test for 5 seconds
    while (std::chrono::steady_clock::now() - start_time < test_duration) {
        // Randomize parameters
        auto link_name = cluster_link::model::name_t{ssx::sformat(
          "fuzz_link_{}", random_generators::get_int(0, num_links))};
        auto target_shard = tests::random_bool() ? ss::shard_id{0}
                                                 : ss::shard_id{1};
        bool force_refresh = tests::random_bool(); // 10% chance
        auto operation_delay = std::chrono::milliseconds{
          random_generators::get_int(0, 100)};

        total_operations++;

        // Introduce random delay before operation
        if (operation_delay > 0ms) {
            co_await ss::sleep(operation_delay);
        }

        // Launch operation on random shard
        (void)cache
          .invoke_on(
            target_shard,
            [link_name, force_refresh, timeout_ms](auto& cache_instance) {
                return cache_instance
                  .get_report(link_name, timeout_ms, force_refresh)
                  .then([](auto result) { return result.has_value(); });
            })
          .then([&successful_gets, &failed_gets](bool was_success) {
              if (was_success) {
                  successful_gets++;
              } else {
                  failed_gets++;
              }
          })
          .finally([holder = g.hold()] {});

        // Occasionally randomize the fetcher delay
        if (random_generators::get_int(0, 100) >= 80) {
            auto delay = std::chrono::milliseconds{
              random_generators::get_int(0, 200)};
            fetcher_ptr->fetch_delay = std::chrono::milliseconds{delay};
        }
    }

    // Wait for all operations to complete
    co_await g.close();
    // Verify test ran successfully
    auto total_gets = successful_gets + failed_gets;
    EXPECT_GT(total_gets, 0) << "No operations completed";
    EXPECT_GE(total_operations, total_gets);
    // Most operations should succeed (some may timeout due to random delays)
    auto success_rate = static_cast<double>(successful_gets) / total_gets;
    EXPECT_GT(success_rate, 0.5)
      << "Success rate too low: " << (success_rate * 100) << "%";
}
