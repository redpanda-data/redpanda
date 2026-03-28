/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

/*
 * Multithreaded end-to-end tests for the sharded L0 garbage collector.
 *
 * These tests run the GC on multiple real Seastar shards to verify:
 * - All shards participate in the GC process
 * - Objects are deleted correctly when running concurrently
 * - No crashes or races in concurrent execution
 *
 */

#include "cloud_topics/level_zero/gc/level_zero_gc.h"
#include "cloud_topics/object_utils.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

using namespace std::chrono_literals;
using namespace cloud_topics;

namespace {

/*
 * Shared state for the object storage mock, held on shard 0.
 */
struct shared_bucket_state {
    // Objects in the bucket - use std::vector<std::string> for safe copying
    std::vector<std::string> objects;

    // Track which shards made list requests (bitset represented as int)
    std::atomic<uint64_t> shards_that_listed{0};

    // Track which shards made delete requests
    std::atomic<uint64_t> shards_that_deleted{0};

    // Total objects deleted (may exceed object count due to concurrent deletes)
    std::atomic<size_t> total_deleted{0};

    // Max GC eligible epoch
    std::optional<int64_t> max_epoch;
};

/*
 * Object storage mock that builds results locally on each shard.
 * Cross-shard calls are only used for tracking and atomic updates.
 */
class mt_object_storage : public l0::gc::object_storage {
public:
    mt_object_storage(shared_bucket_state* bucket_state)
      : g_bucket_state(bucket_state) {}
    ss::future<std::expected<
      cloud_storage_clients::client::list_bucket_result,
      cloud_storage_clients::error_outcome>>
    list_objects(
      ss::abort_source* as,
      std::optional<cloud_storage_clients::object_key> prefix,
      std::optional<ss::sstring>) override {
        if (as->abort_requested()) {
            co_return std::unexpected{
              cloud_storage_clients::error_outcome::fail};
        }
        auto caller_shard = ss::this_shard_id();
        g_bucket_state->shards_that_listed.fetch_or(
          1ULL << caller_shard, std::memory_order_relaxed);

        auto has_prefix = [&prefix](const std::string& k) {
            // if no prefix, allow everything through
            return !prefix.has_value()
                   || k.starts_with(prefix.value()().string());
        };

        auto make_list_item = [](const std::string& k) {
            return cloud_storage_clients::client::list_bucket_item{
              .key = ss::sstring(k),
              .last_modified = std::chrono::system_clock::now() - 24h,
            };
        };

        // reasonably safe under the assumption that we populate objects before
        // starting GC
        auto items = g_bucket_state->objects | std::views::filter(has_prefix)
                     | std::views::transform(make_list_item)
                     | std::ranges::to<chunked_vector<
                       cloud_storage_clients::client::list_bucket_item>>();

        // Sort lexicographically to simulate real cloud storage
        std::ranges::sort(
          items, {}, &cloud_storage_clients::client::list_bucket_item::key);

        co_return cloud_storage_clients::client::list_bucket_result{
          .contents = std::move(items),
        };
    }

    ss::future<std::expected<void, cloud_io::upload_result>> delete_objects(
      ss::abort_source* as,
      chunked_vector<cloud_storage_clients::client::list_bucket_item> objects)
      override {
        if (as->abort_requested()) {
            co_return std::unexpected{cloud_io::upload_result::cancelled};
        }

        auto caller_shard = ss::this_shard_id();
        auto prev_deleted = local_deleted_keys.size();
        local_deleted_keys.insert_range(
          objects
          | std::views::transform([](const auto& item) { return item.key; }));
        auto num_deleted = local_deleted_keys.size() - prev_deleted;

        g_bucket_state->shards_that_deleted.fetch_or(
          1ULL << caller_shard, std::memory_order_relaxed);
        g_bucket_state->total_deleted.fetch_add(
          num_deleted, std::memory_order_relaxed);

        co_return std::expected<void, cloud_io::upload_result>();
    }
    shared_bucket_state* g_bucket_state;

    std::unordered_set<ss::sstring> local_deleted_keys{};
};

/*
 * Epoch source that accesses the global shared state.
 */
class mt_epoch_source : public l0::gc::epoch_source {
public:
    explicit mt_epoch_source(shared_bucket_state* bucket_state)
      : g_bucket_state(bucket_state) {}
    ss::future<std::expected<std::optional<cluster_epoch>, std::string>>
    max_gc_eligible_epoch(ss::abort_source*) override {
        auto epoch = co_await ss::smp::submit_to(
          0, [this]() { return g_bucket_state->max_epoch; });
        if (epoch.has_value()) {
            co_return std::optional<cluster_epoch>{
              cluster_epoch(epoch.value())};
        }
        co_return std::optional<cluster_epoch>{std::nullopt};
    }

    ss::future<std::expected<partitions_snapshot, std::string>>
    get_partitions(ss::abort_source*) override {
        co_return std::unexpected("not implemented");
    }

    ss::future<std::expected<partitions_max_gc_epoch, std::string>>
    get_partitions_max_gc_epoch(ss::abort_source*) override {
        co_return std::unexpected("not implemented");
    }
    shared_bucket_state* g_bucket_state;
};

/*
 * Node info that returns shard index based on the current Seastar shard.
 */
class mt_node_info : public l0::gc::node_info {
public:
    size_t shard_index() const override { return ss::this_shard_id(); }
    size_t total_shards() const override { return ss::smp::count; }
};

class safety_monitor_test_impl : public cloud_topics::l0::gc::safety_monitor {
public:
    result can_proceed() const override {
        return {.ok = true, .reason = std::nullopt};
    }
};

/*
 * Test fixture for multithreaded GC tests.
 */
struct level_zero_gc_mt_test : public seastar_test {
    ss::future<> SetUpAsync() override {
        vassert(ss::this_shard_id() == ss::shard_id{0}, "Setup on not shard 0");
        vassert(ss::smp::count > 1, "Too few shards");
        // Create shared state on shard 0
        g_bucket_state = std::make_unique<shared_bucket_state>();

        // Start GC on all shards
        co_await gc_.start(
          ss::sharded_parameter([] {
              return level_zero_gc_config{
                .deletion_grace_period
                = config::mock_binding<std::chrono::milliseconds>(12h),
                .throttle_progress
                = config::mock_binding<std::chrono::milliseconds>(10ms),
                .throttle_no_progress
                = config::mock_binding<std::chrono::milliseconds>(10ms),
              };
          }),
          ss::sharded_parameter([this] {
              return std::make_unique<mt_object_storage>(g_bucket_state.get());
          }),
          ss::sharded_parameter([this] {
              return std::make_unique<mt_epoch_source>(g_bucket_state.get());
          }),
          ss::sharded_parameter(
            [] { return std::make_unique<mt_node_info>(); }),
          ss::sharded_parameter(
            [] { return std::make_unique<safety_monitor_test_impl>(); }));
    }

    ss::future<> TearDownAsync() override {
        co_await gc_.invoke_on_all(&level_zero_gc::stop);
        co_await gc_.stop();
        std::exchange(g_bucket_state, nullptr);
    }

    // Add objects with various prefixes (call from shard 0 context)
    void populate_objects(size_t count, bool dynamic_epoch = false) {
        g_bucket_state->objects.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            auto prefix = static_cast<object_id::prefix_t>(i % 1000);
            auto id = object_id{
              .epoch = cluster_epoch(
                dynamic_epoch ? static_cast<int64_t>(i) : 1),
              .name = uuid_t::create(),
              .prefix = prefix,
            };
            auto key = object_path_factory::level_zero_path(id);
            g_bucket_state->objects.push_back(key().string());
        }
    }

    void set_max_epoch(int64_t epoch) { g_bucket_state->max_epoch = epoch; }

    size_t get_total_deleted() const {
        return g_bucket_state->total_deleted.load(std::memory_order_relaxed);
    }

    size_t get_shards_that_listed() const {
        return std::popcount(
          g_bucket_state->shards_that_listed.load(std::memory_order_relaxed));
    }

    size_t get_shards_that_deleted() const {
        return std::popcount(
          g_bucket_state->shards_that_deleted.load(std::memory_order_relaxed));
    }

    ss::sharded<level_zero_gc> gc_;
    // Global pointer to shared state (lives on shard 0)
    // This is safe because tests run sequentially and we manage its lifecycle
    std::unique_ptr<shared_bucket_state> g_bucket_state{};
};

static constexpr size_t num_objects = 10000;

} // namespace

/*
 * Test that objects are deleted when running on multiple shards.
 */
TEST_F_CORO(level_zero_gc_mt_test, objects_deleted_across_shards) {
    populate_objects(num_objects);
    set_max_epoch(100);

    EXPECT_EQ(get_total_deleted(), 0);

    co_await gc_.invoke_on_all(&level_zero_gc::start);

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return get_total_deleted() == num_objects; });

    EXPECT_EQ(get_shards_that_deleted(), ss::smp::count);
    EXPECT_EQ(get_shards_that_listed(), ss::smp::count);
}

/*
 * Test that GC works correctly when there are no objects.
 */
TEST_F_CORO(level_zero_gc_mt_test, no_objects_no_crash) {
    set_max_epoch(100);

    // Start GC on all shards with an empty bucket
    co_await gc_.invoke_on_all(&level_zero_gc::start);

    // Give it some time to run
    co_await ss::sleep(500ms);

    // Should complete without crashing or trying to delete anything
    EXPECT_EQ(get_shards_that_listed(), ss::smp::count)
      << "No shards attempted to list";
    EXPECT_EQ(get_shards_that_deleted(), 0);
    EXPECT_EQ(get_total_deleted(), 0);
}

/*
 * Test that GC handles no eligible epoch gracefully.
 */
TEST_F_CORO(level_zero_gc_mt_test, no_eligible_epoch) {
    populate_objects(num_objects);
    // Don't set max_epoch - it will be nullopt

    // Start GC on all shards
    co_await gc_.invoke_on_all(&level_zero_gc::start);

    // Give it some time to run
    co_await ss::sleep(500ms);

    // Objects should not be deleted since there's no eligible epoch
    EXPECT_EQ(get_shards_that_listed(), ss::smp::count)
      << "No shards attempted to list";
    EXPECT_EQ(get_shards_that_deleted(), 0);
    EXPECT_EQ(get_total_deleted(), 0);
}

/*
 * Concurrent reset/start/pause cycles don't crash or corrupt state.
 */
TEST_F_CORO(level_zero_gc_mt_test, concurrent_reset_start_pause) {
    populate_objects(num_objects, true /* dynamic_epoch */);
    set_max_epoch(num_objects / 2 - 1);

    co_await gc_.invoke_on_all(&level_zero_gc::start);
    co_await ss::sleep(100ms);

    std::vector<ss::future<>> futs;

    for (int i = 0; i < 10; ++i) {
        futs.push_back(
          gc_.invoke_on_all([](level_zero_gc& gc) { return gc.reset(); }));
        futs.push_back(
          gc_.invoke_on_all([](level_zero_gc& gc) { return gc.reset(); }));
        futs.push_back(gc_.invoke_on_all(&level_zero_gc::start));
        futs.push_back(gc_.invoke_on_all(&level_zero_gc::pause));
        futs.push_back(gc_.invoke_on_all(&level_zero_gc::start));
    }

    co_await ss::when_all_succeed(std::move(futs));

    co_await gc_.invoke_on_all(&level_zero_gc::start);

    set_max_epoch(num_objects);

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return get_total_deleted() == num_objects; });
}

/*
 * Test start/pause/start cycle works correctly.
 */
TEST_F_CORO(level_zero_gc_mt_test, start_pause_start_cycle) {
    populate_objects(num_objects);
    // don't set max epoch so nothing will be deleted initially

    // Start GC
    co_await gc_.invoke_on_all(&level_zero_gc::start);
    co_await ss::sleep(200ms);

    // Pause GC
    co_await gc_.invoke_on_all(&level_zero_gc::pause);
    co_await ss::sleep(100ms);

    EXPECT_EQ(get_total_deleted(), 0);

    // now bump max epoch so we'll start deleting after unpause
    set_max_epoch(100);

    // Start again
    co_await gc_.invoke_on_all(&level_zero_gc::start);

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return get_total_deleted() == num_objects; });
}
