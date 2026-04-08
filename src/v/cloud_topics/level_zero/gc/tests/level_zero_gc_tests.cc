/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/gc/level_zero_gc.h"
#include "cloud_topics/object_utils.h"
#include "config/mock_property.h"
#include "ssx/mutex.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

using namespace std::chrono_literals;

namespace {
struct gc_test_config {
    size_t list_page_size{std::numeric_limits<int>::max()};
    std::chrono::milliseconds list_cost{0ms};
    std::chrono::milliseconds delete_cost{0ms};
};
constexpr size_t prefix_max = cloud_topics::object_id::prefix_max;
constexpr size_t n_prefixes = prefix_max + 1;
} // namespace

class object_storage_test_impl : public cloud_topics::l0::gc::object_storage {
public:
    object_storage_test_impl(
      chunked_vector<cloud_storage_clients::client::list_bucket_item>* listed,
      std::unordered_set<ss::sstring>* deleted,
      gc_test_config* cfg)
      : listed_(listed)
      , deleted_(deleted)
      , cfg_(cfg) {}

    /*
     * Returns objects in `listed_` that aren't in `deleted_`.
     */
    seastar::future<std::expected<
      cloud_storage_clients::client::list_bucket_result,
      cloud_storage_clients::error_outcome>>
    list_objects(
      seastar::abort_source* as,
      std::optional<cloud_storage_clients::object_key> prefix,
      std::optional<ss::sstring> continuation_token) override {
        if (as->abort_requested()) {
            co_return std::unexpected{
              cloud_storage_clients::error_outcome::fail};
        }
        ++list_call_count_;
        chunked_vector<cloud_storage_clients::client::list_bucket_item> keep;
        co_await seastar::sleep(cfg_->list_cost);
        auto lu = co_await list_mtx_.get_units(*as);
        for (const auto& object : *listed_) {
            if (continuation_token.has_value()) {
                if (object.key == continuation_token) {
                    continuation_token.reset();
                }
                continue;
            }
            if (
              prefix.has_value()
              && !object.key.starts_with(prefix.value()().string())) {
                continue;
            }
            auto not_deleted = true;
            {
                auto du = co_await delete_mtx_.get_units(*as);
                not_deleted = !deleted_->contains(object.key);
            }
            if (not_deleted) {
                keep.push_back(object);
            }
            if (keep.size() >= cfg_->list_page_size) {
                break;
            }
        }

        auto continuation = keep.empty() ? ss::sstring{} : keep.back().key;

        co_return cloud_storage_clients::client::list_bucket_result{
          .is_truncated = !continuation.empty(),
          .next_continuation_token = std::move(continuation),
          .contents = std::move(keep),
        };
    }

    /*
     * Adds deleted objects to the `deleted_` set.
     */
    seastar::future<std::expected<void, cloud_io::upload_result>>
    delete_objects(
      seastar::abort_source* as,
      chunked_vector<cloud_storage_clients::client::list_bucket_item> objects)
      override {
        if (as->abort_requested()) {
            co_return std::unexpected{cloud_io::upload_result::cancelled};
        }
        auto abort = as->subscribe([this]() noexcept { delete_cv_.broken(); });
        co_await delete_cv_.wait([this] { return !deletes_blocked_; });
        co_await seastar::sleep(cfg_->delete_cost);
        auto u = co_await delete_mtx_.get_units(*as);
        deleted_->insert_range(
          std::move(objects)
          | std::views::transform([](auto& s) { return std::move(s.key); }));
        co_return std::expected<void, cloud_io::upload_result>();
    }

    void block_deletes() { deletes_blocked_ = true; }
    void unblock_deletes() {
        deletes_blocked_ = false;
        delete_cv_.broadcast();
    }
    bool has_delete_waiters() const { return delete_cv_.has_waiters(); }

    chunked_vector<cloud_storage_clients::client::list_bucket_item>* listed_;
    std::unordered_set<ss::sstring>* deleted_;
    gc_test_config* cfg_;
    uint64_t list_call_count_{0};

    ssx::mutex list_mtx_{"object-store-impl-list"};
    ssx::mutex delete_mtx_{"object-store-impl-delete"};

private:
    bool deletes_blocked_{false};
    seastar::condition_variable delete_cv_;
};

class epoch_source_test_impl : public cloud_topics::l0::gc::epoch_source {
public:
    explicit epoch_source_test_impl(std::optional<int64_t>* epoch)
      : epoch_(epoch) {}

    /*
     * Returns the configured epoch.
     */
    seastar::future<
      std::expected<std::optional<cloud_topics::cluster_epoch>, std::string>>
    max_gc_eligible_epoch(seastar::abort_source*) override {
        if (epoch_->has_value()) {
            co_return cloud_topics::cluster_epoch(epoch_->value());
        }
        co_return std::nullopt;
    }

    seastar::future<std::expected<partitions_snapshot, std::string>>
    get_partitions(seastar::abort_source*) override {
        /*
         * this impl only cares about the final derived value
         */
        co_return std::unexpected("unimplemented");
    }

    seastar::future<std::expected<partitions_max_gc_epoch, std::string>>
    get_partitions_max_gc_epoch(seastar::abort_source*) override {
        /*
         * this impl only cares about the final derived value
         */
        co_return std::unexpected("unimplemented");
    }

private:
    std::optional<int64_t>* epoch_;
};

/*
 * Configurable node_info implementation for testing different shard
 * configurations. Unlike the production node_info_impl which computes shard
 * indices based on the members table, this allows direct control over the
 * shard index and total shard count.
 */
class node_info_test_impl : public cloud_topics::l0::gc::node_info {
public:
    node_info_test_impl() = default;
    node_info_test_impl(size_t shard_idx, size_t total)
      : shard_idx_(shard_idx)
      , total_shards_(total) {}
    size_t shard_index() const final { return shard_idx_; }
    size_t total_shards() const final { return total_shards_; }

private:
    size_t shard_idx_{0};
    size_t total_shards_{1};
};

class safety_monitor_test_impl : public cloud_topics::l0::gc::safety_monitor {
    static constexpr bool default_ok{true};

public:
    safety_monitor_test_impl() = default;
    explicit safety_monitor_test_impl(bool* ok)
      : ok_(ok) {}

    result can_proceed() const override {
        if (*ok_) {
            return {.ok = true, .reason = std::nullopt};
        }
        return {.ok = false, .reason = "test: unsafe"};
    }

private:
    const bool* ok_{&default_ok};
};

class LevelZeroGCTest : public testing::Test {
public:
    LevelZeroGCTest(
      std::chrono::milliseconds throttle_progress = 10ms,
      std::chrono::milliseconds throttle_no_progress = 10ms) {
        auto storage = std::make_unique<object_storage_test_impl>(
          &listed, &deleted, &cfg);
        storage_ = storage.get();
        gc = std::make_unique<cloud_topics::level_zero_gc>(
          cloud_topics::level_zero_gc_config{
            .deletion_grace_period = grace_period_.bind(),
            .throttle_progress
            = config::mock_binding<std::chrono::milliseconds>(
              throttle_progress),
            .throttle_no_progress
            = config::mock_binding<std::chrono::milliseconds>(
              throttle_no_progress),
          },
          std::move(storage),
          std::make_unique<epoch_source_test_impl>(&max_epoch),
          std::make_unique<node_info_test_impl>(),
          std::make_unique<safety_monitor_test_impl>(&safety_ok),
          [](ss::lowres_clock::duration) { return 0ms; });
    }

    void TearDown() override { gc->stop().get(); }

    /*
     * Insert an entry into the `listed` container which is the source of
     * objects reported by `list_objects`.
     */
    void add_listed(int64_t epoch, std::chrono::milliseconds age) {
        auto key = cloud_topics::object_path_factory::level_zero_path(
          cloud_topics::object_id{
            .epoch = cloud_topics::cluster_epoch(epoch),
            .name = uuid_t::create(),
            .prefix = 0,
          });
        cloud_storage_clients::client::list_bucket_item item{
          .key = key().string(),
          .last_modified = std::chrono::system_clock::now() - age,
        };
        listed.push_back(item);
    }

    chunked_vector<cloud_storage_clients::client::list_bucket_item> listed;
    std::unordered_set<ss::sstring> deleted;
    std::optional<int64_t> max_epoch;
    config::mock_property<std::chrono::milliseconds> grace_period_{
      std::chrono::milliseconds{12h}};
    std::unique_ptr<cloud_topics::level_zero_gc> gc;
    gc_test_config cfg{};
    object_storage_test_impl* storage_{nullptr};
    bool safety_ok{true};
};

template<typename Func>
::testing::AssertionResult Eventually(
  Func func, int retries = 50, std::chrono::milliseconds delay = 20ms) {
    while (retries-- > 0) {
        if (func()) {
            return ::testing::AssertionSuccess();
        }
        seastar::sleep_abortable(delay).get();
    }
    return ::testing::AssertionFailure() << "Timeout";
}

class LevelZeroGCSafetyTest : public LevelZeroGCTest {};

TEST_F(LevelZeroGCSafetyTest, ProceedsWhenSafe) {
    for (int i = 0; i < 100; ++i) {
        add_listed(i, 24h);
    }
    max_epoch = 100;
    safety_ok = true;
    gc->start().get();
    EXPECT_TRUE(Eventually([this] { return deleted.size() == 100; }));
}

TEST_F(LevelZeroGCSafetyTest, BlockedWhenUnsafe) {
    for (int i = 0; i < 100; ++i) {
        add_listed(i, 24h);
    }
    max_epoch = 100;
    safety_ok = false;
    gc->start().get();
    EXPECT_FALSE(Eventually([this] { return deleted.size() > 0; }));
}

TEST_F(LevelZeroGCSafetyTest, ResumesAfterSafetyRestored) {
    for (int i = 0; i < 100; ++i) {
        add_listed(i, 24h);
    }
    max_epoch = 100;
    safety_ok = false;
    gc->start().get();

    // GC should not delete anything while unsafe
    EXPECT_FALSE(Eventually([this] { return deleted.size() > 0; }));

    // Flip to safe
    safety_ok = true;

    // GC should now proceed
    EXPECT_TRUE(Eventually([this] { return deleted.size() == 100; }));
}

// all 100 objects are deleted
TEST_F(LevelZeroGCTest, ListedIsDeleted) {
    for (int i = 0; i < 100; ++i) {
        add_listed(i, 24h);
    }
    this->max_epoch = 100;
    gc->start().get();
    EXPECT_TRUE(Eventually([this] { return deleted.size() == 100; }));
}

// all the objects below the max epoch are deleted
TEST_F(LevelZeroGCTest, ListedIsDeletedBelowEpoch) {
    for (int i = 0; i < 100; ++i) {
        add_listed(i, 24h);
    }
    this->max_epoch = 49;
    gc->start().get();
    EXPECT_TRUE(Eventually([this] { return deleted.size() == 50; }));
}

// none are deleted when max_epoch is not available
TEST_F(LevelZeroGCTest, NoDeletesWithoutMaxEpoch) {
    for (int i = 0; i < 100; ++i) {
        add_listed(i, 24h);
    }
    gc->start().get();
    EXPECT_FALSE(Eventually([this] { return deleted.size() > 0; }));
}

// recently created objects aren't deleted
TEST_F(LevelZeroGCTest, NoDeletesForYoungObjects) {
    for (int i = 0; i < 100; ++i) {
        add_listed(i, std::chrono::hours(i));
    }
    this->max_epoch = 100;
    gc->start().get();
    EXPECT_TRUE(Eventually([this] { return deleted.size() == 88; }));
}

// reset while paused keeps GC paused
TEST_F(LevelZeroGCTest, ResetWhilePaused) {
    for (int i = 0; i < 50; ++i) {
        add_listed(i, 24h);
    }
    this->max_epoch = 50;
    gc->start().get();
    EXPECT_TRUE(Eventually([this] { return deleted.size() == 50; }));

    gc->pause().get();
    gc->reset().get();

    // GC should remain paused — no new deletes
    EXPECT_FALSE(Eventually([this] { return deleted.size() > 50; }, 10));
}

// reset while running resumes collection automatically
TEST_F(LevelZeroGCTest, ResetWhileRunning) {
    for (int i = 0; i < 100; ++i) {
        add_listed(i, 24h);
    }
    this->max_epoch = 50;
    gc->start().get();

    // Wait for some progress
    EXPECT_TRUE(Eventually([this] { return !deleted.empty(); }));

    // Reset while running — should resume and eventually delete all
    gc->reset().get();

    this->max_epoch = 100;

    EXPECT_TRUE(Eventually([this] { return deleted.size() == 100; }));
}

// reset on a GC that was never started is a no-op
TEST_F(LevelZeroGCTest, ResetBeforeStart) {
    for (int i = 0; i < 10; ++i) {
        add_listed(i, 24h);
    }
    this->max_epoch = 10;

    // Reset before ever starting — should not crash
    gc->reset().get();

    // Now start and verify it works normally
    gc->start().get();
    EXPECT_TRUE(Eventually([this] { return deleted.size() == 10; }));
}

// concurrent reset is a no-op: the second reset returns immediately while the
// first is still draining, and the resetting state is observable
TEST_F(LevelZeroGCTest, ResetConcurrentOps) {
    for (int i = 0; i < 50; ++i) {
        add_listed(i, 24h);
    }
    this->max_epoch = 50;

    // Block deletes before starting — GC will list objects and submit
    // delete tasks, but they'll block on the CV inside the mock storage.
    // This means the gate holds open fibers when reset() tries gate_.close().
    storage_->block_deletes();
    gc->start().get();

    // Wait for the worker loop to have submitted at least one delete task.
    EXPECT_TRUE(Eventually(
      [this] { return storage_->has_delete_waiters(); }, 5 /* wait ~100ms */));

    // Kick off the first reset — it will block waiting for gate_.close()
    auto reset_fut = gc->reset();

    // Give it a chance to enter the resetting state
    EXPECT_TRUE(Eventually(
      [this] {
          return gc->get_state() == cloud_topics::l0::gc::state::resetting;
      },
      5 /* wait ~100ms */));

    // A second concurrent reset should return immediately (no-op)
    gc->reset().get();

    // start() blocks on the reset CV — launch it in the background
    auto start_fut = gc->start();
    EXPECT_FALSE(start_fut.available());

    // Still resetting (first reset is blocked)
    EXPECT_EQ(gc->get_state(), cloud_topics::l0::gc::state::resetting);
    EXPECT_EQ(deleted.size(), 0);

    // Unblock deletes — reset completes, which signals the CV, unblocking
    // start()
    storage_->unblock_deletes();
    reset_fut.get();
    start_fut.get();

    // After reset completes, GC resumes and finishes the work
    EXPECT_TRUE(Eventually([this] { return deleted.size() == 50; }));
}

/*
 * Returns copies of the configured output for the two pure virtual methods.
 * This fixture is intended to test the default implementation of the max gc
 * eligible epoch calculation, so that is not overriden.
 */
class epoch_source_test_default_impl
  : public cloud_topics::l0::gc::epoch_source {
public:
    explicit epoch_source_test_default_impl(
      partitions_snapshot* get_partitions_value,
      partitions_max_gc_epoch* get_partitions_max_gc_epoch_value)
      : get_partitions_value_(get_partitions_value)
      , get_partitions_max_gc_epoch_value_(get_partitions_max_gc_epoch_value) {}

    seastar::future<std::expected<partitions_snapshot, std::string>>
    get_partitions(seastar::abort_source*) override {
        // manually copy out from the fragmented map structure
        const auto& src = *get_partitions_value_;
        partitions_snapshot::partition_map partitions;
        for (const auto& entry : src.partitions) {
            partitions[entry.first] = entry.second.copy();
        }
        co_return partitions_snapshot{
          .partitions = std::move(partitions),
          .snap_revision = src.snap_revision,
        };
    }

    seastar::future<std::expected<partitions_max_gc_epoch, std::string>>
    get_partitions_max_gc_epoch(seastar::abort_source*) override {
        // manually copy the two-level fragmented map structure
        const auto& src = *get_partitions_max_gc_epoch_value_;
        partitions_max_gc_epoch ret;
        for (const auto& entry : src) {
            chunked_hash_map<model::partition_id, cloud_topics::cluster_epoch>
              copy;
            for (const auto& partition : entry.second) {
                copy[partition.first] = partition.second;
            }
            ret[entry.first] = std::move(copy);
        }
        co_return ret;
    }

private:
    partitions_snapshot* get_partitions_value_;
    partitions_max_gc_epoch* get_partitions_max_gc_epoch_value_;
};

class LevelZeroGCMaxEpochTest : public testing::Test {
public:
    using epoch_source_type = cloud_topics::l0::gc::epoch_source;
    using partitions_snapshot = epoch_source_type::partitions_snapshot;
    using partitions_max_gc_epoch = epoch_source_type::partitions_max_gc_epoch;

    LevelZeroGCMaxEpochTest()
      : epoch_source(
          std::make_unique<epoch_source_test_default_impl>(
            &get_partitions_value, &get_partitions_max_gc_epoch_value)) {}

    partitions_snapshot get_partitions_value;
    partitions_max_gc_epoch get_partitions_max_gc_epoch_value;
    std::unique_ptr<epoch_source_type> epoch_source;

    // shortcut accessors
    auto max_gc() { return epoch_source->max_gc_eligible_epoch(nullptr).get(); }
    auto& snapshot() { return get_partitions_value; }
    auto& partition_epochs() { return get_partitions_max_gc_epoch_value; }
};

TEST_F(LevelZeroGCMaxEpochTest, EmptySnapshot) {
    // no error
    ASSERT_TRUE(max_gc().has_value());
    // no result
    ASSERT_FALSE(max_gc().value().has_value());
}

namespace {
model::topic_namespace tpns0(model::ns("ns0"), model::topic("t0"));
}

TEST_F(LevelZeroGCMaxEpochTest, EmptyGcEpochReport) {
    // here we have a non-empty snapshot, but no reported epochs from individual
    // partitions. in this case there should be no result after the join
    get_partitions_value.partitions[tpns0].push_back(model::partition_id(0));
    ASSERT_FALSE(max_gc().has_value());
    ASSERT_EQ(
      max_gc().error(),
      "Topic '{ns0/t0}' in snapshot has no reported max GC epoch");

    // now we add the topic/ns to the report, but report on a different
    // partition so there is still no output in the join
    get_partitions_max_gc_epoch_value[tpns0][model::partition_id(1)]
      = cloud_topics::cluster_epoch(0);
    ASSERT_FALSE(max_gc().has_value());
    ASSERT_EQ(
      max_gc().error(),
      "Partition '{ns0/t0}/0' in snapshot has no reported max GC epoch");
}

TEST_F(LevelZeroGCMaxEpochTest, MinReduce) {
    get_partitions_value.snap_revision = cloud_topics::cluster_epoch(100);
    get_partitions_value.partitions[tpns0].push_back(model::partition_id(0));

    // the minimum is the last applied
    get_partitions_max_gc_epoch_value[tpns0][model::partition_id(0)]
      = cloud_topics::cluster_epoch(200);
    ASSERT_EQ(max_gc().value().value(), cloud_topics::cluster_epoch(100));

    // now its the epoch from the report
    get_partitions_max_gc_epoch_value[tpns0][model::partition_id(0)]
      = cloud_topics::cluster_epoch(50);
    ASSERT_EQ(max_gc().value().value(), cloud_topics::cluster_epoch(50));
}

TEST_F(LevelZeroGCMaxEpochTest, EmptySnapshotNonEmptyReport) {
    get_partitions_max_gc_epoch_value[tpns0][model::partition_id(0)]
      = cloud_topics::cluster_epoch(200);
    ASSERT_TRUE(max_gc().has_value());
    ASSERT_FALSE(max_gc().value().has_value());
}

class LevelZeroGCScaleOutTest : public LevelZeroGCTest {
public:
    LevelZeroGCScaleOutTest()
      : LevelZeroGCTest(1s, 2s) {}
};

TEST_F(LevelZeroGCScaleOutTest, MultiPageDelete) {
    static constexpr size_t list_page_size{100};
    int n = list_page_size * 4;
    for (int i = 0; i < n; i++) {
        add_listed(i, 24h);
    }
    this->max_epoch = n;
    this->cfg.list_page_size = list_page_size;
    gc->start().get();
    EXPECT_TRUE(Eventually(
      [this, expected = (size_t)n] { return deleted.size() == expected; }));
}

TEST_F(LevelZeroGCScaleOutTest, CleanShutdown) {
    static constexpr size_t list_page_size{10};
    int n = list_page_size * 10;
    for (int i = 0; i < n; i++) {
        add_listed(i, 24h);
    }
    this->max_epoch = n;
    this->cfg.list_page_size = list_page_size;
    this->cfg.delete_cost = 200ms;
    gc->start().get();
    // wait until we process one page
    EXPECT_TRUE(Eventually([this] { return !deleted.empty(); }));
    // then immediately shutdown gc
    gc->stop().get();
}

TEST_F(LevelZeroGCScaleOutTest, ConcurrentDeletes) {
    static constexpr size_t list_page_size{20};
    int n = list_page_size * 20;
    for (int i = 0; i < n; i++) {
        add_listed(i, 24h);
    }
    this->max_epoch = n;
    this->cfg.list_page_size = list_page_size;
    this->cfg.delete_cost = 100ms;
    gc->start().get();
    EXPECT_TRUE(Eventually(
      [this, expected = (size_t)n] { return deleted.size() == expected; }));
}

// make sure we make progress when the total size of eligible keys exceeds the
// in-flight limit
// i.e. 50 * 500 * len(key)=72 ~= 1.5MiB > 1MiB
TEST_F(LevelZeroGCScaleOutTest, ConcurrentDeletesPipelineSaturation) {
    static constexpr size_t list_page_size{500};
    int n = list_page_size * 50;
    for (int i = 0; i < n; i++) {
        add_listed(i, 24h);
    }
    this->max_epoch = n;
    this->cfg.list_page_size = list_page_size;
    this->cfg.delete_cost = 50ms;
    gc->start().get();
    EXPECT_TRUE(Eventually(
      [this, expected = (size_t)n] { return deleted.size() == expected; },
      50,
      100ms));
}

// =============================================================================
// Backoff behavior tests (manual clock)
//
// These instantiate level_zero_gc_t<ss::manual_clock> so that time
// progression is fully controlled by the test. Each test advances the
// manual clock and verifies the worker wakes at the right time.
// =============================================================================

class LevelZeroGCBackoffTest : public seastar_test {
public:
    ss::future<> TearDownAsync() override {
        if (gc_) {
            co_await gc_->stop();
        }
    }

    void configure(
      std::chrono::milliseconds gp,
      std::chrono::milliseconds throttle_progress = 10ms,
      std::chrono::milliseconds throttle_no_progress = 10ms) {
        grace_period_.update(std::chrono::milliseconds{gp});
        auto s = std::make_unique<object_storage_test_impl>(
          &listed, &deleted, &cfg);
        storage_ = s.get();
        gc_ = std::make_unique<cloud_topics::level_zero_gc_t<ss::manual_clock>>(
          cloud_topics::level_zero_gc_config{
            .deletion_grace_period = grace_period_.bind(),
            .throttle_progress
            = config::mock_binding<std::chrono::milliseconds>(
              throttle_progress),
            .throttle_no_progress
            = config::mock_binding<std::chrono::milliseconds>(
              throttle_no_progress),
          },
          std::move(s),
          std::make_unique<epoch_source_test_impl>(&max_epoch),
          std::make_unique<node_info_test_impl>(),
          std::make_unique<safety_monitor_test_impl>(&safety_ok),
          [](ss::manual_clock::duration) { return 0ms; });
    }

    void add_listed(int64_t epoch, std::chrono::milliseconds age) {
        auto key = cloud_topics::object_path_factory::level_zero_path(
          cloud_topics::object_id{
            .epoch = cloud_topics::cluster_epoch(epoch),
            .name = uuid_t::create(),
            .prefix = 0,
          });
        cloud_storage_clients::client::list_bucket_item item{
          .key = key().string(),
          .last_modified = std::chrono::system_clock::now() - age,
        };
        listed.push_back(item);
    }

    ss::future<>
    tick(std::chrono::milliseconds delta = std::chrono::milliseconds{0}) {
        ss::manual_clock::advance(delta);
        co_await tests::drain_task_queue();
    }

    template<class Fn>
    ss::future<> tick_until(
      Fn&& fn, std::chrono::milliseconds step = 10ms, int retries = 20) {
        for (int i = 0; i < retries && !fn(); ++i) {
            co_await tick(step);
        }
    }

    /// Wait for a collection round to fully complete (all pages +
    /// inter-page throttle sleeps) and the worker to enter its
    /// backoff sleep. Returns the stabilized list_call_count_.
    ss::future<uint64_t> wait_for_round() {
        co_await tick_until([this] { return storage_->list_call_count_ > 0; });
        // Require the count to be unchanged for two consecutive ticks.
        // Use a step larger than throttle_progress (the inter-page
        // sleep) so each tick fully flushes any pending page work.
        int stable_ticks = 0;
        uint64_t prev = 0;
        co_await tick_until(
          [this, &prev, &stable_ticks] {
              auto cur = storage_->list_call_count_;
              if (cur == prev && cur > 0) {
                  ++stable_ticks;
              } else {
                  stable_ticks = 0;
              }
              prev = cur;
              return stable_ticks >= 2;
          },
          20ms);
        co_return storage_->list_call_count_;
    }

    chunked_vector<cloud_storage_clients::client::list_bucket_item> listed;
    std::unordered_set<ss::sstring> deleted;
    std::optional<int64_t> max_epoch;
    config::mock_property<std::chrono::milliseconds> grace_period_{
      std::chrono::milliseconds{12h}};
    gc_test_config cfg{};
    object_storage_test_impl* storage_{nullptr};
    bool safety_ok{true};
    std::unique_ptr<cloud_topics::level_zero_gc_t<ss::manual_clock>> gc_;
};

// age_backoff computes a duration from system_clock time points. That
// duration is passed to sleep_abortable<manual_clock>, so advancing
// the manual clock by that amount resolves the sleep.
TEST_F_CORO(LevelZeroGCBackoffTest, AgeIneligibleWakesAtGracePeriod) {
    configure(1000ms, 10ms, 10ms);
    for (int i = 0; i < 5; ++i) {
        add_listed(i, 200ms);
    }
    max_epoch = 100;
    gc_->start().get();

    auto baseline = co_await wait_for_round();

    // The computed backoff is ~800ms (1000ms grace - ~200ms age).
    // Advance 500ms — well within. Worker should still be sleeping.
    co_await tick(500ms);
    ASSERT_EQ_CORO(storage_->list_call_count_, baseline);

    // Advance past the backoff. Worker should wake and list again.
    co_await tick_until(
      [this, baseline] { return storage_->list_call_count_ > baseline; }, 50ms);
    ASSERT_GT_CORO(storage_->list_call_count_, baseline);
}

TEST_F_CORO(LevelZeroGCBackoffTest, EpochIneligiblePollsAtThrottle) {
    configure(1000ms, 10ms, 50ms);
    for (int i = 50; i < 55; ++i) {
        add_listed(i, 24h);
    }
    max_epoch = 10;
    gc_->start().get();

    auto count_after_first = co_await wait_for_round();

    // Advance past throttle_no_progress (50ms) — another round fires.
    co_await tick_until(
      [this, count_after_first] {
          return storage_->list_call_count_ > count_after_first;
      },
      10ms);
    ASSERT_GT_CORO(storage_->list_call_count_, count_after_first);
}

TEST_F_CORO(LevelZeroGCBackoffTest, EmptyStorageSleepsForGracePeriod) {
    configure(500ms, 10ms, 10ms);
    max_epoch = 100;
    gc_->start().get();

    auto baseline = co_await wait_for_round();

    // 300ms — well within the 500ms grace period. Still sleeping.
    co_await tick(300ms);
    ASSERT_EQ_CORO(storage_->list_call_count_, baseline);

    // Past the grace period — worker should wake.
    co_await tick_until(
      [this, baseline] { return storage_->list_call_count_ > baseline; }, 50ms);
    ASSERT_GT_CORO(storage_->list_call_count_, baseline);
}

TEST_F_CORO(LevelZeroGCBackoffTest, GracePeriodReductionWakesWorker) {
    configure(1000ms, 10ms, 10ms);
    for (int i = 0; i < 5; ++i) {
        add_listed(i, 500ms);
    }
    max_epoch = 100;
    gc_->start().get();

    co_await wait_for_round();

    // Reduce grace period below the object age — objects become
    // eligible. The config watcher aborts the backoff sleep.
    grace_period_.update(std::chrono::milliseconds{200ms});
    co_await tick_until([this] { return deleted.size() == 5; });
    ASSERT_EQ_CORO(deleted.size(), 5u);
}

// After pause/start, the skip_backoff flag should cause the first
// round after restart to run immediately, not after the old backoff.
TEST_F_CORO(LevelZeroGCBackoffTest, StartAfterPauseSkipsBackoff) {
    configure(1000ms, 10ms, 10ms);
    // Objects 200ms old with 1000ms grace period → too young, worker
    // enters a ~800ms backoff.
    for (int i = 0; i < 10; ++i) {
        add_listed(i, 200ms);
    }
    max_epoch = 100;
    gc_->start().get();

    auto count_before = co_await wait_for_round();

    // Verify worker is sleeping — no new LISTs after 200ms
    // (well within ~800ms backoff).
    co_await tick(200ms);
    ASSERT_EQ_CORO(storage_->list_call_count_, count_before);

    gc_->pause().get();
    gc_->start().get();

    // A fresh round should happen promptly (skip_backoff_ set).
    co_await tick_until(
      [this, count_before] {
          return storage_->list_call_count_ > count_before;
      },
      10ms);
    ASSERT_GT_CORO(storage_->list_call_count_, count_before);
}

// =============================================================================
// Prefix Range Computation Tests (Static, no GC required)
// =============================================================================

class PrefixRangeComputationTest : public testing::Test {};

namespace {
void check_range_contents(
  std::optional<cloud_topics::prefix_range_inclusive> range) {
    ASSERT_TRUE(range.has_value());
    for (auto i = 0u; i < cloud_topics::prefix_range_inclusive::t_max; ++i) {
        if (i >= range->min && i <= range->max) {
            EXPECT_TRUE(range->contains(i));
        } else {
            EXPECT_FALSE(range->contains(i));
        }
    }
}
} // namespace

/*
 * With a single shard, it should handle all prefixes [0, prefix_max].
 */
TEST_F(PrefixRangeComputationTest, SingleShardCoversAllPrefixes) {
    auto range = cloud_topics::compute_prefix_range(
      0 /* shard_idx */, 1 /* total_shards */);
    check_range_contents(range);
    auto [min, max] = range.value();
    EXPECT_EQ(min, 0);
    EXPECT_EQ(max, cloud_topics::object_id::prefix_max);
}

/*
 * With two shards, verify non-overlapping ranges that cover the full space.
 */
TEST_F(PrefixRangeComputationTest, TwoShardsPartitionSpace) {
    {
        auto range = cloud_topics::compute_prefix_range(
          0 /* shard_idx */, 2 /* total_shards */);
        check_range_contents(range);
        // First shard: [0, 500)
        EXPECT_EQ(range->min, 0);
        EXPECT_EQ(range->max, 499);
    }

    {
        auto range = cloud_topics::compute_prefix_range(
          1 /* shard_idx */, 2 /* total_shards */);
        check_range_contents(range);
        // Second shard: [500, prefix_max]
        EXPECT_EQ(range->min, 500);
        EXPECT_EQ(range->max, cloud_topics::object_id::prefix_max);
    }
}

/*
 * With 1000 shards (one per prefix), each shard handles exactly one prefix.
 */
TEST_F(PrefixRangeComputationTest, ThousandShardsOnePerPrefix) {
    constexpr size_t total = n_prefixes;

    for (size_t i = 0; i < total; ++i) {
        auto range = cloud_topics::compute_prefix_range(
          i /* shard_idx */, total /* total_shards */);
        check_range_contents(range);
        auto [min, max] = range.value();
        if (i < total - 1) {
            EXPECT_EQ(min, i);
            EXPECT_EQ(max, i);
        } else {
            EXPECT_EQ(min, i);
            EXPECT_EQ(max, cloud_topics::object_id::prefix_max);
        }
    }
}

TEST_F(PrefixRangeComputationTest, MoreShardsThanPrefixes) {
    constexpr size_t total = 2000;

    {
        auto range = cloud_topics::compute_prefix_range(
          0 /* shard_idx */, total /* total_shards */);
        check_range_contents(range);
        EXPECT_EQ(range->min, 0);
        EXPECT_EQ(range->max, 0);
    }

    {
        auto range = cloud_topics::compute_prefix_range(
          cloud_topics::object_id::prefix_max /* shard_idx */,
          total /* total_shards */);
        check_range_contents(range);
        EXPECT_EQ(range->min, cloud_topics::object_id::prefix_max);
        EXPECT_EQ(range->max, cloud_topics::object_id::prefix_max);
    }

    EXPECT_FALSE(
      cloud_topics::compute_prefix_range(
        total - 1 /* shard_idx */, total /* total_shards */)
        .has_value());
}

/*
 * Verify complete coverage with 41 shards (simulating a heterogeneous cluster).
 */
TEST_F(PrefixRangeComputationTest, HeterogeneousCompleteCoverage) {
    constexpr size_t total = 41;

    std::vector<int> coverage_count(n_prefixes, 0);

    for (size_t shard = 0; shard < total; ++shard) {
        auto r = cloud_topics::compute_prefix_range(
          shard /* shard_idx */, total /* total_shards */);
        check_range_contents(r);
        auto [min, max] = r.value();
        EXPECT_GE(min, 0);
        EXPECT_LE(max, cloud_topics::object_id::prefix_max);
        for (auto prefix = min; prefix <= max && prefix < n_prefixes;
             ++prefix) {
            coverage_count[prefix]++;
        }
    }

    // Verify all prefixes are covered exactly once
    for (size_t prefix = 0; prefix < n_prefixes; ++prefix) {
        EXPECT_EQ(coverage_count[prefix], 1) << fmt::format(
          "Prefix {} covered {} times", prefix, coverage_count[prefix]);
    }
}

/*
 * Verify that prefix ranges are balanced: no shard gets more than one extra
 * prefix compared to any other. Also checks complete, non-overlapping coverage
 * for several shard counts including 32 (the case that exposed the original
 * imbalance where the last shard received all leftover prefixes).
 */
TEST_F(PrefixRangeComputationTest, BalancedDistribution) {
    for (size_t total : std::vector<size_t>{
           2, 3, 7, 10, 24, 32, 41, 64, 128, prefix_max, n_prefixes}) {
        SCOPED_TRACE(fmt::format("total_shards={}", total));

        std::vector<int> coverage_count(n_prefixes, 0);
        size_t min_width = std::numeric_limits<size_t>::max();
        size_t max_width = 0;

        for (size_t shard = 0; shard < total; ++shard) {
            auto r = cloud_topics::compute_prefix_range(shard, total);
            ASSERT_TRUE(r.has_value());
            ASSERT_LE(r->min, r->max);
            ASSERT_LE(r->max, prefix_max);
            size_t width = r->max - r->min + 1;
            min_width = std::min(min_width, width);
            max_width = std::max(max_width, width);

            for (auto pfx = r->min; pfx <= r->max; ++pfx) {
                coverage_count[pfx]++;
            }
        }

        EXPECT_LE(max_width - min_width, 1) << fmt::format(
          "Imbalance too large: min_width={}, max_width={}",
          min_width,
          max_width);

        for (size_t pfx = 0; pfx < n_prefixes; ++pfx) {
            EXPECT_EQ(coverage_count[pfx], 1) << fmt::format(
              "Prefix {} covered {} times (total_shards={})",
              pfx,
              coverage_count[pfx],
              total);
        }
    }
}

/*
 * Base test fixture for prefix-based partitioning tests.
 * Creates a single GC instance per test - do NOT create multiple GC instances.
 */
class LevelZeroGCPartitioningTest
  : public testing::TestWithParam<std::tuple<size_t, size_t>> {
public:
    LevelZeroGCPartitioningTest()
      : gc_(
          cloud_topics::level_zero_gc_config{
            .deletion_grace_period
            = config::mock_binding<std::chrono::milliseconds>(12h),
            .throttle_progress
            = config::mock_binding<std::chrono::milliseconds>(10ms),
            .throttle_no_progress
            = config::mock_binding<std::chrono::milliseconds>(10ms),
          },
          std::make_unique<object_storage_test_impl>(
            &listed_, &deleted_, &cfg_),
          std::make_unique<epoch_source_test_impl>(&max_epoch_),
          std::make_unique<node_info_test_impl>(
            std::get<0>(GetParam()), std::get<1>(GetParam())),
          std::make_unique<safety_monitor_test_impl>(),
          [](ss::lowres_clock::duration) { return 0ms; }) {}

    void TearDown() override { gc_.stop().get(); }

    /*
     * Insert an object with a specific prefix and epoch.
     */
    void add_listed_with_prefix(
      cloud_topics::object_id::prefix_t prefix,
      int64_t epoch,
      std::chrono::seconds age = 24h) {
        auto key = cloud_topics::object_path_factory::level_zero_path(
          cloud_topics::object_id{
            .epoch = cloud_topics::cluster_epoch(epoch),
            .name = uuid_t::create(),
            .prefix = prefix,
          });
        cloud_storage_clients::client::list_bucket_item item{
          .key = key().string(),
          .last_modified = std::chrono::system_clock::now() - age,
        };
        listed_.push_back(item);
    }

    /*
     * Sort the listed objects to simulate lexicographic ordering from cloud
     * storage. Must be called after adding all objects and before starting GC.
     */
    void sort_listed() {
        std::ranges::sort(
          listed_, {}, &cloud_storage_clients::client::list_bucket_item::key);
    }

    /*
     * Count how many objects have prefixes within a given range.
     */
    size_t count_objects_in_range(cloud_topics::prefix_range_inclusive range) {
        size_t count = 0;
        for (const auto& obj : listed_) {
            auto prefix
              = cloud_topics::object_path_factory::level_zero_path_to_prefix(
                obj.key);
            if (
              prefix.has_value() && prefix.value() >= range.min
              && prefix.value() <= range.max) {
                ++count;
            }
        }
        return count;
    }

    /*
     * Count deleted objects with prefixes within a given range.
     */
    size_t count_deleted_in_range(cloud_topics::prefix_range_inclusive range) {
        cloud_topics::prefix_compressor ps;
        ps.set_range(
          cloud_topics::prefix_range_inclusive{range.min, range.max});
        auto compressed_prefixes = ps.compressed_key_prefixes();

        size_t count = 0;
        for (const auto& key : deleted_) {
            if (
              std::ranges::any_of(compressed_prefixes, [&key](const auto& pfx) {
                  return key.starts_with(pfx().string());
              })) {
                ++count;
            }
        }
        return count;
    }

    size_t shard_idx() const { return std::get<0>(GetParam()); }
    size_t total_shards() const { return std::get<1>(GetParam()); }

    chunked_vector<cloud_storage_clients::client::list_bucket_item> listed_;
    std::unordered_set<ss::sstring> deleted_;
    std::optional<int64_t> max_epoch_;
    cloud_topics::level_zero_gc gc_;
    gc_test_config cfg_{};
};

// =============================================================================
// Parameterized Tests for Single Shard Configurations
// =============================================================================

/*
 * Test that a shard only deletes objects within its assigned prefix range.
 */
TEST_P(LevelZeroGCPartitioningTest, ShardOnlyDeletesObjectsInRange) {
    auto range = cloud_topics::compute_prefix_range(
      shard_idx(), total_shards());
    ASSERT_TRUE(range.has_value());
    auto [min, max] = range.value();

    // Add objects across the full prefix range (every 50th prefix)
    for (size_t prefix = 0; prefix <= prefix_max; prefix += 50) {
        add_listed_with_prefix(prefix, 1);
    }
    sort_listed();

    auto expected_in_range = count_objects_in_range(
      cloud_topics::prefix_range_inclusive{min, max});
    max_epoch_ = 100;

    gc_.start().get();

    // Only objects in this shard's range should be deleted
    EXPECT_TRUE(Eventually([this, expected_in_range] {
        return deleted_.size() == expected_in_range;
    }));

    // Verify all deleted objects are within range
    EXPECT_EQ(
      count_deleted_in_range(cloud_topics::prefix_range_inclusive{min, max}),
      deleted_.size());
}

/*
 * Test pagination across prefix boundaries.
 */
TEST_P(LevelZeroGCPartitioningTest, PaginationWithinRange) {
    auto range = cloud_topics::compute_prefix_range(
      shard_idx(), total_shards());
    ASSERT_TRUE(range.has_value());
    auto [min, max] = range.value();

    // Use small pages to force pagination
    cfg_.list_page_size = 5;

    // Add several objects within this shard's range
    for (auto prefix = min; prefix <= max && prefix < n_prefixes; prefix += 5) {
        for (int i = 0; i < 10; ++i) {
            add_listed_with_prefix(prefix, 1);
        }
    }
    sort_listed();

    auto expected = count_objects_in_range(
      cloud_topics::prefix_range_inclusive{min, max});
    max_epoch_ = 100;

    gc_.start().get();

    EXPECT_TRUE(
      Eventually([this, expected] { return deleted_.size() == expected; }));
    EXPECT_FALSE(
      Eventually([this, expected] { return deleted_.size() > expected; }, 10));
}

/*
 * Test that epoch filtering still works with prefix partitioning.
 */
TEST_P(LevelZeroGCPartitioningTest, EpochFilteringWithPartitioning) {
    auto range = cloud_topics::compute_prefix_range(
      shard_idx(), total_shards());
    ASSERT_TRUE(range.has_value());
    auto [min, max] = range.value();

    // Add objects with various epochs, using prefixes in our range
    if (min < n_prefixes) {
        add_listed_with_prefix(min, 50);  // epoch 50, eligible
        add_listed_with_prefix(min, 100); // epoch 100, boundary
        add_listed_with_prefix(min, 150); // epoch 150, not eligible
        add_listed_with_prefix(min, 200); // epoch 200, not eligible
    }
    sort_listed();

    max_epoch_ = 100; // Only epochs <= 100 are eligible

    gc_.start().get();

    // Only 2 objects (epochs 50 and 100) should be deleted
    EXPECT_TRUE(Eventually([this] { return deleted_.size() == 2; }));
    EXPECT_FALSE(Eventually([this] { return deleted_.size() > 2; }, 10));
}

/*
 * Test that age filtering still works with prefix partitioning.
 */
TEST_P(LevelZeroGCPartitioningTest, AgeFilteringWithPartitioning) {
    auto range = cloud_topics::compute_prefix_range(
      shard_idx(), total_shards());
    ASSERT_TRUE(range.has_value());
    auto [min, max] = range.value();

    if (min < n_prefixes) {
        add_listed_with_prefix(min, 1, 24h); // old enough
        add_listed_with_prefix(
          min + 1 > max ? min : min + 1, 1, 24h); // old enough
        add_listed_with_prefix(
          min + 2 > max ? min : min + 2, 1, 1h); // too young
        add_listed_with_prefix(
          min + 3 > max ? min : min + 3, 1, 1h); // too young
    }
    sort_listed();

    max_epoch_ = 100;

    gc_.start().get();

    // Only 2 old objects should be deleted
    EXPECT_TRUE(Eventually([this] { return deleted_.size() == 2; }));
}

/*
 * Test behavior when this shard has no objects in its range.
 */
TEST_P(LevelZeroGCPartitioningTest, NoObjectsInRange) {
    auto range = cloud_topics::compute_prefix_range(
      shard_idx(), total_shards());
    ASSERT_TRUE(range.has_value());
    auto [min, max] = range.value();

    // Add objects outside this shard's range
    if (min > 0) {
        // Add objects before our range
        add_listed_with_prefix(0, 1);
    }
    if (max < prefix_max) {
        // Add objects after our range
        add_listed_with_prefix(prefix_max, 1);
    }
    sort_listed();

    max_epoch_ = 100;

    gc_.start().get();

    // No objects should be deleted since none are in our range
    EXPECT_FALSE(Eventually([this] { return !deleted_.empty(); }, 10));
}

/*
 * Test objects at exact boundary prefix values.
 */
TEST_P(LevelZeroGCPartitioningTest, ObjectsAtBoundaries) {
    auto range = cloud_topics::compute_prefix_range(
      shard_idx(), total_shards());
    ASSERT_TRUE(range.has_value());
    auto [min, max] = range.value();

    // Add object at min boundary
    add_listed_with_prefix(min, 1);
    // Add object at max boundary
    if (max < n_prefixes) {
        add_listed_with_prefix(max, 1);
    }
    sort_listed();

    auto expected = count_objects_in_range(
      cloud_topics::prefix_range_inclusive{min, max});
    max_epoch_ = 100;

    gc_.start().get();

    EXPECT_TRUE(
      Eventually([this, expected] { return deleted_.size() == expected; }));
}

// Instantiate tests for various shard configurations
INSTANTIATE_TEST_SUITE_P(
  VariousShardCounts,
  LevelZeroGCPartitioningTest,
  testing::Values(
    std::make_tuple(0, 1),  // Single shard covering all prefixes
    std::make_tuple(0, 2),  // First half [0, 500)
    std::make_tuple(1, 2),  // Second half [500, prefix_max]
    std::make_tuple(0, 10), // First range [0, 100)
    std::make_tuple(4, 10), // Middle range [400, 500)
    std::make_tuple(23, 24) // Last shard (handles remainder)
    ),
  [](const testing::TestParamInfo<std::tuple<size_t, size_t>>& info) {
      return "Shard_" + std::to_string(std::get<0>(info.param)) + "_Of_"
             + std::to_string(std::get<1>(info.param));
  });
