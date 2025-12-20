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
#include "ssx/mutex.h"

#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

using namespace std::chrono_literals;

namespace {
struct gc_test_config {
    size_t list_page_size{std::numeric_limits<int>::max()};
    std::chrono::milliseconds list_cost{0ms};
    std::chrono::milliseconds delete_cost{0ms};
};
} // namespace

class object_storage_test_impl
  : public cloud_topics::level_zero_gc::object_storage {
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
      std::optional<ss::sstring> continuation_token) override {
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
        co_await seastar::sleep(cfg_->delete_cost);
        auto u = co_await delete_mtx_.get_units(*as);
        deleted_->insert_range(
          std::move(objects)
          | std::views::transform([](auto& s) { return std::move(s.key); }));
        co_return std::expected<void, cloud_io::upload_result>();
    }

    chunked_vector<cloud_storage_clients::client::list_bucket_item>* listed_;
    std::unordered_set<ss::sstring>* deleted_;
    gc_test_config* cfg_;

    ssx::mutex list_mtx_{"object-store-impl-list"};
    ssx::mutex delete_mtx_{"object-store-impl-delete"};
};

class epoch_source_test_impl
  : public cloud_topics::level_zero_gc::epoch_source {
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

class node_info_test_impl : public cloud_topics::level_zero_gc::node_info {
    size_t shard_index() const final { return 0; }
    size_t total_shards() const final { return 1; }
};

class LevelZeroGCTest : public testing::Test {
public:
    LevelZeroGCTest(
      std::chrono::milliseconds throttle_progress = 10ms,
      std::chrono::milliseconds throttle_no_progress = 10ms)
      : gc(
          cloud_topics::level_zero_gc_config{
            .deletion_grace_period
            = config::mock_binding<std::chrono::milliseconds>(12h),
            .throttle_progress
            = config::mock_binding<std::chrono::milliseconds>(
              throttle_progress),
            .throttle_no_progress
            = config::mock_binding<std::chrono::milliseconds>(
              throttle_no_progress),
          },
          std::make_unique<object_storage_test_impl>(&listed, &deleted, &cfg),
          std::make_unique<epoch_source_test_impl>(&max_epoch),
          std::make_unique<node_info_test_impl>()) {}

    void TearDown() override { gc.stop().get(); }

    /*
     * Insert an entry into the `listed` container which is the source of
     * objects reported by `list_objects`.
     */
    void add_listed(int64_t epoch, std::chrono::seconds age) {
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
    cloud_topics::level_zero_gc gc;
    gc_test_config cfg{};
};

template<typename Func>
::testing::AssertionResult Eventually(Func func, int retries = 50) {
    while (retries-- > 0) {
        if (func()) {
            return ::testing::AssertionSuccess();
        }
        seastar::sleep_abortable(std::chrono::milliseconds(100)).get();
    }
    return ::testing::AssertionFailure() << "Timeout";
}

// all 100 objects are deleted
TEST_F(LevelZeroGCTest, ListedIsDeleted) {
    for (int i = 0; i < 100; ++i) {
        add_listed(i, 24h);
    }
    this->max_epoch = 100;
    gc.start();
    EXPECT_TRUE(Eventually([this] { return deleted.size() == 100; }));
}

// all the objects below the max epoch are deleted
TEST_F(LevelZeroGCTest, ListedIsDeletedBelowEpoch) {
    for (int i = 0; i < 100; ++i) {
        add_listed(i, 24h);
    }
    this->max_epoch = 49;
    gc.start();
    EXPECT_TRUE(Eventually([this] { return deleted.size() == 50; }));
}

// none are deleted when max_epoch is not available
TEST_F(LevelZeroGCTest, NoDeletesWithoutMaxEpoch) {
    for (int i = 0; i < 100; ++i) {
        add_listed(i, 24h);
    }
    gc.start();
    EXPECT_FALSE(Eventually([this] { return deleted.size() > 0; }));
}

// recently created objects aren't deleted
TEST_F(LevelZeroGCTest, NoDeletesForYoungObjects) {
    for (int i = 0; i < 100; ++i) {
        add_listed(i, std::chrono::hours(i));
    }
    this->max_epoch = 100;
    gc.start();
    EXPECT_TRUE(Eventually([this] { return deleted.size() == 88; }));
}

/*
 * Returns copies of the configured output for the two pure virtual methods.
 * This fixture is intended to test the default implementation of the max gc
 * eligible epoch calculation, so that is not overriden.
 */
class epoch_source_test_default_impl
  : public cloud_topics::level_zero_gc::epoch_source {
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
    using epoch_source_type = cloud_topics::level_zero_gc::epoch_source;
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
    gc.start();
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
    gc.start();
    // wait until we process one page
    EXPECT_TRUE(Eventually([this] { return !deleted.empty(); }));
    // then immediately shutdown gc
    gc.stop().get();
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
    gc.start();
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
    gc.start();
    EXPECT_TRUE(Eventually(
      [this, expected = (size_t)n] { return deleted.size() == expected; }));
}
