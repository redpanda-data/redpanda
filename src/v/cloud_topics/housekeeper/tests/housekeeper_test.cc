/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/units.h"
#include "cloud_topics/housekeeper/housekeeper.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"

#include <gtest/gtest.h>

#include <chrono>
#include <expected>
#include <memory>
#include <source_location>
#include <stdexcept>
#include <type_traits>

namespace {

class fake_l0_metastore
  : public cloud_topics::housekeeper::l0_metadata_storage {
public:
    fake_l0_metastore(
      model::topic_id_partition tidp,
      kafka::offset start_offset,
      kafka::offset max_allowed_start_offset = kafka::offset::max())
      : _tidp(tidp)
      , _start_offset(start_offset)
      , _max_allowed_start_offset(max_allowed_start_offset) {}

    kafka::offset
    get_start_offset(const model::topic_id_partition& tidp) override {
        if (tidp == _tidp) {
            return _start_offset;
        }
        return kafka::offset{0};
    }

    kafka::offset
    get_last_reconciled_offset(const model::topic_id_partition&) override {
        return kafka::offset::max();
    }

    ss::future<> set_start_offset(
      const model::topic_id_partition& tidp,
      kafka::offset offset,
      ss::abort_source*) override {
        if (tidp == _tidp) {
            _start_offset = std::max(_start_offset, offset);
        }
        co_return;
    }

    kafka::offset start_offset() const { return _start_offset; }

    kafka::offset
    get_max_allowed_start_offset(const model::topic_id_partition&) override {
        return _max_allowed_start_offset;
    }

    std::optional<cloud_topics::cluster_epoch> estimate_inactive_epoch(
      const model::topic_id_partition&) noexcept override {
        return _estimated_inactive_epoch;
    }

    ss::future<std::optional<cloud_topics::cluster_epoch>>
    get_current_cluster_epoch(
      const model::topic_id_partition&, ss::abort_source*) noexcept override {
        co_return _current_cluster_epoch;
    }

    ss::future<> advance_epoch(
      const model::topic_id_partition& tidp,
      cloud_topics::cluster_epoch epoch,
      ss::abort_source*) noexcept override {
        if (tidp == _tidp) {
            _advance_epoch_calls.push_back(epoch);
        }
        co_return;
    }

    ss::future<> sync_to_next_placeholder(
      const model::topic_id_partition& tidp,
      ss::abort_source*) noexcept override {
        if (tidp == _tidp) {
            ++_sync_to_next_placeholder_calls;
        }
        co_return;
    }

    void set_max_allowed_start_offset(kafka::offset offset) {
        _max_allowed_start_offset = offset;
    }

    // Setters for epoch-related test configuration
    void set_estimated_inactive_epoch(
      std::optional<cloud_topics::cluster_epoch> epoch) {
        _estimated_inactive_epoch = epoch;
    }

    void set_current_cluster_epoch(
      std::optional<cloud_topics::cluster_epoch> epoch) {
        _current_cluster_epoch = epoch;
    }

    // Accessors for verifying calls
    const std::vector<cloud_topics::cluster_epoch>&
    advance_epoch_calls() const {
        return _advance_epoch_calls;
    }

    size_t sync_to_next_placeholder_calls() const {
        return _sync_to_next_placeholder_calls;
    }

    void reset_call_tracking() {
        _advance_epoch_calls.clear();
        _sync_to_next_placeholder_calls = 0;
    }

private:
    model::topic_id_partition _tidp;
    kafka::offset _start_offset;
    kafka::offset _max_allowed_start_offset;

    // Epoch-related state
    std::optional<cloud_topics::cluster_epoch> _estimated_inactive_epoch
      = cloud_topics::cluster_epoch::min();
    std::optional<cloud_topics::cluster_epoch> _current_cluster_epoch
      = cloud_topics::cluster_epoch{1};

    // Call tracking
    std::vector<cloud_topics::cluster_epoch> _advance_epoch_calls;
    size_t _sync_to_next_placeholder_calls{0};
};

struct simple_retention_config {
    std::optional<size_t> bytes;
    std::optional<std::chrono::milliseconds> duration;
};

class retention_config_impl
  : public cloud_topics::housekeeper::retention_configuration {
public:
    explicit retention_config_impl(const simple_retention_config& cfg)
      : _cfg(cfg) {}

    std::optional<size_t>
    retention_bytes(const model::topic_id_partition&) override {
        return _cfg.bytes;
    }

    std::optional<std::chrono::milliseconds>
    retention_duration(const model::topic_id_partition&) override {
        return _cfg.duration;
    }

private:
    simple_retention_config _cfg;
};

struct object_params {
    int64_t records = 0;
    size_t size = 0;
    model::timestamp_clock::time_point max_timestamp;
};

template<typename T>
T handle_error(
  std::expected<T, cloud_topics::l1::metastore::errc> result,
  std::source_location loc = std::source_location::current()) {
    if (result.has_value()) {
        return std::move(result.value());
    }
    if constexpr (
      std::is_same_v<T, cloud_topics::l1::metastore::offsets_response>) {
        if (result.error() == cloud_topics::l1::metastore::errc::missing_ntp) {
            return {
              .start_offset = kafka::offset::min(),
              .next_offset = kafka::offset(0),
            };
        }
    }
    throw std::runtime_error(
      fmt::format(
        "[{}:{}] metastore error: {}",
        loc.file_name(),
        loc.line(),
        std::to_underlying(result.error())));
}

class counting_metastore : public cloud_topics::l1::simple_metastore {
public:
    ss::future<std::expected<void, errc>> set_start_offset(
      const model::topic_id_partition& tidp, kafka::offset o) override {
        ++set_start_offset_calls;
        return simple_metastore::set_start_offset(tidp, o);
    }

    size_t set_start_offset_calls{0};
};

} // namespace

using namespace std::chrono_literals;

class HousekeeperTest : public testing::Test {
public:
    cloud_topics::housekeeper make_housekeeper(simple_retention_config cfg) {
        _config_impl = std::make_unique<retention_config_impl>(cfg);
        return {
          _tidp,
          &_l0_metastore,
          &_l1_metastore,
          _config_impl.get(),
          config::mock_binding<std::chrono::milliseconds>(1ms)};
    }

    kafka::offset start_offset() const { return _l0_metastore.start_offset(); }

    void set_start_offset(kafka::offset offset) {
        ss::abort_source as;
        _l0_metastore.set_start_offset(_tidp, offset, &as).get();
    }

    void set_max_allowed_start_offset(kafka::offset offset) {
        _l0_metastore.set_max_allowed_start_offset(offset);
    }

    size_t l1_set_start_offset_calls() const {
        return _l1_metastore.set_start_offset_calls;
    }

    kafka::offset l1_start_offset() {
        auto result = _l1_metastore.get_offsets(_tidp).get();
        if (!result.has_value()) {
            return kafka::offset{0};
        }
        return result.value().start_offset;
    }

    void add_object(object_params params) {
        auto result = handle_error(_l1_metastore.get_offsets(_tidp).get());
        chunked_vector<cloud_topics::l1::metastore::object_metadata> meta;
        auto metas = cloud_topics::l1::metastore::object_metadata::
          ntp_metas_list_t::single(
            /*tidp=*/_tidp,
            /*base_offset=*/result.next_offset,
            /*last_offset=*/result.next_offset
              + kafka::offset{params.records - 1},
            /*max_timestamp=*/model::to_timestamp(params.max_timestamp),
            /*pos=*/0,
            /*size=*/params.size);
        cloud_topics::l1::metastore::object_metadata obj{
          .oid = cloud_topics::l1::create_object_id(),
          .footer_pos = params.size,
          .object_size = params.size + 1,
          .ntp_metas = std::move(metas),
        };
        cloud_topics::l1::metastore::term_offset_map_t term_map;
        term_map.emplace(
          _tidp,
          decltype(term_map)::value_type::second_type::single(
            model::term_id{0}, result.next_offset));
        auto oid = obj.oid;
        chunked_vector<decltype(obj)> objects;
        objects.push_back(std::move(obj));
        _l1_metastore.preregister_objects(
          chunked_vector<cloud_topics::l1::object_id>::single(oid));
        handle_error(_l1_metastore.add_objects(objects, term_map).get());
    }

    // Epoch-related test helpers
    void set_estimated_inactive_epoch(
      std::optional<cloud_topics::cluster_epoch> epoch) {
        _l0_metastore.set_estimated_inactive_epoch(epoch);
    }

    void set_current_cluster_epoch(
      std::optional<cloud_topics::cluster_epoch> epoch) {
        _l0_metastore.set_current_cluster_epoch(epoch);
    }

    const std::vector<cloud_topics::cluster_epoch>&
    advance_epoch_calls() const {
        return _l0_metastore.advance_epoch_calls();
    }

    size_t sync_to_next_placeholder_calls() const {
        return _l0_metastore.sync_to_next_placeholder_calls();
    }

    void reset_epoch_call_tracking() { _l0_metastore.reset_call_tracking(); }

private:
    model::topic_id_partition _tidp{
      model::create_topic_id(), model::partition_id{0}};
    fake_l0_metastore _l0_metastore{_tidp, kafka::offset{0}};
    counting_metastore _l1_metastore;
    std::unique_ptr<retention_config_impl> _config_impl;
};

TEST_F(HousekeeperTest, InfiniteRetention) {
    auto housekeeper = make_housekeeper({});
    EXPECT_EQ(start_offset(), kafka::offset{0});
    add_object({
      .records = 100,
      .size = 1_MiB,
      .max_timestamp = model::timestamp_clock::now() - 1h,
    });
    housekeeper.do_housekeeping().get();
    EXPECT_EQ(start_offset(), kafka::offset{0});
}

TEST_F(HousekeeperTest, TimeBasedRetention) {
    simple_retention_config cfg;
    cfg.duration = 30min;
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Add old object (should be deleted)
    add_object({
      .records = 50,
      .size = 500_KiB,
      .max_timestamp = model::timestamp_clock::now() - 2h,
    });

    // Add recent object (should be kept)
    add_object({
      .records = 75,
      .size = 750_KiB,
      .max_timestamp = model::timestamp_clock::now() - 10min,
    });

    housekeeper.do_housekeeping().get();
    EXPECT_EQ(start_offset(), kafka::offset{50});
}

TEST_F(HousekeeperTest, TimeBasedRetentionNoOldObjects) {
    simple_retention_config cfg;
    cfg.duration = 1h;
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Add recent objects (all should be kept)
    add_object({
      .records = 100,
      .size = 1_MiB,
      .max_timestamp = model::timestamp_clock::now() - 30min,
    });
    add_object({
      .records = 200,
      .size = 2_MiB,
      .max_timestamp = model::timestamp_clock::now() - 10min,
    });

    housekeeper.do_housekeeping().get();
    // No objects should be deleted
    EXPECT_EQ(start_offset(), kafka::offset{0});
}

TEST_F(HousekeeperTest, TimeBasedRetentionDeleteAll) {
    simple_retention_config cfg;
    cfg.duration = 10min; // Very short retention
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Add objects that are all too old
    add_object({
      .records = 75,
      .size = 750_KiB,
      .max_timestamp = model::timestamp_clock::now() - 2h,
    });
    add_object({
      .records = 100,
      .size = 1_MiB,
      .max_timestamp = model::timestamp_clock::now() - 1h,
    });
    add_object({
      .records = 125,
      .size = 1_MiB + 250_KiB,
      .max_timestamp = model::timestamp_clock::now() - 30min,
    });

    housekeeper.do_housekeeping().get();
    EXPECT_EQ(start_offset(), kafka::offset{300});
}

TEST_F(HousekeeperTest, BytesBasedRetention) {
    simple_retention_config cfg;
    cfg.bytes = 2_MiB;
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Add objects totaling more than 2MiB to trigger retention
    add_object({
      .records = 100,
      .size = 3_MiB,
      .max_timestamp = model::timestamp_clock::now() - 1h,
    });
    add_object({
      .records = 150,
      .size = 2_MiB,
      .max_timestamp = model::timestamp_clock::now() - 30min,
    });

    housekeeper.do_housekeeping().get();
    // Should retain only the most recent ~2MiB, advancing past first object
    EXPECT_EQ(start_offset(), kafka::offset{100});
}

TEST_F(HousekeeperTest, BytesBasedRetentionUnderLimit) {
    simple_retention_config cfg;
    cfg.bytes = 5_MiB;
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Add objects totaling less than 5MiB
    add_object({
      .records = 100,
      .size = 1_MiB,
      .max_timestamp = model::timestamp_clock::now() - 1h,
    });
    add_object({
      .records = 200,
      .size = 2_MiB,
      .max_timestamp = model::timestamp_clock::now() - 30min,
    });

    housekeeper.do_housekeeping().get();
    // No objects should be deleted since total is under limit
    EXPECT_EQ(start_offset(), kafka::offset{0});
}

TEST_F(HousekeeperTest, MixedRetentionBytesMoreRestrictive) {
    // Time allows 2h retention, but bytes only allows 500KiB
    simple_retention_config cfg;
    cfg.bytes = 500_KiB;
    cfg.duration = 2h;
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Add objects - all recent enough for time retention but may exceed bytes
    add_object({
      .records = 100,
      .size = 1_MiB,
      .max_timestamp = model::timestamp_clock::now() - 1h,
    });
    add_object({
      .records = 150,
      .size = 600_KiB,
      .max_timestamp = model::timestamp_clock::now() - 30min,
    });

    housekeeper.do_housekeeping().get();
    EXPECT_EQ(start_offset(), kafka::offset{100});
}

TEST_F(HousekeeperTest, MixedRetentionTimeMoreRestrictive) {
    // Bytes allows 5MiB retention, but time only allows 30min
    simple_retention_config cfg;
    cfg.bytes = 5_MiB;
    cfg.duration = 30min;
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Add objects - under bytes limit but some too old for time retention
    add_object({
      .records = 100,
      .size = 1_MiB,
      .max_timestamp = model::timestamp_clock::now() - 2h,
    });
    add_object({
      .records = 150,
      .size = 1_MiB,
      .max_timestamp = model::timestamp_clock::now() - 10min,
    });

    housekeeper.do_housekeeping().get();
    // Time retention is more restrictive, should advance past old object
    // First object: 100 records ending at offset 100, second starts at 101
    EXPECT_EQ(start_offset(), kafka::offset{100});
}

TEST_F(HousekeeperTest, MixedRetentionBothRetainAll) {
    simple_retention_config cfg;
    cfg.bytes = 10_MiB;
    cfg.duration = 3h;
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Add objects that satisfy both retention policies
    add_object({
      .records = 100,
      .size = 1_MiB,
      .max_timestamp = model::timestamp_clock::now() - 1h,
    });
    add_object({
      .records = 200,
      .size = 2_MiB,
      .max_timestamp = model::timestamp_clock::now() - 30min,
    });

    housekeeper.do_housekeeping().get();
    // Both policies allow retention of all objects
    EXPECT_EQ(start_offset(), kafka::offset{0});
}

TEST_F(HousekeeperTest, MixedRetentionBothDeleteAll) {
    // Very restrictive time policy should force deletion.
    simple_retention_config cfg;
    cfg.bytes = 50_KiB;  // Very small limit
    cfg.duration = 5min; // Very short time to force time retention
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Add old objects that violate time policy
    add_object({
      .records = 100,
      .size = 2_MiB,
      .max_timestamp = model::timestamp_clock::now() - 2h,
    });
    add_object({
      .records = 150,
      .size = 2_MiB,
      .max_timestamp = model::timestamp_clock::now() - 1h,
    });

    housekeeper.do_housekeeping().get();
    EXPECT_EQ(start_offset(), kafka::offset{250});
}

TEST_F(HousekeeperTest, MultipleObjectsTimeRetention) {
    simple_retention_config cfg;
    cfg.duration = 1h;
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Add multiple objects across time boundary
    add_object({
      .records = 50,
      .size = 500_KiB,
      .max_timestamp = model::timestamp_clock::now() - 3h,
    });
    add_object({
      .records = 75,
      .size = 750_KiB,
      .max_timestamp = model::timestamp_clock::now() - 2h,
    });
    add_object({
      .records = 100,
      .size = 1_MiB,
      .max_timestamp = model::timestamp_clock::now() - 45min,
    });
    add_object({
      .records = 125,
      .size = 1_MiB + 250_KiB,
      .max_timestamp = model::timestamp_clock::now() - 15min,
    });

    housekeeper.do_housekeeping().get();
    EXPECT_EQ(start_offset(), kafka::offset{125});
}

TEST_F(HousekeeperTest, MultipleObjectsBytesRetention) {
    simple_retention_config cfg;
    cfg.bytes = 2_MiB;
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Add multiple objects with increasing sizes
    add_object({
      .records = 50,
      .size = 800_KiB,
      .max_timestamp = model::timestamp_clock::now() - 2h,
    });
    add_object({
      .records = 75,
      .size = 600_KiB,
      .max_timestamp = model::timestamp_clock::now() - 1h,
    });
    add_object({
      .records = 100,
      .size = 700_KiB,
      .max_timestamp = model::timestamp_clock::now() - 30min,
    });
    add_object({
      .records = 125,
      .size = 900_KiB,
      .max_timestamp = model::timestamp_clock::now() - 10min,
    });

    housekeeper.do_housekeeping().get();
    EXPECT_EQ(start_offset(), kafka::offset{50});
}

TEST_F(HousekeeperTest, SyncsToL1) {
    // This test simulates the scenario where Delete Records updates L0's
    // start offset, and the housekeeper syncs it to L1.
    simple_retention_config cfg;
    auto housekeeper = make_housekeeper(cfg);

    add_object({
      .records = 100,
      .size = 1_MiB,
      .max_timestamp = model::timestamp_clock::now() - 1h,
    });

    EXPECT_EQ(start_offset(), kafka::offset{0});
    EXPECT_EQ(l1_start_offset(), kafka::offset{0});

    set_start_offset(kafka::offset{50});

    EXPECT_EQ(start_offset(), kafka::offset{50});
    EXPECT_EQ(l1_start_offset(), kafka::offset{0});

    housekeeper.do_housekeeping().get();

    EXPECT_EQ(start_offset(), kafka::offset{50});
    EXPECT_EQ(l1_start_offset(), kafka::offset{50});
}

TEST_F(HousekeeperTest, TimeRetentionLimitedByMaxAllowedStartOffset) {
    // Time retention would advance to offset 50, but max_allowed_start_offset
    // limits it to 25.
    simple_retention_config cfg;
    cfg.duration = 30min;
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Set max allowed before housekeeping runs
    set_max_allowed_start_offset(kafka::offset{25});

    // Add old object (would normally be deleted entirely)
    add_object({
      .records = 50,
      .size = 500_KiB,
      .max_timestamp = model::timestamp_clock::now() - 2h,
    });

    // Add recent object (should be kept)
    add_object({
      .records = 75,
      .size = 750_KiB,
      .max_timestamp = model::timestamp_clock::now() - 10min,
    });

    housekeeper.do_housekeeping().get();
    // Should be clamped to max_allowed_start_offset (25), not 50
    EXPECT_EQ(start_offset(), kafka::offset{25});
}

TEST_F(HousekeeperTest, BytesRetentionLimitedByMaxAllowedStartOffset) {
    // Bytes retention would advance to offset 100, but max_allowed_start_offset
    // limits it to 50.
    simple_retention_config cfg;
    cfg.bytes = 2_MiB;
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Set max allowed before housekeeping runs
    set_max_allowed_start_offset(kafka::offset{50});

    // Add objects totaling more than 2MiB to trigger retention
    add_object({
      .records = 100,
      .size = 3_MiB,
      .max_timestamp = model::timestamp_clock::now() - 1h,
    });
    add_object({
      .records = 150,
      .size = 2_MiB,
      .max_timestamp = model::timestamp_clock::now() - 30min,
    });

    housekeeper.do_housekeeping().get();
    // Should be clamped to max_allowed_start_offset (50), not 100
    EXPECT_EQ(start_offset(), kafka::offset{50});
}

TEST_F(HousekeeperTest, MixedRetentionLimitedByMaxAllowedStartOffset) {
    // Both retention policies agree on deleting to offset 100, but
    // max_allowed_start_offset limits to 75.
    simple_retention_config cfg;
    cfg.bytes = 1_MiB;
    cfg.duration = 30min;
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Set max allowed before housekeeping runs
    set_max_allowed_start_offset(kafka::offset{75});

    // Add old large object (violates both policies)
    add_object({
      .records = 100,
      .size = 3_MiB,
      .max_timestamp = model::timestamp_clock::now() - 2h,
    });
    // Add recent small object
    add_object({
      .records = 150,
      .size = 500_KiB,
      .max_timestamp = model::timestamp_clock::now() - 10min,
    });

    housekeeper.do_housekeeping().get();
    // Should be clamped to max_allowed_start_offset (75), not 100
    EXPECT_EQ(start_offset(), kafka::offset{75});
}

TEST_F(HousekeeperTest, MaxAllowedStartOffsetDoesNotLimitWhenHigher) {
    // max_allowed_start_offset is higher than what retention computes,
    // so it should not affect the result.
    simple_retention_config cfg;
    cfg.duration = 30min;
    auto housekeeper = make_housekeeper(cfg);
    EXPECT_EQ(start_offset(), kafka::offset{0});

    // Set max allowed to a high value
    set_max_allowed_start_offset(kafka::offset{1000});

    // Add old object (should be deleted)
    add_object({
      .records = 50,
      .size = 500_KiB,
      .max_timestamp = model::timestamp_clock::now() - 2h,
    });

    // Add recent object (should be kept)
    add_object({
      .records = 75,
      .size = 750_KiB,
      .max_timestamp = model::timestamp_clock::now() - 10min,
    });

    housekeeper.do_housekeeping().get();
    // Should advance to 50 as normal, not limited by max_allowed
    EXPECT_EQ(start_offset(), kafka::offset{50});
}

// Tests for do_bump_epoch()

TEST_F(HousekeeperTest, BumpEpochProgressNaturally) {
    // When the estimated_inactive_epoch changes between calls, the housekeeper
    // should NOT force an epoch advance.
    auto housekeeper = make_housekeeper({});

    set_estimated_inactive_epoch(cloud_topics::cluster_epoch{1});
    set_current_cluster_epoch(cloud_topics::cluster_epoch{5});

    // First call - epoch changes from initial (nullopt/min) to 1
    housekeeper.do_bump_epoch().get();
    EXPECT_TRUE(advance_epoch_calls().empty());
    EXPECT_EQ(sync_to_next_placeholder_calls(), 0);

    // Change the estimated epoch before next call
    set_estimated_inactive_epoch(cloud_topics::cluster_epoch{2});

    // Second call - epoch changed again, so no forced advance
    housekeeper.do_bump_epoch().get();
    EXPECT_TRUE(advance_epoch_calls().empty());
    EXPECT_EQ(sync_to_next_placeholder_calls(), 0);
}

TEST_F(HousekeeperTest, BumpEpochIdleTriggersAdvance) {
    // When estimated_inactive_epoch stays the same across a housekeeping
    // interval, housekeeper should force an epoch advance.
    auto housekeeper = make_housekeeper({});

    set_estimated_inactive_epoch(cloud_topics::cluster_epoch{1});
    set_current_cluster_epoch(cloud_topics::cluster_epoch{5});

    // First call - initializes _last_epoch
    housekeeper.do_bump_epoch().get();
    EXPECT_TRUE(advance_epoch_calls().empty());

    // Second call - same epoch, should trigger advance
    housekeeper.do_bump_epoch().get();
    ASSERT_EQ(advance_epoch_calls().size(), 1);
    EXPECT_EQ(advance_epoch_calls()[0], cloud_topics::cluster_epoch{5});
    EXPECT_EQ(sync_to_next_placeholder_calls(), 1);
}

TEST_F(HousekeeperTest, BumpEpochGetCurrentEpochReturnsNullopt) {
    // When get_current_cluster_epoch returns nullopt, we should not call
    // advance_epoch even if the partition is idle.
    auto housekeeper = make_housekeeper({});

    set_estimated_inactive_epoch(cloud_topics::cluster_epoch{1});
    set_current_cluster_epoch(std::nullopt); // Simulate failure to get epoch

    // First call - initializes _last_epoch
    housekeeper.do_bump_epoch().get();
    EXPECT_TRUE(advance_epoch_calls().empty());

    // Second call - idle but get_current_cluster_epoch returns nullopt
    housekeeper.do_bump_epoch().get();
    EXPECT_TRUE(advance_epoch_calls().empty());
    EXPECT_EQ(sync_to_next_placeholder_calls(), 0);
}

TEST_F(HousekeeperTest, BumpEpochForcedAdvanceKeepsLastEpoch) {
    // After a forced epoch advance, _last_epoch is NOT updated. When the
    // estimated epoch catches up to reflect the advance, the next call sees
    // that as natural progress (no forced advance needed).
    auto housekeeper = make_housekeeper({});

    set_estimated_inactive_epoch(cloud_topics::cluster_epoch{1});
    set_current_cluster_epoch(cloud_topics::cluster_epoch{5});

    // First call - initializes _last_epoch to 1
    housekeeper.do_bump_epoch().get();

    // Second call - same epoch, triggers forced advance
    housekeeper.do_bump_epoch().get();
    ASSERT_EQ(advance_epoch_calls().size(), 1);
    EXPECT_EQ(sync_to_next_placeholder_calls(), 1);

    // Simulate the estimated epoch catching up (as would happen after advance)
    set_estimated_inactive_epoch(cloud_topics::cluster_epoch{5});
    reset_epoch_call_tracking();

    // Next call sees epoch progress (1 -> 5), so no forced advance
    housekeeper.do_bump_epoch().get();
    EXPECT_TRUE(advance_epoch_calls().empty());
    EXPECT_EQ(sync_to_next_placeholder_calls(), 0);
}

TEST_F(HousekeeperTest, BumpEpochMultipleIdleCycles) {
    // Test multiple idle cycles - after advancing, if the partition goes idle
    // again, it should trigger another advance.
    auto housekeeper = make_housekeeper({});

    set_estimated_inactive_epoch(cloud_topics::cluster_epoch{1});
    set_current_cluster_epoch(cloud_topics::cluster_epoch{5});

    // First cycle: init -> advance
    housekeeper.do_bump_epoch().get();
    housekeeper.do_bump_epoch().get();
    ASSERT_EQ(advance_epoch_calls().size(), 1);
    EXPECT_EQ(advance_epoch_calls()[0], cloud_topics::cluster_epoch{5});

    // Simulate epoch catching up after forced advance, with new cluster epoch
    set_estimated_inactive_epoch(cloud_topics::cluster_epoch{5});
    set_current_cluster_epoch(cloud_topics::cluster_epoch{10});
    reset_epoch_call_tracking();

    // Sees progress (1 -> 5) since _last_epoch was preserved at 1
    housekeeper.do_bump_epoch().get();
    EXPECT_TRUE(advance_epoch_calls().empty());

    // Second cycle: idle again (epoch 5 unchanged) -> advance to 10
    housekeeper.do_bump_epoch().get();
    ASSERT_EQ(advance_epoch_calls().size(), 1);
    EXPECT_EQ(advance_epoch_calls()[0], cloud_topics::cluster_epoch{10});
    EXPECT_EQ(sync_to_next_placeholder_calls(), 1);
}

TEST_F(HousekeeperTest, SyncStartOffsetSkipsRedundantRPC) {
    auto housekeeper = make_housekeeper({});
    add_object({.records = 100, .size = 1_MiB});
    set_start_offset(kafka::offset{50});

    housekeeper.do_housekeeping().get();
    EXPECT_EQ(l1_set_start_offset_calls(), 1);

    // No change — should skip the call.
    housekeeper.do_housekeeping().get();
    EXPECT_EQ(l1_set_start_offset_calls(), 1);

    // Changed offset — should sync again.
    set_start_offset(kafka::offset{75});
    housekeeper.do_housekeeping().get();
    EXPECT_EQ(l1_set_start_offset_calls(), 2);
}
