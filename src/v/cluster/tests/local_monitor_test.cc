/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/seastarx.h"
#include "cluster/logger.h"
#include "config/configuration.h"
#include "local_monitor_fixture.h"
#include "redpanda/tests/fixture.h"
#include "storage/types.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/interface.hpp>
#include <fmt/format.h>

#include <filesystem>
#include <string_view>
#include <system_error>
#include <vector>

using namespace cluster;

using storage::disk_space_alert;

local_monitor_fixture::local_monitor_fixture() {
    auto log_conf = storage::log_config{
      "test.dir",
      1024,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()};

    auto kvstore_conf = storage::kvstore_config(
      1_MiB,
      config::mock_binding(10ms),
      log_conf.base_dir,
      storage::make_sanitized_file_config());

    _feature_table.start().get();
    _feature_table
      .invoke_on_all(
        [](features::feature_table& f) { f.testing_activate_all(); })
      .get();

    clusterlog.info("{}: create", __func__);
    auto test_dir = "local_monitor_test."
                    + random_generators::gen_alphanum_string(4);

    _test_path = std::filesystem::absolute(test_dir.c_str());

    std::error_code errc;
    std::filesystem::create_directory(_test_path, errc);
    if (errc) {
        clusterlog.warn(
          "{}: failed to create test dir {}: {}", __func__, _test_path, errc);
    } else {
        clusterlog.info("{}: created test dir {}", __func__, _test_path);
    }

    _storage_node_api.start_single(_test_path.string(), _test_path.string())
      .get();

    _local_monitor
      .start(
        ss::sharded_parameter([] {
            return config::shard_local_cfg()
              .storage_space_alert_free_threshold_bytes.bind();
        }),
        ss::sharded_parameter([] {
            return config::shard_local_cfg()
              .storage_space_alert_free_threshold_percent.bind();
        }),
        std::ref(_storage_node_api))
      .get();

    BOOST_ASSERT(ss::engine_is_ready());
}

local_monitor_fixture::~local_monitor_fixture() {
    clusterlog.info("{}: destroy", __func__);
    std::error_code err;
    std::filesystem::remove_all(std::filesystem::path(_test_path), err);
    if (err) {
        clusterlog.warn("Cleanup got error {} removing test dir.", err);
    }
    _storage_node_api.stop().get();
    _local_monitor.stop().get();
    _feature_table.stop().get();
}

node::local_state local_monitor_fixture::update_state() {
    local_monitor()
      .update_state()
      .then([&]() { clusterlog.info("Updated local state."); })
      .get();
    return local_monitor().get_state_cached();
}

struct statvfs local_monitor_fixture::make_statvfs(
  size_t blk_free, size_t blk_total, size_t blk_size) {
    struct statvfs s = {
      .f_frsize = blk_size, .f_blocks = blk_total, .f_bfree = blk_free};
    return s;
}

void local_monitor_fixture::set_config_free_thresholds(
  unsigned alert_percent, size_t alert_bytes, size_t min_bytes) {
    auto f = ss::smp::invoke_on_all([&]() {
        config::shard_local_cfg()
          .get("storage_space_alert_free_threshold_bytes")
          .set_value(std::any_cast<size_t>(alert_bytes));
        config::shard_local_cfg()
          .get("storage_space_alert_free_threshold_percent")
          .set_value(std::any_cast<unsigned>(alert_percent));
        config::shard_local_cfg()
          .get("storage_min_free_bytes")
          .set_value(std::any_cast<size_t>(min_bytes));
    });
    f.get();
}

FIXTURE_TEST(local_state_has_single_disk, local_monitor_fixture) {
    auto ls = update_state();
    BOOST_TEST_REQUIRE(ls.disks().size() == 1);
}

FIXTURE_TEST(local_monitor_inject_statvfs, local_monitor_fixture) {
    _storage_node_api.local().set_statvfs_overrides(
      storage::node::disk_type::data,
      {.total_bytes = 2 << 20, .free_bytes = 1 << 20});

    auto ls = update_state();
    BOOST_TEST_REQUIRE(ls.data_disk.total == 2 << 20);
    BOOST_TEST_REQUIRE(ls.data_disk.free == 1 << 20);
}

void local_monitor_fixture::assert_space_alert(
  size_t volume,
  size_t bytes_alert,
  size_t percent_alert_bytes,
  size_t min_bytes,
  size_t free,
  disk_space_alert expected) {
    unsigned percent = (percent_alert_bytes * 100) / volume;
    set_config_free_thresholds(percent, bytes_alert, min_bytes);

    _storage_node_api.local().set_statvfs_overrides(
      storage::node::disk_type::data,
      {.total_bytes = volume, .free_bytes = free});

    auto ls = update_state();
    BOOST_TEST_REQUIRE(ls.data_disk.alert == expected);
}

FIXTURE_TEST(local_monitor_alert_none, local_monitor_fixture) {
    assert_space_alert(
      100_TiB, // total volume size
      200_GiB, // alert min free bytes
      200_GiB, // alert min percent (in bytes here)
      1_GiB,   // min bytes
      201_GiB, // current free
      disk_space_alert::ok);
    assert_space_alert(
      100_GiB, // total
      10_MiB,  // alert bytes
      20_MiB,  // alert percent
      5_GiB,   // min bytes
      100_GiB, // current free
      disk_space_alert::ok);
}

FIXTURE_TEST(local_monitor_alert_on_space_percent, local_monitor_fixture) {
    assert_space_alert(
      1_TiB,  // total
      1_GiB,  // alert bytes
      11_GiB, // alert percent
      1_GiB,  // min bytes
      4_GiB,  // current free
      disk_space_alert::low_space);
    assert_space_alert(
      2_TiB,   // total
      0,       // alert bytes
      100_GiB, // alert percent
      1_GiB,   // min bytes
      44_GiB,  // current free
      disk_space_alert::low_space);
}

FIXTURE_TEST(local_monitor_alert_on_space_bytes, local_monitor_fixture) {
    assert_space_alert(
      20_TiB,  // total
      1_TiB,   // alert bytes
      100_GiB, // alert percent
      20_GiB,  // min bytes
      99_GiB,  // current free
      disk_space_alert::low_space);
    assert_space_alert(
      20_TiB,  // total
      100_GiB, // alert bytes
      0,       // alert percent
      20_GiB,  // min bytes
      99_GiB,  // current free
      disk_space_alert::low_space);
}

FIXTURE_TEST(local_monitor_min_bytes, local_monitor_fixture) {
    assert_space_alert(
      20_TiB,  // total
      0,       // alert bytes
      100_GiB, // alert percent
      20_GiB,  // min bytes
      19_GiB,  // current free
      disk_space_alert::degraded);
    assert_space_alert(
      10_GiB,    // total
      0,         // alert bytes
      0,         // alert percent
      2_MiB,     // min bytes
      2_MiB - 1, // current free
      disk_space_alert::degraded);
}
