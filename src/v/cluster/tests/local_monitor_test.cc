/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/logger.h"
#include "local_monitor_fixture.h"
#include "redpanda/tests/fixture.h"
#include "seastarx.h"

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

local_monitor_fixture::local_monitor_fixture() {
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
    _local_monitor.set_path_for_test(_test_path.string());
    BOOST_ASSERT(ss::engine_is_ready());

    set_config_alert_thresholds(
      default_percent_threshold, default_bytes_threshold);
}

local_monitor_fixture::~local_monitor_fixture() {
    clusterlog.info("{}: destroy", __func__);
    std::error_code err;
    std::filesystem::remove_all(std::filesystem::path(_test_path), err);
    if (err) {
        clusterlog.warn("Cleanup got error {} removing test dir.", err);
    }
}

node::local_state local_monitor_fixture::update_state() {
    _local_monitor.update_state()
      .then([&]() { clusterlog.info("Updated local state."); })
      .get();
    return _local_monitor.get_state_cached();
}

struct statvfs local_monitor_fixture::make_statvfs(
  size_t blk_free, size_t blk_total, size_t blk_size) {
    struct statvfs s = {
      .f_frsize = blk_size, .f_blocks = blk_total, .f_bfree = blk_free};
    return s;
}

void local_monitor_fixture::set_config_alert_thresholds(
  unsigned percent, size_t bytes) {
    (void)ss::smp::invoke_on_all([percent, bytes]() {
        config::shard_local_cfg()
          .get("storage_space_alert_free_threshold_bytes")
          .set_value(std::any_cast<size_t>(bytes));
        config::shard_local_cfg()
          .get("storage_space_alert_free_threshold_percent")
          .set_value(std::any_cast<unsigned>(percent));
    });
}

FIXTURE_TEST(local_state_has_single_disk, local_monitor_fixture) {
    auto ls = update_state();
    BOOST_TEST_REQUIRE(ls.disks.size() == 1);
}

FIXTURE_TEST(local_monitor_inject_statvfs, local_monitor_fixture) {
    static constexpr auto free = 100UL, total = 200UL, block_size = 4096UL;
    struct statvfs stats = make_statvfs(free, total, block_size);
    auto lamb = [&](const ss::sstring& _ignore) { return stats; };
    _local_monitor.set_statvfs_for_test(lamb);

    auto ls = update_state();
    BOOST_TEST_REQUIRE(ls.disks.size() == 1);
    BOOST_TEST_REQUIRE(ls.disks[0].total == total * block_size);
    BOOST_TEST_REQUIRE(ls.disks[0].free == free * block_size);
}

FIXTURE_TEST(local_monitor_alert_on_space_percent, local_monitor_fixture) {
    // Minimum by %: 200 * 4k block = 800KiB total * 0.05 -> 40 KiB
    // Minimum by bytes:                                      1 GiB
    static constexpr auto total = 200UL, free = 0UL, block_size = 4096UL;
    size_t min_free_percent_blocks = total
                                     * (default_percent_threshold / 100.0);
    struct statvfs stats = make_statvfs(free, total, block_size);
    auto lamb = [&](const ss::sstring&) { return stats; };
    _local_monitor.set_statvfs_for_test(lamb);

    // One block over the threshold should not alert
    stats.f_bfree = min_free_percent_blocks + 1;
    auto ls = update_state();
    BOOST_TEST_REQUIRE(ls.storage_space_alert == node::disk_space_alert::ok);

    // One block under the free threshold should alert
    stats.f_bfree = min_free_percent_blocks - 1;
    ls = update_state();
    BOOST_TEST_REQUIRE(ls.storage_space_alert != node::disk_space_alert::ok);
}

FIXTURE_TEST(local_monitor_alert_on_space_bytes, local_monitor_fixture) {
    // Minimum by %: 30 GiB total * 0.05 => 1.5 GiB
    // Minimum by bytes:                    1   GiB
    static constexpr auto total = 30 * 1024 * 1024 * 1024UL,
                          block_size = 1024UL;
    static constexpr auto min_bytes_in_blocks = default_bytes_threshold
                                                / block_size;
    clusterlog.debug(
      "{}: bytes free threshold -> {} blocks", __func__, min_bytes_in_blocks);

    // Minimum bytes + one block -> No alert
    struct statvfs stats = make_statvfs(
      min_bytes_in_blocks + 1, total, block_size);
    auto lamb = [&](const ss::sstring&) { return stats; };
    _local_monitor.set_statvfs_for_test(lamb);

    auto ls = update_state();
    BOOST_TEST_REQUIRE(ls.storage_space_alert == node::disk_space_alert::ok);

    // Min bytes threshold minus a blocks -> Alert
    stats.f_bfree = min_bytes_in_blocks - 1;
    ls = update_state();
    BOOST_TEST_REQUIRE(ls.storage_space_alert != node::disk_space_alert::ok);
}