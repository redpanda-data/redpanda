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

inline ss::logger logger(__FILE__); // NOLINT static may throw

local_monitor_fixture::local_monitor_fixture() {
    logger.info("{}: create", __func__);
    auto test_dir = "local_monitor_test."
                    + random_generators::gen_alphanum_string(4);

    _test_path = std::filesystem::absolute(test_dir.c_str());

    std::error_code errc;
    std::filesystem::create_directory(_test_path, errc);
    if (errc) {
        logger.warn(
          "{}: failed to create test dir {}: {}", __func__, _test_path, errc);
    } else {
        logger.info("{}: created test dir {}", __func__, _test_path);
    }
    _local_monitor.set_path_for_test(_test_path.string());
    BOOST_ASSERT(ss::engine_is_ready());
}

local_monitor_fixture::~local_monitor_fixture() {
    logger.info("{}: destroy", __func__);
    std::error_code err;
    std::filesystem::remove_all(std::filesystem::path(_test_path), err);
    if (err) {
        logger.warn("Cleanup got error {} removing test dir.", err);
    }
}

cluster::node::local_state local_monitor_fixture::update_state() {
    _local_monitor.update_state()
      .then([&]() { logger.info("Updated local state."); })
      .get();
    return _local_monitor.get_state_cached();
}

struct statvfs local_monitor_fixture::make_statvfs(
  unsigned long blk_free, unsigned long blk_total, unsigned long blk_size) {
    struct statvfs s = {
      .f_frsize = blk_size, .f_blocks = blk_total, .f_bfree = blk_free};
    return s;
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