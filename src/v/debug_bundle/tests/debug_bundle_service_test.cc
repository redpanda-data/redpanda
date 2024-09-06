/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config/configuration.h"
#include "debug_bundle/debug_bundle_service.h"
#include "debug_bundle/error.h"
#include "random/generators.h"
#include "test_utils/test.h"

#include <seastar/core/seastar.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

struct debug_bundle_service_fixture : public seastar_test {
    ss::future<> SetUpAsync() override {
        const char* script_path = std::getenv("RPK_SHIM");
        ASSERT_NE_CORO(script_path, nullptr)
          << "Missing 'RPK_SHIM' env variable";
        ASSERT_TRUE_CORO(co_await ss::file_exists(script_path))
          << script_path << " does not exist";
        _rpk_shim_path = script_path;

        _data_dir = "test_dir_" + random_generators::gen_alphanum_string(6);
        ASSERT_NO_THROW_CORO(
          co_await ss::recursive_touch_directory(_data_dir.native()))
          << "Failed to create " << _data_dir;

        config::shard_local_cfg().rpk_path.set_value(_rpk_shim_path);
        co_await _service.start(_data_dir);
    }

    ss::future<> TearDownAsync() override { co_await _service.stop(); }

    std::filesystem::path _rpk_shim_path;
    std::filesystem::path _data_dir;
    ss::sharded<debug_bundle::service> _service;
};

TEST_F_CORO(debug_bundle_service_fixture, basic_start_stop) {
    auto debug_bundle_dir = _data_dir
                            / debug_bundle::service::debug_bundle_dir_name;
    EXPECT_FALSE(co_await ss::file_exists(debug_bundle_dir.native()));
    ASSERT_NO_THROW_CORO(co_await _service.invoke_on_all(
      [](debug_bundle::service& s) -> ss::future<> { co_await s.start(); }));
    EXPECT_TRUE(co_await ss::file_exists(debug_bundle_dir.native()));
}

TEST_F_CORO(debug_bundle_service_fixture, bad_rpk_path) {
    config::shard_local_cfg().rpk_path.set_value("/no/such/bin");
    auto debug_bundle_dir = _data_dir
                            / debug_bundle::service::debug_bundle_dir_name;
    EXPECT_FALSE(co_await ss::file_exists(debug_bundle_dir.native()));
    // We should still expect the service to start, but it will emit a warning
    ASSERT_NO_THROW_CORO(co_await _service.invoke_on_all(
      [](debug_bundle::service& s) -> ss::future<> { co_await s.start(); }));
    EXPECT_TRUE(co_await ss::file_exists(debug_bundle_dir.native()));
}
