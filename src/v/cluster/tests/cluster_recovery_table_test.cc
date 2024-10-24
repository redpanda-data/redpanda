/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cluster_recovery_table.h"
#include "cluster/commands.h"
#include "cluster/logger.h"
#include "cluster/types.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

namespace cluster {

namespace {
cluster_recovery_init_cmd make_init_cmd(int meta_id) {
    cluster_recovery_init_cmd_data data;
    data.state.manifest.metadata_id
      = cluster::cloud_metadata::cluster_metadata_id{meta_id};
    return cluster_recovery_init_cmd{0, std::move(data)};
}
cluster_recovery_update_cmd make_update_cmd(
  cluster::recovery_stage s, std::optional<ss::sstring> err = std::nullopt) {
    cluster_recovery_update_cmd_data data;
    data.stage = s;
    data.error_msg = std::move(err);
    return cluster_recovery_update_cmd{0, std::move(data)};
}

void set_stage(cluster_recovery_table& t, recovery_stage stage) {
    std::vector<cluster_recovery_state> s;
    cluster_recovery_state state;
    state.stage = stage;
    s.emplace_back(std::move(state));
    t.set_recovery_states(std::move(s));
}

} // anonymous namespace

TEST(ClusterRecovery, InitStateChange) {
    cluster_recovery_table table;
    ASSERT_FALSE(table.current_recovery().has_value());
    ASSERT_FALSE(table.current_status().has_value());

    // The update command needs to follow an init command.
    for (int i = 0; i <= static_cast<int>(recovery_stage::failed); i++) {
        auto stage = static_cast<recovery_stage>(i);
        auto err = table.apply(model::offset{0}, make_update_cmd(stage));
        ASSERT_EQ(err, errc::invalid_request);
    }
    ASSERT_TRUE(!table.current_recovery().has_value());

    // We can initialize if in complete or failed state, but no other state.
    set_stage(table, recovery_stage::complete);
    for (int i = 0; i <= static_cast<int>(recovery_stage::failed); i++) {
        auto stage = static_cast<recovery_stage>(i);
        set_stage(table, stage);

        // Init commands only when existing recoveries are finished.
        if (
          stage == recovery_stage::complete
          || stage == recovery_stage::failed) {
            ASSERT_EQ(
              errc::success, table.apply(model::offset{0}, make_init_cmd(0)));
        } else {
            ASSERT_EQ(
              errc::update_in_progress,
              table.apply(model::offset{0}, make_init_cmd(0)));
        }
    }
}

TEST(ClusterRecovery, StartStateChange) {
    cluster_recovery_table table;
    for (int i = 0; i <= static_cast<int>(recovery_stage::failed); i++) {
        auto stage = static_cast<recovery_stage>(i);
        set_stage(table, stage);

        // Start commands only succeed from the initializing stage.
        auto start_cmd = make_update_cmd(recovery_stage::starting);
        if (stage == recovery_stage::initialized) {
            ASSERT_EQ(errc::success, table.apply(model::offset{0}, start_cmd));
        } else {
            ASSERT_EQ(
              errc::invalid_request, table.apply(model::offset{0}, start_cmd));
        }
    }
}

TEST(ClusterRecovery, FailStateChange) {
    cluster_recovery_table table;
    for (int i = 0; i <= static_cast<int>(recovery_stage::failed); i++) {
        auto stage = static_cast<recovery_stage>(i);
        set_stage(table, stage);

        // Fail commands succeed from any active stage.
        auto fail_cmd = make_update_cmd(recovery_stage::failed);
        if (table.is_recovery_active()) {
            ASSERT_EQ(errc::success, table.apply(model::offset{0}, fail_cmd));
            ASSERT_FALSE(table.is_recovery_active());
            ASSERT_EQ(recovery_stage::failed, table.current_status().value());
        } else {
            ASSERT_EQ(
              errc::invalid_request, table.apply(model::offset{0}, fail_cmd));
        }
    }
}

TEST(ClusterRecovery, CompleteStateChange) {
    cluster_recovery_table table;
    for (int i = 0; i <= static_cast<int>(recovery_stage::failed); i++) {
        auto stage = static_cast<recovery_stage>(i);
        set_stage(table, stage);

        // Complete command succeeds from any active stage.
        auto complete_cmd = make_update_cmd(recovery_stage::complete);
        if (table.is_recovery_active()) {
            ASSERT_EQ(
              errc::success, table.apply(model::offset{0}, complete_cmd));
            ASSERT_TRUE(table.current_status().has_value());
            ASSERT_EQ(recovery_stage::complete, table.current_status().value());
        } else {
            ASSERT_EQ(
              errc::invalid_request,
              table.apply(model::offset{0}, complete_cmd));
        }
    }
}

TEST_CORO(ClusterRecovery, WaitForRecovery) {
    cluster_recovery_table table;
    auto cleanup = ss::defer([&table] { table.stop().get(); });
    auto wait_fut = table.wait_for_active_recovery();
    for (int i = 0; i < 10; i++) {
        co_await ss::yield();
    }
    ASSERT_FALSE_CORO(wait_fut.available());

    // Arbitrary bad updates should not wake waiters.
    ASSERT_EQ_CORO(
      errc::invalid_request,
      table.apply(model::offset{0}, make_update_cmd(recovery_stage::starting)));
    for (int i = 0; i < 10; i++) {
        co_await ss::yield();
    }
    ASSERT_FALSE_CORO(wait_fut.available());

    // Successful init should wake waiters.
    ASSERT_EQ_CORO(
      errc::success, table.apply(model::offset{0}, make_init_cmd(0)));
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      1s, [&wait_fut] { return wait_fut.available(); });
    co_await std::move(wait_fut);
}

} // namespace cluster
