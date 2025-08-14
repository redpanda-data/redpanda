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

#include "cluster_link/errc.h"

#include <gtest/gtest.h>

namespace cluster_link {

TEST(cluster_link_errc_fixture, errc_all_values_message_test) {
    struct errc_expected {
        errc value;
        const char* expected_message;
    };

    const auto test_cases = std::to_array<errc_expected>({
      {.value = errc::success, .expected_message = "success"},
      {.value = errc::invalid_task_state_change,
       .expected_message = "invalid task state change"},
      {.value = errc::task_not_running, .expected_message = "task not running"},
      {.value = errc::task_already_running,
       .expected_message = "task already running"},
      {.value = errc::failed_to_start_task,
       .expected_message = "failed to start task"},
      {.value = errc::task_already_registered_on_link,
       .expected_message = "task already registered on link"},
      {.value = errc::failed_to_connect_to_remote_cluster,
       .expected_message = "failed to connect to remote cluster"},
      {.value = errc::remote_cluster_does_not_support_required_api,
       .expected_message = "remote cluster does not support required API"},
    });

    for (const auto& tc : test_cases) {
        auto ec = make_error_code(tc.value);
        EXPECT_EQ(ec.value(), static_cast<int>(tc.value));
        EXPECT_EQ(ec.category().name(), std::string("cluster_link"));
        EXPECT_EQ(ec.message(), std::string(tc.expected_message));
    }
}

TEST(cluster_link_errc_fixture, errc_unknown_value_message_test) {
    // Use a value not defined in errc
    int unknown_value = 9999;
    auto ec = std::error_code(unknown_value, error_category());
    EXPECT_EQ(ec.message(), "(unknown error code)");
}

TEST(cluster_link_errc_fixture, err_info_test) {
    err_info info(errc::invalid_task_state_change, "Custom error message");
    EXPECT_EQ(info.code(), errc::invalid_task_state_change);
    EXPECT_EQ(info.message(), "Custom error message");

    err_info default_info(errc::success);
    EXPECT_EQ(default_info.code(), errc::success);
    EXPECT_EQ(default_info.message(), "success");
}
} // namespace cluster_link
