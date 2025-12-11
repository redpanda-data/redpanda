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

#include "proto/redpanda/core/admin/v2/error_reason.pb.h"

#include <gtest/gtest.h>

namespace cluster_link {

// Forward declare internal function for testing
redpanda::core::admin::v2::Reason errc_to_reason(errc ec);

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

TEST(cluster_link_errc_fixture, errc_to_reason_all_values_mapped) {
    using redpanda::core::admin::v2::Reason;

    // Test that all errc values are mapped to a Reason
    struct errc_reason_mapping {
        errc error_code;
        Reason expected_reason;
    };

    const auto test_cases = std::to_array<errc_reason_mapping>({
      {errc::success, Reason::REASON_UNSPECIFIED},
      {errc::link_unsupported_api_version,
       Reason::REASON_SHADOW_LINK_API_VERSION_UNSUPPORTED},
      {errc::link_broker_unreachable,
       Reason::REASON_SHADOW_LINK_BROKER_UNREACHABLE},
      {errc::link_cluster_unreachable,
       Reason::REASON_SHADOW_LINK_CLUSTER_UNREACHABLE},
      {errc::link_broker_verification_failed,
       Reason::REASON_SHADOW_LINK_BROKER_VERIFICATION_FAILED},
      {errc::link_connection_failed,
       Reason::REASON_SHADOW_LINK_CONNECTION_FAILED},
      {errc::link_verification_unknown_error,
       Reason::REASON_SHADOW_LINK_VERIFICATION_UNKNOWN_ERROR},
      {errc::link_creation_failed, Reason::REASON_SHADOW_LINK_CREATION_FAILED},
      {errc::link_id_not_found, Reason::REASON_SHADOW_LINK_NOT_FOUND},
      {errc::service_shutting_down,
       Reason::REASON_SHADOW_LINK_SERVICE_SHUTTING_DOWN},
      {errc::rpc_error, Reason::REASON_SHADOW_LINK_RPC_ERROR},
      {errc::invalid_configuration,
       Reason::REASON_SHADOW_LINK_INVALID_CONFIGURATION},
      {errc::topic_already_mirrored,
       Reason::REASON_SHADOW_TOPIC_ALREADY_MIRRORED},
      {errc::topic_mirrored_by_other_link,
       Reason::REASON_SHADOW_TOPIC_MIRRORED_BY_OTHER_LINK},
      {errc::topic_not_being_mirrored,
       Reason::REASON_SHADOW_TOPIC_NOT_MIRRORED},
      {errc::link_has_active_shadow_topics,
       Reason::REASON_SHADOW_LINK_HAS_ACTIVE_TOPICS},
      {errc::topic_does_not_exist, Reason::REASON_SHADOW_TOPIC_DOES_NOT_EXIST},
      {errc::topic_metadata_stale, Reason::REASON_SHADOW_TOPIC_METADATA_STALE},
      {errc::invalid_task_state_change,
       Reason::REASON_SHADOW_LINK_INVALID_TASK_STATE},
      {errc::task_not_running, Reason::REASON_SHADOW_LINK_TASK_NOT_RUNNING},
      {errc::task_already_running,
       Reason::REASON_SHADOW_LINK_TASK_ALREADY_RUNNING},
      {errc::failed_to_start_task, Reason::REASON_SHADOW_LINK_TASK_START_FAILED},
      {errc::task_creation_failed,
       Reason::REASON_SHADOW_LINK_TASK_CREATION_FAILED},
      {errc::task_already_registered_on_link,
       Reason::REASON_SHADOW_LINK_TASK_ALREADY_REGISTERED},
      {errc::failed_to_stop_task, Reason::REASON_SHADOW_LINK_TASK_STOP_FAILED},
      {errc::failed_to_pause_task, Reason::REASON_SHADOW_LINK_TASK_PAUSE_FAILED},
      {errc::cluster_link_disabled,
       Reason::REASON_SHADOW_LINK_FEATURE_DISABLED},
      {errc::license_required, Reason::REASON_SHADOW_LINK_LICENSE_REQUIRED},
      {errc::link_limit_reached, Reason::REASON_SHADOW_LINK_LIMIT_REACHED},
      {errc::service_not_ready, Reason::REASON_SHADOW_LINK_SERVICE_NOT_READY},
      {errc::remote_cluster_does_not_support_required_api,
       Reason::REASON_SHADOW_LINK_API_VERSION_UNSUPPORTED},
      {errc::failed_to_connect_to_remote_cluster,
       Reason::REASON_SHADOW_LINK_REMOTE_API_UNSUPPORTED},
    });

    for (const auto& tc : test_cases) {
        auto reason = errc_to_reason(tc.error_code);
        EXPECT_EQ(reason, tc.expected_reason)
          << "Failed for errc: " << static_cast<int>(tc.error_code);
    }
}

TEST(cluster_link_errc_fixture, make_error_info_basic) {
    // Test make_error_info with empty metadata
    auto info = make_error_info(errc::link_broker_unreachable);

    EXPECT_EQ(
      info.reason, "REASON_SHADOW_LINK_BROKER_UNREACHABLE");
    EXPECT_EQ(info.domain, "redpanda.com/core");
    EXPECT_TRUE(info.metadata.empty());
}

TEST(cluster_link_errc_fixture, make_error_info_with_metadata) {
    // Test make_error_info with metadata
    chunked_hash_map<ss::sstring, ss::sstring> metadata;
    metadata.emplace("brokerId", "123");
    metadata.emplace("linkName", "test-link");

    auto info = make_error_info(
      errc::link_cluster_unreachable, std::move(metadata));

    EXPECT_EQ(info.reason, "REASON_SHADOW_LINK_CLUSTER_UNREACHABLE");
    EXPECT_EQ(info.domain, "redpanda.com/core");
    EXPECT_EQ(info.metadata.size(), 2);
    EXPECT_EQ(info.metadata["brokerId"], "123");
    EXPECT_EQ(info.metadata["linkName"], "test-link");
}

TEST(cluster_link_errc_fixture, err_info_with_error_info) {
    // Test err_info with error_info
    chunked_hash_map<ss::sstring, ss::sstring> metadata;
    metadata.emplace("topicName", "test-topic");

    auto error_info = make_error_info(
      errc::topic_already_mirrored, std::move(metadata));

    err_info info(
      errc::topic_already_mirrored,
      "Topic is already mirrored",
      std::move(error_info));

    EXPECT_EQ(info.code(), errc::topic_already_mirrored);
    EXPECT_EQ(info.message(), "Topic is already mirrored");
    ASSERT_TRUE(info.info().has_value());
    EXPECT_EQ(info.info()->reason, "REASON_SHADOW_TOPIC_ALREADY_MIRRORED");
    EXPECT_EQ(info.info()->domain, "redpanda.com/core");
    EXPECT_EQ(info.info()->metadata.size(), 1);
    EXPECT_EQ(info.info()->metadata.at("topicName"), "test-topic");
}

TEST(cluster_link_errc_fixture, err_info_without_error_info) {
    // Test backward compatibility - err_info without error_info
    err_info info(errc::link_id_not_found, "Link not found");

    EXPECT_EQ(info.code(), errc::link_id_not_found);
    EXPECT_EQ(info.message(), "Link not found");
    EXPECT_FALSE(info.info().has_value());
}

TEST(cluster_link_errc_fixture, reason_format_validation) {
    // Verify all reason strings follow the REASON_[A-Z0-9_]+ format
    using redpanda::core::admin::v2::Reason;

    const auto all_errc_values = std::to_array<errc>({
      errc::success,
      errc::link_unsupported_api_version,
      errc::link_broker_unreachable,
      errc::link_cluster_unreachable,
      errc::link_broker_verification_failed,
      errc::link_connection_failed,
      errc::link_verification_unknown_error,
      errc::link_creation_failed,
      errc::link_id_not_found,
      errc::service_shutting_down,
      errc::rpc_error,
      errc::invalid_configuration,
      errc::topic_already_mirrored,
      errc::topic_mirrored_by_other_link,
      errc::topic_not_being_mirrored,
      errc::link_has_active_shadow_topics,
      errc::topic_does_not_exist,
      errc::topic_metadata_stale,
      errc::invalid_task_state_change,
      errc::task_not_running,
      errc::task_already_running,
      errc::failed_to_start_task,
      errc::task_creation_failed,
      errc::task_already_registered_on_link,
      errc::failed_to_stop_task,
      errc::failed_to_pause_task,
      errc::cluster_link_disabled,
      errc::license_required,
      errc::link_limit_reached,
      errc::service_not_ready,
      errc::remote_cluster_does_not_support_required_api,
      errc::failed_to_connect_to_remote_cluster,
    });

    for (const auto& ec : all_errc_values) {
        auto info = make_error_info(ec);

        // Verify reason starts with "REASON_"
        EXPECT_TRUE(info.reason.starts_with("REASON_"))
          << "Reason '" << info.reason << "' doesn't start with REASON_";

        // Verify reason is uppercase with underscores only
        for (char c : info.reason) {
            EXPECT_TRUE(
              std::isupper(c) || std::isdigit(c) || c == '_')
              << "Reason '" << info.reason
              << "' contains invalid character: " << c;
        }

        // Verify reason length <= 63 chars (protobuf requirement)
        EXPECT_LE(info.reason.size(), 63)
          << "Reason '" << info.reason << "' exceeds 63 characters";
    }
}

} // namespace cluster_link
