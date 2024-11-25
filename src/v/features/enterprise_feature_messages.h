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

#pragma once

#include "base/seastarx.h"
#include "ssx/sformat.h"

#include <seastar/core/sstring.hh>

#include <fmt/core.h>

#include <string>
#include <string_view>

namespace features::enterprise_error_message {

constexpr std::string_view required
  = "A Redpanda Enterprise Edition license is required";

constexpr std::string_view request_or_trial
  = "To request an Enterprise license, please visit "
    "https://redpanda.com/upgrade. To try Redpanda Enterprise for 30 days, "
    "visit https://redpanda.com/try-enterprise.";

constexpr std::string_view get_started
  = "For more information, see "
    "<https://docs.redpanda.com/current/get-started/licenses>.";

inline ss::sstring license_nag(const auto& features) {
    return ssx::sformat(
      "{} to use the currently enabled feature(s): ({}). Please provide a "
      "valid license key via rpk using 'rpk cluster license set <key>', or "
      "via Redpanda Console. {} {}",
      required,
      fmt::join(features, ", "),
      request_or_trial,
      get_started);
}

inline constexpr std::string upgrade_failure(const auto& features) {
    return fmt::format(
      "{} to use the currently enabled feature(s): ({}). To apply your "
      "license, downgrade this broker to the pre-upgrade version and provide a "
      "valid license key via rpk using 'rpk cluster license set <key>', or via "
      "Redpanda Console. {} {}",
      required,
      fmt::join(features, ", "),
      request_or_trial,
      get_started);
}

inline ss::sstring cluster_property(std::string_view name, auto value) {
    return ssx::sformat(
      "Rejected {}: '{}' '{}' is restricted to clusters with an Enterprise "
      "license. {}",
      name,
      value,
      name,
      request_or_trial);
}

inline ss::sstring partition_autobalancing_continuous() {
    return ssx::sformat(
      R"({} to use feature "partition_autobalancing_mode" with value "continuous".  Behavior is restricted to "node_add". {})",
      required,
      request_or_trial);
}

inline ss::sstring core_balancing_continuous() {
    return ssx::sformat(
      R"({} to use enterprise feature "core_balancing_continuous".  This property is being ignored. {})",
      required,
      request_or_trial);
}

inline ss::sstring default_leaders_preference() {
    return ssx::sformat(
      R"({} to use the enterprise feature "leadership pinning".  This feature is disabled.  The values of the cluster property "default_leaders_preference" and the topic property "redpanda.leaders.preference" are being ignored. {})",
      required,
      request_or_trial);
}

inline ss::sstring topic_property(auto features) {
    return ssx::sformat(
      "{} to enable ({}). {}.",
      required,
      fmt::join(features, ", "),
      request_or_trial);
}

inline ss::sstring create_partition(auto features) {
    return ssx::sformat(
      "{} to create partitions with ({}). {}",
      required,
      fmt::join(features, ", "),
      request_or_trial);
}

inline ss::sstring acl_with_rbac() {
    return ssx::sformat(
      "{} to create an ACL with a role binding. {}",
      required,
      request_or_trial);
}

inline ss::sstring audit_log_fetch() {
    return ssx::sformat(
      "{} to consume the audit log topic. {}", required, request_or_trial);
}

} // namespace features::enterprise_error_message
