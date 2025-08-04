/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "absl/strings/match.h"
#include "security/license.h"

namespace security::testing {

constexpr std::string_view skip_no_license_msg
  = "Skipping the test without a valid license";

inline bool is_on_ci() {
    const char* ci_env = std::getenv("CI");
    return ci_env && absl::EqualsIgnoreCase(ci_env, "true");
}

/// Retrieves and constructs a license from an environment variable. If the
/// environment variable is missing, std::nullopt is returned outside of CI
/// (which is typically used as a signal to skip the test) and an exception is
/// thrown in CI if missing.
inline std::optional<security::license>
get_test_license(const char* env_var = "REDPANDA_SAMPLE_LICENSE") {
    const char* sample_valid_license = std::getenv(env_var);
    if (sample_valid_license == nullptr) {
        if (is_on_ci()) {
            throw std::runtime_error{fmt::format(
              "Expecting the {} env var in the CI environment", env_var)};
        }
        return std::nullopt;
    }
    const ss::sstring license_str{sample_valid_license};
    return security::make_license(license_str);
}

} // namespace security::testing
