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

#pragma once

#include "config/client_group_byte_rate_quota.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <optional>
#include <vector>

namespace config {

std::optional<std::pair<ss::sstring, int64_t>>
parse_connection_rate_override(const ss::sstring& raw_option);

std::optional<ss::sstring>
validate_connection_rate(const std::vector<ss::sstring>& ips_with_limit);

std::optional<ss::sstring> validate_client_groups_byte_rate_quota(
  const std::unordered_map<ss::sstring, config::client_group_quota>&);

std::optional<ss::sstring>
validate_sasl_mechanisms(const std::vector<ss::sstring>& mechanisms);

std::optional<ss::sstring> validate_0_to_1_ratio(const double d);

std::optional<ss::sstring>
validate_non_empty_string_vec(const std::vector<ss::sstring>&);

std::optional<ss::sstring>
validate_non_empty_string_opt(const std::optional<ss::sstring>&);

}; // namespace config
