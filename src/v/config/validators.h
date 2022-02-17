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

#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace config {

std::optional<std::pair<ss::sstring, int64_t>>
parse_connection_rate_override(const ss::sstring& raw_option);

std::optional<ss::sstring>
validate_connection_rate(const std::vector<ss::sstring>& ips_with_limit);

}; // namespace config