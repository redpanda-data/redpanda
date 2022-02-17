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

#include "config/validators.h"

#include "net/inet_address_wrapper.h"

#include <absl/container/node_hash_set.h>
#include <fmt/format.h>

#include <optional>

namespace config {

std::optional<std::pair<ss::sstring, int64_t>>
parse_connection_rate_override(const ss::sstring& raw_option) {
    auto del_pos = raw_option.find(":");
    if (del_pos == std::string::npos || del_pos == raw_option.size() - 1) {
        return std::nullopt;
    }

    auto ip = raw_option.substr(0, del_pos);
    auto rate_str = raw_option.substr(del_pos + 1);

    std::pair<ss::sstring, int64_t> ans;
    ans.first = ip;
    try {
        ans.second = std::stoi(rate_str);
    } catch (...) {
        return std::nullopt;
    }
    return ans;
}

std::optional<ss::sstring>
validate_connection_rate(const std::vector<ss::sstring>& ips_with_limit) {
    absl::node_hash_set<net::inet_address_wrapper> ip_set;
    for (const auto& ip_and_limit : ips_with_limit) {
        auto parsing_setting = parse_connection_rate_override(ip_and_limit);
        if (!parsing_setting) {
            return fmt::format(
              "Can not parse connection_rate override {}", ip_and_limit);
        }

        ss::net::inet_address addr;
        try {
            addr = ss::net::inet_address(parsing_setting->first);
        } catch (...) {
            return fmt::format(
              "Looks like {} is not ip", parsing_setting->first);
        }

        if (!ip_set.insert(addr).second) {
            return fmt::format(
              "Duplicate setting for ip: {}", parsing_setting->first);
        }
    }

    return std::nullopt;
}

}; // namespace config