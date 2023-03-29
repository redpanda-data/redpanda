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

#include "config/validators.h"

#include "config/client_group_byte_rate_quota.h"
#include "net/inet_address_wrapper.h"
#include "ssx/sformat.h"

#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_set.h>
#include <fmt/format.h>

#include <optional>
#include <unordered_map>

namespace config {

std::optional<std::pair<ss::sstring, int64_t>>
split_string_int_by_colon(const ss::sstring& raw_option) {
    auto del_pos = raw_option.find(":");
    if (del_pos == ss::sstring::npos || del_pos == raw_option.size() - 1) {
        return std::nullopt;
    }

    auto str_val = raw_option.substr(0, del_pos);
    auto int_val = raw_option.substr(del_pos + 1);

    std::pair<ss::sstring, int64_t> ans;
    ans.first = str_val;
    try {
        ans.second = std::stoll(int_val);
    } catch (...) {
        return std::nullopt;
    }
    return ans;
}

std::optional<std::pair<ss::sstring, int64_t>>
parse_connection_rate_override(const ss::sstring& raw_option) {
    return split_string_int_by_colon(raw_option);
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

std::optional<ss::sstring> validate_client_groups_byte_rate_quota(
  const std::unordered_map<ss::sstring, config::client_group_quota>&
    groups_with_limit) {
    for (const auto& gal : groups_with_limit) {
        if (gal.second.quota <= 0) {
            return fmt::format(
              "Quota must be a non zero positive number, got: {}",
              gal.second.quota);
        }

        for (const auto& another_group : groups_with_limit) {
            if (another_group.first == gal.first) {
                continue;
            }
            if (std::string_view(gal.second.clients_prefix)
                  .starts_with(
                    std::string_view(another_group.second.clients_prefix))) {
                return fmt::format(
                  "Group client prefix can not be prefix for another group "
                  "name. "
                  "Violation: {}, {}",
                  gal.second.clients_prefix,
                  another_group.second.clients_prefix);
            }
        }
    }

    return std::nullopt;
}

std::optional<ss::sstring>
validate_sasl_mechanisms(const std::vector<ss::sstring>& mechanisms) {
    static const absl::flat_hash_set<std::string_view> supported{
      "GSSAPI", "SCRAM"};

    // Validate results
    for (const auto& m : mechanisms) {
        if (!supported.contains(m)) {
            return ssx::sformat("'{}' is not a supported SASL mechanism", m);
        }
    }
    return std::nullopt;
}

std::optional<ss::sstring> validate_0_to_1_ratio(const double d) {
    if (d < 0 || d > 1) {
        return fmt::format("Ratio must be in the [0,1] range, got: {}", d);
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_non_empty_string_vec(const std::vector<ss::sstring>& vs) {
    for (const auto& s : vs) {
        if (s.empty()) {
            return "Empty strings are not valid in this collection";
        }
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_non_empty_string_opt(const std::optional<ss::sstring>& os) {
    if (os.has_value() && os.value().empty()) {
        return "Empty string is not valid";
    } else {
        return std::nullopt;
    }
}
}; // namespace config
