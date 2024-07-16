// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "json/document.h"

#include <fmt/format.h>

#include <stdexcept>

namespace iceberg {

std::optional<std::reference_wrapper<const json::Value>>
parse_optional(const json::Value& v, const char* member_name) {
    if (!v.IsObject()) {
        throw std::invalid_argument(
          fmt::format("Expected JSON object to parse field '{}'", member_name));
    }
    auto iter = v.FindMember(member_name);
    if (iter == v.MemberEnd()) {
        return std::nullopt;
    }
    return iter->value;
}

const json::Value&
parse_required(const json::Value& v, const char* member_name) {
    if (!v.IsObject()) {
        throw std::invalid_argument(
          fmt::format("Expected JSON object to parse field '{}'", member_name));
    }
    auto iter = v.FindMember(member_name);
    if (iter == v.MemberEnd()) {
        throw std::invalid_argument(
          fmt::format("No member named '{}'", member_name));
    }
    return iter->value;
}

ss::sstring parse_required_str(const json::Value& v, const char* member_name) {
    const auto& str_json = parse_required(v, member_name);
    if (!str_json.IsString()) {
        throw std::invalid_argument(
          fmt::format("Expected string for field '{}'", member_name));
    }
    return str_json.GetString();
}

int32_t parse_required_i32(const json::Value& v, const char* member_name) {
    const auto& int_json = parse_required(v, member_name);
    if (!int_json.IsInt()) {
        throw std::invalid_argument(
          fmt::format("Expected integer for field '{}'", member_name));
    }
    return int_json.GetInt();
}

bool parse_required_bool(const json::Value& v, const char* member_name) {
    const auto& bool_json = parse_required(v, member_name);
    if (!bool_json.IsBool()) {
        throw std::invalid_argument(
          fmt::format("Expected bool for field '{}'", member_name));
    }
    return bool_json.GetBool();
}

} // namespace iceberg
