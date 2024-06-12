
// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "client_quota_serde.h"

#include "utils/to_string.h"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <ostream>

namespace cluster::client_quota {

std::ostream&
operator<<(std::ostream& os, const entity_key::part::client_id_default_match&) {
    fmt::print(os, "client_id_default_match{{}}");
    return os;
}

std::ostream&
operator<<(std::ostream& os, const entity_key::part::client_id_match& c) {
    fmt::print(os, "client_id_match{{value:{}}}", c.value);
    return os;
}

std::ostream& operator<<(
  std::ostream& os, const entity_key::part::client_id_prefix_match& c) {
    fmt::print(os, "client_id_prefix_match{{value:{}}}", c.value);
    return os;
}

std::ostream& operator<<(std::ostream& os, const entity_key::part& part) {
    fmt::print(os, "{}", part.part);
    return os;
}

std::ostream& operator<<(std::ostream& os, const entity_key& key) {
    fmt::print(os, "{{parts: {}}}", key.parts);
    return os;
}

std::ostream& operator<<(std::ostream& os, const entity_value& value) {
    fmt::print(
      os,
      "{{producer_byte_rate: {}, consumer_byte_rate: {}, "
      "controller_mutation_rate: {}}}",
      value.producer_byte_rate,
      value.consumer_byte_rate,
      value.controller_mutation_rate);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const entity_value_diff::entry& entry) {
    switch (entry.op) {
    case entity_value_diff::operation::upsert:
        fmt::print(
          os, "upsert: {}={}", to_string_view(entry.type), entry.value);
        return os;
    case entity_value_diff::operation::remove:
        fmt::print(os, "remove: {}", to_string_view(entry.type));
        return os;
    }
}

std::ostream& operator<<(std::ostream& os, const entity_value_diff& value) {
    fmt::print(os, "{}", value.entries);
    return os;
}

bool operator==(
  const entity_value_diff::entry& lhs, const entity_value_diff::entry& rhs) {
    if (lhs.op != rhs.op) {
        return false;
    }
    switch (lhs.op) {
    case entity_value_diff::operation::upsert:
        return lhs.type == rhs.type && lhs.value == rhs.value;
    case entity_value_diff::operation::remove:
        return lhs.type == rhs.type;
    }
}

} // namespace cluster::client_quota
