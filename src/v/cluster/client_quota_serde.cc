
// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "client_quota_serde.h"

#include "serde/rw/envelope.h"
#include "serde/rw/set.h"     // IWYU pragma: keep
#include "serde/rw/sstring.h" // IWYU pragma: keep
#include "utils/to_string.h"

#include <seastar/util/variant_utils.hh>

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

std::ostream&
operator<<(std::ostream& os, const entity_key::part::user_default_match&) {
    fmt::print(os, "user_default_match{{}}");
    return os;
}

std::ostream&
operator<<(std::ostream& os, const entity_key::part::user_match& u) {
    fmt::print(os, "user_match{{value:{}}}", u.value);
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

std::ostream& operator<<(std::ostream& os, const entity_key::part_v0& part) {
    fmt::print(os, "{}", part.part);
    return os;
}

std::ostream& operator<<(std::ostream& os, const entity_key::part_t& part) {
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

bool entity_key::part::is_v0_compat() const {
    const auto v0_fields = [](const auto&) { return true; };
    const auto not_v0_fields = [](const auto&) { return false; };
    return ss::visit(part, part_v0::visitor{v0_fields, not_v0_fields});
}

// This is used to make entity_part::part_t backwards compatible. Currently,
// serde::variant atomic and not compatible with any other variant type.
// To enable forward-compatible behavior, old version is being deserialized as
// the old type and then converted. The envelope header dictates what variant
// type was serialized. This extra layer can be removed after v26.2
void entity_key::part::serde_read(iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;

    if (h._version == 0) {
        auto lv0 = read_nested<part_v0::variant>(in, h._bytes_left_limit);
        ss::visit(lv0, [this](auto value) { part = value; });
    } else {
        part = read_nested<part::variant>(in, h._bytes_left_limit);
    }

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

void entity_key::part::serde_write(iobuf& out) const {
    serde::write(out, part);
}

void tag_invoke(
  serde::tag_t<serde::read_tag>,
  iobuf_parser& in,
  entity_key::part_t& p,
  const std::size_t bytes_left_limit) {
    using serde::read_nested;
    p = entity_key::part_t{read_nested<entity_key::part>(in, bytes_left_limit)};
}

// This is used to make entity_part::part_t backwards compatible. Currently,
// serde::variant atomic and not compatible with any other variant type.
// To enable backwards-compatible behavior, values existing in the previous
// versions are being serialized as the old type. This cannot happen in
// entity_type::part because it also needs to have an envelope header that is
// version<0>. This extra layer can be removed after v26.2
void tag_invoke(
  serde::tag_t<serde::write_tag>, iobuf& out, const entity_key::part_t& part) {
    const auto v0_write = [&out](const auto& v) {
        serde::write(out, entity_key::part_v0{.part = v});
    };
    const auto write = [&out](const auto& v) {
        serde::write(out, entity_key::part{.part = v});
    };
    ss::visit(part.part, entity_key::part_v0::visitor{v0_write, write});
}

} // namespace cluster::client_quota
