/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/conversion/json_schema/ir.h"

#include "strings/string_switch.h"

#include <fmt/core.h>

namespace iceberg::conversion::json_schema {

json_value_type parse_json_value_type(const std::string& str) {
    return string_switch<json_value_type>(str)
      .match("null", json_value_type::null)
      .match("boolean", json_value_type::boolean)
      .match("object", json_value_type::object)
      .match("array", json_value_type::array)
      .match("number", json_value_type::number)
      .match("integer", json_value_type::integer)
      .match("string", json_value_type::string);
}

const schema_resource& subschema::base() const {
    // Schema resource is its own base.
    if (auto rsc = dynamic_cast<const schema_resource*>(this)) {
        return *rsc;
    }

    return *parent_base_;
}

} // namespace iceberg::conversion::json_schema

namespace fmt {
using namespace iceberg::conversion::json_schema;

auto fmt::formatter<json_value_type>::format(
  json_value_type c, format_context& ctx) const -> format_context::iterator {
    switch (c) {
    case json_value_type::null:
        return formatter<string_view>::format("null", ctx);
    case json_value_type::boolean:
        return formatter<string_view>::format("boolean", ctx);
    case json_value_type::object:
        return formatter<string_view>::format("object", ctx);
    case json_value_type::array:
        return formatter<string_view>::format("array", ctx);
    case json_value_type::number:
        return formatter<string_view>::format("number", ctx);
    case json_value_type::integer:
        return formatter<string_view>::format("integer", ctx);
    case json_value_type::string:
        return formatter<string_view>::format("string", ctx);
    }
    vassert(
      false,
      "Invalid json_value_type value: {}",
      static_cast<std::underlying_type_t<json_value_type>>(c));
}

auto fmt::formatter<dialect>::format(dialect c, format_context& ctx) const
  -> format_context::iterator {
    format_context::iterator result = ctx.out();

    bool found = false;
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ((void)((!found && c == dialect_by_schema_id[Is].second)
                  ? (found = true,
                     result = formatter<string_view>::format(
                       dialect_by_schema_id[Is].first, ctx),
                     true)
                  : false),
         ...);
    }(std::make_index_sequence<dialect_by_schema_id.size()>{});

    if (!found) {
        vassert(
          false,
          "Invalid dialect value: {}",
          static_cast<std::underlying_type_t<dialect>>(c));
    }

    return result;
}

} // namespace fmt
