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

#include "serde/parquet/schema.h"

#include <ranges>
#include <variant>

namespace serde::parquet {

namespace {

indexed_schema_element transform(int32_t index, const schema_element& root) {
    indexed_schema_element out{
      .index = index,
      .type = root.type,
      .repetition_type = root.repetition_type,
      .name = root.name,
      .field_id = root.field_id,
      .logical_type = root.logical_type,
    };
    out.children.reserve(root.children.size());
    return out;
}

} // namespace

bool schema_element::is_leaf() const {
    return !std::holds_alternative<std::monostate>(type);
}

bool indexed_schema_element::is_leaf() const {
    return !std::holds_alternative<std::monostate>(type);
}

indexed_schema_element index_schema(const schema_element& root) {
    int32_t index = 0;
    indexed_schema_element indexed_root = transform(index++, root);
    chunked_vector<std::pair<indexed_schema_element*, const schema_element*>>
      to_visit;
    to_visit.reserve(root.children.size());
    for (const auto& child : std::ranges::reverse_view(root.children)) {
        to_visit.emplace_back(&indexed_root, &child);
    }
    while (!to_visit.empty()) {
        const auto [parent, popped] = to_visit.back();
        to_visit.pop_back();
        indexed_schema_element* indexed = &parent->children.emplace_back(
          transform(index++, *popped));
        for (const auto& child : std::ranges::reverse_view(popped->children)) {
            to_visit.emplace_back(indexed, &child);
        }
    }
    return indexed_root;
}

} // namespace serde::parquet

auto fmt::formatter<serde::parquet::schema_element>::format(
  const serde::parquet::schema_element& e, fmt::format_context& ctx) const
  -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{type: {}, repetition_type: {}, name: {}, logical_type: {}, field_id: "
      "{}, children: [{}]}}",
      e.type.index(),
      static_cast<uint8_t>(e.repetition_type),
      e.name,
      e.logical_type.index(),
      e.field_id.value_or(-1),
      fmt::join(e.children, ", "));
}

auto fmt::formatter<serde::parquet::indexed_schema_element>::format(
  const serde::parquet::indexed_schema_element& e,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{index: {}, type: {}, repetition_type: {}, name: {}, "
      "logical_type: {}, field_id: {}, children: [{}]}}",
      e.index,
      e.type.index(),
      static_cast<uint8_t>(e.repetition_type),
      e.name,
      e.logical_type.index(),
      e.field_id.value_or(-1),
      fmt::join(e.children, ", "));
}
