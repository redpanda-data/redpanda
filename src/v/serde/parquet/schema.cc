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

bool schema_element::is_leaf() const {
    return !std::holds_alternative<std::monostate>(type);
}

void index_schema(schema_element& root) {
    int32_t position = 0;
    chunked_vector<schema_element*> to_visit;
    to_visit.push_back(&root);
    while (!to_visit.empty()) {
        schema_element* popped = to_visit.back();
        to_visit.pop_back();
        popped->position = position++;
        for (auto& child : std::ranges::reverse_view(popped->children)) {
            to_visit.push_back(&child);
        }
    }
}

} // namespace serde::parquet

auto fmt::formatter<serde::parquet::schema_element>::format(
  const serde::parquet::schema_element& e,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{position: {}, type: {}, repetition_type: {}, name: {}, logical_type: "
      "{}, field_id: {}, children: [{}]}}",
      e.position,
      e.type.index(),
      static_cast<uint8_t>(e.repetition_type),
      e.name,
      e.logical_type.index(),
      e.field_id.value_or(-1),
      fmt::join(e.children, ", "));
}
