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

using std::ranges::reverse_view;

namespace serde::parquet {

bool schema_element::is_leaf() const {
    return !std::holds_alternative<std::monostate>(type);
}

void index_schema(schema_element& root) {
    int32_t position = 0;
    struct entry {
        rep_level rep_level;
        def_level def_level;
        schema_element* element;
    };
    chunked_vector<entry> to_visit;
    to_visit.push_back({
      .rep_level = rep_level(0),
      .def_level = def_level(0),
      .element = &root,
    });
    while (!to_visit.empty()) {
        auto [rep_level, def_level, element] = to_visit.back();
        to_visit.pop_back();
        element->position = position++;
        if (element->repetition_type != field_repetition_type::required) {
            ++def_level;
        }
        if (element->repetition_type == field_repetition_type::repeated) {
            ++rep_level;
        }
        element->max_definition_level = def_level;
        element->max_repetition_level = rep_level;
        for (auto& child : reverse_view(element->children)) {
            to_visit.push_back({
              .rep_level = rep_level,
              .def_level = def_level,
              .element = &child,
            });
        }
    }
}

} // namespace serde::parquet

auto fmt::formatter<serde::parquet::schema_element>::format(
  const serde::parquet::schema_element& e,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{position: {}, type: {}, repetition_type: {}, max_def_level: {}, "
      "max_rep_level: {}, name: {}, logical_type: "
      "{}, field_id: {}, children: [{}]}}",
      e.position,
      e.type.index(),
      static_cast<uint8_t>(e.repetition_type),
      e.max_definition_level(),
      e.max_repetition_level(),
      e.name,
      e.logical_type.index(),
      e.field_id.value_or(-1),
      fmt::join(e.children, ", "));
}
