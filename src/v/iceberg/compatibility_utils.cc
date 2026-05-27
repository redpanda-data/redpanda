/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/compatibility_utils.h"

#include "iceberg/unicode.h"

namespace iceberg {
bool schemas_equivalent(
  const struct_type& source,
  const struct_type& dest,
  field_name_comparison norm) {
    chunked_vector<const nested_field*> source_stk;
    source_stk.reserve(source.fields.size());
    for (const auto& f : std::ranges::reverse_view(source.fields)) {
        source_stk.push_back(f.get());
    }
    chunked_vector<const nested_field*> dest_stk;
    dest_stk.reserve(dest.fields.size());
    for (const auto& f : std::ranges::reverse_view(dest.fields)) {
        dest_stk.push_back(f.get());
    }

    while (!source_stk.empty() && !dest_stk.empty()) {
        const auto* sf = source_stk.back();
        source_stk.pop_back();
        const auto* df = dest_stk.back();
        dest_stk.pop_back();

        if (sf == nullptr || df == nullptr) {
            if (sf != df) {
                return false;
            }
            continue;
        }

        static constexpr auto is_primitive = [](const field_type& ft) -> bool {
            return std::holds_alternative<primitive_type>(ft);
        };

        static constexpr auto get_index = [](const field_type& ft) -> size_t {
            if (is_primitive(ft)) {
                return std::get<primitive_type>(ft).index();
            }
            return ft.index();
        };

        const bool names_match = names_equal(sf->name, df->name, norm);
        if (
          !names_match || sf->required != df->required
          || is_primitive(sf->type) != is_primitive(df->type)
          || get_index(sf->type) != get_index(df->type)) {
            return false;
        }
        std::visit(
          reverse_const_field_collecting_visitor{source_stk}, sf->type);
        std::visit(reverse_const_field_collecting_visitor{dest_stk}, df->type);
    }
    return source_stk.empty() && dest_stk.empty();
}
} // namespace iceberg
