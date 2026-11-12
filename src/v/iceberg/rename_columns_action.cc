/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/rename_columns_action.h"

#include "iceberg/schema.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"

#include <seastar/core/coroutine.hh>

namespace iceberg {

namespace {

/// Finds a mutable field by name within a struct, descending into nested
/// struct types and list element types for intermediate path components.
/// Returns the leaf field, or nullptr if the path cannot be resolved.
nested_field*
find_mutable_field(struct_type& st, std::span<const ss::sstring> path) {
    if (path.empty()) {
        return nullptr;
    }
    for (auto& f : st.fields) {
        if (f->name != path[0]) {
            continue;
        }
        if (path.size() == 1) {
            return f.get();
        }
        auto remaining = path.subspan(1);
        return std::visit(
          [&](auto& inner) -> nested_field* {
              using T = std::decay_t<decltype(inner)>;
              if constexpr (std::is_same_v<T, struct_type>) {
                  return find_mutable_field(inner, remaining);
              } else if constexpr (std::is_same_v<T, list_type>) {
                  if (!inner.element_field) {
                      return nullptr;
                  }
                  if (
                    auto* inner_st = std::get_if<struct_type>(
                      &inner.element_field->type)) {
                      return find_mutable_field(*inner_st, remaining);
                  }
                  return nullptr;
              } else if constexpr (std::is_same_v<T, map_type>) {
                  if (inner.value_field) {
                      if (
                        auto* inner_st = std::get_if<struct_type>(
                          &inner.value_field->type)) {
                          return find_mutable_field(*inner_st, remaining);
                      }
                  }
                  return nullptr;
              } else {
                  return nullptr;
              }
          },
          f->type);
    }
    return nullptr;
}

} // namespace

rename_columns_action::rename_columns_action(
  const table_metadata& table, chunked_vector<rename_entry> renames)
  : table_(table)
  , renames_(std::move(renames)) {}

ss::future<action::action_outcome> rename_columns_action::build_updates() && {
    if (renames_.empty()) {
        co_return updates_and_reqs{};
    }

    const auto cur_schema_id = table_.current_schema_id;
    auto cur_it = std::ranges::find(
      table_.schemas, cur_schema_id, &schema::schema_id);
    if (cur_it == table_.schemas.end()) {
        co_return errc::unexpected_state;
    }

    auto new_schema = cur_it->copy();

    for (const auto& entry : renames_) {
        auto* field = find_mutable_field(
          new_schema.schema_struct, entry.field_path);
        if (!field) {
            co_return errc::unexpected_state;
        }
        field->name = entry.new_name;
    }

    if (new_schema.schema_struct == cur_it->schema_struct) {
        co_return updates_and_reqs{};
    }

    auto highest_schema_id = cur_schema_id;
    for (const auto& s : table_.schemas) {
        highest_schema_id = std::max(highest_schema_id, s.schema_id);
    }
    new_schema.schema_id = schema::id_t{highest_schema_id() + 1};

    updates_and_reqs ret;
    ret.updates.emplace_back(
      table_update::add_schema{
        .schema = std::move(new_schema),
        .last_column_id = std::nullopt,
      });
    ret.updates.emplace_back(
      table_update::set_current_schema{
        table_update::set_current_schema::last_added});
    ret.requirements.emplace_back(
      table_requirement::assert_current_schema_id{cur_schema_id});
    co_return ret;
}

} // namespace iceberg
