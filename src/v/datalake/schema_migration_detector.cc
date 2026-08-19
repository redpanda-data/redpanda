/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/schema_migration_detector.h"

#include "datalake/table_definition.h"

namespace datalake {

using namespace iceberg;

namespace {

const nested_field*
find_field_by_name(const struct_type& st, std::string_view name) {
    for (const auto& f : st.fields) {
        if (f && f->name == name) {
            return f.get();
        }
    }
    return nullptr;
}

bool is_type(const field_type& ft, auto type_checker) {
    if (!std::holds_alternative<primitive_type>(ft)) {
        return false;
    }
    return type_checker(std::get<primitive_type>(ft));
}

} // namespace

chunked_vector<column_migration_entry>
detect_redpanda_struct_renames(const schema& table_schema) {
    chunked_vector<column_migration_entry> result;

    const auto* rp_field = find_field_by_name(
      table_schema.schema_struct, rp_struct_name);
    if (!rp_field) {
        return result;
    }
    if (!std::holds_alternative<struct_type>(rp_field->type)) {
        return result;
    }
    const auto& rp_struct = std::get<struct_type>(rp_field->type);

    const auto* ts_field = find_field_by_name(rp_struct, "timestamp");
    if (ts_field) {
        bool is_old_type = is_type(ts_field->type, [](const primitive_type& p) {
            return std::holds_alternative<timestamp_type>(p);
        });
        if (is_old_type) {
            result.emplace_back(
              column_migration_entry{
                .field_path = {"redpanda", "timestamp"},
                .column_path = "redpanda.timestamp",
                .current_name = "timestamp",
                .suggested_new_name = "timestamp_v1",
                .reason = "timestamp -> timestamptz",
              });
        }
    }

    const auto* headers_field = find_field_by_name(rp_struct, "headers");
    if (
      headers_field && std::holds_alternative<list_type>(headers_field->type)) {
        const auto& headers_list = std::get<list_type>(headers_field->type);
        if (
          headers_list.element_field
          && std::holds_alternative<struct_type>(
            headers_list.element_field->type)) {
            const auto& elem_struct = std::get<struct_type>(
              headers_list.element_field->type);
            const auto* key_field = find_field_by_name(elem_struct, "key");
            if (key_field) {
                bool is_old_type = is_type(
                  key_field->type, [](const primitive_type& p) {
                      return std::holds_alternative<binary_type>(p);
                  });
                if (is_old_type) {
                    result.emplace_back(
                      column_migration_entry{
                        .field_path = {"redpanda", "headers", "key"},
                        .column_path = "redpanda.headers.key",
                        .current_name = "key",
                        .suggested_new_name = "key_v1",
                        .reason = "binary -> string",
                      });
                }
            }
        }
    }

    return result;
}

chunked_vector<column_migration_entry> detect_user_schema_renames(
  const struct_type& existing,
  const struct_type& expected,
  const ss::sstring& path_prefix) {
    chunked_vector<column_migration_entry> result;

    for (const auto& expected_field : expected.fields) {
        if (!expected_field) {
            continue;
        }
        if (expected_field->name == rp_struct_name) {
            continue;
        }

        const auto* existing_field = find_field_by_name(
          existing, expected_field->name);
        if (!existing_field) {
            continue;
        }

        ss::sstring field_path_str
          = path_prefix.empty()
              ? expected_field->name
              : ss::sstring(
                  fmt::format("{}.{}", path_prefix, expected_field->name));

        if (
          std::holds_alternative<struct_type>(existing_field->type)
          && std::holds_alternative<struct_type>(expected_field->type)) {
            auto nested = detect_user_schema_renames(
              std::get<struct_type>(existing_field->type),
              std::get<struct_type>(expected_field->type),
              field_path_str);
            for (auto& e : nested) {
                result.emplace_back(std::move(e));
            }
            continue;
        }

        if (existing_field->type != expected_field->type) {
            // Build nested path by splitting the dot-separated path.
            std::vector<ss::sstring> nested_path;
            size_t start = 0;
            while (start < field_path_str.size()) {
                auto dot = field_path_str.find('.', start);
                if (dot == ss::sstring::npos) {
                    nested_path.emplace_back(field_path_str.substr(start));
                    break;
                }
                nested_path.emplace_back(
                  field_path_str.substr(start, dot - start));
                start = dot + 1;
            }

            result.emplace_back(
              column_migration_entry{
                .field_path = std::move(nested_path),
                .column_path = field_path_str,
                .current_name = existing_field->name,
                .suggested_new_name = fmt::format(
                  "{}_v1", existing_field->name),
                .reason = "type changed",
              });
        }
    }

    return result;
}

} // namespace datalake
