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

#include "serde/parquet/shredder.h"

#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"
#include "src/v/serde/parquet/schema.h"

#include <seastar/util/variant_utils.hh>

#include <stdexcept>

namespace serde::parquet {

namespace {

struct traversal_levels {
    uint8_t repetition_depth;
    uint8_t repetition_level;
    uint8_t definition_level;
};

void for_each_required_child(
  value val,
  const indexed_schema_element& element,
  traversal_levels levels,
  absl::FunctionRef<
    void(const indexed_schema_element&, traversal_levels, value)> cb) {
    ss::visit(
      std::move(val),
      [&element, levels, cb](struct_value& fields) {
          if (fields.size() != element.children.size()) {
              throw std::runtime_error(fmt::format(
                "schema/struct mismatch, schema had {} children, struct had {} "
                "fields. At column {}",
                element.children.size(),
                fields.size(),
                element.column_index));
          }
          // Levels don't change for require elements because they always have
          // to be there so no additional bits need to be tracked (they'd be
          // wasteful).
          for (size_t i = 0; i < element.children.size(); ++i) {
              struct_field& member = fields[i];
              const indexed_schema_element& child = element.children[i];
              cb(child, levels, std::move(member.field));
          }
      },
      [&element](null_value&) {
          throw std::runtime_error(fmt::format(
            "unexpected null value for required schema element {}",
            element.name));
      },
      [&element](list_value&) {
          throw std::runtime_error(fmt::format(
            "unexpected list value for non-repeated schema element {}",
            element.name));
      },
      [&element](map_value&) {
          throw std::runtime_error(fmt::format(
            "unexpected map value for non-repeated schema element {}",
            element.name));
      },
      [&element](auto& v) {
          throw std::runtime_error(fmt::format(
            "unexpected leaf value for required schema element {}: {}",
            element.name,
            value(std::move(v))));
      });
}

void for_each_optional_child(
  value val,
  const indexed_schema_element& element,
  traversal_levels levels,
  absl::FunctionRef<
    void(const indexed_schema_element&, traversal_levels, value)> cb) {
    ss::visit(
      std::move(val),
      [&element, levels, cb](struct_value& fields) {
          if (fields.size() != element.children.size()) {
              throw std::runtime_error(fmt::format(
                "schema/struct mismatch, schema had {} children, struct had {} "
                "fields. At column {}",
                element.children.size(),
                fields.size(),
                element.column_index));
          }
          traversal_levels child_levels = levels;
          // Increment the definition level as this node in the hierarchy is
          // defined.
          ++child_levels.definition_level;
          for (size_t i = 0; i < element.children.size(); ++i) {
              struct_field& member = fields[i];
              const indexed_schema_element& child = element.children[i];
              cb(child, child_levels, std::move(member.field));
          }
      },
      [&element, levels, cb](null_value& v) {
          // If the value is `null`, we use the parent definition_level so that
          // assembly can determine where the `null` started.
          for (const auto& child : element.children) {
              cb(child, levels, value(v));
          }
      },
      [&element](list_value&) {
          throw std::runtime_error(fmt::format(
            "unexpected list value for non-repeated schema element {}",
            element.name));
      },
      [&element](map_value&) {
          throw std::runtime_error(fmt::format(
            "unexpected map value for non-repeated schema element {}",
            element.name));
      },
      [&element](auto& v) {
          throw std::runtime_error(fmt::format(
            "unexpected leaf value for optional schema element {}: {}",
            element.name,
            value(std::move(v))));
      });
}

void for_each_repeated_child(
  value val,
  const indexed_schema_element& element,
  traversal_levels levels,
  absl::FunctionRef<
    void(const indexed_schema_element&, traversal_levels, value)> cb) {
    ss::visit(
      std::move(val),
      [&element, levels, cb](null_value& v) {
          for_each_optional_child(v, element, levels, cb);
      },
      [&element, levels, cb](list_value& list) {
          // Empty lists are equivalent to a `null` value.
          if (list.empty()) {
              for_each_optional_child(null_value(), element, levels, cb);
              return;
          }
          traversal_levels child_levels = levels;
          // We are marking that there is a higher depth here we
          // need to record, but the repetition_level is only set
          // *when* we are repeating (so not for the first element).
          ++child_levels.repetition_depth;
          for (auto& member : list) {
              for_each_optional_child(
                std::move(member.element), element, child_levels, cb);
              child_levels.repetition_level = child_levels.repetition_depth;
          }
      },
      [&element](map_value&) {
          throw std::runtime_error(
            fmt::format("TODO(rockwood) handle maps: {}", element.name));
      },
      [&element](struct_value&) {
          throw std::runtime_error(fmt::format(
            "unexpected struct value for repeated schema element {}",
            element.name));
      },
      [&element](auto& v) {
          throw std::runtime_error(fmt::format(
            "unexpected leaf value for repeated schema element {}: {}",
            element.name,
            value(std::move(v))));
      });
}

void for_each_child(
  value val,
  const indexed_schema_element& element,
  traversal_levels levels,
  absl::FunctionRef<
    void(const indexed_schema_element&, traversal_levels, value)> cb) {
    switch (element.repetition_type) {
    case field_repetition_type::required:
        for_each_required_child(std::move(val), element, levels, cb);
        break;
    case field_repetition_type::optional:
        for_each_optional_child(std::move(val), element, levels, cb);
        break;
    case field_repetition_type::repeated:
        for_each_repeated_child(std::move(val), element, levels, cb);
        break;
    }
}

void emit_leaf(
  const indexed_schema_element& element,
  value val,
  traversal_levels levels,
  absl::FunctionRef<void(shredded_value)> cb) {
    ss::visit(
      std::move(val),
      [cb, &element, levels](list_value& list) {
          // Empty lists are treated as `null`
          if (list.empty()) {
              emit_leaf(element, null_value(), levels, cb);
              return;
          }
          traversal_levels child_levels = levels;
          // We are marking that there is a higher depth here we
          // need to record, but the repetition_level is only set
          // *when* we are repeating (so not for the first element).
          ++child_levels.repetition_depth;
          for (auto& member : list) {
              emit_leaf(element, std::move(member.element), child_levels, cb);
              child_levels.repetition_level = child_levels.repetition_depth;
          }
      },
      [&element](map_value&) {
          throw std::runtime_error(
            fmt::format("TODO(rockwood) handle maps: {}", element.name));
      },
      [&element](struct_value&) {
          throw std::runtime_error(fmt::format(
            "unexpected struct value for leaf schema element {}",
            element.name));
      },
      [cb, &element, levels](null_value& v) {
          cb({
            .column_index = element.column_index,
            .val = v,
            .rep_level = levels.repetition_level,
            .def_level = levels.definition_level,
          });
      },
      [cb, &element, levels](auto& v) {
          traversal_levels leaf_levels = levels;
          if (element.repetition_type != field_repetition_type::required) {
              ++leaf_levels.definition_level;
          }
          cb({
            .column_index = element.column_index,
            .val = value(std::move(v)),
            .rep_level = leaf_levels.repetition_level,
            .def_level = leaf_levels.definition_level,
          });
      });
}

void shred_record(
  const indexed_schema_element& element,
  value val,
  traversal_levels levels,
  absl::FunctionRef<void(shredded_value)> cb) {
    if (element.is_leaf()) {
        emit_leaf(element, std::move(val), levels, cb);
    } else {
        for_each_child(
          std::move(val),
          element,
          levels,
          [cb](
            const indexed_schema_element& element,
            traversal_levels levels,
            value val) { shred_record(element, std::move(val), levels, cb); });
    }
}

} // namespace

void shred_record(
  const indexed_schema_element& root,
  struct_value record,
  absl::FunctionRef<void(shredded_value)> callback) {
    shred_record(root, std::move(record), traversal_levels{}, callback);
}

} // namespace serde::parquet
