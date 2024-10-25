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

ss::future<> process_required_struct(
  // NOLINTNEXTLINE(*reference*)
  const schema_element& element,
  traversal_levels levels,
  struct_value fields,
  absl::FunctionRef<
    ss::future<>(const schema_element&, traversal_levels, value)> cb) {
    if (fields.size() != element.children.size()) {
        throw std::runtime_error(fmt::format(
          "schema/struct mismatch, schema had {} children, struct had {} "
          "fields. At column {}",
          element.children.size(),
          fields.size(),
          element.position));
    }
    // Levels don't change for require elements because they always have
    // to be there so no additional bits need to be tracked (they'd be
    // wasteful).
    for (size_t i = 0; i < element.children.size(); ++i) {
        struct_field& member = fields[i];
        const schema_element& child = element.children[i];
        co_await cb(child, levels, std::move(member.field));
    }
}

ss::future<> for_each_required_child(
  value val,
  const schema_element& element,
  traversal_levels levels,
  absl::FunctionRef<
    ss::future<>(const schema_element&, traversal_levels, value)> cb) {
    return ss::visit(
      std::move(val),
      [&element, levels, cb](struct_value& fields) -> ss::future<> {
          return process_required_struct(
            element, levels, std::move(fields), cb);
      },
      [&element](null_value&) -> ss::future<> {
          return ss::make_exception_future(std::runtime_error(fmt::format(
            "unexpected null value for required schema element {}",
            element.name)));
      },
      [&element](repeated_value&) -> ss::future<> {
          return ss::make_exception_future(std::runtime_error(fmt::format(
            "unexpected list value for non-repeated schema element {}",
            element.name)));
      },
      [&element](auto& v) -> ss::future<> {
          return ss::make_exception_future(std::runtime_error(fmt::format(
            "unexpected leaf value for required schema element {}: {}",
            element.name,
            value(std::move(v)))));
      });
}

ss::future<> process_optional_struct(
  const schema_element& element,
  traversal_levels levels,
  struct_value fields,
  absl::FunctionRef<
    ss::future<>(const schema_element&, traversal_levels, value)> cb) {
    // Increment the definition level as this node in the hierarchy is
    // defined.
    ++levels.definition_level;
    return process_required_struct(element, levels, std::move(fields), cb);
}

ss::future<> process_null_optional(
  // NOLINTNEXTLINE(*reference*)
  const schema_element& element,
  traversal_levels levels,
  absl::FunctionRef<
    ss::future<>(const schema_element&, traversal_levels, value)> cb) {
    // If the value is `null`, we use the parent definition_level so that
    // assembly can determine where the `null` started.
    for (const auto& child : element.children) {
        co_await cb(child, levels, null_value());
    }
}

ss::future<> for_each_optional_child(
  value val,
  const schema_element& element,
  traversal_levels levels,
  absl::FunctionRef<
    ss::future<>(const schema_element&, traversal_levels, value)> cb) {
    return ss::visit(
      std::move(val),
      [&element, levels, cb](struct_value& fields) -> ss::future<> {
          return process_optional_struct(
            element, levels, std::move(fields), cb);
      },
      [&element, levels, cb](null_value&) -> ss::future<> {
          return process_null_optional(element, levels, cb);
      },
      [&element](repeated_value&) -> ss::future<> {
          return ss::make_exception_future(std::runtime_error(fmt::format(
            "unexpected list value for non-repeated schema element {}",
            element.name)));
      },
      [&element](auto& v) -> ss::future<> {
          return ss::make_exception_future(std::runtime_error(fmt::format(
            "unexpected leaf value for optional schema element {}: {}",
            element.name,
            value(std::move(v)))));
      });
}

ss::future<> process_repeated(
  // NOLINTNEXTLINE(*reference*)
  const schema_element& element,
  traversal_levels levels,
  repeated_value list,
  absl::FunctionRef<
    ss::future<>(const schema_element&, traversal_levels, value)> cb) {
    // Empty lists are equivalent to a `null` value.
    if (list.empty()) {
        co_return co_await for_each_optional_child(
          null_value(), element, levels, cb);
    }
    traversal_levels child_levels = levels;
    // We are marking that there is a higher depth here we
    // need to record, but the repetition_level is only set
    // *when* we are repeating (so not for the first element).
    ++child_levels.repetition_depth;
    for (auto& member : list) {
        co_await for_each_optional_child(
          std::move(member.element), element, child_levels, cb);
        child_levels.repetition_level = child_levels.repetition_depth;
    }
}

ss::future<> for_each_repeated_child(
  value val,
  const schema_element& element,
  traversal_levels levels,
  absl::FunctionRef<
    ss::future<>(const schema_element&, traversal_levels, value)> cb) {
    return ss::visit(
      std::move(val),
      [&element, levels, cb](null_value& v) {
          return for_each_optional_child(v, element, levels, cb);
      },
      [&element, levels, cb](repeated_value& list) {
          return process_repeated(element, levels, std::move(list), cb);
      },
      [&element](struct_value&) {
          return ss::make_exception_future(std::runtime_error(fmt::format(
            "unexpected struct value for repeated schema element {}",
            element.name)));
      },
      [&element](auto& v) {
          return ss::make_exception_future(std::runtime_error(fmt::format(
            "unexpected leaf value for repeated schema element {}: {}",
            element.name,
            value(std::move(v)))));
      });
}

ss::future<> for_each_child(
  value val,
  const schema_element& element,
  traversal_levels levels,
  absl::FunctionRef<
    ss::future<>(const schema_element&, traversal_levels, value)> cb) {
    switch (element.repetition_type) {
    case field_repetition_type::required:
        return for_each_required_child(std::move(val), element, levels, cb);
    case field_repetition_type::optional:
        return for_each_optional_child(std::move(val), element, levels, cb);
    case field_repetition_type::repeated:
        return for_each_repeated_child(std::move(val), element, levels, cb);
    }
}

ss::future<> emit_leaf(
  const schema_element& element,
  value val,
  traversal_levels levels,
  absl::FunctionRef<ss::future<>(shredded_value)> cb);

ss::future<> process_repeated_leaf(
  // NOLINTNEXTLINE(*reference*)
  const schema_element& element,
  repeated_value list,
  traversal_levels levels,
  absl::FunctionRef<ss::future<>(shredded_value)> cb) {
    // Empty lists are treated as `null`
    if (list.empty()) {
        co_return co_await emit_leaf(element, null_value(), levels, cb);
    }
    traversal_levels child_levels = levels;
    // We are marking that there is a higher depth here we
    // need to record, but the repetition_level is only set
    // *when* we are repeating (so not for the first element).
    ++child_levels.repetition_depth;
    for (auto& member : list) {
        co_await emit_leaf(
          element, std::move(member.element), child_levels, cb);
        child_levels.repetition_level = child_levels.repetition_depth;
    }
}

ss::future<> emit_leaf(
  const schema_element& element,
  value val,
  traversal_levels levels,
  absl::FunctionRef<ss::future<>(shredded_value)> cb) {
    return ss::visit(
      std::move(val),
      [cb, &element, levels](repeated_value& list) {
          return process_repeated_leaf(element, std::move(list), levels, cb);
      },
      [&element](struct_value&) {
          return ss::make_exception_future(std::runtime_error(fmt::format(
            "unexpected struct value for leaf schema element {}",
            element.name)));
      },
      [cb, &element, levels](null_value& v) {
          return cb({
            .schema_element_position = element.position,
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
          return cb({
            .schema_element_position = element.position,
            .val = value(std::move(v)),
            .rep_level = leaf_levels.repetition_level,
            .def_level = leaf_levels.definition_level,
          });
      });
}

ss::future<> shred_record(
  // NOLINTNEXTLINE(*reference*)
  const schema_element& element,
  value val,
  traversal_levels levels,
  absl::FunctionRef<ss::future<>(shredded_value)> cb) {
    if (element.is_leaf()) {
        co_return co_await emit_leaf(element, std::move(val), levels, cb);
    }
    auto callback =
      [cb](const schema_element& element, traversal_levels levels, value val) {
          return shred_record(element, std::move(val), levels, cb);
      };
    // NOTE: This takes an absl::FunctionRef, so make sure to keep the callback
    // alive on the stack and use a coroutine for this method.
    co_await for_each_child(std::move(val), element, levels, callback);
}

} // namespace

ss::future<> shred_record(
  const schema_element& root,
  struct_value record,
  absl::FunctionRef<ss::future<>(shredded_value)> callback) {
    if (root.repetition_type != field_repetition_type::required) {
        throw std::runtime_error("schema root nodes must be required");
    }
    return shred_record(root, std::move(record), traversal_levels{}, callback);
}

} // namespace serde::parquet
