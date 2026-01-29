/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/conversion/ir_json.h"

#include "base/format_to.h"
#include "base/vassert.h"
#include "iceberg/conversion/conversion_outcome.h"
#include "iceberg/conversion/json_schema/ir.h"
#include "iceberg/datatypes.h"

#include <seastar/util/defer.hh>
#include <seastar/util/variant_utils.hh>

#include <array>
#include <bitset>
#include <map>
#include <optional>
#include <utility>

namespace iceberg {
namespace {
constexpr iceberg::nested_field::id_t placeholder_field_id{0};

using json_type = conversion::json_schema::json_value_type;
using json_format = conversion::json_schema::format;

/// Set of JSON value types for constraint tracking.
///
/// Provides type-safe operations over a bitset where each bit corresponds
/// to a json_value_type enum value.
class type_set {
public:
    /// Creates an unconstrained type set (all types allowed).
    static type_set all() {
        type_set s;
        s.bits_.set();
        return s;
    }

    /// Creates an empty type set (no types allowed).
    static type_set none() { return type_set{}; }

    void set(json_type t) { bits_.set(index(t)); }
    bool test(json_type t) const { return bits_.test(index(t)); }

    /// Returns the single non-null type if exactly one is set.
    std::optional<json_type> single_non_null_type() const {
        auto copy = bits_;
        copy.reset(index(json_type::null));
        if (copy.count() != 1) {
            return std::nullopt;
        }
        for (auto t : conversion::json_schema::all_json_value_types) {
            if (copy.test(index(t))) {
                return t;
            }
        }
        vunreachable("bitset count is 1 but no type found");
    }

    /// Formats type names (for error messages).
    fmt::iterator format_to(fmt::iterator it) const {
        bool first = true;
        for (auto t : conversion::json_schema::all_json_value_types) {
            if (bits_.test(index(t))) {
                if (!first) {
                    it = fmt::format_to(it, ", ");
                }
                it = fmt::format_to(it, "{}", t);
                first = false;
            }
        }
        return it;
    }

private:
    using bits_t
      = std::bitset<conversion::json_schema::all_json_value_types.size()>;

    type_set() = default;

    static size_t index(json_type t) { return static_cast<size_t>(t); }

    bits_t bits_;
};

/// Constraint collected from a JSON Schema.
///
/// Models type deduction as constraint satisfaction: the types bitfield
/// tracks which JSON types are still possible. Resolution succeeds when
/// exactly one non-null type remains.
struct constraint {
    constraint() = default;
    constraint(constraint&&) = default;
    constraint& operator=(constraint&&) = default;
    constraint(const constraint&) = default;
    constraint& operator=(const constraint&) = default;
    ~constraint() = default;

    // Allowed types. Starts as all-set (unconstrained).
    type_set types = type_set::all();

    // Format annotation for strings (date, time, date-time).
    std::optional<json_format> format = std::nullopt;

    // Object properties, keyed by name.
    std::map<std::string, constraint> properties = {};

    // Array item constraints.
    // - nullopt: no "items" keyword
    // - empty vector: "items": []
    // - non-empty: item schema(s) to validate
    std::optional<std::vector<constraint>> items = std::nullopt;
};

/// Context for the resolution phase.
struct resolution_context {
    json_conversion_ir::struct_field_map_t root_field_map;
    json_conversion_ir::struct_field_map_t* current_field_map{&root_field_map};
};

class collect_context {
public:
    [[nodiscard]] auto recurse_guard() {
        constexpr static size_t max_depth{32};
        if (depth_ >= max_depth) {
            throw std::runtime_error(
              "Schema depth limit exceeded during constraint collection");
        }
        ++depth_;
        return ss::defer([this] noexcept { --depth_; });
    }

private:
    size_t depth_{0};
};

// Forward declarations.
conversion_outcome<constraint>
collect(collect_context& ctx, const conversion::json_schema::subschema&);

conversion_outcome<field_type> resolve(resolution_context&, const constraint&);

/// Collect item constraints from a JSON Schema array.
conversion_outcome<std::optional<std::vector<constraint>>> collect_items(
  collect_context& ctx, const conversion::json_schema::subschema& s) {
    using ret_t = conversion_outcome<std::optional<std::vector<constraint>>>;

    return ss::visit(
      s.items(),
      [](std::monostate) -> ret_t { return std::nullopt; },
      [&ctx](
        std::reference_wrapper<const conversion::json_schema::subschema> item)
        -> ret_t {
          auto c = collect(ctx, item.get());
          if (c.has_error()) {
              return c.error();
          }
          return std::vector<constraint>{std::move(c.value())};
      },
      [&ctx, &s](
        const conversion::json_schema::const_list_view& tuple_items) -> ret_t {
          std::vector<constraint> result;
          result.reserve(tuple_items.size() + (s.additional_items() ? 1 : 0));

          for (const auto& item : tuple_items) {
              auto c = collect(ctx, item);
              if (c.has_error()) {
                  return c.error();
              }
              result.push_back(std::move(c.value()));
          }

          if (s.additional_items()) {
              auto c = collect(ctx, s.additional_items()->get());
              if (c.has_error()) {
                  return c.error();
              }
              result.push_back(std::move(c.value()));
          }

          return result;
      });
}

/// Collect constraints from a JSON Schema subschema.
conversion_outcome<constraint>
collect(collect_context& ctx, const conversion::json_schema::subschema& s) {
    auto recurse_guard = ctx.recurse_guard();

    // Validate dialect for each subschema.
    if (s.base().dialect() != conversion::json_schema::dialect::draft7) {
        return conversion_exception(
          fmt::format(
            "Unsupported JSON schema dialect: {}", s.base().dialect()));
    }

    constraint c;

    if (s.ref() != nullptr) {
        // draft-07: $ref takes precedence over all other keywords
        return collect(ctx, *s.ref());
    }

    // Type keyword.
    const auto& schema_types = s.types();
    if (!schema_types.empty()) {
        c.types = type_set::none();
        for (auto t : schema_types) {
            c.types.set(t);
        }
    }

    // Format annotation.
    c.format = s.format();

    // Object properties (only if object type is possible).
    if (c.types.test(json_type::object)) {
        for (const auto& [name, prop] : s.properties()) {
            auto prop_constraint = collect(ctx, prop);
            if (prop_constraint.has_error()) {
                return prop_constraint.error();
            }
            c.properties[name] = std::move(prop_constraint.value());
        }

        if (
          s.additional_properties()
          && s.additional_properties()->get().boolean_subschema() != false) {
            return conversion_exception(
              "Only 'false' subschema is supported "
              "for additionalProperties keyword");
        }
    }

    // Array items (only if array type is possible).
    if (c.types.test(json_type::array)) {
        auto items_result = collect_items(ctx, s);
        if (items_result.has_error()) {
            return items_result.error();
        }
        c.items = std::move(items_result.value());
    }

    return c;
}

/// Resolve object constraint to Iceberg struct.
conversion_outcome<struct_type>
resolve_object(resolution_context& ctx, const constraint& c) {
    struct_type result;

    // std::map iterates in sorted key order, giving deterministic field
    // ordering.
    for (const auto& [name, prop] : c.properties) {
        // Recurse with a fresh field map for nested structs.
        json_conversion_ir::struct_field_map_t nested_map;

        // Scope defer to cover only the recursive call - restore must happen
        // before we emplace into the parent's field map.
        std::optional<field_type> resolved_type;
        {
            auto* prev_field_map = std::exchange(
              ctx.current_field_map, &nested_map);
            auto restore = ss::defer([&ctx, prev_field_map] noexcept {
                ctx.current_field_map = prev_field_map;
            });

            auto resolved = resolve(ctx, prop);
            if (resolved.has_error()) {
                return resolved.error();
            }
            resolved_type = std::move(resolved.value());
        }

        // Record field position for value deserialization.
        auto pos = result.fields.size();
        auto [_, inserted] = ctx.current_field_map->emplace(
          name,
          json_conversion_ir::field_annotation{
            .field_pos = pos, .nested_fields = std::move(nested_map)});
        if (!inserted) {
            return conversion_exception(
              fmt::format("Duplicate field name in JSON schema: {}", name));
        }

        result.fields.push_back(
          nested_field::create(
            placeholder_field_id,
            name,
            field_required::no,
            std::move(*resolved_type)));
    }

    return result;
}

/// Resolve array constraint to Iceberg list.
conversion_outcome<list_type>
resolve_array(resolution_context& ctx, const constraint& c) {
    if (!c.items.has_value()) {
        return conversion_exception(
          "Cannot convert JSON schema list type without items");
    }

    if (c.items->empty()) {
        return conversion_exception(
          "List type items must have the type defined in JSON schema");
    }

    // Resolve all item schemas and verify they produce the same type.
    std::optional<field_type> element_type;
    for (const auto& item : *c.items) {
        auto resolved = resolve(ctx, item);
        if (resolved.has_error()) {
            return resolved.error();
        }

        if (!element_type) {
            element_type = std::move(resolved.value());
        } else if (*element_type != resolved.value()) {
            return conversion_exception(
              fmt::format(
                "List type items must have the same type, but found {} and {}",
                *element_type,
                resolved.value()));
        }
    }

    return list_type::create(
      placeholder_field_id, field_required::yes, std::move(*element_type));
}

/// Resolve a constraint to an Iceberg field type.
conversion_outcome<field_type>
resolve(resolution_context& ctx, const constraint& c) {
    auto single_type = c.types.single_non_null_type();
    if (!single_type) {
        return conversion_exception(
          fmt::format(
            "Type constraint is not sufficient for transforming. Types: [{}]",
            c.types));
    }

    switch (*single_type) {
    case json_type::boolean:
        return boolean_type{};
    case json_type::integer:
        return long_type{};
    case json_type::number:
        return double_type{};
    case json_type::string:
        if (c.format) {
            switch (*c.format) {
            case json_format::date_time:
                return timestamptz_type{};
            case json_format::date:
                return date_type{};
            case json_format::time:
                return time_type{};
            }
        }
        return string_type{};
    case json_type::object: {
        auto result = resolve_object(ctx, c);
        if (result.has_error()) {
            return result.error();
        }
        return std::move(result.value());
    }
    case json_type::array: {
        auto result = resolve_array(ctx, c);
        if (result.has_error()) {
            return result.error();
        }
        return std::move(result.value());
    }
    case json_type::null:
        vunreachable("null type should not be resolved");
    }
}

} // namespace

conversion_outcome<json_conversion_ir>
type_to_ir(const conversion::json_schema::schema& schema) {
    if (schema.root().dialect() != conversion::json_schema::dialect::draft7) {
        return conversion_exception(
          fmt::format(
            "Unsupported JSON schema dialect: {}", schema.root().dialect()));
    }

    collect_context collect_ctx;

    auto c = collect(collect_ctx, schema.root());
    if (c.has_error()) {
        return c.error();
    }

    resolution_context resolution_ctx;
    auto result = resolve(resolution_ctx, c.value());
    if (result.has_error()) {
        return result.error();
    }

    return json_conversion_ir(
      std::make_unique<field_type>(std::move(result.value())),
      std::move(resolution_ctx.root_field_map));
}

} // namespace iceberg
