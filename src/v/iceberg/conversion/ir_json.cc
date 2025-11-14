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

#include "iceberg/conversion/conversion_outcome.h"
#include "iceberg/conversion/json_schema/ir.h"
#include "iceberg/datatypes.h"

#include <seastar/util/variant_utils.hh>

#include <fmt/ranges.h>

#include <optional>
#include <variant>

namespace {
static constexpr iceberg::nested_field::id_t placeholder_field_id{0};
}

namespace iceberg {

namespace {

std::optional<iceberg::field_type>
convert_type(const conversion::json_schema::json_value_type& t) {
    switch (t) {
    case conversion::json_schema::json_value_type::null:
        return std::nullopt;
    case conversion::json_schema::json_value_type::boolean:
        return iceberg::boolean_type{};
    case conversion::json_schema::json_value_type::object:
        return iceberg::struct_type{};
    case conversion::json_schema::json_value_type::array:
        return iceberg::list_type{};
    case conversion::json_schema::json_value_type::number:
        return iceberg::double_type{};
    case conversion::json_schema::json_value_type::integer:
        return iceberg::long_type{};
    case conversion::json_schema::json_value_type::string:
        return iceberg::string_type{};
    }

    vunreachable("Unexpected JSON conversion type {}", t);
}

conversion_outcome<std::optional<iceberg::field_type>>
validation_types_to_field_type(
  const std::vector<conversion::json_schema::json_value_type>& types) {
    if (types.empty()) {
        return std::nullopt;
    } else if (types.size() == 1) {
        return convert_type(types[0]);
    } else if (
      types.size() == 2
      && types[0] == conversion::json_schema::json_value_type::null) {
        return convert_type(types[1]);
    } else if (
      types.size() == 2
      && types[1] == conversion::json_schema::json_value_type::null) {
        return convert_type(types[0]);
    } else {
        return conversion_exception(
          fmt::format(
            "Type constraint is not sufficient for transforming. Types: [{}]",
            fmt::join(types, ", ")));
    }
}

struct conversion_context {
    conversion::json_schema::dialect dialect{};

    json_conversion_ir::struct_field_map_t field_index;

    // Pointer to the current struct field index. We use it when walking nested
    // structs.
    json_conversion_ir::struct_field_map_t* current{&field_index};
};

conversion_outcome<iceberg::field_type>
convert(conversion_context& ctx, const conversion::json_schema::subschema& s) {
    if (ctx.dialect != conversion::json_schema::dialect::draft7) {
        return conversion_exception(
          fmt::format("Unsupported JSON schema dialect: {}", ctx.dialect));
    }

    std::optional<iceberg::field_type> t;

    {
        auto res = validation_types_to_field_type(s.types());

        if (res.has_error()) {
            return res.error();
        } else if (res.value().has_value()) {
            t = std::move(res.value().value());
        }
    }

    if (!t.has_value()) {
        return conversion_exception(
          fmt::format("Unsupported JSON conversion: missing type keyword"));
    }

    if (t.value() == iceberg::string_type{} && s.format().has_value()) {
        // If the type is string, we can have a format.
        switch (s.format().value()) {
        case conversion::json_schema::format::date_time:
            return iceberg::timestamptz_type{};
        case conversion::json_schema::format::date:
            return iceberg::date_type{};
        case conversion::json_schema::format::time:
            return iceberg::time_type{};
        }

        return std::move(t.value());
    }

    return ss::visit(
      t.value(),
      [](const iceberg::primitive_type& t)
        -> conversion_outcome<iceberg::field_type> { return t; },
      [&ctx, &s](
        iceberg::struct_type& st) -> conversion_outcome<iceberg::field_type> {
          auto sorted_prop_keys = std::views::keys(s.properties())
                                  | std::ranges::to<std::vector<std::string>>();
          std::ranges::sort(sorted_prop_keys);
          for (const auto& name : sorted_prop_keys) {
              auto field_index = json_conversion_ir::struct_field_map_t{};

              // Depth first.
              auto tmp = ctx.current;
              ctx.current = &field_index;
              auto child_res = convert(ctx, s.properties().at(name));
              if (child_res.has_error()) {
                  return child_res.error();
              }

              auto field_position = st.fields.size();

              // After we converted the children, restore the context and
              // update it.
              ctx.current = tmp;
              if (!ctx.current
                     ->emplace(
                       name,
                       json_conversion_ir::field_annotation{
                         field_position, std::move(field_index)})
                     .second) {
                  return conversion_exception(
                    fmt::format(
                      "Duplicate field name in JSON schema: {}", name));
              }

              st.fields.push_back(
                iceberg::nested_field::create(
                  placeholder_field_id,
                  name,
                  iceberg::field_required::no,
                  std::move(child_res.value())));
          }

          if (
            s.additional_properties()
            && s.additional_properties()->get().boolean_subschema() != false) {
              return conversion_exception(
                "Only 'false' subschema is supported "
                "for additionalProperties keyword");
          }

          return std::move(st);
      },
      [&ctx, &s](
        const iceberg::list_type&) -> conversion_outcome<iceberg::field_type> {
          using ret_t = conversion_outcome<iceberg::field_type>;

          return ss::visit(
            s.items(),
            [](const std::monostate&) -> ret_t {
                return conversion_exception(
                  "Cannot convert JSON schema list type without items");
            },
            [&](
              const std::reference_wrapper<
                const conversion::json_schema::subschema>& item) -> ret_t {
                auto item_res = convert(ctx, item.get());
                if (item_res.has_error()) {
                    return item_res.error();
                }

                return iceberg::list_type::create(
                  placeholder_field_id,
                  iceberg::field_required::yes,
                  std::move(item_res.value()));
            },
            [&](const iceberg::conversion::json_schema::const_list_view& items)
              -> ret_t {
                std::optional<iceberg::field_type> resolved_type;

                for (const auto& item : items) {
                    auto item_res = convert(ctx, item);
                    if (item_res.has_error()) {
                        return item_res.error();
                    }

                    if (!resolved_type.has_value()) {
                        resolved_type = std::move(item_res.value());
                    } else if (resolved_type.value() != item_res.value()) {
                        return conversion_exception(
                          fmt::format(
                            "List type items must have the same type, but "
                            "found "
                            "{} and {}",
                            resolved_type.value(),
                            item_res.value()));
                    }
                }

                if (s.additional_items()) {
                    auto additional_item_res = convert(
                      ctx, s.additional_items().value().get());
                    if (additional_item_res.has_error()) {
                        return additional_item_res.error();
                    }
                    if (!resolved_type.has_value()) {
                        resolved_type = std::move(additional_item_res.value());
                    } else if (
                      resolved_type.value() != additional_item_res.value()) {
                        return conversion_exception(
                          fmt::format(
                            "List type items must have the same type, but "
                            "found "
                            "{} "
                            "and {}",
                            resolved_type.value(),
                            additional_item_res.value()));
                    }
                }

                if (!resolved_type.has_value()) {
                    return conversion_exception(
                      "List type items must have the type defined in JSON "
                      "schema");
                }

                return iceberg::list_type::create(
                  placeholder_field_id,
                  iceberg::field_required::yes,
                  std::move(resolved_type.value()));
            });
      },
      [](iceberg::map_type&) -> conversion_outcome<iceberg::field_type> {
          return conversion_exception(
            "Map type is not expected in JSON schema conversion");
      });
}

} // namespace

conversion_outcome<json_conversion_ir>
type_to_ir(const conversion::json_schema::schema& schema) {
    conversion_context ctx;

    ctx.dialect = schema.root().dialect();

    auto r = convert(ctx, schema.root());
    if (r.has_error()) {
        return r.error();
    }

    return json_conversion_ir(
      std::make_unique<iceberg::field_type>(std::move(r.value())),
      ctx.field_index);
}

} // namespace iceberg
