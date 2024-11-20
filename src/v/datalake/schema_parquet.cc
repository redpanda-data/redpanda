/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/datatypes.h"
#include "serde/parquet/schema.h"

namespace datalake {
namespace {

auto map_repetition_type(iceberg::field_required required) {
    if (required) {
        return serde::parquet::field_repetition_type::required;
    }
    return serde::parquet::field_repetition_type::optional;
}

struct primitive_type_converting_visitor {
    serde::parquet::schema_element operator()(const iceberg::boolean_type&) {
        return serde::parquet::schema_element{
          .type = serde::parquet::bool_type{},
        };
    }
    serde::parquet::schema_element operator()(const iceberg::int_type&) {
        return serde::parquet::schema_element{
          .type = serde::parquet::i32_type{},
          .logical_type
          = serde::parquet::int_type{.bit_width = 32, .is_signed = true},
        };
    }
    serde::parquet::schema_element operator()(const iceberg::long_type&) {
        return serde::parquet::schema_element{
          .type = serde::parquet::i64_type{},
          .logical_type
          = serde::parquet::int_type{.bit_width = 64, .is_signed = true},
        };
    }
    serde::parquet::schema_element operator()(const iceberg::float_type&) {
        return serde::parquet::schema_element{
          .type = serde::parquet::f32_type{},
        };
    }
    serde::parquet::schema_element operator()(const iceberg::double_type&) {
        return serde::parquet::schema_element{
          .type = serde::parquet::f64_type{},
        };
    }
    serde::parquet::schema_element
    operator()(const iceberg::decimal_type& type) {
        return serde::parquet::schema_element{
            // Redpanda uses int128 to represent decimal
            .type = serde::parquet::byte_array_type{.fixed_length=16},
            .logical_type = serde::parquet::decimal_type{
                .scale = static_cast<int32_t>(type.scale),
                .precision = static_cast<int32_t>(type.precision),
            },
        };
    }
    serde::parquet::schema_element operator()(const iceberg::date_type&) {
        return serde::parquet::schema_element{
          .type = serde::parquet::i32_type{},
          .logical_type = serde::parquet::date_type{},
        };
    }
    serde::parquet::schema_element operator()(const iceberg::time_type&) {
        return serde::parquet::schema_element{
          .type = serde::parquet::i64_type{},
          .logical_type = serde::parquet::
            time_type{.is_adjusted_to_utc = false, .unit = serde::parquet::time_unit::micros},
        };
    }
    serde::parquet::schema_element operator()(const iceberg::timestamp_type&) {
        return serde::parquet::schema_element{
          .type = serde::parquet::i64_type{},
          .logical_type = serde::parquet::
            timestamp_type{.is_adjusted_to_utc = false, .unit = serde::parquet::time_unit::micros},
        };
    }

    serde::parquet::schema_element
    operator()(const iceberg::timestamptz_type&) {
        return serde::parquet::schema_element{
          .type = serde::parquet::i64_type{},
          .logical_type = serde::parquet::
            timestamp_type{.is_adjusted_to_utc = true, .unit = serde::parquet::time_unit::micros},
        };
    }
    serde::parquet::schema_element operator()(const iceberg::string_type&) {
        return serde::parquet::schema_element{
          .type = serde::parquet::byte_array_type{},
          .logical_type = serde::parquet::string_type{},
        };
    }
    serde::parquet::schema_element operator()(const iceberg::uuid_type&) {
        return serde::parquet::schema_element{
          .type = serde::parquet::byte_array_type{.fixed_length = 16},
          .logical_type = serde::parquet::uuid_type{},
        };
    }
    serde::parquet::schema_element operator()(const iceberg::fixed_type& type) {
        return serde::parquet::schema_element{
          .type = serde::parquet::byte_array_type{.fixed_length = type.length},
        };
    }
    serde::parquet::schema_element operator()(const iceberg::binary_type&) {
        return serde::parquet::schema_element{
          .type = serde::parquet::byte_array_type{},
        };
    }
};

serde::parquet::schema_element
struct_to_parquet(const iceberg::struct_type& schema);
serde::parquet::schema_element
field_to_parquet(const iceberg::nested_field& field);

struct type_converting_visitor {
    serde::parquet::schema_element
    operator()(const iceberg::primitive_type& type) {
        auto res = std::visit(primitive_type_converting_visitor{}, type);
        res.repetition_type = map_repetition_type(required);
        return res;
    }

    serde::parquet::schema_element
    operator()(const iceberg::struct_type& type) {
        auto res = struct_to_parquet(type);
        res.repetition_type = map_repetition_type(required);
        return res;
    }

    serde::parquet::schema_element operator()(const iceberg::list_type& type) {
        /**
         * Lists in parquet are encoded as:
         *
         * <list-repetition> group <name> (LIST) {
         *   repeated group list {
         *     <element-repetition> <element-type> element;
         *    }
         * }
         */
        serde::parquet::schema_element res;
        res.logical_type = serde::parquet::list_type{};
        res.repetition_type = map_repetition_type(required);

        serde::parquet::schema_element list_wrapper;
        list_wrapper.repetition_type
          = serde::parquet::field_repetition_type::repeated;
        list_wrapper.path.emplace_back("list");
        auto element = field_to_parquet(*type.element_field);
        element.path.emplace_back("element");
        list_wrapper.children.push_back(std::move(element));
        res.children.push_back(std::move(list_wrapper));
        return res;
    }

    serde::parquet::schema_element operator()(const iceberg::map_type& type) {
        /*
         * Maps in parquet are encoded as:
         * <map - repetition> group<name>(MAP) {
         *     repeated group key_value {
         *         required<key - type> key;
         *         <value - repetition><value - type> value;
         *     }
         * }
         */
        serde::parquet::schema_element res;
        serde::parquet::schema_element map_wrapper;
        map_wrapper.repetition_type
          = serde::parquet::field_repetition_type::repeated;
        map_wrapper.path.emplace_back("key_value");
        map_wrapper.children.reserve(2);
        auto key_element = field_to_parquet(*type.key_field);
        key_element.repetition_type
          = serde::parquet::field_repetition_type::required;
        key_element.path.emplace_back("key");
        map_wrapper.children.push_back(std::move(key_element));
        auto value_element = field_to_parquet(*type.value_field);
        value_element.path.emplace_back("value");
        map_wrapper.children.push_back(std::move(value_element));
        res.children.push_back(std::move(map_wrapper));
        res.repetition_type = map_repetition_type(required);
        return res;
    }
    iceberg::field_required required;
};

serde::parquet::schema_element
field_to_parquet(const iceberg::nested_field& field) {
    // type converting visitor takes care of setting the schema element
    // repetition type
    auto element = std::visit(
      type_converting_visitor{field.required}, field.type);
    element.field_id = field.id;

    return element;
}

serde::parquet::schema_element
struct_to_parquet(const iceberg::struct_type& schema) {
    serde::parquet::schema_element res;

    res.children.reserve(schema.fields.size());
    for (const auto& f : schema.fields) {
        auto field = field_to_parquet(*f);
        field.path.emplace_back(f->name);
        res.children.push_back(std::move(field));
    }
    return res;
}

} // namespace

serde::parquet::schema_element
schema_to_parquet(const iceberg::struct_type& schema) {
    auto root = struct_to_parquet(schema);
    root.repetition_type = serde::parquet::field_repetition_type::required;
    root.path.emplace_back("root");
    return root;
}
} // namespace datalake
