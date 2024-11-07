/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/schema_avro.h"

#include <seastar/util/defer.hh>

#include <avro/Schema.hh>
namespace datalake {

namespace {
static constexpr size_t max_schema_depth = 100;
/**
 * When converting schema from Avro to Iceberg, we do not need to keep track of
 * fields id. A placeholder id is used instead. When the schema is used in a
 * catalog either to create a table or update the table schema field ids will be
 * assigned.
 */
static constexpr iceberg::nested_field::id_t placeholder_field_id{0};
struct state {
    chunked_vector<avro::NodePtr> type_hierarchy;
};

conversion_outcome<std::optional<iceberg::field_type>>
field_type_from_avro(const avro::NodePtr& node, state& state);

conversion_outcome<iceberg::struct_type>
struct_from_avro_record(const avro::NodePtr& node, state& state) {
    iceberg::struct_type ret;
    ret.fields.reserve(node->leaves());
    for (size_t i = 0; i < node->leaves(); ++i) {
        auto leaf = node->leafAt(i);

        auto result = field_type_from_avro(leaf, state);
        if (result.has_error()) {
            return result.error();
        }

        auto field = std::move(result.value());
        if (field.has_value()) {
            ret.fields.push_back(iceberg::nested_field::create(
              placeholder_field_id,
              node->nameAt(i),
              iceberg::field_required::yes,
              std::move(field.value())));
        }
    }
    return ret;
}

iceberg::decimal_type create_decimal(const avro::NodePtr& node) {
    return iceberg::decimal_type{
      .precision = static_cast<uint32_t>(node->logicalType().precision()),
      .scale = static_cast<uint32_t>(node->logicalType().scale())};
}

bool is_recursive_type(const state& state, const avro::NodePtr& node) {
    for (auto& n : state.type_hierarchy) {
        if (node == n) {
            return true;
        }
    }
    return false;
}

conversion_outcome<std::optional<iceberg::field_type>>
inner_field_type_from_avro(const avro::NodePtr& node, state& state) {
    if (is_recursive_type(state, node)) {
        return schema_conversion_exception(
          fmt::format("Unsupported recursive type: {}", *node));
    }
    if (state.type_hierarchy.size() >= max_schema_depth) {
        return schema_conversion_exception(
          fmt::format("Max nesting depth of {} reached", max_schema_depth));
    }

    state.type_hierarchy.push_back(node);
    auto deferred_pop = ss::defer(
      [&state] { state.type_hierarchy.pop_back(); });

    switch (node->type()) {
    case avro::AVRO_STRING:
        if (node->logicalType().type() == avro::LogicalType::UUID) {
            return iceberg::uuid_type{};
        }

        return iceberg::string_type{};
    case avro::AVRO_BYTES:
        if (node->logicalType().type() == avro::LogicalType::DECIMAL) {
            return create_decimal(node);
        }
        return iceberg::binary_type{};
    case avro::AVRO_INT: {
        auto lt = node->logicalType().type();
        if (lt == avro::LogicalType::DATE) {
            return iceberg::date_type{};
        }
        if (lt == avro::LogicalType::TIME_MILLIS) {
            return iceberg::time_type{};
        }
        return iceberg::int_type{};
    }
    case avro::AVRO_LONG: {
        auto lt = node->logicalType().type();
        if (lt == avro::LogicalType::TIME_MICROS) {
            return iceberg::time_type{};
        }
        if (
          lt == avro::LogicalType::TIMESTAMP_MILLIS
          || lt == avro::LogicalType::TIMESTAMP_MICROS) {
            return iceberg::timestamp_type{};
        }
        return iceberg::long_type{};
    }
    case avro::AVRO_FLOAT:
        return iceberg::float_type{};
    case avro::AVRO_DOUBLE:
        return iceberg::double_type{};
    case avro::AVRO_BOOL:
        return iceberg::boolean_type{};
    case avro::AVRO_NULL:
        return std::nullopt;
    case avro::AVRO_RECORD: {
        auto struct_result = struct_from_avro_record(node, state);
        if (struct_result.has_error()) {
            return struct_result.error();
        }
        return std::make_optional<iceberg::field_type>(
          std::move(struct_result.value()));
    }
    case avro::AVRO_ENUM:
        return iceberg::long_type{};
    case avro::AVRO_ARRAY: {
        if (node->leaves() != 1) {
            return schema_conversion_exception(fmt::format(
              "Invalid number of leaves {} in AVRO_ARRAY, expected to have "
              "only 1 leaf",
              node->leaves()));
        }
        auto element_type = node->leafAt(0);
        auto field_res = field_type_from_avro(element_type, state);
        if (field_res.has_error()) {
            return field_res.error();
        }
        if (!field_res.value().has_value()) {
            return schema_conversion_exception(
              fmt::format("Unsupported type of AVRO_NULL as an array element"));
        }
        return iceberg::list_type::create(
          placeholder_field_id,
          iceberg::field_required::yes,
          std::move(*field_res.value()));
    }
    case avro::AVRO_MAP: {
        if (node->leaves() != 2) {
            return schema_conversion_exception(fmt::format(
              "Invalid number of leaves {} in AVRO_MAP, expected to have "
              "exactly 2 leaves",
              node->leaves()));
        }

        if (node->leafAt(0)->type() != avro::AVRO_STRING) {
            return schema_conversion_exception(fmt::format(
              "AVRO_MAP is expected to be a string. Current key type: {}",
              node->leafAt(0)->type()));
        }
        auto value_t_result = field_type_from_avro(node->leafAt(1), state);
        if (value_t_result.has_error()) {
            return value_t_result.error();
        }
        if (!value_t_result.value().has_value()) {
            return schema_conversion_exception(
              fmt::format("Unsupported type of AVRO_NULL as a map value"));
        }

        return iceberg::map_type::create(
          placeholder_field_id,
          iceberg::string_type{},
          placeholder_field_id,
          iceberg::field_required::yes,
          std::move(*value_t_result.value()));
    }
    case avro::AVRO_UNION: {
        // Avro union is flattened as a struct with fields that are not
        // required, only one of the fields will be present at a time.
        iceberg::struct_type ret;
        ret.fields.reserve(node->leaves());
        for (size_t i = 0; i < node->leaves(); ++i) {
            auto leaf = node->leafAt(i);
            auto result = field_type_from_avro(leaf, state);
            if (result.has_error()) {
                return result.error();
            }
            auto field = std::move(result.value());
            if (field.has_value()) {
                ret.fields.push_back(iceberg::nested_field::create(
                  placeholder_field_id,
                  // union struct fields name are represented as
                  // <union_name>_<leaf_idx>
                  fmt::format("union_opt_{}", i),
                  // union struct fields are not required
                  iceberg::field_required::no,
                  std::move(field.value())));
            }
        }
        return ret;
    }

    case avro::AVRO_FIXED:
        if (node->logicalType().type() == avro::LogicalType::DECIMAL) {
            return create_decimal(node);
        }
        return iceberg::fixed_type{node->fixedSize()};
    case avro::AVRO_SYMBOLIC:
        /**
         * Avro schema symbolic type is indicating that the current node is of
         * type already present in a schema f.e.:
         * {
         *     "name": "nestedrecord",
         *     "type": {
         *         "type": "record",
         *         "name": "Nested",
         *         "fields": [
         *             {
         *                 "name": "inval1",
         *                 "type": "double"
         *             }
         *         ]
         *     }
         * },
         * {
         *     "name": "mymap",
         *     "type": {
         *         "type": "map",
         *         "values": "Nested"
         *     }
         * }
         *
         * The `mymap` value type will be a symbolic type pointing to Nested.
         *
         * For iceberg schema we flatten the representation and the same
         * stucture will be repeated every time it occur in schema DAG.
         */
        return field_type_from_avro(::avro::resolveSymbol(node), state);
    case avro::AVRO_UNKNOWN:
        return schema_conversion_exception(
          "Conversion of unknown avro type is not supported");
    }
}

conversion_outcome<std::optional<iceberg::field_type>>
field_type_from_avro(const avro::NodePtr& node, state& state) {
    try {
        return inner_field_type_from_avro(node, state);
    } catch (...) {
        return schema_conversion_exception(fmt::format(
          "exception thrown while converting AVRO {} schema of type {} to "
          "iceberg - {}",
          *node,
          node->type(),
          std::current_exception()));
    }
}

} // namespace

conversion_outcome<iceberg::struct_type>
type_to_iceberg(const avro::NodePtr& node) {
    state s;

    // schema root is not a record
    if (node->type() != avro::AVRO_RECORD) {
        iceberg::struct_type ret;
        auto result = field_type_from_avro(node, s);
        if (result.has_error()) {
            return result.error();
        }
        auto field = std::move(result.value());
        if (!field.has_value()) {
            return schema_conversion_exception(
              "AVRO_NULL is not supported top level type");
        }

        ret.fields.push_back(iceberg::nested_field::create(
          placeholder_field_id,
          node->hasName() ? node->name().fullname() : "root",
          iceberg::field_required::yes,
          std::move(field.value())));
        return ret;
    }

    // schema root is a record
    return struct_from_avro_record(node, s);
}

} // namespace datalake
