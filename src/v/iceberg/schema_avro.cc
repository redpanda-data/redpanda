// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/schema_avro.h"

#include "iceberg/datatypes.h"

#include <avro/CustomAttributes.hh>
#include <avro/GenericDatum.hh>
#include <avro/LogicalType.hh>
#include <avro/Schema.hh>
#include <fmt/format.h>

#include <variant>

namespace iceberg {

namespace {

avro::CustomAttributes field_attrs(int field_id) {
    avro::CustomAttributes attrs;
    attrs.addAttribute("field-id", fmt::to_string(field_id));
    return attrs;
}

avro::Schema optional_of(const avro::Schema& type) {
    avro::UnionSchema s;
    s.addType(avro::NullSchema());
    s.addType(type);
    return s;
}

avro::Schema map_of(const avro::Schema& kv_type) {
    // Map types are represented as an array of key-value pairs with a logical
    // type of "map".
    avro::ArraySchema schema(kv_type);
    schema.root()->setLogicalType(avro::LogicalType(avro::LogicalType::MAP));
    return schema;
}

avro::Schema array_of(const avro::Schema& type, int element_id) {
    avro::ArraySchema schema(type, element_id);
    return schema;
}

avro::Schema kv_schema(
  const std::string& name,
  int key_id,
  const avro::Schema& key_schema,
  int val_id,
  const avro::Schema& val_schema) {
    avro::RecordSchema schema(name);
    schema.addField("key", key_schema, field_attrs(key_id));
    schema.addField("value", val_schema, field_attrs(val_id));
    return schema;
}

struct avro_primitive_type_visitor {
    avro::Schema operator()(const boolean_type&) { return avro::BoolSchema(); }
    avro::Schema operator()(const int_type&) { return avro::IntSchema(); }
    avro::Schema operator()(const long_type&) { return avro::LongSchema(); }
    avro::Schema operator()(const float_type&) { return avro::FloatSchema(); }
    avro::Schema operator()(const double_type&) { return avro::DoubleSchema(); }
    avro::Schema operator()(const decimal_type& type) {
        auto bytes_for_p = static_cast<int>(
          std::ceil((type.precision * std::log2(10) + 1) / 8));
        auto ret = avro::FixedSchema(bytes_for_p, "decimal");
        avro::LogicalType l_type(avro::LogicalType::DECIMAL);
        l_type.setPrecision(type.precision);
        l_type.setScale(type.scale);
        ret.root()->setLogicalType(l_type);

        return ret;
    }
    avro::Schema operator()(const date_type&) {
        auto ret = avro::LongSchema();
        ret.root()->setLogicalType(avro::LogicalType(avro::LogicalType::DATE));
        return ret;
    }
    avro::Schema operator()(const time_type&) {
        auto ret = avro::LongSchema();
        ret.root()->setLogicalType(
          avro::LogicalType(avro::LogicalType::TIME_MICROS));
        return ret;
    }
    avro::Schema operator()(const timestamp_type&) {
        auto ret = avro::LongSchema();
        ret.root()->setLogicalType(
          avro::LogicalType(avro::LogicalType::TIMESTAMP_MICROS));
        return ret;
    }
    avro::Schema operator()(const timestamptz_type&) {
        auto ret = avro::LongSchema();
        ret.root()->setLogicalType(
          avro::LogicalType(avro::LogicalType::TIMESTAMP_MICROS));
        return ret;
    }
    avro::Schema operator()(const string_type&) { return avro::StringSchema(); }
    avro::Schema operator()(const uuid_type&) {
        // Out of the two string and fixed, we choose to encode UUIDs as string
        // values as this is the only way it is supported in Avro C++ library
        auto ret = avro::StringSchema();
        ret.root()->setLogicalType(avro::LogicalType(avro::LogicalType::UUID));
        return ret;
    }
    avro::Schema operator()(const fixed_type& type) {
        auto ret = avro::FixedSchema(
          static_cast<int>(type.length), "fixed_bytes");
        return ret;
    }
    avro::Schema operator()(const binary_type&) { return avro::BytesSchema(); }
};

struct avro_field_visitor {
    explicit avro_field_visitor(const nested_field::id_t& id)
      : id_(id) {}
    const nested_field::id_t& id_;

    avro::Schema operator()(const primitive_type& type) {
        return std::visit(avro_primitive_type_visitor{}, type);
    }
    avro::Schema operator()(const struct_type& type) {
        return struct_type_to_avro(type, fmt::format("r{}", id_));
    }
    avro::Schema operator()(const list_type& type) {
        return array_of(
          field_to_avro(*type.element_field), type.element_field->id);
    }
    avro::Schema operator()(const map_type& type) {
        auto kv = kv_schema(
          fmt::format("k{}_v{}", type.key_field->id(), type.value_field->id()),
          type.key_field->id(),
          field_to_avro(*type.key_field),
          type.value_field->id(),
          field_to_avro(*type.value_field));
        return map_of(kv);
    }
};

// Returns an avro::Schema for the given field, ignoring top-level optionality
// of field. To consider optionality of the top-level field, use
// field_as_avro().
avro::Schema base_field_as_avro(const nested_field& field) {
    return std::visit(avro_field_visitor{field.id}, field.type);
}

struct type_and_required {
    field_type type;
    field_required required;
};

// Returns the parsed Iceberg type of the given node, handling unions as
// optional values.
type_and_required maybe_optional_from_avro(const avro::NodePtr& n) {
    const auto& type = n->type();
    if (type != avro::AVRO_UNION) {
        return {type_from_avro(n), field_required::yes};
    }
    if (n->leaves() != 2) {
        throw std::invalid_argument(
          fmt::format("Expected 2 leaves in union: {}", n->leaves()));
    }
    size_t num_nulls = 0;
    size_t child_idx = 0;
    for (size_t i = 0; i < 2; i++) {
        if (n->leafAt(i)->type() == avro::AVRO_NULL) {
            ++num_nulls;
        } else {
            child_idx = i;
        }
    }
    if (num_nulls != 1) {
        throw std::invalid_argument(
          fmt::format("Expected 1 null in union: {}", num_nulls));
    }
    const auto& child = n->leafAt(child_idx);
    field_type parsed_type = type_from_avro(child);
    return {type_from_avro(child), field_required::no};
}

} // namespace

avro::Schema field_to_avro(const nested_field& field) {
    if (field.required) {
        return base_field_as_avro(field);
    }
    return optional_of(base_field_as_avro(field));
}

avro::Schema
struct_type_to_avro(const struct_type& type, std::string_view name) {
    avro::RecordSchema avro_schema(std::string{name});
    const auto& fields = type.fields;
    for (const auto& field_ptr : fields) {
        const auto child_schema = field_to_avro(*field_ptr);
        avro_schema.addField(
          field_ptr->name,
          field_to_avro(*field_ptr),
          field_attrs(field_ptr->id()),
          // Optional fields (unions types with null children) have
          // defaults of GenericUnion(null), rather than no default.
          // NOTE: no other union types are allowed by Iceberg.
          field_ptr->required
            ? avro::GenericDatum()
            : avro::GenericDatum(
                child_schema.root(), avro::GenericUnion(child_schema.root())));
    }
    return avro_schema;
}

nested_field_ptr
child_field_from_avro(const avro::NodePtr& parent, size_t child_idx) {
    const auto& type = parent->type();
    if (type != avro::AVRO_RECORD) {
        throw std::invalid_argument("Expected Avro record type");
    }
    if (child_idx >= parent->leaves()) {
        throw std::invalid_argument(
          fmt::format("Cannot get {}, {} leaves", child_idx, parent->leaves()));
    }
    const auto& child = parent->leafAt(child_idx);
    const auto& name = parent->nameAt(child_idx);
    const auto& attrs = parent->customAttributesAt(child_idx);
    auto field_id_str = attrs.getAttribute("field-id");
    if (!field_id_str.has_value()) {
        throw std::invalid_argument("Missing field-id attribute");
    }
    auto field_id = std::stoi(field_id_str.value());
    type_and_required parsed_child = maybe_optional_from_avro(child);
    return nested_field::create(
      field_id, name, parsed_child.required, std::move(parsed_child.type));
}

field_type type_from_avro(const avro::NodePtr& n) {
    const auto& type = n->type();
    switch (type) {
    case avro::AVRO_STRING:
        if (n->logicalType().type() == avro::LogicalType::UUID) {
            return uuid_type{};
        }
        return string_type{};
    case avro::AVRO_BYTES:
        return binary_type{};
    case avro::AVRO_INT: {
        if (n->logicalType().type() == avro::LogicalType::DATE) {
            return date_type{};
        }
        return int_type{};
    }
    case avro::AVRO_LONG:
        if (n->logicalType().type() == avro::LogicalType::TIME_MICROS) {
            return time_type{};
        }
        if (n->logicalType().type() == avro::LogicalType::TIMESTAMP_MICROS) {
            // XXX: handle 'adjust-to-utc'.
            return timestamp_type{};
        }
        return long_type{};
    case avro::AVRO_FLOAT:
        return float_type{};
    case avro::AVRO_DOUBLE:
        return double_type{};
    case avro::AVRO_BOOL:
        return boolean_type{};
    case avro::AVRO_NULL:
        // NOTE: should be handled by maybe_optional_from_avro().
        throw std::invalid_argument(
          "Avro null type expected to be a part of an Avro union");
    case avro::AVRO_RECORD: {
        struct_type ret;
        const auto num_leaves = n->leaves();
        for (size_t i = 0; i < num_leaves; i++) {
            ret.fields.emplace_back(child_field_from_avro(n, i));
        }
        return ret;
    }
    case avro::AVRO_ENUM:
        throw std::invalid_argument("Avro enum type not supported");
    case avro::AVRO_ARRAY: {
        if (n->leaves() != 1) {
            throw std::invalid_argument(fmt::format(
              "Avro list type must have 1 child type", n->leaves()));
        }
        if (n->logicalType().type() == avro::LogicalType::MAP) {
            const auto& kv_node = n->leafAt(0);
            if (kv_node->type() != avro::AVRO_RECORD) {
                throw std::invalid_argument(
                  "Expected map type to be array of records");
            }
            if (kv_node->leaves() != 2) {
                throw std::invalid_argument(fmt::format(
                  "Expected 2 leaves in key-value record: {}", n->leaves()));
            }
            auto key_field = child_field_from_avro(kv_node, 0);
            auto val_field = child_field_from_avro(kv_node, 1);
            return map_type::create(
              key_field->id,
              std::move(key_field->type),
              val_field->id,
              val_field->required,
              std::move(val_field->type));
        }
        const auto& node = dynamic_cast<const avro::NodeArray&>(*n);
        if (!node.elementId_.has_value()) {
            throw std::invalid_argument("Avro array type missing element id");
        }
        type_and_required parsed_child = maybe_optional_from_avro(n->leafAt(0));
        return list_type::create(
          static_cast<int32_t>(*node.elementId_),
          parsed_child.required,
          std::move(parsed_child.type));
    }
    case avro::AVRO_MAP:
        // NOTE: should be handled as an array.
        throw std::invalid_argument("Avro map type not supported");
    case avro::AVRO_UNION:
        // NOTE: should be handled by maybe_optional_from_avro().
        throw std::invalid_argument("Avro union type not supported");
    case avro::AVRO_FIXED: {
        auto logical_type = n->logicalType();
        if (logical_type.type() == avro::LogicalType::DECIMAL) {
            // casting to uint32_t should be safe, the only reason the values
            // are signed is that unsigned counterparts are missing in java.
            return decimal_type{
              .precision = static_cast<uint32_t>(logical_type.precision()),
              .scale = static_cast<uint32_t>(logical_type.scale()),
            };
        } else if (logical_type.type() == avro::LogicalType::UUID) {
            // UUID can be either a string or a fixed type
            return uuid_type{};
        }
        return fixed_type{n->fixedSize()};
    }
    case avro::AVRO_SYMBOLIC:
        throw std::invalid_argument("Avro symbolic type not supported");
    case avro::AVRO_UNKNOWN:
        throw std::invalid_argument("Avro unknown type");
    }
}

} // namespace iceberg
