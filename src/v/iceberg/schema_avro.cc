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
        auto ret = avro::FixedSchema(bytes_for_p, "");
        ret.root()->setLogicalType(
          avro::LogicalType(avro::LogicalType::DECIMAL));
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
        static constexpr auto uuid_size_bytes = 16;
        auto ret = avro::FixedSchema(uuid_size_bytes, "");
        ret.root()->setLogicalType(avro::LogicalType(avro::LogicalType::UUID));
        return ret;
    }
    avro::Schema operator()(const fixed_type& type) {
        auto ret = avro::FixedSchema(static_cast<int>(type.length), "");
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

} // namespace

avro::Schema field_to_avro(const nested_field& field) {
    if (field.required) {
        return base_field_as_avro(field);
    }
    return optional_of(base_field_as_avro(field));
}

avro::Schema
struct_type_to_avro(const struct_type& type, const ss::sstring& name) {
    avro::RecordSchema avro_schema(name);
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

} // namespace iceberg
