/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/arrow_schema_translator.h"

#include "iceberg/datatypes.h"

#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>

#include <memory>
#include <stdexcept>

namespace datalake {
class schema_converter_interface {
public:
    virtual ~schema_converter_interface() noexcept = default;
    // Return an Arrow field descriptor for this converter.
    virtual std::shared_ptr<arrow::Field>
    field(const std::string& name, iceberg::field_required required) = 0;
};

template<typename ArrowType>
class scalar_schema_converter : public schema_converter_interface {
public:
    explicit scalar_schema_converter(
      const std::shared_ptr<arrow::DataType>& data_type)
      : _arrow_data_type{data_type} {
        if (!_arrow_data_type) {
            throw std::invalid_argument("Invalid arrow data type");
        }
    }

    std::shared_ptr<arrow::Field>
    field(const std::string& name, iceberg::field_required required) override {
        if (!_arrow_data_type) {
            throw std::runtime_error(
              "Calling field() on uninitialized scalar converter");
        }
        return arrow::field(
          name, _arrow_data_type, required == iceberg::field_required::no);
    }

private:
    std::shared_ptr<arrow::DataType> _arrow_data_type;
};

class struct_schema_converter : public schema_converter_interface {
public:
    explicit struct_schema_converter(const iceberg::struct_type& schema);

    std::shared_ptr<arrow::Field>
    field(const std::string& name, iceberg::field_required required) override {
        if (!_arrow_data_type) {
            throw std::runtime_error(
              "Calling field() on uninitialized struct converter");
        }
        return arrow::field(
          name, _arrow_data_type, required == iceberg::field_required::no);
    }

    arrow::FieldVector get_field_vector() { return _fields; }

private:
    arrow::FieldVector _fields;
    std::shared_ptr<arrow::DataType> _arrow_data_type;
};

class list_schema_converter : public schema_converter_interface {
public:
    explicit list_schema_converter(const iceberg::list_type& schema);

    std::shared_ptr<arrow::Field>
    field(const std::string& name, iceberg::field_required required) override {
        if (!_arrow_data_type) {
            throw std::runtime_error(
              "Calling field() on uninitialized list converter");
        }
        return arrow::field(
          name, _arrow_data_type, required == iceberg::field_required::no);
    }

private:
    std::shared_ptr<arrow::DataType> _arrow_data_type;
};

class map_schema_converter : public schema_converter_interface {
public:
    explicit map_schema_converter(const iceberg::map_type& schema);

    std::shared_ptr<arrow::Field>
    field(const std::string& name, iceberg::field_required required) override {
        if (!_arrow_data_type) {
            throw std::runtime_error(
              "Calling field() on uninitialized map converter");
        }
        return arrow::field(
          name, _arrow_data_type, required == iceberg::field_required::no);
    }

private:
    std::shared_ptr<arrow::DataType> _arrow_data_type;
};

struct schema_converter_builder_visitor {
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::struct_type& s) {
        return std::make_unique<struct_schema_converter>(s);
    }

    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::list_type& s) {
        return std::make_unique<list_schema_converter>(s);
    }

    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::map_type& m) {
        return std::make_unique<map_schema_converter>(m);
    }

    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::primitive_type& p) {
        return std::visit(*this, p);
    }

    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::boolean_type& /*t*/) {
        return std::make_unique<scalar_schema_converter<arrow::BooleanType>>(
          arrow::boolean());
    }
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::int_type& /*t*/) {
        return std::make_unique<scalar_schema_converter<arrow::Int32Type>>(
          arrow::int32());
    }
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::long_type& /*t*/) {
        return std::make_unique<scalar_schema_converter<arrow::Int64Type>>(
          arrow::int64());
    }
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::float_type& /*t*/) {
        return std::make_unique<scalar_schema_converter<arrow::FloatType>>(
          arrow::float32());
    }
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::double_type& /*t*/) {
        return std::make_unique<scalar_schema_converter<arrow::DoubleType>>(
          arrow::float64());
    }
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::decimal_type& t) {
        if (t.precision == 0) {
            return nullptr;
        }
        return std::make_unique<scalar_schema_converter<arrow::Decimal128Type>>(
          arrow::decimal(t.precision, t.scale));
    }
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::date_type& /*t*/) {
        return std::make_unique<scalar_schema_converter<arrow::Date32Type>>(
          arrow::date32());
    }
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::time_type& /*t*/) {
        return std::make_unique<scalar_schema_converter<arrow::Time64Type>>(
          arrow::time64(arrow::TimeUnit::MICRO));
    }
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::timestamp_type& /*t*/) {
        return std::make_unique<scalar_schema_converter<arrow::TimestampType>>(
          arrow::timestamp(arrow::TimeUnit::MICRO));
    }
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::timestamptz_type& /*t*/) {
        // Iceberg assumes timestampttz are in UTC.
        return std::make_unique<scalar_schema_converter<arrow::TimestampType>>(
          arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"));
    }
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::string_type& /*t*/) {
        return std::make_unique<scalar_schema_converter<arrow::StringType>>(
          arrow::utf8());
    }
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::uuid_type& /*t*/) {
        // TODO: there's an arrow extension for a uuid logical type.
        // Under-the-hood it's stored as a fixed(16). Do we want to use the
        // logical type here?
        return std::make_unique<
          scalar_schema_converter<arrow::FixedSizeBinaryType>>(
          arrow::fixed_size_binary(16));
    }
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::fixed_type& t) {
        return std::make_unique<
          scalar_schema_converter<arrow::FixedSizeBinaryType>>(
          arrow::fixed_size_binary(static_cast<uint32_t>(t.length)));
    }
    std::unique_ptr<schema_converter_interface>
    operator()(const iceberg::binary_type& /*t*/) {
        return std::make_unique<scalar_schema_converter<arrow::BinaryType>>(
          arrow::binary());
    }
};

struct_schema_converter::struct_schema_converter(
  const iceberg::struct_type& schema) {
    _fields.reserve(schema.fields.size());
    for (const auto& field : schema.fields) {
        std::unique_ptr<schema_converter_interface> child = std::visit(
          schema_converter_builder_visitor{}, field->type);
        if (!child) {
            // Don't have a valid data type.
            _arrow_data_type = nullptr;
            // We shouldn't get here. The child should throw if it can't be
            // constructed correctly.
            throw std::runtime_error("Unable to construct child converter");
        }
        _fields.push_back(child->field(field->name, field->required));
    }
    _arrow_data_type = arrow::struct_(_fields);
}

list_schema_converter::list_schema_converter(const iceberg::list_type& schema) {
    std::unique_ptr<schema_converter_interface> child = std::visit(
      schema_converter_builder_visitor{}, schema.element_field->type);
    if (!child) {
        // Don't have a valid data type.
        _arrow_data_type = nullptr;
        // We shouldn't get here. The child should throw if it can't be
        // constructed correctly.
        throw std::runtime_error("Unable to construct child converter");
    }
    std::shared_ptr<arrow::Field> child_field = child->field(
      schema.element_field->name, schema.element_field->required);
    if (child_field) {
        _arrow_data_type = arrow::list(child_field);
    } else {
        throw std::runtime_error("Child converter has null field");
    }
}

map_schema_converter::map_schema_converter(const iceberg::map_type& schema) {
    std::unique_ptr<schema_converter_interface> key = std::visit(
      schema_converter_builder_visitor{}, schema.key_field->type);
    std::unique_ptr<schema_converter_interface> value = std::visit(
      schema_converter_builder_visitor{}, schema.value_field->type);
    if (!key || !value) {
        _arrow_data_type = nullptr;
        // We shouldn't get here. The child should throw if it can't be
        // constructed correctly.
        throw std::runtime_error("Unable to construct child converter");
    }

    std::shared_ptr<arrow::Field> key_field = key->field(
      schema.key_field->name, schema.key_field->required);
    std::shared_ptr<arrow::Field> value_field = value->field(
      schema.value_field->name, schema.value_field->required);
    if (!key_field || !value_field) {
        _arrow_data_type = nullptr;
        throw std::runtime_error("Child converter has null field");
    }

    _arrow_data_type = arrow::map(key_field->type(), value_field->type());
}

arrow_schema_translator::arrow_schema_translator(iceberg::struct_type&& schema)
  : _schema(std::move(schema))
  , _struct_converter(std::make_unique<struct_schema_converter>(_schema)) {}

arrow_schema_translator::~arrow_schema_translator() = default;

std::shared_ptr<arrow::Schema> arrow_schema_translator::build_arrow_schema() {
    return arrow::schema(_struct_converter->get_field_vector());
}
} // namespace datalake
