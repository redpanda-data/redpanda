/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/arrow_translator.h"

#include "container/fragmented_vector.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"

#include <arrow/api.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>

#include <memory>
#include <stdexcept>

namespace datalake {
class converter_interface {
public:
    virtual ~converter_interface() noexcept = default;
    // Return an Arrow field descriptor for this converter. This is needed both
    // for schema generation and for converters for compound types: list,
    // struct, map.
    virtual std::shared_ptr<arrow::Field>
    field(const std::string& name, iceberg::field_required required) = 0;

    // Return the underlying builder. This is needed to get the child builders
    // for compound types.
    virtual std::shared_ptr<arrow::ArrayBuilder> builder() = 0;

    // The add_data and add_optional_data methods add a data item to the
    // converter. They throw an exception on error.
    // - invalid_argument: a bad data type in input data
    // - runtime_error: an error in underlying Arrow library
    // Data should be validated before calling this method: if an error occurs
    // the converter is in an indeterminate state.

    // There are two methods because in most cases we need to be able to add an
    // optional value, but the key of a map field is a corner case where the
    // value is not optional.
    virtual void
    add_optional_data(const std::optional<iceberg::value>& /*value*/)
      = 0;
    virtual void add_data(const iceberg::value& /*value*/) = 0;
};

template<typename ArrowType>
class scalar_converter : public converter_interface {
    using BuilderType = arrow::TypeTraits<ArrowType>::BuilderType;

public:
    explicit scalar_converter(
      const std::shared_ptr<arrow::DataType>& data_type,
      std::shared_ptr<BuilderType> builder);

    std::shared_ptr<arrow::Field>
    field(const std::string& name, iceberg::field_required required) override;

    std::shared_ptr<arrow::ArrayBuilder> builder() override;

    void
    add_optional_data(const std::optional<iceberg::value>& /*value*/) override;
    void add_data(const iceberg::value& /*value*/) override;
    void add_primitive(const iceberg::primitive_value& prim_value);

private:
    std::shared_ptr<arrow::DataType> _arrow_data_type;
    std::shared_ptr<BuilderType> _builder;
};

class struct_converter : public converter_interface {
public:
    explicit struct_converter(const iceberg::struct_type& schema);

    std::shared_ptr<arrow::Field>
    field(const std::string& name, iceberg::field_required required) override;

    std::shared_ptr<arrow::ArrayBuilder> builder() override;

    void add_optional_data(
      const std::optional<iceberg::value>& maybe_value) override;

    void add_data(const iceberg::value& value) override;

    void add_struct_data(const iceberg::struct_value value);

    arrow::FieldVector get_field_vector();

    std::shared_ptr<arrow::Array> take_chunk();

private:
    fragmented_vector<bool> _field_required;
    arrow::FieldVector _fields;
    std::shared_ptr<arrow::DataType> _arrow_data_type;

    fragmented_vector<std::unique_ptr<converter_interface>> _child_converters;
    std::shared_ptr<arrow::StructBuilder> _builder;
};

class list_converter : public converter_interface {
public:
    explicit list_converter(const iceberg::list_type& schema);

    std::shared_ptr<arrow::Field>
    field(const std::string& name, iceberg::field_required required) override;

    std::shared_ptr<arrow::ArrayBuilder> builder() override;

    void add_optional_data(
      const std::optional<iceberg::value>& maybe_value) override;

    void add_data(const iceberg::value& value) override;

private:
    std::shared_ptr<arrow::DataType> _arrow_data_type;

    std::unique_ptr<converter_interface> _child_converter;
    std::shared_ptr<arrow::ListBuilder> _builder;
};

class map_converter : public converter_interface {
public:
    explicit map_converter(const iceberg::map_type& schema);

    std::shared_ptr<arrow::Field>
    field(const std::string& name, iceberg::field_required required) override;

    std::shared_ptr<arrow::ArrayBuilder> builder() override;

    void add_optional_data(
      const std::optional<iceberg::value>& maybe_value) override;

    void add_data(const iceberg::value& value) override;

private:
    std::shared_ptr<arrow::DataType> _arrow_data_type;

    std::unique_ptr<converter_interface> _key_converter;
    std::unique_ptr<converter_interface> _value_converter;
    std::shared_ptr<arrow::MapBuilder> _builder;
};

// Helper method to throw nicer error messages if an std::get would fail.
// std::get throws a bad_variant_access, but without much useful information.
template<typename ValueT, typename SourceT>
const ValueT& get_or_throw(const SourceT& v, const std::string& on_error) {
    if (!std::holds_alternative<ValueT>(v)) {
        throw std::invalid_argument(on_error);
    }
    return std::get<ValueT>(v);
}

template<typename ValueT, typename BuilderT>
void append_or_throw(BuilderT& builder, const ValueT& val) {
    auto status = builder->Append(val);
    if (!status.ok()) {
        throw std::runtime_error(
          fmt::format("Unable to append to builder: {}", status.ToString()));
    }
}

struct converter_builder_visitor {
    std::unique_ptr<converter_interface>
    operator()(const iceberg::struct_type& s) {
        return std::make_unique<struct_converter>(s);
    }

    std::unique_ptr<converter_interface>
    operator()(const iceberg::list_type& s) {
        return std::make_unique<list_converter>(s);
    }

    std::unique_ptr<converter_interface>
    operator()(const iceberg::map_type& m) {
        return std::make_unique<map_converter>(m);
    }

    std::unique_ptr<converter_interface>
    operator()(const iceberg::primitive_type& p) {
        return std::visit(*this, p);
    }

    std::unique_ptr<converter_interface>
    operator()(const iceberg::boolean_type& /*t*/) {
        return std::make_unique<scalar_converter<arrow::BooleanType>>(
          arrow::boolean(), std::make_shared<arrow::BooleanBuilder>());
    }
    std::unique_ptr<converter_interface>
    operator()(const iceberg::int_type& /*t*/) {
        return std::make_unique<scalar_converter<arrow::Int32Type>>(
          arrow::int32(), std::make_shared<arrow::Int32Builder>());
    }
    std::unique_ptr<converter_interface>
    operator()(const iceberg::long_type& /*t*/) {
        return std::make_unique<scalar_converter<arrow::Int64Type>>(
          arrow::int64(), std::make_shared<arrow::Int64Builder>());
    }
    std::unique_ptr<converter_interface>
    operator()(const iceberg::float_type& /*t*/) {
        return std::make_unique<scalar_converter<arrow::FloatType>>(
          arrow::float32(), std::make_shared<arrow::FloatBuilder>());
    }
    std::unique_ptr<converter_interface>
    operator()(const iceberg::double_type& /*t*/) {
        return std::make_unique<scalar_converter<arrow::DoubleType>>(
          arrow::float64(), std::make_shared<arrow::DoubleBuilder>());
    }
    std::unique_ptr<converter_interface>
    operator()(const iceberg::decimal_type& t) {
        if (t.precision == 0) {
            return nullptr;
        }
        return std::make_unique<scalar_converter<arrow::Decimal128Type>>(
          arrow::decimal(t.precision, t.scale),
          std::make_shared<arrow::DecimalBuilder>(
            arrow::decimal(t.precision, t.scale)));
    }
    std::unique_ptr<converter_interface>
    operator()(const iceberg::date_type& /*t*/) {
        return std::make_unique<scalar_converter<arrow::Date32Type>>(
          arrow::date32(), std::make_shared<arrow::Date32Builder>());
    }
    std::unique_ptr<converter_interface>
    operator()(const iceberg::time_type& /*t*/) {
        return std::make_unique<scalar_converter<arrow::Time64Type>>(
          arrow::time64(arrow::TimeUnit::MICRO),
          std::make_shared<arrow::Time64Builder>(
            arrow::time64(arrow::TimeUnit::MICRO),
            arrow::default_memory_pool()));
    }
    std::unique_ptr<converter_interface>
    operator()(const iceberg::timestamp_type& /*t*/) {
        return std::make_unique<scalar_converter<arrow::TimestampType>>(
          arrow::timestamp(arrow::TimeUnit::MICRO),
          std::make_shared<arrow::TimestampBuilder>(
            arrow::timestamp(arrow::TimeUnit::MICRO),
            arrow::default_memory_pool()));
    }
    std::unique_ptr<converter_interface>
    operator()(const iceberg::timestamptz_type& /*t*/) {
        // Iceberg timestamps are UTC
        return std::make_unique<scalar_converter<arrow::TimestampType>>(
          arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
          std::make_shared<arrow::TimestampBuilder>(
            arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
            arrow::default_memory_pool()));
    }
    std::unique_ptr<converter_interface>
    operator()(const iceberg::string_type& /*t*/) {
        return std::make_unique<scalar_converter<arrow::StringType>>(
          arrow::utf8(), std::make_shared<arrow::StringBuilder>());
    }
    std::unique_ptr<converter_interface>
    operator()(const iceberg::uuid_type& /*t*/) {
        // TODO: there's an arrow extension for a uuid logical type.
        // Under-the-hood it's stored as a fixed(16). Do we want to use the
        // logical type here?
        return std::make_unique<scalar_converter<arrow::FixedSizeBinaryType>>(
          arrow::fixed_size_binary(16),
          std::make_shared<arrow::FixedSizeBinaryBuilder>(
            arrow::fixed_size_binary(16)));
    }
    std::unique_ptr<converter_interface>
    operator()(const iceberg::fixed_type& t) {
        return std::make_unique<scalar_converter<arrow::FixedSizeBinaryType>>(
          arrow::fixed_size_binary(static_cast<uint32_t>(t.length)),
          std::make_shared<arrow::FixedSizeBinaryBuilder>(
            arrow::fixed_size_binary(t.length)));
    }
    std::unique_ptr<converter_interface>
    operator()(const iceberg::binary_type& /*t*/) {
        return std::make_unique<scalar_converter<arrow::BinaryType>>(
          arrow::binary(), std::make_shared<arrow::BinaryBuilder>());
    }
};

template<typename ArrowType>
scalar_converter<ArrowType>::scalar_converter(
  const std::shared_ptr<arrow::DataType>& data_type,
  std::shared_ptr<BuilderType> builder)
  : _arrow_data_type{data_type}
  , _builder{builder} {}

template<typename ArrowType>
std::shared_ptr<arrow::Field> scalar_converter<ArrowType>::field(
  const std::string& name, iceberg::field_required required) {
    return arrow::field(
      name, _arrow_data_type, required == iceberg::field_required::no);
}

template<typename ArrowType>
std::shared_ptr<arrow::ArrayBuilder> scalar_converter<ArrowType>::builder() {
    return _builder;
};

template<typename ArrowType>
void scalar_converter<ArrowType>::add_optional_data(
  const std::optional<iceberg::value>& maybe_value) {
    if (!maybe_value.has_value()) {
        auto status = _builder->AppendNull();
        if (!status.ok()) {
            throw std::runtime_error(fmt::format(
              "Unable to append null to scalar converter: {}",
              status.ToString()));
        }
    } else {
        add_data(maybe_value.value());
    }
}

template<typename ArrowType>
void scalar_converter<ArrowType>::add_data(const iceberg::value& value) {
    add_primitive(get_or_throw<iceberg::primitive_value>(
      value, "Scalar converter got non-scalar data type"));
}

template<>
void scalar_converter<arrow::BooleanType>::add_primitive(
  const iceberg::primitive_value& prim_value) {
    auto val = get_or_throw<iceberg::boolean_value>(
      prim_value, "Scalar converter expected a boolean");
    append_or_throw(_builder, val.val);
}

template<>
void scalar_converter<arrow::Int32Type>::add_primitive(
  const iceberg::primitive_value& prim_value) {
    auto val = get_or_throw<iceberg::int_value>(
      prim_value, "Scalar converter expected an int32");
    append_or_throw(_builder, val.val);
}

template<>
void scalar_converter<arrow::Int64Type>::add_primitive(
  const iceberg::primitive_value& prim_value) {
    auto val = get_or_throw<iceberg::long_value>(
      prim_value, "Scalar converter expected an int64");
    append_or_throw(_builder, val.val);
}

template<>
void scalar_converter<arrow::FloatType>::add_primitive(
  const iceberg::primitive_value& prim_value) {
    auto val = get_or_throw<iceberg::float_value>(
      prim_value, "Scalar converter expected a float");
    append_or_throw(_builder, val.val);
}

template<>
void scalar_converter<arrow::DoubleType>::add_primitive(
  const iceberg::primitive_value& prim_value) {
    auto val = get_or_throw<iceberg::double_value>(
      prim_value, "Scalar converter expected a double");
    append_or_throw(_builder, val.val);
}

template<>
void scalar_converter<arrow::Decimal128Type>::add_primitive(
  const iceberg::primitive_value& prim_value) {
    auto val = get_or_throw<iceberg::decimal_value>(
                 prim_value, "Scalar converter expected a decimal")
                 .val;

    auto status = _builder->Append(
      arrow::Decimal128(absl::Int128High64(val), absl::Int128Low64(val)));
    if (!status.ok()) {
        throw std::runtime_error(
          fmt::format("Unable to append to builder: {}", status.ToString()));
    }
}

template<>
void scalar_converter<arrow::Date32Type>::add_primitive(
  const iceberg::primitive_value& prim_value) {
    auto val = get_or_throw<iceberg::date_value>(
      prim_value, "Scalar converter expected a date");
    append_or_throw(_builder, val.val);
}

template<>
void scalar_converter<arrow::Time64Type>::add_primitive(
  const iceberg::primitive_value& prim_value) {
    auto val = get_or_throw<iceberg::time_value>(
      prim_value, "Scalar converter expected a time");
    append_or_throw(_builder, val.val);
}

template<>
void scalar_converter<arrow::TimestampType>::add_primitive(
  const iceberg::primitive_value& prim_value) {
    if (std::holds_alternative<iceberg::timestamp_value>(prim_value)) {
        append_or_throw(
          _builder, std::get<iceberg::timestamp_value>(prim_value).val);
    } else if (std::holds_alternative<iceberg::timestamptz_value>(prim_value)) {
        append_or_throw(
          _builder, std::get<iceberg::timestamptz_value>(prim_value).val);
    } else {
        throw std::invalid_argument("Scalar converter expected a timestamp");
    }
}

template<>
void scalar_converter<arrow::StringType>::add_primitive(
  const iceberg::primitive_value& prim_value) {
    auto& val = get_or_throw<iceberg::string_value>(
      prim_value, "Scalar converter expected a string");
    auto status = _builder->AppendEmptyValue();
    if (!status.ok()) {
        throw std::runtime_error("Unable to add value to string builder");
    }
    for (const auto& frag : val.val) {
        status = _builder->ExtendCurrent(
          reinterpret_cast<const uint8_t*>(frag.get()), frag.size());
        if (!status.ok()) {
            throw std::runtime_error("Unable to add value to string builder");
        }
    }
}

template<>
void scalar_converter<arrow::FixedSizeBinaryType>::add_primitive(
  const iceberg::primitive_value& prim_value) {
    if (std::holds_alternative<iceberg::fixed_value>(prim_value)) {
        // TODO: This copies the entire string into a std::string.
        // FixedSizeBinaryBuilder doesn't have ExtendCurrent. For small
        // values that's probably ok, but this will cause problems with
        // seastar allocation limits. If we're going to continue using Arrow
        // we should make an ArrayBuilder for iobuf and/or ss:sstring.
        const auto& val = std::get<iceberg::fixed_value>(prim_value).val;
        auto begin = iobuf::byte_iterator(val.cbegin(), val.cend());
        auto end = iobuf::byte_iterator(val.cend(), val.cend());
        std::string str;
        std::copy(begin, end, std::back_inserter(str));
        if (str.size() != static_cast<size_t>(_builder->byte_width())) {
            throw std::invalid_argument(fmt::format(
              "Expected {} byte value. Got {}.",
              static_cast<size_t>(_builder->byte_width()),
              str.size()));
        }
        append_or_throw(_builder, str);
    } else if (std::holds_alternative<iceberg::uuid_value>(prim_value)) {
        const auto& val
          = std::get<iceberg::uuid_value>(prim_value).val.to_vector();
        append_or_throw(_builder, val.data());
    } else {
        throw std::invalid_argument(
          "Scalar converter expected a fixed or uuid");
    }
}

template<>
void scalar_converter<arrow::BinaryType>::add_primitive(
  const iceberg::primitive_value& prim_value) {
    const auto& val = get_or_throw<iceberg::binary_value>(
                        prim_value, "Scalar converter expected a binary field")
                        .val;
    auto status = _builder->AppendEmptyValue();
    if (!status.ok()) {
        throw std::runtime_error("Unable to add value to binary builder");
    }
    for (const auto& frag : val) {
        status = _builder->ExtendCurrent(
          reinterpret_cast<const uint8_t*>(frag.get()), frag.size());
        if (!status.ok()) {
            throw std::runtime_error("Unable to add value to binary builder");
        }
    }
}

struct_converter::struct_converter(const iceberg::struct_type& schema) {
    _fields.reserve(schema.fields.size());
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> child_builders;
    child_builders.reserve(schema.fields.size());
    for (const auto& field : schema.fields) {
        std::unique_ptr<converter_interface> child = std::visit(
          converter_builder_visitor{}, field->type);
        if (!child) {
            // We shouldn't get here. The child should throw if it can't be
            // constructed correctly.
            throw std::invalid_argument("Unable to construct child converter");
        }
        _fields.push_back(child->field(field->name, field->required));
        _field_required.push_back(bool(field->required));
        child_builders.push_back(child->builder());
        _child_converters.push_back(std::move(child));
    }
    _arrow_data_type = arrow::struct_(_fields);
    _builder = std::make_shared<arrow::StructBuilder>(
      _arrow_data_type, arrow::default_memory_pool(), child_builders);
}

std::shared_ptr<arrow::Field> struct_converter::field(
  const std::string& name, iceberg::field_required required) {
    return arrow::field(
      name, _arrow_data_type, required == iceberg::field_required::no);
}

std::shared_ptr<arrow::ArrayBuilder> struct_converter::builder() {
    return _builder;
};

void struct_converter::add_optional_data(
  const std::optional<iceberg::value>& maybe_value) {
    if (!maybe_value.has_value()) {
        // "Automatically appends an empty value to each child builder."
        auto status = _builder->AppendNull();
        if (!status.ok()) {
            throw std::runtime_error(fmt::format(
              "Unable to append null to struct builder: {}",
              status.ToString()));
        }
    } else {
        return add_data(maybe_value.value());
    }
}

void struct_converter::add_struct_data(const iceberg::struct_value struct_val) {
    if (struct_val.fields.size() != _child_converters.size()) {
        throw std::invalid_argument(fmt::format(
          "Got incorrect number of fields in struct converter. Expected {} "
          "got {}",
          _child_converters.size(),
          struct_val.fields.size()));
    }
    for (size_t i = 0; i < _child_converters.size(); i++) {
        if (_field_required[i] && !struct_val.fields[i].has_value()) {
            throw std::invalid_argument("Missing required field");
        }
        _child_converters[i]->add_optional_data(struct_val.fields[i]);
    }
    auto status = _builder->Append();
    if (!status.ok()) {
        // This is unexpected: if we were able to add to all of the child
        // builders, this append should be fine.
        throw std::runtime_error(fmt::format(
          "Unable to append to struct builder: {}", status.ToString()));
    }
}

void struct_converter::add_data(const iceberg::value& value) {
    // 1. Call add_data on all children
    // 2. Call append on our builder
    const auto& struct_val
      = get_or_throw<std::unique_ptr<iceberg::struct_value>>(
        value, "Scalar converter expected a binary field");

    add_struct_data(std::move(*struct_val));
}

arrow::FieldVector struct_converter::get_field_vector() { return _fields; }

std::shared_ptr<arrow::Array> struct_converter::take_chunk() {
    arrow::Result<std::shared_ptr<arrow::Array>> builder_result
      = _builder->Finish();
    if (!builder_result.status().ok()) {
        throw std::runtime_error(fmt::format(
          "Unable to create Arrow array: {}",
          builder_result.status().ToString()));
    }
    return builder_result.ValueUnsafe();
}

list_converter::list_converter(const iceberg::list_type& schema) {
    _child_converter = std::visit(
      converter_builder_visitor{}, schema.element_field->type);
    if (!_child_converter) {
        _arrow_data_type = nullptr;
        // We shouldn't get here. The child should throw if it can't be
        // constructed correctly.
        throw std::runtime_error("Unable to construct child converter");
    }
    std::shared_ptr<arrow::Field> child_field = _child_converter->field(
      schema.element_field->name, schema.element_field->required);
    if (child_field) {
        _arrow_data_type = arrow::list(child_field);
    } else {
        throw std::runtime_error("Child converter has null field");
    }
    _builder = std::make_shared<arrow::ListBuilder>(
      arrow::default_memory_pool(),
      _child_converter->builder(),
      _arrow_data_type);
}

std::shared_ptr<arrow::Field> list_converter::field(
  const std::string& name, iceberg::field_required required) {
    return arrow::field(
      name, _arrow_data_type, required == iceberg::field_required::no);
}

std::shared_ptr<arrow::ArrayBuilder> list_converter::builder() {
    return _builder;
};

void list_converter::add_optional_data(
  const std::optional<iceberg::value>& maybe_value) {
    if (!maybe_value.has_value()) {
        // This represents adding a null list to the builder, rather than
        // a null element to a list
        auto status = _builder->AppendNull();
        if (!status.ok()) {
            // This is unexpected: if we were able to add to all of the
            // child builders, this append should be fine.
            throw std::runtime_error(fmt::format(
              "Unable to append null to list builder: {}", status.ToString()));
        }
    } else {
        return add_data(maybe_value.value());
    }
}
void list_converter::add_data(const iceberg::value& value) {
    if (std::holds_alternative<std::unique_ptr<iceberg::list_value>>(value)) {
        // List API is:
        // 1. Append elements to child
        // 2. Append to the parent to start a new list
        const iceberg::list_value& list_value
          = *get_or_throw<std::unique_ptr<iceberg::list_value>>(
            value, "List converter expected a list");
        for (const auto& elem : list_value.elements) {
            _child_converter->add_optional_data(elem);
        }
        auto status = _builder->Append();
        if (!status.ok()) {
            // This is unexpected: if we were able to add to all of the
            // child builders, this append should be fine.
            throw std::runtime_error(fmt::format(
              "Unable to append to list builder: {}", status.ToString()));
        }
    } else {
        throw std::invalid_argument("Got invalid type for list converter");
    }
}

map_converter::map_converter(const iceberg::map_type& schema) {
    _key_converter = std::visit(
      converter_builder_visitor{}, schema.key_field->type);
    _value_converter = std::visit(
      converter_builder_visitor{}, schema.value_field->type);
    if (!_key_converter || !_value_converter) {
        _arrow_data_type = nullptr;
        // We shouldn't get here. The child should throw if it can't be
        // constructed correctly.
        throw std::runtime_error("Unable to construct child converter");
    }

    std::shared_ptr<arrow::Field> key_field = _key_converter->field(
      schema.key_field->name, schema.key_field->required);
    std::shared_ptr<arrow::Field> value_field = _value_converter->field(
      schema.value_field->name, schema.value_field->required);
    if (!key_field || !value_field) {
        _arrow_data_type = nullptr;
        throw std::runtime_error("Child converter has null field");
    }

    _arrow_data_type = arrow::map(key_field->type(), value_field->type());
    _builder = std::make_shared<arrow::MapBuilder>(
      arrow::default_memory_pool(),
      _key_converter->builder(),
      _value_converter->builder());
}

std::shared_ptr<arrow::Field> map_converter::field(
  const std::string& name, iceberg::field_required required) {
    return arrow::field(
      name, _arrow_data_type, required == iceberg::field_required::no);
}

std::shared_ptr<arrow::ArrayBuilder> map_converter::builder() {
    return _builder;
};

void map_converter::add_optional_data(
  const std::optional<iceberg::value>& maybe_value) {
    // The order of this is reversed from ListBuilder:
    // 1. Call Append on the parent builder to create a new slot
    // 2. Call append on the child builders to add data
    if (!maybe_value.has_value()) {
        auto status = _builder->AppendNull();
        if (!status.ok()) {
            // This is unexpected: if we were able to add to all of the
            // child builders, this append should be fine.
            throw std::runtime_error(fmt::format(
              "Unable to append null to list builder: {}", status.ToString()));
        }
    } else {
        add_data(maybe_value.value());
    }
}

void map_converter::add_data(const iceberg::value& value) {
    const auto& map_val = *get_or_throw<std::unique_ptr<iceberg::map_value>>(
      value, "Got invalid type for map converter");
    auto status = _builder->Append();
    if (!status.ok()) {
        throw std::runtime_error(
          fmt::format("Unable to append to map builder {}", status.ToString()));
    }

    for (const iceberg::kv_value& kv : map_val.kvs) {
        _key_converter->add_data(kv.key);
        _value_converter->add_optional_data(kv.val);
    }
}

arrow_translator::arrow_translator(iceberg::struct_type schema)
  : _struct_converter(std::make_unique<struct_converter>(std::move(schema))) {}

arrow_translator::~arrow_translator() = default;

std::shared_ptr<arrow::Schema> arrow_translator::build_arrow_schema() {
    return arrow::schema(_struct_converter->get_field_vector());
}

void arrow_translator::add_data(iceberg::struct_value value) {
    _struct_converter->add_struct_data(std::move(value));
}

std::shared_ptr<arrow::Array> arrow_translator::take_chunk() {
    return _struct_converter->take_chunk();
}
} // namespace datalake
