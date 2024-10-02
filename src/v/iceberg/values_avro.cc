// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/values_avro.h"

#include "bytes/iobuf_parser.h"
#include "iceberg/values.h"

#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Node.hh>
#include <avro/Types.hh>

namespace iceberg {

namespace {

// Returns the Avro datum corresponding to the given value. The value is
// expected to match the given Avro schema.
//
// The given value may be null if the schema refers to an optional value.
avro::GenericDatum
val_to_avro(const std::optional<value>&, const avro::NodePtr&);
avro::GenericDatum base_val_to_avro(const value&, const avro::NodePtr&);

// Throws an exception if the given types don't match.
void maybe_throw_wrong_type(
  const avro::Type& expected, const avro::Type& actual) {
    if (expected != actual) {
        throw std::invalid_argument(fmt::format(
          "Expected {} type but got {}",
          avro::toString(expected),
          avro::toString(actual)));
    }
}

void maybe_throw_wrong_logical_type(
  const avro::LogicalType& expected, const avro::LogicalType& actual) {
    if (
      expected.type() != actual.type() || expected.scale() != actual.scale()
      || expected.precision() != actual.precision()) {
        throw std::invalid_argument(fmt::format(
          "Expected (type: {}, precision: {}, scale: {}) logical_type but got: "
          "(type: {}, precision: {}, scale: {})",
          expected.type(),
          expected.precision(),
          expected.scale(),
          actual.type(),
          actual.precision(),
          actual.scale()));
    }
}

// Throws an exception if the given schema isn't valid for the given type.
void maybe_throw_invalid_schema(
  const avro::NodePtr& actual_schema, const avro::Type& expected_type) {
    if (!actual_schema) {
        throw std::invalid_argument(fmt::format(
          "Unexpected null schema, expected {} type!",
          avro::toString(expected_type)));
    }
    const auto& actual_type = actual_schema->type();
    maybe_throw_wrong_type(expected_type, actual_type);
}

struct primitive_value_avro_visitor {
    explicit primitive_value_avro_visitor(const avro::NodePtr& avro_schema)
      : avro_schema_(avro_schema) {}

    // Expected to match with the value on which the caller is calling
    // operator().
    const avro::NodePtr& avro_schema_;

    avro::GenericDatum operator()(const boolean_value& v) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_BOOL);
        return {v.val};
    }
    avro::GenericDatum operator()(const int_value& v) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_INT);
        return {v.val};
    }
    avro::GenericDatum operator()(const long_value& v) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_LONG);
        return {v.val};
    }
    avro::GenericDatum operator()(const float_value& v) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_FLOAT);
        return {v.val};
    }
    avro::GenericDatum operator()(const double_value& v) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_DOUBLE);
        return {v.val};
    }
    avro::GenericDatum operator()(const date_value& v) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_INT);
        return {v.val};
    }
    avro::GenericDatum operator()(const time_value& v) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_LONG);
        return {v.val};
    }
    avro::GenericDatum operator()(const timestamp_value& v) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_LONG);
        return {v.val};
    }
    avro::GenericDatum operator()(const timestamptz_value& v) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_LONG);
        return {v.val};
    }
    avro::GenericDatum operator()(const string_value& v) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_STRING);
        const auto size_bytes = v.val.size_bytes();
        iobuf_const_parser buf(v.val);
        return {buf.read_string(size_bytes)};
    }
    avro::GenericDatum operator()(const binary_value& v) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_BYTES);
        std::vector<uint8_t> data;
        auto bytes = iobuf_to_bytes(v.val);
        data.resize(bytes.size());
        for (size_t i = 0; i < bytes.size(); i++) {
            data[i] = bytes[i];
        }
        return {data};
    }
    avro::GenericDatum operator()(const decimal_value& v) {
        std::vector<uint8_t> bytes(16);
        auto low = absl::Int128Low64(v.val);
        auto high = absl::Int128High64(v.val);
        // Store in big-endian order (most significant byte at index 0)
        for (size_t i = 0; i < 8; ++i) {
            bytes[i + 8] = static_cast<uint8_t>(low >> ((7 - i) * 8));
            bytes[i] = static_cast<uint8_t>(high >> ((7 - i) * 8));
        }

        return {avro_schema_, avro::GenericFixed(avro_schema_, bytes)};
    }
    avro::GenericDatum operator()(const fixed_value& fixed) {
        std::vector<uint8_t> bytes(fixed.val.size_bytes());
        iobuf::iterator_consumer it(fixed.val.cbegin(), fixed.val.cend());
        it.consume_to(fixed.val.size_bytes(), bytes.data());
        return {avro_schema_, avro::GenericFixed(avro_schema_, bytes)};
    }
    avro::GenericDatum operator()(const uuid_value& uuid) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_STRING);
        return {avro_schema_, fmt::to_string(uuid.val)};
    }
};

struct value_avro_visitor {
    explicit value_avro_visitor(const avro::NodePtr& avro_schema)
      : avro_schema_(avro_schema) {}

    // Expected to match with the value on which the caller is calling
    // operator().
    const avro::NodePtr& avro_schema_;

    avro::GenericDatum operator()(const primitive_value& v) {
        return std::visit(primitive_value_avro_visitor{avro_schema_}, v);
    }
    avro::GenericDatum operator()(const std::unique_ptr<struct_value>& v) {
        return struct_to_avro(*v, avro_schema_);
    }
    avro::GenericDatum operator()(const std::unique_ptr<list_value>& v) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_ARRAY);
        if (avro_schema_->leaves() != 1) {
            throw std::invalid_argument(
              fmt::format("Expected 1 leaf: {}", avro_schema_->leaves()));
        }
        avro::GenericDatum datum(avro_schema_);
        auto& arr = datum.value<avro::GenericArray>();
        const auto& child_schema = avro_schema_->leafAt(0);
        for (const auto& element_ptr : v->elements) {
            arr.value().emplace_back(val_to_avro(element_ptr, child_schema));
        }
        return datum;
    }
    avro::GenericDatum operator()(const std::unique_ptr<map_value>& v) {
        maybe_throw_invalid_schema(avro_schema_, avro::AVRO_ARRAY);
        if (avro_schema_->leaves() != 1) {
            throw std::invalid_argument(
              fmt::format("Expected 1 leaf: {}", avro_schema_->leaves()));
        }
        avro::GenericDatum datum(avro_schema_);
        auto& m = datum.value<avro::GenericArray>();
        const auto& kv_schema = avro_schema_->leafAt(0);
        maybe_throw_invalid_schema(kv_schema, avro::AVRO_RECORD);
        // Map Avro schemas are generated by Redpanda as an array of key-value
        // records.
        if (kv_schema->leaves() != 2) {
            throw std::invalid_argument(
              fmt::format("Expected 2 leaves: {}", kv_schema->leaves()));
        }
        for (const auto& kv_ptr : v->kvs) {
            avro::GenericDatum kv_datum(kv_schema);
            auto& kv_record = kv_datum.value<avro::GenericRecord>();
            kv_record.setFieldAt(
              0, base_val_to_avro(kv_ptr.key, kv_schema->leafAt(0)));
            kv_record.setFieldAt(
              1, val_to_avro(kv_ptr.val, kv_schema->leafAt(1)));
            m.value().emplace_back(std::move(kv_datum));
        }
        return datum;
    }
};

avro::GenericDatum
base_val_to_avro(const value& val, const avro::NodePtr& avro_schema) {
    return std::visit(value_avro_visitor{avro_schema}, val);
}
avro::GenericDatum
val_to_avro(const std::optional<value>& val, const avro::NodePtr& avro_schema) {
    if (!avro_schema) {
        throw std::invalid_argument("Null schema for val!");
    }
    // NOTE: there isn't enough typing metadata to infer from the value alone
    // if this is optional or not, so we must rely on the schema.
    //
    // In Iceberg, this is represented as a union, and is the only valid use of
    // union in Iceberg data files.
    if (avro_schema->type() == avro::AVRO_UNION) {
        // Union Avro schemas are generated by Redpanda for optional fields,
        // which should have just two leaves.
        if (avro_schema->leaves() != 2) {
            throw std::invalid_argument(
              fmt::format("Expected 2 leaves: {}", avro_schema->leaves()));
        }
        auto un = avro::GenericUnion(avro_schema);
        if (val == std::nullopt) {
            un.selectBranch(0);
            return avro::GenericDatum(avro_schema, un);
        }
        un.selectBranch(1);
        un.datum() = val_to_avro(val, avro_schema->leafAt(1));
        return avro::GenericDatum(avro_schema, un);
    }
    if (!val.has_value()) {
        throw std::invalid_argument(fmt::format(
          "Value is null but expected {} type",
          avro::toString(avro_schema->type())));
    }
    return base_val_to_avro(*val, avro_schema);
}

} // namespace

avro::GenericDatum
struct_to_avro(const struct_value& v, const avro::NodePtr& avro_schema) {
    maybe_throw_invalid_schema(avro_schema, avro::AVRO_RECORD);
    avro::GenericDatum datum(avro_schema);
    auto& record = datum.value<avro::GenericRecord>();
    if (record.fieldCount() != v.fields.size()) {
        throw std::invalid_argument(fmt::format(
          "Struct value does not match given Avro schema: {} "
          "fields vs expected {}: {}",
          v.fields.size(),
          record.fieldCount(),
          v));
    }
    for (size_t i = 0; i < v.fields.size(); i++) {
        const auto& child_val_ptr = v.fields[i];
        const auto& child_schema = avro_schema->leafAt(i);
        record.setFieldAt(i, val_to_avro(child_val_ptr, child_schema));
    }
    return datum;
}

namespace {

struct primitive_value_parsing_visitor {
    explicit primitive_value_parsing_visitor(const avro::GenericDatum& data)
      : data_(data) {}
    const avro::GenericDatum& data_;

    value operator()(const boolean_type&) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_BOOL);
        return boolean_value{data_.value<bool>()};
    }
    value operator()(const int_type&) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_INT);
        return int_value{data_.value<int>()};
    }
    value operator()(const long_type&) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_LONG);
        return long_value{data_.value<long>()};
    }
    value operator()(const float_type&) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_FLOAT);
        return float_value{data_.value<float>()};
    }
    value operator()(const double_type&) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_DOUBLE);
        return double_value{data_.value<double>()};
    }
    value operator()(const date_type&) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_INT);
        return date_value{data_.value<int32_t>()};
    }
    value operator()(const time_type&) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_LONG);
        return time_value{data_.value<int64_t>()};
    }
    value operator()(const timestamp_type&) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_LONG);
        return timestamp_value{data_.value<int64_t>()};
    }
    value operator()(const timestamptz_type&) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_LONG);
        return timestamptz_value{data_.value<int64_t>()};
    }
    value operator()(const string_type&) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_STRING);
        return string_value{iobuf::from(data_.value<std::string>())};
    }
    value operator()(const fixed_type& schema_type) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_FIXED);
        auto lt = avro::LogicalType(avro::LogicalType::NONE);
        maybe_throw_wrong_logical_type(data_.logicalType(), lt);

        const auto& v = data_.value<avro::GenericFixed>().value();
        if (v.size() != schema_type.length) {
            throw std::invalid_argument(fmt::format(
              "Fixed type length mismatch, schema length: {}, current value "
              "length: {}",
              schema_type.length,
              v.size()));
        }
        iobuf b;
        b.append(v.data(), v.size());
        return fixed_value{.val = std::move(b)};
    }
    value operator()(const uuid_type&) {
        // in Avro UUID can be either fixed or string type
        if (data_.type() == avro::AVRO_FIXED) {
            maybe_throw_wrong_logical_type(
              data_.logicalType(), avro::LogicalType(avro::LogicalType::UUID));
            const auto& v = data_.value<avro::GenericFixed>().value();
            return uuid_value{.val = uuid_t(v)};
        }
        maybe_throw_wrong_type(data_.type(), avro::AVRO_STRING);
        maybe_throw_wrong_logical_type(
          data_.logicalType(), avro::LogicalType(avro::LogicalType::UUID));
        const auto& v = data_.value<std::string>();

        auto uuid = uuid_t::from_string(v);
        return uuid_value{uuid};
    }
    value operator()(const binary_type&) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_BYTES);
        const auto& v = data_.value<std::vector<uint8_t>>();
        iobuf b;
        b.append(v.data(), v.size());
        return binary_value{std::move(b)};
    }
    value operator()(const decimal_type& dt) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_FIXED);
        auto lt = avro::LogicalType(avro::LogicalType::DECIMAL);
        lt.setPrecision(dt.precision);
        lt.setScale(dt.scale);
        maybe_throw_wrong_logical_type(data_.logicalType(), lt);
        const auto& v = data_.value<avro::GenericFixed>().value();
        if (v.size() > 16) {
            throw std::invalid_argument(fmt::format(
              "decimals with more than 16 bytes are not supported, current "
              "value size: {}",
              v.size()));
        }
        int64_t high_half{0};
        uint64_t low_half{0};
        for (size_t i = 0; i < std::min<size_t>(v.size(), 8); ++i) {
            high_half |= int64_t(v[i]) << (7 - i) * 8;
        };
        for (size_t i = 8; i < std::min<size_t>(v.size(), 16); ++i) {
            low_half |= uint64_t(v[i]) << (15 - i) * 8;
        };

        return decimal_value{.val = absl::MakeInt128(high_half, low_half)};
    }
};

struct value_parsing_visitor {
    explicit value_parsing_visitor(const avro::GenericDatum& data)
      : data_(data) {}
    const avro::GenericDatum& data_;

    value operator()(const primitive_type& t) {
        return std::visit(primitive_value_parsing_visitor{data_}, t);
    }
    value operator()(const list_type& t) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_ARRAY);
        auto v = std::make_unique<list_value>();
        const auto& array = data_.value<avro::GenericArray>();
        for (const auto& d : array.value()) {
            v->elements.emplace_back(val_from_avro(
              d, t.element_field->type, t.element_field->required));
        }
        return v;
    }
    value operator()(const struct_type& t) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_RECORD);
        auto v = std::make_unique<struct_value>();
        const auto& record = data_.value<avro::GenericRecord>();
        if (t.fields.size() != record.fieldCount()) {
            throw std::invalid_argument(fmt::format(
              "Expected key-value record of size {}: {}",
              t.fields.size(),
              record.fieldCount()));
        }
        for (size_t i = 0; i < t.fields.size(); i++) {
            v->fields.emplace_back(val_from_avro(
              record.fieldAt(i), t.fields[i]->type, t.fields[i]->required));
        }
        return v;
    }
    value operator()(const map_type& t) {
        maybe_throw_wrong_type(data_.type(), avro::AVRO_ARRAY);
        auto v = std::make_unique<map_value>();
        const auto& array = data_.value<avro::GenericArray>();
        for (const auto& d : array.value()) {
            maybe_throw_wrong_type(d.type(), avro::AVRO_RECORD);
            const auto& kv_record = d.value<avro::GenericRecord>();
            if (kv_record.fieldCount() != 2) {
                throw std::invalid_argument(fmt::format(
                  "Expected key-value record of size 2: {}",
                  kv_record.fieldCount()));
            }
            const auto& k_record = kv_record.fieldAt(0);
            const auto& v_record = kv_record.fieldAt(1);
            kv_value kv_val{
              .key = std::move(*val_from_avro(
                k_record, t.key_field->type, t.key_field->required)),
              .val = val_from_avro(
                v_record, t.value_field->type, t.value_field->required),
            };
            v->kvs.emplace_back(std::move(kv_val));
        }
        return v;
    }
};

} // namespace

std::optional<value> val_from_avro(
  const avro::GenericDatum& d,
  const field_type& expected_type,
  field_required required) {
    if (required) {
        if (d.isUnion()) {
            throw std::invalid_argument(fmt::format(
              "Unexpected union for required field: {}",
              avro::toString(d.type())));
        }
        return std::visit(value_parsing_visitor{d}, expected_type);
    }
    if (!d.isUnion()) {
        throw std::invalid_argument(fmt::format(
          "Expected union for non-required field: {}",
          avro::toString(d.type())));
    }
    // NOTE: type() on a union unwraps the union type.
    if (d.type() == avro::AVRO_NULL) {
        return std::nullopt;
    }
    return std::visit(value_parsing_visitor{d}, expected_type);
}

} // namespace iceberg
