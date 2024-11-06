/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/values_avro.h"

#include "bytes/iobuf_parser.h"
#include "iceberg/avro_decimal.h"
#include "serde/avro/parser.h"

namespace datalake {

namespace {

ss::future<value_outcome> avro_parsed_to_value(
  std::unique_ptr<serde::avro::parsed::message> parsed_msg, avro::NodePtr node);

template<typename T, typename ValueT>
value_outcome
convert_primitive(T v, avro::Type expected_type, const avro::NodePtr& node) {
    if (node->type() != expected_type) {
        return value_conversion_exception(fmt::format(
          "Schema mismatch detected expected type {} got {}, current node: {}",
          expected_type,
          node->type(),
          node));
    }
    return ValueT{std::move(v)};
}

struct primitive_visitor {
    using ret_t = value_outcome;
    ret_t operator()(int32_t v) {
        if (node->logicalType().type() == avro::LogicalType::DATE) {
            return convert_primitive<int32_t, iceberg::date_value>(
              v, avro::AVRO_INT, node);
        }
        if (node->logicalType().type() == avro::LogicalType::TIME_MILLIS) {
            std::chrono::milliseconds ms(v);
            return convert_primitive<int64_t, iceberg::time_value>(
              std::chrono::duration_cast<std::chrono::microseconds>(ms).count(),
              avro::AVRO_INT,
              node);
        }
        return convert_primitive<int32_t, iceberg::int_value>(
          v, avro::AVRO_INT, node);
    }
    ret_t operator()(int64_t v) {
        if (node->type() == avro::AVRO_ENUM) {
            return convert_primitive<int64_t, iceberg::long_value>(
              v, avro::AVRO_ENUM, node);
        }
        if (node->logicalType().type() == avro::LogicalType::TIME_MICROS) {
            return convert_primitive<int64_t, iceberg::time_value>(
              v, avro::AVRO_LONG, node);
        }
        if (node->logicalType().type() == avro::LogicalType::TIMESTAMP_MILLIS) {
            // times 1000 to convert from milliseconds to microseconds that
            // iceberg requires
            return convert_primitive<int64_t, iceberg::timestamp_value>(
              v * 1000, avro::AVRO_LONG, node);
        }
        if (node->logicalType().type() == avro::LogicalType::TIMESTAMP_MICROS) {
            return convert_primitive<int64_t, iceberg::timestamp_value>(
              v, avro::AVRO_LONG, node);
        }
        return convert_primitive<int64_t, iceberg::long_value>(
          v, avro::AVRO_LONG, node);
    }
    ret_t operator()(bool v) {
        return convert_primitive<bool, iceberg::boolean_value>(
          v, avro::AVRO_BOOL, node);
    }
    ret_t operator()(serde::avro::parsed::avro_null) {
        return value_conversion_exception(
          "Conversion of NULL type is not supported. This type should be "
          "explicitly skipped");
    }
    ret_t operator()(double v) {
        return convert_primitive<double, iceberg::double_value>(
          v, avro::AVRO_DOUBLE, node);
    }
    ret_t operator()(float v) {
        return convert_primitive<float, iceberg::float_value>(
          v, avro::AVRO_FLOAT, node);
    }
    ret_t operator()(iobuf buffer) {
        if (node->logicalType().type() == avro::LogicalType::DECIMAL) {
            return iceberg::decimal_value{
              .val = iceberg::iobuf_to_avro_decimal(std::move(buffer))};
        }

        if (node->type() == avro::AVRO_FIXED) {
            return convert_primitive<iobuf, iceberg::fixed_value>(
              std::move(buffer), avro::AVRO_FIXED, node);
        }

        if (node->type() == avro::AVRO_STRING) {
            if (node->logicalType().type() == avro::LogicalType::UUID) {
                auto sz = buffer.size_bytes();
                iobuf_parser p(std::move(buffer));
                return convert_primitive<uuid_t, iceberg::uuid_value>(
                  uuid_t::from_string(p.read_string(sz)),
                  avro::AVRO_STRING,
                  node);
            }
            return convert_primitive<iobuf, iceberg::string_value>(
              std::move(buffer), avro::AVRO_STRING, node);
        }
        return convert_primitive<iobuf, iceberg::binary_value>(
          std::move(buffer), avro::AVRO_BYTES, node);
    }
    avro::NodePtr node;
};

struct parsed_msg_visitor {
    ss::future<value_outcome> operator()(serde::avro::parsed::record record) {
        auto ret = std::make_unique<iceberg::struct_value>();
        ret->fields.reserve(node->leaves());
        for (size_t i = 0; i < node->leaves(); ++i) {
            const auto& field_node = node->leafAt(i);
            if (field_node->type() == avro::AVRO_NULL) {
                continue;
            }
            auto result = co_await avro_parsed_to_value(
              std::move(record.fields[i]), field_node);
            if (result.has_error()) {
                co_return result.error();
            }
            ret->fields.push_back(std::move(result.value()));
        }
        co_return ret;
    }
    ss::future<value_outcome> operator()(serde::avro::parsed::map parsed_map) {
        auto ret = std::make_unique<iceberg::map_value>();
        ret->kvs.reserve(parsed_map.entries.size());
        const auto key_node = node->leafAt(0);
        const auto value_node = node->leafAt(1);
        for (auto& [k, v] : parsed_map.entries) {
            if (key_node->type() != avro::AVRO_STRING) {
                co_return value_conversion_exception(fmt::format(
                  "Map key type mismatch. Expected string, got {}",
                  key_node->type()));
            }
            // Avro map keys are always stings, use primitive visitor directly
            auto key_result = primitive_visitor{key_node}(std::move(k));
            if (key_result.has_error()) {
                co_return key_result.error();
            }
            auto value_result = co_await avro_parsed_to_value(
              std::move(v), value_node);
            if (value_result.has_error()) {
                co_return value_result.error();
            }
            ret->kvs.emplace_back(
              std::move(key_result.value()), std::move(value_result.value()));
        }
        co_return ret;
    }
    ss::future<value_outcome> operator()(serde::avro::parsed::list v) {
        auto ret = std::make_unique<iceberg::list_value>();
        ret->elements.reserve(v.elements.size());
        auto element_node = node->leafAt(0);
        for (auto& e : v.elements) {
            auto result = co_await avro_parsed_to_value(
              std::move(e), element_node);
            if (result.has_error()) {
                co_return result.error();
            }
            ret->elements.push_back(std::move(result.value()));
        }
        co_return ret;
    }
    ss::future<value_outcome> operator()(serde::avro::parsed::avro_union v) {
        // union is represented as a struct with all possible types but only one
        // present
        auto ret = std::make_unique<iceberg::struct_value>();
        ret->fields.reserve(node->leaves());
        for (size_t i = 0; i < node->leaves(); ++i) {
            // handle branch that wasn't selected
            const auto& field_node = node->leafAt(i);
            // if it is null, do nothing as it is not represented in iceberg
            // schema
            if (field_node->type() == avro::AVRO_NULL) {
                continue;
            }

            if (i != v.branch) {
                ret->fields.push_back(std::nullopt);
                continue;
            }
            // parse the selected branch
            auto result = co_await avro_parsed_to_value(
              std::move(v.message), field_node);
            if (result.has_error()) {
                co_return result.error();
            }
            ret->fields.push_back(std::move(result.value()));
        }
        co_return ret;
    }
    ss::future<value_outcome> operator()(serde::avro::parsed::primitive v) {
        co_return std::visit(primitive_visitor{node}, std::move(v));
    }
    avro::NodePtr node;
};

ss::future<value_outcome> avro_parsed_to_value(
  std::unique_ptr<serde::avro::parsed::message> parsed_msg,
  avro::NodePtr node) {
    if (node->type() == avro::AVRO_NULL) {
        co_return value_conversion_exception(
          "Conversion of NULL type is not supported. This type should be "
          "explicitly skipped");
    }

    if (node->type() == avro::AVRO_UNKNOWN) {
        co_return value_conversion_exception(
          "AVRO_UNKNOWN type conversion is not supported");
    }

    if (node->type() == avro::AVRO_SYMBOLIC) {
        co_return co_await avro_parsed_to_value(
          std::move(parsed_msg), avro::resolveSymbol(node));
    }

    co_return co_await std::visit(
      parsed_msg_visitor{node}, std::move(*parsed_msg));
}

} // namespace

ss::future<value_outcome>
deserialize_avro(iobuf buffer, avro::ValidSchema schema) {
    try {
        // Avro parser handles max recursion depth, it is safe not to track it
        // when converting parsed to avro values as the traversal is based on
        // the parsed messages, in contrast to protobuf avro values are always
        // serialized even if set to default.
        auto parsed = co_await serde::avro::parse(std::move(buffer), schema);
        /**
         * The same way we do with the schema. Top level type is always encoded
         * as struct value with one field to
         * match iceberg schema.
         */
        if (schema.root()->type() != avro::AVRO_RECORD) {
            auto root_field = co_await avro_parsed_to_value(
              std::move(parsed), schema.root());
            if (root_field.has_error()) {
                co_return root_field.error();
            }
            auto ret = std::make_unique<iceberg::struct_value>();
            ret->fields.push_back(std::move(root_field.value()));
            co_return ret;
        }
        co_return co_await avro_parsed_to_value(
          std::move(parsed), schema.root());
    } catch (...) {
        co_return value_outcome{value_conversion_exception(fmt::format(
          "Error parsing avro message - {}", std::current_exception()))};
    }
}

} // namespace datalake
