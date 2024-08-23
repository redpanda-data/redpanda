// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/tests/value_generator.h"

#include "iceberg/datatypes.h"
#include "iceberg/values.h"

namespace iceberg::tests {

struct generating_primitive_value_visitor {
    explicit generating_primitive_value_visitor(const value_spec& spec)
      : spec_(spec) {}
    const value_spec& spec_;

    value operator()(const boolean_type&) { return boolean_value{false}; }
    value operator()(const int_type&) {
        return int_value{static_cast<int>(spec_.forced_num_val.value_or(0))};
    }
    value operator()(const long_type&) {
        return long_value{spec_.forced_num_val.value_or(0)};
    }
    value operator()(const float_type&) {
        return float_value{
          static_cast<float>(spec_.forced_num_val.value_or(0))};
    }
    value operator()(const double_type&) {
        return double_value{
          static_cast<double>(spec_.forced_num_val.value_or(0))};
    }
    value operator()(const decimal_type&) {
        return decimal_value{spec_.forced_num_val.value_or(0)};
    }
    value operator()(const date_type&) {
        return date_value{static_cast<int>(spec_.forced_num_val.value_or(0))};
    }
    value operator()(const time_type&) {
        return time_value{spec_.forced_num_val.value_or(0)};
    }
    value operator()(const timestamp_type&) {
        return timestamp_value{spec_.forced_num_val.value_or(0)};
    }
    value operator()(const timestamptz_type&) {
        return timestamptz_value{spec_.forced_num_val.value_or(0)};
    }
    value operator()(const string_type&) { return string_value{iobuf{}}; }
    value operator()(const uuid_type&) { return uuid_value{uuid_t{}}; }
    value operator()(const fixed_type&) { return fixed_value{iobuf{}}; }
    value operator()(const binary_type&) { return binary_value{iobuf{}}; }
};

struct generating_value_visitor {
    explicit generating_value_visitor(const value_spec& spec)
      : spec_(spec) {}
    const value_spec& spec_;

    value operator()(const primitive_type& t) {
        return std::visit(generating_primitive_value_visitor{spec_}, t);
    }
    value operator()(const struct_type& t) {
        auto ret = std::make_unique<struct_value>();
        for (const auto& f : t.fields) {
            ret->fields.emplace_back(make_value(spec_, f->type));
        }
        return ret;
    }
    value operator()(const list_type& t) {
        auto ret = std::make_unique<list_value>();
        for (size_t i = 0; i < spec_.max_elements; i++) {
            ret->elements.emplace_back(
              make_value(spec_, t.element_field->type));
        }
        return ret;
    }
    value operator()(const map_type& t) {
        auto ret = std::make_unique<map_value>();
        for (size_t i = 0; i < spec_.max_elements; i++) {
            kv_value kv{
              make_value(spec_, t.key_field->type),
              make_value(spec_, t.value_field->type)};
            ret->kvs.emplace_back(std::move(kv));
        }
        return ret;
    }
};

value make_value(const value_spec& spec, const field_type& type) {
    return std::visit(generating_value_visitor{spec}, type);
}

} // namespace iceberg::tests
