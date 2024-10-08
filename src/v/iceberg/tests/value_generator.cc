// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/tests/value_generator.h"

#include "bytes/random.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "random/generators.h"
#include "test_utils/randoms.h"

namespace iceberg::tests {

struct generating_primitive_value_visitor {
    explicit generating_primitive_value_visitor(const value_spec& spec)
      : spec_(spec) {}
    const value_spec& spec_;

    int64_t generate_numeric_val() const {
        switch (spec_.pattern) {
        case value_pattern::zeros:
            return 0;
        case value_pattern::random:
            return random_generators::get_int<int64_t>();
        }
    }

    value operator()(const boolean_type&) {
        switch (spec_.pattern) {
        case value_pattern::zeros:
            return boolean_value{false};
        case value_pattern::random:
            return boolean_value{::tests::random_bool()};
        }
    }
    value operator()(const int_type&) {
        return int_value{static_cast<int>(
          spec_.forced_num_val.value_or(generate_numeric_val()))};
    }
    value operator()(const long_type&) {
        return long_value{
          spec_.forced_num_val.value_or(generate_numeric_val())};
    }
    value operator()(const float_type&) {
        return float_value{static_cast<float>(
          spec_.forced_num_val.value_or(generate_numeric_val()))};
    }
    value operator()(const double_type&) {
        return double_value{static_cast<double>(
          spec_.forced_num_val.value_or(generate_numeric_val()))};
    }
    value operator()(const decimal_type&) {
        if (spec_.forced_num_val) {
            return decimal_value{absl::MakeInt128(0, generate_numeric_val())};
        }
        return decimal_value{
          absl::MakeInt128(generate_numeric_val(), generate_numeric_val())};
    }
    value operator()(const date_type&) {
        return date_value{static_cast<int>(
          spec_.forced_num_val.value_or(generate_numeric_val()))};
    }
    value operator()(const time_type&) {
        return time_value{
          spec_.forced_num_val.value_or(generate_numeric_val())};
    }
    value operator()(const timestamp_type&) {
        return timestamp_value{
          spec_.forced_num_val.value_or(generate_numeric_val())};
    }
    value operator()(const timestamptz_type&) {
        return timestamptz_value{
          spec_.forced_num_val.value_or(generate_numeric_val())};
    }
    value operator()(const string_type&) {
        switch (spec_.pattern) {
        case value_pattern::zeros:
            return string_value{iobuf{}};
        case value_pattern::random:
            return string_value{
              random_generators::make_iobuf(spec_.max_strlen)};
        }
    }
    value operator()(const uuid_type&) {
        switch (spec_.pattern) {
        case value_pattern::zeros:
            return uuid_value{uuid_t{}};
        case value_pattern::random:
            return uuid_value{uuid_t::create()};
        }
    }
    value operator()(const fixed_type& t) {
        if (spec_.forced_fixed_val.has_value()) {
            return fixed_value{spec_.forced_fixed_val.value().copy()};
        }
        switch (spec_.pattern) {
        case value_pattern::zeros:
            return fixed_value{
              bytes_to_iobuf(bytes::from_string(std::string(t.length, 0)))};
        case value_pattern::random:
            return fixed_value{random_generators::make_iobuf(t.length)};
        }
    }
    value operator()(const binary_type&) {
        switch (spec_.pattern) {
        case value_pattern::zeros:
            return binary_value{iobuf{}};
        case value_pattern::random:
            return binary_value{
              random_generators::make_iobuf(spec_.max_strlen)};
        }
    }
};

struct generating_value_visitor {
    explicit generating_value_visitor(const value_spec& spec)
      : spec_(spec) {}
    const value_spec& spec_;

    size_t generate_num_elements() const {
        switch (spec_.pattern) {
        case value_pattern::zeros:
            return spec_.max_elements;
        case value_pattern::random:
            return random_generators::get_int<size_t>() % spec_.max_elements;
        }
    }

    bool should_null(const nested_field_ptr& field) const {
        if (field->required) {
            return false;
        }
        if (spec_.null_pct <= 0) {
            return false;
        }
        auto null_pct = std::min(spec_.null_pct, 100);
        return random_generators::get_int(0, 100) <= null_pct;
    }

    value operator()(const primitive_type& t) {
        return std::visit(generating_primitive_value_visitor{spec_}, t);
    }
    value operator()(const struct_type& t) {
        auto ret = std::make_unique<struct_value>();
        for (const auto& f : t.fields) {
            ret->fields.emplace_back(
              should_null(f)
                ? std::nullopt
                : std::make_optional<value>(make_value(spec_, f->type)));
        }
        return ret;
    }
    value operator()(const list_type& t) {
        auto ret = std::make_unique<list_value>();
        for (size_t i = 0; i < generate_num_elements(); i++) {
            ret->elements.emplace_back(
              should_null(t.element_field)
                ? std::nullopt
                : std::make_optional<value>(
                    make_value(spec_, t.element_field->type)));
        }
        return ret;
    }
    value operator()(const map_type& t) {
        auto ret = std::make_unique<map_value>();
        for (size_t i = 0; i < generate_num_elements(); i++) {
            kv_value kv{
              make_value(spec_, t.key_field->type),
              should_null(t.value_field) ? std::nullopt
                                         : std::make_optional<value>(make_value(
                                             spec_, t.value_field->type))};
            ret->kvs.emplace_back(std::move(kv));
        }
        return ret;
    }
};

value make_value(const value_spec& spec, const field_type& type) {
    return std::visit(generating_value_visitor{spec}, type);
}

struct_value make_struct_value(const value_spec& spec, const field_type& type) {
    auto val = std::visit(generating_value_visitor{spec}, type);

    return std::move(*std::get<std::unique_ptr<struct_value>>(val));
}

} // namespace iceberg::tests
