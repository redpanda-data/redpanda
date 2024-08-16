// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/struct_accessor.h"

#include "iceberg/datatypes.h"
#include "iceberg/values.h"

#include <variant>

namespace iceberg {

namespace {

struct type_validating_visitor {
    explicit type_validating_visitor(const primitive_type& t)
      : type_(t) {}
    const primitive_type& type_;

    bool operator()(const boolean_value&) const {
        return std::holds_alternative<boolean_type>(type_);
    }
    bool operator()(const int_value&) const {
        return std::holds_alternative<int_type>(type_);
    }
    bool operator()(const long_value&) const {
        return std::holds_alternative<long_type>(type_);
    }
    bool operator()(const float_value&) const {
        return std::holds_alternative<float_type>(type_);
    }
    bool operator()(const double_value&) const {
        return std::holds_alternative<double_type>(type_);
    }
    bool operator()(const decimal_value&) const {
        return std::holds_alternative<decimal_type>(type_);
    }
    bool operator()(const date_value&) const {
        return std::holds_alternative<date_type>(type_);
    }
    bool operator()(const time_value&) const {
        return std::holds_alternative<time_type>(type_);
    }
    bool operator()(const timestamp_value&) const {
        return std::holds_alternative<timestamp_type>(type_);
    }
    bool operator()(const timestamptz_value&) const {
        return std::holds_alternative<timestamptz_type>(type_);
    }
    bool operator()(const string_value&) const {
        return std::holds_alternative<string_type>(type_);
    }
    bool operator()(const uuid_value&) const {
        return std::holds_alternative<uuid_type>(type_);
    }
    bool operator()(const fixed_value&) const {
        return std::holds_alternative<fixed_type>(type_);
    }
    bool operator()(const binary_value&) const {
        return std::holds_alternative<binary_type>(type_);
    }
};

} // namespace

bool value_matches_type(const primitive_value& v, const primitive_type& t) {
    return std::visit(type_validating_visitor{t}, v);
}

struct_accessor::ids_accessor_map_t
struct_accessor::from_struct_type(const struct_type& s) {
    ids_accessor_map_t ret;
    for (size_t i = 0; i < s.fields.size(); i++) {
        const auto& field = s.fields[i];
        if (!field) {
            continue;
        }
        const auto& child = field->type;
        if (std::holds_alternative<primitive_type>(child)) {
            ret.emplace(
              field->id,
              std::make_unique<struct_accessor>(
                i, std::get<primitive_type>(child)));
        } else if (std::holds_alternative<struct_type>(child)) {
            const auto& child_as_struct = std::get<struct_type>(child);
            auto child_accessors = from_struct_type(child_as_struct);
            for (auto& [id, child_accessor] : child_accessors) {
                ret.emplace(
                  id,
                  std::make_unique<struct_accessor>(
                    i, std::move(child_accessor)));
            }
        }
        // Fallthrough in case of a list or map. It is valid for a top-level
        // type to include a list or a map --  we will not return accessors for
        // them or any descendants of them.
    }
    return ret;
}

const std::optional<value>&
struct_accessor::get(const struct_value& parent_val) const {
    if (position_ >= parent_val.fields.size()) {
        throw std::invalid_argument(fmt::format(
          "Invalid access to position {}, struct has {} fields",
          position_,
          parent_val.fields.size()));
    }
    const auto& child_val = parent_val.fields[position_];
    if (!child_val) {
        return child_val;
    }
    if (inner_) {
        if (!std::holds_alternative<std::unique_ptr<struct_value>>(
              *child_val)) {
            throw std::invalid_argument("Unexpected non-struct value");
        }
        const auto& child_as_struct = std::get<std::unique_ptr<struct_value>>(
          *child_val);
        return inner_->get(*child_as_struct);
    }
    if (!std::holds_alternative<primitive_value>(*child_val)) {
        throw std::invalid_argument(
          fmt::format("Unexpected non-primitive value: {}", *child_val));
    }
    const primitive_value& prim_val = std::get<primitive_value>(*child_val);
    if (!value_matches_type(prim_val, type_)) {
        throw std::invalid_argument(
          fmt::format("Expected value of {} type, got {}", type_, prim_val));
    }
    return child_val;
}

} // namespace iceberg
