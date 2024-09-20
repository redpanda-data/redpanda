// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/datatypes.h"

#include <variant>

namespace iceberg {

struct primitive_type_comparison_visitor {
    template<typename T, typename U>
    bool operator()(const T&, const U&) const {
        return false;
    }
    bool operator()(const decimal_type& lhs, const decimal_type& rhs) const {
        return lhs.precision == rhs.precision && lhs.scale == rhs.scale;
    }
    bool operator()(const fixed_type& lhs, const fixed_type& rhs) const {
        return lhs.length == rhs.length;
    }
    template<typename T>
    bool operator()(const T&, const T&) const {
        return true;
    }
};

bool operator==(const primitive_type& lhs, const primitive_type& rhs) {
    return std::visit(primitive_type_comparison_visitor{}, lhs, rhs);
}

struct field_type_comparison_visitor {
    template<typename T, typename U>
    bool operator()(const T&, const U&) const {
        return false;
    }
    bool
    operator()(const primitive_type& lhs, const primitive_type& rhs) const {
        return lhs == rhs;
    }
    bool operator()(const struct_type& lhs, const struct_type& rhs) const {
        return lhs == rhs;
    }
    bool operator()(const list_type& lhs, const list_type& rhs) const {
        return lhs == rhs;
    }
    bool operator()(const map_type& lhs, const map_type& rhs) const {
        return lhs == rhs;
    }
};

bool operator==(const field_type& lhs, const field_type& rhs) {
    return std::visit(field_type_comparison_visitor{}, lhs, rhs);
}
bool operator==(const nested_field& lhs, const nested_field& rhs) {
    return lhs.id == rhs.id && lhs.required == rhs.required
           && lhs.name == rhs.name && lhs.type == rhs.type;
}

namespace {

struct type_copying_visitor {
    field_type operator()(const primitive_type& t) { return make_copy(t); }
    field_type operator()(const struct_type& t) {
        struct_type ret;
        for (const auto& field_ptr : t.fields) {
            ret.fields.emplace_back(field_ptr ? field_ptr->copy() : nullptr);
        }
        return ret;
    }
    field_type operator()(const list_type& t) {
        list_type ret;
        if (t.element_field) {
            ret.element_field = t.element_field->copy();
        }
        return ret;
    }
    field_type operator()(const map_type& t) {
        map_type ret;
        if (t.key_field) {
            ret.key_field = t.key_field->copy();
        }
        if (t.value_field) {
            ret.value_field = t.value_field->copy();
        }
        return ret;
    }
};

} // namespace

primitive_type make_copy(const primitive_type& type) { return type; }

field_type make_copy(const field_type& type) {
    return std::visit(type_copying_visitor{}, type);
}

std::ostream& operator<<(std::ostream& o, const boolean_type&) {
    o << "boolean";
    return o;
}

std::ostream& operator<<(std::ostream& o, const int_type&) {
    o << "int";
    return o;
}

std::ostream& operator<<(std::ostream& o, const long_type&) {
    o << "long";
    return o;
}

std::ostream& operator<<(std::ostream& o, const float_type&) {
    o << "float";
    return o;
}

std::ostream& operator<<(std::ostream& o, const double_type&) {
    o << "double";
    return o;
}

std::ostream& operator<<(std::ostream& o, const decimal_type& t) {
    o << fmt::format("decimal({}, {})", t.precision, t.scale);
    return o;
}

std::ostream& operator<<(std::ostream& o, const date_type&) {
    o << "date";
    return o;
}

std::ostream& operator<<(std::ostream& o, const time_type&) {
    o << "time";
    return o;
}

std::ostream& operator<<(std::ostream& o, const timestamp_type&) {
    o << "timestamp";
    return o;
}

std::ostream& operator<<(std::ostream& o, const timestamptz_type&) {
    o << "timestamptz";
    return o;
}

std::ostream& operator<<(std::ostream& o, const string_type&) {
    o << "string";
    return o;
}

std::ostream& operator<<(std::ostream& o, const uuid_type&) {
    o << "uuid";
    return o;
}

std::ostream& operator<<(std::ostream& o, const fixed_type& t) {
    // NOTE: square brackets to match how fixed type is serialized as JSON,
    // though this matching isn't necessarily important for operator<<.
    o << fmt::format("fixed[{}]", t.length);
    return o;
}

std::ostream& operator<<(std::ostream& o, const binary_type&) {
    o << "binary";
    return o;
}

std::ostream& operator<<(std::ostream& o, const struct_type&) {
    o << "struct";
    return o;
}

std::ostream& operator<<(std::ostream& o, const list_type&) {
    o << "list";
    return o;
}

std::ostream& operator<<(std::ostream& o, const map_type&) {
    o << "map";
    return o;
}

bool operator==(const struct_type& lhs, const struct_type& rhs) {
    if (lhs.fields.size() != rhs.fields.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.fields.size(); i++) {
        bool has_lhs = lhs.fields[i] != nullptr;
        bool has_rhs = rhs.fields[i] != nullptr;
        if (has_lhs != has_rhs) {
            return false;
        }
        if (has_lhs == false) {
            continue;
        }
        if (*lhs.fields[i] != *rhs.fields[i]) {
            return false;
        }
    }
    return true;
}
bool operator==(const list_type& lhs, const list_type& rhs) {
    bool has_lhs = lhs.element_field != nullptr;
    bool has_rhs = rhs.element_field != nullptr;
    if (has_lhs != has_rhs) {
        return false;
    }
    if (!has_lhs) {
        // Both nullptr.
        return true;
    }
    return *lhs.element_field == *rhs.element_field;
}
bool operator==(const map_type& lhs, const map_type& rhs) {
    bool has_key_lhs = lhs.key_field != nullptr;
    bool has_key_rhs = rhs.key_field != nullptr;
    if (has_key_lhs != has_key_rhs) {
        return false;
    }
    bool has_val_lhs = lhs.value_field != nullptr;
    bool has_val_rhs = rhs.value_field != nullptr;
    if (has_val_lhs != has_val_rhs) {
        return false;
    }
    if (has_key_lhs && *lhs.key_field != *rhs.key_field) {
        return false;
    }
    if (has_val_lhs && *lhs.value_field != *rhs.value_field) {
        return false;
    }
    return true;
}

namespace {
struct ostream_visitor {
    explicit ostream_visitor(std::ostream& o)
      : os(o) {}
    std::ostream& os;

    template<typename T>
    void operator()(const T& v) const {
        os << v;
    }
};
} // namespace

std::ostream& operator<<(std::ostream& o, const primitive_type& t) {
    std::visit(ostream_visitor{o}, t);
    return o;
}

std::ostream& operator<<(std::ostream& o, const field_type& t) {
    std::visit(ostream_visitor{o}, t);
    return o;
}

struct_type struct_type::copy() const {
    chunked_vector<nested_field_ptr> fields_copy;
    fields_copy.reserve(fields.size());
    for (const auto& f : fields) {
        fields_copy.emplace_back(f->copy());
    }
    return {std::move(fields_copy)};
}

list_type list_type::create(
  int32_t element_id, field_required element_required, field_type element) {
    // NOTE: the element field doesn't have a name. Functionally, the list type
    // is represented as:
    // - element-id
    // - element-type
    // - element-required
    // Despite the missing name though, many Iceberg implementations represent
    // the list with a nested_field.
    return list_type{
      .element_field = nested_field::create(
        element_id, "element", element_required, std::move(element))};
}

map_type map_type::create(
  int32_t key_id,
  field_type key_type,
  int32_t val_id,
  field_required val_req,
  field_type val_type) {
    // NOTE: the keys and values don't have names, and the key is always
    // required. Functionally, a map type is represented as:
    // - key-id
    // - key-type
    // - value-id
    // - value-required
    // - value-type
    // Despite the missing names though, many Iceberg implementations represent
    // the map with two nested_fields.
    return map_type{
      .key_field = nested_field::create(
        key_id, "key", field_required::yes, std::move(key_type)),
      .value_field = nested_field::create(
        val_id, "value", val_req, std::move(val_type)),
    };
}

nested_field_ptr nested_field::copy() const {
    return nested_field::create(id, name, required, make_copy(type));
};

} // namespace iceberg
