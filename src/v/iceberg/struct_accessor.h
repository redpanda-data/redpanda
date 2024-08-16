// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/chunked_hash_map.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"

namespace iceberg {

// Returns true if the given value is of the given type.
bool value_matches_type(const primitive_value&, const primitive_type&);

// Nested accessor to get child values from a struct value, e.g. to build a
// partition key input for a struct value.
//
// Example usage, as pseudo-code:
//
//   ids_to_accessors = struct_accessor::from_struct_type(struct_type)
//   field_value = ids_to_accessors[field_id(0)].get(struct_value)
//
// NOTE: intended use is just for building the partition key, and therefore no
// support for searching lists or maps is done here -- just structs and
// primitives. Other partitioning values are not supported by the Iceberg spec.
class struct_accessor {
public:
    using ids_accessor_map_t
      = chunked_hash_map<nested_field::id_t, std::unique_ptr<struct_accessor>>;

    static ids_accessor_map_t from_struct_type(const struct_type&);

    // Returns the child value from the given struct at `position_`.
    const std::optional<value>& get(const struct_value& parent_val) const;

    // Public for make_unique<> only.
    struct_accessor(size_t position, const primitive_type& type)
      : position_(position)
      , type_(type) {}
    struct_accessor(size_t position, std::unique_ptr<struct_accessor> inner)
      : position_(position)
      , type_(inner->type_)
      , inner_(std::move(inner)) {}

private:
    // The position that this accessor will operate on when calling get().
    const size_t position_;

    // If `inner_` is set, `position_` is expected to refer to a struct field
    // by callers of get() and the caller will be returned that struct value.
    // Otherwise, the value referred to by `position_` is expected to be a
    // value of type `type_`.
    primitive_type type_;
    std::unique_ptr<struct_accessor> inner_;
};

} // namespace iceberg
