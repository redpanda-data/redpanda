// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"
#include "container/fragmented_vector.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <variant>

namespace iceberg {

struct boolean_type {};
struct int_type {};
struct long_type {};
struct float_type {};
struct double_type {};
struct decimal_type {
    uint32_t precision;
    uint32_t scale;
};
struct date_type {};
struct time_type {};
struct timestamp_type {};
struct timestamptz_type {};
struct string_type {};
struct uuid_type {};
struct fixed_type {
    uint64_t length;
};
struct binary_type {};
using primitive_type = std::variant<
  boolean_type,
  int_type,
  long_type,
  float_type,
  double_type,
  decimal_type,
  date_type,
  time_type,
  timestamp_type,
  timestamptz_type,
  string_type,
  uuid_type,
  fixed_type,
  binary_type>;
bool operator==(const primitive_type& lhs, const primitive_type& rhs);

struct struct_type;
struct list_type;
struct map_type;
using field_type
  = std::variant<primitive_type, struct_type, list_type, map_type>;
bool operator==(const field_type& lhs, const field_type& rhs);
field_type make_copy(const field_type&);
primitive_type make_copy(const primitive_type&);

std::ostream& operator<<(std::ostream&, const boolean_type&);
std::ostream& operator<<(std::ostream&, const int_type&);
std::ostream& operator<<(std::ostream&, const long_type&);
std::ostream& operator<<(std::ostream&, const float_type&);
std::ostream& operator<<(std::ostream&, const double_type&);
std::ostream& operator<<(std::ostream&, const decimal_type&);
std::ostream& operator<<(std::ostream&, const date_type&);
std::ostream& operator<<(std::ostream&, const time_type&);
std::ostream& operator<<(std::ostream&, const timestamp_type&);
std::ostream& operator<<(std::ostream&, const timestamptz_type&);
std::ostream& operator<<(std::ostream&, const string_type&);
std::ostream& operator<<(std::ostream&, const uuid_type&);
std::ostream& operator<<(std::ostream&, const fixed_type&);
std::ostream& operator<<(std::ostream&, const binary_type&);
std::ostream& operator<<(std::ostream&, const struct_type&);
std::ostream& operator<<(std::ostream&, const list_type&);
std::ostream& operator<<(std::ostream&, const map_type&);
std::ostream& operator<<(std::ostream&, const primitive_type&);
std::ostream& operator<<(std::ostream&, const field_type&);

struct nested_field;
using nested_field_ptr = std::unique_ptr<nested_field>;
using field_required = ss::bool_class<struct field_opt_tag>;
struct struct_type {
    chunked_vector<nested_field_ptr> fields;
    friend bool operator==(const struct_type& lhs, const struct_type& rhs);
    struct_type copy() const;
};

struct list_type {
    nested_field_ptr element_field;
    friend bool operator==(const list_type& lhs, const list_type& rhs);

    static list_type create(
      int32_t element_id, field_required element_required, field_type element);
};

struct map_type {
    nested_field_ptr key_field;
    nested_field_ptr value_field;
    friend bool operator==(const map_type& lhs, const map_type& rhs);

    static map_type create(
      int32_t key_id,
      field_type key_type,
      int32_t val_id,
      field_required val_req,
      field_type val_type);
};

struct nested_field {
    using id_t = named_type<int32_t, struct field_id_tag>;
    id_t id;
    ss::sstring name;
    field_required required;
    field_type type;
    std::optional<ss::sstring> doc;
    // TODO: support initial-default and write-default optional literals.

    static nested_field_ptr
    create(int32_t id, ss::sstring name, field_required req, field_type t) {
        return std::make_unique<nested_field>(
          id_t{id}, std::move(name), req, std::move(t), std::nullopt);
    }

    nested_field_ptr copy() const;

    friend bool operator==(const nested_field& lhs, const nested_field& rhs);
};

} // namespace iceberg
