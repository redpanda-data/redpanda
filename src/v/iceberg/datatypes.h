/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "container/chunked_vector.h"
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

    // `nested_name` contains components of the nested field name in increasing
    // depth order. If the field is not found, returns nullptr.
    const nested_field*
    find_field_by_name(const std::vector<ss::sstring>& nested_name) const;
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

    /**
     * evolution_metadata - Variant holding annotation related to schema
     * evolution (i.e. transforming some 'source' schema into some 'destination'
     * schema).
     *
     * The 'meta' field is populated during a schema struct "annotation" pass
     * (see compatibility.h for detail), for fields under both structs in a
     * traversal, though with different value types depending on whether
     * 'source' or 'destination' (see below).
     *
     * - src_info: For destination schema fields that are backwards compatible
     *   with some source schema field
     *   - id: source field ID, assigned to destination field ID
     *   - required: requiredness of source field, validated against proposed
     *     requiredness of destination field
     *   - type: Optionally, the primitive type of the source field, validated
     *     against proposed type of the destination field
     * - is_new: For destination schema fields that do not appear in the source
     *   schema
     * - removed: For source schema fields, indicating whether the field was
     *   dropped from the schema (i.e. does not have a corresponding destination
     *   field)
     */
    struct src_info {
        id_t id{};
        field_required required{};
        std::optional<primitive_type> type{};
    };
    struct is_new {};
    using removed = ss::bool_class<struct removed_field_tag>;
    using evolution_metadata
      = std::variant<std::nullopt_t, src_info, is_new, removed>;
    mutable evolution_metadata meta{std::nullopt};

    void set_evolution_metadata(evolution_metadata v) const;
    bool has_evolution_metadata() const;
    bool is_add() const;
    bool is_drop() const;

    static nested_field_ptr create(
      int32_t id,
      ss::sstring name,
      field_required req,
      field_type t,
      evolution_metadata meta = std::nullopt) {
        return std::make_unique<nested_field>(
          id_t{id}, std::move(name), req, std::move(t), std::nullopt, meta);
    }

    nested_field_ptr copy() const;

    friend bool operator==(const nested_field& lhs, const nested_field& rhs);
    friend std::ostream& operator<<(std::ostream&, const nested_field&);
};

} // namespace iceberg
