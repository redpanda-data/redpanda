/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "container/fragmented_vector.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <cstdint>
#include <optional>
#include <variant>

namespace serde::parquet {

using def_level = named_type<int16_t, struct def_level_tag>;
using rep_level = named_type<int16_t, struct rep_level_tag>;

struct bool_type {
    bool operator==(const bool_type&) const = default;
};
struct i32_type {
    bool operator==(const i32_type&) const = default;
};
struct i64_type {
    bool operator==(const i64_type&) const = default;
};
struct f32_type {
    bool operator==(const f32_type&) const = default;
};
struct f64_type {
    bool operator==(const f64_type&) const = default;
};
struct byte_array_type {
    std::optional<int32_t> fixed_length;
    bool operator==(const byte_array_type&) const = default;
};

/**
 * Types supported by Parquet.  These types are intended to be used in
 * combination with the encodings to control the on disk storage format. For
 * example INT16 is not included as a type since a good encoding of INT32 would
 * handle this.
 */
using physical_type = std::variant<
  std::monostate,
  bool_type,
  i32_type,
  i64_type,
  f32_type,
  f64_type,
  byte_array_type>;

/**
 * Representation of Schemas
 */
enum class field_repetition_type : uint8_t {
    /**
     * This field is required (can not be null) and each row has exactly 1
     * value.
     */
    required = 0,

    /** The field is optional (can be null) and each row has 0 or 1 values. */
    optional = 1,

    /** The field is repeated and can contain 0 or more values */
    repeated = 2,
};

/** Empty structs to use as logical type annotations */

// allowed for BYTE_ARRAY, must be encoded with UTF-8
struct string_type {
    bool operator==(const string_type&) const = default;
};
// allowed for FIXED[16], must encoded raw UUID bytes
struct uuid_type {
    bool operator==(const uuid_type&) const = default;
};
// see LogicalTypes.md
struct map_type {
    bool operator==(const map_type&) const = default;
};
// see LogicalTypes.md
struct list_type {
    bool operator==(const list_type&) const = default;
};
// allowed for BYTE_ARRAY, must be encoded with UTF-8
struct enum_type {
    bool operator==(const enum_type&) const = default;
};
// allowed for INT32
struct date_type {
    bool operator==(const date_type&) const = default;
};
// allowed for FIXED[2], must encoded raw FLOAT16 bytes
struct f16_type {
    bool operator==(const f16_type&) const = default;
};

/**
 * Logical type to annotate a column that is always null.
 *
 * Sometimes when discovering the schema of existing data, values are always
 * null and the physical type can't be determined. This annotation signals
 * the case where the physical type was guessed from all null values.
 */
struct null_type {
    bool operator==(const null_type&) const = default;
}; // allowed for any physical type, only null values stored

/**
 * Decimal logical type annotation
 *
 * Scale must be zero or a positive integer less than or equal to the precision.
 * Precision must be a non-zero positive integer.
 *
 * To maintain forward-compatibility in v1, implementations using this logical
 * type must also set scale and precision on the annotated SchemaElement.
 *
 * Allowed for physical types: INT32, INT64, FIXED_LEN_BYTE_ARRAY, and
 * BYTE_ARRAY.
 */
struct decimal_type {
    int32_t scale;
    int32_t precision;
    bool operator==(const decimal_type&) const = default;
};

/**
 * Time units for logical types.
 *
 * This is encoded as a union of empty structs in the spec, which is rather
 * silly, but we represent it as an enum to simplify things.
 */
enum class time_unit : int8_t {
    millis = 1,
    micros = 2,
    nanos = 3,
};

/**
 * Timestamp logical type annotation
 *
 * Allowed for physical types: INT64
 */
struct timestamp_type {
    bool is_adjusted_to_utc;
    time_unit unit;
    bool operator==(const timestamp_type&) const = default;
};

/**
 * Time logical type annotation
 *
 * Allowed for physical types: INT32 (millis), INT64 (micros, nanos)
 */
struct time_type {
    bool is_adjusted_to_utc;
    time_unit unit;
    bool operator==(const time_type&) const = default;
};

/**
 * Integer logical type annotation
 *
 * bitWidth must be 8, 16, 32, or 64.
 *
 * Allowed for physical types: INT32, INT64
 */
struct int_type {
    int8_t bit_width = 0;
    bool is_signed = false;
    bool operator==(const int_type&) const = default;
};

/**
 * Embedded JSON logical type annotation
 *
 * Allowed for physical types: BYTE_ARRAY
 */
struct json_type {
    bool operator==(const json_type&) const = default;
};

/**
 * Embedded BSON logical type annotation
 *
 * Allowed for physical types: BYTE_ARRAY
 */
struct bson_type {
    bool operator==(const bson_type&) const = default;
};

/**
 * LogicalType annotations to replace ConvertedType.
 *
 * To maintain compatibility, implementations using LogicalType for a
 * SchemaElement must also set the corresponding ConvertedType (if any)
 * from the following table.
 */
using logical_type = std::variant<
  std::monostate,
  string_type,  // use ConvertedType UTF8
  map_type,     // use ConvertedType MAP
  list_type,    // use ConvertedType LIST
  enum_type,    // use ConvertedType ENUM
  decimal_type, // use ConvertedType DECIMAL + SchemaElement.{scale, precision}
  date_type,    // use ConvertedType DATE
  // use ConvertedType TIME_MICROS for TIME(isAdjustedToUTC = *, unit = MICROS)
  // use ConvertedType TIME_MILLIS for TIME(isAdjustedToUTC = *, unit = MILLIS)
  time_type,
  // use ConvertedType TIMESTAMP_MICROS for
  // TIMESTAMP(isAdjustedToUTC=*, unit=MICROS)
  // use ConvertedType TIMESTAMP_MILLIS for
  // TIMESTAMP(isAdjustedToUTC=*, unit=MILLIS)
  timestamp_type,
  int_type,  // use ConvertedType INT_* or UINT_*
  null_type, // no compatible ConvertedType
  json_type, // use ConvertedType JSON
  bson_type, // use ConvertedType BSON
  uuid_type, // no compatible ConvertedType
  f16_type>; // no compatible ConvertedType

/**
 * Represents a element inside a schema definition.
 *  - if it is a group (inner node) then type is undefined and num_children is
 * defined
 *  - if it is a primitive type (leaf) then type is defined and num_children is
 * undefined the nodes are listed in depth first traversal order.
 */
struct schema_element {
    /**
     * The overall index of the schema element within the schema.
     *
     * This is effectively an ID for the schema_element.
     *
     * This is filled out during schema indexing.
     */
    int32_t position = -1;

    /**
     * The physical encoding of the data. Left unset for intermediate nodes.
     */
    physical_type type;

    /**
     * repetition of the field. The root of the schema does not have a
     * repetition_type. All other nodes must have one.
     *
     * This must be set to required on the schema root
     */
    field_repetition_type repetition_type{field_repetition_type::required};

    /**
     * The full path of the node within the schema.
     *
     * During initial construction of the schema tree, only the name should be
     * placed here (IE the last segment of the path). During schema indexing
     * the ancestor path segments will be prepended to this path.
     *
     * This is never empty, the root element has only a single segment - the
     * name of the schema.
     */
    chunked_vector<ss::sstring> path;

    /** Name of the field in the schema. */
    const ss::sstring& name() const;

    /**
     * Nested fields.
     */
    chunked_vector<schema_element> children;

    /**
     * When the original schema supports field ids, this will save the
     * original field id in the parquet schema
     */
    std::optional<int32_t> field_id;

    /**
     * For leaf nodes, the logical type the physical bytes represent.
     */
    logical_type logical_type;

    /**
     * The maximum definition level for this node.
     *
     * The definition level is used to determine where the `null` value is in a
     * heirarchy of schema nodes. See shredder.h for more on definition level
     * and how it's computed.
     *
     * This is filled out during schema indexing.
     */
    def_level max_definition_level = def_level(-1);

    /**
     * The maximum repetition level for this node.
     *
     * The repetition level is used to determine the list index in a repeated
     * value heirarchy of (possibility repeated) schema nodes. See shredder.h
     * for more on repetition level and how it's computed.
     *
     * This is filled out during schema indexing.
     */
    rep_level max_repetition_level = rep_level(-1);

    /**
     * A simple depth first traversal of the schema.
     *
     * This is the required order of the flattened schema in
     * the parquet metadata, and also the ordering we write
     * columns in.
     *
     * NOTE: This function uses the stack for recursion, so it's assumed that
     * input schemas are checked for limited depth already.
     */
    template<typename Func>
    requires std::is_invocable_v<Func, const schema_element&>
    void for_each(Func func) const {
        func(*this);
        for (const auto& child : children) {
            child.for_each(func);
        }
    }

    /** If this is a leaf in the schema.*/
    bool is_leaf() const;
    bool operator==(const schema_element&) const = default;
};

/**
 * Index the schema such that the position, and max level information is known
 * for each element.
 */
void index_schema(schema_element& root);

} // namespace serde::parquet

template<>
struct fmt::formatter<serde::parquet::schema_element>
  : fmt::formatter<std::string_view> {
    auto format(const serde::parquet::schema_element&, fmt::format_context& ctx)
      const -> decltype(ctx.out());
};
