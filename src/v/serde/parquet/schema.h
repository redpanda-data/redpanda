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

#include <seastar/core/sstring.hh>

#include <cstdint>
#include <optional>
#include <variant>

namespace serde::parquet {

struct bool_type {};
struct i32_type {};
struct i64_type {};
struct f32_type {};
struct f64_type {};
struct byte_array_type {
    std::optional<int32_t> fixed_length;
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
struct string_type {}; // allowed for BYTE_ARRAY, must be encoded with UTF-8
struct uuid_type {};   // allowed for FIXED[16], must encoded raw UUID bytes
struct map_type {};    // see LogicalTypes.md
struct list_type {};   // see LogicalTypes.md
struct enum_type {};   // allowed for BYTE_ARRAY, must be encoded with UTF-8
struct date_type {};   // allowed for INT32
struct f16_type {};    // allowed for FIXED[2], must encoded raw FLOAT16 bytes

/**
 * Logical type to annotate a column that is always null.
 *
 * Sometimes when discovering the schema of existing data, values are always
 * null and the physical type can't be determined. This annotation signals
 * the case where the physical type was guessed from all null values.
 */
struct null_type {}; // allowed for any physical type, only null values stored

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
};

/**
 * Time logical type annotation
 *
 * Allowed for physical types: INT32 (millis), INT64 (micros, nanos)
 */
struct time_type {
    bool is_adjusted_to_utc;
    time_unit unit;
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
};

/**
 * Embedded JSON logical type annotation
 *
 * Allowed for physical types: BYTE_ARRAY
 */
struct json_type {};

/**
 * Embedded BSON logical type annotation
 *
 * Allowed for physical types: BYTE_ARRAY
 */
struct bson_type {};

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
     * The physical encoding of the data. Left unset for intermediate nodes.
     */
    physical_type type;

    /**
     * repetition of the field. The root of the schema does not have a
     * repetition_type. All other nodes must have one.
     *
     * (this field is ignored on the schema root).
     */
    field_repetition_type repetition_type;

    /** Name of the field in the schema. */
    ss::sstring name;

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
};

} // namespace serde::parquet
