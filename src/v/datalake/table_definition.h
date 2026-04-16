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

#include "datalake/schema_descriptor.h"
#include "iceberg/schema.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"

namespace datalake {

// The schema is defined once here as a type. All field ordering,
// naming, and type information is derived from this single source.
// Adding, removing, or reordering fields here automatically updates:
//   - schemaless_struct_type() (runtime struct_type)
//   - build_rp_struct() (runtime struct_value)
//   - All typed field accessors

/// Header key/value struct inside the headers list.
using header_kv_desc = struct_desc<
  field_desc<"key", iceberg::string_type>,
  field_desc<"value", iceberg::binary_type>>;

/// The redpanda system struct.
using rp_desc = struct_desc<
  field_desc<"partition", iceberg::int_type>,
  field_desc<"offset", iceberg::long_type>,
  field_desc<"timestamp", iceberg::timestamptz_type>,
  field_desc<"headers", list_desc<header_kv_desc>>,
  field_desc<"key", iceberg::binary_type>,
  field_desc<"timestamp_type", iceberg::int_type>>;

/// The top-level schemaless table struct.
using schemaless_desc = struct_desc<field_desc<"redpanda", rp_desc>>;

inline constexpr std::string_view rp_struct_name = "redpanda";

/// Next available pre-assignment field ID after the schemaless struct.
/// Translators that add fields should start IDs from here.
inline const int schemaless_next_field_id = schemaless_desc::total_fields();

/// Build the runtime struct_type from the compile-time descriptor.
iceberg::struct_type schemaless_struct_type();

/// Build the default iceberg schema.
iceberg::schema default_schema();

/// Build the redpanda system struct_value. Single definition used
/// by all translators.
std::unique_ptr<iceberg::struct_value> build_rp_struct(
  model::partition_id pid,
  kafka::offset o,
  std::optional<iobuf> key,
  model::timestamp ts,
  model::timestamp_type ts_t,
  const chunked_vector<model::record_header>& headers);

/// Get the redpanda struct_type from a schemaless struct_type.
inline iceberg::struct_type& rp_struct_type(iceberg::struct_type& schemaless) {
    return std::get<iceberg::struct_type>(
      type_field<schemaless_desc, "redpanda">(schemaless).type);
}

/// Get the redpanda struct_value from a data row.
inline iceberg::struct_value& rp_struct_value(iceberg::struct_value& row) {
    return *std::get<std::unique_ptr<iceberg::struct_value>>(
      value_field<schemaless_desc, "redpanda">(row).value());
}

} // namespace datalake
