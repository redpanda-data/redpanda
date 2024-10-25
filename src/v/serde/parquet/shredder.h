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

#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"

namespace serde::parquet {

// A shredded value that has an assigned repetition level
// (for determining list index) and definition level (for
// determining where the null value in the hierarchy is).
struct shredded_value {
    int32_t schema_element_index;
    value val;
    uint8_t rep_level;
    uint8_t def_level;
};

// Preform the dremel record shredding algoritm on this struct,
// emitting shredded_values as they are emitted.
void shred_record(
  const indexed_schema_element& root,
  struct_value record,
  absl::FunctionRef<void(shredded_value)> callback);

} // namespace serde::parquet
