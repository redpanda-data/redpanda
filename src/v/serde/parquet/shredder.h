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

#include "base/seastarx.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"

#include <seastar/core/future.hh>

#include <absl/functional/function_ref.h>

namespace serde::parquet {

// A shredded value that has an assigned repetition level
// (for determining list index) and definition level (for
// determining where the null value in the hierarchy is).
struct shredded_value {
    int32_t schema_element_position;
    value val;
    rep_level rep_level;
    def_level def_level;
};

// Preform the dremel record shredding algoritm on this struct,
// emitting shredded_values as they are emitted.
//
// NOTE: the caller is responsible for ensuring that the schema
// element and the callback both live until the returned future
// completes.
ss::future<> shred_record(
  const schema_element& root,
  group_value record,
  absl::FunctionRef<ss::future<>(shredded_value)> callback);

} // namespace serde::parquet
